use super::*;
use runtime_core::{RuntimeError, RuntimeEventRecord};
use std::sync::{Arc, Barrier};

fn fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    SessionRecord,
    TurnRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut session = sample_session();
    session.status = "ready".into();
    session.active_turn_id = None;
    let mut turn = sample_turn();
    turn.status = "completed".into();
    turn.completed_at = Some(150);
    repository.upsert_session(&session).unwrap();
    repository.upsert_turn(&turn).unwrap();
    (temp, repository, session, turn)
}

fn conflict(
    session: &SessionRecord,
    turn: &TurnRecord,
    incoming: &str,
    at: i64,
) -> (SessionRecord, NewRuntimeEvent) {
    let mut failed = session.clone();
    failed.status = "failed".into();
    failed.failure_code = Some("terminal_conflict".into());
    failed.failure_message = Some(format!(
        "conflicting terminal state for turn {} (stored={}, incoming={})",
        turn.id, turn.status, incoming
    ));
    failed.updated_at = at;
    let event = NewRuntimeEvent {
        event_id: format!("evt_turn_{}_terminal_conflict", turn.id),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: "provider.terminal_conflict".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "stored_status": turn.status,
            "status": incoming,
            "reason": "idle_terminal_conflict",
            "state_preserved": false,
        }),
        provider: None,
        provider_seq: None,
        created_at: at,
    };
    (failed, event)
}

fn receipt_rows(
    repo: &SqliteRuntimeRepository,
    session: &SessionRecord,
) -> Vec<RuntimeEventRecord> {
    repo.list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 20)
        .unwrap()
}

#[test]
fn idle_conflict_event_fault_rolls_back_failure_then_exact_restart_replay_is_one_receipt() {
    let (_temp, repo, session, turn) = fixture();
    let (failed, event) = conflict(&session, &turn, "failed", 300);
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_terminal_conflict BEFORE INSERT ON runtime_events
             WHEN NEW.kind='provider.terminal_conflict'
             BEGIN SELECT RAISE(ABORT,'injected Critical insert failure'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(repo
        .commit_provider_terminal_conflict(&turn, &session, &failed, &event)
        .is_err());
    let after_failure = SqliteRuntimeRepository::new(repo.database_path.clone());
    let state = after_failure.hydrate_runtime_state().unwrap();
    assert_eq!(state.sessions[0], session);
    assert_eq!(state.turns[0], turn);
    assert!(receipt_rows(&after_failure, &session).is_empty());

    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_terminal_conflict")
        .unwrap();
    let (committed, receipt) = after_failure
        .commit_provider_terminal_conflict(&turn, &session, &failed, &event)
        .unwrap();
    assert_eq!(committed, failed);
    let receipt = receipt.expect("newly committed Critical receipt");
    assert_eq!(receipt.seq, 1);
    assert_eq!(receipt.payload["reason"], "idle_terminal_conflict");
    assert_eq!(receipt.criticality, RuntimeEventCriticality::Critical);
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let (later_failed, later_event) = conflict(&session, &turn, "failed", 400);
    assert_eq!(
        reopened
            .commit_provider_terminal_conflict(&turn, &session, &later_failed, &later_event)
            .unwrap(),
        (failed.clone(), None)
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().sessions[0],
        failed
    );
    assert_eq!(receipt_rows(&reopened, &session), vec![receipt]);
}

#[test]
fn newer_owner_closed_session_and_changed_original_turn_block_stale_conflicts() {
    for changed in ["newer_owner", "closed", "changed_turn"] {
        let (_temp, repo, session, turn) = fixture();
        let (failed, event) = conflict(&session, &turn, "failed", 300);
        let mut expected_session = session.clone();
        let mut expected_turn = turn.clone();
        match changed {
            "newer_owner" => {
                expected_session.status = "turn_running".into();
                expected_session.active_turn_id = Some("successor".into());
                expected_session.updated_at = 250;
                repo.upsert_session(&expected_session).unwrap();
            }
            "closed" => {
                expected_session.status = "closed".into();
                expected_session.closed_at = Some(240);
                expected_session.updated_at = 240;
                repo.upsert_session(&expected_session).unwrap();
            }
            _ => {
                expected_turn.status = "failed".into();
                repo.upsert_turn(&expected_turn).unwrap();
            }
        }
        assert!(matches!(
            repo.commit_provider_terminal_conflict(&turn, &session, &failed, &event),
            Err(RuntimeError::Conflict(_))
        ));
        let hydrated = repo.hydrate_runtime_state().unwrap();
        assert_eq!(hydrated.sessions[0], expected_session);
        assert_eq!(hydrated.turns[0], expected_turn);
        assert!(receipt_rows(&repo, &session).is_empty());
    }
}

#[test]
fn concurrent_independent_conflict_writers_commit_exactly_one_original_result() {
    let (_temp, repo, session, turn) = fixture();
    let gate = Arc::new(Barrier::new(2));
    let workers = ["failed", "interrupted"]
        .into_iter()
        .map(|incoming| {
            let repo = SqliteRuntimeRepository::new(repo.database_path.clone());
            let session = session.clone();
            let turn = turn.clone();
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                let (failed, event) = conflict(&session, &turn, incoming, 300);
                gate.wait();
                repo.commit_provider_terminal_conflict(&turn, &session, &failed, &event)
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(repo.hydrate_runtime_state().unwrap().sessions[0], winner.0);
    assert_eq!(receipt_rows(&repo, &session), vec![winner.1.unwrap()]);
}

#[test]
fn forged_receipt_and_quarantined_or_newly_owned_session_cannot_invent_failure_authority() {
    let (_temp, repo, session, turn) = fixture();
    let (failed, event) = conflict(&session, &turn, "failed", 300);
    let mut forged = event.clone();
    forged.payload["status"] = "interrupted".into();
    repo.append_runtime_event(&forged).unwrap();
    assert!(matches!(
        repo.commit_provider_terminal_conflict(&turn, &session, &failed, &event),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repo.hydrate_runtime_state().unwrap().sessions[0], session);
    assert_eq!(receipt_rows(&repo, &session).len(), 1);

    let mut recovery = session.clone();
    recovery.status = "turn_recovery_required".into();
    let (wrong_failed, wrong_event) = conflict(&recovery, &turn, "failed", 300);
    assert!(matches!(
        repo.commit_provider_terminal_conflict(&turn, &recovery, &wrong_failed, &wrong_event),
        Err(RuntimeError::ProtocolViolation(_))
    ));
}

#[test]
fn identical_but_uncommitted_critical_receipt_cannot_certify_a_failed_session() {
    let (_temp, repo, session, turn) = fixture();
    let (failed, event) = conflict(&session, &turn, "failed", 300);
    // An ordinary event append cannot counterfeit the missing atomic state
    // transition even if its payload/ID exactly match the canonical event.
    let forged = repo.append_runtime_event(&event).unwrap();
    assert!(matches!(
        repo.commit_provider_terminal_conflict(&turn, &session, &failed, &event),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repo.hydrate_runtime_state().unwrap().sessions[0], session);
    assert_eq!(receipt_rows(&repo, &session), vec![forged]);
}

#[test]
fn replay_after_another_manager_claims_newer_turn_returns_current_owner_without_new_event() {
    let (_temp, repo, session, turn) = fixture();
    let (failed, event) = conflict(&session, &turn, "failed", 300);
    let (_, original_receipt) = repo
        .commit_provider_terminal_conflict(&turn, &session, &failed, &event)
        .unwrap();
    let original_receipt = original_receipt.unwrap();
    let mut successor = failed.clone();
    successor.status = "turn_running".into();
    successor.active_turn_id = Some("newer-durable-turn".into());
    successor.failure_code = None;
    successor.failure_message = None;
    successor.updated_at = 400;
    let independent = SqliteRuntimeRepository::new(repo.database_path.clone());
    independent.upsert_session(&successor).unwrap();

    let (later, later_event) = conflict(&session, &turn, "failed", 500);
    assert_eq!(
        repo.commit_provider_terminal_conflict(&turn, &session, &later, &later_event)
            .unwrap(),
        (successor.clone(), None)
    );
    assert_eq!(
        independent.hydrate_runtime_state().unwrap().sessions[0],
        successor
    );
    assert_eq!(receipt_rows(&repo, &session), vec![original_receipt]);
}
