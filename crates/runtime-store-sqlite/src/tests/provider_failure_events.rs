use super::*;
use runtime_core::{ProviderTerminalCommit, RuntimeError};
use std::sync::{Arc, Barrier};

fn fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    SessionRecord,
    TurnRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    repo.initialize_schema().unwrap();
    let mut session = sample_session();
    session.status = "turn_running".into();
    let mut turn = sample_turn();
    turn.status = "in_progress".into();
    session.active_turn_id = Some(turn.id.clone());
    repo.upsert_session(&session).unwrap();
    repo.upsert_turn(&turn).unwrap();
    (temp, repo, session, turn)
}

fn result_bundle(
    before_session: &SessionRecord,
    before_turn: &TurnRecord,
    unknown: bool,
) -> (SessionRecord, TurnRecord, NewRuntimeEvent) {
    let mut turn = before_turn.clone();
    let mut session = before_session.clone();
    let (kind, id, payload) = if unknown {
        turn.status = "provider_event_recovery_required".into();
        turn.error =
            Some(serde_json::json!({"code":"stream_lost","message":"provider bridge lost"}));
        session.status = "turn_recovery_required".into();
        session.failure_code = Some("stream_lost".into());
        session.failure_message = Some("provider bridge lost".into());
        (
            "provider.event_stream_unknown",
            format!("evt_turn_{}_event_stream_unknown", before_turn.id),
            serde_json::json!({"code":"stream_lost","message":"provider bridge lost"}),
        )
    } else {
        turn.status = "failed".into();
        turn.completed_at = Some(301);
        turn.error = Some(serde_json::json!({"message":"native wait failed"}));
        session.status = "failed".into();
        session.active_turn_id = None;
        session.failure_code = Some("provider_wait_failure".into());
        session.failure_message = Some("native wait failed".into());
        (
            "provider.error",
            format!("evt_turn_{}_wait_failure", before_turn.id),
            serde_json::json!({"error":"native wait failed"}),
        )
    };
    session.updated_at += 3;
    let event = NewRuntimeEvent {
        event_id: id,
        scope: RuntimeEventScope::Session,
        scope_id: before_session.id.clone(),
        session_id: Some(before_session.id.clone()),
        team_id: None,
        turn_id: Some(before_turn.id.clone()),
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: 301,
    };
    (session, turn, event)
}

fn commit(
    repo: &SqliteRuntimeRepository,
    before_session: &SessionRecord,
    before_turn: &TurnRecord,
    unknown: bool,
) -> Result<ProviderTerminalCommit, RuntimeError> {
    let (after_session, after_turn, event) = result_bundle(before_session, before_turn, unknown);
    repo.commit_provider_failure_result(
        before_turn,
        &after_turn,
        before_session,
        &after_session,
        &event,
    )
}

#[test]
fn provider_wait_error_fault_rolls_back_turn_session_and_critical_receipt() {
    let (_temp, repo, before_session, before_turn) = fixture();
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_wait_error BEFORE INSERT ON runtime_events
         WHEN NEW.kind='provider.error'
         BEGIN SELECT RAISE(ABORT, 'wait journal injection'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(commit(&repo, &before_session, &before_turn, false).is_err());
    let recovered = repo.hydrate_runtime_state().unwrap();
    assert_eq!(recovered.sessions, vec![before_session.clone()]);
    assert_eq!(recovered.turns, vec![before_turn.clone()]);
    assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_wait_error")
        .unwrap();
    drop(connection);

    let first = commit(&repo, &before_session, &before_turn, false).unwrap();
    assert_eq!(first.turn.status, "failed");
    assert_eq!(first.session.as_ref().unwrap().status, "failed");
    assert_eq!(first.event.kind, "provider.error");
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let repeat = commit(&reopened, &before_session, &before_turn, false).unwrap();
    assert_eq!(repeat, first);
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        1
    );
    let mut newer = first.session.unwrap();
    newer.status = "turn_running".into();
    newer.active_turn_id = Some("new_owner_turn".into());
    newer.updated_at += 5;
    reopened.upsert_session(&newer).unwrap();
    let replay = commit(&reopened, &before_session, &before_turn, false).unwrap();
    assert_eq!(replay.session, Some(newer));
    assert_eq!(replay.turn, first.turn);
}

#[test]
fn provider_stream_unknown_fault_rolls_back_then_reopens_exact_quarantine_receipt() {
    let (_temp, repo, before_session, before_turn) = fixture();
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_stream_unknown BEFORE INSERT ON runtime_events
         WHEN NEW.kind='provider.event_stream_unknown'
         BEGIN SELECT RAISE(ABORT, 'stream journal injection'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(commit(&repo, &before_session, &before_turn, true).is_err());
    let recovered = repo.hydrate_runtime_state().unwrap();
    assert_eq!(recovered.sessions, vec![before_session.clone()]);
    assert_eq!(recovered.turns, vec![before_turn.clone()]);
    assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_stream_unknown")
        .unwrap();
    drop(connection);

    let first = commit(&repo, &before_session, &before_turn, true).unwrap();
    assert_eq!(first.turn.status, "provider_event_recovery_required");
    assert_eq!(
        first.session.as_ref().unwrap().status,
        "turn_recovery_required"
    );
    assert_eq!(first.event.kind, "provider.event_stream_unknown");
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        commit(&reopened, &before_session, &before_turn, true).unwrap(),
        first
    );
    assert!(matches!(
        commit(&reopened, &before_session, &before_turn, false),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        1
    );
}

#[test]
fn competing_wait_failure_and_unknown_stream_projections_never_both_commit() {
    let (_temp, repo, before_session, before_turn) = fixture();
    let gate = Arc::new(Barrier::new(2));
    let workers = [false, true]
        .into_iter()
        .map(|unknown| {
            let gate = Arc::clone(&gate);
            let repo = repo.clone();
            let session = before_session.clone();
            let turn = before_turn.clone();
            std::thread::spawn(move || {
                gate.wait();
                commit(&repo, &session, &turn, unknown)
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    let durable = repo.hydrate_runtime_state().unwrap();
    assert_eq!(durable.turns, vec![winner.turn]);
    assert_eq!(durable.sessions, vec![winner.session.unwrap()]);
    assert_eq!(
        repo.list_runtime_events(None, None, 10).unwrap(),
        vec![winner.event]
    );
}

#[test]
fn stale_attachment_or_forged_receipts_fail_without_changing_authority() {
    let (_temp, repo, before_session, before_turn) = fixture();
    let mut newer = before_session.clone();
    newer.active_turn_id = Some("new_attached_turn".into());
    newer.updated_at += 5;
    repo.upsert_session(&newer).unwrap();
    assert!(matches!(
        commit(&repo, &before_session, &before_turn, true),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().turns,
        vec![before_turn.clone()]
    );
    assert_eq!(repo.hydrate_runtime_state().unwrap().sessions, vec![newer]);

    let (_temp, repo, before_session, before_turn) = fixture();
    let (after_session, after_turn, mut forged) =
        result_bundle(&before_session, &before_turn, true);
    forged.payload["code"] = "another_code".into();
    assert!(matches!(
        repo.commit_provider_failure_result(
            &before_turn,
            &after_turn,
            &before_session,
            &after_session,
            &forged,
        ),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    repo.append_runtime_event(&result_bundle(&before_session, &before_turn, true).2)
        .unwrap();
    assert!(matches!(
        commit(&repo, &before_session, &before_turn, true),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().turns,
        vec![before_turn]
    );
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().sessions,
        vec![before_session]
    );
}

#[test]
fn provider_terminal_result_and_wait_failure_race_cannot_mutate_each_others_state() {
    let (_temp, repo, before_session, before_turn) = fixture();
    let mut completed_turn = before_turn.clone();
    completed_turn.status = "completed".into();
    completed_turn.completed_at = Some(375);
    let mut completed_session = before_session.clone();
    completed_session.status = "ready".into();
    completed_session.active_turn_id = None;
    completed_session.updated_at += 5;
    let terminal_event = NewRuntimeEvent {
        event_id: format!("evt_turn_{}_terminal", before_turn.id),
        scope: RuntimeEventScope::Session,
        scope_id: before_session.id.clone(),
        session_id: Some(before_session.id.clone()),
        team_id: None,
        turn_id: Some(before_turn.id.clone()),
        kind: "turn.completed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"status":"completed"}),
        provider: None,
        provider_seq: None,
        created_at: 375,
    };
    let gate = Arc::new(Barrier::new(2));
    let first = {
        let repo = repo.clone();
        let gate = Arc::clone(&gate);
        let before_session = before_session.clone();
        let before_turn = before_turn.clone();
        std::thread::spawn(move || {
            gate.wait();
            commit(&repo, &before_session, &before_turn, false)
        })
    };
    let second = {
        let repo = repo.clone();
        let gate = Arc::clone(&gate);
        let before_session = before_session.clone();
        let before_turn = before_turn.clone();
        std::thread::spawn(move || {
            gate.wait();
            repo.commit_provider_turn_result(
                &before_turn,
                &completed_turn,
                Some(&before_session),
                Some(&completed_session),
                &terminal_event,
            )
        })
    };
    let results = [first.join().unwrap(), second.join().unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    let stored = repo.hydrate_runtime_state().unwrap();
    assert_eq!(stored.turns, vec![winner.turn]);
    assert_eq!(stored.sessions, vec![winner.session.unwrap()]);
    assert_eq!(
        repo.list_runtime_events(None, None, 20).unwrap(),
        vec![winner.event]
    );
}

#[test]
fn standalone_wait_receipt_with_only_turn_update_cannot_masquerade_as_atomic_session_commit() {
    let (_temp, repo, before_session, before_turn) = fixture();
    let (_after_session, after_turn, event) = result_bundle(&before_session, &before_turn, false);
    repo.upsert_turn(&after_turn).unwrap();
    repo.append_runtime_event(&event).unwrap();
    assert!(matches!(
        commit(&repo, &before_session, &before_turn, false),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().sessions,
        vec![before_session]
    );
}
