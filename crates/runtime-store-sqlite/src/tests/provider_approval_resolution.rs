use super::provider_native_approval_events::{fixture, submit};
use super::*;
use runtime_core::{ProviderApprovalFinalization, RuntimeError};
use std::sync::{Arc, Barrier};

fn ready() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    ApprovalRecord,
    TurnRecord,
    SessionRecord,
) {
    let (tmp, repo, session, turn) = fixture();
    let admitted = submit(&repo, &session, &turn, "apr_outcome_1").unwrap();
    let mut reserved = admitted.approval.clone();
    reserved.status = "decision_pending".into();
    reserved.response = Some(serde_json::json!({
        "decision":"accept", "payload":{"operator":"approved"},
        "decision_attempt_id":"attempt_a",
    }));
    let event = NewRuntimeEvent {
        event_id: format!("evt_approval_{}_decision_reserved_attempt_a", reserved.id),
        scope: RuntimeEventScope::Session,
        scope_id: reserved.session_id.clone(),
        session_id: Some(reserved.session_id.clone()),
        team_id: None,
        turn_id: Some(reserved.turn_id.clone()),
        kind: "approval.decision_reserved".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "approval_id":reserved.id, "decision_attempt_id":"attempt_a",
            "decision":"accept", "origin":"provider",
        }),
        provider: None,
        provider_seq: None,
        created_at: 300,
    };
    repo.claim_provider_approval_decision(
        &admitted.approval,
        &reserved,
        &admitted.turn,
        &admitted.session,
        &event,
    )
    .unwrap();
    (tmp, repo, reserved, admitted.turn, admitted.session)
}

fn resolved(reserved: &ApprovalRecord) -> ApprovalRecord {
    let mut after = reserved.clone();
    after.status = "accept".into();
    after.response = Some(serde_json::json!({"operator":"approved"}));
    after.resolved_at = Some(400);
    after
}

fn outcome_event(reserved: &ApprovalRecord, unknown: bool) -> NewRuntimeEvent {
    let mut payload = serde_json::json!({
        "approval_id":reserved.id,
        "decision_attempt_id":"attempt_a",
        "decision":"accept",
        "origin":"provider",
    });
    if unknown {
        payload["error"] = "ambiguous native write".into();
    }
    NewRuntimeEvent {
        event_id: if unknown {
            format!("evt_approval_{}_decision_unknown_attempt_a", reserved.id)
        } else {
            format!("evt_approval_{}_resolved", reserved.id)
        },
        scope: RuntimeEventScope::Session,
        scope_id: reserved.session_id.clone(),
        session_id: Some(reserved.session_id.clone()),
        team_id: None,
        turn_id: Some(reserved.turn_id.clone()),
        kind: if unknown {
            "approval.decision_unknown"
        } else {
            "approval.resolved"
        }
        .into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: 401,
    }
}

fn projections(
    reserved: &ApprovalRecord,
    turn: &TurnRecord,
    session: &SessionRecord,
    unknown: bool,
) -> (TurnRecord, SessionRecord) {
    let mut after_turn = turn.clone();
    let mut after_session = session.clone();
    if unknown {
        after_turn.status = "approval_decision_unknown".into();
        after_turn.error = Some(serde_json::json!({
            "message":"ambiguous native write", "approval_id":reserved.id,
            "decision_attempt_id":"attempt_a",
        }));
        after_session.status = "turn_recovery_required".into();
    } else {
        after_turn.status = "in_progress".into();
        after_turn.error = None;
        after_session.status = "turn_running".into();
    }
    after_session.updated_at += 1;
    (after_turn, after_session)
}

fn commit_result(
    repo: &SqliteRuntimeRepository,
    reserved: &ApprovalRecord,
    turn: &TurnRecord,
    session: &SessionRecord,
    unknown: bool,
) -> Result<ProviderApprovalFinalization, RuntimeError> {
    let (after_turn, after_session) = projections(reserved, turn, session, unknown);
    let event = outcome_event(reserved, unknown);
    if unknown {
        repo.quarantine_provider_approval_decision(
            reserved,
            turn,
            &after_turn,
            session,
            &after_session,
            &event,
        )
    } else {
        repo.finalize_provider_approval_decision(
            reserved,
            &resolved(reserved),
            turn,
            &after_turn,
            session,
            &after_session,
            &event,
        )
    }
}

#[test]
fn successful_native_ack_rolls_back_on_event_fault_and_replays_after_restart() {
    let (_tmp, repo, reserved, turn, session) = ready();
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_native_resolution BEFORE INSERT ON runtime_events
         WHEN NEW.kind='approval.resolved'
         BEGIN SELECT RAISE(ABORT, 'native resolution journal fault'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(commit_result(&repo, &reserved, &turn, &session, false).is_err());
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.approvals, vec![reserved.clone()]);
    assert_eq!(hydrated.turns, vec![turn.clone()]);
    assert_eq!(hydrated.sessions, vec![session.clone()]);
    assert_eq!(
        repo.list_runtime_events(None, None, 40)
            .unwrap()
            .iter()
            .filter(|row| row.kind == "approval.resolved")
            .count(),
        0
    );

    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_native_resolution")
        .unwrap();
    drop(connection);
    let first = commit_result(&repo, &reserved, &turn, &session, false).unwrap();
    assert_eq!(first.approval, resolved(&reserved));
    assert_eq!(first.turn.status, "in_progress");
    assert_eq!(first.session.status, "turn_running");
    assert_eq!(first.event.as_ref().unwrap().kind, "approval.resolved");
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let replay = commit_result(&reopened, &reserved, &turn, &session, false).unwrap();
    assert_eq!(replay.approval, first.approval);
    assert_eq!(replay.turn, first.turn);
    assert_eq!(replay.session, first.session);
    assert!(
        replay.event.is_none(),
        "lost reply cannot append a duplicate receipt"
    );
    assert_eq!(
        reopened
            .list_runtime_events(None, None, 40)
            .unwrap()
            .iter()
            .filter(|row| row.kind == "approval.resolved")
            .count(),
        1
    );
}

#[test]
fn ambiguous_native_return_quarantines_atomically_and_never_releases_reservation() {
    let (_tmp, repo, reserved, turn, session) = ready();
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_unknown BEFORE INSERT ON runtime_events
         WHEN NEW.kind='approval.decision_unknown'
         BEGIN SELECT RAISE(ABORT, 'unknown native journal fault'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(commit_result(&repo, &reserved, &turn, &session, true).is_err());
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.approvals, vec![reserved.clone()]);
    assert_eq!(hydrated.turns, vec![turn.clone()]);
    assert_eq!(hydrated.sessions, vec![session.clone()]);
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_unknown")
        .unwrap();
    drop(connection);

    let first = commit_result(&repo, &reserved, &turn, &session, true).unwrap();
    assert_eq!(first.approval, reserved);
    assert_eq!(first.turn.status, "approval_decision_unknown");
    assert_eq!(first.session.status, "turn_recovery_required");
    assert_eq!(
        first.event.as_ref().unwrap().kind,
        "approval.decision_unknown"
    );
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let replay = commit_result(&reopened, &reserved, &turn, &session, true).unwrap();
    assert!(replay.event.is_none());
    assert_eq!(replay.turn, first.turn);
    assert_eq!(replay.session, first.session);
    assert_eq!(replay.approval, first.approval);
    assert!(
        commit_result(&reopened, &reserved, &turn, &session, false).is_err(),
        "unresolved ambiguity cannot automatically become a successful resumption"
    );
}

#[test]
fn known_terminal_completion_before_native_ack_preserves_terminal_and_new_owner() {
    let (_tmp, repo, reserved, turn, session) = ready();
    let mut finished = turn.clone();
    finished.status = "completed".into();
    finished.completed_at = Some(350);
    let mut detached = session.clone();
    detached.status = "ready".into();
    detached.active_turn_id = None;
    detached.updated_at = 351;
    let terminal_event = NewRuntimeEvent {
        event_id: format!("evt_turn_{}_terminal", turn.id),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: "turn.completed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"status":"completed"}),
        provider: None,
        provider_seq: None,
        created_at: 351,
    };
    repo.commit_provider_turn_result(
        &turn,
        &finished,
        Some(&session),
        Some(&detached),
        &terminal_event,
    )
    .unwrap();

    let first = commit_result(&repo, &reserved, &turn, &session, false).unwrap();
    assert_eq!(first.approval.status, "accept");
    assert_eq!(first.turn, finished);
    assert_eq!(first.session, detached);
    let mut newer = detached.clone();
    newer.status = "turn_running".into();
    newer.active_turn_id = Some("other_turn".into());
    newer.updated_at += 2;
    repo.upsert_session(&newer).unwrap();
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let replay = commit_result(&reopened, &reserved, &turn, &session, false).unwrap();
    assert_eq!(replay.turn, finished);
    assert_eq!(replay.session, newer);
    assert!(replay.event.is_none());
}

#[test]
fn stale_nonterminal_owner_and_forged_event_cannot_resolve_native_decision() {
    let (_tmp, repo, reserved, turn, session) = ready();
    let mut stale = session.clone();
    stale.active_turn_id = Some("another_turn".into());
    stale.updated_at += 2;
    repo.upsert_session(&stale).unwrap();
    assert!(matches!(
        commit_result(&repo, &reserved, &turn, &session, false),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().approvals,
        vec![reserved.clone()]
    );
    assert_eq!(repo.hydrate_runtime_state().unwrap().sessions, vec![stale]);
    assert!(repo
        .list_runtime_events(None, None, 40)
        .unwrap()
        .iter()
        .all(|row| row.kind != "approval.resolved"));

    let (_tmp, repo, reserved, turn, session) = ready();
    let (after_turn, after_session) = projections(&reserved, &turn, &session, false);
    let mut forged = outcome_event(&reserved, false);
    forged.payload["decision_attempt_id"] = "forged".into();
    assert!(matches!(
        repo.finalize_provider_approval_decision(
            &reserved,
            &resolved(&reserved),
            &turn,
            &after_turn,
            &session,
            &after_session,
            &forged,
        ),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().approvals,
        vec![reserved]
    );
}

#[test]
fn standalone_unknown_event_cannot_masquerade_as_atomic_quarantine() {
    let (_tmp, repo, reserved, turn, session) = ready();
    repo.append_runtime_event(&outcome_event(&reserved, true))
        .unwrap();
    assert!(matches!(
        commit_result(&repo, &reserved, &turn, &session, true),
        Err(RuntimeError::Conflict(_))
    ));
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.approvals, vec![reserved]);
    assert_eq!(hydrated.turns, vec![turn]);
    assert_eq!(hydrated.sessions, vec![session]);
}

#[test]
fn racing_known_and_ambiguous_outcomes_never_commit_competing_receipts() {
    let (_tmp, repo, reserved, turn, session) = ready();
    let barrier = Arc::new(Barrier::new(2));
    let jobs = [false, true]
        .into_iter()
        .map(|unknown| {
            let repo = repo.clone();
            let reserved = reserved.clone();
            let turn = turn.clone();
            let session = session.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                commit_result(&repo, &reserved, &turn, &session, unknown)
            })
        })
        .collect::<Vec<_>>();
    let results = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let events = repo.list_runtime_events(None, None, 40).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|row| matches!(
                row.kind.as_str(),
                "approval.resolved" | "approval.decision_unknown"
            ))
            .count(),
        1
    );
}
