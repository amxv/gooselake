use super::provider_native_approval_events::{fixture, submit};
use super::*;
use runtime_core::{ProviderApprovalDecisionReceipt, RuntimeError};
use std::sync::{Arc, Barrier};

fn ready() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    ApprovalRecord,
    TurnRecord,
    SessionRecord,
) {
    let (temp, repository, session, turn) = fixture();
    let admitted = submit(&repository, &session, &turn, "apr_decision_1").unwrap();
    (
        temp,
        repository,
        admitted.approval,
        admitted.turn,
        admitted.session,
    )
}

fn decision(pending: &ApprovalRecord, attempt: &str) -> ApprovalRecord {
    let mut reserved = pending.clone();
    reserved.status = "decision_pending".into();
    reserved.response = Some(serde_json::json!({
        "decision":"accept",
        "payload":{"test":true},
        "decision_attempt_id":attempt,
    }));
    reserved
}

fn event(pending: &ApprovalRecord, attempt: &str, released: bool) -> NewRuntimeEvent {
    let suffix = if released {
        "not_dispatched"
    } else {
        "reserved"
    };
    NewRuntimeEvent {
        event_id: format!("evt_approval_{}_decision_{suffix}_{attempt}", pending.id),
        scope: RuntimeEventScope::Session,
        scope_id: pending.session_id.clone(),
        session_id: Some(pending.session_id.clone()),
        team_id: None,
        turn_id: Some(pending.turn_id.clone()),
        kind: format!("approval.decision_{suffix}"),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "approval_id": pending.id,
            "decision_attempt_id": attempt,
            "decision": "accept",
            "origin": "provider",
        }),
        provider: None,
        provider_seq: None,
        created_at: 300,
    }
}

fn claim(
    repo: &SqliteRuntimeRepository,
    pending: &ApprovalRecord,
    turn: &TurnRecord,
    session: &SessionRecord,
    attempt: &str,
) -> Result<ProviderApprovalDecisionReceipt, RuntimeError> {
    repo.claim_provider_approval_decision(
        pending,
        &decision(pending, attempt),
        turn,
        session,
        &event(pending, attempt, false),
    )
}

fn release(
    repo: &SqliteRuntimeRepository,
    pending: &ApprovalRecord,
    attempt: &str,
) -> Result<ProviderApprovalDecisionReceipt, RuntimeError> {
    repo.release_unexecuted_provider_approval_decision(
        &decision(pending, attempt),
        pending,
        &event(pending, attempt, true),
    )
}

#[test]
fn decision_claim_event_fault_leaves_approval_pending_and_retry_is_not_a_native_replay_token() {
    let (_temp, repo, pending, turn, session) = ready();
    let db = open_connection(&repo.database_path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_decision_reservation BEFORE INSERT ON runtime_events
         WHEN NEW.kind='approval.decision_reserved'
         BEGIN SELECT RAISE(ABORT, 'injected decision receipt failure'); END;",
    )
    .unwrap();
    drop(db);
    assert!(claim(&repo, &pending, &turn, &session, "attempt_1").is_err());
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.approvals, vec![pending.clone()]);
    assert_eq!(hydrated.turns, vec![turn.clone()]);
    assert_eq!(hydrated.sessions, vec![session.clone()]);
    assert_eq!(
        repo.list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 50)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "approval.decision_reserved")
            .count(),
        0
    );
    let db = open_connection(&repo.database_path).unwrap();
    db.execute_batch("DROP TRIGGER fail_decision_reservation")
        .unwrap();
    drop(db);
    let first = claim(&repo, &pending, &turn, &session, "attempt_1").unwrap();
    assert_eq!(first.approval.status, "decision_pending");
    assert_eq!(
        first.event.as_ref().unwrap().kind,
        "approval.decision_reserved"
    );
    let restarted = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert!(
        matches!(
            claim(&restarted, &pending, &turn, &session, "attempt_1"),
            Err(RuntimeError::Conflict(_))
        ),
        "existing reservation can never authorize a second native decision"
    );
    assert!(
        matches!(
            claim(&restarted, &pending, &turn, &session, "attempt_2"),
            Err(RuntimeError::Conflict(_))
        ),
        "other attempts also blocked while native outcome is unknown"
    );
    assert_eq!(
        restarted.hydrate_runtime_state().unwrap().approvals,
        vec![first.approval.clone()]
    );
    let rows = restarted
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 50)
        .unwrap();
    assert_eq!(
        rows.iter()
            .filter(|e| e.kind == "approval.decision_reserved")
            .count(),
        1
    );
}

#[test]
fn racing_independent_decision_writers_only_one_can_cross_native_gate() {
    let (_temp, repo, pending, turn, session) = ready();
    let barrier = Arc::new(Barrier::new(2));
    let workers = ["attempt_a", "attempt_b"]
        .into_iter()
        .map(|attempt| {
            let repo = repo.clone();
            let pending = pending.clone();
            let turn = turn.clone();
            let session = session.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                claim(&repo, &pending, &turn, &session, attempt)
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let committed = results.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().approvals,
        vec![committed.approval]
    );
    assert_eq!(
        repo.list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 50)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "approval.decision_reserved")
            .count(),
        1
    );
}

#[test]
fn proven_no_effect_release_is_atomic_and_allows_only_fresh_attempt_after_recovery() {
    let (_temp, repo, pending, turn, session) = ready();
    claim(&repo, &pending, &turn, &session, "attempt_1").unwrap();
    let db = open_connection(&repo.database_path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_no_effect_release BEFORE INSERT ON runtime_events
         WHEN NEW.kind='approval.decision_not_dispatched'
         BEGIN SELECT RAISE(ABORT, 'injected no-effect receipt failure'); END;",
    )
    .unwrap();
    drop(db);
    assert!(release(&repo, &pending, "attempt_1").is_err());
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().approvals,
        vec![decision(&pending, "attempt_1")]
    );
    let db = open_connection(&repo.database_path).unwrap();
    db.execute_batch("DROP TRIGGER fail_no_effect_release")
        .unwrap();
    drop(db);
    let returned = release(&repo, &pending, "attempt_1").unwrap();
    assert_eq!(returned.approval, pending);
    assert_eq!(
        returned.event.as_ref().unwrap().kind,
        "approval.decision_not_dispatched"
    );
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert!(release(&reopened, &pending, "attempt_1")
        .unwrap()
        .event
        .is_none());
    assert!(
        matches!(
            claim(&reopened, &pending, &turn, &session, "attempt_1"),
            Err(RuntimeError::Conflict(_))
        ),
        "proven no-effect release must not replay the old attempt ID"
    );
    assert_eq!(
        claim(&reopened, &pending, &turn, &session, "attempt_2")
            .unwrap()
            .approval
            .status,
        "decision_pending"
    );
    assert!(
        matches!(
            release(&reopened, &pending, "attempt_1"),
            Err(RuntimeError::Conflict(_))
        ),
        "stale no-effect release cannot rewind newer reservation"
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().approvals,
        vec![decision(&pending, "attempt_2")]
    );
    let events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 50)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "approval.decision_reserved")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "approval.decision_not_dispatched")
            .count(),
        1
    );
}

#[test]
fn stale_ownership_quarantine_and_forged_receipts_fail_closed_before_claim() {
    let (_temp, repo, pending, turn, session) = ready();
    let mut newer = session.clone();
    newer.active_turn_id = Some("turn_new".into());
    newer.updated_at += 1;
    repo.upsert_session(&newer).unwrap();
    assert!(matches!(
        claim(&repo, &pending, &turn, &session, "attempt_1"),
        Err(RuntimeError::Conflict(_))
    ));
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.approvals, vec![pending.clone()]);
    assert_eq!(hydrated.sessions, vec![newer]);
    assert_eq!(hydrated.turns, vec![turn.clone()]);
    let (_temp, repo, pending, turn, session) = ready();
    let mut quarantined = turn.clone();
    quarantined.status = "approval_decision_unknown".into();
    repo.upsert_turn(&quarantined).unwrap();
    assert!(matches!(
        claim(&repo, &pending, &turn, &session, "attempt_1"),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().approvals,
        vec![pending.clone()]
    );
    let (_temp, repo, pending, turn, session) = ready();
    let mut forged = event(&pending, "attempt_1", false);
    forged.payload["origin"] = serde_json::json!("untrusted");
    assert!(matches!(
        repo.claim_provider_approval_decision(
            &pending,
            &decision(&pending, "attempt_1"),
            &turn,
            &session,
            &forged
        ),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().approvals,
        vec![pending]
    );
}

#[test]
fn another_native_approval_remains_decidable_after_first_one_resumed_the_turn() {
    let (_temp, repo, pending, mut turn, mut session) = ready();
    // Two native tools can request approval on the same turn. Once another
    // approval resolves, its in-progress projection must not strand this
    // independently pending approval.
    turn.status = "in_progress".into();
    session.status = "turn_running".into();
    session.updated_at += 1;
    repo.upsert_turn(&turn).unwrap();
    repo.upsert_session(&session).unwrap();
    let first = claim(&repo, &pending, &turn, &session, "attempt_parallel").unwrap();
    assert_eq!(first.approval.status, "decision_pending");
    assert_eq!(first.event.unwrap().kind, "approval.decision_reserved");
    assert_eq!(repo.hydrate_runtime_state().unwrap().turns, vec![turn]);
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().sessions,
        vec![session]
    );
}
