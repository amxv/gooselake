//! Pre-native legacy deferral/failure/explicit retry is one durable generation
//! change and Critical event. Never infer native retry authority from status.

use super::legacy_team_delivery_claim::claim_plan;
use super::legacy_team_messages::{commit, fixture, proposed};
use super::*;
use runtime_core::RuntimeError;
use std::sync::{Arc, Barrier};

fn next(
    before: &TeamDeliveryRecord,
    status: &str,
    trigger: &str,
    at: i64,
) -> (TeamDeliveryRecord, NewRuntimeEvent) {
    let mut after = before.clone();
    after.status = status.into();
    after.updated_at = at;
    if status == "failed" {
        after.last_error_code = Some("recipient_unavailable".into());
        after.last_error_message = Some("not dispatched".into());
    } else {
        after.last_error_code = None;
        after.last_error_message = None;
    }
    let event = NewRuntimeEvent {
        event_id: format!("evt_legacy_delivery_{}_{}_{}", before.id, status, at),
        scope: RuntimeEventScope::Team,
        scope_id: before.team_id.clone(),
        session_id: Some(before.recipient_agent_id.clone()),
        team_id: Some(before.team_id.clone()),
        turn_id: None,
        kind: format!("team_delivery.{status}"),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "delivery":after,
            "before_status":before.status,
            "before_updated_at":before.updated_at,
            "trigger":trigger,
        }),
        provider: None,
        provider_seq: None,
        created_at: at,
    };
    (after, event)
}

#[test]
fn deferred_retry_and_native_claim_require_exact_generation_critical_lineage() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_transition_deferred", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let first = &admission.1[0];
    let (deferred, event) = next(first, "deferred", "queue", 1200);
    assert_eq!(
        repo.commit_legacy_team_delivery_pre_native_transition(first, &deferred, &event)
            .unwrap(),
        deferred
    );
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_deliveries,
        vec![deferred.clone()]
    );
    assert_eq!(
        reopened
            .commit_legacy_team_delivery_pre_native_transition(first, &deferred, &event)
            .unwrap(),
        deferred
    );
    let (pending, retry_event) = next(&deferred, "pending", "retry", 1400);
    reopened
        .commit_legacy_team_delivery_pre_native_transition(&deferred, &pending, &retry_event)
        .unwrap();
    let (injecting, claim) = claim_plan(&pending, 1600, "claim");
    assert!(reopened
        .claim_legacy_team_delivery_injection(&pending, &injecting, &claim)
        .unwrap());
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_deliveries,
        vec![injecting]
    );
    let events = reopened.list_runtime_events(None, None, 20).unwrap();
    assert_eq!(
        events
            .iter()
            .map(|row| row.kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "team_message.created",
            "team_delivery.pending",
            "team_delivery.deferred",
            "team_delivery.pending",
            "team_delivery.injecting",
        ]
    );
    assert_eq!(events[2].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(events[3].payload, retry_event.payload);
}

#[test]
fn terminal_completion_event_failure_rolls_back_failed_status_and_prior_event() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_transition_failure", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (failed, event) = next(&admission.1[0], "failed", "queue", 1300);
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fault_terminal BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team_message.completed'
         BEGIN SELECT RAISE(ABORT,'terminal critical journal failed'); END;",
    )
    .unwrap();
    assert!(repo
        .commit_legacy_team_delivery_pre_native_transition(&admission.1[0], &failed, &event)
        .is_err());
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        2
    );
    conn.execute_batch("DROP TRIGGER fault_terminal").unwrap();
    reopened
        .commit_legacy_team_delivery_pre_native_transition(&admission.1[0], &failed, &event)
        .unwrap();
    assert_eq!(
        reopened
            .commit_legacy_team_delivery_pre_native_transition(&admission.1[0], &failed, &event)
            .unwrap(),
        failed
    );
    let events = reopened.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(events[2].kind, "team_delivery.failed");
    assert_eq!(events[3].kind, "team_message.completed");
    assert_eq!(
        events[3].payload,
        serde_json::json!({"message_id":admission.0.id,"deliveries":[failed.clone()]})
    );
    let (reopened_pending, retry_event) = next(&failed, "pending", "retry", 1500);
    assert!(matches!(
        reopened.commit_legacy_team_delivery_pre_native_transition(
            &failed,
            &reopened_pending,
            &retry_event
        ),
        Err(RuntimeError::Conflict(_))
    )); // terminal message must not silently reopen or duplicate native work
}

#[test]
fn failed_recipient_in_unfinished_broadcast_can_recover_only_with_explicit_retry() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "msg_transition_broadcast",
        &["session_a", "session_b"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let (failed, event) = next(&admission.1[0], "failed", "queue", 1300);
    repo.commit_legacy_team_delivery_pre_native_transition(&admission.1[0], &failed, &event)
        .unwrap();
    assert!(repo
        .list_runtime_events(None, None, 10)
        .unwrap()
        .iter()
        .all(|row| row.kind != "team_message.completed"));
    let (pending, retry) = next(&failed, "pending", "retry", 1400);
    repo.commit_legacy_team_delivery_pre_native_transition(&failed, &pending, &retry)
        .unwrap();
    let (injecting, claim) = claim_plan(&pending, 1600, "recovered");
    assert!(repo
        .claim_legacy_team_delivery_injection(&pending, &injecting, &claim)
        .unwrap());
    let (bad_pending, bad_event) = next(&failed, "pending", "queue", 1410);
    assert!(matches!(
        repo.commit_legacy_team_delivery_pre_native_transition(&failed, &bad_pending, &bad_event),
        Err(RuntimeError::ProtocolViolation(_))
    ));
}

#[test]
fn forged_direct_upsert_and_unjournaled_deferred_generation_never_grant_native_claim() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_transition_forgery", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (deferred, event) = next(&admission.1[0], "deferred", "queue", 1300);
    assert!(matches!(
        repo.upsert_team_delivery(&deferred),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(matches!(
        repo.upsert_team_delivery(&admission.1[0]),
        Err(RuntimeError::Conflict(_))
    ));
    // An intentionally corrupted historical row from another writer has
    // neither the new Critical receipt nor an authority to dispatch/retry.
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "UPDATE team_deliveries SET status='deferred',updated_at=?1 WHERE id=?2",
            params![deferred.updated_at, deferred.id],
        )
        .unwrap();
    assert!(matches!(
        repo.commit_legacy_team_delivery_pre_native_transition(
            &deferred,
            &next(&deferred, "pending", "retry", 1400).0,
            &next(&deferred, "pending", "retry", 1400).1
        ),
        Err(RuntimeError::Conflict(_))
    ));
    let (injecting, claim) = claim_plan(&deferred, 1500, "stale");
    assert!(matches!(
        repo.claim_legacy_team_delivery_injection(&deferred, &injecting, &claim),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 2);
    // The original row remains unchanged for all other receipts, and the
    // caller-supplied event did not manufacture transition authorization.
    assert_eq!(event.kind, "team_delivery.deferred");
}

#[test]
fn concurrent_owners_only_one_pre_native_generation_can_commit() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "msg_transition_race",
        &["session_a", "session_b"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for (status, at) in [("deferred", 1300), ("failed", 1400)] {
        let repo = repo.clone();
        let previous = admission.1[0].clone();
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let (next_row, event) = next(&previous, status, "queue", at);
            barrier.wait();
            repo.commit_legacy_team_delivery_pre_native_transition(&previous, &next_row, &event)
        }));
    }
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert!(outcomes
        .iter()
        .any(|outcome| matches!(outcome, Err(RuntimeError::Conflict(_)))));
    let events = repo.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.iter().filter(|row| row.kind == "team_delivery.deferred" || row.kind == "team_delivery.failed").count(), 1);
}
