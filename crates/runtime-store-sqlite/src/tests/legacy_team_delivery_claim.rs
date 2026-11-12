use super::legacy_team_messages::{commit, fixture, proposed};
use super::*;
use runtime_core::{RuntimeError, TeamDeliveryRecord};
use std::sync::{Arc, Barrier};

pub(super) fn claim_plan(
    previous: &TeamDeliveryRecord,
    now: i64,
    _attempt: &str,
) -> (TeamDeliveryRecord, NewRuntimeEvent) {
    let mut injecting = previous.clone();
    injecting.status = "injecting".into();
    injecting.updated_at = now;
    let event = NewRuntimeEvent {
        event_id: format!("evt_legacy_delivery_{}_injecting_{}", previous.id, now),
        scope: RuntimeEventScope::Team,
        scope_id: previous.team_id.clone(),
        session_id: Some(previous.recipient_agent_id.clone()),
        team_id: Some(previous.team_id.clone()),
        turn_id: None,
        kind: "team_delivery.injecting".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"delivery":injecting,"trigger":"queue"}),
        provider: None,
        provider_seq: None,
        created_at: now,
    };
    (injecting, event)
}

#[test]
fn native_claim_commits_status_and_critical_event_once_across_restart() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_claim", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let previous = &admission.1[0];
    let (injecting, event) = claim_plan(previous, 1300, "first");
    assert!(matches!(
        repo.upsert_team_delivery(&injecting),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(repo
        .claim_legacy_team_delivery_injection(previous, &injecting, &event)
        .unwrap());
    let mut unsafe_downgrade = injecting.clone();
    unsafe_downgrade.status = "deferred".into();
    unsafe_downgrade.updated_at += 1;
    assert!(matches!(
        repo.upsert_team_delivery(&unsafe_downgrade),
        Err(RuntimeError::Conflict(_))
    ));
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_deliveries,
        vec![injecting]
    );
    let (again, other) = claim_plan(previous, 1400, "second");
    assert!(!reopened
        .claim_legacy_team_delivery_injection(previous, &again, &other)
        .unwrap());
    let events = reopened.list_runtime_events(None, None, 20).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|row| row.kind == "team_delivery.injecting")
            .count(),
        1
    );
    assert_eq!(events.last().unwrap().event_id, event.event_id);
    assert_eq!(events.last().unwrap().payload, event.payload);
    assert_eq!(
        events.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

#[test]
fn critical_event_failure_rolls_back_entire_pre_native_claim() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_claim_fault", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (injecting, event) = claim_plan(&admission.1[0], 1300, "fault");
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fault_claim BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team_delivery.injecting'
         BEGIN SELECT RAISE(ABORT, 'injected claim event fault'); END;",
        )
        .unwrap();
    assert!(repo
        .claim_legacy_team_delivery_injection(&admission.1[0], &injecting, &event)
        .is_err());
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 2);
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER fault_claim")
        .unwrap();
    // Retrying pre-dispatch after an *observed* rollback needs a distinct
    // authorized attempt. This is an isolated DB test, not provider replay.
    assert!(repo
        .claim_legacy_team_delivery_injection(&admission.1[0], &injecting, &event)
        .unwrap());
}

#[test]
fn two_independent_sqlite_owners_cannot_both_dispatch_same_generation() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_claim_race", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut threads = Vec::new();
    for (at, id) in [(1300, "one"), (1400, "two")] {
        let repo = repo.clone();
        let previous = admission.1[0].clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let (injecting, event) = claim_plan(&previous, at, id);
            barrier.wait();
            repo.claim_legacy_team_delivery_injection(&previous, &injecting, &event)
                .unwrap()
        }));
    }
    let accepted: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(accepted.iter().filter(|claimed| **claimed).count(), 1);
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 3);
}

#[test]
fn native_claim_refuses_cancelled_or_retargeted_or_unjournaled_delivery() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_claim_cancel", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (injecting, mut event) = claim_plan(&admission.1[0], 1300, "cancel");
    event.payload["delivery"]["recipient_agent_id"] = serde_json::json!("session_b");
    assert!(matches!(
        repo.claim_legacy_team_delivery_injection(&admission.1[0], &injecting, &event),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    let (injecting, event) = claim_plan(&admission.1[0], 1300, "cancel");
    let mut cancelled = admission.1[0].clone();
    cancelled.status = "cancelled".into();
    cancelled.updated_at = 1200;
    assert!(matches!(
        repo.upsert_team_delivery(&cancelled),
        Err(RuntimeError::Conflict(_))
    ));
    // Simulate an uncertified historical/corrupted status, which must never
    // authorize native injection. Certified cancellation uses its own
    // Critical transaction rather than raw upsert.
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "UPDATE team_deliveries SET status='cancelled',updated_at=?1 WHERE id=?2",
            params![cancelled.updated_at, cancelled.id],
        )
        .unwrap();
    assert!(!repo
        .claim_legacy_team_delivery_injection(&admission.1[0], &injecting, &event)
        .unwrap());

    let legacy = proposed(&team, "msg_unjournaled_claim", &["session_a"], None);
    repo.upsert_team_message(&legacy.0).unwrap();
    repo.upsert_team_delivery(&legacy.1[0]).unwrap();
    let (next, event) = claim_plan(&legacy.1[0], 1500, "old");
    assert!(matches!(
        repo.claim_legacy_team_delivery_injection(&legacy.1[0], &next, &event),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn claim_cannot_run_after_deletion_or_consumed_event_id() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_claim_deletion", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (injecting, event) = claim_plan(&admission.1[0], 1300, "stale");
    let mut deleted_team = team.clone();
    deleted_team.deleted_at = Some(1200);
    deleted_team.updated_at = 1200;
    repo.upsert_team(&deleted_team).unwrap();
    assert!(matches!(
        repo.claim_legacy_team_delivery_injection(&admission.1[0], &injecting, &event),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
}

#[test]
fn exact_injection_receipt_id_collision_cannot_make_unclaimed_row_dispatchable() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_claim_collision", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (injecting, event) = claim_plan(&admission.1[0], 1300, "same_revision");
    // Simulate a forged/unpaired prior event at the deterministic ID. An
    // idempotent event insert is NOT permission to dispatch native work.
    repo.append_runtime_event(&event).unwrap();
    assert!(matches!(
        repo.claim_legacy_team_delivery_injection(&admission.1[0], &injecting, &event),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
}

#[test]
fn closed_session_or_removed_member_cannot_claim_native_effect() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_claim_closed", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (injecting, event) = claim_plan(&admission.1[0], 1300, "closed");
    let mut closed = sample_session();
    closed.id = "session_a".into();
    closed.status = "closed".into();
    closed.closed_at = Some(1200);
    closed.updated_at = 1200;
    closed.active_turn_id = None;
    repo.upsert_session(&closed).unwrap();
    assert!(matches!(
        repo.claim_legacy_team_delivery_injection(&admission.1[0], &injecting, &event),
        Err(RuntimeError::Conflict(_))
    ));

    let (_temp2, repo2, team2) = fixture();
    let another = proposed(&team2, "msg_native_claim_removed", &["session_a"], None);
    commit(&repo2, &another).unwrap();
    repo2.delete_team_member(&team2.id, "session_a").unwrap();
    let (other_injecting, other_event) = claim_plan(&another.1[0], 1300, "left");
    assert!(matches!(
        repo2.claim_legacy_team_delivery_injection(&another.1[0], &other_injecting, &other_event),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
    assert_eq!(
        repo2.hydrate_runtime_state().unwrap().team_deliveries,
        another.1
    );
}
