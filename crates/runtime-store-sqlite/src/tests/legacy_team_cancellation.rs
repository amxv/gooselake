use super::legacy_team_messages::{commit, fixture, proposed};
use super::*;
use runtime_core::{RuntimeError, TeamDeliveryRecord, TeamMessageRecord};
use std::sync::{Arc, Barrier};

fn cancel_plan(
    message: &TeamMessageRecord,
    before: &[TeamDeliveryRecord],
    now: i64,
) -> (Vec<TeamDeliveryRecord>, Vec<NewRuntimeEvent>) {
    let after = before
        .iter()
        .map(|row| {
            let mut new = row.clone();
            new.status = "cancelled".into();
            new.updated_at = now;
            new
        })
        .collect::<Vec<_>>();
    let mut events = Vec::new();
    for (old, new) in before.iter().zip(&after) {
        events.push(NewRuntimeEvent {
            event_id: format!("evt_legacy_delivery_{}_cancelled", new.id),
            scope: RuntimeEventScope::Team,
            scope_id: message.team_id.clone(),
            session_id: Some(new.recipient_agent_id.clone()),
            team_id: Some(message.team_id.clone()),
            turn_id: None,
            kind: "team_delivery.cancelled".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({
                "delivery":new,
                "before_updated_at":old.updated_at,
                "before_status":old.status,
            }),
            provider: None,
            provider_seq: None,
            created_at: now,
        });
    }
    events.push(NewRuntimeEvent {
        event_id: format!("evt_legacy_message_{}_completed_cancel", message.id),
        scope: RuntimeEventScope::Team,
        scope_id: message.team_id.clone(),
        session_id: None,
        team_id: Some(message.team_id.clone()),
        turn_id: None,
        kind: "team_message.completed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"message_id":message.id}),
        provider: None,
        provider_seq: None,
        created_at: now,
    });
    (after, events)
}

fn cancel(
    repo: &SqliteRuntimeRepository,
    message: &TeamMessageRecord,
    before: &[TeamDeliveryRecord],
    after: &[TeamDeliveryRecord],
    events: &[NewRuntimeEvent],
) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
    repo.commit_legacy_team_message_cancellation(message, before, after, events)
}

#[test]
fn late_completion_event_failure_rolls_back_all_cancelled_delivery_rows_and_events() {
    let (_temp, repo, team) = fixture();
    let admitted = proposed(
        &team,
        "msg_cancel_last_event_fault",
        &["session_a", "session_b"],
        Some("cancel-a"),
    );
    commit(&repo, &admitted).unwrap();
    assert!(repo
        .get_legacy_team_message_cancellation_receipt(&admitted.0)
        .unwrap()
        .is_none());
    let (after, events) = cancel_plan(&admitted.0, &admitted.1, 1400);
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_cancel_completion BEFORE INSERT ON runtime_events
             WHEN NEW.kind='team_message.completed'
             BEGIN SELECT RAISE(ABORT, 'forced late completion event fault'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(cancel(&repo, &admitted.0, &admitted.1, &after, &events).is_err());
    let state = repo.hydrate_runtime_state().unwrap();
    assert_eq!(state.team_deliveries, admitted.1);
    assert_eq!(state.team_messages, vec![admitted.0.clone()]);
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 3);
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_cancel_completion")
        .unwrap();

    assert_eq!(
        cancel(&repo, &admitted.0, &admitted.1, &after, &events).unwrap(),
        after
    );
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened
            .get_legacy_team_message_cancellation_receipt(&admitted.0)
            .unwrap(),
        Some(after.clone())
    );
    // A lost response can be retried with a different wall clock sample. The
    // stored receipt is reused, not a second cancellation or native action.
    let (different_after, different_events) = cancel_plan(&admitted.0, &admitted.1, 1600);
    assert_eq!(
        cancel(
            &reopened,
            &admitted.0,
            &admitted.1,
            &different_after,
            &different_events
        )
        .unwrap(),
        after
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_deliveries,
        after
    );
    let events = reopened.list_runtime_events(None, None, 30).unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        [
            "team_message.created",
            "team_delivery.pending",
            "team_delivery.pending",
            "team_delivery.cancelled",
            "team_delivery.cancelled",
            "team_message.completed"
        ]
    );
    assert_eq!(
        events.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6]
    );
}

#[test]
fn competing_native_injection_generation_cannot_be_cancelled_as_stale_pending() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_cancel_native_race", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (after, events) = cancel_plan(&admission.0, &admission.1, 1300);
    let mut injecting = admission.1[0].clone();
    injecting.status = "injecting".into();
    injecting.updated_at += 1;
    repo.upsert_team_delivery(&injecting).unwrap();
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &events),
        Err(RuntimeError::Conflict(_))
    ));
    let state = repo.hydrate_runtime_state().unwrap();
    assert_eq!(state.team_deliveries, vec![injecting]);
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 2);
}

#[test]
fn two_independent_cancellation_owners_converge_to_one_durable_receipt() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "msg_two_cancel_owners",
        &["session_a", "session_b"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for now in [1400, 1500] {
        let repo = repo.clone();
        let barrier = Arc::clone(&barrier);
        let message = admission.0.clone();
        let before = admission.1.clone();
        handles.push(std::thread::spawn(move || {
            let (after, events) = cancel_plan(&message, &before, now);
            barrier.wait();
            cancel(&repo, &message, &before, &after, &events)
        }));
    }
    let results: Vec<_> = handles
        .into_iter()
        .map(|thread| thread.join().unwrap().unwrap())
        .collect();
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0].len(), 2);
    assert!(results[0]
        .iter()
        .all(|delivery| delivery.status == "cancelled"));
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 6);
}

#[test]
fn cancellation_event_collision_and_forgery_reject_every_change() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "msg_cancel_bad_event",
        &["session_a", "session_b"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let (after, events) = cancel_plan(&admission.0, &admission.1, 1400);
    let mut bad_events = events.clone();
    bad_events[0].criticality = RuntimeEventCriticality::Droppable;
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &bad_events),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    bad_events = events.clone();
    bad_events[1].payload["delivery"]["recipient_agent_id"] = serde_json::json!("session_lead");
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &bad_events),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    bad_events.pop();
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &bad_events),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    let mut forged = events[0].clone();
    forged.kind = "foreign.injected".into();
    forged.payload = serde_json::json!({"foreign":true});
    repo.append_runtime_event(&forged).unwrap();
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &events),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 4);
}

#[test]
fn legacy_pre_journal_message_cancels_without_forging_initial_admission() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_historical_cancellation", &["session_a"], None);
    repo.upsert_team_message(&admission.0).unwrap();
    repo.upsert_team_delivery(&admission.1[0]).unwrap();
    let (after, events) = cancel_plan(&admission.0, &admission.1, 1700);
    cancel(&repo, &admission.0, &admission.1, &after, &events).unwrap();
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        cancel(&reopened, &admission.0, &admission.1, &after, &events).unwrap(),
        after
    );
    assert_eq!(
        reopened
            .list_runtime_events(None, None, 20)
            .unwrap()
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        ["team_delivery.cancelled", "team_message.completed"]
    );
}

#[test]
fn empty_broadcast_completion_is_one_critical_receipt_and_restart_replay() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_empty_broadcast", &[], Some("empty"));
    commit(&repo, &admission).unwrap();
    let (after, events) = cancel_plan(&admission.0, &admission.1, 1700);
    assert_eq!(
        cancel(&repo, &admission.0, &admission.1, &after, &events).unwrap(),
        Vec::<TeamDeliveryRecord>::new()
    );
    assert_eq!(
        cancel(&repo, &admission.0, &admission.1, &after, &events).unwrap(),
        Vec::<TeamDeliveryRecord>::new()
    );
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 2);
}

#[test]
fn team_deletion_or_missing_delivery_cannot_admit_or_replay_new_cancel() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_deleted_team_cancel", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let (after, events) = cancel_plan(&admission.0, &admission.1, 1400);
    let mut tombstoned = team.clone();
    tombstoned.deleted_at = Some(1400);
    repo.upsert_team(&tombstoned).unwrap();
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &events),
        Err(RuntimeError::Conflict(_))
    ));
    let mut restored = team.clone();
    restored.updated_at += 1;
    repo.upsert_team(&restored).unwrap();
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "DELETE FROM team_deliveries WHERE id=?1",
            [&admission.1[0].id],
        )
        .unwrap();
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &events),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 2);
}

#[test]
fn forged_cancellation_delivery_receipt_after_commit_is_rejected_on_read_only_replay() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "msg_forged_cancel_read",
        &["session_a"],
        Some("forged-read"),
    );
    commit(&repo, &admission).unwrap();
    let (after, events) = cancel_plan(&admission.0, &admission.1, 1500);
    cancel(&repo, &admission.0, &admission.1, &after, &events).unwrap();
    assert_eq!(
        repo.get_legacy_team_message_cancellation_receipt(&admission.0)
            .unwrap(),
        Some(after)
    );
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute(
        "UPDATE runtime_events SET payload_json=?1 WHERE event_id=?2",
        params![
            "{\"before_updated_at\":1000,\"before_status\":\"injecting\",\"delivery\":{}}",
            format!("evt_legacy_delivery_{}_cancelled", admission.1[0].id),
        ],
    )
    .unwrap();
    assert!(matches!(
        repo.get_legacy_team_message_cancellation_receipt(&admission.0),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn missing_cancelled_delivery_cannot_be_misread_as_an_empty_successful_receipt() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "msg_missing_cancelled_delivery",
        &["session_a"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let (after, events) = cancel_plan(&admission.0, &admission.1, 1600);
    cancel(&repo, &admission.0, &admission.1, &after, &events).unwrap();
    open_connection(&repo.database_path)
        .unwrap()
        .execute("DELETE FROM team_deliveries WHERE id=?1", [&after[0].id])
        .unwrap();
    assert!(matches!(
        repo.get_legacy_team_message_cancellation_receipt(&admission.0),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(matches!(
        cancel(&repo, &admission.0, &admission.1, &after, &events),
        Err(RuntimeError::Conflict(_))
    ));
}
