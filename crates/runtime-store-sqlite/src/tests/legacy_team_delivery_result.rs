use super::legacy_team_delivery_claim::claim_plan;
use super::legacy_team_messages::{commit, fixture, proposed};
use super::*;
use runtime_core::{RuntimeError, TeamDeliveryRecord};
use std::sync::{Arc, Barrier};

pub(super) fn claimed(
    repo: &SqliteRuntimeRepository,
    row: &TeamDeliveryRecord,
    at: i64,
) -> TeamDeliveryRecord {
    let (injecting, event) = claim_plan(row, at, "claim");
    assert!(repo
        .claim_legacy_team_delivery_injection(row, &injecting, &event)
        .unwrap());
    injecting
}

pub(super) fn accepted_turn(
    repo: &SqliteRuntimeRepository,
    row: &TeamDeliveryRecord,
    turn_id: &str,
) {
    repo.upsert_turn(&TurnRecord {
        id: turn_id.into(),
        session_id: row.recipient_agent_id.clone(),
        provider_turn_ref: Some(format!("native_{turn_id}")),
        status: "in_progress".into(),
        input: serde_json::json!([{"type":"text","text":"legacy accepted recipient"}]),
        source: Some("agent_message_delivery_transport".into()),
        started_at: Some(1400),
        completed_at: None,
        usage: None,
        error: None,
    })
    .unwrap();
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "INSERT INTO turn_admissions (
                turn_id,session_id,provider,projection_source,user_input_snapshot_json,
                dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at
             ) VALUES (?1,?2,?3,'agent_message_delivery_transport','[]','{}',?4,'dispatched',1400,1401)",
            params![turn_id,row.recipient_agent_id,row.provider,
               serde_json::json!({"correlation_id":row.message_id}).to_string()],
        )
        .unwrap();
}

pub(super) fn plan(
    before: &TeamDeliveryRecord,
    turn_id: &str,
    at: i64,
) -> (TeamDeliveryRecord, NewRuntimeEvent) {
    let mut injected = before.clone();
    injected.status = "injected".into();
    injected.injection_strategy = Some("runtime_send_turn".into());
    injected.injected_turn_id = Some(turn_id.into());
    injected.updated_at = at;
    injected.last_error_code = None;
    injected.last_error_message = None;
    let event = NewRuntimeEvent {
        event_id: format!(
            "evt_legacy_delivery_{}_injected_{}",
            before.id, before.updated_at
        ),
        scope: RuntimeEventScope::Team,
        scope_id: before.team_id.clone(),
        session_id: Some(before.recipient_agent_id.clone()),
        team_id: Some(before.team_id.clone()),
        turn_id: Some(turn_id.into()),
        kind: "team_delivery.injected".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"delivery":injected,"before_updated_at":before.updated_at,"trigger":"queue"}),
        provider: None,
        provider_seq: None,
        created_at: at,
    };
    (injected, event)
}

#[test]
fn acknowledged_native_result_and_completion_commit_once_and_replay_after_restart() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_result", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let injecting = claimed(&repo, &admission.1[0], 1300);
    accepted_turn(&repo, &injecting, "turn_native_result");
    let (injected, event) = plan(&injecting, "turn_native_result", 1600);
    assert!(matches!(
        repo.upsert_team_delivery(&injected),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event)
            .unwrap(),
        injected
    );
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_deliveries,
        vec![injected.clone()]
    );
    assert_eq!(
        reopened
            .commit_legacy_team_delivery_injected(&injecting, &injected, &event)
            .unwrap(),
        injected
    );
    let rows = reopened.list_runtime_events(None, None, 30).unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[3].kind, "team_delivery.injected");
    assert_eq!(rows[3].payload, event.payload);
    assert_eq!(rows[3].turn_id.as_deref(), Some("turn_native_result"));
    assert_eq!(rows[4].kind, "team_message.completed");
    assert_eq!(rows[4].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(
        rows[4].payload,
        serde_json::json!({"message_id":admission.0.id,"deliveries":[injected.clone()]})
    );
    let mut direct_rewrite = injected.clone();
    direct_rewrite.status = "deferred".into();
    direct_rewrite.updated_at += 1;
    assert!(matches!(
        reopened.upsert_team_delivery(&direct_rewrite),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn last_completion_event_fault_rolls_back_native_result_and_earlier_critical_insert() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_result_fault", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let injecting = claimed(&repo, &admission.1[0], 1300);
    accepted_turn(&repo, &injecting, "turn_native_result_fault");
    let (injected, event) = plan(&injecting, "turn_native_result_fault", 1600);
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fault_complete BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team_message.completed'
         BEGIN SELECT RAISE(ABORT,'late completion event fault'); END;",
        )
        .unwrap();
    assert!(repo
        .commit_legacy_team_delivery_injected(&injecting, &injected, &event)
        .is_err());
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_deliveries,
        vec![injecting.clone()]
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 30).unwrap().len(),
        3
    );
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER fault_complete")
        .unwrap();
    // Reconcile an EXISTING known native acknowledgement and exact event
    // inputs. This must not call the native provider again.
    assert_eq!(
        reopened
            .commit_legacy_team_delivery_injected(&injecting, &injected, &event)
            .unwrap(),
        injected
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 30).unwrap().len(),
        5
    );
}

#[test]
fn native_turn_source_correlation_and_critical_claim_are_required() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_result_guard", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let injecting = claimed(&repo, &admission.1[0], 1300);
    let (injected, event) = plan(&injecting, "turn_missing", 1600);
    assert!(matches!(
        repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event),
        Err(RuntimeError::Conflict(_))
    ));
    accepted_turn(&repo, &injecting, "turn_missing");
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "UPDATE turn_admissions SET correlation_json='{}' WHERE turn_id='turn_missing'",
            [],
        )
        .unwrap();
    assert!(matches!(
        repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event),
        Err(RuntimeError::Conflict(_))
    ));
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "UPDATE turn_admissions SET correlation_json=?1 WHERE turn_id='turn_missing'",
            [serde_json::json!({"correlation_id":injecting.message_id}).to_string()],
        )
        .unwrap();
    assert_eq!(
        repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event)
            .unwrap(),
        injected
    );
}

#[test]
fn two_native_result_writers_only_one_critical_accepted_turn_commits() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_result_race", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let injecting = claimed(&repo, &admission.1[0], 1300);
    accepted_turn(&repo, &injecting, "turn_race_one");
    accepted_turn(&repo, &injecting, "turn_race_two");
    let barrier = Arc::new(Barrier::new(2));
    let mut threads = Vec::new();
    for (id, time) in [("turn_race_one", 1600), ("turn_race_two", 1700)] {
        let repo = repo.clone();
        let before = injecting.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let (after, event) = plan(&before, id, time);
            barrier.wait();
            repo.commit_legacy_team_delivery_injected(&before, &after, &event)
        }));
    }
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|row| row.is_ok()).count(), 1);
    assert_eq!(repo.list_runtime_events(None, None, 30).unwrap().len(), 5);
}

#[test]
fn broadcast_completion_is_critical_only_after_final_recipient_ack() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "msg_native_result_multi",
        &["session_a", "session_b"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let one = claimed(&repo, &admission.1[0], 1300);
    let two = claimed(&repo, &admission.1[1], 1310);
    accepted_turn(&repo, &one, "turn_multi_a");
    accepted_turn(&repo, &two, "turn_multi_b");
    let (first, first_event) = plan(&one, "turn_multi_a", 1600);
    let (second, second_event) = plan(&two, "turn_multi_b", 1610);
    repo.commit_legacy_team_delivery_injected(&one, &first, &first_event)
        .unwrap();
    assert!(!repo
        .list_runtime_events(None, None, 30)
        .unwrap()
        .iter()
        .any(|row| row.kind == "team_message.completed"));
    repo.commit_legacy_team_delivery_injected(&two, &second, &second_event)
        .unwrap();
    let rows = repo.list_runtime_events(None, None, 30).unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| row.kind == "team_delivery.injected")
            .count(),
        2
    );
    let completed = rows
        .iter()
        .filter(|row| row.kind == "team_message.completed")
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed[0].payload,
        serde_json::json!({"message_id":admission.0.id,"deliveries":[first,second]})
    );
    assert_eq!(completed[0].criticality, RuntimeEventCriticality::Critical);
}

#[test]
fn direct_upsert_cannot_forge_an_ack_or_rewrite_an_injected_receipt() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_result_upsert", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let mut forged = admission.1[0].clone();
    forged.status = "injected".into();
    forged.injection_strategy = Some("runtime_send_turn".into());
    forged.injected_turn_id = Some("turn_not_dispatched".into());
    forged.updated_at = 1700;
    assert!(matches!(
        repo.upsert_team_delivery(&forged),
        Err(RuntimeError::Conflict(_))
    ));

    let injecting = claimed(&repo, &admission.1[0], 1300);
    accepted_turn(&repo, &injecting, "turn_upsert_guard");
    let (injected, event) = plan(&injecting, "turn_upsert_guard", 1600);
    repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event)
        .unwrap();
    for changed in [
        {
            let mut row = injected.clone();
            row.injected_turn_id = Some("turn_forged_rewrite".into());
            row
        },
        {
            let mut row = injected.clone();
            row.updated_at += 1;
            row
        },
        injected.clone(),
    ] {
        assert!(matches!(
            repo.upsert_team_delivery(&changed),
            Err(RuntimeError::Conflict(_))
        ));
    }
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        vec![injected]
    );
}

#[test]
fn replay_requires_committed_native_admission_not_just_a_matching_result_event() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native_result_replay", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let injecting = claimed(&repo, &admission.1[0], 1300);
    accepted_turn(&repo, &injecting, "turn_replay_authority");
    let (injected, event) = plan(&injecting, "turn_replay_authority", 1600);
    repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event)
        .unwrap();
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute(
        "UPDATE turn_admissions SET correlation_json='{}' WHERE turn_id=?1",
        ["turn_replay_authority"],
    )
    .unwrap();
    assert!(matches!(
        repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event),
        Err(RuntimeError::Conflict(_))
    ));
    conn.execute(
        "UPDATE turn_admissions SET correlation_json=?1 WHERE turn_id=?2",
        params![
            serde_json::json!({"correlation_id":injecting.message_id}).to_string(),
            "turn_replay_authority"
        ],
    )
    .unwrap();
    assert_eq!(
        repo.commit_legacy_team_delivery_injected(&injecting, &injected, &event)
            .unwrap(),
        injected
    );
}
