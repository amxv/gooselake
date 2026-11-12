use super::*;
use runtime_core::{RuntimeError, TeamMessageAck};
use std::sync::{Arc, Barrier};

pub(super) fn fixture() -> (tempfile::TempDir, SqliteRuntimeRepository, TeamRecord) {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    repo.initialize_schema().unwrap();
    for id in ["session_lead", "session_a", "session_b"] {
        let mut session = sample_session();
        session.id = id.into();
        session.active_turn_id = None;
        session.provider_session_ref = None;
        repo.upsert_session(&session).unwrap();
    }
    let team = TeamRecord {
        id: "team_legacy_messages".into(),
        name: "Legacy message team".into(),
        lead_agent_id: "session_lead".into(),
        created_by: "operator".into(),
        created_at: 1000,
        updated_at: 1000,
        deleted_at: None,
    };
    repo.upsert_team(&team).unwrap();
    for id in ["session_lead", "session_a", "session_b"] {
        repo.upsert_team_member(&TeamMemberRecord {
            team_id: team.id.clone(),
            agent_id: id.into(),
            title: None,
            joined_at: 1000,
            added_by: "operator".into(),
            creator_agent_id: None,
            creator_compaction_subscription: "auto".into(),
            worktree_id: None,
        })
        .unwrap();
    }
    (temp, repo, team)
}

pub(super) fn proposed(
    team: &TeamRecord,
    id: &str,
    recipients: &[&str],
    key: Option<&str>,
) -> (
    TeamMessageRecord,
    Vec<TeamDeliveryRecord>,
    Vec<NewRuntimeEvent>,
) {
    let message = TeamMessageRecord {
        id: id.into(),
        team_id: team.id.clone(),
        scope: if recipients.len() == 1 {
            "direct"
        } else {
            "broadcast"
        }
        .into(),
        sender_agent_id: "session_lead".into(),
        recipient_agent_ids: serde_json::json!(recipients),
        input: serde_json::json!([{"type":"text","text":"hello"}]),
        image_paths: serde_json::json!([]),
        priority: "normal".into(),
        policy: "non_interrupting".into(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: key.map(str::to_owned),
        created_at: 1100,
    };
    let deliveries = recipients
        .iter()
        .enumerate()
        .map(|(i, recipient)| TeamDeliveryRecord {
            id: format!("dlv_{id}_{i}"),
            message_id: message.id.clone(),
            team_id: team.id.clone(),
            recipient_agent_id: (*recipient).into(),
            provider: "codex".into(),
            status: "pending".into(),
            effective_policy: Some(message.policy.clone()),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: None,
            last_error_message: None,
            created_at: 1100,
            updated_at: 1100,
        })
        .collect::<Vec<_>>();
    let mut events = vec![event(
        team,
        &format!("evt_legacy_message_{}_created", message.id),
        "team_message.created",
        &message.sender_agent_id,
        serde_json::json!({"message":message,"deliveries":deliveries}),
    )];
    for delivery in &deliveries {
        events.push(event(
            team,
            &format!("evt_legacy_delivery_{}_pending", delivery.id),
            "team_delivery.pending",
            &delivery.recipient_agent_id,
            serde_json::json!({"delivery":delivery}),
        ));
    }
    (message, deliveries, events)
}

fn event(
    team: &TeamRecord,
    id: &str,
    kind: &str,
    subject: &str,
    payload: serde_json::Value,
) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: id.into(),
        scope: RuntimeEventScope::Team,
        scope_id: team.id.clone(),
        session_id: Some(subject.into()),
        team_id: Some(team.id.clone()),
        turn_id: None,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: 1100,
    }
}

pub(super) fn commit(
    repo: &SqliteRuntimeRepository,
    admission: &(
        TeamMessageRecord,
        Vec<TeamDeliveryRecord>,
        Vec<NewRuntimeEvent>,
    ),
) -> Result<TeamMessageAck, RuntimeError> {
    repo.commit_legacy_team_message_admission(&admission.0, &admission.1, &admission.2)
}

#[test]
fn late_pending_event_failure_rolls_back_message_all_deliveries_and_prior_events() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_fault", &["session_a", "session_b"], Some("key"));
    let rejected_id = &admission.2[2].event_id;
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(&format!(
        "CREATE TRIGGER reject_last_pending BEFORE INSERT ON runtime_events
          WHEN NEW.event_id='{rejected_id}'
          BEGIN SELECT RAISE(ABORT, 'forced last pending event fault'); END;"
    ))
    .unwrap();
    drop(conn);
    assert!(commit(&repo, &admission).is_err());
    let state = repo.hydrate_runtime_state().unwrap();
    assert!(state.team_messages.is_empty());
    assert!(state.team_deliveries.is_empty());
    assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_last_pending")
        .unwrap();

    let created = commit(&repo, &admission).unwrap();
    assert_eq!(created.disposition, "created");
    assert_eq!(created.deliveries.len(), 2);
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let replay = commit(&reopened, &admission).unwrap();
    assert_eq!(replay.disposition, "existing");
    assert_eq!(replay.message, created.message);
    assert_eq!(replay.deliveries, created.deliveries);
    let events = reopened.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(
        events
            .iter()
            .map(|row| row.kind.as_str())
            .collect::<Vec<_>>(),
        [
            "team_message.created",
            "team_delivery.pending",
            "team_delivery.pending"
        ]
    );
    assert_eq!(
        events.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

#[test]
fn message_event_failure_and_forged_id_never_publish_partial_delivery() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_message_fault", &["session_a"], None);
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER reject_message BEFORE INSERT ON runtime_events
          WHEN NEW.kind='team_message.created'
          BEGIN SELECT RAISE(ABORT, 'forced message event fault'); END;",
    )
    .unwrap();
    drop(conn);
    assert!(commit(&repo, &admission).is_err());
    assert!(repo
        .hydrate_runtime_state()
        .unwrap()
        .team_messages
        .is_empty());
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_message")
        .unwrap();

    let mut forged = admission.2[0].clone();
    forged.kind = "spoofed.event".into();
    forged.payload = serde_json::json!({"forged":true});
    repo.append_runtime_event(&forged).unwrap();
    assert!(matches!(
        commit(&repo, &admission),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(repo
        .hydrate_runtime_state()
        .unwrap()
        .team_messages
        .is_empty());
    assert!(repo
        .hydrate_runtime_state()
        .unwrap()
        .team_deliveries
        .is_empty());
    assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 1);
}

#[test]
fn concurrent_independent_idempotency_owners_converge_without_second_message_or_delivery() {
    let (_temp, repo, team) = fixture();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for id in ["msg_writer_a", "msg_writer_b"] {
        let repo = repo.clone();
        let barrier = Arc::clone(&barrier);
        let admission = proposed(&team, id, &["session_a", "session_b"], Some("same-key"));
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            commit(&repo, &admission)
        }));
    }
    let receipts = handles
        .into_iter()
        .map(|h| h.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.disposition == "created")
            .count(),
        1
    );
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.disposition == "existing")
            .count(),
        1
    );
    assert_eq!(receipts[0].message.id, receipts[1].message.id);
    assert_eq!(receipts[0].deliveries, receipts[1].deliveries);
    assert_eq!(repo.hydrate_runtime_state().unwrap().team_messages.len(), 1);
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries.len(),
        2
    );
    assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 3);
    let mut changed = proposed(
        &team,
        "msg_forged_key",
        &["session_a", "session_b"],
        Some("same-key"),
    );
    changed.0.input = serde_json::json!([{"type":"text","text":"different"}]);
    changed.2[0].payload = serde_json::json!({"message":changed.0,"deliveries":changed.1});
    assert!(matches!(
        commit(&repo, &changed),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn two_owners_with_same_generated_numeric_id_and_exact_key_converge_on_first_receipt() {
    let (_temp, repo, team) = fixture();
    let first = proposed(
        &team,
        "msg_same_counter",
        &["session_a"],
        Some("shared-key"),
    );
    let ack = commit(&repo, &first).unwrap();
    // Different service processes can observe the same numeric max counter,
    // even while their wall clock samples differ. The exact logical key is
    // authority, not the losing service process' timestamp.
    let mut second = proposed(
        &team,
        "msg_same_counter",
        &["session_a"],
        Some("shared-key"),
    );
    second.0.created_at += 10;
    second.1[0].created_at += 10;
    second.1[0].updated_at += 10;
    second.2[0].created_at += 10;
    second.2[1].created_at += 10;
    second.2[0].payload = serde_json::json!({"message":second.0,"deliveries":second.1});
    second.2[1].payload = serde_json::json!({"delivery":second.1[0]});
    let replay = commit(&repo, &second).unwrap();
    assert_eq!(replay.disposition, "existing");
    assert_eq!(replay.message, ack.message);
    assert_eq!(replay.deliveries, ack.deliveries);
    assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 2);
    assert_eq!(repo.hydrate_runtime_state().unwrap().team_messages.len(), 1);

    let id_without_key = proposed(&team, "msg_same_counter", &["session_a"], None);
    assert!(matches!(
        commit(&repo, &id_without_key),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn replay_after_delivery_injected_never_rewinds_native_status_or_republishes_events() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_native", &["session_a"], Some("native-key"));
    let created = commit(&repo, &admission).unwrap();
    let mut delivery = created.deliveries[0].clone();
    delivery.status = "injected".into();
    delivery.injected_turn_id = Some("turn_1".into());
    delivery.updated_at += 100;
    repo.upsert_team_delivery(&delivery).unwrap();
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let replay = commit(&reopened, &admission).unwrap();
    assert_eq!(replay.disposition, "existing");
    assert_eq!(replay.deliveries, vec![delivery]);
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        2
    );
}

#[test]
fn deleted_team_missing_member_and_unjournaled_old_row_fail_closed() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_membership", &["session_b"], None);
    repo.delete_team_member(&team.id, "session_b").unwrap();
    assert!(matches!(
        commit(&repo, &admission),
        Err(RuntimeError::Conflict(_))
    ));
    let mut tombstoned = team.clone();
    tombstoned.deleted_at = Some(1300);
    repo.upsert_team(&tombstoned).unwrap();
    assert!(matches!(
        commit(&repo, &admission),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(repo
        .hydrate_runtime_state()
        .unwrap()
        .team_messages
        .is_empty());

    let mut restored = team.clone();
    restored.updated_at += 1;
    repo.upsert_team(&restored).unwrap();
    let old = proposed(&team, "msg_old", &[], Some("legacy-key"));
    // Historical /v1 upsert never wrote any canonical critical receipt.
    repo.upsert_team_message(&old.0).unwrap();
    assert!(matches!(
        commit(&repo, &old),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repo.hydrate_runtime_state().unwrap().team_messages.len(), 1);
    assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
}

#[test]
fn mismatched_critical_event_or_delivery_snapshot_is_rejected_before_write() {
    let (_temp, repo, team) = fixture();
    let original = proposed(&team, "msg_malformed", &["session_a", "session_b"], None);
    let mut admission = original.clone();
    admission.2[1].criticality = RuntimeEventCriticality::Droppable;
    assert!(matches!(
        commit(&repo, &admission),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    let mut admission = original.clone();
    admission.1[1].recipient_agent_id = "session_a".into();
    assert!(matches!(
        commit(&repo, &admission),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    let mut admission = original.clone();
    admission.2.pop();
    assert!(matches!(
        commit(&repo, &admission),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert!(repo
        .hydrate_runtime_state()
        .unwrap()
        .team_messages
        .is_empty());
    assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
}

#[test]
fn historical_upsert_must_not_rewrite_certified_message_or_delivery_identity() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "msg_sealed", &["session_a"], Some("sealed-key"));
    commit(&repo, &admission).unwrap();
    let mut message = admission.0.clone();
    message.input = serde_json::json!([{"type":"text","text":"forged"}]);
    assert!(matches!(
        repo.upsert_team_message(&message),
        Err(RuntimeError::Conflict(_))
    ));
    let mut alias = admission.0.clone();
    alias.id = "msg_alias".into();
    assert!(matches!(
        repo.upsert_team_message(&alias),
        Err(RuntimeError::Conflict(_))
    ));

    let mut retarget = admission.1[0].clone();
    retarget.recipient_agent_id = "session_b".into();
    retarget.updated_at += 1;
    assert!(matches!(
        repo.upsert_team_delivery(&retarget),
        Err(RuntimeError::Conflict(_))
    ));
    let mut change_provider = admission.1[0].clone();
    change_provider.provider = "claude".into();
    change_provider.updated_at += 2;
    assert!(matches!(
        repo.upsert_team_delivery(&change_provider),
        Err(RuntimeError::Conflict(_))
    ));
    let mut change_policy = admission.1[0].clone();
    change_policy.effective_policy = Some("immediate_interrupt".into());
    change_policy.updated_at += 3;
    assert!(matches!(
        repo.upsert_team_delivery(&change_policy),
        Err(RuntimeError::Conflict(_))
    ));

    let mut injected = admission.1[0].clone();
    injected.status = "injected".into();
    injected.injected_turn_id = Some("native_turn_1".into());
    injected.updated_at += 10;
    repo.upsert_team_delivery(&injected).unwrap();
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let state = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(state.team_messages, vec![admission.0.clone()]);
    assert_eq!(state.team_deliveries, vec![injected.clone()]);
    assert_eq!(
        commit(&reopened, &admission).unwrap().disposition,
        "existing"
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        2
    );

    // The old direct-upsert/import path still operates for historical rows
    // without a critical receipt; this is not a silent data migration.
    let old = proposed(&team, "msg_historical", &["session_b"], None);
    repo.upsert_team_message(&old.0).unwrap();
    let mut historical = old.0.clone();
    historical.input = serde_json::json!([{"type":"text","text":"old rewrite"}]);
    repo.upsert_team_message(&historical).unwrap();
    repo.upsert_team_delivery(&old.1[0]).unwrap();
    let mut historical_delivery = old.1[0].clone();
    historical_delivery.provider = "claude".into();
    historical_delivery.updated_at += 1;
    repo.upsert_team_delivery(&historical_delivery).unwrap();
    assert_eq!(repo.hydrate_runtime_state().unwrap().team_messages.len(), 2);
    assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 2);
}
