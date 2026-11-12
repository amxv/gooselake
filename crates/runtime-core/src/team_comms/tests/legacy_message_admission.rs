use super::*;
use crate::{TeamListMessagesRequest, TeamSendDirectRequest};

fn request(team_id: &str, sender: &str, recipient: &str) -> TeamSendDirectRequest {
    TeamSendDirectRequest {
        team_id: team_id.into(),
        sender_agent_id: sender.into(),
        recipient_agent_id: recipient.into(),
        input: serde_json::json!([{"type":"text","text":"admission before native"}]),
        image_paths: Vec::new(),
        priority: "normal".into(),
        policy: "non_interrupting".into(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: Some("immutable-intent".into()),
    }
}

fn admission_events(store: &TestStore) -> Vec<RuntimeEventRecord> {
    store
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(
                event.kind.as_str(),
                "team_message.created" | "team_delivery.pending"
            )
        })
        .cloned()
        .collect()
}

#[tokio::test]
async fn legacy_message_critical_pending_fault_rejects_before_cache_or_native_delivery() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let team = service
        .create_team(TeamCreateRequest {
            name: "Atomic message admission".into(),
            lead_agent_id: sender.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: None,
        })
        .await
        .unwrap();
    let req = request(&team.team.id, &sender, &recipient);
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team_delivery.pending".into());
    assert!(matches!(
        service.send_direct(req.clone()).await,
        Err(RuntimeError::Io(_))
    ));
    let snapshot = store.hydrate_runtime_state().unwrap();
    assert!(snapshot.team_messages.is_empty());
    assert!(snapshot.team_deliveries.is_empty());
    assert!(admission_events(&store).is_empty());
    assert!(service
        .list_messages(TeamListMessagesRequest {
            team_id: team.team.id.clone(),
            cursor: None,
            limit: None,
        })
        .await
        .unwrap()
        .messages
        .is_empty());

    let first = service.send_direct(req.clone()).await.unwrap();
    assert_eq!(first.disposition, "created");
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_messages.len(),
        1
    );
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries.len(),
        1
    );
    let events = admission_events(&store);
    assert_eq!(
        events
            .iter()
            .map(|row| row.kind.as_str())
            .collect::<Vec<_>>(),
        ["team_message.created", "team_delivery.pending"]
    );

    let replay = service.send_direct(req).await.unwrap();
    assert_eq!(replay.disposition, "existing");
    assert_eq!(replay.message, first.message);
    assert_eq!(admission_events(&store).len(), 2);
}

#[tokio::test]
async fn legacy_message_key_reuse_with_different_input_is_conflict_not_new_native_effect() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let team = service
        .create_team(TeamCreateRequest {
            name: "Immutable message idempotency".into(),
            lead_agent_id: sender.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: None,
        })
        .await
        .unwrap();
    let req = request(&team.team.id, &sender, &recipient);
    service.send_direct(req.clone()).await.unwrap();
    let mut conflicting = req;
    conflicting.input = serde_json::json!([{"type":"text","text":"forged"}]);
    assert!(matches!(
        service.send_direct(conflicting).await,
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_messages.len(),
        1
    );
    assert_eq!(admission_events(&store).len(), 2);
}
