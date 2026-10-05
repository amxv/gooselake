use super::*;

#[tokio::test]
async fn agent_first_direct_routes_cross_workspace_without_legacy_team_ownership() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    add_workspace_agent(
        store.as_ref(),
        &runtime,
        &sender,
        "workspace_alpha",
        "Alpha",
        ProviderKind::Codex,
        WorkspaceAgentLifecycleState::Active,
    )
    .await;
    add_workspace_agent(
        store.as_ref(),
        &runtime,
        &recipient,
        "workspace_beta",
        "Beta",
        ProviderKind::Codex,
        WorkspaceAgentLifecycleState::Active,
    )
    .await;

    let ack = service
        .send_agent_direct(AgentDirectMessageRequest {
            sender_agent_id: sender.clone(),
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "cross workspace" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: Some("corr-cross-workspace".to_string()),
            reply_to_message_id: None,
            idempotency_key: Some("cross-workspace-key".to_string()),
        })
        .await
        .expect("cross-workspace direct");

    assert_eq!(
        ack.message.context_kind,
        AgentMessageContextKind::GlobalDirect
    );
    assert_eq!(ack.message.workspace_id, None);
    assert_eq!(ack.message.legacy_team_id, None);
    assert_eq!(ack.message.recipient_agent_ids, vec![recipient.clone()]);
    assert_eq!(ack.deliveries.len(), 1);
    assert_eq!(ack.deliveries[0].status, DELIVERY_STATUS_INJECTED);

    let turns = runtime
        .list_session_turns(&recipient)
        .await
        .expect("recipient turns");
    let delivered = turns.last().expect("delivered turn");
    assert_eq!(
        delivered.source.as_deref(),
        Some("agent_message_delivery_transport")
    );
    let items = delivered.input.as_array().expect("turn input array");
    assert!(items.iter().any(|item| {
        item.get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| {
                text.contains("<agent_msg kind=\"direct\"")
                    && text.contains("sender=\"Alpha\"")
                    && text.contains("context=\"global\"")
            })
    }));
}

#[tokio::test]
async fn workspace_broadcast_snapshots_active_roster_and_excludes_sender() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let first = create_test_session(&runtime).await;
    let second = create_test_session(&runtime).await;
    let archived = create_test_session(&runtime).await;
    for (agent_id, alias, lifecycle) in [
        (
            &sender,
            "Leadless Sender",
            WorkspaceAgentLifecycleState::Active,
        ),
        (&first, "One", WorkspaceAgentLifecycleState::Active),
        (&second, "Two", WorkspaceAgentLifecycleState::Active),
        (&archived, "Old", WorkspaceAgentLifecycleState::Archived),
    ] {
        add_workspace_agent(
            store.as_ref(),
            &runtime,
            agent_id,
            "workspace_broadcast",
            alias,
            ProviderKind::Codex,
            lifecycle,
        )
        .await;
    }

    let ack = service
        .broadcast_workspace(AgentBroadcastMessageRequest {
            sender_agent_id: sender.clone(),
            input: serde_json::json!([{ "type": "text", "text": "snapshot roster" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            idempotency_key: Some("broadcast-snapshot".to_string()),
        })
        .await
        .expect("workspace broadcast");

    assert_eq!(
        ack.message.context_kind,
        AgentMessageContextKind::WorkspaceTeam
    );
    assert_eq!(
        ack.message.workspace_id.as_deref(),
        Some("workspace_broadcast")
    );
    assert_eq!(
        ack.message.recipient_agent_ids,
        vec![first.clone(), second.clone()]
    );
    assert_eq!(ack.deliveries.len(), 2);
    assert!(ack
        .deliveries
        .iter()
        .all(|delivery| delivery.recipient_agent_id != sender));
    assert!(ack
        .deliveries
        .iter()
        .all(|delivery| delivery.recipient_agent_id != archived));

    let late = create_test_session(&runtime).await;
    add_workspace_agent(
        store.as_ref(),
        &runtime,
        &late,
        "workspace_broadcast",
        "Late",
        ProviderKind::Codex,
        WorkspaceAgentLifecycleState::Active,
    )
    .await;
    let persisted = service
        .list_agent_messages(crate::AgentMessageListRequest {
            workspace_id: Some("workspace_broadcast".to_string()),
            sender_agent_id: None,
            cursor: None,
            limit: None,
        })
        .await
        .expect("list messages");
    assert_eq!(persisted.messages.len(), 1);
    assert_eq!(
        persisted.messages[0].recipient_agent_ids,
        vec![first, second]
    );
    assert!(!persisted.messages[0].recipient_agent_ids.contains(&late));
}

#[tokio::test]
async fn agent_message_idempotency_replays_identical_request_and_rejects_mismatch() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store, 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let request = AgentDirectMessageRequest {
        sender_agent_id: sender.clone(),
        recipient_agent_id: recipient.clone(),
        input: serde_json::json!([{ "type": "text", "text": "same" }]),
        image_paths: Vec::new(),
        priority: "normal".to_string(),
        policy: "non_interrupting".to_string(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: Some("stable-key".to_string()),
    };
    let first = service
        .send_agent_direct(request.clone())
        .await
        .expect("first send");
    let replay = service
        .send_agent_direct(request)
        .await
        .expect("idempotent replay");
    assert_eq!(replay.message.id, first.message.id);
    assert_eq!(replay.disposition, "existing");

    let mismatch = service
        .send_agent_direct(AgentDirectMessageRequest {
            sender_agent_id: sender,
            recipient_agent_id: recipient,
            input: serde_json::json!([{ "type": "text", "text": "different" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("stable-key".to_string()),
        })
        .await;
    assert!(matches!(mismatch, Err(RuntimeError::Conflict(_))));
}

#[tokio::test]
async fn agent_message_images_are_validated_and_delivered_as_native_items_for_codex_and_claude() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store, 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session_for(&runtime, ProviderKind::Claude).await;
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let image_path = temp_dir.path().join("reference.png");
    std::fs::write(
        &image_path,
        [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0],
    )
    .expect("write image fixture");

    service
        .send_agent_direct(AgentDirectMessageRequest {
            sender_agent_id: sender,
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "inspect image" }]),
            image_paths: vec![image_path.to_string_lossy().to_string()],
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
        })
        .await
        .expect("claude image send");
    let turns = runtime
        .list_session_turns(&recipient)
        .await
        .expect("claude turns");
    assert!(turns
        .last()
        .and_then(|turn| turn.input.as_array())
        .expect("delivered turn input")
        .iter()
        .any(|item| {
            item.get("type").and_then(Value::as_str) == Some("image")
                && item.get("path").and_then(Value::as_str)
                    == Some(image_path.to_string_lossy().as_ref())
        }));

    let codex = create_test_session(&runtime).await;
    service
        .send_agent_direct(AgentDirectMessageRequest {
            sender_agent_id: recipient,
            recipient_agent_id: codex.clone(),
            input: serde_json::json!([{ "type": "text", "text": "image" }]),
            image_paths: vec![image_path.to_string_lossy().to_string()],
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
        })
        .await
        .expect("codex image send");
    let codex_turns = runtime
        .list_session_turns(&codex)
        .await
        .expect("codex turns");
    assert!(codex_turns
        .last()
        .and_then(|turn| turn.input.as_array())
        .expect("codex delivered turn input")
        .iter()
        .any(|item| {
            item.get("type").and_then(Value::as_str) == Some("image")
                && item.get("path").and_then(Value::as_str)
                    == Some(image_path.to_string_lossy().as_ref())
        }));

    let fake_png = temp_dir.path().join("fake.png");
    std::fs::write(&fake_png, b"not actually an image").expect("write fake image");
    let invalid_type = service
        .send_agent_direct(AgentDirectMessageRequest {
            sender_agent_id: create_test_session(&runtime).await,
            recipient_agent_id: create_test_session_for(&runtime, ProviderKind::Claude).await,
            input: serde_json::json!([{ "type": "text", "text": "fake image" }]),
            image_paths: vec![fake_png.to_string_lossy().to_string()],
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
        })
        .await;
    assert!(matches!(invalid_type, Err(RuntimeError::InvalidState(_))));
}

#[test]
fn agent_message_image_validation_accepts_supported_formats_in_order_and_enforces_count() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let png = temp_dir.path().join("one.png");
    let jpeg = temp_dir.path().join("two.jpg");
    let gif = temp_dir.path().join("three.gif");
    let webp = temp_dir.path().join("four.webp");
    std::fs::write(&png, [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]).expect("png");
    std::fs::write(&jpeg, [0xFF, 0xD8, 0xFF, 0x00]).expect("jpeg");
    std::fs::write(&gif, b"GIF89a").expect("gif");
    std::fs::write(&webp, b"RIFF\x00\x00\x00\x00WEBP").expect("webp");

    let ordered = vec![
        png.display().to_string(),
        jpeg.display().to_string(),
        gif.display().to_string(),
        webp.display().to_string(),
    ];
    assert_eq!(
        validate_message_image_paths(ordered.clone()).expect("supported image paths"),
        ordered
    );

    let too_many = validate_message_image_paths(vec![png.display().to_string(); 9]);
    assert!(matches!(too_many, Err(RuntimeError::InvalidState(_))));

    let not_file = validate_message_image_paths(vec![temp_dir.path().display().to_string()]);
    assert!(matches!(not_file, Err(RuntimeError::InvalidState(_))));
}

#[tokio::test]
async fn leadless_workspace_broadcast_with_no_other_members_is_a_durable_empty_snapshot() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    add_workspace_agent(
        store.as_ref(),
        &runtime,
        &sender,
        "workspace_solo",
        "Solo",
        ProviderKind::Codex,
        WorkspaceAgentLifecycleState::Active,
    )
    .await;
    let ack = service
        .broadcast_workspace(AgentBroadcastMessageRequest {
            sender_agent_id: sender,
            input: serde_json::json!([{ "type": "text", "text": "anybody here" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            idempotency_key: Some("solo-broadcast".to_string()),
        })
        .await
        .expect("solo broadcast");
    assert_eq!(
        ack.message.context_kind,
        AgentMessageContextKind::WorkspaceTeam
    );
    assert_eq!(ack.message.workspace_id.as_deref(), Some("workspace_solo"));
    assert!(ack.message.recipient_agent_ids.is_empty());
    assert!(ack.deliveries.is_empty());
    let hydrated = store.hydrate_runtime_state().expect("hydrate");
    assert_eq!(hydrated.agent_messages.len(), 1);
    assert!(hydrated.agent_deliveries.is_empty());
}

#[tokio::test]
async fn canonical_pending_delivery_recovers_after_service_restart() {
    let store = Arc::new(TestStore::default());
    let (runtime, _initial_service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let message = AgentMessageRecord {
        id: "msg_7001".to_string(),
        scope: "direct".to_string(),
        context_kind: AgentMessageContextKind::GlobalDirect,
        workspace_id: None,
        legacy_team_id: None,
        sender_agent_id: sender,
        recipient_agent_ids: vec![recipient.clone()],
        input: serde_json::json!([{ "type": "text", "text": "recover me" }]),
        image_paths: Vec::new(),
        priority: "normal".to_string(),
        policy: "non_interrupting".to_string(),
        correlation_id: Some("restart-correlation".to_string()),
        reply_to_message_id: None,
        idempotency_key: Some("restart-key".to_string()),
        created_at: 7_001,
    };
    let delivery = AgentDeliveryRecord {
        id: "dlv_7001".to_string(),
        message_id: message.id.clone(),
        recipient_agent_id: recipient.clone(),
        provider: "codex".to_string(),
        status: DELIVERY_STATUS_PENDING.to_string(),
        effective_policy: Some("non_interrupting".to_string()),
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 7_001,
        updated_at: 7_001,
    };
    store
        .insert_agent_message_with_deliveries(&message, &[delivery])
        .expect("seed canonical delivery");
    let service = RuntimeTeamCommsService::new(
        store.clone(),
        runtime,
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("restart comms service");
    let recovered = service
        .recover_startup_deferred_deliveries()
        .await
        .expect("recover pending delivery");
    assert!(recovered >= 1);
    let rows = service
        .get_agent_deliveries(AgentDeliveryListRequest {
            message_id: Some(message.id),
            recipient_agent_id: Some(recipient),
        })
        .await
        .expect("recovered rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, DELIVERY_STATUS_INJECTED);
    assert!(rows[0].injected_turn_id.is_some());
}

#[tokio::test]
async fn migrated_legacy_delivery_recovery_uses_only_canonical_authority() {
    let store = Arc::new(TestStore::default());
    let (runtime, _initial_service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let message_id = "msg_migrated_7002".to_string();
    let delivery_id = "dlv_migrated_7002".to_string();

    store
        .upsert_team(&TeamRecord {
            id: "team_migrated_7002".to_string(),
            name: "Migrated Team".to_string(),
            lead_agent_id: sender.clone(),
            created_by: "test".to_string(),
            created_at: 7_002,
            updated_at: 7_002,
            deleted_at: None,
        })
        .expect("seed legacy team");
    store
        .upsert_team_message(&TeamMessageRecord {
            id: message_id.clone(),
            team_id: "team_migrated_7002".to_string(),
            scope: "direct".to_string(),
            sender_agent_id: sender.clone(),
            recipient_agent_ids: serde_json::json!([recipient.clone()]),
            input: serde_json::json!([{ "type": "text", "text": "recover once" }]),
            image_paths: serde_json::json!([]),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: Some("migrated-restart-correlation".to_string()),
            reply_to_message_id: None,
            idempotency_key: Some("migrated-restart-key".to_string()),
            created_at: 7_002,
        })
        .expect("seed legacy message");
    store
        .upsert_team_delivery(&TeamDeliveryRecord {
            id: delivery_id.clone(),
            message_id: message_id.clone(),
            team_id: "team_migrated_7002".to_string(),
            recipient_agent_id: recipient.clone(),
            provider: "codex".to_string(),
            status: DELIVERY_STATUS_PENDING.to_string(),
            effective_policy: Some("non_interrupting".to_string()),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: None,
            last_error_message: None,
            created_at: 7_002,
            updated_at: 7_002,
        })
        .expect("seed legacy delivery");

    let canonical_message = AgentMessageRecord {
        id: message_id.clone(),
        scope: "direct".to_string(),
        context_kind: AgentMessageContextKind::LegacyTeam,
        workspace_id: None,
        legacy_team_id: Some("team_migrated_7002".to_string()),
        sender_agent_id: sender,
        recipient_agent_ids: vec![recipient.clone()],
        input: serde_json::json!([{ "type": "text", "text": "recover once" }]),
        image_paths: Vec::new(),
        priority: "normal".to_string(),
        policy: "non_interrupting".to_string(),
        correlation_id: Some("migrated-restart-correlation".to_string()),
        reply_to_message_id: None,
        idempotency_key: Some("migrated-restart-key".to_string()),
        created_at: 7_002,
    };
    store
        .insert_agent_message_with_deliveries(
            &canonical_message,
            &[AgentDeliveryRecord {
                id: delivery_id.clone(),
                message_id: message_id.clone(),
                recipient_agent_id: recipient.clone(),
                provider: "codex".to_string(),
                status: DELIVERY_STATUS_PENDING.to_string(),
                effective_policy: Some("non_interrupting".to_string()),
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: None,
                last_error_message: None,
                created_at: 7_002,
                updated_at: 7_002,
            }],
        )
        .expect("seed canonical migration mirror");

    let service = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("restart comms service");
    let recovered = service
        .recover_startup_deferred_deliveries()
        .await
        .expect("recover migration mirror");
    assert_eq!(recovered, 1, "migrated row should be retried exactly once");

    let canonical_rows = service
        .get_agent_deliveries(AgentDeliveryListRequest {
            message_id: Some(message_id.clone()),
            recipient_agent_id: Some(recipient.clone()),
        })
        .await
        .expect("canonical delivery");
    assert_eq!(canonical_rows.len(), 1);
    assert_eq!(canonical_rows[0].status, DELIVERY_STATUS_INJECTED);

    let hydrated = store
        .hydrate_runtime_state()
        .expect("hydrate migration mirror");
    let legacy_row = hydrated
        .team_deliveries
        .iter()
        .find(|row| row.id == delivery_id)
        .expect("legacy delivery mirror");
    assert_eq!(legacy_row.status, DELIVERY_STATUS_INJECTED);

    let turns = runtime
        .list_session_turns(&recipient)
        .await
        .expect("recipient turns");
    assert_eq!(turns.len(), 1, "migration recovery must inject one turn");
}

#[tokio::test]
async fn concurrent_agent_directs_preserve_per_recipient_fifo_order() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store, 300);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let first_request = AgentDirectMessageRequest {
        sender_agent_id: sender.clone(),
        recipient_agent_id: recipient.clone(),
        input: serde_json::json!([{ "type": "text", "text": "first" }]),
        image_paths: Vec::new(),
        priority: "normal".to_string(),
        policy: "non_interrupting".to_string(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: Some("fifo-one".to_string()),
    };
    let second_request = AgentDirectMessageRequest {
        sender_agent_id: sender,
        recipient_agent_id: recipient.clone(),
        input: serde_json::json!([{ "type": "text", "text": "second" }]),
        image_paths: Vec::new(),
        priority: "normal".to_string(),
        policy: "non_interrupting".to_string(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: Some("fifo-two".to_string()),
    };
    let (first, second) = tokio::join!(
        service.send_agent_direct(first_request),
        service.send_agent_direct(second_request)
    );
    first.expect("first direct");
    second.expect("second direct");
    let mut rows = service
        .get_agent_deliveries(AgentDeliveryListRequest {
            message_id: None,
            recipient_agent_id: Some(recipient),
        })
        .await
        .expect("recipient deliveries");
    rows.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].status, DELIVERY_STATUS_INJECTED);
    assert_eq!(rows[1].status, DELIVERY_STATUS_DEFERRED);
}

#[tokio::test]
async fn direct_message_image_paths_are_injected_as_image_items() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store, 0);
    let lead = create_test_session(&runtime).await;
    let member = create_test_session(&runtime).await;

    let team = service
        .create_team(TeamCreateRequest {
            name: "Image Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![member.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team");

    service
        .send_direct(TeamSendDirectRequest {
            team_id: team.team.id,
            sender_agent_id: lead,
            recipient_agent_id: member.clone(),
            input: serde_json::json!([{ "type": "text", "text": "please inspect" }]),
            image_paths: vec!["/tmp/reference.png".to_string()],
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
        })
        .await
        .expect("send direct with image");

    let turns = runtime
        .list_session_turns(member.as_str())
        .await
        .expect("member turns");
    assert!(turns
        .iter()
        .flat_map(|turn| turn.input.as_array().into_iter().flatten())
        .any(|item| {
            item.get("type").and_then(Value::as_str) == Some("image")
                && item.get("path").and_then(Value::as_str) == Some("/tmp/reference.png")
        }));
}
