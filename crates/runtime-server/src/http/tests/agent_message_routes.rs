use super::*;

#[tokio::test]
async fn agent_messages_route_cross_workspace_direct_and_snapshot_workspace_broadcasts() {
    let (router, token, temp_dir) = build_test_router().await;
    let root_a = temp_dir.path().join("workspace-a");
    let root_b = temp_dir.path().join("workspace-b");
    std::fs::create_dir_all(&root_a).expect("root a");
    std::fs::create_dir_all(&root_b).expect("root b");
    let workspace_a = register_workspace(&router, &token, &root_a, "Workspace A").await;
    let workspace_b = register_workspace(&router, &token, &root_b, "Workspace B").await;
    let sender = create_agent(
        &router,
        &token,
        &workspace_a.workspace_id,
        &root_a,
        "Sender",
    )
    .await;
    let same_one = create_agent(
        &router,
        &token,
        &workspace_a.workspace_id,
        &root_a,
        "Same One",
    )
    .await;
    let same_two = create_agent(
        &router,
        &token,
        &workspace_a.workspace_id,
        &root_a,
        "Same Two",
    )
    .await;
    let cross = create_agent(&router, &token, &workspace_b.workspace_id, &root_b, "Cross").await;

    let direct = create_message(
        &router,
        &token,
        "cross-direct",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":cross.agent_id,
            "input":[{"type":"text","text":"cross workspace"}],
        }),
    )
    .await;
    assert_eq!(
        direct.message.context_kind,
        runtime_core::AgentMessageContextKind::GlobalDirect
    );
    assert_eq!(direct.message.workspace_id, None);
    assert_eq!(
        direct.message.recipient_agent_ids,
        vec![cross.agent_id.clone()]
    );
    assert_eq!(direct.disposition, "created");

    let replay = create_message(
        &router,
        &token,
        "cross-direct",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":cross.agent_id,
            "input":[{"type":"text","text":"cross workspace"}],
        }),
    )
    .await;
    assert_eq!(replay.message.id, direct.message.id);
    assert_eq!(replay.disposition, "existing");

    let conflict = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/messages")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", "cross-direct")
                .body(Body::from(
                    serde_json::json!({
                        "mode":"direct",
                        "sender_agent_id":sender.agent_id,
                        "recipient_agent_id":cross.agent_id,
                        "input":[{"type":"text","text":"different input"}],
                    })
                    .to_string(),
                ))
                .expect("conflict request"),
        )
        .await
        .expect("conflict response");
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    let spoofed_context = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/messages")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "mode":"broadcast",
                        "sender_agent_id":sender.agent_id,
                        "workspace_id":workspace_b.workspace_id,
                        "input":[{"type":"text","text":"spoofed workspace"}],
                    })
                    .to_string(),
                ))
                .expect("spoofed-context request"),
        )
        .await
        .expect("spoofed-context response");
    assert_eq!(spoofed_context.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let same_workspace = create_message(
        &router,
        &token,
        "same-direct",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":same_one.agent_id,
            "input":[{"type":"text","text":"same workspace"}],
        }),
    )
    .await;
    assert_eq!(
        same_workspace.message.context_kind,
        runtime_core::AgentMessageContextKind::WorkspaceTeam
    );
    assert_eq!(
        same_workspace.message.workspace_id.as_deref(),
        Some(workspace_a.workspace_id.as_str())
    );

    let broadcast = create_message(
        &router,
        &token,
        "broadcast-snapshot",
        serde_json::json!({
            "mode":"broadcast",
            "sender_agent_id":sender.agent_id,
            "input":[{"type":"text","text":"workspace only"}],
        }),
    )
    .await;
    assert_eq!(broadcast.message.scope, "broadcast");
    assert_eq!(
        broadcast.message.workspace_id.as_deref(),
        Some(workspace_a.workspace_id.as_str())
    );
    assert_eq!(
        broadcast.message.recipient_agent_ids,
        vec![same_one.agent_id.clone(), same_two.agent_id.clone()]
    );
    assert!(!broadcast
        .message
        .recipient_agent_ids
        .contains(&sender.agent_id));
    assert!(!broadcast
        .message
        .recipient_agent_ids
        .contains(&cross.agent_id));

    let late = create_agent(&router, &token, &workspace_a.workspace_id, &root_a, "Late").await;
    let listed = list_messages(&router, &token, Some(&workspace_a.workspace_id)).await;
    let persisted_broadcast = listed
        .messages
        .iter()
        .find(|message| message.id == broadcast.message.id)
        .expect("broadcast in workspace list");
    assert_eq!(
        persisted_broadcast.recipient_agent_ids,
        vec![same_one.agent_id.clone(), same_two.agent_id.clone()]
    );
    assert!(!persisted_broadcast
        .recipient_agent_ids
        .contains(&late.agent_id));

    let legacy = create_legacy_session(&router, &token).await;
    let legacy_direct = create_message(
        &router,
        &token,
        "legacy-direct",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":legacy.id,
            "input":[{"type":"text","text":"migration compatibility"}],
        }),
    )
    .await;
    assert_eq!(
        legacy_direct.message.context_kind,
        runtime_core::AgentMessageContextKind::GlobalDirect
    );
}

#[tokio::test]
async fn agent_message_images_are_validated_ordered_and_acp_fails_truthfully() {
    let (router, token, temp_dir, _) = build_mixed_provider_test_router().await;
    let root = temp_dir.path().join("images-workspace");
    std::fs::create_dir_all(&root).expect("root");
    let workspace = register_workspace(&router, &token, &root, "Images").await;
    let sender = create_agent(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Image Sender",
    )
    .await;
    let recipient = create_agent_with_provider(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Image Recipient",
        "claude",
    )
    .await;
    let acp = create_agent_with_provider(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "ACP Recipient",
        "acp",
    )
    .await;
    let png = temp_dir.path().join("first.png");
    let jpeg = temp_dir.path().join("second.jpg");
    std::fs::write(&png, [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]).expect("png");
    std::fs::write(&jpeg, [0xFF, 0xD8, 0xFF, 0x00]).expect("jpeg");

    let message = create_message(
        &router,
        &token,
        "ordered-images",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":recipient.agent_id,
            "input":[{"type":"text","text":"with images"}],
            "image_paths":[png, jpeg],
        }),
    )
    .await;
    assert_eq!(
        message.message.image_paths,
        vec![png.display().to_string(), jpeg.display().to_string()]
    );
    let injected_turn_id = message.deliveries[0]
        .injected_turn_id
        .clone()
        .expect("delivery turn id");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    });
    let hydrated = store.hydrate_runtime_state().expect("hydrate");
    let turn = hydrated
        .turns
        .iter()
        .find(|turn| turn.id == injected_turn_id)
        .expect("injected turn");
    let items = turn.input.as_array().expect("turn input array");
    let prefix = items[0]
        .get("text")
        .and_then(Value::as_str)
        .expect("prefix");
    assert!(prefix.contains("<agent_msg"));
    assert!(prefix.contains(sender.alias.as_str()));
    assert!(prefix.contains("attachments=\"2\""));
    assert_eq!(
        items[2].get("path").and_then(Value::as_str),
        Some(png.to_str().unwrap())
    );
    assert_eq!(
        items[3].get("path").and_then(Value::as_str),
        Some(jpeg.to_str().unwrap())
    );

    let acp_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/messages")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "mode":"direct",
                        "sender_agent_id":sender.agent_id,
                        "recipient_agent_id":acp.agent_id,
                        "input":[{"type":"text","text":"unsupported image"}],
                        "image_paths":[png],
                    })
                    .to_string(),
                ))
                .expect("acp image request"),
        )
        .await
        .expect("acp image response");
    assert_eq!(acp_response.status(), StatusCode::BAD_REQUEST);

    let missing_path = "/private/secret/never-there.png";
    let invalid = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/messages")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "mode":"direct",
                        "sender_agent_id":sender.agent_id,
                        "recipient_agent_id":recipient.agent_id,
                        "input":[{"type":"text","text":"bad path"}],
                        "image_paths":[missing_path],
                    })
                    .to_string(),
                ))
                .expect("invalid image request"),
        )
        .await
        .expect("invalid image response");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8(
        to_bytes(invalid.into_body(), usize::MAX)
            .await
            .expect("invalid body")
            .to_vec(),
    )
    .expect("body utf8");
    assert!(
        !body.contains(missing_path),
        "image errors must not disclose source paths"
    );
}

#[tokio::test]
async fn deferred_agent_delivery_can_be_queried_and_cancelled_durably() {
    let (router, token, temp_dir) = build_test_router().await;
    let root = temp_dir.path().join("deferred-workspace");
    std::fs::create_dir_all(&root).expect("root");
    let workspace = register_workspace(&router, &token, &root, "Deferred").await;
    let sender = create_agent(&router, &token, &workspace.workspace_id, &root, "Sender").await;
    let recipient = create_agent_with_permission(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Recipient",
        "require_approval",
    )
    .await;
    let busy = send_turn(&router, &token, &recipient.agent_id).await;
    assert_eq!(busy.status, "waiting_for_approval");

    let message = create_message(
        &router,
        &token,
        "deferred-message",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":recipient.agent_id,
            "input":[{"type":"text","text":"wait"}],
            "policy":"non_interrupting",
        }),
    )
    .await;
    assert_eq!(message.deliveries[0].status, "deferred");

    let deliveries = list_deliveries(&router, &token, &message.message.id).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, "deferred");
    let retry = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/deliveries/{}/retry", deliveries[0].id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("retry request"),
        )
        .await
        .expect("retry response");
    assert_eq!(retry.status(), StatusCode::OK);
    let retried: runtime_core::AgentDeliveryRecord = serde_json::from_slice(
        &to_bytes(retry.into_body(), usize::MAX)
            .await
            .expect("retry body"),
    )
    .expect("retry json");
    assert_eq!(retried.status, "deferred");

    let cancel = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/messages/{}/cancel", message.message.id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("cancel request"),
        )
        .await
        .expect("cancel response");
    assert_eq!(cancel.status(), StatusCode::OK);
    let cancelled: Vec<runtime_core::AgentDeliveryRecord> = serde_json::from_slice(
        &to_bytes(cancel.into_body(), usize::MAX)
            .await
            .expect("cancel body"),
    )
    .expect("cancel json");
    assert_eq!(cancelled[0].status, "cancelled");

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    });
    let hydrated = store.hydrate_runtime_state().expect("hydrate");
    assert_eq!(
        hydrated
            .agent_deliveries
            .iter()
            .find(|delivery| delivery.id == cancelled[0].id)
            .expect("persisted delivery")
            .status,
        "cancelled"
    );
}

async fn register_workspace(
    router: &Router,
    token: &str,
    root: &Path,
    name: &str,
) -> runtime_core::WorkspaceRecord {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({"canonical_root":root,"display_name":name}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice::<runtime_core::WorkspaceRegisterResponse>(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()
    .workspace
}

async fn create_agent(
    router: &Router,
    token: &str,
    workspace_id: &str,
    root: &Path,
    title: &str,
) -> runtime_core::WorkspaceAgentRecord {
    create_agent_with_permission(router, token, workspace_id, root, title, "workspace_write").await
}

async fn create_agent_with_permission(
    router: &Router,
    token: &str,
    workspace_id: &str,
    root: &Path,
    title: &str,
    permission: &str,
) -> runtime_core::WorkspaceAgentRecord {
    create_agent_with_provider_and_permission(
        router,
        token,
        workspace_id,
        root,
        title,
        "codex",
        permission,
    )
    .await
}

async fn create_agent_with_provider(
    router: &Router,
    token: &str,
    workspace_id: &str,
    root: &Path,
    title: &str,
    provider: &str,
) -> runtime_core::WorkspaceAgentRecord {
    create_agent_with_provider_and_permission(
        router,
        token,
        workspace_id,
        root,
        title,
        provider,
        "workspace_write",
    )
    .await
}

async fn create_agent_with_provider_and_permission(
    router: &Router,
    token: &str,
    workspace_id: &str,
    root: &Path,
    title: &str,
    provider: &str,
    permission: &str,
) -> runtime_core::WorkspaceAgentRecord {
    let response = router.clone().oneshot(
        Request::builder().method("POST").uri(format!("/v2/workspaces/{workspace_id}/agents"))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(serde_json::json!({"provider":provider,"model": if provider == "acp" { Value::Null } else { Value::String("test-model".to_string()) },"permission_intent":permission,"cwd":root,"title":title}).to_string())).unwrap()
    ).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn create_legacy_session(router: &Router, token: &str) -> runtime_core::SessionRecord {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"provider":"codex","model":"test-model"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn create_message(
    router: &Router,
    token: &str,
    key: &str,
    body: Value,
) -> runtime_core::AgentMessageAck {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/messages")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("idempotency-key", key)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "message response: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

async fn list_messages(
    router: &Router,
    token: &str,
    workspace_id: Option<&str>,
) -> runtime_core::AgentMessageListResponse {
    let uri = workspace_id
        .map(|workspace_id| format!("/v2/messages?workspace_id={workspace_id}"))
        .unwrap_or_else(|| "/v2/messages".to_string());
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn list_deliveries(
    router: &Router,
    token: &str,
    message_id: &str,
) -> Vec<runtime_core::AgentDeliveryRecord> {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/messages/{message_id}/deliveries"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn send_turn(
    router: &Router,
    token: &str,
    session_id: &str,
) -> runtime_core::SendTurnAccepted {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"input":[{"type":"text","text":"busy"}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}
