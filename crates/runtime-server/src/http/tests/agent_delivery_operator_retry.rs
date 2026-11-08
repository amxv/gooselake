use super::agent_message_routes::{
    create_agent, create_agent_with_permission, create_message, list_deliveries, register_workspace,
};
use super::*;

#[tokio::test]
async fn keyed_retry_admits_once_replays_exact_pending_snapshot_and_preserves_legacy_unkeyed() {
    let (router, token, temp_dir, runtime) = build_test_router_with_runtime().await;
    let root = temp_dir.path().join("keyed-retry-workspace");
    fs::create_dir_all(&root).unwrap();
    let workspace = register_workspace(&router, &token, &root, "Keyed Retry").await;
    let sender = create_agent(&router, &token, &workspace.workspace_id, &root, "Sender").await;
    let recipient = create_agent_with_permission(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Approval Recipient",
        "require_approval",
    )
    .await;
    runtime.dispatch_workspace_system_notices().await.unwrap();
    assert_eq!(
        runtime
            .get_session(&recipient.agent_id)
            .await
            .unwrap()
            .status,
        "waiting_for_approval"
    );
    let message = create_message(
        &router,
        &token,
        "keyed-retry-message",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":recipient.agent_id,
            "input":[{"type":"text","text":"retry after onboarding"}],
            "policy":"non_interrupting"
        }),
    )
    .await;
    assert_eq!(message.deliveries[0].status, "deferred");

    let uri = format!("/v2/deliveries/{}/retry", message.deliveries[0].id);
    let invalid = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "must-not-be-admitted")
                .body(Body::from(r#"{"dry_run":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let error: serde_json::Value =
        serde_json::from_slice(&to_bytes(invalid.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(error["error"]["code"], "invalid_request");
    assert_eq!(
        list_deliveries(&router, &token, &message.message.id).await[0].status,
        "deferred",
        "unrecognized retry options must not dispatch"
    );
    let keyed = |uri: &str, key: &str| {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("Idempotency-Key", key)
            .body(Body::empty())
            .unwrap()
    };
    let first = router
        .clone()
        .oneshot(keyed(&uri, "stable-retry"))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let operation_id = first.headers()["x-operation-id"]
        .to_str()
        .unwrap()
        .to_string();
    let snapshot: runtime_core::AgentDeliveryRecord =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        snapshot.status, "pending",
        "receipt is queue admission, not provider result"
    );
    let current = list_deliveries(&router, &token, &message.message.id).await;
    assert_eq!(
        current[0].status, "deferred",
        "provider is still blocked by approval"
    );

    let inspection = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/operations/{operation_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(inspection.status(), StatusCode::OK);
    let operation: runtime_core::OperationDetails =
        serde_json::from_slice(&to_bytes(inspection.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(operation.operation.kind, "agent_delivery_retry_admission");
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(
        operation.operation.exact_terminal_result,
        Some(serde_json::json!({"operation_id":operation_id,"delivery":snapshot}))
    );
    assert!(operation.claims.is_empty());

    let replay = router
        .clone()
        .oneshot(keyed(&uri, "stable-retry"))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["x-operation-id"], operation_id.as_str());
    let replay_snapshot: runtime_core::AgentDeliveryRecord =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(replay_snapshot, snapshot);
    assert_eq!(
        list_deliveries(&router, &token, &message.message.id).await[0].status,
        "deferred"
    );

    let other = create_message(
        &router,
        &token,
        "keyed-retry-other-message",
        serde_json::json!({
            "mode":"direct",
            "sender_agent_id":sender.agent_id,
            "recipient_agent_id":recipient.agent_id,
            "input":[{"type":"text","text":"different"}],
            "policy":"non_interrupting"
        }),
    )
    .await;
    assert_eq!(other.deliveries[0].status, "deferred");
    let conflict = router
        .clone()
        .oneshot(keyed(
            &format!("/v2/deliveries/{}/retry", other.deliveries[0].id),
            "stable-retry",
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    let duplicate_header = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("Idempotency-Key", "a")
                .header("Idempotency-Key", "b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate_header.status(), StatusCode::BAD_REQUEST);

    // Old callers still receive a live deferred status after a synchronous
    // attempt and do not get a misleading admission-operation header.
    let legacy = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(legacy.status(), StatusCode::OK);
    assert!(legacy.headers().get("x-operation-id").is_none());
    let live: runtime_core::AgentDeliveryRecord =
        serde_json::from_slice(&to_bytes(legacy.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(live.status, "deferred");
}
