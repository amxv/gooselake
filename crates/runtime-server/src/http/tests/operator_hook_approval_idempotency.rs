use super::workspace_control_routes::register_control_workspace;
use super::*;

#[tokio::test]
async fn hook_approval_http_replays_old_receipt_after_config_drift_and_new_revision() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("approved-hooks-once");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = register_control_workspace(&router, &token, &root, "Approval receipts").await;
    let config_path = root.join(runtime_core::TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(
        &config_path,
        serde_json::json!({"version":1,"hooks":{
            "post_teammate_add":[{"name":"Notify","command":"echo initial","timeout_ms":500}]
        }})
        .to_string(),
    )
    .unwrap();
    let uri = format!("/v2/workspaces/{}/hooks/approval", workspace.workspace_id);
    let discover = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{}/hooks", workspace.workspace_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discover.status(), StatusCode::OK);
    let discovered: serde_json::Value =
        serde_json::from_slice(&to_bytes(discover.into_body(), usize::MAX).await.unwrap()).unwrap();
    let fingerprint = discovered["hooks"][0]["fingerprint"].as_str().unwrap();
    let selected = serde_json::json!({"expected_revision":0,"hooks":[{
        "event_name":"post_teammate_add", "name":"notify", "fingerprint":fingerprint
    }]});
    let put = |body: serde_json::Value, key: &str, etag: &str| {
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::IF_MATCH, etag)
            .header("Idempotency-Key", key)
            .body(Body::from(body.to_string()))
            .unwrap()
    };

    let unauth = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "operator-hook-key")
                .body(Body::from(selected.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
    let original = router
        .clone()
        .oneshot(put(selected.clone(), "operator-hook-key", "\"0\""))
        .await
        .unwrap();
    assert_eq!(original.status(), StatusCode::OK);
    assert_eq!(original.headers()[header::ETAG], "\"1\"");
    let operation_id = original.headers()["x-operation-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(operation_id.starts_with("op_"));
    let original_approval: runtime_core::TeamHookApproval =
        serde_json::from_slice(&to_bytes(original.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(original_approval.revision, 1);
    assert_eq!(original_approval.hooks[0].command, "echo initial");

    let get_operation = router
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
    assert_eq!(get_operation.status(), StatusCode::OK);
    let details: runtime_core::OperationDetails = serde_json::from_slice(
        &to_bytes(get_operation.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(details.operation.kind, "workspace_hook_approval_update");
    assert_eq!(
        details.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert!(details.claims.is_empty());
    assert_eq!(
        details.operation.exact_terminal_result,
        Some(serde_json::json!({
            "operation_id": operation_id, "approval":original_approval,
        }))
    );

    // A different request advances revision; replacing the project config
    // with invalid JSON must not invalidate the old successful HTTP receipt.
    std::fs::write(&config_path, r#"{"version":1,"hooks":{"unknown":[]}}"#).unwrap();
    let revoke = router
        .clone()
        .oneshot(put(
            serde_json::json!({"expected_revision":1,"hooks":[]}),
            "revoke-hooks",
            "\"1\"",
        ))
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::OK);
    assert_eq!(revoke.headers()[header::ETAG], "\"2\"");
    let replay = router
        .clone()
        .oneshot(put(selected.clone(), "operator-hook-key", "\"0\""))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()[header::ETAG], "\"1\"");
    assert_eq!(
        replay.headers()["x-operation-id"].to_str().unwrap(),
        operation_id
    );
    let replayed: runtime_core::TeamHookApproval =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(replayed, original_approval);

    let mut changed = selected.clone();
    changed["hooks"] = serde_json::json!([]);
    assert_eq!(
        router
            .clone()
            .oneshot(put(changed, "operator-hook-key", "\"0\""))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let stale = router
        .clone()
        .oneshot(put(selected, "fresh-key", "\"99\""))
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
    let get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.headers()[header::ETAG], "\"2\"");
    let current: runtime_core::TeamHookApproval =
        serde_json::from_slice(&to_bytes(get.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(current.hooks.is_empty());
}
