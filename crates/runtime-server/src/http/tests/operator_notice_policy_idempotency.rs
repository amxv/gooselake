use super::*;

#[tokio::test]
async fn notice_policy_http_idempotency_replays_exact_receipt_with_revision_and_operation_lookup() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("durable-notice-policy");
    std::fs::create_dir_all(&root).expect("root");
    let workspace = register_control_workspace(&router, &token, &root, "Durable Policy").await;
    let uri = format!("/v2/workspaces/{}/notice-policy", workspace.workspace_id);
    let input = serde_json::json!({
        "expected_revision": 0,
        "policy": {
            "lead_transitions": false,
            "membership_to_lead": true,
            "worktree_reassignment": true,
            "workspace_interrupt": true,
            "workspace_retirement": true,
            "compaction_to_lead": true,
            "compaction_to_team": false
        }
    });
    let put = |body: serde_json::Value, key: &str, match_revision: &str| {
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::IF_MATCH, match_revision)
            .header("Idempotency-Key", key)
            .body(Body::from(body.to_string()))
            .expect("request")
    };
    let unauth = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "lost-http-policy")
                .body(Body::from(input.to_string()))
                .expect("request"),
        )
        .await
        .expect("unauth response");
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let updated = router
        .clone()
        .oneshot(put(input.clone(), "lost-http-policy", "\"0\""))
        .await
        .expect("first response");
    assert_eq!(updated.status(), StatusCode::OK);
    assert_eq!(updated.headers()[header::ETAG], "\"1\"");
    let operation_id = updated.headers()["x-operation-id"]
        .to_str()
        .expect("operation id")
        .to_string();
    assert!(operation_id.starts_with("op_"));
    let original: runtime_core::WorkspaceNoticePolicyRecord =
        serde_json::from_slice(&to_bytes(updated.into_body(), usize::MAX).await.unwrap())
            .expect("first policy");
    assert_eq!(original.revision, 1);
    assert!(!original.policy.lead_transitions);

    let operation_get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/operations/{operation_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("operation request"),
        )
        .await
        .expect("operation response");
    assert_eq!(operation_get.status(), StatusCode::OK);
    let operation: runtime_core::OperationDetails = serde_json::from_slice(
        &to_bytes(operation_get.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .expect("operation details");
    assert_eq!(operation.operation.operation_id, operation_id);
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert!(operation.claims.is_empty());
    assert_eq!(
        operation.operation.exact_terminal_result,
        Some(serde_json::json!({"operation_id":operation_id,"policy":original}))
    );

    // A later independent update moves the revision forward; the original
    // lost-response replay must still return its immutable exact older result.
    let next_input = serde_json::json!({
        "expected_revision": 1,
        "policy": {
            "lead_transitions": true,
            "membership_to_lead": false,
            "worktree_reassignment": true,
            "workspace_interrupt": true,
            "workspace_retirement": true,
            "compaction_to_lead": true,
            "compaction_to_team": false
        }
    });
    let next = router
        .clone()
        .oneshot(put(next_input, "later-policy", "\"1\""))
        .await
        .expect("later change");
    assert_eq!(next.status(), StatusCode::OK);
    assert_eq!(next.headers()[header::ETAG], "\"2\"");

    let replay = router
        .clone()
        .oneshot(put(input.clone(), "lost-http-policy", "\"0\""))
        .await
        .expect("exact replay");
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()[header::ETAG], "\"1\"");
    assert_eq!(replay.headers()["x-operation-id"], operation_id.as_str());
    let replayed: runtime_core::WorkspaceNoticePolicyRecord =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap())
            .expect("replayed original response");
    assert_eq!(replayed, original);

    let mut different = input.clone();
    different["policy"]["lead_transitions"] = serde_json::json!(true);
    let conflict = router
        .clone()
        .oneshot(put(different, "lost-http-policy", "\"0\""))
        .await
        .expect("different payload response");
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    let cross_kind = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "lost-http-policy")
                .body(Body::from(
                    serde_json::json!({
                        "canonical_root": root.to_string_lossy(),
                        "display_name": "Different operation"
                    })
                    .to_string(),
                ))
                .expect("cross-kind request"),
        )
        .await
        .expect("cross-kind response");
    assert_eq!(cross_kind.status(), StatusCode::CONFLICT);
    let stale_precondition = router
        .clone()
        .oneshot(put(input.clone(), "new-key", "\"99\""))
        .await
        .expect("precondition failure");
    assert_eq!(stale_precondition.status(), StatusCode::PRECONDITION_FAILED);
    let duplicate_key = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "a")
                .header("Idempotency-Key", "b")
                .body(Body::from(input.to_string()))
                .expect("duplicate header request"),
        )
        .await
        .expect("duplicate key rejection");
    assert_eq!(duplicate_key.status(), StatusCode::BAD_REQUEST);
    let final_get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("get request"),
        )
        .await
        .expect("final get response");
    assert_eq!(final_get.status(), StatusCode::OK);
    assert_eq!(final_get.headers()[header::ETAG], "\"2\"");
    let saved: runtime_core::WorkspaceNoticePolicyRecord =
        serde_json::from_slice(&to_bytes(final_get.into_body(), usize::MAX).await.unwrap())
            .expect("saved policy");
    assert_eq!(saved.revision, 2);
    assert!(!saved.policy.membership_to_lead);
}
