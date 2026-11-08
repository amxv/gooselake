use super::workspace_worktree_routes::fixture;
use super::*;

#[tokio::test]
async fn unknown_provider_rebind_returns_durable_coordinate_and_replays_without_dispatch() {
    for mode in ["unknown", "bad_evidence"] {
        let (router, token, temp, provider, workspace, agent, _) = fixture().await;
        provider.set_rebind_mode(Some(mode)).await;
        let uri = format!(
            "/v2/workspaces/{}/agents/{}/worktree",
            workspace.workspace_id, agent.agent_id
        );
        let key = format!("lost-native-{mode}");
        let request = || {
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", &key)
                .body(Body::from(
                    serde_json::json!({
                        "worktree_id":"wt_linked", "expected_revision":agent.revision
                    })
                    .to_string(),
                ))
                .unwrap()
        };
        let original = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(original.status(), StatusCode::CONFLICT, "{mode}");
        let operation_id = original.headers()["x-operation-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert!(operation_id.starts_with("rebind_"));
        assert!(original.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .starts_with("req_"));
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(original.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["error"]["code"], "reassignment_recovery_required");
        assert_eq!(body["error"]["retryable"], false);
        assert_eq!(body["error"]["details"]["dispatch_outcome"], "unknown");
        assert_eq!(body["error"]["details"]["operation_id"], operation_id);

        let store = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp.path().join("runtime.sqlite3"),
        });
        let recorded = store
            .get_workspace_agent_rebind_by_key(&workspace.workspace_id, &agent.agent_id, &key)
            .unwrap()
            .unwrap();
        assert_eq!(recorded.operation_id, operation_id);
        assert_eq!(recorded.phase, "manual_review");
        assert_eq!(
            store
                .get_operation(&operation_id)
                .unwrap()
                .unwrap()
                .operation
                .phase,
            runtime_core::OperationPhase::ManualReview
        );
        assert_eq!(
            store
                .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
                .unwrap()
                .unwrap()
                .recreation_policy
                .authoritative_cwd,
            workspace.canonical_root,
            "unverified native change must not update authoritative cwd"
        );

        let replay = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(replay.status(), StatusCode::CONFLICT, "{mode}");
        assert_eq!(replay.headers()["x-operation-id"], operation_id);
        let retry_body: serde_json::Value =
            serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            retry_body["error"]["code"],
            "reassignment_recovery_required"
        );
        assert_eq!(retry_body["error"]["details"]["operation_id"], operation_id);
        assert_eq!(
            retry_body["error"]["details"]["dispatch_outcome"],
            "unknown"
        );
        assert_eq!(
            provider.rebind_calls().await,
            1,
            "{mode} must not rerun native rebind"
        );
    }
}
