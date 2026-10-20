use super::*;

#[tokio::test]
async fn effective_workspace_hooks_are_operator_only_filtered_and_never_executed() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("hook-workspace");
    std::fs::create_dir(&root).unwrap();
    let registered = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({"canonical_root":root,"display_name":"Hook workspace"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(registered.status(), StatusCode::OK);
    let workspace: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let config_path = root.join(runtime_core::TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    let marker = root.join("hook-must-not-run");
    let payload = serde_json::json!({
        "version":1,
        "hooks":{
            "pre_teammate_add":[{
                "name":"Project Setup",
                "command":format!("touch {}",marker.display()),
                "timeout_ms":250
            }],
            "post_compaction":[{"name":"summary","command":"echo summary"}]
        }
    });
    std::fs::write(&config_path, payload.to_string()).unwrap();
    let url = format!("/v2/workspaces/{}/hooks", workspace.workspace.workspace_id);
    let get = |path: &str, authorized: bool| {
        let mut req = Request::builder().uri(path);
        if authorized {
            req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        req.body(Body::empty()).unwrap()
    };
    let unauthenticated = router.clone().oneshot(get(&url, false)).await.unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let all = router.clone().oneshot(get(&url, true)).await.unwrap();
    assert_eq!(all.status(), StatusCode::OK);
    let full: serde_json::Value =
        serde_json::from_slice(&to_bytes(all.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(full["execution_enabled"], false);
    assert_eq!(full["config_version"], 1);
    assert_eq!(full["hooks"].as_array().unwrap().len(), 2);
    assert_eq!(full["hooks"][0]["scope"], "project");
    assert_eq!(full["hooks"][0]["name"], "project_setup");
    assert_eq!(full["hooks"][0]["timeout_ms"], 250);
    assert!(
        !marker.exists(),
        "config discovery cannot execute hook commands"
    );

    let one = router
        .clone()
        .oneshot(get(&format!("{url}?event=post_compaction"), true))
        .await
        .unwrap();
    assert_eq!(one.status(), StatusCode::OK);
    let filtered: serde_json::Value =
        serde_json::from_slice(&to_bytes(one.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(filtered["hooks"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["hooks"][0]["name"], "summary");
    assert_eq!(
        router
            .clone()
            .oneshot(get(&format!("{url}?event=not_a_hook"), true))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        router
            .clone()
            .oneshot(get("/v2/workspaces/no_such_workspace/hooks", true))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    std::fs::write(&config_path, r#"{"version":1,"hooks":{"unknown":[]}}"#).unwrap();
    let invalid = router.clone().oneshot(get(&url, true)).await.unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert!(!marker.exists());
}

#[tokio::test]
async fn workspace_routes_are_durable_idempotent_and_leave_v1_available() {
    let (router, token, temp_dir) = build_test_router().await;
    let workspace_root = temp_dir.path().join("registered-workspace");
    std::fs::create_dir(&workspace_root).expect("workspace root");
    let body = serde_json::json!({
        "canonical_root": workspace_root,
        "display_name": "Registered Workspace"
    });

    let register = |body: serde_json::Value, key: &str| {
        Request::builder()
            .method("POST")
            .uri("/v2/workspaces")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("Idempotency-Key", key)
            .body(Body::from(body.to_string()))
            .expect("registration request")
    };

    let first_response = router
        .clone()
        .oneshot(register(body.clone(), "workspace-register-1"))
        .await
        .expect("first registration response");
    assert_eq!(first_response.status(), StatusCode::OK);
    let first: runtime_core::WorkspaceRegisterResponse = serde_json::from_slice(
        &to_bytes(first_response.into_body(), usize::MAX)
            .await
            .expect("first body"),
    )
    .expect("first registration json");

    let replay_response = router
        .clone()
        .oneshot(register(body.clone(), "workspace-register-1"))
        .await
        .expect("replay response");
    assert_eq!(replay_response.status(), StatusCode::OK);
    let replay: runtime_core::WorkspaceRegisterResponse = serde_json::from_slice(
        &to_bytes(replay_response.into_body(), usize::MAX)
            .await
            .expect("replay body"),
    )
    .expect("replay json");
    assert_eq!(
        replay, first,
        "same key must replay the exact terminal result"
    );

    let second_operation_response = router
        .clone()
        .oneshot(register(body.clone(), "workspace-register-2"))
        .await
        .expect("second operation response");
    assert_eq!(second_operation_response.status(), StatusCode::OK);
    let second_operation: runtime_core::WorkspaceRegisterResponse = serde_json::from_slice(
        &to_bytes(second_operation_response.into_body(), usize::MAX)
            .await
            .expect("second operation body"),
    )
    .expect("second operation json");
    assert_eq!(
        second_operation.workspace.workspace_id,
        first.workspace.workspace_id
    );
    assert_ne!(second_operation.operation_id, first.operation_id);

    let conflict_body = serde_json::json!({
        "canonical_root": body["canonical_root"],
        "display_name": "Changed Input"
    });
    let conflict = router
        .clone()
        .oneshot(register(conflict_body, "workspace-register-1"))
        .await
        .expect("conflict response");
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    let list_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("list request"),
        )
        .await
        .expect("list response");
    assert_eq!(list_response.status(), StatusCode::OK);
    let list: Vec<runtime_core::WorkspaceRecord> = serde_json::from_slice(
        &to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("list body"),
    )
    .expect("list json");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0], first.workspace);

    let get_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{}", first.workspace.workspace_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("get request"),
        )
        .await
        .expect("get response");
    assert_eq!(get_response.status(), StatusCode::OK);
    let fetched: runtime_core::WorkspaceRecord = serde_json::from_slice(
        &to_bytes(get_response.into_body(), usize::MAX)
            .await
            .expect("get body"),
    )
    .expect("get json");
    assert_eq!(fetched, first.workspace);

    let operation_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/operations/{}", first.operation_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("operation request"),
        )
        .await
        .expect("operation response");
    assert_eq!(operation_response.status(), StatusCode::OK);
    let operation: runtime_core::OperationDetails = serde_json::from_slice(
        &to_bytes(operation_response.into_body(), usize::MAX)
            .await
            .expect("operation body"),
    )
    .expect("operation json");
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(operation.transitions.len(), 2);
    assert!(operation.claims.is_empty());

    let v1_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/health")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("v1 health request"),
        )
        .await
        .expect("v1 health response");
    assert_eq!(v1_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn workspace_routes_require_operator_bearer_auth() {
    let (router, _token, _temp_dir) = build_test_router().await;
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v2/workspaces")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
