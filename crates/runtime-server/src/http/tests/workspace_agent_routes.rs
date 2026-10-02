use super::*;

#[tokio::test]
async fn workspace_agent_create_archive_restore_and_restart_preserve_authority() {
    let (router, token, temp_dir) = build_test_router().await;
    let workspace_root = temp_dir.path().join("agent-workspace");
    std::fs::create_dir(&workspace_root).expect("workspace root");
    let workspace = register_workspace(&router, &token, &workspace_root).await;

    let create_body = serde_json::json!({
        "provider": "codex",
        "model": "test-model",
        "permission_intent": "workspace_write",
        "setting_sources_intent": ["user", "project", "local"],
        "system_prompt": "Keep the exact launch policy.",
        "allowed_tools": ["read", "search"],
        "disallowed_tools": ["danger"],
        "cwd": workspace_root,
        "harness_version_slot": "runtime-v2",
        "title": "Builder",
        "metadata": {"purpose":"phase4"}
    });
    let create_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{}/agents", workspace.workspace_id))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(create_body.to_string()))
                .expect("create agent request"),
        )
        .await
        .expect("create agent response");
    assert_eq!(create_response.status(), StatusCode::OK);
    let created: runtime_core::WorkspaceAgentRecord = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    assert!(created.agent_id.starts_with("sess_codex_"));
    assert!(created.alias.contains('-'));
    assert_eq!(created.workspace_id, workspace.workspace_id);
    assert_eq!(created.profile.title.as_deref(), Some("Builder"));
    assert_eq!(
        created.recreation_policy.setting_sources_intent,
        vec!["user", "project", "local"]
    );
    assert_eq!(
        created.recreation_policy.system_prompt.as_deref(),
        Some("Keep the exact launch policy.")
    );
    assert_eq!(
        created.recreation_policy.allowed_tools,
        vec!["read", "search"]
    );
    assert_eq!(created.recreation_policy.disallowed_tools, vec!["danger"]);
    assert_eq!(
        created.recreation_policy.harness_version_slot.as_deref(),
        Some("runtime-v2")
    );

    let active = list_agents(&router, &token, &workspace.workspace_id, None).await;
    assert_eq!(active, vec![created.clone()]);

    let fetched = get_agent(&router, &token, &workspace.workspace_id, &created.agent_id).await;
    assert_eq!(fetched, created);

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    });
    let hydrated = store.hydrate_runtime_state().expect("hydrate after create");
    assert!(
        hydrated.team_members.is_empty(),
        "v2 create must not use legacy join"
    );

    let capture_provider = Arc::new(TestProvider::default());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry
        .register(capture_provider.clone())
        .expect("register restart provider");
    let restart_store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    let restarted = Arc::new(
        RuntimeSessionManager::new(restart_store, Arc::new(registry), 128)
            .expect("restarted runtime"),
    );
    restarted.recover_startup().await.expect("startup recovery");
    let resume_requests = capture_provider.resumed_requests().await;
    let resumed = resume_requests
        .iter()
        .find(|request| request.runtime_session_id == created.agent_id)
        .expect("workspace agent resume request");
    assert_eq!(resumed.model.as_deref(), Some("test-model"));
    assert_eq!(resumed.permission_mode.as_deref(), Some("workspace_write"));
    assert_eq!(resumed.setting_sources, vec!["user", "project", "local"]);
    assert_eq!(
        resumed.system_prompt.as_deref(),
        Some("Keep the exact launch policy.")
    );
    assert_eq!(resumed.allowed_tools, vec!["read", "search"]);
    assert_eq!(resumed.disallowed_tools, vec!["danger"]);
    assert_eq!(resumed.harness_version_slot.as_deref(), Some("runtime-v2"));

    let archive_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/workspaces/{}/agents/{}/archive",
                    workspace.workspace_id, created.agent_id
                ))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"reason":"done for now"}"#))
                .expect("archive request"),
        )
        .await
        .expect("archive response");
    assert_eq!(archive_response.status(), StatusCode::OK);
    let archived: runtime_core::WorkspaceAgentRecord = serde_json::from_slice(
        &to_bytes(archive_response.into_body(), usize::MAX)
            .await
            .expect("archive body"),
    )
    .expect("archive json");
    assert_eq!(archived.agent_id, created.agent_id);
    assert_eq!(archived.alias, created.alias);
    assert_eq!(archived.recreation_policy, created.recreation_policy);
    assert_eq!(
        archived.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );
    assert!(list_agents(&router, &token, &workspace.workspace_id, None)
        .await
        .is_empty());
    assert_eq!(
        list_agents(&router, &token, &workspace.workspace_id, Some("archived")).await,
        vec![archived.clone()]
    );

    let blocked_turn = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{}/turns", created.agent_id))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"input":[{"type":"text","text":"no"}]}"#))
                .expect("blocked turn request"),
        )
        .await
        .expect("blocked turn response");
    assert_eq!(blocked_turn.status(), StatusCode::BAD_REQUEST);

    let restore_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/workspaces/{}/agents/{}/restore",
                    workspace.workspace_id, created.agent_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("restore request"),
        )
        .await
        .expect("restore response");
    assert_eq!(restore_response.status(), StatusCode::OK);
    let restored: runtime_core::WorkspaceAgentRecord = serde_json::from_slice(
        &to_bytes(restore_response.into_body(), usize::MAX)
            .await
            .expect("restore body"),
    )
    .expect("restore json");
    assert_eq!(restored.agent_id, created.agent_id);
    assert_eq!(restored.alias, created.alias);
    assert_eq!(restored.recreation_policy, created.recreation_policy);
    assert_eq!(
        restored.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Active
    );
    assert_eq!(
        list_agents(&router, &token, &workspace.workspace_id, None).await,
        vec![restored]
    );

    let unowned_v2 = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/agents")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(create_body.to_string()))
                .expect("unowned v2 request"),
        )
        .await
        .expect("unowned v2 response");
    assert_eq!(unowned_v2.status(), StatusCode::NOT_FOUND);
}

async fn register_workspace(
    router: &Router,
    token: &str,
    root: &Path,
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
                    serde_json::json!({
                        "canonical_root": root,
                        "display_name": "Agent Workspace"
                    })
                    .to_string(),
                ))
                .expect("register workspace request"),
        )
        .await
        .expect("register workspace response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice::<runtime_core::WorkspaceRegisterResponse>(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("workspace body"),
    )
    .expect("workspace json")
    .workspace
}

async fn list_agents(
    router: &Router,
    token: &str,
    workspace_id: &str,
    lifecycle: Option<&str>,
) -> Vec<runtime_core::WorkspaceAgentRecord> {
    let suffix = lifecycle
        .map(|value| format!("?lifecycle={value}"))
        .unwrap_or_default();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace_id}/agents{suffix}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("list agents request"),
        )
        .await
        .expect("list agents response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("list agents body"),
    )
    .expect("list agents json")
}

async fn get_agent(
    router: &Router,
    token: &str,
    workspace_id: &str,
    agent_id: &str,
) -> runtime_core::WorkspaceAgentRecord {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace_id}/agents/{agent_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("get agent request"),
        )
        .await
        .expect("get agent response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("get agent body"),
    )
    .expect("get agent json")
}
