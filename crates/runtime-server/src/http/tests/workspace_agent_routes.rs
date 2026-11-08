use super::*;

#[tokio::test]
async fn agent_creation_and_archival_reject_unknown_fields_before_lifecycle_effects() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("strict-agent-mutations");
    std::fs::create_dir(&root).unwrap();
    let workspace = register_workspace(&router, &token, &root).await;
    let base = format!("/v2/workspaces/{}/agents", workspace.workspace_id);
    for input in [
        serde_json::json!({
            "provider":"codex", "model":"test-model", "unsafe_extra":"ignored"
        }),
        serde_json::json!({
            "provider":"codex", "model":"test-model",
            "current_preferences":{"thinking_efort":"max"}
        }),
        serde_json::json!({
            "provider":"codex", "model":"test-model",
            "onboarding":{"skill":{
                "provider":"codex", "name":"audit",
                "display_name":null, "path":null,
                "bypass_skill_validation":true
            }}
        }),
    ] {
        let (status, body) = super::workspace_worktree_routes::call(
            &router,
            &token,
            "POST",
            &base,
            Some(input),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "invalid_request");
    }
    let (status, rows) =
        super::workspace_worktree_routes::call(&router, &token, "GET", &base, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(rows.as_array().unwrap().is_empty());

    let (status, created) = super::workspace_worktree_routes::call(
        &router,
        &token,
        "POST",
        &base,
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let agent_id = created["agent_id"].as_str().unwrap();
    let agent_url = format!("{base}/{agent_id}");
    let (status, body) = super::workspace_worktree_routes::call(
        &router,
        &token,
        "POST",
        &format!("{agent_url}/archive"),
        Some(serde_json::json!({"reason":"hold","force":true})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], "invalid_request");
    let (status, unchanged) =
        super::workspace_worktree_routes::call(&router, &token, "GET", &agent_url, None, None)
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(unchanged["lifecycle_state"], "active");
}

#[tokio::test]
async fn workspace_notices_recover_and_dispatch_after_more_than_256_blocked_older_entries() {
    let (router, token, temp, provider) =
        build_test_router_with_provider(TeamMcpPolicy::default()).await;
    let root = temp.path().join("many-notices-workspace");
    std::fs::create_dir(&root).unwrap();
    let workspace = register_workspace(&router, &token, &root).await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{}/agents", workspace.workspace_id))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({"provider":"codex","model":"test-model"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let created: runtime_core::WorkspaceAgentRecord =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

    let db = temp.path().join("runtime.sqlite3");
    let mut connection = rusqlite::Connection::open(&db).unwrap();
    let tx = connection.transaction().unwrap();
    for index in 0..260 {
        let id = format!("blocked-notice-{index:03}");
        let input = serde_json::json!([{"type":"image","path":format!(
            "/this/image/does/not/exist/{index}.png"
        )}]);
        tx.execute(
            "INSERT INTO workspace_system_notices
             (notice_id,workspace_id,recipient_agent_id,kind,correlation_id,
              input_json,state,created_at_ms,updated_at_ms)
             VALUES (?1,?2,?3,'member_joined',?4,?5,'injecting',?6,?6)",
            rusqlite::params![
                id,
                workspace.workspace_id,
                created.agent_id,
                format!("notice-correlation-{index:03}"),
                input.to_string(),
                created.created_at - 1,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: db,
    }));
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider).unwrap();
    let manager =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256).unwrap());
    manager.recover_startup().await.unwrap();
    assert_eq!(
        manager.recover_workspace_system_notices().await.unwrap(),
        260
    );

    assert_eq!(
        manager.dispatch_workspace_system_notices().await.unwrap(),
        256
    );
    let onboarding_id = format!("workspace-onboarding:{}", created.agent_id);
    let notice = store
        .get_workspace_system_notice(&onboarding_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        notice.state, "pending",
        "later notice has not been scanned yet"
    );

    let later = manager.dispatch_workspace_system_notices().await.unwrap();
    assert!(
        later >= 4,
        "later page reached all four remaining blocked notices"
    );
    let notice = store
        .get_workspace_system_notice(&onboarding_id)
        .unwrap()
        .unwrap();
    assert_ne!(
        notice.state, "pending",
        "onboarding must not starve behind blocked notices"
    );
    assert_eq!(notice.attempt_count, 1);
    let matching = store
        .list_turn_admissions()
        .unwrap()
        .into_iter()
        .filter(|admission| {
            admission.correlation.correlation_id.as_deref() == Some(notice.correlation_id.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "one admitted automation-context turn");
    assert_eq!(
        matching[0].projection_source,
        runtime_core::TurnInputProjectionSource::AutomationContext
    );
}

#[tokio::test]
async fn operator_onboarding_dispatches_automation_context_once_after_restart() {
    let (router, token, temp, provider) =
        build_test_router_with_provider(TeamMcpPolicy::default()).await;
    let root = temp.path().join("onboarding-workspace");
    std::fs::create_dir(&root).expect("workspace");
    let workspace = register_workspace(&router, &token, &root).await;
    let body = serde_json::json!({
        "provider":"codex", "model":"test-model",
        "permission_intent":"workspace_write",
        "setting_sources_intent":["user","project"],
        "cwd":root, "title":"Researcher",
        "onboarding":{"prompt":"Inspect the API for regressions"}
    });
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{}/agents", workspace.workspace_id))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("create response");
    assert_eq!(response.status(), StatusCode::OK);
    let created: runtime_core::WorkspaceAgentRecord =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    // The worker is asynchronous. A user request immediately after creation
    // must never become the first provider turn, even through the old API.
    let premature = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{}/turns", created.agent_id))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"input":[{"type":"text","text":"early"}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(premature.status(), StatusCode::CONFLICT);
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    assert!(store.list_turn_admissions().unwrap().is_empty());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider).expect("provider");
    let manager =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256).unwrap());
    manager.recover_startup().await.expect("restart");
    let onboarding_id = format!("workspace-onboarding:{}", created.agent_id);
    let pending = store
        .get_workspace_system_notice(&onboarding_id)
        .unwrap()
        .unwrap();
    // Matching the public correlation and immutable payload must still be
    // insufficient to impersonate the private notice-dispatch worker.
    let forged = runtime_core::SendTurnInput {
        input: pending.input.clone(),
        expected_turn_id: None,
        permission_mode: None,
        projection_source: Some(runtime_core::TurnInputProjectionSource::AutomationContext),
        user_input_snapshot: None,
        correlation_id: Some(pending.correlation_id.clone()),
        turn_settings: Default::default(),
    };
    assert!(store
        .transition_workspace_system_notice(
            &onboarding_id,
            "pending",
            "injecting",
            None,
            None,
            created.created_at + 1,
        )
        .unwrap());
    assert!(matches!(
        manager.send_turn(&created.agent_id, forged.clone()).await,
        Err(runtime_core::RuntimeError::OnboardingPending(_))
    ));
    assert!(store
        .transition_workspace_system_notice(
            &onboarding_id,
            "injecting",
            "pending",
            None,
            None,
            created.created_at + 2,
        )
        .unwrap());
    // A notice requiring operator review also blocks later ordinary input;
    // requeueing the same durable notice does not invent another turn.
    assert!(store
        .transition_workspace_system_notice(
            &onboarding_id,
            "pending",
            "manual_review",
            None,
            Some("missing_attachment"),
            created.created_at + 1,
        )
        .unwrap());
    assert!(matches!(
        manager.send_turn(&created.agent_id, forged).await,
        Err(runtime_core::RuntimeError::OnboardingPending(_))
    ));
    assert!(store
        .transition_workspace_system_notice(
            &onboarding_id,
            "manual_review",
            "pending",
            None,
            None,
            created.created_at + 2,
        )
        .unwrap());
    assert!(store.list_turn_admissions().unwrap().is_empty());
    assert_eq!(
        manager.dispatch_workspace_system_notices().await.unwrap(),
        1
    );
    let notice = manager
        .get_workspace_onboarding_notice(&workspace.workspace_id, &created.agent_id)
        .unwrap()
        .unwrap();
    assert!(
        manager
            .get_workspace_onboarding_notice("workspace_not_the_owner", &created.agent_id,)
            .is_err(),
        "workspace-scoped read cannot escape membership authority"
    );
    assert_eq!(notice.state, "delivered");
    assert!(notice.turn_id.is_some());
    assert!(notice.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("Inspect the API for regressions"));
    let admissions = store.list_turn_admissions().expect("durable admissions");
    let admission = admissions
        .iter()
        .find(|row| {
            row.correlation.correlation_id.as_deref() == Some(notice.correlation_id.as_str())
        })
        .expect("onboarding admission");
    assert_eq!(
        admission.projection_source,
        runtime_core::TurnInputProjectionSource::AutomationContext
    );
    assert_eq!(
        manager.dispatch_workspace_system_notices().await.unwrap(),
        0
    );
    assert_eq!(
        store.list_turn_admissions().unwrap().len(),
        admissions.len()
    );
}

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
        "metadata": {"purpose":"workspace_agent_route"}
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
    let onboarding_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/v2/workspaces/{}/agents/{}/onboarding",
                    workspace.workspace_id, created.agent_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("onboarding request"),
        )
        .await
        .expect("onboarding response");
    assert_eq!(onboarding_response.status(), StatusCode::OK);
    let onboarding: runtime_core::WorkspaceSystemNoticeRecord = serde_json::from_slice(
        &to_bytes(onboarding_response.into_body(), usize::MAX)
            .await
            .expect("onboarding body"),
    )
    .expect("onboarding json");
    assert_eq!(
        onboarding.state, "pending",
        "creation commits intent, not provider dispatch"
    );
    assert_eq!(onboarding.workspace_id, workspace.workspace_id);
    assert_eq!(onboarding.recipient_agent_id, created.agent_id);
    assert!(onboarding.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("no team lead"));
    assert!(onboarding.input[0]["text"]
        .as_str()
        .unwrap()
        .contains(created.alias.as_str()));
    assert_eq!(
        created.recreation_policy.setting_sources_intent,
        runtime_core::ProviderSettingSourcesIntent::Explicit {
            sources: vec![
                runtime_core::ProviderSettingSource::User,
                runtime_core::ProviderSettingSource::Project,
                runtime_core::ProviderSettingSource::Local,
            ],
        }
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
    let global_archived = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/agents/{}", archived.agent_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("global archived lookup"),
        )
        .await
        .expect("global archived response");
    assert_eq!(global_archived.status(), StatusCode::OK);
    assert_eq!(
        global_archived.headers()[header::ETAG].to_str().unwrap(),
        format!("\"{}\"", archived.revision)
    );
    let globally_resolved: runtime_core::WorkspaceAgentRecord = serde_json::from_slice(
        &to_bytes(global_archived.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .expect("global archived agent");
    assert_eq!(globally_resolved, archived);
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

    // Archival may cancel a queued notice before it ever enters turn
    // admission. Restoration must requeue exactly that immutable onboarding
    // rather than leaving the agent unable to accept its first model turn.
    assert_eq!(
        restarted.dispatch_workspace_system_notices().await.unwrap(),
        1
    );
    let onboarding_id = format!("workspace-onboarding:{}", created.agent_id);
    let notice_before_restore = store
        .get_workspace_system_notice(&onboarding_id)
        .unwrap()
        .unwrap();
    assert_eq!(notice_before_restore.state, "cancelled");
    assert!(notice_before_restore.turn_id.is_none());
    assert_eq!(
        notice_before_restore.last_error.as_deref(),
        Some("recipient_archived")
    );

    let restore_uri = format!(
        "/v2/workspaces/{}/agents/{}/restore",
        workspace.workspace_id, created.agent_id
    );
    let invalid_restore = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&restore_uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"force":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_restore.status(), StatusCode::BAD_REQUEST);
    let invalid: serde_json::Value = serde_json::from_slice(
        &to_bytes(invalid_restore.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(invalid["error"]["code"], "invalid_request");
    assert_eq!(
        store
            .get_workspace_system_notice(&onboarding_id)
            .unwrap()
            .unwrap()
            .state,
        "cancelled",
        "ignored restore options must not reinstate onboarding"
    );

    let restore_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&restore_uri)
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
    let restored_notice = store
        .get_workspace_system_notice(&onboarding_id)
        .unwrap()
        .unwrap();
    assert_eq!(restored_notice.state, "pending");
    assert_eq!(restored_notice.notice_id, notice_before_restore.notice_id);
    assert_eq!(restored_notice.input, notice_before_restore.input);
    assert!(
        !store
            .transition_workspace_system_notice(
                &onboarding_id,
                "pending",
                "cancelled",
                None,
                Some("recipient_archived"),
                restored.created_at + 3,
            )
            .unwrap(),
        "a stale worker must not cancel restored onboarding"
    );
    assert_eq!(
        restarted.dispatch_workspace_system_notices().await.unwrap(),
        1
    );
    let delivered_notice = store
        .get_workspace_system_notice(&onboarding_id)
        .unwrap()
        .unwrap();
    assert_eq!(delivered_notice.state, "delivered");
    assert_eq!(
        restarted.dispatch_workspace_system_notices().await.unwrap(),
        0,
        "the original onboarding must be admitted at most once"
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

#[tokio::test]
async fn workspace_agent_keyset_cursor_must_match_workspace_lifecycle_and_exact_timestamp() {
    let (router, token, temp) = build_test_router().await;
    let root_a = temp.path().join("cursor-workspace-a");
    let root_b = temp.path().join("cursor-workspace-b");
    fs::create_dir_all(&root_a).unwrap();
    fs::create_dir_all(&root_b).unwrap();
    let first_workspace = register_workspace(&router, &token, &root_a).await;
    let second_workspace = register_workspace(&router, &token, &root_b).await;

    let mut records = Vec::new();
    for (workspace_id, title) in [
        (&first_workspace.workspace_id, "First"),
        (&first_workspace.workspace_id, "Second"),
        (&second_workspace.workspace_id, "Foreign"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v2/workspaces/{workspace_id}/agents"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "provider":"codex", "model":"test-model", "title":title
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let record: runtime_core::WorkspaceAgentRecord =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        records.push(record);
    }

    let uri = format!(
        "/v2/workspaces/{}/agents?lifecycle=active&limit=1",
        first_workspace.workspace_id
    );
    let get = |url: &str| {
        Request::builder()
            .uri(url)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let first = router.clone().oneshot(get(&uri)).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let next = first.headers()["x-next-cursor"]
        .to_str()
        .unwrap()
        .to_string();
    let first_page: Vec<runtime_core::WorkspaceAgentRecord> =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(first_page.len(), 1);
    assert_eq!(
        next,
        format!("{}:{}", first_page[0].created_at, first_page[0].agent_id)
    );
    let second = router
        .clone()
        .oneshot(get(&format!("{uri}&cursor={next}")))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert!(second.headers().get("x-next-cursor").is_none());
    let second_page: Vec<runtime_core::WorkspaceAgentRecord> =
        serde_json::from_slice(&to_bytes(second.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(second_page.len(), 1);
    assert_ne!(first_page[0].agent_id, second_page[0].agent_id);
    assert_eq!(
        [
            first_page[0].agent_id.as_str(),
            second_page[0].agent_id.as_str()
        ]
        .into_iter()
        .collect::<std::collections::HashSet<_>>(),
        records[..2]
            .iter()
            .map(|row| row.agent_id.as_str())
            .collect::<std::collections::HashSet<_>>()
    );

    for bad_uri in [
        format!(
            "{uri}&cursor={}:{}",
            records[2].created_at, records[2].agent_id
        ),
        format!(
            "{uri}&cursor={}:{}",
            first_page[0].created_at + 1,
            first_page[0].agent_id
        ),
        format!(
            "/v2/workspaces/{}/agents?lifecycle=archived&cursor={next}",
            first_workspace.workspace_id
        ),
    ] {
        let response = router.clone().oneshot(get(&bad_uri)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bad_uri}");
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["error"]["code"], "invalid_state", "{bad_uri}");
    }
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
