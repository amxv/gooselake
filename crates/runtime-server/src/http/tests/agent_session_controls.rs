use super::workspace_worktree_routes::call;
use super::*;

#[tokio::test]
async fn public_agent_session_controls_are_authenticated_revisioned_and_capability_gated() {
    let (router, token, temp, runtime) = build_test_router_with_runtime().await;
    let root = temp.path().join("agent-session-controls");
    std::fs::create_dir(&root).expect("workspace root");
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Controls Test"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let agent_id = agent["agent_id"].as_str().expect("agent id");
    finish_test_onboarding(&runtime, workspace_id, agent_id).await;
    let revision = agent["revision"].as_u64().expect("agent revision");
    let prefix = format!("/v2/agents/{agent_id}");

    let (status, _) = call(
        &router, &token, "POST",
        &format!("{prefix}/turns/nonexistent/edit-rerun"),
        Some(serde_json::json!({"edited_snapshot":{"prompt_text":"new request","image_refs":[],"invocation":null}})),
        None,
    ).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unsupported native fork is capability-gated"
    );
    let (status, _) = call(
        &router,
        &token,
        "GET",
        &format!("{prefix}/edits/nonexistent"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, empty_context) = call(
        &router,
        &token,
        "GET",
        &format!("{prefix}/context-limit"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        empty_context.is_null(),
        "missing provider evidence is JSON null"
    );

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{prefix}/context-limit/refresh"),
        None,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "missing provider capability must fail explicitly"
    );

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{prefix}/preferences"),
        Some(serde_json::json!({"expected_revision":revision+1,
            "current_preferences":{"thinking_effort":"high"}})),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "stale revision must fail before provider mutation"
    );

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{prefix}/preferences"),
        Some(serde_json::json!({"expected_revision":revision,
            "current_preferences":{"thinking_effort":"high"}})),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "test provider lacks native thinking mutation"
    );

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{prefix}/permission"),
        Some(serde_json::json!({"expected_revision":revision,
            "permission_intent":{"kind":"provider_default"}})),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "Codex permission mutations are unsupported"
    );

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{prefix}/compact"),
        None,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unsupported provider compaction cannot be admitted"
    );
    let (status, _) = call(
        &router,
        &token,
        "GET",
        &format!("{prefix}/compactions/nonexistent"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{prefix}/turns"),
        Some(serde_json::json!({
            "input": [{"type":"text","text":"hello"}],
            "turn_settings": {"thinking_effort":"high"}
        })),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unsupported turn override rejected before admission"
    );

    let (status, accepted) = call(
        &router,
        &token,
        "POST",
        &format!("{prefix}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"normal"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(accepted["turn_id"].as_str().is_some());

    let unauthenticated = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("{prefix}/context-limit"))
                .body(Body::empty())
                .expect("unauthenticated request"),
        )
        .await
        .expect("unauthenticated response");
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let (status, unchanged) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(unchanged["revision"], revision);
}

#[tokio::test]
async fn thinking_selected_on_a_turn_is_revisioned_and_inherited_on_later_turns() {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    provider.enable_thinking_mutation();
    let root = temp.path().join("turn-thinking-current");
    std::fs::create_dir(&root).expect("workspace root");
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Thinking"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let agent_id = agent["agent_id"].as_str().unwrap();
    finish_test_onboarding(&runtime, workspace_id, agent_id).await;
    let initial_revision = agent["revision"].as_u64().unwrap();
    let turns_uri = format!("/v2/agents/{agent_id}/turns");

    let (status, selected) = call(
        &router,
        &token,
        "POST",
        &turns_uri,
        Some(serde_json::json!({
            "input":[{"type":"text","text":"select high"}],
            "turn_settings":{"thinking_effort":"high","codex_collaboration_mode":"plan"}
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first turn must admit");
    assert!(selected["turn_id"].is_string());
    let (status, updated) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["revision"], initial_revision + 1);
    assert_eq!(
        updated["recreation_policy"]["current_preferences"]["thinking_effort"],
        "high"
    );

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (status, row) = call(
                &router,
                &token,
                "GET",
                &format!("/v1/sessions/{agent_id}"),
                None,
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            if row["active_turn_id"].is_null() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first turn should complete");

    let (status, inherited) = call(
        &router,
        &token,
        "POST",
        &turns_uri,
        Some(serde_json::json!({"input":[{"type":"text","text":"inherit effort"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(inherited["turn_id"].is_string());
    assert_eq!(
        provider
            .observed_thinking_efforts(agent_id)
            .await
            .into_iter()
            .skip(1)
            .collect::<Vec<_>>(),
        vec![
            Some(runtime_core::ProviderThinkingEffort::High),
            Some(runtime_core::ProviderThinkingEffort::High),
        ]
    );
    let (status, unchanged) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        unchanged["revision"],
        initial_revision + 1,
        "inheriting thinking must not mutate current preference revision"
    );
}
