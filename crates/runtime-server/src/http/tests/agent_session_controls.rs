use super::workspace_worktree_routes::call;
use super::*;

#[tokio::test]
async fn public_agent_session_controls_are_authenticated_revisioned_and_capability_gated() {
    let (router, token, temp) = build_test_router().await;
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
    let revision = agent["revision"].as_u64().expect("agent revision");
    let prefix = format!("/v2/agents/{agent_id}");

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
