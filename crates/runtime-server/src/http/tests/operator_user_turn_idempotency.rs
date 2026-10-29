use super::workspace_worktree_routes::call;
use super::*;

async fn fixture() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    Arc<RuntimeSessionManager>,
    String,
    String,
) {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    let root = temp.path().join("operator-turn-workspace");
    std::fs::create_dir_all(&root).unwrap();
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Turn Replay"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workspace_id = workspace["workspace"]["workspace_id"]
        .as_str()
        .unwrap()
        .to_owned();
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
    let agent_id = agent["agent_id"].as_str().unwrap().to_owned();
    finish_test_onboarding(&runtime, &workspace_id, &agent_id).await;
    (
        router,
        token,
        temp,
        provider,
        runtime,
        workspace_id,
        agent_id,
    )
}

fn prompt_count(provider: &TestProvider) -> usize {
    provider
        .send_invocations
        .load(std::sync::atomic::Ordering::SeqCst)
}

async fn post_with_headers(
    router: &Router,
    token: &str,
    uri: &str,
    key: &str,
    input: &serde_json::Value,
) -> (StatusCode, Option<String>, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", key)
                .body(Body::from(input.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let operation_id = response
        .headers()
        .get("x-operation-id")
        .map(|value| value.to_str().unwrap().to_owned());
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, operation_id, body)
}

#[tokio::test]
async fn keyed_user_turn_replays_exact_logical_admission_after_completion_and_archive() {
    let (router, token, _temp, provider, runtime, workspace_id, agent_id) = fixture().await;
    let uri = format!("/v2/agents/{agent_id}/turns");
    let input = serde_json::json!({"input":[{"type":"text","text":"run once"}]});
    let first_count = prompt_count(&provider);
    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "once-user-turn")
                .body(Body::from(input.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(prompt_count(&provider), first_count);

    let (status, operation_id, first) =
        post_with_headers(&router, &token, &uri, "once-user-turn", &input).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["session_id"], agent_id);
    assert_eq!(first["status"], "in_progress");
    assert!(first["turn_id"].as_str().unwrap().starts_with("turn_"));
    let operation_id = operation_id.expect("durable operation response header");
    assert!(operation_id.starts_with("op_turn_"));
    assert_eq!(prompt_count(&provider), first_count + 1);

    timeout(Duration::from_secs(5), async {
        loop {
            let session = runtime.get_session(&agent_id).await.unwrap();
            if session.active_turn_id.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("logical turn must complete before lost-response retry");
    let (status, replay_operation, replay) =
        post_with_headers(&router, &token, &uri, "once-user-turn", &input).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay_operation.as_deref(), Some(operation_id.as_str()));
    assert_eq!(replay, first);
    assert_eq!(prompt_count(&provider), first_count + 1);

    let (status, changed) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({"input":[{"type":"text","text":"changed payload"}]})),
        Some("once-user-turn"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{changed}");
    assert_eq!(changed["error"]["code"], "conflict");

    let (status, different_agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let different_agent_id = different_agent["agent_id"].as_str().unwrap();
    let (status, scoped) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{different_agent_id}/turns"),
        Some(input.clone()),
        Some("once-user-turn"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{scoped}");
    assert_eq!(prompt_count(&provider), first_count + 1);

    let (status, archived) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"),
        Some(serde_json::json!({"reason":"user-turn-replay-test"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    let (status, replay_operation, replay) =
        post_with_headers(&router, &token, &uri, "once-user-turn", &input).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay_operation.as_deref(), Some(operation_id.as_str()));
    assert_eq!(replay, first);
    assert_eq!(prompt_count(&provider), first_count + 1);
    let (status, operation) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(operation["operation"]["phase"], "completed");
    assert_eq!(operation["effects"][0]["phase"], "finalized");
    assert!(operation["claims"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn unknown_provider_user_turn_is_nonretryable_and_fences_every_new_key() {
    let (router, token, _temp, provider, _runtime, _workspace_id, agent_id) = fixture().await;
    let uri = format!("/v2/agents/{agent_id}/turns");
    let input = serde_json::json!({"input":[{"type":"text","text":"might have run"}]});
    provider
        .send_failure_mode
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let first_count = prompt_count(&provider);
    let (status, operation_id, result) =
        post_with_headers(&router, &token, &uri, "unknown-user-turn", &input).await;
    assert_eq!(status, StatusCode::CONFLICT, "{result}");
    assert_eq!(result["error"]["code"], "outcome_unresolved");
    assert_eq!(result["error"]["retryable"], false);
    let operation_id = operation_id.expect("unknown native effect durable operation");
    assert_eq!(result["error"]["details"]["operation_id"], operation_id);
    assert_eq!(prompt_count(&provider), first_count + 1);

    let (status, repeated) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(input.clone()),
        Some("unknown-user-turn"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{repeated}");
    assert_eq!(repeated["error"]["code"], "outcome_unresolved");
    assert_eq!(repeated["error"]["details"]["operation_id"], operation_id);
    let (status, another_key) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(input.clone()),
        Some("bypass-unknown-dispatch"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{another_key}");
    let (status, unkeyed) = call(&router, &token, "POST", &uri, Some(input), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{unkeyed}");
    assert_eq!(unkeyed["error"]["code"], "outcome_unresolved");
    assert_eq!(unkeyed["error"]["details"]["operation_id"], operation_id);
    assert_eq!(prompt_count(&provider), first_count + 1);
    let (status, operation) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(operation["operation"]["phase"], "manual_review");
    assert_eq!(operation["effects"][0]["phase"], "uncertain");
    assert_eq!(operation["claims"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn proven_not_dispatched_user_turn_replays_original_error_but_allows_new_key() {
    let (router, token, _temp, provider, runtime, _workspace_id, agent_id) = fixture().await;
    let uri = format!("/v2/agents/{agent_id}/turns");
    let input = serde_json::json!({"input":[{"type":"text","text":"capacity test"}]});
    provider
        .send_failure_mode
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let first_count = prompt_count(&provider);
    let (status, operation_id, first) =
        post_with_headers(&router, &token, &uri, "capacity-key", &input).await;
    assert_eq!(status, StatusCode::CONFLICT, "{first}");
    assert_eq!(first["error"]["code"], "capacity_rejected");
    assert_eq!(
        first["error"]["details"]["dispatch_outcome"],
        "not_dispatched"
    );
    let operation_id = operation_id.expect("durable not-dispatch receipt");
    assert_eq!(first["error"]["details"]["operation_id"], operation_id);
    assert_eq!(prompt_count(&provider), first_count + 1);
    provider
        .send_failure_mode
        .store(0, std::sync::atomic::Ordering::SeqCst);

    let (status, replay_operation, repeated) =
        post_with_headers(&router, &token, &uri, "capacity-key", &input).await;
    assert_eq!(status, StatusCode::CONFLICT, "{repeated}");
    assert_eq!(replay_operation.as_deref(), Some(operation_id.as_str()));
    assert_eq!(repeated["error"]["code"], "capacity_rejected");
    assert_eq!(repeated["error"]["details"]["operation_id"], operation_id);
    assert_eq!(prompt_count(&provider), first_count + 1);
    assert!(runtime
        .get_session(&agent_id)
        .await
        .unwrap()
        .active_turn_id
        .is_none());
    let (status, operation) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(operation["operation"]["phase"], "failed");
    assert!(operation["claims"].as_array().unwrap().is_empty());

    let (status, newer) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(input),
        Some("new-proven-safe-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{newer}");
    assert_eq!(prompt_count(&provider), first_count + 2);
    assert_ne!(newer["turn_id"], operation["effects"][0]["target_id"]);
}

#[tokio::test]
async fn keyed_turn_rejects_unjournaled_thinking_preference_mutation_before_native_work() {
    let (router, token, _temp, provider, _runtime, _workspace_id, agent_id) = fixture().await;
    let count = prompt_count(&provider);
    let (status, response) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({
            "input":[{"type":"text","text":"do not mutate thinking early"}],
            "turn_settings":{"thinking_effort":"high"},
        })),
        Some("not-allowed-to-mutate-thinking"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(response["error"]["code"], "unsupported_capability");
    assert_eq!(prompt_count(&provider), count);
}
