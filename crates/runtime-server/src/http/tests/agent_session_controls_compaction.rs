use super::workspace_worktree_routes::call;
use super::*;

#[tokio::test]
async fn native_auto_compaction_is_durable_without_a_manual_api_request() {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    let root = temp.path().join("auto-compaction");
    std::fs::create_dir(&root).unwrap();
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Automatic Compact"})),
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
    let mut events = runtime.subscribe_events();
    runtime.start_provider_compaction_observers();
    provider
        .emit_compaction_event_with_trigger(agent_id, "completed", "auto")
        .await;
    let run_id = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.unwrap();
            if event.kind == "compaction.lifecycle_changed" && event.scope_id == agent_id {
                assert_eq!(event.payload["phase"], "completed");
                assert_eq!(event.payload["source"], "provider");
                break event.payload["run_id"].as_str().unwrap().to_owned();
            }
        }
    })
    .await
    .expect("native unsolicited compaction should reach durable manager");
    let (status, recorded) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/agents/{agent_id}/compactions/{run_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recorded["source"], "provider");
    assert_eq!(recorded["phase"], "completed");
    assert_eq!(recorded["outcome"], serde_json::Value::Null);
    assert_eq!(recorded["trigger"], "auto");
    assert_eq!(
        provider.compaction_calls(),
        0,
        "no manual native compact RPC"
    );
}

#[tokio::test]
async fn manual_compaction_is_admitted_once_observed_and_never_replayed_ambiguously() {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    provider.set_compaction_mode(1);
    let root = temp.path().join("manual-compaction");
    std::fs::create_dir(&root).expect("workspace root");
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Compact Test"})),
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
    let compact_uri = format!("/v2/agents/{agent_id}/compact");

    let (status, accepted) = call(
        &router,
        &token,
        "POST",
        &compact_uri,
        None,
        Some("same-request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(accepted["phase"], "accepted");
    let run_id = accepted["run_id"].as_str().unwrap();
    assert_eq!(provider.compaction_calls(), 1);
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &compact_uri,
        None,
        Some("same-request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["run_id"], run_id);
    assert_eq!(
        provider.compaction_calls(),
        1,
        "replay cannot invoke native provider"
    );
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &compact_uri,
        None,
        Some("new-request"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "singleflight is durable");
    assert_eq!(provider.compaction_calls(), 1);

    provider.emit_compaction_event(agent_id, "completed").await;
    let uri = format!("/v2/agents/{agent_id}/compactions/{run_id}");
    let confirmed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (status, record) = call(&router, &token, "GET", &uri, None, None).await;
            assert_eq!(status, StatusCode::OK);
            if record["phase"] == "completed" {
                break record;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native completion must unblock agent");
    assert_eq!(confirmed["outcome"], "accepted");

    provider.set_compaction_mode(2);
    let (status, no_compact) =
        call(&router, &token, "POST", &compact_uri, None, Some("no-op")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(no_compact["phase"], "not_performed");
    assert_eq!(provider.compaction_calls(), 2);

    provider.set_compaction_mode(3);
    let (status, _) = call(&router, &token, "POST", &compact_uri, None, Some("unknown")).await;
    assert_ne!(status, StatusCode::OK);
    let (status, review) = call(&router, &token, "POST", &compact_uri, None, Some("unknown")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(review["phase"], "manual_review");
    assert_eq!(provider.compaction_calls(), 3);
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"do not dispatch"}]})),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unknown native outcome must fence new work"
    );
    let review_uri = format!(
        "/v2/agents/{agent_id}/compactions/{}/resolve",
        review["run_id"].as_str().unwrap()
    );
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(serde_json::json!({"phase":"completed","evidence":"no"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, settled) = call(
        &router, &token, "POST", &review_uri,
        Some(serde_json::json!({"phase":"failed","evidence":"Operator verified native compaction did not complete"})), None,
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(settled["phase"], "failed");
    assert!(settled["review_evidence"].as_str().unwrap().len() > 8);
    assert_eq!(provider.compaction_calls(), 3, "review must not redispatch");
    let (status, _) = call(
        &router, &token, "POST", &review_uri,
        Some(serde_json::json!({"phase":"failed","evidence":"Operator verified native compaction did not complete"})), None,
    ).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, next) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"safe now"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(next["turn_id"].as_str().is_some());
}
