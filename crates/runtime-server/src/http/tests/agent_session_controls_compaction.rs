use super::workspace_worktree_routes::call;
use super::*;

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

    provider.emit_compaction_event(agent_id, "completed");
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
