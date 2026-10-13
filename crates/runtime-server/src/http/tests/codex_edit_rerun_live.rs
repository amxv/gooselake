use super::workspace_worktree_routes::call;
use super::*;

async fn await_idle(router: &Router, token: &str, agent_id: &str) {
    timeout(Duration::from_secs(150), async {
        loop {
            let (status, session) = call(
                router,
                token,
                "GET",
                &format!("/v1/sessions/{agent_id}"),
                None,
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{session}");
            if session["active_turn_id"].is_null() {
                assert_eq!(session["status"], "ready", "{session}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("real Codex edited turn failed to become idle");
}

async fn send_and_finish(router: &Router, token: &str, agent_id: &str, prompt: &str) -> String {
    let (status, response) = call(
        router,
        token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":prompt}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let turn_id = response["turn_id"]
        .as_str()
        .expect("admitted turn ID")
        .to_string();
    await_idle(router, token, agent_id).await;
    let (status, events) = call(
        router,
        token,
        "GET",
        &format!("/v1/sessions/{agent_id}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{events}");
    assert!(
        events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| { event["turn_id"] == turn_id && event["kind"] == "turn.completed" }),
        "real Codex logical turn did not complete: {events}"
    );
    turn_id
}

#[tokio::test]
#[ignore = "authenticated Codex app-server and disposable staged auth required"]
async fn live_workspace_historical_edit_rerun_forks_resends_and_resumes_exact_child() {
    let home = std::path::PathBuf::from(std::env::var("HOME").expect("HOME"));
    assert!(
        home.join(".gg/codex/auth.json").is_file(),
        "staged Codex auth source missing"
    );
    let temp = tempfile::tempdir().expect("disposable test root");
    let workspace_root = temp.path().join("edit-workspace");
    std::fs::create_dir_all(&workspace_root).expect("workspace root");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp.path().to_path_buf();
    config.providers.claude.enabled = false;
    config.providers.codex.enabled = true;

    let bootstrapped = bootstrap_runtime(config.clone())
        .await
        .expect("Codex runtime bootstrap");
    let codex_provider = bootstrapped
        .app
        .provider_registry
        .get(runtime_core::ProviderKind::Codex)
        .expect("Codex provider");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":workspace_root,"display_name":"Live fork"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{workspace}");
    let workspace_id = workspace["workspace"]["workspace_id"]
        .as_str()
        .expect("workspace ID");
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(serde_json::json!({"provider":"codex","model":codex_test_model()})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent}");
    let agent_id = agent["agent_id"].as_str().expect("agent ID").to_string();

    let original = send_and_finish(
        &router,
        &token,
        &agent_id,
        "Reply with precisely the word live_original_317, with no other text.",
    )
    .await;
    let superseded = send_and_finish(
        &router,
        &token,
        &agent_id,
        "Reply with precisely the word live_superseded_318, with no other text.",
    )
    .await;
    let (status, before) = call(
        &router,
        &token,
        "GET",
        &format!("/v1/sessions/{agent_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let original_ref = before["provider_session_ref"]
        .as_str()
        .expect("native source ID");

    let edit_body = serde_json::json!({"edited_snapshot":{
        "prompt_text":"Reply with precisely the word live_edited_319, with no other text.",
        "image_refs":[],"invocation":null,
    }});
    let edit_uri = format!("/v2/agents/{agent_id}/turns/{original}/edit-rerun");
    let (status, edited) = call(
        &router,
        &token,
        "POST",
        &edit_uri,
        Some(edit_body.clone()),
        Some("real-native-edit"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["phase"], "completed");
    let edited_turn = edited["replacement_turn_id"]
        .as_str()
        .expect("replacement turn ID");
    assert_ne!(edited_turn, original);
    assert_ne!(edited_turn, superseded);
    let child = edited["child_provider_session_ref"]
        .as_str()
        .expect("verified native child");
    assert_ne!(
        original_ref, child,
        "native provider attachment must change"
    );
    await_idle(&router, &token, &agent_id).await;

    let (status, after) = call(
        &router,
        &token,
        "GET",
        &format!("/v1/sessions/{agent_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["provider_session_ref"], child);
    let projection = after["metadata"]["session_transcript"].to_string();
    assert!(
        projection.contains("live_edited_319"),
        "edited transcript: {projection}"
    );
    assert!(
        !projection.contains("live_original_317"),
        "original was superseded: {projection}"
    );
    assert!(
        !projection.contains("live_superseded_318"),
        "later was superseded: {projection}"
    );
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &edit_uri,
        Some(edit_body),
        Some("real-native-edit"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["operation_id"], edited["operation_id"]);
    assert_eq!(replay["replacement_turn_id"], edited_turn);

    codex_provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: agent_id.clone(),
            reason: Some("live_edit_test_restart_detach".into()),
        })
        .await
        .expect("detach before restart");
    drop(codex_provider);
    drop(router);
    let restarted = bootstrap_runtime(config)
        .await
        .expect("restart with edited child");
    let restarted_token = restarted.auth.bearer_token.clone();
    let restarted_router = build_router(AppState {
        app: restarted.app,
        runtime: restarted.runtime,
        bearer_token: restarted_token.clone(),
        public_base_url: restarted.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });
    let (status, resumed) = call(
        &restarted_router,
        &restarted_token,
        "GET",
        &format!("/v1/sessions/{agent_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resumed["provider_session_ref"], child);
    send_and_finish(
        &restarted_router,
        &restarted_token,
        &agent_id,
        "Reply with precisely the word live_resumed_320, with no other text.",
    )
    .await;
}
