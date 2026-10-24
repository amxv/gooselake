use super::workspace_worktree_routes::call;
use super::*;

async fn await_native_idle(router: &Router, token: &str, workspace_id: &str, agent_id: &str) {
    timeout(Duration::from_secs(150), async {
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            let (status, notice) = call(
                router,
                token,
                "GET",
                &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding"),
                None,
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{notice}");
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
            if notice["state"] == "delivered" && session["active_turn_id"].is_null() {
                assert_eq!(session["status"], "ready", "{session}");
                break;
            }
            assert!(
                matches!(
                    notice["state"].as_str(),
                    Some("pending" | "injecting" | "delivered")
                ),
                "{notice}"
            );
            tick.tick().await;
        }
    })
    .await
    .expect("native Codex onboarding/turn did not become idle");
}

async fn retire_native(
    router: &Router,
    token: &str,
    workspace_id: &str,
    key: &str,
) -> serde_json::Value {
    let (status, workspace) = call(
        router,
        token,
        "GET",
        &format!("/v2/workspaces/{workspace_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{workspace}");
    let input = serde_json::json!({"expected_revision":workspace["revision"]});
    timeout(Duration::from_secs(45), async {
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            let (status, operation) = call(
                router,
                token,
                "POST",
                &format!("/v2/workspaces/{workspace_id}/retirement/advance"),
                Some(input.clone()),
                Some(key),
            )
            .await;
            assert!(
                matches!(status, StatusCode::OK | StatusCode::ACCEPTED),
                "{status}: {operation}"
            );
            match operation["operation"]["phase"].as_str() {
                Some("completed") => {
                    assert_eq!(status, StatusCode::OK);
                    let effects = operation["effects"].as_array().expect("retirement effects");
                    let close = effects
                        .iter()
                        .find(|effect| effect["effect_kind"] == "workspace_provider_close")
                        .expect("native close receipt");
                    assert_eq!(close["phase"], "finalized");
                    assert_eq!(close["evidence"]["verified"], true);
                    assert_eq!(close["evidence"]["proof_kind"], "native_process_terminated");
                    return operation;
                }
                Some("requested") => tick.tick().await,
                _ => panic!("real native retirement did not settle: {operation}"),
            };
        }
    })
    .await
    .expect("real native retirement exceeded its bounded close wait")
}

#[tokio::test]
#[ignore = "authenticated Codex app-server and disposable staged auth required"]
async fn live_codex_workspace_retirement_confirms_native_close_and_preserves_identity() {
    let home = PathBuf::from(std::env::var("HOME").expect("HOME"));
    assert!(
        home.join(".gg/codex/auth.json").is_file(),
        "Codex auth source missing"
    );
    let temp = tempfile::tempdir().expect("disposable live root");
    let root = temp.path().join("retirement-workspace");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("retained.txt"),
        "workspace data must survive retirement\n",
    )
    .unwrap();
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp.path().to_path_buf();
    config.providers.claude.enabled = false;
    config.providers.codex.enabled = true;
    prepare_codex_smoke_config(&config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.server.public_base_url = format!("http://{}", listener.local_addr().unwrap());
    let bootstrapped = bootstrap_runtime(config.clone())
        .await
        .expect("native retirement bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });
    let gateway = tokio::spawn({
        let router = router.clone();
        async move {
            axum::serve(listener, router)
                .await
                .expect("serve native test gateway");
        }
    });
    let (status, registered) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Native retirement"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{registered}");
    let workspace_id = registered["workspace"]["workspace_id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(
            serde_json::json!({"provider":"codex","model":codex_test_model(),
            "current_preferences":{"thinking_effort":"low"}}),
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent}");
    let agent_id = agent["agent_id"].as_str().unwrap().to_string();
    assert_eq!(agent["recreation_policy"]["model"], "gpt-6-luna");
    assert_eq!(
        agent["recreation_policy"]["current_preferences"]["thinking_effort"],
        "low"
    );
    await_native_idle(&router, &token, &workspace_id, &agent_id).await;
    let first = retire_native(&router, &token, &workspace_id, "native-retirement-first").await;
    assert_eq!(
        first["operation"]["exact_terminal_result"]["workspace"]["lifecycle_state"],
        "retired"
    );
    assert!(root.join("retained.txt").is_file());
    let (status, reactivated) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Native retirement"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactivated}");
    assert_eq!(reactivated["workspace"]["workspace_id"], workspace_id);
    assert!(reactivated["workspace"]["lead_agent_id"].is_null());
    let (status, archived) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(archived["lifecycle_state"], "archived");
    let (status, restored) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/restore"),
        Some(serde_json::json!({})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    assert_eq!(restored["agent_id"], agent_id);
    assert_eq!(restored["recreation_policy"]["model"], "gpt-6-luna");
    let (status, turn) = call(&router, &token, "POST", &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"Reply with exactly retirement_resume_ok and do not call tools."}],
            "turn_settings":{"thinking_effort":"low"}})), None).await;
    assert_eq!(status, StatusCode::OK, "{turn}");
    await_native_idle(&router, &token, &workspace_id, &agent_id).await;
    let second = retire_native(&router, &token, &workspace_id, "native-retirement-restored").await;
    assert_ne!(
        first["operation"]["operation_id"],
        second["operation"]["operation_id"]
    );
    assert!(root.join("retained.txt").is_file());
    gateway.abort();
    let _ = gateway.await;
}
