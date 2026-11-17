use super::*;

#[tokio::test]
async fn mixed_provider_team_flow_uses_shared_runtime_services() {
    let (router, token, _temp_dir, _acp_provider, _claude, _runtime, _app, history) =
        build_mixed_provider_test_router_with_history(|_| {
            vec![
                json!({"provider":"codex","model":"test-model","metadata":{}}),
                json!({"provider":"claude","model":"test-claude-model","metadata":{}}),
                json!({"provider":"acp","metadata":{}}),
            ]
        })
        .await;
    let codex_json = historical_session_json(&history[0]);
    let codex_session_id = codex_json["id"].as_str().expect("codex session id");

    let claude_json = historical_session_json(&history[1]);
    let claude_session_id = claude_json["id"].as_str().expect("claude session id");

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "mixed-provider-team",
                        "lead_agent_id": codex_session_id,
                        "member_agent_ids": []
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("create team body"),
    )
    .expect("create team json");
    let team_id = create_team_json["team"]["id"].as_str().expect("team id");

    let join_member_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/members"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "agent_id": claude_session_id,
                        "title": "Claude Teammate"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("join member");
    assert_eq!(join_member_response.status(), StatusCode::OK);

    let acp_json = historical_session_json(&history[2]);
    let acp_session_id = acp_json["id"].as_str().expect("acp session id");

    let join_acp_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/members"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "agent_id": acp_session_id,
                        "title": "ACP Teammate"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("join acp member");
    assert_eq!(join_acp_response.status(), StatusCode::OK);

    let send_direct_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/messages"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": codex_session_id,
                        "recipient_agent_id": claude_session_id,
                        "input": [{"type":"text","text":"hello mixed provider"}],
                        "priority": "normal",
                        "policy": "non_interrupting"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("send direct");
    assert_eq!(send_direct_response.status(), StatusCode::OK);
    let direct_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(send_direct_response.into_body(), usize::MAX)
            .await
            .expect("send direct body"),
    )
    .expect("send direct json");
    assert_eq!(
        direct_json["message"]["team_id"].as_str(),
        Some(team_id),
        "team comms should accept mixed-provider sender/recipient sessions"
    );

    let broadcast_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/broadcasts"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": acp_session_id,
                        "input": [{"type":"text","text":"hello whole team"}],
                        "priority": "normal",
                        "policy": "non_interrupting"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("broadcast");
    assert_eq!(broadcast_response.status(), StatusCode::OK);
    let broadcast_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(broadcast_response.into_body(), usize::MAX)
            .await
            .expect("broadcast body"),
    )
    .expect("broadcast json");
    assert_eq!(
        broadcast_json["message"]["team_id"].as_str(),
        Some(team_id),
        "team comms should accept ACP broadcasts without provider-specific branching"
    );
}

#[tokio::test]
async fn mcp_process_acp_session_can_use_runtime_process_gateway_and_ownership_rules() {
    let (router, token, _temp_dir, _acp_provider, _claude, _runtime, _app, history) =
        build_mixed_provider_test_router_with_history(|_| {
            vec![
                json!({"provider":"acp","metadata":{}}),
                json!({"provider":"codex","model":"test-model","metadata":{}}),
            ]
        })
        .await;
    let acp_json = historical_session_json(&history[0]);
    let acp_session_id = acp_json["id"].as_str().expect("acp session id").to_string();

    let codex_json = historical_session_json(&history[1]);
    let codex_session_id = codex_json["id"]
        .as_str()
        .expect("codex session id")
        .to_string();

    let invoke_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/mcp/invoke")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "namespace": "gg_process",
                        "tool_name": "gg_process_run",
                        "caller_agent_id": acp_session_id,
                        "invocation_id": "acp_inv_1",
                        "args": {
                            "command": "echo acp_mcp_process"
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("invoke");
    assert_eq!(invoke_response.status(), StatusCode::OK);
    let invoke_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(invoke_response.into_body(), usize::MAX)
            .await
            .expect("invoke body"),
    )
    .expect("invoke json");
    assert_eq!(invoke_json["ok"].as_bool(), Some(true));
    let process_id = invoke_json
        .pointer("/result/process/process_id")
        .and_then(serde_json::Value::as_str)
        .expect("process id")
        .to_string();

    let mut done = false;
    for _ in 0..80 {
        let get_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}?session_id={acp_session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("owner get process");
        assert_eq!(get_response.status(), StatusCode::OK);
        let process_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(get_response.into_body(), usize::MAX)
                .await
                .expect("get body"),
        )
        .expect("process json");
        let status = process_json
            .pointer("/process/status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(done, "ACP-owned process did not reach terminal state");

    let unauthorized_get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/processes/{process_id}?session_id={codex_session_id}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("unauthorized get");
    assert_eq!(unauthorized_get.status(), StatusCode::BAD_REQUEST);

    let unauthorized_logs = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/processes/{process_id}/logs?session_id={codex_session_id}&stream=stdout"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("unauthorized logs");
    assert_eq!(unauthorized_logs.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mcp_process_acp_provider_injects_gg_mcp_server_on_create_and_resume() {
    let fake_agent = fake_acp_agent_with_request_log();
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.server.public_base_url = "http://127.0.0.1:8787".to_string();
    config.providers.codex.enabled = false;
    config.providers.claude.enabled = false;
    config.providers.acp.enabled = true;
    config.providers.acp.command = Some("python3".to_string());
    config.providers.acp.args = vec![fake_agent.script_path.display().to_string()];

    let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let runtime = bootstrapped.runtime.clone();
    let app = bootstrapped.app.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let workspace_root = temp_dir.path().join("acp-create");
    std::fs::create_dir_all(&workspace_root).unwrap();
    let workspace = super::workspace_control_routes::register_control_workspace(
        &router,
        &token,
        &workspace_root,
        "ACP MCP lifecycle",
    )
    .await;
    let (status, created, _) = super::operator_agent_create::create_http(
        &router,
        &token,
        &workspace.workspace_id,
        json!({
            "provider":"acp", "title":"ACP MCP lifecycle",
            "setting_sources_intent":{"kind":"isolated"},
        }),
        Some("acp-mcp-create"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let session_id = created["agent_id"].as_str().unwrap().to_string();
    finish_test_onboarding(&runtime, &workspace.workspace_id, &session_id).await;
    let original = runtime.get_session(&session_id).await.unwrap();
    // Model process restart with a new adapter and registry. The original
    // adapter is still attached and correctly refuses an in-place rebind.
    // Keep the same durable identity and actual bootstrap MCP configuration;
    // the startup claim and receipt authorize this one native reattachment.
    let restarted_provider = Arc::new(runtime_provider_acp::AcpProvider::new(
        runtime_provider_acp::AcpProviderConfig {
            enabled: config.providers.acp.enabled,
            provider_dir: config.resolve_provider_dir("acp"),
            max_instances: config.providers.acp.max_instances,
            max_sessions_per_instance: config.providers.acp.max_sessions_per_instance,
            command: config.providers.acp.command.clone(),
            args: config.providers.acp.args.clone(),
            env: config.providers.acp.env.clone(),
            transport: config.providers.acp.transport.clone(),
            request_timeout_secs: config.providers.acp.request_timeout_secs,
            wait_timeout_secs: config.providers.acp.wait_timeout_secs,
            gg_mcp_enabled: true,
            gg_mcp_server_name: "gg".to_string(),
            gg_mcp_command: std::env::var("GG_MCP_SERVER_PATH").unwrap_or_else(|_| {
                runtime_provider_claude::standalone_gg_mcp_server_command_path()
                    .display()
                    .to_string()
            }),
            gg_mcp_args: Vec::new(),
            gg_mcp_enable_process_tools: config.processes.enabled,
            gg_mcp_gateway_url: Some(format!(
                "{}/v2/mcp",
                config.server.public_base_url.trim_end_matches('/')
            )),
            gg_mcp_gateway_token: Some(token.clone()),
        },
    ));
    let mut restarted_providers = runtime_core::ProviderRegistry::new();
    restarted_providers.register(restarted_provider).unwrap();
    let restarted = Arc::new(
        RuntimeSessionManager::new(
            app.services.store.clone(),
            Arc::new(restarted_providers),
            512,
        )
        .unwrap(),
    );
    let summary = restarted.recover_startup().await.unwrap();
    assert_eq!(summary.resumed_sessions, 1, "{:?}", summary.notes);
    let resumed = restarted.get_session(&session_id).await.unwrap();
    assert_eq!(resumed.provider_session_ref, original.provider_session_ref);
    assert_eq!(
        resumed.canonical_provider_session_ref,
        original.canonical_provider_session_ref
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    let requests = read_logged_jsonl(fake_agent.log_path.as_path());
    let create_request = requests
        .iter()
        .find(|entry| entry["method"].as_str() == Some("session/new"))
        .expect("session/new request");
    let resume_request = requests
        .iter()
        .find(|entry| entry["method"].as_str() == Some("session/resume"))
        .expect("session/resume request");
    assert_eq!(
        resume_request["params"]["sessionId"].as_str(),
        original.provider_session_ref.as_deref()
    );

    assert_eq!(
        requests
            .iter()
            .filter(|entry| entry["method"] == "session/new")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|entry| entry["method"] == "session/resume")
            .count(),
        1
    );
    for entry in [create_request, resume_request] {
        let expected_cwd = workspace.canonical_root.as_str();
        assert_eq!(entry["params"]["cwd"].as_str(), Some(expected_cwd));
        let gg_server = entry["params"]["mcpServers"]
            .as_array()
            .and_then(|servers| servers.first())
            .expect("gg mcp server");
        assert_eq!(gg_server["name"].as_str(), Some("gg"));
        assert!(gg_server["command"]
            .as_str()
            .is_some_and(|command| command.contains("gg-mcp-server")));
        let env = gg_server["env"]
            .as_array()
            .expect("gg mcp env array")
            .iter()
            .filter_map(|entry| {
                Some((
                    entry.get("name")?.as_str()?.to_string(),
                    entry.get("value")?.as_str()?.to_string(),
                ))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(env.get("GG_MCP_CALLER_AGENT_ID"), Some(&session_id));
        assert_eq!(
            env.get("GG_MCP_GATEWAY_URL"),
            Some(&"http://127.0.0.1:8787/v2/mcp".to_string())
        );
        let scoped = runtime_core::mint_scoped_mcp_token(&token, &session_id);
        assert_eq!(env.get("GG_MCP_GATEWAY_TOKEN"), Some(&scoped));
        assert_ne!(env.get("GG_MCP_GATEWAY_TOKEN"), Some(&token));
        assert_eq!(
            env.get("GG_MCP_ENABLE_PROCESS_TOOLS"),
            Some(&"1".to_string())
        );
    }
}

#[tokio::test]
async fn mcp_process_run_and_logs_share_runtime_process_service() {
    let (router, token, _temp_dir, _provider, _runtime, _app, history) =
        build_test_router_with_history(TeamMcpPolicy::default(), |_| {
            vec![json!({"provider":"codex","model":"test-model","metadata":{}})]
        })
        .await;
    let created = historical_session_json(&history[0]);
    let session_id = created["id"].as_str().expect("session id").to_string();

    let invoke_body = serde_json::json!({
        "namespace": "gg_process",
        "tool_name": "gg_process_run",
        "caller_agent_id": session_id,
        "invocation_id": "inv_1",
        "args": {
            "command": "echo phase4_mcp_runtime_path"
        }
    });
    let invoke_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/mcp/invoke")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(invoke_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("invoke");
    assert_eq!(invoke_response.status(), StatusCode::OK);
    let invoke_bytes = to_bytes(invoke_response.into_body(), usize::MAX)
        .await
        .expect("invoke body");
    let invoke_json: serde_json::Value =
        serde_json::from_slice(&invoke_bytes).expect("invoke json");
    assert_eq!(invoke_json["ok"].as_bool(), Some(true));
    let process_id = invoke_json
        .pointer("/result/process/process_id")
        .and_then(serde_json::Value::as_str)
        .expect("process_id")
        .to_string();

    let mut done = false;
    for _ in 0..80 {
        let get_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}?session_id={session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("get process");
        assert_eq!(get_response.status(), StatusCode::OK);
        let get_bytes = to_bytes(get_response.into_body(), usize::MAX)
            .await
            .expect("get process body");
        let process_json: serde_json::Value =
            serde_json::from_slice(&get_bytes).expect("get process json");
        let status = process_json
            .pointer("/process/status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(done, "process did not reach terminal state");

    let logs_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/processes/{process_id}/logs?session_id={session_id}&stream=stdout"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("logs response");
    assert_eq!(logs_response.status(), StatusCode::OK);
    let logs_bytes = to_bytes(logs_response.into_body(), usize::MAX)
        .await
        .expect("logs body");
    let logs_json: serde_json::Value = serde_json::from_slice(&logs_bytes).expect("logs json");
    let combined = logs_json
        .as_array()
        .into_iter()
        .flat_map(|rows| rows.iter())
        .filter_map(|row| row.get("content"))
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        combined.contains("phase4_mcp_runtime_path"),
        "expected process output in logs, got {combined}"
    );
}

#[tokio::test]
async fn mcp_invoke_team_tools_share_http_team_services() {
    let (router, token, temp_dir, _provider, _runtime, app, history) =
        build_test_router_with_history(TeamMcpPolicy::default(), |root| {
            let cwd = root.join("mcp-team-cwd");
            std::fs::create_dir_all(&cwd).expect("create mcp team cwd");
            ["lead", "member", "observer"]
                .into_iter()
                .map(|label| {
                    json!({
                        "provider":"codex", "model":"test-model", "cwd":cwd,
                        "metadata":{"suite":"mcp_team", "label":label},
                    })
                })
                .collect()
        })
        .await;
    let lead_session_id = history[0].id.clone();
    let member_session_id = history[1].id.clone();
    let observer_session_id = history[2].id.clone();

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "MCP Team Parity",
                        "lead_agent_id": lead_session_id,
                        "member_agent_ids": [member_session_id, observer_session_id],
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("team body"),
    )
    .expect("team json");
    let team_id = create_team_json["team"]["id"]
        .as_str()
        .expect("team id")
        .to_string();

    let invoke = |caller_agent_id: String,
                  tool_name: &'static str,
                  invocation_id: Option<&'static str>,
                  args: serde_json::Value| {
        let router = router.clone();
        let token = token.clone();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/mcp/invoke")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(
                            serde_json::json!({
                                "namespace": "gg_team",
                                "tool_name": tool_name,
                                "caller_agent_id": caller_agent_id,
                                "invocation_id": invocation_id,
                                "args": args,
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .expect("mcp invoke");
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("mcp body"),
            )
            .expect("mcp json")
        }
    };

    let status_json = invoke(
        lead_session_id.clone(),
        "gg_team_status",
        None,
        serde_json::json!({ "team_id": team_id.clone() }),
    )
    .await;
    assert_eq!(status_json["ok"].as_bool(), Some(true));
    assert_eq!(
        status_json["result"]["members"].as_array().map(Vec::len),
        Some(3)
    );

    let direct_json = invoke(
        lead_session_id.clone(),
        "gg_team_message",
        Some("mcp_direct_1"),
        serde_json::json!({
            "team_id": team_id.clone(),
            "recipient_agent_id": member_session_id,
            "message": "hello direct",
        }),
    )
    .await;
    assert_eq!(direct_json["ok"].as_bool(), Some(true));
    assert_eq!(direct_json["result"]["scope"].as_str(), Some("direct"));
    assert_eq!(direct_json["result"]["recipient_count"].as_u64(), Some(1));

    let broadcast_json = invoke(
        lead_session_id.clone(),
        "gg_team_message",
        Some("mcp_broadcast_1"),
        serde_json::json!({
            "team_id": team_id.clone(),
            "recipient_agent_id": "broadcast",
            "message": "hello broadcast",
        }),
    )
    .await;
    assert_eq!(broadcast_json["ok"].as_bool(), Some(true));
    assert_eq!(
        broadcast_json["result"]["scope"].as_str(),
        Some("broadcast")
    );
    assert_eq!(
        broadcast_json["result"]["recipient_count"].as_u64(),
        Some(2)
    );

    let non_lead_status_json = invoke(
        member_session_id.clone(),
        "gg_team_status",
        None,
        serde_json::json!({ "team_id": team_id.clone() }),
    )
    .await;
    assert_eq!(non_lead_status_json["ok"].as_bool(), Some(true));

    let non_lead_message_json = invoke(
        member_session_id.clone(),
        "gg_team_message",
        Some("mcp_non_lead_message_1"),
        serde_json::json!({
            "team_id": team_id.clone(),
            "recipient_agent_id": lead_session_id.clone(),
            "message": "non-lead message still allowed",
        }),
    )
    .await;
    assert_eq!(non_lead_message_json["ok"].as_bool(), Some(true));

    let non_lead_manage_json = invoke(
        member_session_id,
        "gg_team_manage",
        Some("mcp_non_lead_add_denied_1"),
        serde_json::json!({
            "team_id": team_id.clone(),
            "title": "Denied MCP Spawn",
        }),
    )
    .await;
    assert_eq!(non_lead_manage_json["ok"].as_bool(), Some(false));
    assert_eq!(
        non_lead_manage_json["error"]["code"].as_str(),
        Some("unsupported_capability")
    );

    let (status, historical) = legacy_spawn_fixture_json(
        &app,
        &team_id,
        Some("gg_team_manage:mcp_add_1"),
        serde_json::json!({"source_session_id": lead_session_id,
            "creator_agent_id": lead_session_id, "title":"MCP Spawned"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{historical}");
    // The historical seeder writes no in-memory projections. Reopen all
    // consumers over the same database and provider registry before checking
    // the saved Add result, current roster, and subsequent effectful Remove.
    let (router, _reopened_runtime, _reopened_app) =
        reopen_test_router_with_services(&app, temp_dir.path(), &token, TeamMcpPolicy::default())
            .await;
    let invoke = |caller_agent_id: String,
                  tool_name: &'static str,
                  invocation_id: Option<&'static str>,
                  args: serde_json::Value| {
        let router = router.clone();
        let token = token.clone();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/mcp/invoke")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(
                            serde_json::json!({
                                "namespace": "gg_team",
                                "tool_name": tool_name,
                                "caller_agent_id": caller_agent_id,
                                "invocation_id": invocation_id,
                                "args": args,
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .expect("mcp invoke");
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("mcp body"),
            )
            .expect("mcp json")
        }
    };

    let add_json = invoke(
        lead_session_id.clone(),
        "gg_team_manage",
        Some("mcp_add_1"),
        serde_json::json!({
            "team_id": team_id.clone(),
            "title": "MCP Spawned",
        }),
    )
    .await;
    assert_eq!(
        add_json["ok"].as_bool(),
        Some(true),
        "unexpected add response: {add_json}"
    );
    assert_eq!(add_json["result"]["operation"].as_str(), Some("add"));
    let spawned_agent_id = add_json["result"]["spawned_agent_id"]
        .as_str()
        .expect("spawned agent id")
        .to_string();

    let remove_json = invoke(
        lead_session_id,
        "gg_team_manage",
        None,
        serde_json::json!({
            "team_id": team_id,
            "remove_agent_ids": [spawned_agent_id],
        }),
    )
    .await;
    assert_eq!(remove_json["ok"].as_bool(), Some(true));
    assert_eq!(remove_json["result"]["operation"].as_str(), Some("remove"));
    assert_eq!(remove_json["result"]["removed_count"].as_u64(), Some(1));
}
