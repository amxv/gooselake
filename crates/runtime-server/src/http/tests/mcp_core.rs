use super::*;

#[tokio::test]
async fn mixed_provider_team_flow_uses_shared_runtime_services() {
    let (router, token, _temp_dir, _acp_provider) = build_mixed_provider_test_router().await;

    let codex_session_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "codex",
                        "model": "test-model",
                        "cwd": null,
                        "permission_mode": null,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create codex session");
    assert_eq!(codex_session_response.status(), StatusCode::OK);
    let codex_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(codex_session_response.into_body(), usize::MAX)
            .await
            .expect("codex body"),
    )
    .expect("codex json");
    let codex_session_id = codex_json["id"].as_str().expect("codex session id");

    let claude_session_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "claude",
                        "model": "test-claude-model",
                        "cwd": null,
                        "permission_mode": null,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create claude session");
    assert_eq!(claude_session_response.status(), StatusCode::OK);
    let claude_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(claude_session_response.into_body(), usize::MAX)
            .await
            .expect("claude body"),
    )
    .expect("claude json");
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

    let acp_session_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "acp",
                        "cwd": null,
                        "permission_mode": null,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create acp session");
    assert_eq!(acp_session_response.status(), StatusCode::OK);
    let acp_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(acp_session_response.into_body(), usize::MAX)
            .await
            .expect("acp body"),
    )
    .expect("acp json");
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
    let (router, token, _temp_dir, _acp_provider) = build_mixed_provider_test_router().await;

    let acp_session_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "acp",
                        "cwd": null,
                        "permission_mode": null,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create acp session");
    assert_eq!(acp_session_response.status(), StatusCode::OK);
    let acp_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(acp_session_response.into_body(), usize::MAX)
            .await
            .expect("acp body"),
    )
    .expect("acp json");
    let acp_session_id = acp_json["id"].as_str().expect("acp session id").to_string();

    let codex_session_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "codex",
                        "model": "test-model",
                        "cwd": null,
                        "permission_mode": null,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create codex session");
    assert_eq!(codex_session_response.status(), StatusCode::OK);
    let codex_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(codex_session_response.into_body(), usize::MAX)
            .await
            .expect("codex body"),
    )
    .expect("codex json");
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

    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let create_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "acp",
                        "cwd": "/tmp/acp-create",
                        "permission_mode": null,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create acp session");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    let session_id = create_json["id"].as_str().expect("session id").to_string();

    let close_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/close"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("close");
    assert_eq!(close_response.status(), StatusCode::OK);

    let resume_response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/resume"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from("{}".to_string()))
                .unwrap(),
        )
        .await
        .expect("resume");
    assert_eq!(resume_response.status(), StatusCode::OK);

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

    for (entry, expected_cwd) in [
        (create_request, "/tmp/acp-create"),
        (resume_request, "/tmp/acp-create"),
    ] {
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
            Some(&"http://127.0.0.1:8787/v1/mcp".to_string())
        );
        assert_eq!(env.get("GG_MCP_GATEWAY_TOKEN"), Some(&token));
        assert_eq!(
            env.get("GG_MCP_ENABLE_PROCESS_TOOLS"),
            Some(&"1".to_string())
        );
    }
}

#[tokio::test]
async fn mcp_process_run_and_logs_share_runtime_process_service() {
    let (router, token, _temp_dir) = build_test_router().await;

    let create_body = serde_json::json!({
        "provider": "codex",
        "model": "test-model",
        "cwd": null,
        "permission_mode": null,
        "metadata": {}
    });
    let create_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("create session");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_bytes = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .expect("create body");
    let created: serde_json::Value = serde_json::from_slice(&create_bytes).expect("create json");
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
    let (router, token, temp_dir) = build_test_router().await;
    let session_cwd = temp_dir.path().join("mcp-team-cwd");
    std::fs::create_dir_all(&session_cwd).expect("create mcp team cwd");

    let create_session = |label: &'static str| {
        let router = router.clone();
        let token = token.clone();
        let cwd = session_cwd.display().to_string();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/sessions")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(
                            serde_json::json!({
                                "provider": "codex",
                                "model": "test-model",
                                "cwd": cwd,
                                "metadata": {"suite": "mcp_team", "label": label}
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .expect("create session");
            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value = serde_json::from_slice(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("session body"),
            )
            .expect("session json");
            body["id"].as_str().expect("session id").to_string()
        }
    };

    let lead_session_id = create_session("lead").await;
    let member_session_id = create_session("member").await;
    let observer_session_id = create_session("observer").await;

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
        Some("unauthorized")
    );

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
