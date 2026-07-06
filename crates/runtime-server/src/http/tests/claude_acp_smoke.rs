use super::*;

#[tokio::test]
async fn claude_auth_endpoints_are_runtime_managed() {
    let prior_bridge_home_override = std::env::var_os("GG_CLAUDE_BRIDGE_HOME");
    let prior_bridge_config_override = std::env::var_os("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR");
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let isolated_bridge_home = temp_dir.path().join("isolated-bridge-home");
    let isolated_bridge_config = temp_dir.path().join("isolated-bridge-config");
    std::fs::create_dir_all(isolated_bridge_home.as_path()).expect("create isolated home dir");
    std::fs::create_dir_all(isolated_bridge_config.as_path()).expect("create isolated config dir");
    std::env::set_var(
        "GG_CLAUDE_BRIDGE_HOME",
        isolated_bridge_home.display().to_string(),
    );
    std::env::set_var(
        "GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR",
        isolated_bridge_config.display().to_string(),
    );

    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.codex.enabled = false;
    config.providers.claude.enabled = true;
    let claude_provider_dir = config.resolve_provider_dir("claude");
    let claude_credentials_path = claude_provider_dir
        .join("home")
        .join(".claude")
        .join(".credentials.json");
    let claude_config_path = claude_provider_dir.join("config").join(".claude.json");

    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let initial_status = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/providers/claude/auth/status")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("initial status response");
    assert_eq!(initial_status.status(), StatusCode::OK);
    let initial_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(initial_status.into_body(), usize::MAX)
            .await
            .expect("initial status body"),
    )
    .expect("initial status json");
    assert_eq!(initial_json["authenticated"], false);

    let api_key_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/claude/auth/api-key")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({ "api_key": "sk-ant-test-123" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("api-key response");
    assert_eq!(api_key_response.status(), StatusCode::OK);
    let api_key_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(api_key_response.into_body(), usize::MAX)
            .await
            .expect("api-key body"),
    )
    .expect("api-key json");
    assert_eq!(api_key_json["authenticated"], true);
    assert_eq!(api_key_json["mode"], "api_key");

    let import_json_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/claude/auth/import-json")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "auth_json": {
                            "credentials_json": {
                                "claudeAiOauth": {
                                    "accessToken": "runtime-managed-auth",
                                    "refreshToken": "runtime-managed-auth"
                                }
                            },
                            "config_json": {
                                "projects": {}
                            }
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("import-json response");
    assert_eq!(import_json_response.status(), StatusCode::OK);
    let import_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(import_json_response.into_body(), usize::MAX)
            .await
            .expect("import-json body"),
    )
    .expect("import-json json");
    assert_eq!(import_json["authenticated"], true);
    assert_eq!(import_json["mode"], "claude_code_oauth");
    assert!(claude_credentials_path.exists());
    assert!(claude_config_path.exists());

    let boundary = "phase7boundary";
    let multipart_body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"auth_bundle.json\"\r\nContent-Type: application/json\r\n\r\n{{\"credentials_json\":{{\"claudeAiOauth\":{{\"accessToken\":\"multipart-import\",\"refreshToken\":\"multipart-import\"}}}},\"config_json\":{{\"projects\":{{}}}}}}\r\n--{boundary}--\r\n"
        );
    let import_file_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/claude/auth/import-file")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(multipart_body))
                .unwrap(),
        )
        .await
        .expect("import-file response");
    assert_eq!(import_file_response.status(), StatusCode::OK);

    let logout_response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/claude/auth/logout")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("logout response");
    assert_eq!(logout_response.status(), StatusCode::OK);
    let logout_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(logout_response.into_body(), usize::MAX)
            .await
            .expect("logout body"),
    )
    .expect("logout json");
    assert_eq!(logout_json["authenticated"], false);
    assert!(!claude_credentials_path.exists());
    assert!(!claude_config_path.exists());

    match prior_bridge_home_override {
        Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_HOME", value),
        None => std::env::remove_var("GG_CLAUDE_BRIDGE_HOME"),
    }
    match prior_bridge_config_override {
        Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR", value),
        None => std::env::remove_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR"),
    }
}

#[tokio::test]
#[ignore = "requires local Claude auth sources at ~/.claude/.credentials.json and ~/.gg/claude/.claude.json (or ~/.claude.json fallback)"]
async fn ignored_real_claude_http_smoke_host_credentials_mcp_turn_complete() {
    let prior_bridge_home_override = std::env::var_os("GG_CLAUDE_BRIDGE_HOME");
    let prior_bridge_config_override = std::env::var_os("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR");
    let prior_force_oauth_export = std::env::var_os("GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN");
    std::env::remove_var("GG_CLAUDE_BRIDGE_HOME");
    std::env::remove_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR");
    std::env::remove_var("GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN");

    let home_dir = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .expect("HOME must be set for Claude smoke");
    let credentials_source_path = std::env::var("GG_CLAUDE_SMOKE_CREDENTIALS_SOURCE")
        .ok()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| home_dir.join(".claude").join(".credentials.json"));
    let config_source_path = std::env::var("GG_CLAUDE_SMOKE_CONFIG_SOURCE")
        .ok()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let gg_claude_config = home_dir.join(".gg").join("claude").join(".claude.json");
            if gg_claude_config.exists() {
                gg_claude_config
            } else {
                home_dir.join(".claude.json")
            }
        });
    assert!(
        credentials_source_path.exists(),
        "Claude smoke credentials source path must exist: {}",
        credentials_source_path.display()
    );
    assert!(
        config_source_path.exists(),
        "Claude smoke config source path must exist: {}",
        config_source_path.display()
    );
    std::env::set_var("GG_CLAUDE_BRIDGE_HOME", home_dir.display().to_string());
    if let Some(config_source_dir) = config_source_path.parent() {
        std::env::set_var(
            "GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR",
            config_source_dir.display().to_string(),
        );
    }

    let claude_bridge_command_path = standalone_claude_bridge_command_path();
    let gg_mcp_command_path = standalone_gg_mcp_server_command_path();
    assert!(
        claude_bridge_command_path.exists(),
        "branch-owned Claude bridge launcher is missing at {}",
        claude_bridge_command_path.display()
    );
    assert!(
        gg_mcp_command_path.exists(),
        "branch-owned gg-mcp-server launcher is missing at {}",
        gg_mcp_command_path.display()
    );

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.codex.enabled = false;
    config.providers.claude.enabled = true;
    config.providers.claude_auth_mode = "host_machine".to_string();
    config.processes.enabled = true;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind smoke listener");
    let listen_addr = listener.local_addr().expect("smoke listener addr");
    config.server.public_base_url = format!("http://{listen_addr}");

    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });
    let smoke_server_router = router.clone();
    let smoke_server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, smoke_server_router).await;
    });

    let initial_auth_status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/providers/claude/auth/status")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("auth status response");
    assert_eq!(initial_auth_status_response.status(), StatusCode::OK);
    let initial_auth_status_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(initial_auth_status_response.into_body(), usize::MAX)
            .await
            .expect("auth status body"),
    )
    .expect("auth status json");
    assert_eq!(
            initial_auth_status_json["authenticated"],
            true,
            "host-machine mode should resolve authenticated state from host login; credentials_source_path={} config_source_path={} detail={}",
            credentials_source_path.display(),
            config_source_path.display(),
            initial_auth_status_json["detail"]
        );

    let smoke_model = std::env::var("GG_CLAUDE_SMOKE_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "claude-sonnet-5".to_string());
    let smoke_permission_mode = std::env::var("GG_CLAUDE_SMOKE_PERMISSION_MODE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "bypassPermissions".to_string());
    let smoke_cwd = std::env::current_dir()
        .expect("current dir")
        .display()
        .to_string();

    let create_session_response = router
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
                        "model": smoke_model,
                        "cwd": smoke_cwd,
                        "permission_mode": smoke_permission_mode,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create session response");
    assert_eq!(create_session_response.status(), StatusCode::OK);
    let create_session_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_session_response.into_body(), usize::MAX)
            .await
            .expect("create session body"),
    )
    .expect("create session json");
    let session_id = create_session_json["id"]
        .as_str()
        .expect("session id")
        .to_string();

    let marker_path = temp_dir.path().join("mcp-smoke-marker.txt");
    let marker_path_display = marker_path.display().to_string();
    let marker_token = "CLAUDE_MCP_HTTP_SMOKE_MARKER_47290";
    let completion_token = "CLAUDE_HTTP_SMOKE_OK_92174";
    let tool_prompt = format!(
            "Use the gg_process_run tool exactly once to run this command: printf '{marker_token}\\n' > '{marker_path_display}'. After the tool completes, reply with exactly: {completion_token}"
        );

    let send_turn_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "input": [
                            {
                                "type": "text",
                                "text": tool_prompt,
                            }
                        ],
                        "expected_turn_id": null,
                        "permission_mode": null
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("send turn response");
    assert_eq!(send_turn_response.status(), StatusCode::OK);
    let send_turn_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(send_turn_response.into_body(), usize::MAX)
            .await
            .expect("send turn body"),
    )
    .expect("send turn json");
    let turn_id = send_turn_json["turn_id"]
        .as_str()
        .expect("turn id")
        .to_string();

    let max_wait_secs = std::env::var("GG_CLAUDE_SMOKE_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(300);
    let deadline = std::time::Instant::now() + Duration::from_secs(max_wait_secs.max(30));
    let mut saw_terminal_event = false;
    let mut terminal_kind: Option<String> = None;
    let mut terminal_text: Option<String> = None;
    let mut accepted_approvals = std::collections::BTreeSet::new();
    let mut event_cursor: Option<i64> = None;
    let smoke_debug = std::env::var("GG_CLAUDE_SMOKE_DEBUG")
        .ok()
        .map(|value| value.trim() == "1")
        .unwrap_or(false);
    let mut recent_matching_events: std::collections::VecDeque<String> =
        std::collections::VecDeque::new();
    while std::time::Instant::now() < deadline {
        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/sessions/{session_id}/events?after_seq={}",
                        event_cursor.unwrap_or(0)
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("session events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(events_response.into_body(), usize::MAX)
                .await
                .expect("session events body"),
        )
        .expect("session events json");

        let events = events_json
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(last_seq) = events
            .iter()
            .filter_map(|event| event.get("seq").and_then(serde_json::Value::as_i64))
            .max()
        {
            event_cursor = Some(last_seq);
        }

        for event in events {
            let kind = event
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let event_seq = event
                .get("seq")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default();
            if smoke_debug {
                let event_turn_id = event
                    .get("turn_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<none>");
                eprintln!(
                    "[claude-smoke] session_event seq={} kind={} turn_id={}",
                    event_seq, kind, event_turn_id
                );
            }
            let is_matching_turn = event
                .get("turn_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == turn_id);
            if !is_matching_turn {
                continue;
            }
            let summary = format!("{event_seq}:{kind}");
            recent_matching_events.push_back(summary.clone());
            while recent_matching_events.len() > 24 {
                let _ = recent_matching_events.pop_front();
            }
            if smoke_debug {
                eprintln!("[claude-smoke] event {summary}");
            }
            if kind == "approval.requested" {
                if let Some(approval_id) = event
                    .get("payload")
                    .and_then(|payload| {
                        payload
                            .get("approval_id")
                            .or_else(|| payload.get("approvalId"))
                    })
                    .and_then(serde_json::Value::as_str)
                {
                    if !accepted_approvals.contains(approval_id) {
                        let approval_response = router
                            .clone()
                            .oneshot(
                                Request::builder()
                                    .method("POST")
                                    .uri(format!(
                                        "/v1/sessions/{session_id}/approvals/{approval_id}"
                                    ))
                                    .header(header::CONTENT_TYPE, "application/json")
                                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                                    .body(Body::from(
                                        serde_json::json!({
                                            "decision": "accept",
                                            "payload": null,
                                        })
                                        .to_string(),
                                    ))
                                    .unwrap(),
                            )
                            .await
                            .expect("approval response");
                        assert_eq!(approval_response.status(), StatusCode::OK);
                        accepted_approvals.insert(approval_id.to_string());
                        if smoke_debug {
                            eprintln!("[claude-smoke] accepted approval_id={approval_id}");
                        }
                    }
                }
            }
            if matches!(kind, "turn.completed" | "turn.failed" | "turn.interrupted") {
                saw_terminal_event = true;
                terminal_kind = Some(kind.to_string());
                terminal_text = event
                    .get("payload")
                    .and_then(|payload| {
                        payload
                            .get("assistant_text")
                            .or_else(|| payload.get("assistantText"))
                    })
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .or_else(|| {
                        event
                            .get("payload")
                            .and_then(|payload| payload.get("usage"))
                            .and_then(|usage| {
                                usage
                                    .get("last_message")
                                    .or_else(|| usage.get("lastMessage"))
                                    .or_else(|| usage.get("assistant_text"))
                                    .or_else(|| usage.get("assistantText"))
                            })
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(str::to_string)
                    });
                break;
            }
        }

        if saw_terminal_event {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let session_snapshot_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sessions/{session_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("session snapshot response");
    let session_snapshot_status = session_snapshot_response.status();
    let session_snapshot: serde_json::Value = serde_json::from_slice(
        &to_bytes(session_snapshot_response.into_body(), usize::MAX)
            .await
            .expect("session snapshot body"),
    )
    .unwrap_or(serde_json::json!({
        "error": "failed to parse session snapshot body",
    }));
    assert!(
            saw_terminal_event,
            "Claude turn did not reach terminal state; recent_events={:?}; approvals_accepted={:?}; session_status_code={}; session_snapshot={}",
            recent_matching_events,
            accepted_approvals,
            session_snapshot_status,
            session_snapshot
        );
    assert_eq!(
        terminal_kind.as_deref(),
        Some("turn.completed"),
        "Claude HTTP smoke turn should complete successfully"
    );
    let terminal_text = terminal_text.unwrap_or_default();
    assert!(
        terminal_text.contains(completion_token),
        "Claude HTTP smoke terminal text missing expected token; terminal_text={terminal_text}"
    );
    assert!(
        marker_path.exists(),
        "expected Claude MCP tool call to create marker file {}",
        marker_path.display()
    );
    let marker_contents = std::fs::read_to_string(&marker_path).unwrap_or_default();
    assert!(
        marker_contents.contains(marker_token),
        "expected marker file to contain token {}; contents={marker_contents}",
        marker_token
    );

    let close_response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/close"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("close response");
    assert_eq!(close_response.status(), StatusCode::OK);
    smoke_server_handle.abort();
    match prior_bridge_home_override {
        Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_HOME", value),
        None => std::env::remove_var("GG_CLAUDE_BRIDGE_HOME"),
    }
    match prior_bridge_config_override {
        Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR", value),
        None => std::env::remove_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR"),
    }
    match prior_force_oauth_export {
        Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN", value),
        None => std::env::remove_var("GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN"),
    }
}

#[tokio::test]
#[ignore = "real ACP smoke test: requires GG_ACP_SMOKE_COMMAND and optional GG_ACP_SMOKE_ARGS_JSON/GG_ACP_SMOKE_ENV_JSON"]
async fn ignored_real_acp_http_smoke_turn_completes() {
    let smoke_command = std::env::var("GG_ACP_SMOKE_COMMAND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .expect("GG_ACP_SMOKE_COMMAND must be set for ACP smoke");
    let smoke_args = std::env::var("GG_ACP_SMOKE_ARGS_JSON")
        .ok()
        .map(|raw| {
            serde_json::from_str::<Vec<String>>(raw.as_str())
                .expect("GG_ACP_SMOKE_ARGS_JSON must be a JSON string array")
        })
        .unwrap_or_default();
    let smoke_env = std::env::var("GG_ACP_SMOKE_ENV_JSON")
        .ok()
        .map(|raw| {
            serde_json::from_str::<std::collections::BTreeMap<String, String>>(raw.as_str())
                .expect("GG_ACP_SMOKE_ENV_JSON must be a JSON object of string pairs")
        })
        .unwrap_or_default();
    let smoke_debug = std::env::var("GG_ACP_SMOKE_DEBUG")
        .ok()
        .map(|value| value.trim() == "1")
        .unwrap_or(false);

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.codex.enabled = false;
    config.providers.claude.enabled = false;
    config.providers.acp.enabled = true;
    config.providers.acp.command = Some(smoke_command.clone());
    config.providers.acp.args = smoke_args.clone();
    config.providers.acp.env = smoke_env;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind smoke listener");
    let listen_addr = listener.local_addr().expect("smoke listener addr");
    config.server.public_base_url = format!("http://{listen_addr}");

    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });
    let smoke_server_router = router.clone();
    let smoke_server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, smoke_server_router).await;
    });

    let auth_status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/providers/acp/auth/status")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("auth status response");
    assert_eq!(auth_status_response.status(), StatusCode::OK);

    let smoke_cwd = temp_dir.path().join("smoke-cwd");
    std::fs::create_dir_all(smoke_cwd.as_path()).expect("create smoke cwd");
    let completion_token = "ACP_HTTP_SMOKE_OK_39142";
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
                        "cwd": smoke_cwd.display().to_string(),
                        "permission_mode": null,
                        "metadata": {"smoke":"real_acp_http"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create session response");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    let session_id = create_json["id"].as_str().expect("session id").to_string();

    let send_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "input": [{
                            "type": "text",
                            "text": format!("Reply with exactly {completion_token}")
                        }],
                        "expected_turn_id": null,
                        "permission_mode": null
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("send turn response");
    assert_eq!(send_response.status(), StatusCode::OK);
    let send_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(send_response.into_body(), usize::MAX)
            .await
            .expect("send body"),
    )
    .expect("send json");
    let turn_id = send_json["turn_id"].as_str().expect("turn id").to_string();

    let max_wait_secs = std::env::var("GG_ACP_SMOKE_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(180);
    let deadline = std::time::Instant::now() + Duration::from_secs(max_wait_secs.max(30));
    let mut terminal_event: Option<Value> = None;
    let mut event_cursor: Option<i64> = None;
    while std::time::Instant::now() < deadline {
        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/sessions/{session_id}/events?after_seq={}",
                        event_cursor.unwrap_or(0)
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events_json: Vec<Value> = serde_json::from_slice(
            &to_bytes(events_response.into_body(), usize::MAX)
                .await
                .expect("events body"),
        )
        .expect("events json");

        if let Some(last_seq) = events_json
            .iter()
            .filter_map(|event| event.get("seq").and_then(Value::as_i64))
            .max()
        {
            event_cursor = Some(last_seq);
        }

        for event in events_json {
            if event.get("turn_id").and_then(Value::as_str) != Some(turn_id.as_str()) {
                continue;
            }
            if smoke_debug {
                eprintln!(
                    "[acp-smoke] seq={} kind={}",
                    event.get("seq").and_then(Value::as_i64).unwrap_or_default(),
                    event
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                );
            }
            if matches!(
                event.get("kind").and_then(Value::as_str),
                Some("turn.completed" | "turn.failed" | "turn.interrupted")
            ) {
                terminal_event = Some(event);
                break;
            }
        }

        if terminal_event.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let terminal_event = terminal_event.expect("ACP smoke turn should reach terminal state");
    assert_eq!(
        terminal_event.get("kind").and_then(Value::as_str),
        Some("turn.completed")
    );
    let terminal_text = terminal_event
        .get("payload")
        .and_then(|payload| payload.get("assistant_text"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            terminal_event
                .get("payload")
                .and_then(|payload| payload.get("usage"))
                .and_then(|usage| usage.get("last_message"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();
    assert!(
        terminal_text.contains(completion_token),
        "ACP smoke terminal text missing expected token; text={terminal_text}"
    );

    let session_snapshot_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sessions/{session_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("session snapshot response");
    assert_eq!(session_snapshot_response.status(), StatusCode::OK);
    let session_snapshot: serde_json::Value = serde_json::from_slice(
        &to_bytes(session_snapshot_response.into_body(), usize::MAX)
            .await
            .expect("session snapshot body"),
    )
    .expect("session snapshot json");
    let transcript_text = session_snapshot["metadata"]["session_transcript"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        transcript_text.contains(completion_token),
        "ACP smoke transcript missing expected token; transcript={transcript_text}"
    );

    let close_response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/close"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("close response");
    assert_eq!(close_response.status(), StatusCode::OK);
    smoke_server_handle.abort();
}
