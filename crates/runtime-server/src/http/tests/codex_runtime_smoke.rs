use super::*;

#[tokio::test]
#[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json"]
async fn smoke_real_codex_runtime_slice_with_staged_auth_copy() {
    let home_dir = std::env::var("HOME").expect("HOME must be set");
    let source_auth = std::path::PathBuf::from(home_dir)
        .join(".gg")
        .join("codex")
        .join("auth.json");
    assert!(
        source_auth.exists(),
        "expected real auth file at {}",
        source_auth.display()
    );

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.claude.enabled = false;
    config.providers.codex.enabled = true;
    prepare_codex_smoke_config(&config);

    let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");
    let codex_provider = bootstrapped
        .app
        .provider_registry
        .get(runtime_core::ProviderKind::Codex)
        .expect("registered Codex provider");

    let staged_auth = config
        .resolve_provider_dir("codex")
        .join("home")
        .join("auth.json");
    assert!(staged_auth.exists(), "expected staged auth copy");

    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let auth_status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/providers/codex/auth/status")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("auth status response");
    assert_eq!(auth_status_response.status(), StatusCode::OK);
    let auth_status_bytes = to_bytes(auth_status_response.into_body(), usize::MAX)
        .await
        .expect("auth status body");
    let auth_status_json: serde_json::Value =
        serde_json::from_slice(&auth_status_bytes).expect("auth status json");
    assert_eq!(
        auth_status_json["authenticated"].as_bool(),
        Some(true),
        "expected codex auth to be authenticated"
    );

    let smoke_model = codex_test_model();
    let create_body = serde_json::json!({
        "provider": "codex",
        "model": smoke_model,
        "cwd": temp_dir.path().display().to_string(),
        "permission_mode": null,
        "metadata": {
            "smoke": "real_codex_runtime"
        }
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
        .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_bytes = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .expect("create body");
    let created_session: serde_json::Value =
        serde_json::from_slice(&create_bytes).expect("create json");
    let session_id = created_session["id"]
        .as_str()
        .expect("session id")
        .to_string();

    let turn_body = serde_json::json!({
        "input": [
            {
                "type": "text",
                "text": "Reply with exactly this token and nothing else: codex_runtime_token_94731"
            }
        ],
        "expected_turn_id": null,
        "permission_mode": null
    });
    let send_turn_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(turn_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("send turn response");
    assert_eq!(send_turn_response.status(), StatusCode::OK);
    let send_turn_bytes = to_bytes(send_turn_response.into_body(), usize::MAX)
        .await
        .expect("send turn body");
    let accepted_turn: serde_json::Value =
        serde_json::from_slice(&send_turn_bytes).expect("send turn json");
    let turn_id = accepted_turn["turn_id"]
        .as_str()
        .expect("turn id")
        .to_string();

    let mut finished = false;
    for _attempt in 0..80 {
        let get_session_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("get session response");
        assert_eq!(get_session_response.status(), StatusCode::OK);
        let session_bytes = to_bytes(get_session_response.into_body(), usize::MAX)
            .await
            .expect("get session body");
        let session_json: serde_json::Value =
            serde_json::from_slice(&session_bytes).expect("get session json");
        if session_json["active_turn_id"].is_null() {
            finished = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(finished, "turn did not reach terminal state in time");

    let second_turn_body = serde_json::json!({
        "input": [
            {
                "type": "text",
                "text": "What exact token did you reply with previously? Reply with only that token."
            }
        ],
        "expected_turn_id": null,
        "permission_mode": null
    });
    let second_send_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(second_turn_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("second send response");
    assert_eq!(second_send_response.status(), StatusCode::OK);
    let second_send_bytes = to_bytes(second_send_response.into_body(), usize::MAX)
        .await
        .expect("second send body");
    let second_accepted_turn: serde_json::Value =
        serde_json::from_slice(&second_send_bytes).expect("second send json");
    let second_turn_id = second_accepted_turn["turn_id"]
        .as_str()
        .expect("second turn id")
        .to_string();

    let mut second_finished = false;
    for _attempt in 0..80 {
        let get_session_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("get session response");
        assert_eq!(get_session_response.status(), StatusCode::OK);
        let session_bytes = to_bytes(get_session_response.into_body(), usize::MAX)
            .await
            .expect("get session body");
        let session_json: serde_json::Value =
            serde_json::from_slice(&session_bytes).expect("get session json");
        if session_json["active_turn_id"].is_null() {
            second_finished = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        second_finished,
        "second turn did not reach terminal state in time"
    );

    let events_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sessions/{session_id}/events"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("events response");
    assert_eq!(events_response.status(), StatusCode::OK);
    let events_bytes = to_bytes(events_response.into_body(), usize::MAX)
        .await
        .expect("events body");
    let events: serde_json::Value = serde_json::from_slice(&events_bytes).expect("events json");
    let tracked_turn_ids = [turn_id.as_str(), second_turn_id.as_str()];
    let events_array = events.as_array().expect("events array");
    let mut completed_messages = std::collections::BTreeMap::<String, String>::new();
    let mut failed_or_interrupted = Vec::new();
    for event in events_array {
        let Some(kind) = event.get("kind").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(event_turn_id) = event.get("turn_id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !tracked_turn_ids.contains(&event_turn_id) {
            continue;
        }
        match kind {
            "turn.completed" => {
                let last_message = event
                    .get("payload")
                    .and_then(|payload| payload.get("usage"))
                    .and_then(|usage| usage.get("last_message"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                completed_messages.insert(event_turn_id.to_string(), last_message);
            }
            "turn.failed" | "turn.interrupted" => {
                failed_or_interrupted.push(format!("{event_turn_id}:{kind}"));
            }
            _ => {}
        }
    }
    assert!(
        failed_or_interrupted.is_empty(),
        "real codex runtime smoke observed non-success terminal turns: {:?}",
        failed_or_interrupted
    );
    for tracked_turn in tracked_turn_ids {
        let last_message = completed_messages
            .get(tracked_turn)
            .cloned()
            .unwrap_or_default();
        assert!(
            last_message.contains("codex_runtime_token_94731"),
            "turn {} did not complete with expected token; last_message={}",
            tracked_turn,
            last_message
        );
    }
    assert_eq!(
        completed_messages.len(),
        2,
        "expected exactly two successful completed turns before restart"
    );

    // Simulate a real process restart without mutating the durable runtime
    // session: tear down only the provider attachment/app-server, then drop the
    // first server composition before bootstrapping against the same store.
    codex_provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: session_id.clone(),
            reason: Some("real_codex_smoke_restart_detach".to_string()),
        })
        .await
        .expect("detach Codex provider session before restart");
    drop(codex_provider);
    drop(router);

    // Verify the persisted session can be resumed and used.
    let restarted = bootstrap_runtime(config.clone())
        .await
        .expect("restart bootstrap");
    let restarted_token = restarted.auth.bearer_token.clone();
    let restarted_router = build_router(AppState {
        app: restarted.app,
        runtime: restarted.runtime,
        bearer_token: restarted_token.clone(),
        public_base_url: restarted.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let resume_response = restarted_router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/resume"))
                .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("resume response");
    let resume_status = resume_response.status();
    let resume_body = to_bytes(resume_response.into_body(), usize::MAX)
        .await
        .expect("resume body");
    assert_eq!(
        resume_status,
        StatusCode::OK,
        "resume failed: {}",
        String::from_utf8_lossy(&resume_body)
    );

    let third_turn_body = serde_json::json!({
        "input": [
            {
                "type": "text",
                "text": "After resume, reply with exactly this token and nothing else: codex_runtime_token_94731"
            }
        ],
        "expected_turn_id": null,
        "permission_mode": null
    });
    let third_send_response = restarted_router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                .body(Body::from(third_turn_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("third send response");
    assert_eq!(third_send_response.status(), StatusCode::OK);
    let third_send_bytes = to_bytes(third_send_response.into_body(), usize::MAX)
        .await
        .expect("third send body");
    let third_accepted_turn: serde_json::Value =
        serde_json::from_slice(&third_send_bytes).expect("third send json");
    let third_turn_id = third_accepted_turn["turn_id"]
        .as_str()
        .expect("third turn id")
        .to_string();

    let mut third_finished = false;
    for _attempt in 0..80 {
        let get_session_response = restarted_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("get resumed session response");
        assert_eq!(get_session_response.status(), StatusCode::OK);
        let session_bytes = to_bytes(get_session_response.into_body(), usize::MAX)
            .await
            .expect("get resumed session body");
        let session_json: serde_json::Value =
            serde_json::from_slice(&session_bytes).expect("get resumed session json");
        if session_json["active_turn_id"].is_null() {
            third_finished = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        third_finished,
        "third turn after resume did not reach terminal state in time"
    );

    let resumed_events_response = restarted_router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sessions/{session_id}/events"))
                .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("resumed events response");
    assert_eq!(resumed_events_response.status(), StatusCode::OK);
    let resumed_events_bytes = to_bytes(resumed_events_response.into_body(), usize::MAX)
        .await
        .expect("resumed events body");
    let resumed_events: serde_json::Value =
        serde_json::from_slice(&resumed_events_bytes).expect("resumed events json");
    let resumed_kinds = resumed_events
        .as_array()
        .expect("resumed events array")
        .iter()
        .filter_map(|event| event.get("kind"))
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert!(
        resumed_kinds.iter().any(|kind| kind == "session.resumed"),
        "expected session.resumed event after explicit resume"
    );
    let resumed_events_array = resumed_events.as_array().expect("resumed events array");
    let resumed_completion = resumed_events_array
        .iter()
        .filter(|event| {
            event
                .get("turn_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == third_turn_id)
        })
        .find_map(|event| {
            let kind = event.get("kind").and_then(serde_json::Value::as_str)?;
            if !matches!(kind, "turn.completed" | "turn.failed" | "turn.interrupted") {
                return None;
            }
            let last_message = event
                .get("payload")
                .and_then(|payload| payload.get("usage"))
                .and_then(|usage| usage.get("last_message"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            Some((kind.to_string(), last_message))
        });
    let (resume_kind, resume_message) =
        resumed_completion.expect("expected resumed turn terminal event");
    assert_eq!(
        resume_kind, "turn.completed",
        "resumed turn must complete successfully"
    );
    assert!(
        resume_message.contains("codex_runtime_token_94731"),
        "resumed turn completion missing expected token; last_message={resume_message}"
    );

    let terminal_count_after_resume = resumed_kinds
        .iter()
        .filter(|kind| kind.as_str() == "turn.completed")
        .count();
    assert!(
        terminal_count_after_resume >= 3,
        "expected three successful completed turns after resume"
    );

    let close_response = restarted_router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/close"))
                .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("close response");
    assert_eq!(close_response.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "real Codex GG MCP smoke: requires local ~/.gg/codex/auth.json"]
async fn smoke_real_codex_gg_process_tool_uses_session_scoped_caller_identity() {
    let home_dir = std::env::var("HOME").expect("HOME must be set");
    let source_auth = std::path::PathBuf::from(home_dir)
        .join(".gg")
        .join("codex")
        .join("auth.json");
    assert!(
        source_auth.exists(),
        "expected real auth file at {}",
        source_auth.display()
    );

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Codex GG MCP smoke listener");
    let listen_addr = listener.local_addr().expect("Codex GG MCP listener addr");

    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.claude.enabled = false;
    config.providers.codex.enabled = true;
    config.processes.enabled = true;
    config.server.public_base_url = format!("http://{listen_addr}");
    prepare_codex_smoke_config(&config);

    let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");
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

    let workspace_root = temp_dir.path().join("native-gg-workspace");
    std::fs::create_dir_all(&workspace_root).expect("create Codex GG workspace root");
    let register_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("Idempotency-Key", "codex-native-gg-smoke-workspace")
                .body(Body::from(
                    serde_json::json!({
                        "canonical_root": workspace_root,
                        "display_name": "Native Codex GG smoke"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("register Codex GG workspace");
    assert_eq!(register_response.status(), StatusCode::OK);
    let registered: runtime_core::WorkspaceRegisterResponse = serde_json::from_slice(
        &to_bytes(register_response.into_body(), usize::MAX)
            .await
            .expect("Codex GG workspace response"),
    )
    .expect("Codex GG workspace JSON");

    // A real native provider turn must see approved script-supplied onboarding
    // context, not merely a notice stored by the runtime or echoed in the
    // human-authored prompt. The token below occurs only in the script output.
    let hook_token = "CODEX_NATIVE_HOOK_CONTEXT_82736";
    let hook_output = workspace_root.join("pre-add-hook-output.json");
    std::fs::write(
        &hook_output,
        serde_json::json!({
            "pre_teammate_add": {
                "onboarding_context_append_text": format!(
                    "In your final response, include the exact onboarding marker {hook_token}."
                )
            }
        })
        .to_string(),
    )
    .expect("write hook response fixture");
    let hook_command = format!("cat '{}'", hook_output.display());
    let hook_config = workspace_root.join(runtime_core::TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(hook_config.parent().unwrap()).expect("create hook config directory");
    std::fs::write(
        &hook_config,
        serde_json::json!({"version":1,"hooks":{"pre_teammate_add":[{
            "name":"native_onboarding_context", "command":hook_command, "timeout_ms":5000
        }]}})
        .to_string(),
    )
    .expect("write approved-hook candidate");
    let hooks_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/workspaces/{}/hooks",
                    registered.workspace.workspace_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("discover native hook candidate");
    assert_eq!(hooks_response.status(), StatusCode::OK);
    let hooks: serde_json::Value = serde_json::from_slice(
        &to_bytes(hooks_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let fingerprint = hooks["hooks"][0]["fingerprint"].as_str().unwrap();
    let approved_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/v2/workspaces/{}/hooks/approval",
                    registered.workspace.workspace_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "expected_revision":0,"hooks":[{
                            "event_name":"pre_teammate_add",
                            "name":"native_onboarding_context", "fingerprint":fingerprint
                        }]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("approve native hook candidate");
    assert_eq!(approved_response.status(), StatusCode::OK);

    let marker_token = "CODEX_GG_PROCESS_MARKER_71423";
    let marker_path = temp_dir.path().join(format!("{marker_token}.txt"));
    let completion_token = "CODEX_GG_PROCESS_OK_63851";
    // A simple native command proves actual process execution without relying
    // on the provider rewriting shell quoting/redirection faithfully.
    let tool_command = format!("touch {}", marker_path.display());
    let prompt = format!(
        "Use the native gg_process tool exactly once with mode=run and command exactly: {tool_command}. Do not use shell or command execution directly. After the GG tool returns, include {completion_token} in your final reply and follow the separate onboarding context instructions."
    );

    let create_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/workspaces/{}/agents",
                    registered.workspace.workspace_id
                ))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "codex",
                        "model": codex_test_model(),
                        "current_preferences": {"thinking_effort": "low"},
                        "cwd": workspace_root,
                        "permission_intent": "danger_full_access",
                        "title": "Native GG live smoke agent",
                        "metadata": {"smoke": "real_codex_gg_process_tool"},
                        "onboarding": {"prompt": prompt}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create canonical Codex GG smoke agent");
    let create_status = create_response.status();
    let create_body = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .expect("create Codex GG MCP body");
    assert_eq!(
        create_status,
        StatusCode::OK,
        "native Codex agent creation failed: {}",
        String::from_utf8_lossy(&create_body)
    );
    let created: runtime_core::WorkspaceAgentRecord =
        serde_json::from_slice(&create_body).expect("create Codex GG MCP json");
    let session_id = created.agent_id;

    let session_config = config
        .resolve_provider_dir("codex")
        .join("home")
        .join("runtime-sessions")
        .join(&session_id)
        .join("config.toml");
    let rendered_session_config =
        std::fs::read_to_string(&session_config).expect("session-specific Codex config");
    let native_config: toml::Value =
        toml::from_str(&rendered_session_config).expect("native Codex session TOML");
    assert_eq!(native_config["model"].as_str(), Some("gpt-6-luna"));
    assert_eq!(
        native_config["model_reasoning_effort"].as_str(),
        Some("low")
    );
    // Codex uses native dynamic GG tools; it must not receive a redundant GG
    // MCP registration, caller identity metadata, or any gateway credential.
    assert!(
        !rendered_session_config.contains("mcp_servers.gg")
            && !rendered_session_config.contains("GG_MCP_GATEWAY_TOKEN")
            && !rendered_session_config.contains("GG_MCP_CALLER_AGENT_ID"),
        "session-specific Codex config must not expose the legacy GG MCP bridge; config={}",
        session_config.display()
    );

    // The first native model turn is the system-owned onboarding task. A
    // direct user turn before that notice would correctly be rejected.
    let notice_deadline = std::time::Instant::now() + Duration::from_secs(120);
    let turn_id = loop {
        let notice_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v2/workspaces/{}/agents/{session_id}/onboarding",
                        registered.workspace.workspace_id
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("Codex onboarding notice read");
        assert_eq!(notice_response.status(), StatusCode::OK);
        let notice: runtime_core::WorkspaceSystemNoticeRecord = serde_json::from_slice(
            &to_bytes(notice_response.into_body(), usize::MAX)
                .await
                .expect("Codex onboarding notice body"),
        )
        .expect("Codex onboarding notice json");
        if notice.state == "delivered" {
            break notice.turn_id.expect("native onboarding turn identity");
        }
        assert!(
            matches!(notice.state.as_str(), "pending" | "injecting")
                && std::time::Instant::now() < notice_deadline,
            "Codex onboarding did not reach native admission: {notice:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let mut event_cursor = 0_i64;
    let mut terminal_event: Option<serde_json::Value> = None;
    while std::time::Instant::now() < deadline {
        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/sessions/{session_id}/events?after_seq={event_cursor}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("Codex GG MCP events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events: Vec<serde_json::Value> = serde_json::from_slice(
            &to_bytes(events_response.into_body(), usize::MAX)
                .await
                .expect("Codex GG MCP events body"),
        )
        .expect("Codex GG MCP events json");

        if let Some(last_seq) = events
            .iter()
            .filter_map(|event| event.get("seq").and_then(serde_json::Value::as_i64))
            .max()
        {
            event_cursor = last_seq;
        }
        for event in events {
            if event.get("turn_id").and_then(serde_json::Value::as_str) != Some(turn_id.as_str()) {
                continue;
            }
            if matches!(
                event.get("kind").and_then(serde_json::Value::as_str),
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

    let terminal_event = terminal_event.expect("Codex GG MCP turn must reach terminal state");
    assert_eq!(
        terminal_event
            .get("kind")
            .and_then(serde_json::Value::as_str),
        Some("turn.completed"),
        "Codex GG MCP turn did not complete successfully: {terminal_event}"
    );
    let terminal_text = terminal_event
        .get("payload")
        .and_then(|payload| payload.get("assistant_text"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            terminal_event
                .get("payload")
                .and_then(|payload| payload.get("usage"))
                .and_then(|usage| usage.get("last_message"))
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or_default();
    assert!(
        terminal_text.contains(completion_token),
        "Codex GG MCP terminal text missing completion token; text={terminal_text}"
    );
    assert!(
        terminal_text.contains(hook_token),
        "native Codex model did not receive approved pre-add hook onboarding context; text={terminal_text}"
    );

    // `run` admits work asynchronously. The model can complete its initial
    // turn before the scheduler launches the process, so poll the *operator*
    // process view (not another model/tool turn) until the durable record is
    // terminal. This also proves an actual native tool invocation happened.
    let process_deadline = std::time::Instant::now() + Duration::from_secs(40);
    let tool_process = loop {
        let processes_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes?session_id={session_id}&include_completed=true"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("list Codex-owned processes");
        assert_eq!(processes_response.status(), StatusCode::OK);
        let observed_processes = serde_json::from_slice::<Vec<runtime_core::ProcessSummary>>(
            &to_bytes(processes_response.into_body(), usize::MAX)
                .await
                .expect("Codex process list body"),
        )
        .expect("Codex process list json");
        if let Some(process) = observed_processes
            .iter()
            .find(|process| process.command.to_string().contains(marker_token))
        {
            if runtime_core::process_status_is_terminal(&process.status) {
                break process.clone();
            }
        }
        assert!(
            std::time::Instant::now() < process_deadline,
            "native gg_process never reached terminal state; observed={observed_processes:?}, turn={terminal_event}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!(
        tool_process.session_id.as_deref(),
        Some(session_id.as_str())
    );
    assert_eq!(tool_process.status, "completed");
    assert_eq!(
        tool_process
            .command
            .get("command")
            .and_then(serde_json::Value::as_str),
        Some(tool_command.as_str()),
        "native GG process must execute the exact requested command"
    );
    assert!(
        marker_path.is_file(),
        "native GG process did not create its marker at {}; process={tool_process:?}",
        marker_path.display()
    );

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
        .expect("close Codex GG MCP smoke session");
    assert_eq!(close_response.status(), StatusCode::OK);
    smoke_server_handle.abort();
}

pub(super) async fn create_test_session(router: Router, token: &str, suite: &str) -> String {
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
                        "model": codex_test_model(),
                        "metadata": {"suite": suite}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create session response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("create session body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("create session json");
    json["id"].as_str().expect("session id").to_string()
}

pub(super) fn codex_test_model() -> String {
    "gpt-6-luna".to_string()
}

// Keep the legacy /v1 and automation-driven live turns on the same model
// and effort as the workspace-owned /v2 smokes. The legacy create API does
// not accept session preferences, so the disposable Codex home supplies
// native defaults inherited by each per-session app-server home.
pub(super) fn prepare_codex_smoke_config(config: &RuntimeServerConfig) {
    let home = config.resolve_provider_dir("codex").join("home");
    std::fs::create_dir_all(&home).expect("create disposable Codex smoke home");
    std::fs::write(
        home.join("config.toml"),
        "model = \"gpt-6-luna\"\nmodel_reasoning_effort = \"low\"\n",
    )
    .expect("write Codex Luna/low smoke config");
}

#[test]
fn codex_live_smoke_defaults_are_luna_low() {
    assert_eq!(codex_test_model(), "gpt-6-luna");
    let temp = tempfile::tempdir().expect("smoke fixture temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp.path().to_path_buf();
    prepare_codex_smoke_config(&config);
    let rendered = std::fs::read_to_string(
        config
            .resolve_provider_dir("codex")
            .join("home/config.toml"),
    )
    .expect("disposable native config");
    let native_config: toml::Value = toml::from_str(&rendered).expect("native config TOML");
    assert_eq!(native_config["model"].as_str(), Some("gpt-6-luna"));
    assert_eq!(
        native_config["model_reasoning_effort"].as_str(),
        Some("low")
    );
}
