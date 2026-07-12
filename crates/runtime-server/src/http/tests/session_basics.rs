use super::*;

#[tokio::test]
async fn version_route_is_available() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");

    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let response = router
        .oneshot(
            Request::builder()
                .uri("/v1/version")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("version response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("version body");
    let json: serde_json::Value = serde_json::from_slice(&payload).expect("version json");
    assert_eq!(
        json.get("version").and_then(serde_json::Value::as_str),
        Some(env!("CARGO_PKG_VERSION"))
    );
}

#[tokio::test]
async fn source_bootstrap_route_returns_runtime_epoch_and_empty_watermark() {
    let (router, token, _temp_dir) = build_test_router().await;
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v1/bootstrap")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("bootstrap response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("bootstrap body");
    let bootstrap: runtime_core::RuntimeSourceBootstrap =
        serde_json::from_slice(&body).expect("bootstrap json");
    assert!(bootstrap.source_epoch.starts_with("src_"));
    assert_eq!(bootstrap.high_watermark, 0);
    assert!(bootstrap.records.sessions.is_empty());
}

#[tokio::test]
async fn session_stream_replays_from_cursor_before_live_events() {
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
        .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_payload = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .expect("create payload");
    let created: serde_json::Value = serde_json::from_slice(&create_payload).expect("create json");
    let session_id = created
        .get("id")
        .and_then(serde_json::Value::as_str)
        .expect("session id")
        .to_string();

    for text in ["first prompt", "what was my first prompt"] {
        let send_body = serde_json::json!({
            "input": [{ "type": "text", "text": text }],
            "expected_turn_id": null,
            "permission_mode": null
        });
        let send_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/turns"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(send_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("send response");
        assert_eq!(send_response.status(), StatusCode::OK);

        let mut idle = false;
        for _ in 0..50 {
            let session_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/sessions/{session_id}"))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("session response");
            let body = to_bytes(session_response.into_body(), usize::MAX)
                .await
                .expect("session body");
            let session: serde_json::Value = serde_json::from_slice(&body).expect("session json");
            if session
                .get("active_turn_id")
                .is_some_and(serde_json::Value::is_null)
            {
                idle = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(idle, "turn did not finish in time for replay test");
    }

    let replay_response = router
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
    let replay_payload = to_bytes(replay_response.into_body(), usize::MAX)
        .await
        .expect("events payload");
    let events: Vec<runtime_core::RuntimeEventRecord> =
        serde_json::from_slice(&replay_payload).expect("events json");
    assert!(
        events.len() >= 5,
        "expected at least session.created + 2 turn start/terminal pairs"
    );
    let cursor = events
        .iter()
        .find(|event| event.kind == "turn.completed")
        .map(|event| event.seq)
        .expect("turn.completed seq");
    let expected_ids = events
        .iter()
        .filter(|event| event.seq > cursor)
        .map(|event| event.seq.to_string())
        .collect::<Vec<_>>();
    assert!(
        !expected_ids.is_empty(),
        "expected replay window after cursor"
    );
    let recalled_message = events
        .iter()
        .filter_map(|event| event.payload.get("usage"))
        .filter_map(|usage| usage.get("last_message"))
        .filter_map(serde_json::Value::as_str)
        .find(|message| *message == "first prompt");
    assert_eq!(
        recalled_message,
        Some("first prompt"),
        "second turn should preserve context from the first turn"
    );

    let stream_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/sessions/{session_id}/events/stream?after_seq={cursor}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("stream response");
    assert_eq!(stream_response.status(), StatusCode::OK);

    let mut data_stream = stream_response.into_body().into_data_stream();
    let mut sse_payload = String::new();
    for _ in 0..8 {
        let next = timeout(Duration::from_secs(1), data_stream.next()).await;
        match next {
            Ok(Some(Ok(chunk))) => {
                sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                let all_present = expected_ids
                    .iter()
                    .all(|seq| sse_payload.contains(format!("id: {seq}").as_str()));
                if all_present {
                    break;
                }
            }
            _ => break,
        }
    }
    for seq in expected_ids {
        assert!(
            sse_payload.contains(format!("id: {seq}").as_str()),
            "missing replayed seq {seq} in SSE payload: {sse_payload}"
        );
    }
}

#[tokio::test]
async fn openapi_yaml_route_serves_snapshot() {
    let (router, token, _temp_dir) = build_test_router().await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/openapi.yaml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("openapi response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("openapi body");
    let body = String::from_utf8(payload.to_vec()).expect("openapi utf8");
    assert!(body.contains("openapi: 3.1.0"));
    assert!(body.contains("/v1/sessions"));
    assert!(body.contains("/v1/events/stream"));

    let protected_response = router
        .oneshot(
            Request::builder()
                .uri("/v1/openapi.yaml")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("protected openapi response");
    assert_eq!(protected_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn session_stream_replays_exhaustive_backlog_across_pages() {
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
        .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_payload = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .expect("create payload");
    let created: serde_json::Value = serde_json::from_slice(&create_payload).expect("create json");
    let session_id = created
        .get("id")
        .and_then(serde_json::Value::as_str)
        .expect("session id")
        .to_string();

    for index in 0..8 {
        let send_body = serde_json::json!({
            "input": [{ "type": "text", "text": format!("replay page turn {index}") }],
            "expected_turn_id": null,
            "permission_mode": null
        });
        let send_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/turns"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(send_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("send response");
        assert_eq!(send_response.status(), StatusCode::OK);

        let mut idle = false;
        for _ in 0..80 {
            let session_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/sessions/{session_id}"))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("session response");
            let body = to_bytes(session_response.into_body(), usize::MAX)
                .await
                .expect("session body");
            let session: serde_json::Value = serde_json::from_slice(&body).expect("session json");
            if session
                .get("active_turn_id")
                .is_some_and(serde_json::Value::is_null)
            {
                idle = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(idle, "turn {index} did not finish in time");
    }

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
    let events_payload = to_bytes(events_response.into_body(), usize::MAX)
        .await
        .expect("events payload");
    let events: Vec<runtime_core::RuntimeEventRecord> =
        serde_json::from_slice(&events_payload).expect("events json");
    assert!(
        events.len() > 10,
        "expected sizable backlog for pagination regression"
    );
    let cursor = 1_i64;
    let expected_ids = events
        .iter()
        .filter(|event| event.seq > cursor)
        .map(|event| event.seq)
        .collect::<Vec<_>>();
    assert!(
        expected_ids.len() > 8,
        "expected more than one replay page of missed events"
    );

    let stream_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/sessions/{session_id}/events/stream?after_seq={cursor}&limit=3"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("stream response");
    assert_eq!(stream_response.status(), StatusCode::OK);

    let mut data_stream = stream_response.into_body().into_data_stream();
    let mut sse_payload = String::new();
    for _ in 0..80 {
        let next = timeout(Duration::from_millis(300), data_stream.next()).await;
        match next {
            Ok(Some(Ok(chunk))) => {
                sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                let all_present = expected_ids
                    .iter()
                    .all(|seq| sse_payload.contains(format!("id: {seq}\n").as_str()));
                if all_present {
                    break;
                }
            }
            _ => break,
        }
    }

    for seq in expected_ids {
        assert!(
            sse_payload.contains(format!("id: {seq}\n").as_str()),
            "missing replay backlog seq {seq} in paged stream payload"
        );
    }
}

#[tokio::test]
async fn session_stream_handoff_window_event_is_not_lost() {
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
        .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_payload = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .expect("create payload");
    let created: serde_json::Value = serde_json::from_slice(&create_payload).expect("create json");
    let session_id = created
        .get("id")
        .and_then(serde_json::Value::as_str)
        .expect("session id")
        .to_string();

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
    let events_payload = to_bytes(events_response.into_body(), usize::MAX)
        .await
        .expect("events payload");
    let events: Vec<runtime_core::RuntimeEventRecord> =
        serde_json::from_slice(&events_payload).expect("events json");
    let cursor = events.last().map(|event| event.seq).unwrap_or(0);

    let stream_router = router.clone();
    let stream_token = token.clone();
    let stream_session_id = session_id.clone();
    let stream_handle = tokio::spawn(async move {
        stream_router
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/sessions/{stream_session_id}/events/stream?after_seq={cursor}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {stream_token}"))
                    .header("x-gg-test-handoff-delay-ms", "300")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    let send_body = serde_json::json!({
        "input": [{ "type": "text", "text": "handoff window message" }],
        "expected_turn_id": null,
        "permission_mode": null
    });
    let send_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(send_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("send response");
    assert_eq!(send_response.status(), StatusCode::OK);

    let stream_response = stream_handle
        .await
        .expect("stream task join")
        .expect("stream response");
    assert_eq!(stream_response.status(), StatusCode::OK);

    let mut data_stream = stream_response.into_body().into_data_stream();
    let mut sse_payload = String::new();
    for _ in 0..8 {
        let next = timeout(Duration::from_secs(1), data_stream.next()).await;
        match next {
            Ok(Some(Ok(chunk))) => {
                sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                if sse_payload.contains("event: turn.started")
                    || sse_payload.contains("event: turn.completed")
                {
                    break;
                }
            }
            _ => break,
        }
    }

    assert!(
        sse_payload.contains("event: turn.started")
            || sse_payload.contains("event: turn.completed"),
        "expected handoff-window event to be delivered in stream payload: {sse_payload}"
    );
}

#[tokio::test]
async fn health_route_is_public() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");

    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: bootstrapped.auth.bearer_token,
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let response = router
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn protected_route_requires_token() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");

    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = router
        .oneshot(
            Request::builder()
                .uri("/v1/health")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(authorized.status(), StatusCode::OK);
}

#[tokio::test]
async fn diagnostics_routes_return_structured_runtime_state() {
    let (router, token, _temp_dir) = build_test_router().await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("diagnostics response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("diagnostics body");
    let json: Value = serde_json::from_slice(body.as_ref()).expect("diagnostics json");
    assert!(json.get("providers").is_some());
    assert!(json.get("comms").is_some());
    assert!(json.get("processes").is_some());
    assert!(json.get("worktrees").is_some());
    assert!(json.get("recovery").is_some());

    let recovery = router
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/recovery")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("recovery diagnostics response");
    assert_eq!(recovery.status(), StatusCode::OK);
    let recovery_body = to_bytes(recovery.into_body(), usize::MAX)
        .await
        .expect("recovery body");
    let recovery_json: Value =
        serde_json::from_slice(recovery_body.as_ref()).expect("recovery json");
    assert!(recovery_json.get("startup").is_some());
    assert!(recovery_json.get("active_anomalies").is_some());
}

#[tokio::test]
async fn acp_auth_status_returns_not_found_when_provider_is_not_registered() {
    let (router, token, _temp_dir) = build_test_router().await;
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v1/providers/acp/auth/status")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("acp auth status response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("acp auth status body");
    let json: Value = serde_json::from_slice(body.as_ref()).expect("acp auth status json");
    assert_eq!(json["error"], "acp");
}

#[tokio::test]
async fn acp_auth_status_and_diagnostics_are_exposed_when_provider_is_registered() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.codex.enabled = false;
    config.providers.claude.enabled = false;
    config.providers.acp.enabled = true;
    config.providers.acp.command = Some("fake-acp-agent".to_string());

    let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/providers/acp/auth/status")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("acp auth status response");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_body = to_bytes(status_response.into_body(), usize::MAX)
        .await
        .expect("acp auth status body");
    let status_json: Value =
        serde_json::from_slice(status_body.as_ref()).expect("acp auth status json");
    assert_eq!(status_json["authenticated"], false);
    assert_eq!(status_json["mode"], "agent_managed");
    assert!(status_json["detail"]
        .as_str()
        .is_some_and(|detail| detail.contains("agent-managed")));

    let diagnostics_response = router
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/providers")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("provider diagnostics response");
    assert_eq!(diagnostics_response.status(), StatusCode::OK);
    let diagnostics_body = to_bytes(diagnostics_response.into_body(), usize::MAX)
        .await
        .expect("provider diagnostics body");
    let diagnostics_json: Value =
        serde_json::from_slice(diagnostics_body.as_ref()).expect("provider diagnostics json");
    let acp_entry = diagnostics_json["providers"]
        .as_array()
        .and_then(|providers| {
            providers
                .iter()
                .find(|entry| entry["provider"].as_str() == Some("acp"))
        })
        .expect("acp provider diagnostics entry");
    assert_eq!(acp_entry["healthy"], true);
    assert_eq!(acp_entry["auth_status"]["mode"], "agent_managed");
    assert!(acp_entry["auth_error"].is_null());
}

#[tokio::test]
async fn acp_provider_public_surfaces_support_list_models_session_replay_and_stream() {
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

    let providers_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/providers")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("providers response");
    assert_eq!(providers_response.status(), StatusCode::OK);
    let providers_json: Value = serde_json::from_slice(
        &to_bytes(providers_response.into_body(), usize::MAX)
            .await
            .expect("providers body"),
    )
    .expect("providers json");
    let providers = providers_json["providers"]
        .as_array()
        .expect("providers array");
    assert!(providers
        .iter()
        .any(|entry| entry["kind"].as_str() == Some("acp")));

    let models_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/providers/acp/models")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("models response");
    assert_eq!(models_response.status(), StatusCode::OK);
    let models_json: Value = serde_json::from_slice(
        &to_bytes(models_response.into_body(), usize::MAX)
            .await
            .expect("models body"),
    )
    .expect("models json");
    assert_eq!(models_json["provider"].as_str(), Some("acp"));
    assert_eq!(
        models_json["models"].as_array().map(Vec::len),
        Some(0),
        "ACP model list is allowed to be empty in v1"
    );

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
                        "cwd": "/tmp/acp-http-surface",
                        "permission_mode": null,
                        "metadata": {}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: Value = serde_json::from_slice(
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
                        "input": [{"type":"text","text":"runtime acp http"}],
                        "expected_turn_id": null,
                        "permission_mode": null
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("send response");
    assert_eq!(send_response.status(), StatusCode::OK);

    let mut events = Vec::<runtime_core::RuntimeEventRecord>::new();
    for _ in 0..40 {
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
        events = serde_json::from_slice(
            &to_bytes(events_response.into_body(), usize::MAX)
                .await
                .expect("events body"),
        )
        .expect("events json");
        if events.iter().any(|event| event.kind == "turn.completed") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let completed = events
        .iter()
        .find(|event| event.kind == "turn.completed")
        .expect("turn.completed event");
    let assistant_text = completed
        .payload
        .get("assistant_text")
        .or_else(|| {
            completed
                .payload
                .get("usage")
                .and_then(|usage| usage.get("last_message"))
        })
        .and_then(Value::as_str);
    assert_eq!(assistant_text, Some("Echo: runtime acp http"));
    let replay_cursor = events
        .iter()
        .find(|event| event.kind == "turn.started")
        .map(|event| event.seq.saturating_sub(1))
        .expect("turn.started seq");

    let stream_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/sessions/{session_id}/events/stream?after_seq={replay_cursor}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("stream response");
    assert_eq!(stream_response.status(), StatusCode::OK);
    let mut data_stream = stream_response.into_body().into_data_stream();
    let mut sse_payload = String::new();
    for _ in 0..8 {
        let next = timeout(Duration::from_secs(1), data_stream.next()).await;
        match next {
            Ok(Some(Ok(chunk))) => {
                sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                if sse_payload.contains("event: turn.completed")
                    && sse_payload.contains("Echo: runtime acp http")
                {
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(sse_payload.contains("event: turn.started"));
    assert!(sse_payload.contains("event: turn.completed"));
    assert!(sse_payload.contains("Echo: runtime acp http"));
}

#[tokio::test]
async fn sse_stream_rejects_invalid_last_event_id_header() {
    let (router, token, _temp_dir) = build_test_router().await;
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v1/events/stream")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("last-event-id", "not-an-integer")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("sse response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: Value = serde_json::from_slice(body.as_ref()).expect("json");
    assert_eq!(
        json.get("error").and_then(Value::as_str),
        Some("invalid last-event-id header; expected integer")
    );
}
