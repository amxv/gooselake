use super::*;

#[tokio::test]
async fn mcp_body_limit_is_scoped_to_mcp_routes_only() {
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
    let create_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    let session_id = create_json["id"].as_str().expect("session id").to_string();

    let oversized = "x".repeat(MCP_MAX_REQUEST_BODY_BYTES + 4096);
    let oversized_mcp = serde_json::json!({
        "namespace": "gg_process",
        "tool_name": "gg_process_status",
        "caller_agent_id": session_id,
        "args": {
            "blob": oversized
        }
    });
    let oversized_mcp_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/mcp/invoke")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(oversized_mcp.to_string()))
                .unwrap(),
        )
        .await
        .expect("oversized mcp response");
    assert_eq!(
        oversized_mcp_response.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let oversized_metadata = "m".repeat(MCP_MAX_REQUEST_BODY_BYTES + 4096);
    let non_mcp_large_body = serde_json::json!({
        "provider": "codex",
        "model": "test-model",
        "cwd": null,
        "permission_mode": null,
        "metadata": {
            "oversized": oversized_metadata
        }
    });
    let non_mcp_response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(non_mcp_large_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("non mcp response");
    assert_eq!(non_mcp_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn max_concurrent_one_blocks_second_spawn_until_first_finishes() {
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
    let create_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    let session_id = create_json["id"].as_str().expect("session id").to_string();

    let first_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/processes")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "command": "sleep 1",
                        "session_id": session_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("first process");
    assert_eq!(first_response.status(), StatusCode::OK);
    let first_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(first_response.into_body(), usize::MAX)
            .await
            .expect("first body"),
    )
    .expect("first json");
    let first_process_id = first_json
        .pointer("/process/process_id")
        .and_then(serde_json::Value::as_str)
        .expect("first process id")
        .to_string();

    let second_router = router.clone();
    let second_token = token.clone();
    let second_session = session_id.clone();
    let mut second_handle = tokio::spawn(async move {
        second_router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/processes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {second_token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "command": "seq 1 500000",
                            "session_id": second_session,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
    });

    let early = timeout(Duration::from_millis(150), &mut second_handle).await;
    assert!(
        early.is_err(),
        "second process started too early before slot became available"
    );

    let mut first_done = false;
    for _ in 0..80 {
        let get_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{first_process_id}?session_id={session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("first get");
        let body = to_bytes(get_response.into_body(), usize::MAX)
            .await
            .expect("first get body");
        let row: serde_json::Value = serde_json::from_slice(&body).expect("first get json");
        let status = row
            .pointer("/process/status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if status == "completed" {
            first_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(first_done, "first process did not complete in time");

    let second_response = second_handle
        .await
        .expect("second join")
        .expect("second response");
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(second_response.into_body(), usize::MAX)
            .await
            .expect("second body"),
    )
    .expect("second json");
    let second_process_id = second_json
        .pointer("/process/process_id")
        .and_then(serde_json::Value::as_str)
        .expect("second process id")
        .to_string();

    let mut second_done = false;
    for _ in 0..240 {
        let get_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{second_process_id}?session_id={session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("second get");
        assert_eq!(get_response.status(), StatusCode::OK);
        let body = to_bytes(get_response.into_body(), usize::MAX)
            .await
            .expect("second get body");
        let row: serde_json::Value = serde_json::from_slice(&body).expect("second get json");
        let status = row
            .pointer("/process/status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
            second_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        second_done,
        "second process did not complete after first released the slot"
    );
}

#[tokio::test]
async fn process_events_stream_delivers_live_sampled_output() {
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
    let create_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    let session_id = create_json["id"].as_str().expect("session id").to_string();

    let run_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/processes")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "command": "seq 1 200000",
                        "session_id": session_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("run process");
    assert_eq!(run_response.status(), StatusCode::OK);
    let run_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(run_response.into_body(), usize::MAX)
            .await
            .expect("run body"),
    )
    .expect("run json");
    let process_id = run_json
        .pointer("/process/process_id")
        .and_then(serde_json::Value::as_str)
        .expect("process id")
        .to_string();

    let replay_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/processes/{process_id}/events?session_id={session_id}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("replay response");
    assert_eq!(replay_response.status(), StatusCode::OK);
    let replay_events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
        &to_bytes(replay_response.into_body(), usize::MAX)
            .await
            .expect("replay body"),
    )
    .expect("replay json");
    let cursor = replay_events.last().map(|event| event.seq).unwrap_or(0);

    let stream_router = router.clone();
    let stream_token = token.clone();
    let stream_process_id = process_id.clone();
    let stream_session_id = session_id.clone();
    let stream_handle = tokio::spawn(async move {
        stream_router
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/processes/{stream_process_id}/events/stream?session_id={stream_session_id}&after_seq={cursor}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {stream_token}"))
                        .header("x-gg-test-handoff-delay-ms", "200")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
    });

    let stream_response = stream_handle
        .await
        .expect("stream join")
        .expect("stream response");
    assert_eq!(stream_response.status(), StatusCode::OK);

    let mut data_stream = stream_response.into_body().into_data_stream();
    let mut payload = String::new();
    for _ in 0..60 {
        let next = timeout(Duration::from_millis(300), data_stream.next()).await;
        match next {
            Ok(Some(Ok(chunk))) => {
                payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                if payload.contains("event: process.output")
                    || payload.contains("event: process.completed")
                {
                    break;
                }
            }
            _ => break,
        }
    }

    assert!(
        payload.contains("event: process.output") || payload.contains("event: process.completed"),
        "expected live process event in stream payload: {payload}"
    );
}

#[tokio::test]
#[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json"]
async fn smoke_real_codex_mcp_process_run_with_staged_auth_copy() {
    let home_dir = std::env::var("HOME").expect("HOME must be set");
    let source_auth = std::path::PathBuf::from(home_dir)
        .join(".gg")
        .join("codex")
        .join("auth.json");
    assert!(source_auth.exists(), "missing {}", source_auth.display());

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.claude.enabled = false;
    config.providers.codex.enabled = true;

    let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");
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

    let create_body = serde_json::json!({
        "provider": "codex",
        "model": codex_test_model(),
        "cwd": temp_dir.path().display().to_string(),
        "permission_mode": null,
        "metadata": {"smoke":"phase4_mcp_process"}
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
    let create_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    let session_id = create_json["id"].as_str().expect("session id").to_string();

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
                        "caller_agent_id": session_id,
                        "invocation_id": "smoke_phase4_mcp",
                        "args": {
                            "command": "echo phase4_mcp_smoke_token_74211"
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

    let mut completed = false;
    for _ in 0..120 {
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
            completed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(completed, "process did not reach terminal state");

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
        .expect("logs");
    assert_eq!(logs_response.status(), StatusCode::OK);
    let logs_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(logs_response.into_body(), usize::MAX)
            .await
            .expect("logs body"),
    )
    .expect("logs json");
    let output = logs_json
        .as_array()
        .into_iter()
        .flat_map(|rows| rows.iter())
        .filter_map(|row| row.get("content"))
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        output.contains("phase4_mcp_smoke_token_74211"),
        "missing expected process output in logs"
    );
}
