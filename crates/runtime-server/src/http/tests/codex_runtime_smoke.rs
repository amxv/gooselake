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
            "smoke": "real_codex_phase3"
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
                "text": "Reply with exactly this token and nothing else: phase3token_94731"
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
            last_message.contains("phase3token_94731"),
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

    // Simulate restart and verify persisted session can be resumed and used.
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
    assert_eq!(resume_response.status(), StatusCode::OK);

    let third_turn_body = serde_json::json!({
        "input": [
            {
                "type": "text",
                "text": "After resume, reply with exactly this token and nothing else: phase3token_94731"
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
        resume_message.contains("phase3token_94731"),
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
    std::env::var("GG_CODEX_SMOKE_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "gpt-5.4-mini".to_string())
}
