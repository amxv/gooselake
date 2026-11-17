use super::*;

#[tokio::test]
async fn authenticated_legacy_spawn_key_replays_exact_http_response_and_conflicts_on_change() {
    let (router, token, temp, provider, runtime, app) =
        build_test_router_with_services(TeamMcpPolicy::default()).await;
    let lead = legacy_session_fixture_response(
        &runtime,
        serde_json::json!({
            "provider":"codex", "model":"test-model",
            "cwd": temp.path().display().to_string(),
        }),
    )
    .await;
    assert_eq!(lead.status(), StatusCode::OK);
    let lead: Value =
        serde_json::from_slice(&to_bytes(lead.into_body(), usize::MAX).await.unwrap()).unwrap();
    let source = lead["id"].as_str().unwrap();

    let team = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name":"Operator keyed spawn", "lead_agent_id":source,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(team.status(), StatusCode::OK);
    let team: Value =
        serde_json::from_slice(&to_bytes(team.into_body(), usize::MAX).await.unwrap()).unwrap();
    let team_id = team["team"]["id"].as_str().unwrap();
    let uri = format!("/v1/teams/{team_id}/members/spawn");
    let body = serde_json::json!({
        "source_session_id":source,
        "title":"one-time worker",
        "prompt":"No duplicate provider sessions",
    });

    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "http-once")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let creates_before_seed = provider.create_call_count();
    let (status, saved) =
        legacy_spawn_fixture_json(&app, team_id, Some("http-once"), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(
        provider.create_call_count(),
        creates_before_seed,
        "historical seed is database-only"
    );
    let (router, _reopened_runtime, _reopened_app) =
        reopen_test_router_with_services(&app, temp.path(), &token, TeamMcpPolicy::default()).await;

    let first = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("Idempotency-Key", "http-once")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first: Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(first, saved, "HTTP exposes the original saved receipt");
    assert_eq!(first["journal_stage"], "completed");
    let original_id = first["operation_id"].as_str().unwrap();
    assert!(original_id.starts_with("op_spawn_"));

    let replay = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("Idempotency-Key", "http-once")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replay: Value =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(replay, first);

    let mut changed = body.clone();
    changed["prompt"] = Value::String("changed but same key".into());
    let conflict = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("Idempotency-Key", "http-once")
                .body(Body::from(changed.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    let duplicate_headers = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("Idempotency-Key", "some-key")
                .header("Idempotency-Key", "other-key")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate_headers.status(), StatusCode::BAD_REQUEST);

    let state = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(state.status(), StatusCode::OK);
    let team: Value =
        serde_json::from_slice(&to_bytes(state.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(team["members"].as_array().unwrap().len(), 2);
}
