use super::*;
use runtime_core::{RuntimeEventCriticality, RuntimeEventScope};

async fn invoke(
    router: &Router,
    bearer: &str,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    let contents = if let Some(body) = body {
        request = request.header(header::CONTENT_TYPE, "application/json");
        Body::from(body.to_string())
    } else {
        Body::empty()
    };
    let response = router
        .clone()
        .oneshot(request.body(contents).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::json!({"text":String::from_utf8_lossy(&bytes)}));
    (status, value)
}

#[tokio::test]
async fn native_session_lifecycle_http_and_reopened_sqlite_share_three_critical_receipts() {
    let (router, bearer, tmp, provider, _) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    provider.allow_lifecycle_close_for_test();
    let (status, created) = invoke(
        &router,
        &bearer,
        "POST",
        "/v1/sessions",
        Some(serde_json::json!({
            "provider":"codex", "model":"test-model", "cwd":null,
            "permission_mode":null,"metadata":{"test":"journal_session_lifecycle"}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let session_id = created["id"].as_str().unwrap();
    let (status, resumed) = invoke(
        &router,
        &bearer,
        "POST",
        &format!("/v1/sessions/{session_id}/resume"),
        Some(serde_json::json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resumed}");
    assert_eq!(resumed["id"], session_id);
    let (status, closed) = invoke(
        &router,
        &bearer,
        "POST",
        &format!("/v1/sessions/{session_id}/close"),
        Some(serde_json::json!({"reason":"offline_http_test"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{closed}");
    assert_eq!(closed["status"], "closed");

    let reopened =
        runtime_store_sqlite::SqliteRuntimeRepository::new(tmp.path().join("runtime.sqlite3"));
    let stored = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(
        stored
            .sessions
            .iter()
            .find(|record| record.id == session_id)
            .unwrap()
            .status,
        "closed"
    );
    let events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, session_id)), None, 100)
        .unwrap();
    let lifecycle = events
        .into_iter()
        .filter(|row| {
            matches!(
                row.kind.as_str(),
                "session.created" | "session.resumed" | "session.closed"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        lifecycle
            .iter()
            .map(|row| row.kind.as_str())
            .collect::<Vec<_>>(),
        ["session.created", "session.resumed", "session.closed"]
    );
    for event in &lifecycle {
        assert_eq!(event.criticality, RuntimeEventCriticality::Critical);
        let (status, operator_detail) = invoke(
            &router,
            &bearer,
            "GET",
            &format!("/v2/events/{}", event.event_id),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{operator_detail}");
        assert_eq!(operator_detail["event_id"], event.event_id);
        assert_eq!(operator_detail["payload"], event.payload);
    }
    assert_eq!(
        lifecycle[0].event_id,
        format!("evt_session_{session_id}_created")
    );
    assert!(lifecycle[1]
        .event_id
        .starts_with(&format!("evt_session_{session_id}_resumed_")));
    assert_eq!(
        lifecycle[2].event_id,
        format!("evt_session_{session_id}_closed")
    );
    assert_eq!(lifecycle[2].payload["reason"], "offline_http_test");

    let (status, repeated) = invoke(
        &router,
        &bearer,
        "POST",
        &format!("/v1/sessions/{session_id}/close"),
        Some(serde_json::json!({"reason":"different_late_reason"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{repeated}");
    assert_eq!(repeated, closed);
    let restarted_events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, session_id)), None, 100)
        .unwrap();
    assert_eq!(
        restarted_events
            .iter()
            .filter(|row| row.kind == "session.closed")
            .count(),
        1,
        "native close must not be dispatched twice on an already-closed session"
    );
}
