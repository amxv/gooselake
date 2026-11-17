use super::*;
use std::sync::atomic::Ordering;

async fn post(
    router: &Router,
    token: &str,
    uri: &str,
    key: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn fixture() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    String,
    String,
) {
    let (router, token, temp, provider, runtime, _app) =
        build_test_router_with_services(TeamMcpPolicy::default()).await;
    let (status, source) = legacy_session_fixture_json(
        &runtime,
        serde_json::json!({
            "provider":"codex","model":"test-model","cwd":temp.path(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source}");
    let source_id = source["id"].as_str().unwrap().to_string();
    let (status, team) = post(
        &router,
        &token,
        "/v1/teams",
        None,
        serde_json::json!({"name":"Historical inspection only","lead_agent_id":source_id}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{team}");
    let team_id = team["team"]["id"].as_str().unwrap().to_string();
    (router, token, temp, provider, source_id, team_id)
}

fn counts(db: &rusqlite::Connection) -> Vec<i64> {
    [
        "sessions",
        "teams",
        "team_members",
        "team_spawn_admissions",
        "team_operation_journal",
        "team_messages",
        "team_deliveries",
        "managed_worktrees",
        "managed_worktree_claims",
        "runtime_events",
    ]
    .iter()
    .map(|table| {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    })
    .collect()
}

async fn native_calls(provider: &TestProvider) -> [usize; 4] {
    [
        provider.create_call_count(),
        provider.archive_close_count(),
        provider.restore_call_count().await,
        provider.send_invocations.load(Ordering::SeqCst),
    ]
}

#[tokio::test]
async fn fresh_legacy_http_spawn_refuses_keyed_and_unkeyed_without_any_effect() {
    let (router, token, temp, provider, source, team) = fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let before = counts(&db);
    let native_before = native_calls(&provider).await;
    for key in [Some("fresh-refused-key"), None] {
        let (status, body) = post(
            &router,
            &token,
            &format!("/v1/teams/{team}/members/spawn"),
            key,
            serde_json::json!({"source_session_id":source,"title":"Must not spawn"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body["error"].as_str().unwrap().contains("legacy"), "{body}");
        assert_eq!(counts(&db), before);
        assert_eq!(native_calls(&provider).await, native_before);
    }
}

#[tokio::test]
async fn fresh_split_mcp_add_refuses_keyed_and_unkeyed_without_any_effect() {
    let (router, token, temp, provider, source, team) = fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let before = counts(&db);
    let native_before = native_calls(&provider).await;
    for key in [Some("fresh-refused-tool-key"), None] {
        let (status, body) = post(
            &router,
            &token,
            "/v1/mcp/invoke",
            None,
            serde_json::json!({
                "namespace":"gg_team","tool_name":"gg_team_manage","caller_agent_id":source,
                "invocation_id":key,"args":{"team_id":team,"title":"Must not spawn"},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["ok"], false, "{body}");
        assert_eq!(body["error"]["code"], "unsupported_capability", "{body}");
        assert_eq!(counts(&db), before);
        assert_eq!(native_calls(&provider).await, native_before);
    }
}
