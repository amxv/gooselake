use super::operator_event_test_support::{
    assert_error, different_hex, fixture_cursor, replace_field,
};
use super::*;
use runtime_core::{EventCollection, EventCursor};
use runtime_store_sqlite::SqliteRuntimeRepository;
use std::collections::HashSet;

async fn operator_event_get(
    router: &Router,
    token: Option<&str>,
    uri: &str,
) -> (StatusCode, Option<String>, serde_json::Value) {
    let mut request = Request::builder().uri(uri);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let cursor = response
        .headers()
        .get("x-next-cursor")
        .map(|header| header.to_str().unwrap().to_owned());
    let body =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, cursor, body)
}

fn assert_next_cursor(next: Option<&str>, body: &serde_json::Value, collection: &EventCollection) {
    if let Some(raw) = next {
        let row = body.as_array().unwrap().last().unwrap();
        let cursor = EventCursor::decode(raw).unwrap();
        assert_eq!(cursor.collection(), collection);
        assert_eq!(cursor.position(), row["row_id"].as_i64().unwrap());
        cursor
            .validate_anchor(Some(row["event_id"].as_str().unwrap()))
            .unwrap();
    }
}

#[tokio::test]
async fn operator_event_history_scopes_are_indexed_bounded_and_detail_is_explicit() {
    let (router, token, temp) = build_test_router().await;
    let mut db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let tx = db.transaction().unwrap();
    for index in 0..8i64 {
        let (scope, scope_id) = if index < 5 {
            ("workspace", "ws_operator_alpha")
        } else {
            ("process", "proc_operator_beta")
        };
        tx.execute(
            "INSERT INTO runtime_events (
                event_id, scope, scope_id, seq, kind, critical, payload_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, 'operator.fixture', 1, ?5, 100)",
            rusqlite::params![
                format!("evt_operator_{index}"),
                scope,
                scope_id,
                if index < 5 { index + 1 } else { index - 4 },
                serde_json::json!({"secret":"private original event payload"}).to_string(),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let total: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();

    let (status, _, unauth) = operator_event_get(&router, None, "/v2/events").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauth["error"]["code"], "unauthorized");

    let mut cursor: Option<String> = None;
    let mut last_id = 0;
    let mut seen = HashSet::new();
    for _ in 0..100 {
        let limit = if cursor.is_none() { 2 } else { 3 };
        let uri = cursor.as_deref().map_or_else(
            || format!("/v2/events?limit={limit}"),
            |value| format!("/v2/events?limit={limit}&cursor={value}"),
        );
        let (status, next, body) = operator_event_get(&router, Some(&token), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().unwrap();
        assert!(!rows.is_empty() && rows.len() <= limit);
        for row in rows {
            let id = row["row_id"].as_i64().unwrap();
            assert!(id > last_id);
            last_id = id;
            assert!(seen.insert(row["event_id"].as_str().unwrap().to_string()));
            assert_eq!(row.as_object().unwrap().len(), 8);
            for field in [
                "payload",
                "provider",
                "provider_seq",
                "session_id",
                "turn_id",
            ] {
                assert!(row.get(field).is_none(), "leaked {field}");
            }
        }
        assert_next_cursor(
            next.as_deref(),
            &body,
            &EventCollection::history(None).unwrap(),
        );
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), total as usize);
    assert!(seen.contains("evt_operator_0"));
    assert!(seen.contains("evt_operator_7"));

    let mut cursor: Option<String> = None;
    let mut scoped_seen = HashSet::new();
    for _ in 0..10 {
        let limit = if cursor.is_none() { 2 } else { 3 };
        let uri = cursor.as_deref().map_or_else(
            || format!("/v2/events?scope=workspace&scope_id=ws_operator_alpha&limit={limit}"),
            |value| {
                format!(
                    "/v2/events?scope=workspace&scope_id=ws_operator_alpha&limit={limit}&cursor={value}"
                )
            },
        );
        let (status, next, body) = operator_event_get(&router, Some(&token), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().unwrap();
        for row in rows {
            assert_eq!(row["scope"], "workspace");
            assert_eq!(row["scope_id"], "ws_operator_alpha");
            assert!(scoped_seen.insert(row["event_id"].as_str().unwrap().to_string()));
        }
        assert_next_cursor(
            next.as_deref(),
            &body,
            &EventCollection::history(Some((
                RuntimeEventScope::Workspace,
                "ws_operator_alpha".into(),
            )))
            .unwrap(),
        );
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(scoped_seen.len(), 5);

    let workspace_cursor: i64 = db
        .query_row(
            "SELECT id FROM runtime_events WHERE event_id = 'evt_operator_0'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let process_cursor: i64 = db
        .query_row(
            "SELECT id FROM runtime_events WHERE event_id = 'evt_operator_7'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let workspace_row = repository
        .get_operator_event("evt_operator_0")
        .unwrap()
        .unwrap();
    let process_row = repository
        .get_operator_event("evt_operator_7")
        .unwrap()
        .unwrap();
    assert_eq!(workspace_row.row_id, workspace_cursor);
    assert_eq!(process_row.row_id, process_cursor);
    let global = EventCollection::history(None).unwrap();
    let workspace = EventCollection::history(Some((
        RuntimeEventScope::Workspace,
        "ws_operator_alpha".into(),
    )))
    .unwrap();
    let process = EventCollection::history(Some((
        RuntimeEventScope::Process,
        "proc_operator_beta".into(),
    )))
    .unwrap();
    let workspace_stream = EventCollection::stream(Some((
        RuntimeEventScope::Workspace,
        "ws_operator_alpha".into(),
    )))
    .unwrap();
    let global_token = fixture_cursor(&repository, &global, &process_row);
    let wrong_epoch = different_hex(global_token.split('.').nth(1).unwrap());
    let wrong_digest = different_hex(global_token.split('.').nth(6).unwrap());
    for (uri, status, code) in [
        (
            format!(
                "/v2/events?cursor={}",
                replace_field(&global_token, 5, &(process_cursor + 10_000).to_string())
            ),
            StatusCode::BAD_REQUEST,
            "event_cursor_anchor_mismatch",
        ),
        (
            format!(
                "/v2/events?scope=workspace&scope_id=ws_operator_alpha&cursor={}",
                fixture_cursor(&repository, &process, &process_row)
            ),
            StatusCode::BAD_REQUEST,
            "event_cursor_collection_mismatch",
        ),
        (
            format!(
                "/v2/events?scope=workspace&scope_id=ws_operator_alpha&cursor={}",
                fixture_cursor(&repository, &workspace, &process_row)
            ),
            StatusCode::BAD_REQUEST,
            "event_cursor_anchor_mismatch",
        ),
        (
            format!(
                "/v2/events?scope=workspace&scope_id=ws_operator_alpha&cursor={}",
                fixture_cursor(&repository, &workspace_stream, &workspace_row)
            ),
            StatusCode::BAD_REQUEST,
            "event_cursor_collection_mismatch",
        ),
        (
            format!(
                "/v2/events?cursor={}",
                replace_field(&global_token, 1, &wrong_epoch)
            ),
            StatusCode::CONFLICT,
            "event_cursor_epoch_mismatch",
        ),
        (
            format!(
                "/v2/events?cursor={}",
                replace_field(&global_token, 6, &wrong_digest)
            ),
            StatusCode::BAD_REQUEST,
            "event_cursor_anchor_mismatch",
        ),
    ] {
        assert_error(&router, &token, &uri, &[], status, code).await;
    }
    for uri in [
        "/v2/events?cursor=",
        "/v2/events?cursor=0",
        "/v2/events?scope=workspace&scope_id=ws_operator_alpha&cursor=0",
    ] {
        assert_error(
            &router,
            &token,
            uri,
            &[],
            StatusCode::BAD_REQUEST,
            "event_cursor_invalid",
        )
        .await;
    }

    let (status, _, detail) =
        operator_event_get(&router, Some(&token), "/v2/events/evt_operator_0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        detail["payload"]["secret"],
        "private original event payload"
    );
    let (status, _, missing) =
        operator_event_get(&router, Some(&token), "/v2/events/evt_nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["error"]["code"], "not_found");

    for invalid in [
        "/v2/events?scope=workspace",
        "/v2/events?scope_id=ws_operator_alpha",
        "/v2/events?scope=imaginary&scope_id=abc",
        "/v2/events?scope=workspace&scope_id=",
        "/v2/events?cursor=-1",
        "/v2/events?cursor=not-an-integer",
        "/v2/events?limit=0",
        "/v2/events?limit=101",
        "/v2/events?other=anything",
    ] {
        let (status, _, body) = operator_event_get(&router, Some(&token), invalid).await;
        assert!(status.is_client_error(), "{invalid}: {status}");
        assert!(body["error"]["code"].is_string());
    }
    let unsupported = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/events")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unsupported.status(), StatusCode::METHOD_NOT_ALLOWED);
}
