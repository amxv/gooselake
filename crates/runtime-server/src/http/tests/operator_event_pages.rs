use super::*;
use std::collections::HashSet;

async fn operator_event_get(
    router: &Router,
    token: Option<&str>,
    uri: &str,
) -> (StatusCode, Option<i64>, serde_json::Value) {
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
        .map(|header| header.to_str().unwrap().parse::<i64>().unwrap());
    let body =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, cursor, body)
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

    let mut cursor = None;
    let mut last_id = 0;
    let mut seen = HashSet::new();
    for _ in 0..100 {
        let uri = cursor.map_or_else(
            || "/v2/events?limit=2".to_string(),
            |value| format!("/v2/events?limit=2&cursor={value}"),
        );
        let (status, next, body) = operator_event_get(&router, Some(&token), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().unwrap();
        assert!(!rows.is_empty() && rows.len() <= 2);
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
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), total as usize);
    assert!(seen.contains("evt_operator_0"));
    assert!(seen.contains("evt_operator_7"));

    let mut cursor = None;
    let mut scoped_seen = HashSet::new();
    for _ in 0..10 {
        let uri = cursor.map_or_else(
            || "/v2/events?scope=workspace&scope_id=ws_operator_alpha&limit=2".to_owned(),
            |value| {
                format!(
                    "/v2/events?scope=workspace&scope_id=ws_operator_alpha&limit=2&cursor={value}"
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
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(scoped_seen.len(), 5);

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
