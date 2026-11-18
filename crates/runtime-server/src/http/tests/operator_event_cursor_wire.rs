use super::durable_event_stream_routes::new_event;
use super::operator_event_test_support::{
    assert_error as error, assert_terminal, different_hex, fixture_cursor, replace_field, request,
};
use super::*;
use runtime_core::{EventCollection, EventCursor, RuntimeEventRecord};
use runtime_store_sqlite::SqliteRuntimeRepository;

/// Retain partial chunks so the caller acknowledges exactly one wire frame.
pub(super) async fn read_bound_record(
    chunks: &mut axum::body::BodyDataStream,
    pending: &mut Vec<u8>,
    collection: &EventCollection,
    expected_position: i64,
    expected: &RuntimeEventRecord,
) -> String {
    let frame = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(end) = pending.windows(2).position(|bytes| bytes == b"\n\n") {
                let bytes: Vec<_> = pending.drain(..end + 2).collect();
                let frame = String::from_utf8(bytes).unwrap();
                if frame
                    .lines()
                    .all(|line| line.is_empty() || line.starts_with(':'))
                {
                    continue;
                }
                break frame;
            }
            let bytes = chunks
                .next()
                .await
                .expect("SSE closed before expected event")
                .expect("SSE body failed");
            pending.extend_from_slice(&bytes);
        }
    })
    .await
    .expect("bounded committed SSE delivery");
    assert!(!frame.contains("event: stream.error\n"), "{frame}");
    let ids: Vec<_> = frame
        .lines()
        .filter_map(|line| line.strip_prefix("id: "))
        .collect();
    let data: Vec<_> = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect();
    assert_eq!(ids.len(), 1, "{frame}");
    assert_eq!(data.len(), 1, "{frame}");
    let cursor = EventCursor::decode(ids[0]).unwrap();
    assert_eq!(cursor.collection(), collection);
    assert_eq!(cursor.position(), expected_position);
    cursor.validate_anchor(Some(&expected.event_id)).unwrap();
    let payload: serde_json::Value = serde_json::from_str(data[0]).unwrap();
    assert_eq!(payload, serde_json::to_value(expected).unwrap());
    ids[0].to_owned()
}

pub(super) async fn drain_bound(
    chunks: &mut axum::body::BodyDataStream,
    pending: &mut Vec<u8>,
) -> String {
    let mut bytes = std::mem::take(pending);
    timeout(Duration::from_secs(3), async {
        while let Some(chunk) = chunks.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
    })
    .await
    .expect("failed bound stream must reach EOF");
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn v2_cursor_query_and_header_policy_is_bounded_and_unambiguous() {
    let (router, token, temp) = build_test_router().await;
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let first = repository
        .append_runtime_event(&new_event(
            "wire_first",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let second = repository
        .append_runtime_event(&new_event(
            "wire_second",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let collection = EventCollection::stream(None).unwrap();
    let first_token = fixture_cursor(&repository, &collection, &first);
    let second_token = fixture_cursor(&repository, &collection, &second);
    let base = "/v2/events/stream";
    for (uri, headers) in [
        (base.to_owned(), vec![]),
        (base.to_owned(), vec![""]),
        (base.to_owned(), vec![first_token.as_str()]),
        (format!("{base}?after_seq={first_token}"), vec![]),
        (format!("{base}?after_seq={first_token}"), vec![""]),
        (
            format!("{base}?after_seq={first_token}"),
            vec![first_token.as_str()],
        ),
    ] {
        assert_eq!(
            request(&router, &token, &uri, &headers).await.status(),
            StatusCode::OK,
            "{uri} / {headers:?}"
        );
    }
    let cases = vec![
        (format!("{base}?after_seq="), vec![], "event_cursor_invalid"),
        (
            format!("{base}?after_seq=0"),
            vec![],
            "event_cursor_invalid",
        ),
        (
            format!("{base}?after_seq=1"),
            vec![],
            "event_cursor_invalid",
        ),
        (
            format!("{base}?after_seq=%20{first_token}"),
            vec![],
            "event_cursor_invalid",
        ),
        (
            base.to_owned(),
            vec![" ".to_owned()],
            "event_cursor_invalid",
        ),
        (
            base.to_owned(),
            vec![format!(" {first_token}")],
            "event_cursor_invalid",
        ),
        (
            base.to_owned(),
            vec![format!("{first_token} ")],
            "event_cursor_invalid",
        ),
        (
            base.to_owned(),
            vec!["x".repeat(1025)],
            "event_cursor_invalid",
        ),
        (
            format!("{base}?after_seq={}", "x".repeat(1025)),
            vec![],
            "event_cursor_invalid",
        ),
        (
            format!("{base}?after_seq={first_token}"),
            vec!["broken".to_owned()],
            "event_cursor_invalid",
        ),
        (
            format!("{base}?after_seq={first_token}"),
            vec![second_token.clone()],
            "event_cursor_conflict",
        ),
        (
            base.to_owned(),
            vec![first_token.clone(), first_token.clone()],
            "invalid_request",
        ),
        (
            base.to_owned(),
            vec![String::new(), String::new()],
            "invalid_request",
        ),
        (
            format!("{base}?after_seq={first_token}"),
            vec![String::new(), first_token.clone()],
            "invalid_request",
        ),
        (
            format!("{base}?after_seq={first_token}&after_seq={first_token}"),
            vec![],
            "invalid_request",
        ),
    ];
    for (uri, headers, code) in cases {
        let headers: Vec<_> = headers.iter().map(String::as_str).collect();
        error(
            &router,
            &token,
            &uri,
            &headers,
            StatusCode::BAD_REQUEST,
            code,
        )
        .await;
    }
    let history =
        EventCollection::history(Some((RuntimeEventScope::System, "global".into()))).unwrap();
    let wrong_axis = fixture_cursor(&repository, &history, &first);
    error(
        &router,
        &token,
        &format!("{base}?scope=system&scope_id=global&after_seq={wrong_axis}"),
        &[],
        StatusCode::BAD_REQUEST,
        "event_cursor_collection_mismatch",
    )
    .await;
    // Semantically valid limit=1 queries straddle the raw byte bound.
    let exact = format!("limit={}1", "0".repeat(4089));
    assert_eq!(exact.len(), 4096);
    assert_eq!(
        request(&router, &token, &format!("{base}?{exact}"), &[])
            .await
            .status(),
        StatusCode::OK
    );
    let oversized = format!("limit={}1", "0".repeat(4090));
    assert_eq!(oversized.len(), 4097);
    let body = error(
        &router,
        &token,
        &format!("{base}?{oversized}"),
        &[],
        StatusCode::BAD_REQUEST,
        "invalid_request",
    )
    .await;
    assert_eq!(body["error"]["message"], "event query exceeds 4096 bytes");
    for id in ["%20bad", "bad%20"] {
        error(
            &router,
            &token,
            &format!("{base}?scope=workspace&scope_id={id}"),
            &[],
            StatusCode::BAD_REQUEST,
            "invalid_request",
        )
        .await;
    }
    error(
        &router,
        &token,
        &format!("{base}?scope=workspace&scope_id={}", "x".repeat(257)),
        &[],
        StatusCode::BAD_REQUEST,
        "invalid_request",
    )
    .await;
    let exact_id = "ws AΩ";
    let exact_record = repository
        .append_runtime_event(&new_event(
            "wire_exact_scope",
            RuntimeEventScope::Workspace,
            exact_id,
        ))
        .unwrap();
    let exact_collection =
        EventCollection::stream(Some((RuntimeEventScope::Workspace, exact_id.into()))).unwrap();
    let response = request(
        &router,
        &token,
        "/v2/events/stream?scope=workspace&scope_id=ws%20A%CE%A9",
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut chunks = response.into_body().into_data_stream();
    read_bound_record(
        &mut chunks,
        &mut Vec::new(),
        &exact_collection,
        exact_record.seq,
        &exact_record,
    )
    .await;
}

#[tokio::test]
async fn client_cursor_rejection_keeps_peer_alive_but_actual_epoch_change_terminates_it() {
    let (router, token, temp) = build_test_router().await;
    let path = temp.path().join("runtime.sqlite3");
    let repository = SqliteRuntimeRepository::new(path.clone());
    let anchor = repository
        .append_runtime_event(&new_event(
            "peer_anchor",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let collection = EventCollection::stream(None).unwrap();
    let saved = fixture_cursor(&repository, &collection, &anchor);
    let response = request(
        &router,
        &token,
        &format!("/v2/events/stream?after_seq={saved}&limit=10"),
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut chunks = response.into_body().into_data_stream();
    let mut pending = Vec::new();
    let wrong_epoch = replace_field(&saved, 1, &different_hex(saved.split('.').nth(1).unwrap()));
    let wrong_anchor = replace_field(&saved, 6, &different_hex(saved.split('.').nth(6).unwrap()));
    let other =
        EventCollection::stream(Some((RuntimeEventScope::System, "global".into()))).unwrap();
    let wrong_collection = fixture_cursor(&repository, &other, &anchor);
    for (bad, status, code) in [
        (
            wrong_epoch,
            StatusCode::CONFLICT,
            "event_cursor_epoch_mismatch",
        ),
        (
            wrong_anchor,
            StatusCode::BAD_REQUEST,
            "event_cursor_anchor_mismatch",
        ),
        (
            wrong_collection,
            StatusCode::BAD_REQUEST,
            "event_cursor_collection_mismatch",
        ),
    ] {
        error(
            &router,
            &token,
            "/v2/events/stream",
            &[bad.as_str()],
            status,
            code,
        )
        .await;
    }
    let later = repository
        .append_runtime_event(&new_event(
            "peer_after_rejections",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let observed =
        read_bound_record(&mut chunks, &mut pending, &collection, later.row_id, &later).await;
    // Owned disposable mutation changes the epoch without advancing the head.
    let parsed = EventCursor::decode(&observed).unwrap();
    let replacement = different_hex(parsed.epoch().as_str());
    let db = rusqlite::Connection::open(&path).unwrap();
    let before: i64 = db
        .query_row("SELECT COALESCE(MAX(id),0) FROM runtime_events", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        db.execute(
            "UPDATE runtime_event_journal_identity SET epoch=?1 WHERE singleton=1",
            [&replacement]
        )
        .unwrap(),
        1
    );
    let after: i64 = db
        .query_row("SELECT COALESCE(MAX(id),0) FROM runtime_events", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(before, after);
    drop(db);
    let terminal = drain_bound(&mut chunks, &mut pending).await;
    assert_terminal(&terminal, "journal_generation_changed");
}
