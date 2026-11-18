use super::*;
use runtime_core::{EventCollection, EventCursor, EventEpoch, RuntimeEventRecord};
use runtime_store_sqlite::SqliteRuntimeRepository;

pub(super) struct Observed {
    pub(super) text: String,
    pub(super) tokens: Vec<String>,
}

pub(super) fn epoch(repository: &SqliteRuntimeRepository) -> EventEpoch {
    repository
        .list_operator_events_page_bound(&EventCollection::history(None).unwrap(), None, 1)
        .unwrap()
        .epoch
}

/// Request-input builder only. Output expectations use committed records.
/// The record may intentionally be outside the collection in a negative test.
pub(super) fn fixture_cursor(
    repository: &SqliteRuntimeRepository,
    collection: &EventCollection,
    record: &RuntimeEventRecord,
) -> String {
    EventCursor::for_event(
        &epoch(repository),
        collection,
        record.row_id,
        record.seq,
        &record.event_id,
    )
    .unwrap()
    .encode()
}

pub(super) fn replace_field(raw: &str, field: usize, value: &str) -> String {
    let mut parts: Vec<_> = raw.split('.').map(str::to_owned).collect();
    assert_eq!(parts.len(), 7);
    parts[field] = value.to_owned();
    parts.join(".")
}

pub(super) fn different_hex(value: &str) -> String {
    let mut bytes = value.as_bytes().to_vec();
    assert!(!bytes.is_empty());
    bytes[0] = if bytes[0] == b'0' { b'1' } else { b'0' };
    String::from_utf8(bytes).unwrap()
}

async fn request_inner(
    router: &Router,
    token: &str,
    uri: &str,
    last: &[&str],
    delay: Option<u64>,
) -> Response {
    let mut request = Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    for value in last {
        request = request.header("last-event-id", *value);
    }
    if let Some(delay) = delay {
        request = request.header("x-gg-test-handoff-delay-ms", delay.to_string());
    }
    router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

pub(super) async fn request(router: &Router, token: &str, uri: &str, last: &[&str]) -> Response {
    request_inner(router, token, uri, last, None).await
}

pub(super) async fn stream(
    router: &Router,
    token: &str,
    uri: &str,
    delay: Option<u64>,
    last: Option<&str>,
) -> Response {
    let values: Vec<_> = last.into_iter().collect();
    let response = request_inner(router, token, uri, &values, delay).await;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    response
}

pub(super) async fn assert_error(
    router: &Router,
    token: &str,
    uri: &str,
    last: &[&str],
    expected_status: StatusCode,
    expected_code: &str,
) -> serde_json::Value {
    let response = request(router, token, uri, last).await;
    assert_eq!(response.status(), expected_status, "{uri} / {last:?}");
    assert!(
        response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/json"),
        "cursor rejection must precede SSE success"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], expected_code, "{body}");
    assert_eq!(body["error"]["retryable"], false, "{body}");
    body
}

fn data_frames(text: &str) -> Vec<&str> {
    text.split_inclusive("\n\n")
        .filter(|frame| frame.ends_with("\n\n"))
        .filter(|frame| frame.lines().any(|line| line.starts_with("data: ")))
        .collect()
}

pub(super) fn raw_tokens(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("id: "))
        .map(str::to_owned)
        .collect()
}

pub(super) async fn read_events(
    chunks: &mut axum::body::BodyDataStream,
    collection: &EventCollection,
    expected: &[(i64, &RuntimeEventRecord)],
) -> Observed {
    assert!(!expected.is_empty());
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), async {
        loop {
            if data_frames(&String::from_utf8_lossy(&bytes)).len() >= expected.len() {
                break;
            }
            let chunk = chunks
                .next()
                .await
                .expect("bound SSE closed before expected committed records")
                .expect("bound SSE body failed");
            bytes.extend_from_slice(&chunk);
        }
    })
    .await
    .expect("bounded committed SSE delivery");
    let text = String::from_utf8(bytes).unwrap();
    let frames = data_frames(&text);
    assert_eq!(
        frames.len(),
        expected.len(),
        "missing, duplicate or unexpected event frames: {text}"
    );
    let mut tokens = Vec::new();
    for (frame, (position, expected_record)) in frames.into_iter().zip(expected) {
        assert!(!frame.contains("event: stream.error\n"), "{frame}");
        let ids: Vec<_> = frame
            .lines()
            .filter_map(|line| line.strip_prefix("id: "))
            .collect();
        let payloads: Vec<_> = frame
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .collect();
        assert_eq!(ids.len(), 1, "{frame}");
        assert_eq!(payloads.len(), 1, "{frame}");
        let cursor = EventCursor::decode(ids[0]).unwrap();
        assert_eq!(cursor.collection(), collection);
        assert_eq!(
            cursor.position(),
            *position,
            "coordinate oracle comes from the committed fixture record"
        );
        cursor
            .validate_anchor(Some(&expected_record.event_id))
            .unwrap();
        let record: serde_json::Value = serde_json::from_str(payloads[0]).unwrap();
        assert_eq!(
            record,
            serde_json::to_value(expected_record).unwrap(),
            "canonical event envelope changed"
        );
        tokens.push(ids[0].to_owned());
    }
    Observed { text, tokens }
}

pub(super) async fn observe(
    response: Response,
    collection: &EventCollection,
    expected: &[(i64, &RuntimeEventRecord)],
) -> Observed {
    read_events(
        &mut response.into_body().into_data_stream(),
        collection,
        expected,
    )
    .await
}

pub(super) async fn drain(chunks: &mut axum::body::BodyDataStream) -> String {
    timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        while let Some(chunk) = chunks.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        String::from_utf8(bytes).unwrap()
    })
    .await
    .expect("failed bound stream must terminate")
}

pub(super) fn assert_terminal(text: &str, code: &str) {
    let frames = data_frames(text);
    assert_eq!(frames.len(), 1, "{text}");
    assert!(frames[0].contains("event: stream.error\n"), "{text}");
    assert!(
        raw_tokens(text).is_empty(),
        "terminal error acquired an ID: {text}"
    );
    let payload = frames[0]
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap();
    let error: serde_json::Value = serde_json::from_str(payload).unwrap();
    assert_eq!(error["code"], code, "{error}");
    assert_eq!(error["retryable"], false, "{error}");
}
