use super::*;
use runtime_core::{NewRuntimeEvent, RuntimeEventCriticality};
use runtime_store_sqlite::SqliteRuntimeRepository;

pub(super) fn new_event(id: &str, scope: RuntimeEventScope, scope_id: &str) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: id.to_owned(),
        scope,
        scope_id: scope_id.to_owned(),
        session_id: (scope == RuntimeEventScope::Session).then(|| scope_id.to_owned()),
        team_id: (scope == RuntimeEventScope::Team).then(|| scope_id.to_owned()),
        turn_id: None,
        kind: format!("event.{id}"),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"id":id}),
        provider: None,
        provider_seq: None,
        created_at: 101,
    }
}

pub(super) async fn stream(
    router: &Router,
    token: &str,
    uri: &str,
    delay: Option<u64>,
    last: Option<i64>,
) -> Response {
    let mut request = Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(delay) = delay {
        request = request.header("x-gg-test-handoff-delay-ms", delay.to_string());
    }
    if let Some(last) = last {
        request = request.header("last-event-id", last.to_string());
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    response
}

pub(super) async fn observed_sse_ids(response: Response, expected: &[i64]) -> String {
    let mut chunks = response.into_body().into_data_stream();
    let mut output = String::new();
    for _ in 0..(expected.len() + 20) {
        if expected
            .iter()
            .all(|id| output.contains(format!("id: {id}\n").as_str()))
        {
            break;
        }
        let chunk = timeout(Duration::from_secs(3), chunks.next())
            .await
            .expect("durable SSE delivery timed out")
            .expect("durable SSE stream closed")
            .expect("durable SSE body failed");
        output.push_str(&String::from_utf8_lossy(&chunk));
    }
    let found = output
        .lines()
        .filter_map(|line| line.strip_prefix("id: "))
        .filter_map(|id| id.parse::<i64>().ok())
        .collect::<Vec<_>>();
    assert_eq!(
        found, expected,
        "SSE delivered missing, duplicate or out-of-order IDs: {output}"
    );
    output
}

#[tokio::test]
async fn global_session_and_team_sse_follow_durable_commits_without_duplicate_replay() {
    let (router, token, temp) = build_test_router().await;
    let session_id = create_test_session(router.clone(), &token, "durable-sse").await;
    let team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name":"Durable SSE",
                        "lead_agent_id":session_id,
                        "member_agent_ids":[]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(team_response.status(), StatusCode::OK);
    let team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(team_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let team_id = team_json["team"]["id"].as_str().unwrap().to_owned();
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let baseline_global = repository.list_runtime_events(None, None, 100).unwrap();
    let global_cursor = baseline_global.last().unwrap().row_id;
    let session_cursor = repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &session_id)), None, 100)
        .unwrap()
        .last()
        .unwrap()
        .seq;
    let team_cursor = repository
        .list_runtime_events(Some((RuntimeEventScope::Team, &team_id)), None, 100)
        .unwrap()
        .last()
        .unwrap()
        .seq;

    let global = stream(
        &router,
        &token,
        &format!("/v1/events/stream?after_seq={global_cursor}&limit=1"),
        None,
        None,
    )
    .await;
    let session = stream(
        &router,
        &token,
        &format!("/v1/sessions/{session_id}/events/stream?after_seq={session_cursor}&limit=1"),
        None,
        None,
    )
    .await;
    let team = stream(
        &router,
        &token,
        &format!("/v1/teams/{team_id}/events/stream?after_seq={team_cursor}&limit=1"),
        None,
        None,
    )
    .await;

    // These are committed by separate repository connections; the streams
    // receive *only* notification pulses and recover exact scoped rows via SQL.
    let s1 = repository
        .append_runtime_event(&new_event(
            "durable_s1",
            RuntimeEventScope::Session,
            &session_id,
        ))
        .unwrap();
    let t1 = repository
        .append_runtime_event(&new_event("durable_t1", RuntimeEventScope::Team, &team_id))
        .unwrap();
    let s2 = repository
        .append_runtime_event(&new_event(
            "durable_s2",
            RuntimeEventScope::Session,
            &session_id,
        ))
        .unwrap();
    let t2 = repository
        .append_runtime_event(&new_event("durable_t2", RuntimeEventScope::Team, &team_id))
        .unwrap();
    observed_sse_ids(global, &[s1.row_id, t1.row_id, s2.row_id, t2.row_id]).await;
    observed_sse_ids(session, &[s1.seq, s2.seq]).await;
    observed_sse_ids(team, &[t1.seq, t2.seq]).await;

    // Reconnect from an actually observed cursor: replay never duplicates the
    // boundary row or treats a scoped sequence as a global row ID.
    let reconnect = stream(
        &router,
        &token,
        &format!("/v1/teams/{team_id}/events/stream?limit=1"),
        None,
        Some(t1.seq),
    )
    .await;
    observed_sse_ids(reconnect, &[t2.seq]).await;
    let reconnect_global = stream(
        &router,
        &token,
        "/v1/events/stream?limit=1",
        None,
        Some(s2.row_id),
    )
    .await;
    observed_sse_ids(reconnect_global, &[t2.row_id]).await;
}

#[tokio::test]
async fn committed_event_during_replay_handoff_is_delivered_exactly_once() {
    let (router, token, temp) = build_test_router().await;
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let seed = repository
        .append_runtime_event(&new_event(
            "durable_seed",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();

    let delayed_router = router.clone();
    let delayed_token = token.clone();
    let handoff = tokio::spawn(async move {
        stream(
            &delayed_router,
            &delayed_token,
            &format!("/v1/events/stream?after_seq={}&limit=1", seed.row_id),
            Some(240),
            None,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    let during_handoff = repository
        .append_runtime_event(&new_event(
            "during_handoff",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let response = handoff.await.unwrap();
    observed_sse_ids(response, &[during_handoff.row_id]).await;
}

#[tokio::test]
async fn process_sse_preserves_owner_visibility_and_catches_commits_during_handoff() {
    let (router, token, temp) = build_test_router().await;
    let owner = create_test_session(router.clone(), &token, "process-sse-owner").await;
    let other = create_test_session(router.clone(), &token, "process-sse-other").await;
    let start = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/processes")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"command":"printf process-sse", "session_id":owner})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start.status(), StatusCode::OK);
    let result: serde_json::Value =
        serde_json::from_slice(&to_bytes(start.into_body(), usize::MAX).await.unwrap()).unwrap();
    let process_id = result["process"]["process_id"].as_str().unwrap().to_owned();

    // Wait for terminal authority so no scheduler events can race the exact
    // cursor in this deterministic post-commit/handoff test.
    let mut completed = false;
    for _ in 0..100 {
        let get = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/processes/{process_id}?session_id={owner}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::OK);
        let status: serde_json::Value =
            serde_json::from_slice(&to_bytes(get.into_body(), usize::MAX).await.unwrap()).unwrap();
        if matches!(
            status["process"]["status"].as_str(),
            Some("completed" | "failed" | "timed_out" | "killed")
        ) {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(completed, "local process did not settle");
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let cursor = repository
        .list_runtime_events(Some((RuntimeEventScope::Process, &process_id)), None, 1_000)
        .unwrap()
        .last()
        .unwrap()
        .seq;
    let rejected = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/processes/{process_id}/events/stream?session_id={other}&after_seq={cursor}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);

    let delayed_router = router.clone();
    let delayed_token = token.clone();
    let delayed_id = process_id.clone();
    let delayed_owner = owner.clone();
    let handoff = tokio::spawn(async move {
        stream(
            &delayed_router,
            &delayed_token,
            &format!(
                "/v1/processes/{delayed_id}/events/stream?session_id={delayed_owner}&after_seq={cursor}&limit=1"
            ),
            Some(240),
            None,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    let mut event = new_event(
        "process_sse_committed",
        RuntimeEventScope::Process,
        &process_id,
    );
    event.session_id = Some(owner);
    let inserted = repository.append_runtime_event(&event).unwrap();
    observed_sse_ids(handoff.await.unwrap(), &[inserted.seq]).await;

    let resume = stream(
        &router,
        &token,
        &format!("/v1/processes/{process_id}/events/stream?limit=1"),
        None,
        Some(cursor),
    )
    .await;
    observed_sse_ids(resume, &[inserted.seq]).await;
}
