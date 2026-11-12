use super::*;
use runtime_core::ManagedProcessAdmission;
use runtime_store_sqlite::SqliteRuntimeRepository;

use super::durable_event_stream_routes::{new_event, observed_sse_ids, stream};

async fn assert_cursor_status(
    router: &Router,
    token: &str,
    uri: &str,
    last_event_id: Option<i64>,
    expected: StatusCode,
) {
    let mut request = Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(id) = last_event_id {
        request = request.header("last-event-id", id.to_string());
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), expected, "{uri} / {last_event_id:?}");
    if expected == StatusCode::BAD_REQUEST {
        let error: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(error["error"]["code"], "invalid_request", "{uri}: {error}");
    }
}

#[tokio::test]
async fn operator_sse_subject_and_global_resume_cursors_must_be_observed_journal_rows() {
    let (router, token, temp, _, _, workspace_id) = super::operator_agent_create::fixture().await;
    let path = temp.path().join("runtime.sqlite3");
    let repository = SqliteRuntimeRepository::new(path.clone());
    let mut first = new_event(
        "cursor_subject_first",
        RuntimeEventScope::Workspace,
        &workspace_id,
    );
    first.turn_id = Some("turn_cursor_subject".into());
    first.payload = serde_json::json!({
        "message_id": "msg_cursor_subject",
        "operation_id": "op_cursor_subject",
    });
    let first = repository.append_runtime_event(&first).unwrap();
    let foreign = repository
        .append_runtime_event(&new_event(
            "cursor_subject_foreign",
            RuntimeEventScope::System,
            "sys_cursor_foreign",
        ))
        .unwrap();
    let mut second = new_event(
        "cursor_subject_second",
        RuntimeEventScope::Session,
        "sess_cursor_subject",
    );
    second.turn_id = Some("turn_cursor_subject".into());
    second.payload = serde_json::json!({
        "message_id": "msg_cursor_subject",
        "operation_id": "op_cursor_subject",
    });
    let second = repository.append_runtime_event(&second).unwrap();
    assert_eq!(foreign.row_id + 1, second.row_id);

    // A global row is a valid global anchor, even when it has no subject
    // links; the same row is NOT a valid anchor for a filtered subject.
    for (scope, subject_id) in [
        ("turn", "turn_cursor_subject"),
        ("message", "msg_cursor_subject"),
        ("operation", "op_cursor_subject"),
    ] {
        let base = format!("/v2/events/stream?scope={scope}&scope_id={subject_id}&limit=1");
        let resume = format!("{base}&after_seq={}", first.row_id);
        observed_sse_ids(
            stream(&router, &token, &resume, None, None).await,
            &[second.row_id],
        )
        .await;
        observed_sse_ids(
            stream(&router, &token, &base, None, Some(first.row_id)).await,
            &[second.row_id],
        )
        .await;
        for cursor in [foreign.row_id, second.row_id + 10_000] {
            assert_cursor_status(
                &router,
                &token,
                &format!("{base}&after_seq={cursor}"),
                None,
                StatusCode::BAD_REQUEST,
            )
            .await;
            assert_cursor_status(
                &router,
                &token,
                &base,
                Some(cursor),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
        assert_cursor_status(
            &router,
            &token,
            &format!("{base}&after_seq=0"),
            None,
            StatusCode::OK,
        )
        .await;
        assert_cursor_status(
            &router,
            &token,
            &format!("{base}&after_seq={}", first.row_id),
            Some(second.row_id + 10_000),
            StatusCode::OK,
        )
        .await;
    }

    for (uri, cursor, expected) in [
        ("/v2/events/stream", foreign.row_id, StatusCode::OK),
        (
            "/v2/events/stream",
            second.row_id + 10_000,
            StatusCode::BAD_REQUEST,
        ),
        ("/v2/events/stream", 0, StatusCode::OK),
    ] {
        assert_cursor_status(
            &router,
            &token,
            &format!("{uri}?after_seq={cursor}"),
            None,
            expected,
        )
        .await;
    }
    // A restart does not create a secondary cursor authority or silently
    // reclassify a row belonging to a different subject.
    let reopened = SqliteRuntimeRepository::new(path);
    assert_eq!(
        reopened
            .list_runtime_events(
                Some((RuntimeEventScope::Message, "msg_cursor_subject")),
                Some(first.row_id),
                1,
            )
            .unwrap(),
        [second]
    );
}

#[tokio::test]
async fn operator_sse_resource_streams_validate_local_sequence_not_global_row_id() {
    let (router, token, temp, _, _, workspace_id) = super::operator_agent_create::fixture().await;
    let (status, agent, _) = super::operator_agent_create::create_http(
        &router,
        &token,
        &workspace_id,
        serde_json::json!({"provider":"codex", "model":"test-model", "title":"Cursor agent"}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let agent_id = agent["agent_id"].as_str().unwrap().to_owned();

    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let process_id = "proc_cursor_scope";
    repository
        .admit_managed_process(&ManagedProcessAdmission {
            process_id: process_id.into(),
            owner_session_id: Some(agent_id.clone()),
            workspace_id: Some(workspace_id.clone()),
            tool_call_id: None,
            command: serde_json::json!({"command":"true"}),
            cwd: None,
            timeout_ms: None,
            stdout_path: temp.path().join("cursor-stdout").display().to_string(),
            stderr_path: temp.path().join("cursor-stderr").display().to_string(),
            capture_limit_bytes: 1000,
            admitted_at: 100,
        })
        .unwrap();
    for (kind, scope_id) in [
        (RuntimeEventScope::Workspace, workspace_id.as_str()),
        (RuntimeEventScope::Session, agent_id.as_str()),
        (RuntimeEventScope::Process, process_id),
    ] {
        repository
            .append_runtime_event(&new_event(
                &format!("cursor_resource_{kind:?}"),
                kind,
                scope_id,
            ))
            .unwrap();
    }
    for (scope, id, resource) in [
        (
            "workspace",
            workspace_id.as_str(),
            format!("/v2/workspaces/{workspace_id}/events/stream"),
        ),
        (
            "session",
            agent_id.as_str(),
            format!("/v2/agents/{agent_id}/events/stream"),
        ),
        (
            "process",
            process_id,
            format!("/v2/processes/{process_id}/events/stream"),
        ),
    ] {
        let kind = RuntimeEventScope::from_str(scope).unwrap();
        let anchor = repository
            .list_runtime_events(Some((kind, id)), None, 100)
            .unwrap()
            .last()
            .unwrap()
            .clone();
        assert!(anchor.row_id > anchor.seq);
        let scoped = format!("/v2/events/stream?scope={scope}&scope_id={id}");
        for base in [scoped.as_str(), resource.as_str()] {
            let separator = if base.contains('?') { '&' } else { '?' };
            for valid in [0, anchor.seq] {
                assert_cursor_status(
                    &router,
                    &token,
                    &format!("{base}{separator}after_seq={valid}"),
                    None,
                    StatusCode::OK,
                )
                .await;
            }
            for invalid in [anchor.seq + 1_000, anchor.row_id + 10_000] {
                assert_cursor_status(
                    &router,
                    &token,
                    &format!("{base}{separator}after_seq={invalid}"),
                    None,
                    StatusCode::BAD_REQUEST,
                )
                .await;
                assert_cursor_status(
                    &router,
                    &token,
                    base,
                    Some(invalid),
                    StatusCode::BAD_REQUEST,
                )
                .await;
            }
            assert_cursor_status(
                &router,
                &token,
                &format!("{base}{separator}after_seq={}", anchor.seq),
                Some(anchor.seq + 1000),
                StatusCode::OK,
            )
            .await;
        }
    }
    // This deliberately stays a /v2-only rule while /v1 compatibility is
    // active: old global clients may resume at an arbitrary future offset.
    assert_cursor_status(
        &router,
        &token,
        "/v1/events/stream?after_seq=99999999",
        None,
        StatusCode::OK,
    )
    .await;
}
