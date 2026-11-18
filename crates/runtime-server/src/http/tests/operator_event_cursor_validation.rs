use super::durable_event_stream_routes::new_event;
use super::operator_event_test_support::{
    assert_error, fixture_cursor, observe, replace_field, request, stream,
};
use super::*;
use runtime_core::{EventCollection, ManagedProcessAdmission};
use runtime_store_sqlite::SqliteRuntimeRepository;

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

    for (scope, subject_id) in [
        ("turn", "turn_cursor_subject"),
        ("message", "msg_cursor_subject"),
        ("operation", "op_cursor_subject"),
    ] {
        let collection = EventCollection::stream(Some((
            RuntimeEventScope::from_str(scope).unwrap(),
            subject_id.into(),
        )))
        .unwrap();
        let base = format!("/v2/events/stream?scope={scope}&scope_id={subject_id}&limit=1");
        let observed = observe(
            stream(&router, &token, &base, None, None).await,
            &collection,
            &[(first.row_id, &first), (second.row_id, &second)],
        )
        .await;
        let first_token = &observed.tokens[0];
        let second_token = &observed.tokens[1];
        for (uri, last) in [
            (format!("{base}&after_seq={first_token}"), None),
            (base.clone(), Some(first_token.as_str())),
            (
                format!("{base}&after_seq={first_token}"),
                Some(first_token.as_str()),
            ),
        ] {
            observe(
                stream(&router, &token, &uri, None, last).await,
                &collection,
                &[(second.row_id, &second)],
            )
            .await;
        }
        // The global row exists, but carries none of this subject's membership.
        for invalid in [
            fixture_cursor(&repository, &collection, &foreign),
            replace_field(second_token, 5, &(second.row_id + 10_000).to_string()),
        ] {
            assert_error(
                &router,
                &token,
                &format!("{base}&after_seq={invalid}"),
                &[],
                StatusCode::BAD_REQUEST,
                "event_cursor_anchor_mismatch",
            )
            .await;
            assert_error(
                &router,
                &token,
                &base,
                &[&invalid],
                StatusCode::BAD_REQUEST,
                "event_cursor_anchor_mismatch",
            )
            .await;
        }
        assert_error(
            &router,
            &token,
            &format!("{base}&after_seq=0"),
            &[],
            StatusCode::BAD_REQUEST,
            "event_cursor_invalid",
        )
        .await;
        assert_error(
            &router,
            &token,
            &format!("{base}&after_seq={first_token}"),
            &[second_token],
            StatusCode::BAD_REQUEST,
            "event_cursor_conflict",
        )
        .await;
    }
    let global = EventCollection::stream(None).unwrap();
    let foreign_token = fixture_cursor(&repository, &global, &foreign);
    observe(
        stream(
            &router,
            &token,
            "/v2/events/stream",
            None,
            Some(&foreign_token),
        )
        .await,
        &global,
        &[(second.row_id, &second)],
    )
    .await;
    assert_error(
        &router,
        &token,
        &format!(
            "/v2/events/stream?after_seq={}",
            replace_field(&foreign_token, 5, &(second.row_id + 10_000).to_string())
        ),
        &[],
        StatusCode::BAD_REQUEST,
        "event_cursor_anchor_mismatch",
    )
    .await;
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
        let collection = EventCollection::stream(Some((kind, id.into()))).unwrap();
        let history = EventCollection::history(Some((kind, id.into()))).unwrap();
        let cursor = fixture_cursor(&repository, &collection, &anchor);
        let wrong_axis = fixture_cursor(&repository, &history, &anchor);
        let scoped = format!("/v2/events/stream?scope={scope}&scope_id={id}");
        for base in [scoped.as_str(), resource.as_str()] {
            let separator = if base.contains('?') { '&' } else { '?' };
            for (uri, last) in [
                (base.to_owned(), None),
                (base.to_owned(), Some("")),
                (format!("{base}{separator}after_seq={cursor}"), None),
                (base.to_owned(), Some(cursor.as_str())),
                (
                    format!("{base}{separator}after_seq={cursor}"),
                    Some(cursor.as_str()),
                ),
            ] {
                drop(stream(&router, &token, &uri, None, last).await);
            }
            assert_error(
                &router,
                &token,
                &format!("{base}{separator}after_seq={wrong_axis}"),
                &[],
                StatusCode::BAD_REQUEST,
                "event_cursor_collection_mismatch",
            )
            .await;
            for position in [anchor.row_id, anchor.seq + 1_000, anchor.row_id + 10_000] {
                let invalid = replace_field(&cursor, 5, &position.to_string());
                assert_error(
                    &router,
                    &token,
                    &format!("{base}{separator}after_seq={invalid}"),
                    &[],
                    StatusCode::BAD_REQUEST,
                    "event_cursor_anchor_mismatch",
                )
                .await;
                assert_error(
                    &router,
                    &token,
                    base,
                    &[&invalid],
                    StatusCode::BAD_REQUEST,
                    "event_cursor_anchor_mismatch",
                )
                .await;
            }
            assert_error(
                &router,
                &token,
                &format!("{base}{separator}after_seq=0"),
                &[],
                StatusCode::BAD_REQUEST,
                "event_cursor_invalid",
            )
            .await;
        }
    }
    // This deliberately stays a /v2-only rule while /v1 compatibility is
    // active: old global clients may resume at an arbitrary future offset.
    let response = request(&router, &token, "/v1/events/stream?after_seq=99999999", &[]).await;
    assert_eq!(response.status(), StatusCode::OK);
}
