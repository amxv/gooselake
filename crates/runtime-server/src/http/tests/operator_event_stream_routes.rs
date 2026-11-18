use super::*;
use runtime_core::{EventCollection, ManagedProcessAdmission};
use runtime_store_sqlite::SqliteRuntimeRepository;

use super::durable_event_stream_routes::new_event;
use super::operator_event_test_support::{assert_error, fixture_cursor, observe, stream};

#[tokio::test]
async fn v2_subject_streams_catch_commits_during_handoff_without_parent_scope_leaks() {
    let (router, token, temp, _, _, workspace_id) = super::operator_agent_create::fixture().await;
    let path = temp.path().join("runtime.sqlite3");
    let repository = SqliteRuntimeRepository::new(path.clone());
    let subject_ids = [
        ("turn", "turn_handoff_subject"),
        ("message", "msg_handoff_subject"),
        ("operation", "op_handoff_subject"),
    ];
    let mut seed = new_event(
        "subject_handoff_seed",
        RuntimeEventScope::Session,
        "sess_subject_handoff",
    );
    seed.turn_id = Some(subject_ids[0].1.into());
    seed.payload = serde_json::json!({
        "message_id": subject_ids[1].1,
        "operation_id": subject_ids[2].1,
    });
    let seed = repository.append_runtime_event(&seed).unwrap();

    // Retain actual wire tokens before exercising the subscribe/replay handoff.
    let mut collections = Vec::new();
    for (scope, id) in subject_ids {
        let collection = EventCollection::stream(Some((
            RuntimeEventScope::from_str(scope).unwrap(),
            id.into(),
        )))
        .unwrap();
        let base = format!("/v2/events/stream?scope={scope}&scope_id={id}&limit=1");
        let observed = observe(
            stream(&router, &token, &base, None, None).await,
            &collection,
            &[(seed.row_id, &seed)],
        )
        .await;
        collections.push((scope, id, collection, observed.tokens[0].clone()));
    }
    let mut handoffs = Vec::new();
    for (scope, id, _, cursor) in &collections {
        let router = router.clone();
        let token = token.clone();
        let uri =
            format!("/v2/events/stream?scope={scope}&scope_id={id}&after_seq={cursor}&limit=1");
        handoffs.push(tokio::spawn(async move {
            stream(&router, &token, &uri, Some(240), None).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Unrelated primary events may advance the global row ID, but they must
    // not appear in any of the three subject-filtered SSE collections.
    let unrelated = repository
        .append_runtime_event(&new_event(
            "subject_handoff_unrelated",
            RuntimeEventScope::System,
            "system_subject_handoff",
        ))
        .unwrap();
    let mut second = new_event(
        "subject_handoff_second",
        RuntimeEventScope::Workspace,
        &workspace_id,
    );
    second.turn_id = Some(subject_ids[0].1.into());
    second.payload = serde_json::json!({
        "message_id": subject_ids[1].1,
        "operation_id": subject_ids[2].1,
    });
    let second = repository.append_runtime_event(&second).unwrap();
    assert_eq!(second.row_id, unrelated.row_id + 1);
    assert_ne!(second.seq, second.row_id);
    for (handoff, (_, _, collection, _)) in handoffs.into_iter().zip(&collections) {
        observe(
            handoff.await.unwrap(),
            collection,
            &[(second.row_id, &second)],
        )
        .await;
    }

    // Reopen SQLite after the original subscribers are gone. Resuming from
    // the *observed subject cursor* still yields only the unread parent event,
    // not an unrelated global event or a duplicate of the boundary record.
    let reopened = SqliteRuntimeRepository::new(path);
    for (scope, id, collection, cursor) in collections {
        let after_seed = reopened
            .list_runtime_events(
                Some((
                    RuntimeEventScope::from_str(scope).expect("known subject scope"),
                    id,
                )),
                Some(seed.row_id),
                1,
            )
            .unwrap();
        assert_eq!(after_seed, [second.clone()]);
        let uri = format!("/v2/events/stream?scope={scope}&scope_id={id}&limit=1");
        observe(
            stream(&router, &token, &uri, None, Some(&cursor)).await,
            &collection,
            &[(second.row_id, &second)],
        )
        .await;
    }
}

#[tokio::test]
async fn v2_subject_streams_replay_canonical_rows_across_parent_scopes_and_restart_cursor() {
    let (router, token, temp, _, _, workspace_id) = super::operator_agent_create::fixture().await;
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let mut first = new_event(
        "v2_subject_first",
        RuntimeEventScope::Session,
        "sess_parent_one",
    );
    first.turn_id = Some("turn_subject_sse".into());
    first.payload =
        serde_json::json!({"message_id":"msg_subject_sse", "operation_id":"op_subject_sse"});
    let first = repository.append_runtime_event(&first).unwrap();
    let mut second = new_event(
        "v2_subject_second",
        RuntimeEventScope::Workspace,
        &workspace_id,
    );
    second.turn_id = Some("turn_subject_sse".into());
    second.payload =
        serde_json::json!({"message_id":"msg_subject_sse", "operation_id":"op_subject_sse"});
    let second = repository.append_runtime_event(&second).unwrap();
    assert_ne!(first.scope, second.scope);
    assert!(first.row_id < second.row_id);
    // The parent's sequence is NOT the derived subject cursor.
    assert_ne!(second.row_id, second.seq);

    for (scope, id) in [
        ("turn", "turn_subject_sse"),
        ("message", "msg_subject_sse"),
        ("operation", "op_subject_sse"),
    ] {
        let collection = EventCollection::stream(Some((
            RuntimeEventScope::from_str(scope).unwrap(),
            id.into(),
        )))
        .unwrap();
        let uri = format!("/v2/events/stream?scope={scope}&scope_id={id}&limit=1");
        let envelopes = observe(
            stream(&router, &token, &uri, None, None).await,
            &collection,
            &[(first.row_id, &first), (second.row_id, &second)],
        )
        .await;
        let payloads: Vec<serde_json::Value> = envelopes
            .text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|data| serde_json::from_str(data).unwrap())
            .collect();
        assert_eq!(
            payloads,
            [
                serde_json::to_value(&first).unwrap(),
                serde_json::to_value(&second).unwrap()
            ],
            "subject SSE preserves original primary scope, sequence and canonical envelope"
        );
        let uri = format!("/v2/events/stream?scope={scope}&scope_id={id}&limit=1");
        observe(
            stream(&router, &token, &uri, None, Some(&envelopes.tokens[0])).await,
            &collection,
            &[(second.row_id, &second)],
        )
        .await;
        // Redacted paginated list uses the same index and global row ID,
        // without claiming the canonical event's primary scope changed.
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v2/events?scope={scope}&scope_id={id}&limit=10"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body.as_array().unwrap().len(), 2);
        assert_eq!(body[0]["row_id"], first.row_id);
        assert_eq!(body[1]["row_id"], second.row_id);
        assert_eq!(body[0]["scope"], "session");
        assert_eq!(body[1]["scope"], "workspace");
    }

    for uri in [
        "/v2/events/stream?scope=turn",
        "/v2/events/stream?scope_id=turn_subject_sse",
        "/v2/events/stream?scope=turn&scope_id=%20bad",
        "/v2/events/stream?scope=bogus&scope_id=turn_subject_sse",
        &format!(
            "/v2/workspaces/{workspace_id}/events/stream?scope=turn&scope_id=turn_subject_sse"
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
}

#[tokio::test]
async fn v2_global_workspace_agent_and_process_streams_follow_the_same_committed_journal() {
    let (router, token, temp, _provider, _runtime, workspace_id) =
        super::operator_agent_create::fixture().await;
    let (status, agent, _) = super::operator_agent_create::create_http(
        &router,
        &token,
        &workspace_id,
        serde_json::json!({"provider":"codex","model":"test-model","title":"Stream member"}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let agent_id = agent["agent_id"].as_str().unwrap().to_owned();

    let process_id = "proc_v2_operator_sse";
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    repository
        .admit_managed_process(&ManagedProcessAdmission {
            process_id: process_id.to_string(),
            owner_session_id: Some(agent_id.clone()),
            workspace_id: Some(workspace_id.clone()),
            tool_call_id: None,
            command: serde_json::json!({"command":"true"}),
            cwd: None,
            timeout_ms: None,
            stdout_path: temp.path().join("v2-stdout").display().to_string(),
            stderr_path: temp.path().join("v2-stderr").display().to_string(),
            capture_limit_bytes: 1000,
            admitted_at: 100,
        })
        .unwrap();

    // Only append test events after the operator has created all resources.
    // Different scopes have unrelated monotone sequences, while global
    // replay uses the unique row ID across every scope.
    let w_seed = repository
        .append_runtime_event(&new_event(
            "v2_workspace_seed",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    let a_seed = repository
        .append_runtime_event(&new_event(
            "v2_agent_seed",
            RuntimeEventScope::Session,
            &agent_id,
        ))
        .unwrap();
    let p_seed = repository
        .append_runtime_event(&new_event(
            "v2_process_seed",
            RuntimeEventScope::Process,
            process_id,
        ))
        .unwrap();
    let global_seed = repository
        .append_runtime_event(&new_event(
            "v2_global_seed",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let workspace_uri = format!("/v2/workspaces/{workspace_id}/events/stream");
    let agent_uri = format!("/v2/agents/{agent_id}/events/stream");
    let process_uri = format!("/v2/processes/{process_id}/events/stream");

    let global_collection = EventCollection::stream(None).unwrap();
    let workspace_collection =
        EventCollection::stream(Some((RuntimeEventScope::Workspace, workspace_id.clone())))
            .unwrap();
    let agent_collection =
        EventCollection::stream(Some((RuntimeEventScope::Session, agent_id.clone()))).unwrap();
    let process_collection =
        EventCollection::stream(Some((RuntimeEventScope::Process, process_id.into()))).unwrap();
    let global_cursor = fixture_cursor(&repository, &global_collection, &global_seed);
    let workspace_cursor = fixture_cursor(&repository, &workspace_collection, &w_seed);
    let agent_cursor = fixture_cursor(&repository, &agent_collection, &a_seed);
    let process_cursor = fixture_cursor(&repository, &process_collection, &p_seed);

    let global = stream(
        &router,
        &token,
        &format!("/v2/events/stream?after_seq={global_cursor}&limit=1"),
        None,
        None,
    )
    .await;
    assert_eq!(global.headers()[header::CONTENT_TYPE], "text/event-stream");
    assert!(global.headers().get("x-request-id").is_some());
    let workspace = stream(
        &router,
        &token,
        &format!("{workspace_uri}?after_seq={workspace_cursor}&limit=1"),
        None,
        None,
    )
    .await;
    let agent = stream(
        &router,
        &token,
        &format!("{agent_uri}?after_seq={agent_cursor}&limit=1"),
        None,
        None,
    )
    .await;
    let process = stream(
        &router,
        &token,
        &format!("{process_uri}?after_seq={process_cursor}&limit=1"),
        None,
        None,
    )
    .await;

    let w1 = repository
        .append_runtime_event(&new_event(
            "v2_w1",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    let a1 = repository
        .append_runtime_event(&new_event("v2_a1", RuntimeEventScope::Session, &agent_id))
        .unwrap();
    let p1 = repository
        .append_runtime_event(&new_event("v2_p1", RuntimeEventScope::Process, process_id))
        .unwrap();
    let w2 = repository
        .append_runtime_event(&new_event(
            "v2_w2",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    let a2 = repository
        .append_runtime_event(&new_event("v2_a2", RuntimeEventScope::Session, &agent_id))
        .unwrap();
    let p2 = repository
        .append_runtime_event(&new_event("v2_p2", RuntimeEventScope::Process, process_id))
        .unwrap();

    let global = observe(
        global,
        &global_collection,
        &[
            (w1.row_id, &w1),
            (a1.row_id, &a1),
            (p1.row_id, &p1),
            (w2.row_id, &w2),
            (a2.row_id, &a2),
            (p2.row_id, &p2),
        ],
    )
    .await;
    let workspace = observe(
        workspace,
        &workspace_collection,
        &[(w1.seq, &w1), (w2.seq, &w2)],
    )
    .await;
    let agent = observe(agent, &agent_collection, &[(a1.seq, &a1), (a2.seq, &a2)]).await;
    let process = observe(
        process,
        &process_collection,
        &[(p1.seq, &p1), (p2.seq, &p2)],
    )
    .await;
    // Reconnect with unchanged IDs actually consumed from each collection.
    observe(
        stream(
            &router,
            &token,
            &workspace_uri,
            None,
            Some(&workspace.tokens[0]),
        )
        .await,
        &workspace_collection,
        &[(w2.seq, &w2)],
    )
    .await;
    observe(
        stream(&router, &token, &agent_uri, None, Some(&agent.tokens[0])).await,
        &agent_collection,
        &[(a2.seq, &a2)],
    )
    .await;
    observe(
        stream(
            &router,
            &token,
            &process_uri,
            None,
            Some(&process.tokens[0]),
        )
        .await,
        &process_collection,
        &[(p2.seq, &p2)],
    )
    .await;
    observe(
        stream(
            &router,
            &token,
            "/v2/events/stream",
            None,
            Some(&global.tokens[4]),
        )
        .await,
        &global_collection,
        &[(p2.row_id, &p2)],
    )
    .await;
}

#[tokio::test]
async fn v2_streams_reject_unauthorized_missing_or_invalid_cursors_before_opening() {
    let (router, token, temp, _, _, workspace_id) = super::operator_agent_create::fixture().await;
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let seed = repository
        .append_runtime_event(&new_event(
            "v2_preflight_seed",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    let workspace_uri = format!("/v2/workspaces/{workspace_id}/events/stream");
    let collection =
        EventCollection::stream(Some((RuntimeEventScope::Workspace, workspace_id.clone())))
            .unwrap();
    let seed_cursor = fixture_cursor(&repository, &collection, &seed);
    let request = |uri: &str, auth: bool, headers: &[(&str, &str)]| {
        let mut builder = Request::builder().uri(uri);
        if auth {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        for (key, value) in headers {
            builder = builder.header(*key, *value);
        }
        builder.body(Body::empty()).unwrap()
    };
    let cases = [
        (
            "/v2/events/stream".to_string(),
            false,
            vec![],
            StatusCode::UNAUTHORIZED,
        ),
        (
            "/v2/workspaces/not-real/events/stream".to_string(),
            true,
            vec![],
            StatusCode::NOT_FOUND,
        ),
        (
            "/v2/agents/not-real/events/stream".to_string(),
            true,
            vec![],
            StatusCode::NOT_FOUND,
        ),
        (
            "/v2/processes/not-real/events/stream".to_string(),
            true,
            vec![],
            StatusCode::NOT_FOUND,
        ),
        (
            format!("{workspace_uri}?after_seq=-1"),
            true,
            vec![],
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("{workspace_uri}?limit=0"),
            true,
            vec![],
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("{workspace_uri}?limit=10001"),
            true,
            vec![],
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("{workspace_uri}?unknown=1"),
            true,
            vec![],
            StatusCode::BAD_REQUEST,
        ),
        (
            workspace_uri.clone(),
            true,
            vec![("last-event-id", "broken")],
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("{workspace_uri}?after_seq={seed_cursor}"),
            true,
            vec![("last-event-id", "bad")],
            StatusCode::BAD_REQUEST,
        ),
        (
            workspace_uri.clone(),
            true,
            vec![("last-event-id", "1"), ("last-event-id", "2")],
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("{workspace_uri}?after_seq={seed_cursor}"),
            true,
            vec![("last-event-id", "1"), ("last-event-id", "2")],
            StatusCode::BAD_REQUEST,
        ),
    ];
    for (uri, auth, headers, expected) in cases {
        let response = router
            .clone()
            .oneshot(request(&uri, auth, &headers))
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{uri}");
        assert!(response.headers().get("x-request-id").is_some(), "{uri}");
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert!(body["error"]["code"].is_string(), "{uri}: {body}");
    }

    let during = repository
        .append_runtime_event(&new_event(
            "v2_preflight_after",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    let during_cursor = fixture_cursor(&repository, &collection, &during);
    let uri = format!("{workspace_uri}?after_seq={seed_cursor}");
    assert_error(
        &router,
        &token,
        &uri,
        &[&during_cursor],
        StatusCode::BAD_REQUEST,
        "event_cursor_conflict",
    )
    .await;
    observe(
        stream(&router, &token, &uri, None, Some(&seed_cursor)).await,
        &collection,
        &[(during.seq, &during)],
    )
    .await;
}

#[tokio::test]
async fn v2_workspace_stream_catches_a_commit_during_subscribe_replay_handoff() {
    let (router, token, temp, _, _, workspace_id) = super::operator_agent_create::fixture().await;
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let seed = repository
        .append_runtime_event(&new_event(
            "v2_handoff_seed",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    let collection =
        EventCollection::stream(Some((RuntimeEventScope::Workspace, workspace_id.clone())))
            .unwrap();
    let seed_cursor = fixture_cursor(&repository, &collection, &seed);
    let uri =
        format!("/v2/workspaces/{workspace_id}/events/stream?after_seq={seed_cursor}&limit=1");
    let pending = tokio::spawn({
        let router = router.clone();
        let token = token.clone();
        async move { stream(&router, &token, &uri, Some(230), None).await }
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    let written = repository
        .append_runtime_event(&new_event(
            "v2_during_handoff",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    observe(
        pending.await.unwrap(),
        &collection,
        &[(written.seq, &written)],
    )
    .await;
}
