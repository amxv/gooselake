use super::*;
use runtime_core::ManagedProcessAdmission;
use runtime_store_sqlite::SqliteRuntimeRepository;

use super::durable_event_stream_routes::{new_event, observed_sse_ids, stream};

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

    let global = stream(
        &router,
        &token,
        &format!("/v2/events/stream?after_seq={}&limit=1", global_seed.row_id),
        None,
        None,
    )
    .await;
    assert_eq!(global.headers()[header::CONTENT_TYPE], "text/event-stream");
    assert!(global.headers().get("x-request-id").is_some());
    let workspace = stream(
        &router,
        &token,
        &format!("{workspace_uri}?after_seq={}&limit=1", w_seed.seq),
        None,
        None,
    )
    .await;
    let agent = stream(
        &router,
        &token,
        &format!("{agent_uri}?after_seq={}&limit=1", a_seed.seq),
        None,
        None,
    )
    .await;
    let process = stream(
        &router,
        &token,
        &format!("{process_uri}?after_seq={}&limit=1", p_seed.seq),
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

    observed_sse_ids(
        global,
        &[
            w1.row_id, a1.row_id, p1.row_id, w2.row_id, a2.row_id, p2.row_id,
        ],
    )
    .await;
    observed_sse_ids(workspace, &[w1.seq, w2.seq]).await;
    observed_sse_ids(agent, &[a1.seq, a2.seq]).await;
    observed_sse_ids(process, &[p1.seq, p2.seq]).await;

    // Reconnects resume from the last *scoped* sequence, while the global
    // endpoint uses its row ID, never the operator history-page cursor.
    observed_sse_ids(
        stream(&router, &token, &workspace_uri, None, Some(w1.seq)).await,
        &[w2.seq],
    )
    .await;
    observed_sse_ids(
        stream(&router, &token, &agent_uri, None, Some(a1.seq)).await,
        &[a2.seq],
    )
    .await;
    observed_sse_ids(
        stream(&router, &token, &process_uri, None, Some(p1.seq)).await,
        &[p2.seq],
    )
    .await;
    observed_sse_ids(
        stream(&router, &token, "/v2/events/stream", None, Some(a2.row_id)).await,
        &[p2.row_id],
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
            format!("{workspace_uri}?after_seq={}", seed.seq),
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

    // A valid explicit cursor wins over a valid Last-Event-ID fallback.
    let during = repository
        .append_runtime_event(&new_event(
            "v2_preflight_after",
            RuntimeEventScope::Workspace,
            &workspace_id,
        ))
        .unwrap();
    observed_sse_ids(
        stream(
            &router,
            &token,
            &format!("{workspace_uri}?after_seq={}", seed.seq),
            None,
            Some(during.seq),
        )
        .await,
        &[during.seq],
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
    let uri = format!(
        "/v2/workspaces/{workspace_id}/events/stream?after_seq={}&limit=1",
        seed.seq
    );
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
    observed_sse_ids(pending.await.unwrap(), &[written.seq]).await;
}
