use super::*;
use runtime_core::{RuntimeEventCriticality, RuntimeEventScope};

async fn invoke(
    router: &Router,
    bearer: &str,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    key: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let contents = if let Some(body) = body {
        request = request.header(header::CONTENT_TYPE, "application/json");
        Body::from(body.to_string())
    } else {
        Body::empty()
    };
    let response = router
        .clone()
        .oneshot(request.body(contents).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::json!({"text":String::from_utf8_lossy(&bytes)}));
    (status, value)
}

#[tokio::test]
async fn journaled_lifecycle_and_http_inspection_share_three_critical_native_receipts() {
    let (initial_router, bearer, tmp, provider, _runtime, app) =
        build_test_router_with_services(TeamMcpPolicy::default()).await;
    let root = tmp.path().join("journaled-lifecycle");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = super::workspace_control_routes::register_control_workspace(
        &initial_router,
        &bearer,
        &root,
        "Journaled lifecycle",
    )
    .await;
    let (status, created, _) = super::operator_agent_create::create_http(
        &initial_router,
        &bearer,
        &workspace.workspace_id,
        json!({"provider":"codex", "model":"test-model", "title":"Lifecycle"}),
        Some("lifecycle-create"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let session_id = created["agent_id"].as_str().unwrap();
    assert_eq!(provider.create_call_count(), 1);

    let restarted = Arc::new(
        RuntimeSessionManager::new(
            app.services.store.clone(),
            app.provider_registry.clone(),
            512,
        )
        .unwrap(),
    );
    let summary = restarted.recover_startup().await.unwrap();
    assert_eq!(summary.resumed_sessions, 1, "{:?}", summary.notes);
    assert_eq!(provider.restore_call_count().await, 1);
    let router = build_router(AppState {
        app: app.clone(),
        runtime: restarted.clone(),
        bearer_token: bearer.clone(),
        public_base_url: "http://localhost:8080".into(),
        startup_recovery: Arc::new(summary),
    });
    let archive_uri = format!(
        "/v2/workspaces/{}/agents/{session_id}/archive",
        workspace.workspace_id,
    );
    let archive_body = json!({"reason":"offline_http_test"});
    let (status, archived) = invoke(
        &router,
        &bearer,
        "POST",
        &archive_uri,
        Some(archive_body.clone()),
        Some("lifecycle-archive"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(archived["lifecycle_state"], "archived");
    assert_eq!(archived["archive_reason"], "offline_http_test");
    assert_eq!(provider.archive_close_count(), 1);
    assert_eq!(
        restarted.get_session(session_id).await.unwrap().status,
        "closed"
    );

    let reopened =
        runtime_store_sqlite::SqliteRuntimeRepository::new(tmp.path().join("runtime.sqlite3"));
    let stored = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(
        stored
            .sessions
            .iter()
            .find(|row| row.id == session_id)
            .unwrap()
            .status,
        "closed"
    );
    let db = rusqlite::Connection::open(tmp.path().join("runtime.sqlite3")).unwrap();
    let events = reopened
        .list_runtime_events(
            Some((RuntimeEventScope::Workspace, &workspace.workspace_id)),
            None,
            100,
        )
        .unwrap();
    let mut receipts = Vec::new();
    for kind in [
        "operator_agent_create",
        "startup_session_reattach",
        "operator_agent_archive",
    ] {
        let operation_id: String = db
            .query_row(
                "SELECT operation_id FROM runtime_operations WHERE kind=?1",
                [kind],
                |row| row.get(0),
            )
            .unwrap();
        let operation = reopened.get_operation(&operation_id).unwrap().unwrap();
        assert_eq!(
            operation.operation.phase,
            runtime_core::OperationPhase::Completed
        );
        assert!(operation.operation.exact_terminal_result.is_some());
        assert!(operation.claims.is_empty());
        assert_eq!(operation.effects.len(), 1);
        assert_eq!(operation.effects[0].phase, "finalized");
        assert_eq!(operation.effects[0].attempt_count, 1);
        assert_eq!(operation.effects[0].target_id, session_id);
        let event_id = format!("{operation_id}:event:transition:2");
        let event = events
            .iter()
            .find(|event| event.event_id == event_id)
            .unwrap();
        assert_eq!(event.kind, "operation.transitioned");
        assert_eq!(event.payload["to_phase"], "completed");
        assert_eq!(event.criticality, RuntimeEventCriticality::Critical);
        let (status, operator_detail) = invoke(
            &router,
            &bearer,
            "GET",
            &format!("/v2/events/{}", event.event_id),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{operator_detail}");
        assert_eq!(operator_detail["event_id"], event.event_id);
        assert_eq!(operator_detail["payload"], event.payload);
        receipts.push(event.event_id.clone());
    }
    assert_eq!(receipts.len(), 3);
    assert!(receipts.windows(2).all(|pair| pair[0] != pair[1]));
    let before_replay = reopened.list_runtime_events(None, None, 100).unwrap();
    let (status, replay) = invoke(
        &router,
        &bearer,
        "POST",
        &archive_uri,
        Some(archive_body),
        Some("lifecycle-archive"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, archived);
    let (status, _) = invoke(
        &router,
        &bearer,
        "POST",
        &archive_uri,
        Some(json!({"reason":"different_late_reason"})),
        Some("lifecycle-archive"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.archive_close_count(), 1);
    assert_eq!(
        reopened.list_runtime_events(None, None, 100).unwrap(),
        before_replay
    );
    assert_eq!(
        restarted.get_session(session_id).await.unwrap().status,
        "closed"
    );
}
