use super::*;
use runtime_core::{RuntimeEventCriticality, RuntimeEventScope, TurnDispatchState};
use std::sync::atomic::Ordering;

/// A genuine authenticated router and independent SQLite connection, with an
/// inert provider that must never see a denied turn. No live credentials.
#[tokio::test]
async fn http_local_denial_rolls_back_late_event_fault_then_retries_without_native_send() {
    let (router, token, temp, provider, _) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    let call = |method: &str, path: &str, body: Option<serde_json::Value>| {
        let router = router.clone();
        let token = token.clone();
        let path = path.to_string();
        let method = method.to_string();
        async move {
            super::workspace_worktree_routes::call(&router, &token, &method, &path, body, None)
                .await
        }
    };
    let (status, created) = call(
        "POST",
        "/v1/sessions",
        Some(serde_json::json!({"provider":"codex","model":"test-model","metadata":{}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let session_id = created["id"].as_str().unwrap();
    let (status, admitted) = call(
        "POST",
        &format!("/v1/sessions/{session_id}/turns"),
        Some(serde_json::json!({
            "input":[{"type":"text","text":"deny before provider"}],
            "permission_mode":"require_approval"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{admitted}");
    assert_eq!(admitted["status"], "waiting_for_approval");
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 0);
    let repo =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let before = repo.hydrate_runtime_state().unwrap();
    let pending = before
        .approvals
        .iter()
        .find(|row| row.session_id == session_id && row.status == "pending")
        .unwrap();
    let approval_id = pending.id.clone();
    let turn_id = pending.turn_id.clone();
    let uri = format!("/v1/sessions/{session_id}/approvals/{approval_id}");
    let denial = serde_json::json!({"decision":"decline","payload":{"reason":"operator rejected"}});
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_decline_receipt BEFORE INSERT ON runtime_events
        WHEN NEW.kind='turn.interrupted'
        BEGIN SELECT RAISE(ABORT,'injected late denial Critical journal failure'); END;",
    )
    .unwrap();
    let (status, _) = call("POST", &uri, Some(denial.clone())).await;
    assert!(status.is_server_error() || status.is_client_error());
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 0);
    let pending_rows = repo.hydrate_runtime_state().unwrap();
    assert!(pending_rows.approvals.iter().any(|row| row == pending));
    assert!(pending_rows.sessions.iter().any(|row| row.id == session_id
        && row.status == "waiting_for_approval"
        && row.active_turn_id.as_deref() == Some(turn_id.as_str())));
    assert!(pending_rows
        .turns
        .iter()
        .any(|row| row.id == turn_id && row.status == "waiting_for_approval"));
    assert_eq!(
        repo.list_turn_admissions().unwrap()[0].dispatch_state,
        TurnDispatchState::Pending
    );
    assert!(repo
        .list_runtime_events(Some((RuntimeEventScope::Session, session_id)), None, 100)
        .unwrap()
        .iter()
        .all(|row| row.kind != "approval.resolved" && row.kind != "turn.interrupted"));
    db.execute_batch("DROP TRIGGER fail_decline_receipt")
        .unwrap();
    drop(db);
    let (status, resolved) = call("POST", &uri, Some(denial.clone())).await;
    assert_eq!(status, StatusCode::OK, "{resolved}");
    assert_eq!(resolved["status"], "decline");
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 0);
    let reopened =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let hydrated = reopened.hydrate_runtime_state().unwrap();
    assert!(hydrated
        .approvals
        .iter()
        .any(|row| row.id == approval_id && row.status == "decline"));
    assert!(hydrated
        .turns
        .iter()
        .any(|row| row.id == turn_id && row.status == "interrupted"));
    assert!(hydrated
        .sessions
        .iter()
        .any(|row| row.id == session_id && row.status == "ready" && row.active_turn_id.is_none()));
    assert_eq!(
        reopened.list_turn_admissions().unwrap()[0].dispatch_state,
        TurnDispatchState::NotDispatched
    );
    let events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, session_id)), None, 100)
        .unwrap();
    let outcomes: Vec<_> = events
        .iter()
        .filter(|row| row.kind == "approval.resolved" || row.kind == "turn.interrupted")
        .collect();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].kind, "approval.resolved");
    assert_eq!(outcomes[1].seq, outcomes[0].seq + 1);
    assert!(outcomes
        .iter()
        .all(|row| row.criticality == RuntimeEventCriticality::Critical));
    for event in outcomes {
        let (status, detail) = call("GET", &format!("/v2/events/{}", event.event_id), None).await;
        assert_eq!(status, StatusCode::OK, "{detail}");
        assert_eq!(detail["payload"], event.payload);
        assert_eq!(detail["kind"], event.kind);
    }
    let (status, _) = call("POST", &uri, Some(denial)).await;
    assert!(status.is_client_error());
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 0);
}
