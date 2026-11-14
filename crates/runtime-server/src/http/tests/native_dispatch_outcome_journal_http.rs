use super::*;
use runtime_core::{RuntimeEventCriticality, RuntimeEventScope, TurnDispatchState};
use std::sync::atomic::Ordering;

/// Authentication, inert provider callback, fresh SQLite and operator event
/// detail are all real. No external provider, credential, device or user DB.
async fn new_session(router: &Router, token: &str) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "provider":"codex", "model":"test-model", "metadata":{}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    body["id"].as_str().unwrap().to_string()
}

async fn send(router: &Router, token: &str, session_id: &str) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{session_id}/turns"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "input":[{"type":"text","text":"once, if safe"}]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

fn reopened(temp: &tempfile::TempDir) -> runtime_store_sqlite::SqliteRuntimeRepository {
    runtime_store_sqlite::SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"))
}

#[tokio::test]
async fn authenticated_http_native_send_outcomes_have_one_reopened_critical_event() {
    for (mode, state, turn_status, session_status, event_kind, event_suffix) in [
        (
            1,
            TurnDispatchState::NotDispatched,
            "failed",
            "ready",
            "turn.failed",
            "not_dispatched",
        ),
        (
            2,
            TurnDispatchState::Unknown,
            "dispatch_unknown",
            "turn_recovery_required",
            "turn.dispatch_unknown",
            "dispatch_unknown",
        ),
    ] {
        let (router, token, temp, provider, _) =
            build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
        let session_id = new_session(&router, &token).await;
        provider.send_failure_mode.store(mode, Ordering::SeqCst);
        assert!(!send(&router, &token, &session_id).await.is_success());
        assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 1);

        // Independent connection behaves like a process restart's authority
        // reader: event and every projection must agree, not merely the HTTP
        // error response and mutable manager cache.
        let repo = reopened(&temp);
        let admissions = repo.list_turn_admissions().unwrap();
        assert_eq!(admissions.len(), 1);
        let admission = &admissions[0];
        assert_eq!(admission.dispatch_state, state);
        let snapshot = repo.hydrate_runtime_state().unwrap();
        let session = snapshot
            .sessions
            .iter()
            .find(|row| row.id == session_id)
            .unwrap();
        let turn = snapshot
            .turns
            .iter()
            .find(|row| row.id == admission.turn_id)
            .unwrap();
        assert_eq!(session.status, session_status);
        assert_eq!(turn.status, turn_status);
        assert_eq!(session.active_turn_id.is_some(), mode == 2);
        let events = repo
            .list_runtime_events(Some((RuntimeEventScope::Session, &session_id)), None, 100)
            .unwrap();
        let outcome_events = events
            .iter()
            .filter(|row| row.kind == event_kind)
            .collect::<Vec<_>>();
        assert_eq!(outcome_events.len(), 1);
        let event = outcome_events[0];
        assert_eq!(
            event.event_id,
            format!("evt_turn_{}_{}", admission.turn_id, event_suffix)
        );
        assert_eq!(event.criticality, RuntimeEventCriticality::Critical);
        assert_eq!(
            event.payload["provider_dispatch_outcome"],
            if mode == 1 {
                "not_dispatched"
            } else {
                "unknown"
            }
        );
        let (status, detail) = super::workspace_worktree_routes::call(
            &router,
            &token,
            "GET",
            &format!("/v2/events/{}", event.event_id),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{detail}");
        assert_eq!(detail["event_id"], event.event_id);
        assert_eq!(detail["payload"], event.payload);
        if mode == 2 {
            assert!(!send(&router, &token, &session_id).await.is_success());
            assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn authenticated_http_late_sql_event_fault_rolls_back_unknown_and_no_send_outcomes() {
    for (mode, rejected_kind) in [(1, "turn.failed"), (2, "turn.dispatch_unknown")] {
        let (router, token, temp, provider, runtime) =
            build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
        let session_id = new_session(&router, &token).await;
        let connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TRIGGER fail_outcome_event BEFORE INSERT ON runtime_events \
             WHEN NEW.kind='{rejected_kind}' \
             BEGIN SELECT RAISE(ABORT,'injected Critical event insert failure'); END;"
            ))
            .unwrap();
        drop(connection);
        provider.send_failure_mode.store(mode, Ordering::SeqCst);
        assert!(!send(&router, &token, &session_id).await.is_success());
        assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 1);
        let repo = reopened(&temp);
        let admissions = repo.list_turn_admissions().unwrap();
        assert_eq!(admissions.len(), 1);
        let admission = &admissions[0];
        assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatching);
        assert!(admission.dispatch_error.is_none());
        let hydrated = repo.hydrate_runtime_state().unwrap();
        assert_eq!(
            hydrated
                .turns
                .iter()
                .find(|row| row.id == admission.turn_id)
                .unwrap()
                .status,
            "admitted"
        );
        let session = hydrated
            .sessions
            .iter()
            .find(|row| row.id == session_id)
            .unwrap();
        assert_eq!(session.status, "turn_admitted");
        assert_eq!(
            session.active_turn_id.as_deref(),
            Some(admission.turn_id.as_str())
        );
        let events = repo
            .list_runtime_events(Some((RuntimeEventScope::Session, &session_id)), None, 100)
            .unwrap();
        assert!(events.iter().all(|row| row.kind != rejected_kind));
        assert!(!send(&router, &token, &session_id).await.is_success());
        assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 1,
            "HTTP retry must not send a second native turn even on proven no-send with lost Critical receipt");

        // The recoverer cannot recreate the *previous* knowledge that the
        // native send had no effect; it must conservatively classify the
        // durable DISPATCHING fence as UNKNOWN, once, without another send.
        let connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_outcome_event")
            .unwrap();
        drop(connection);
        runtime.recover_startup().await.unwrap();
        assert_eq!(provider.send_invocations.load(Ordering::SeqCst), 1);
        let reopened_after = reopened(&temp);
        let admission_after = reopened_after.list_turn_admissions().unwrap().remove(0);
        assert_eq!(admission_after.dispatch_state, TurnDispatchState::Unknown);
        let hydrated_after = reopened_after.hydrate_runtime_state().unwrap();
        assert_eq!(
            hydrated_after
                .turns
                .iter()
                .find(|row| row.id == admission.turn_id)
                .unwrap()
                .status,
            "dispatch_unknown"
        );
        let session_after = hydrated_after
            .sessions
            .iter()
            .find(|row| row.id == session_id)
            .unwrap();
        assert_eq!(session_after.status, "turn_recovery_required");
        assert_eq!(
            session_after.active_turn_id.as_deref(),
            Some(admission.turn_id.as_str())
        );
        let events_after = reopened_after
            .list_runtime_events(Some((RuntimeEventScope::Session, &session_id)), None, 100)
            .unwrap();
        let unknown = events_after
            .iter()
            .filter(|row| row.kind == "turn.dispatch_unknown")
            .collect::<Vec<_>>();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].criticality, RuntimeEventCriticality::Critical);
        let (status, detail) = super::workspace_worktree_routes::call(
            &router,
            &token,
            "GET",
            &format!("/v2/events/{}", unknown[0].event_id),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{detail}");
        assert_eq!(detail["payload"], unknown[0].payload);
    }
}
