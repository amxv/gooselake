use super::workspace_retirement_support::*;
use super::*;
use runtime_core::RuntimeEventCriticality;
use std::sync::atomic::Ordering;

/// Real authenticated Axum routes, actual SQLite and a disposable inert
/// provider. No credentials, native provider session, or real user DB.
#[tokio::test]
async fn native_approval_http_result_and_restarted_operator_events_share_exact_receipts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("native-approval-journal");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, started) = fixture
        .call(
            "POST",
            &format!("/v2/agents/{}/turns", agent.agent_id),
            Some(serde_json::json!({
                "input":[{"type":"text","text":"retirement_wait_for_interrupt"}]
            })),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let turn_id = started["turn_id"].as_str().unwrap();
    let pending = fixture
        .runtime
        .record_provider_approval(
            &agent.agent_id,
            turn_id,
            "native-approval-for-http",
            Some("tool-from-inert-provider".into()),
            serde_json::json!({"test_native":"no actual process launched"}),
        )
        .await
        .unwrap();
    let uri = format!("/v1/sessions/{}/approvals/{}", agent.agent_id, pending.id);
    let (status, resolved) = fixture
        .call(
            "POST",
            &uri,
            Some(serde_json::json!({"decision":"accept","payload":{"http":true}})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{resolved}");
    assert_eq!(resolved["status"], "accept");
    assert_eq!(provider.approval_responses.load(Ordering::SeqCst), 1);
    let reopened =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let hydrated = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(
        hydrated
            .approvals
            .iter()
            .find(|row| row.id == pending.id)
            .unwrap()
            .status,
        "accept"
    );
    assert_eq!(
        hydrated
            .turns
            .iter()
            .find(|row| row.id == turn_id)
            .unwrap()
            .status,
        "in_progress"
    );
    assert_eq!(
        hydrated
            .sessions
            .iter()
            .find(|row| row.id == agent.agent_id)
            .unwrap()
            .status,
        "turn_running"
    );
    let events = reopened
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &agent.agent_id)),
            None,
            100,
        )
        .unwrap();
    let reserved = events
        .iter()
        .filter(|row| row.kind == "approval.decision_reserved")
        .collect::<Vec<_>>();
    let finalized = events
        .iter()
        .filter(|row| row.kind == "approval.resolved")
        .collect::<Vec<_>>();
    assert_eq!(reserved.len(), 1);
    assert_eq!(finalized.len(), 1);
    assert_eq!(reserved[0].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(finalized[0].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(reserved[0].payload["approval_id"], pending.id);
    assert_eq!(finalized[0].payload["approval_id"], pending.id);
    assert_eq!(
        finalized[0].payload["decision_attempt_id"],
        reserved[0].payload["decision_attempt_id"]
    );
    let event_uri = format!("/v2/events/{}", finalized[0].event_id);
    let (status, detail) = fixture.call("GET", &event_uri, None, None).await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["payload"]["approval_id"], pending.id);
    assert_eq!(
        detail["payload"]["decision_attempt_id"],
        reserved[0].payload["decision_attempt_id"]
    );
    let (status, replay) = fixture
        .call(
            "POST",
            &uri,
            Some(serde_json::json!({"decision":"accept","payload":{"http":true}})),
            None,
        )
        .await;
    assert!(status.is_client_error(), "{replay}");
    assert_eq!(
        provider.approval_responses.load(Ordering::SeqCst),
        1,
        "HTTP retries must never dispatch another native decision"
    );
}
