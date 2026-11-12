use super::workspace_retirement_support::*;
use super::*;
use runtime_core::RuntimeEventCriticality;
use std::sync::atomic::Ordering;

/// Real authenticated Axum + fresh SQLite with a controllable inert provider.
/// No provider credentials, subprocess, network device or user legacy data.
#[tokio::test]
async fn native_wait_failure_has_single_critical_receipt_and_restarted_http_event_history() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("provider-wait-journal-http");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    provider.fail_next_wait.store(true, Ordering::SeqCst);

    let input = "retirement_wait_for_interrupt";
    let (status, accepted) = fixture
        .call(
            "POST",
            &format!("/v2/agents/{}/turns", agent.agent_id),
            Some(serde_json::json!({
                "input":[{"type":"text","text":input}]
            })),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{accepted}");
    let turn_id = accepted["turn_id"].as_str().unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .runtime
                .get_session(&agent.agent_id)
                .await
                .unwrap()
                .status
                == "failed"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("inert provider wait failure was not processed");

    let reopened =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let snapshot = reopened.hydrate_runtime_state().unwrap();
    let turn = snapshot.turns.iter().find(|row| row.id == turn_id).unwrap();
    let session = snapshot
        .sessions
        .iter()
        .find(|row| row.id == agent.agent_id)
        .unwrap();
    assert_eq!(turn.status, "failed");
    assert!(turn.completed_at.is_some());
    assert_eq!(session.status, "failed");
    assert!(session.active_turn_id.is_none());
    assert_eq!(
        session.failure_code.as_deref(),
        Some("provider_wait_failure")
    );
    let events = reopened
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &agent.agent_id)),
            None,
            100,
        )
        .unwrap();
    let failures = events
        .iter()
        .filter(|row| row.kind == "provider.error")
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 1);
    let journal = failures[0];
    assert_eq!(journal.event_id, format!("evt_turn_{turn_id}_wait_failure"));
    assert_eq!(journal.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(
        journal.payload["error"],
        "io error: controlled native provider wait failure"
    );
    let (status, exposed) = fixture
        .call(
            "GET",
            &format!("/v2/events/{}", journal.event_id),
            None,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{exposed}");
    assert_eq!(exposed["event_id"], journal.event_id);
    assert_eq!(exposed["payload"], journal.payload);
    let sent = provider.turns_containing(input).await;
    assert_eq!(sent, 1);
    let (status, repeated) = fixture
        .call(
            "POST",
            &format!("/v2/agents/{}/turns", agent.agent_id),
            Some(serde_json::json!({"input":[{"type":"text","text":input}]})),
            None,
        )
        .await;
    assert!(status.is_client_error(), "{repeated}");
    assert_eq!(
        provider.turns_containing(input).await,
        sent,
        "a provider wait outcome is never permission to blindly resend a turn"
    );
}
