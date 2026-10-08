use std::sync::Arc;
use std::time::Duration;

use runtime_core::{
    CreateSessionInput, ProviderApprovalResponseRequest, ProviderCreateSessionRequest,
    ProviderKind, ProviderRegistry, ProviderRuntimeEvent, ProviderSendTurnRequest,
    ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeProvider, RuntimeSessionManager,
    RuntimeStore, SendTurnInput,
};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
use serde_json::json;

use super::support::FakeAgentHarness;

#[tokio::test]
async fn crashed_acp_prompt_quarantines_admitted_turn_without_replaying() {
    let harness = FakeAgentHarness::new("normal");
    let provider = Arc::new(harness.provider());
    let mut registry = ProviderRegistry::new();
    registry.register(provider).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("unknown-prompt.sqlite3"),
    }));
    store.initialize().await.unwrap();
    let manager =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256).unwrap());
    let session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Acp,
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .unwrap();
    let admitted = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![json!({"type":"text","text":"crash now"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("the logical turn is admitted before the ACP child exits");

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let recorded = manager.get_session(session.id.as_str()).await.unwrap();
            let turns = manager
                .list_session_turns(session.id.as_str())
                .await
                .unwrap();
            if recorded.status == "turn_recovery_required"
                && turns.iter().any(|turn| {
                    turn.id == admitted.turn_id && turn.status == "provider_event_recovery_required"
                })
            {
                assert_eq!(
                    recorded.active_turn_id.as_deref(),
                    Some(admitted.turn_id.as_str())
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("unknown ACP prompt outcome must reach durable recovery state");

    let hydrated = store.hydrate_runtime_state().unwrap();
    assert!(hydrated.turns.iter().any(|turn| {
        turn.id == admitted.turn_id && turn.status == "provider_event_recovery_required"
    }));
    let repeated = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![json!({"type":"text","text":"must not repeat crashed work"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await;
    assert!(
        repeated.is_err(),
        "recovery-required ACP agent cannot be blindly dispatched"
    );
}

#[tokio::test]
async fn approval_from_crashed_child_cannot_be_sent_to_replacement_agent() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let mut events = provider.subscribe_events().unwrap();
    let create = |id: &str| ProviderCreateSessionRequest {
        runtime_session_id: id.to_string(),
        model: None,
        cwd: None,
        permission_mode: None,
        setting_sources: Vec::new(),
        system_prompt: None,
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        harness_version_slot: None,
        metadata: None,
    };
    provider
        .create_session(create("original-agent"))
        .await
        .unwrap();
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "original-agent".into(),
            turn_id: "permission-turn".into(),
            input: vec![json!({"type":"text","text":"permission please"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    let approval_ref = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(ProviderRuntimeEvent::ApprovalRequested {
                provider_approval_ref,
                ..
            }) = events.recv().await
            {
                break provider_approval_ref;
            }
        }
    })
    .await
    .unwrap();

    provider
        .current_connection()
        .await
        .unwrap()
        .shutdown(true)
        .await;
    provider
        .create_session(create("replacement-agent"))
        .await
        .unwrap();
    let stale_decision = provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "original-agent".into(),
            turn_id: "permission-turn".into(),
            approval_id: approval_ref,
            decision: "accept".into(),
            payload: None,
        })
        .await;
    assert!(
        stale_decision.is_err(),
        "a dead ACP child's permission is not transferable"
    );

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "replacement-agent".into(),
            turn_id: "replacement-turn".into(),
            input: vec![json!({"type":"text","text":"split please"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    assert_eq!(
        provider
            .wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: "replacement-agent".into(),
                turn_id: "replacement-turn".into(),
                timeout_ms: Some(5_000),
            })
            .await
            .unwrap()
            .status,
        ProviderTurnStatus::Completed
    );
}
