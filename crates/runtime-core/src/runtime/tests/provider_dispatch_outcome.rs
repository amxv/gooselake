use std::sync::atomic::Ordering;

use super::super::test_support::{
    manager_with_observable_send_provider, observable_manager_for_store, MockSendBehavior,
};
use super::super::{CreateSessionInput, SendTurnInput};
use crate::{ProviderKind, TurnDispatchState};

fn request() -> SendTurnInput {
    SendTurnInput {
        input: vec![serde_json::json!({"type":"text","text":"once only"})],
        expected_turn_id: None,
        permission_mode: None,
        projection_source: None,
        user_input_snapshot: None,
        correlation_id: None,
        turn_settings: Default::default(),
    }
}

#[tokio::test]
async fn unknown_native_outcome_event_fault_keeps_original_fence_and_restart_does_not_send() {
    let (manager, store, sent) =
        manager_with_observable_send_provider(MockSendBehavior::Unknown, None);
    let session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .unwrap();
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("turn.dispatch_unknown".into());
    assert!(manager.send_turn(&session.id, request()).await.is_err());
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    let admission = store
        .turn_admissions
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatching);
    assert_eq!(
        store.turns.lock().unwrap()[&admission.turn_id].status,
        "admitted"
    );
    assert_eq!(
        store.sessions.lock().unwrap()[&session.id].status,
        "turn_admitted"
    );
    assert!(!store
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.kind == "turn.dispatch_unknown"));
    assert!(manager.send_turn(&session.id, request()).await.is_err());
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    drop(manager);

    let (restart, restart_sends) =
        observable_manager_for_store(store.clone(), MockSendBehavior::Success, None);
    restart.recover_startup().await.unwrap();
    assert_eq!(restart_sends.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.turn_admissions.lock().unwrap()[&admission.turn_id].dispatch_state,
        TurnDispatchState::Unknown
    );
    assert_eq!(
        store.turns.lock().unwrap()[&admission.turn_id].status,
        "dispatch_unknown"
    );
    assert_eq!(
        store.sessions.lock().unwrap()[&session.id].status,
        "turn_recovery_required"
    );
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == "turn.dispatch_unknown")
            .count(),
        1
    );
}

#[tokio::test]
async fn proven_no_send_failed_event_fault_does_not_clear_active_turn_or_resend() {
    let (manager, store, sent) =
        manager_with_observable_send_provider(MockSendBehavior::NotDispatched, None);
    let session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .unwrap();
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("turn.failed".into());
    assert!(manager.send_turn(&session.id, request()).await.is_err());
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    let admission = store
        .turn_admissions
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatching);
    assert_eq!(
        store.turns.lock().unwrap()[&admission.turn_id].status,
        "admitted"
    );
    assert_eq!(
        store.sessions.lock().unwrap()[&session.id]
            .active_turn_id
            .as_deref(),
        Some(admission.turn_id.as_str())
    );
    assert!(!store
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.kind == "turn.failed"));
    assert!(manager.send_turn(&session.id, request()).await.is_err());
    assert_eq!(sent.load(Ordering::SeqCst), 1);
}
