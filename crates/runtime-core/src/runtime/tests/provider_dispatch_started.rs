use std::sync::atomic::Ordering;

use super::super::test_support::{manager_with_observable_send_provider, MockSendBehavior};
use super::super::{CreateSessionInput, SendTurnInput};
use crate::{ProviderKind, TurnDispatchState};

#[tokio::test]
async fn positive_native_ack_critical_start_fault_keeps_dispatch_fenced_without_second_send() {
    let (manager, store, send_count) = manager_with_observable_send_provider(
        MockSendBehavior::Success,
        Some("native-already-acknowledged".into()),
    );
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
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("turn.started".into());
    let input = SendTurnInput {
        input: vec![serde_json::json!({"type":"text","text":"once only"})],
        expected_turn_id: None,
        permission_mode: None,
        projection_source: None,
        user_input_snapshot: None,
        correlation_id: None,
        turn_settings: Default::default(),
    };
    assert!(manager.send_turn(&session.id, input.clone()).await.is_err());
    assert_eq!(send_count.load(Ordering::SeqCst), 1);
    let admissions = store.turn_admissions.lock().unwrap();
    let admission = admissions.values().next().unwrap().clone();
    assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatching);
    assert!(admission.provider_native_turn_id.is_none());
    drop(admissions);
    assert_eq!(
        store.turns.lock().unwrap()[&admission.turn_id].status,
        "admitted"
    );
    assert_eq!(
        store.sessions.lock().unwrap()[&session.id].status,
        "turn_admitted"
    );
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == "turn.started")
            .count(),
        0
    );
    // A user-facing retry cannot infer a safe new native dispatch from a
    // missing post-ack Critical event; the admitted turn is still active.
    assert!(manager.send_turn(&session.id, input).await.is_err());
    assert_eq!(send_count.load(Ordering::SeqCst), 1);
}
