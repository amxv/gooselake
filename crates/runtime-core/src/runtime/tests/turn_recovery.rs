use std::sync::atomic::Ordering;
use tokio::time::{sleep, Duration};

use crate::{ProviderKind, TurnDispatchState};

use super::super::test_support::{
    manager_with_observable_send_provider, observable_manager_for_store, MockSendBehavior,
};
use super::super::{CreateSessionInput, SendTurnInput};

#[tokio::test]
async fn startup_recovery_keeps_dispatched_turn_unresolved_when_provider_session_is_unavailable() {
    let (manager, store, initial_send_count) =
        manager_with_observable_send_provider(MockSendBehavior::Success, None);
    let session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create session");
    let accepted = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"provider truth required"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("admit turn");
    assert_eq!(initial_send_count.load(Ordering::SeqCst), 0);

    manager
        .update_turn_dispatch_authority(
            accepted.turn_id.as_str(),
            TurnDispatchState::Dispatching,
            None,
            None,
        )
        .await
        .expect("persist dispatch boundary");
    manager
        .update_turn_dispatch_authority(
            accepted.turn_id.as_str(),
            TurnDispatchState::Dispatched,
            Some("native-before-provider-loss".to_string()),
            None,
        )
        .await
        .expect("persist provider acknowledgement");
    {
        let mut sessions = store.sessions.lock().expect("sessions");
        let persisted = sessions.get_mut(&session.id).expect("persisted session");
        persisted.provider_session_ref = None;
    }
    drop(manager);

    let (restarted, restart_send_count) =
        observable_manager_for_store(store.clone(), MockSendBehavior::Success, None);
    restarted.recover_startup().await.expect("startup recovery");
    sleep(Duration::from_millis(250)).await;

    assert_eq!(restart_send_count.load(Ordering::SeqCst), 0);
    let recovered_session = restarted
        .get_session(session.id.as_str())
        .await
        .expect("recovered session");
    assert_eq!(recovered_session.status, "turn_recovery_required");
    assert_eq!(
        recovered_session.active_turn_id.as_deref(),
        Some(accepted.turn_id.as_str())
    );
    let recovered_turn = store
        .turns
        .lock()
        .expect("turns")
        .get(&accepted.turn_id)
        .cloned()
        .expect("recovered turn");
    assert_eq!(recovered_turn.status, "provider_session_recovery_required");
}
