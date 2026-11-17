use std::sync::atomic::Ordering;
use tokio::time::{sleep, Duration};

use crate::{ProviderKind, RuntimeStore, TurnDispatchState};

use super::super::test_support::{
    manager_with_observable_send_provider, observable_manager_for_store, MockSendBehavior,
};
use super::super::{CreateSessionInput, SendTurnInput};

#[tokio::test]
async fn startup_recovery_retains_dispatched_authority_when_native_session_identity_is_missing() {
    let (manager, store, initial_send_count) =
        manager_with_observable_send_provider(MockSendBehavior::Success, None);
    let session = manager
        .seed_stored_session_for_test(
            CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            },
            "mock:",
        )
        .await
        .expect("seed stored session");
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
                turn_settings: Default::default(),
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
    let before = store
        .get_persisted_retirement_review_session(&session.id)
        .unwrap()
        .unwrap();
    let before_events = store.events.lock().unwrap().clone();
    drop(manager);

    let (restarted, restart_send_count) =
        observable_manager_for_store(store.clone(), MockSendBehavior::Success, None);
    let summary = restarted.recover_startup().await.expect("startup recovery");
    sleep(Duration::from_millis(250)).await;

    assert_eq!(restart_send_count.load(Ordering::SeqCst), 0);
    assert_eq!(summary.resumed_sessions, 0);
    assert_eq!(summary.resumed_waits, 0);
    assert_eq!(summary.sessions_reconciled, 0);
    assert_eq!(summary.turns_reconciled, 0);
    assert_eq!(summary.approvals_reconciled, 0);
    assert!(summary
        .notes
        .iter()
        .any(|note| { note.contains("startup attachment lacks provider_session_ref") }));
    let after = store
        .get_persisted_retirement_review_session(&session.id)
        .unwrap()
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(after.session.status, "waiting_for_approval");
    assert_eq!(
        after.session.active_turn_id.as_deref(),
        Some(accepted.turn_id.as_str())
    );
    assert_eq!(after.turns[0].status, "waiting_for_approval");
    assert_eq!(
        after.admissions[0].dispatch_state,
        TurnDispatchState::Dispatched
    );
    assert_eq!(
        after.admissions[0].provider_native_turn_id.as_deref(),
        Some("native-before-provider-loss")
    );
    assert_eq!(
        restarted.get_session(&session.id).await.unwrap(),
        before.session
    );
    assert_eq!(
        restarted.turns.read().await[&accepted.turn_id],
        before.turns[0]
    );
    assert_eq!(
        restarted.turn_admissions.read().await[&accepted.turn_id],
        before.admissions[0]
    );
    assert_eq!(
        restarted.approvals.read().await[&before.approvals[0].id],
        before.approvals[0]
    );
    assert_eq!(*store.events.lock().unwrap(), before_events);
    let ledger = store.startup.lock().unwrap();
    assert!(ledger.operations.is_empty());
    assert!(ledger.claims.is_empty());
}
