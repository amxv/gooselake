use std::sync::atomic::Ordering;

use super::super::test_support::{manager_with_observable_send_provider, MockSendBehavior};
use super::super::{ApprovalResponseInput, CreateSessionInput, SendTurnInput};
use crate::{ProviderKind, TurnDispatchState};

#[tokio::test]
async fn late_local_decline_event_fault_preserves_pending_cache_and_never_dispatches() {
    for rejected_kind in ["approval.resolved", "turn.interrupted"] {
        let (manager, store, sent) =
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
            .unwrap();
        let admitted = manager
            .send_turn(
                &session.id,
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"no native effect"})],
                    expected_turn_id: None,
                    permission_mode: Some("require_approval".into()),
                    projection_source: None,
                    user_input_snapshot: None,
                    correlation_id: None,
                    turn_settings: Default::default(),
                },
            )
            .await
            .unwrap();
        let approval_id = store
            .approvals
            .lock()
            .unwrap()
            .values()
            .find(|row| row.turn_id == admitted.turn_id)
            .unwrap()
            .id
            .clone();
        *store.fail_next_event_append_for_kind.lock().unwrap() = Some(rejected_kind.into());
        let request = ApprovalResponseInput {
            decision: "decline".into(),
            payload: None,
        };
        assert!(manager
            .respond_approval(&session.id, &approval_id, request.clone())
            .await
            .is_err());
        assert_eq!(sent.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.approvals.lock().unwrap()[&approval_id].status,
            "pending"
        );
        assert_eq!(
            store.turns.lock().unwrap()[&admitted.turn_id].status,
            "waiting_for_approval"
        );
        assert_eq!(
            store.sessions.lock().unwrap()[&session.id].status,
            "waiting_for_approval"
        );
        assert_eq!(
            store.turn_admissions.lock().unwrap()[&admitted.turn_id].dispatch_state,
            TurnDispatchState::Pending
        );
        assert_eq!(
            manager.get_session(&session.id).await.unwrap().status,
            "waiting_for_approval"
        );
        assert!(store
            .events
            .lock()
            .unwrap()
            .iter()
            .all(|row| row.kind != "approval.resolved" && row.kind != "turn.interrupted"));
        manager
            .respond_approval(&session.id, &approval_id, request)
            .await
            .unwrap();
        assert_eq!(sent.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.approvals.lock().unwrap()[&approval_id].status,
            "decline"
        );
        assert_eq!(
            store.turns.lock().unwrap()[&admitted.turn_id].status,
            "interrupted"
        );
        assert_eq!(store.sessions.lock().unwrap()[&session.id].status, "ready");
        assert_eq!(
            store.turn_admissions.lock().unwrap()[&admitted.turn_id].dispatch_state,
            TurnDispatchState::NotDispatched
        );
        let events = store.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|row| row.kind == "approval.resolved")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|row| row.kind == "turn.interrupted")
                .count(),
            1
        );
    }
}
