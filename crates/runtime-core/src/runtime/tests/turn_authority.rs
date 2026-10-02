use serde_json::Value;
use std::sync::atomic::Ordering;

use crate::{
    PersistedUserInputSnapshot, PersistedUserInputSnapshotImageRef,
    PersistedUserInputSnapshotInvocation, ProviderKind, RuntimeError, RuntimeStore,
    TurnDispatchState, TurnInputProjectionSource,
};

use super::super::helpers::now_ms;
use super::super::test_support::{
    manager_with_failing_send_provider, manager_with_observable_send_provider,
    observable_manager_for_store, observable_manager_for_store_with_restore_probe,
    MockSendBehavior,
};
use super::super::{ApprovalResponseInput, CreateSessionInput, SendTurnInput};

#[tokio::test]
async fn send_turn_failure_does_not_leave_session_bricked() {
    let manager = manager_with_failing_send_provider();
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

    let send = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"hello"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await;
    assert!(matches!(
        send,
        Err(RuntimeError::ProviderDispatch {
            outcome: "not_dispatched",
            ..
        })
    ));

    let updated = manager
        .get_session(session.id.as_str())
        .await
        .expect("session");
    assert_eq!(updated.active_turn_id, None);
    assert_eq!(updated.status, "ready");

    // A follow-up send is still allowed to proceed to provider dispatch path.
    let second = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"again"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await;
    assert!(matches!(
        second,
        Err(RuntimeError::ProviderDispatch {
            outcome: "not_dispatched",
            ..
        })
    ));
}

#[tokio::test]
async fn turn_admission_is_durable_before_provider_dispatch_and_preserves_provenance() {
    let (manager, store, send_count) = manager_with_observable_send_provider(
        MockSendBehavior::Success,
        Some("native-turn-42".to_string()),
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
        .expect("create session");
    let snapshot = PersistedUserInputSnapshot {
        prompt_text: "automated prompt".to_string(),
        image_refs: vec![PersistedUserInputSnapshotImageRef {
            media_type: "image/png".to_string(),
            persisted_absolute_path: Some("/tmp/reference.png".to_string()),
            persisted_display_path: Some("reference.png".to_string()),
        }],
        invocation: Some(PersistedUserInputSnapshotInvocation {
            provider: "runtime".to_string(),
            name: "nightly".to_string(),
            display_name: Some("Nightly automation".to_string()),
            path: Some("automation/nightly".to_string()),
        }),
    };
    let accepted = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"automated prompt"})],
                expected_turn_id: Some("prior-turn".to_string()),
                permission_mode: None,
                projection_source: Some(TurnInputProjectionSource::AutomationContext),
                user_input_snapshot: Some(snapshot.clone()),
                correlation_id: Some("job-123".to_string()),
            },
        )
        .await
        .expect("send turn");

    assert_eq!(send_count.load(Ordering::SeqCst), 1);
    let admission = store
        .turn_admissions
        .lock()
        .expect("turn admissions")
        .get(&accepted.turn_id)
        .cloned()
        .expect("admission");
    assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatched);
    assert_eq!(
        admission.provider_native_turn_id.as_deref(),
        Some("native-turn-42")
    );
    assert_eq!(
        admission.projection_source,
        TurnInputProjectionSource::AutomationContext
    );
    assert_eq!(admission.user_input_snapshot, snapshot);
    assert_eq!(
        admission.correlation.expected_turn_id.as_deref(),
        Some("prior-turn")
    );
    assert_eq!(
        admission.correlation.correlation_id.as_deref(),
        Some("job-123")
    );
    let turn = store
        .turns
        .lock()
        .expect("turns")
        .get(&accepted.turn_id)
        .cloned()
        .expect("turn");
    assert_eq!(turn.source.as_deref(), Some("automation_context"));
}

#[tokio::test]
async fn failed_durable_admission_never_calls_provider() {
    let (manager, store, send_count) =
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
    *store.fail_admission.lock().expect("fail admission") = true;

    let result = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"must not dispatch"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::Io(_))));
    assert_eq!(send_count.load(Ordering::SeqCst), 0);
    assert!(store.turns.lock().expect("turns").is_empty());
    assert!(store
        .turn_admissions
        .lock()
        .expect("turn admissions")
        .is_empty());
    let session = manager
        .get_session(session.id.as_str())
        .await
        .expect("session after admission failure");
    assert!(session.active_turn_id.is_none());
    assert_eq!(session.status, "ready");
}

#[tokio::test]
async fn pre_dispatch_policy_approval_blocks_provider_until_acceptance() {
    let (manager, store, send_count) = manager_with_observable_send_provider(
        MockSendBehavior::Success,
        Some("native-after-approval".to_string()),
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
        .expect("create session");

    let accepted = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"guard me"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("admit guarded turn");

    assert_eq!(accepted.status, "waiting_for_approval");
    assert_eq!(send_count.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .turn_admissions
            .lock()
            .expect("turn admissions")
            .get(&accepted.turn_id)
            .expect("turn admission")
            .dispatch_state,
        TurnDispatchState::Pending
    );
    let approval_id = store
        .approvals
        .lock()
        .expect("approvals")
        .values()
        .find(|approval| approval.turn_id == accepted.turn_id)
        .expect("approval")
        .id
        .clone();

    manager
        .respond_approval(
            session.id.as_str(),
            approval_id.as_str(),
            ApprovalResponseInput {
                decision: "accept".to_string(),
                payload: None,
            },
        )
        .await
        .expect("accept guarded turn");

    assert_eq!(send_count.load(Ordering::SeqCst), 1);
    let admission = store
        .turn_admissions
        .lock()
        .expect("turn admissions")
        .get(&accepted.turn_id)
        .cloned()
        .expect("turn admission");
    assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatched);
    assert_eq!(
        admission.provider_native_turn_id.as_deref(),
        Some("native-after-approval")
    );
}

#[tokio::test]
async fn declining_pre_dispatch_policy_approval_never_calls_provider() {
    let (manager, store, send_count) =
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
                input: vec![serde_json::json!({"type":"text","text":"do not run"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("admit guarded turn");
    let approval_id = store
        .approvals
        .lock()
        .expect("approvals")
        .values()
        .find(|approval| approval.turn_id == accepted.turn_id)
        .expect("approval")
        .id
        .clone();

    manager
        .respond_approval(
            session.id.as_str(),
            approval_id.as_str(),
            ApprovalResponseInput {
                decision: "decline".to_string(),
                payload: None,
            },
        )
        .await
        .expect("decline guarded turn");

    assert_eq!(send_count.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .turn_admissions
            .lock()
            .expect("turn admissions")
            .get(&accepted.turn_id)
            .expect("turn admission")
            .dispatch_state,
        TurnDispatchState::NotDispatched
    );
    let session = manager
        .get_session(session.id.as_str())
        .await
        .expect("session");
    assert_eq!(session.status, "ready");
    assert!(session.active_turn_id.is_none());
}

#[tokio::test]
async fn unknown_provider_dispatch_is_quarantined_and_not_blindly_retried() {
    let (manager, store, send_count) =
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
        .expect("create session");
    let first = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"ambiguous"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await;
    assert!(matches!(
        first,
        Err(RuntimeError::ProviderDispatch {
            outcome: "unknown",
            ..
        })
    ));
    assert_eq!(send_count.load(Ordering::SeqCst), 1);
    let quarantined = manager
        .get_session(session.id.as_str())
        .await
        .expect("quarantined session");
    let turn_id = quarantined
        .active_turn_id
        .clone()
        .expect("unknown turn remains active");
    assert_eq!(quarantined.status, "turn_recovery_required");
    assert_eq!(
        store
            .turn_admissions
            .lock()
            .expect("turn admissions")
            .get(&turn_id)
            .expect("admission")
            .dispatch_state,
        TurnDispatchState::Unknown
    );

    let second = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"do not replay"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await;
    assert!(matches!(second, Err(RuntimeError::InvalidState(_))));
    assert_eq!(send_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn startup_recovery_replays_committed_pending_turn_once_after_approval_was_persisted() {
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
                input: vec![serde_json::json!({"type":"text","text":"resume me"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: Some("restart-pending".to_string()),
            },
        )
        .await
        .expect("admit turn");
    assert_eq!(initial_send_count.load(Ordering::SeqCst), 0);

    let approval_id = store
        .approvals
        .lock()
        .expect("approvals")
        .values()
        .find(|approval| approval.turn_id == accepted.turn_id)
        .expect("pre-dispatch approval")
        .id
        .clone();
    {
        let mut approvals = manager.approvals.write().await;
        let approval = approvals.get_mut(&approval_id).expect("approval in memory");
        approval.status = "accept".to_string();
        approval.resolved_at = Some(now_ms());
        store
            .upsert_approval(approval)
            .expect("persist accepted approval before simulated restart");
    }
    drop(manager);

    let (restarted, restart_send_count) = observable_manager_for_store(
        store.clone(),
        MockSendBehavior::Success,
        Some("native-after-restart".to_string()),
    );
    restarted.recover_startup().await.expect("startup recovery");

    assert_eq!(restart_send_count.load(Ordering::SeqCst), 1);
    let admission = store
        .turn_admissions
        .lock()
        .expect("turn admissions")
        .get(&accepted.turn_id)
        .cloned()
        .expect("admission after restart");
    assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatched);
    assert_eq!(
        admission.provider_native_turn_id.as_deref(),
        Some("native-after-restart")
    );
}

#[tokio::test]
async fn startup_recovery_quarantines_dispatching_turn_without_resending() {
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
                input: vec![serde_json::json!({"type":"text","text":"ambiguous boundary"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: Some("dispatching-restart".to_string()),
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
    drop(manager);

    let (restarted, restart_send_count) =
        observable_manager_for_store(store.clone(), MockSendBehavior::Success, None);
    restarted.recover_startup().await.expect("startup recovery");

    assert_eq!(restart_send_count.load(Ordering::SeqCst), 0);
    let admission = store
        .turn_admissions
        .lock()
        .expect("turn admissions")
        .get(&accepted.turn_id)
        .cloned()
        .expect("admission after restart");
    assert_eq!(admission.dispatch_state, TurnDispatchState::Unknown);
    let session = restarted
        .get_session(session.id.as_str())
        .await
        .expect("restarted session");
    assert_eq!(session.status, "turn_recovery_required");
    assert_eq!(
        session.active_turn_id.as_deref(),
        Some(accepted.turn_id.as_str())
    );
}

#[tokio::test]
async fn startup_recovery_preserves_native_mapping_without_resending_dispatched_turn() {
    let (manager, store, initial_send_count) = manager_with_observable_send_provider(
        MockSendBehavior::Success,
        Some("native-before-restart".to_string()),
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
        .expect("create session");
    let accepted = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"already dispatched"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("send turn");
    assert_eq!(initial_send_count.load(Ordering::SeqCst), 1);

    let (restarted, restart_send_count, restored_turn_mappings) =
        observable_manager_for_store_with_restore_probe(
            store.clone(),
            MockSendBehavior::Success,
            None,
        );
    restarted.recover_startup().await.expect("startup recovery");

    assert_eq!(restart_send_count.load(Ordering::SeqCst), 0);
    assert_eq!(
        restored_turn_mappings
            .lock()
            .expect("restored turn mappings")
            .as_slice(),
        &[(
            session.id.clone(),
            accepted.turn_id.clone(),
            "native-before-restart".to_string(),
        )]
    );
    let admission = store
        .turn_admissions
        .lock()
        .expect("turn admissions")
        .get(&accepted.turn_id)
        .cloned()
        .expect("admission after restart");
    assert_eq!(admission.dispatch_state, TurnDispatchState::Dispatched);
    assert_eq!(
        admission.provider_native_turn_id.as_deref(),
        Some("native-before-restart")
    );
}

#[tokio::test]
async fn durable_native_turn_mapping_cannot_be_rewritten_or_regressed() {
    let (manager, _store, _send_count) = manager_with_observable_send_provider(
        MockSendBehavior::Success,
        Some("native-stable".to_string()),
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
        .expect("create session");
    let accepted = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"stable mapping"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("send turn");

    let remap = manager
        .update_turn_dispatch_authority(
            accepted.turn_id.as_str(),
            TurnDispatchState::Dispatched,
            Some("native-different".to_string()),
            None,
        )
        .await;
    assert!(matches!(remap, Err(RuntimeError::ProtocolViolation(_))));

    let regression = manager
        .update_turn_dispatch_authority(
            accepted.turn_id.as_str(),
            TurnDispatchState::Dispatching,
            None,
            None,
        )
        .await;
    assert!(matches!(
        regression,
        Err(RuntimeError::ProtocolViolation(_))
    ));
}

#[tokio::test]
async fn startup_recovery_keeps_pending_pre_dispatch_approval_undispatched() {
    let (manager, store, send_count) =
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
                input: vec![serde_json::json!({"type":"text","text":"wait for approval"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("admit guarded turn");
    assert_eq!(send_count.load(Ordering::SeqCst), 0);

    manager.recover_startup().await.expect("startup recovery");

    assert_eq!(send_count.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .turn_admissions
            .lock()
            .expect("turn admissions")
            .get(&accepted.turn_id)
            .expect("turn admission")
            .dispatch_state,
        TurnDispatchState::Pending
    );
    let session = manager
        .get_session(session.id.as_str())
        .await
        .expect("session");
    assert_eq!(session.status, "waiting_for_approval");
}

#[tokio::test]
async fn startup_recovery_refuses_replay_when_persisted_input_attachment_is_missing() {
    let (manager, store, send_count) =
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
    let missing_path = format!(
        "/tmp/gooselake-missing-attachment-{}-{}.png",
        std::process::id(),
        now_ms()
    );
    let accepted = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"approved after crash"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                projection_source: None,
                user_input_snapshot: Some(PersistedUserInputSnapshot {
                    prompt_text: "approved after crash".to_string(),
                    image_refs: vec![PersistedUserInputSnapshotImageRef {
                        media_type: "image/png".to_string(),
                        persisted_absolute_path: Some(missing_path.clone()),
                        persisted_display_path: Some("missing.png".to_string()),
                    }],
                    invocation: None,
                }),
                correlation_id: None,
            },
        )
        .await
        .expect("admit guarded turn");
    assert_eq!(send_count.load(Ordering::SeqCst), 0);
    let approval_id = store
        .approvals
        .lock()
        .expect("approvals")
        .values()
        .find(|approval| approval.turn_id == accepted.turn_id)
        .expect("approval")
        .id
        .clone();
    {
        let mut approvals = manager.approvals.write().await;
        let approval = approvals.get_mut(&approval_id).expect("approval in memory");
        approval.status = "accept".to_string();
        approval.resolved_at = Some(now_ms());
        store
            .upsert_approval(approval)
            .expect("persist accepted approval");
    }

    manager.recover_startup().await.expect("startup recovery");

    assert_eq!(send_count.load(Ordering::SeqCst), 0);
    let admission = store
        .turn_admissions
        .lock()
        .expect("turn admissions")
        .get(&accepted.turn_id)
        .cloned()
        .expect("turn admission");
    assert_eq!(admission.dispatch_state, TurnDispatchState::NotDispatched);
    let error = admission.dispatch_error.expect("dispatch error");
    assert_eq!(
        error.get("code").and_then(Value::as_str),
        Some("missing_turn_input_attachment")
    );
    assert!(error
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| message.contains(missing_path.as_str())));
}
