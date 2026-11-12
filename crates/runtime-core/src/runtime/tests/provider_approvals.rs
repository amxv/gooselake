use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::time::{sleep, Duration};

use crate::{
    ApprovalRecord, PersistedUserInputSnapshot, ProviderKind, RuntimeStore, SessionRecord,
    TurnAdmissionRecord, TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState,
    TurnInputProjectionSource, TurnRecord,
};

use super::super::helpers::now_ms;
use super::super::test_support::{
    manager_with_observable_send_provider, manager_with_provider_approval_events,
    observable_manager_for_store_with_restore_probe, MockSendBehavior, MockStore,
};
use super::super::{ApprovalResponseInput, CreateSessionInput, SendTurnInput};

async fn native_decision_fixture() -> (
    Arc<super::super::RuntimeSessionManager>,
    Arc<MockStore>,
    String,
    ApprovalRecord,
) {
    let (manager, store, _) =
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
    let accepted = manager
        .send_turn(
            &session.id,
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
                turn_settings: Default::default(),
            },
        )
        .await
        .unwrap();
    let approval = manager
        .record_provider_approval(
            &session.id,
            &accepted.turn_id,
            "native-outcome-test",
            Some("tool-outcome".into()),
            serde_json::json!({"command":"true"}),
        )
        .await
        .unwrap();
    (manager, store, session.id, approval)
}

fn accept_native() -> ApprovalResponseInput {
    ApprovalResponseInput {
        decision: "accept".into(),
        payload: Some(serde_json::json!({"operator":"approved"})),
    }
}

#[tokio::test]
async fn native_success_requires_atomic_result_receipt_and_never_resends_after_fault() {
    let (manager, store, session_id, approval) = native_decision_fixture().await;
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("approval.resolved".into());
    let error = manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("MUST NOT be retried"));
    assert_eq!(store.native_approval_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.approvals.lock().unwrap()[&approval.id].status,
        "decision_pending"
    );
    assert_eq!(
        manager.approvals.read().await[&approval.id].status,
        "decision_pending"
    );
    assert_eq!(
        manager.turns.read().await[&approval.turn_id].status,
        "waiting_for_approval"
    );
    assert_eq!(
        manager.sessions.read().await[&session_id].status,
        "waiting_for_approval"
    );
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|row| row.kind != "approval.resolved"));
    assert!(manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .is_err());
    assert_eq!(store.native_approval_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_success_projects_approval_turn_session_and_critical_event_together() {
    let (manager, store, session_id, approval) = native_decision_fixture().await;
    let resolved = manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .unwrap();
    assert_eq!(resolved.status, "accept");
    assert_eq!(store.native_approval_calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.approvals.lock().unwrap()[&approval.id], resolved);
    assert_eq!(
        manager.turns.read().await[&approval.turn_id].status,
        "in_progress"
    );
    assert_eq!(
        manager.sessions.read().await[&session_id].status,
        "turn_running"
    );
    let rows = store.events.lock().unwrap();
    let resolution = rows
        .iter()
        .find(|event| event.kind == "approval.resolved")
        .unwrap();
    assert_eq!(resolution.payload["approval_id"], approval.id);
    assert_eq!(resolution.payload["decision"], "accept");
    assert!(resolution.payload["decision_attempt_id"].as_str().is_some());
}

#[tokio::test]
async fn ambiguous_native_decision_quarantines_atomically_and_stays_nonretryable() {
    let (manager, store, session_id, approval) = native_decision_fixture().await;
    *store.native_approval_behavior.lock().unwrap() = Some(MockSendBehavior::Unknown);
    let error = manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("approval_decision_unknown")
            || error
                .to_string()
                .contains("mock native approval response ambiguous")
    );
    assert_eq!(
        store.approvals.lock().unwrap()[&approval.id].status,
        "decision_pending"
    );
    assert_eq!(
        manager.turns.read().await[&approval.turn_id].status,
        "approval_decision_unknown"
    );
    assert_eq!(
        manager.sessions.read().await[&session_id].status,
        "turn_recovery_required"
    );
    let rows = store.events.lock().unwrap();
    let uncertain = rows
        .iter()
        .find(|row| row.kind == "approval.decision_unknown")
        .unwrap();
    assert_eq!(uncertain.payload["approval_id"], approval.id);
    assert!(uncertain.payload["decision_attempt_id"].as_str().is_some());
    drop(rows);
    assert!(manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .is_err());
    assert_eq!(store.native_approval_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ambiguous_native_journal_fault_never_partially_quarantines_or_resends() {
    let (manager, store, session_id, approval) = native_decision_fixture().await;
    *store.native_approval_behavior.lock().unwrap() = Some(MockSendBehavior::Unknown);
    *store.fail_next_event_append_for_kind.lock().unwrap() =
        Some("approval.decision_unknown".into());
    let error = manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("quarantine receipt failed"));
    assert_eq!(
        store.approvals.lock().unwrap()[&approval.id].status,
        "decision_pending"
    );
    assert_eq!(
        manager.turns.read().await[&approval.turn_id].status,
        "waiting_for_approval"
    );
    assert_eq!(
        manager.sessions.read().await[&session_id].status,
        "waiting_for_approval"
    );
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|row| row.kind != "approval.decision_unknown"));
    assert!(manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .is_err());
    assert_eq!(store.native_approval_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn proven_native_not_dispatched_releases_only_that_attempt() {
    let (manager, store, session_id, approval) = native_decision_fixture().await;
    *store.native_approval_behavior.lock().unwrap() = Some(MockSendBehavior::NotDispatched);
    assert!(manager
        .respond_approval(&session_id, &approval.id, accept_native())
        .await
        .is_err());
    assert_eq!(store.approvals.lock().unwrap()[&approval.id], approval);
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.kind == "approval.decision_not_dispatched")
            .count(),
        1
    );
    *store.native_approval_behavior.lock().unwrap() = None;
    assert_eq!(
        manager
            .respond_approval(&session_id, &approval.id, accept_native())
            .await
            .unwrap()
            .status,
        "accept"
    );
    assert_eq!(store.native_approval_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn duplicate_provider_approval_event_converges_on_one_durable_record() {
    let (manager, store, _send_count) =
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
                input: vec![serde_json::json!({"type":"text","text":"needs tool"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
                turn_settings: Default::default(),
            },
        )
        .await
        .expect("send turn");
    let first = manager
        .record_provider_approval(
            session.id.as_str(),
            accepted.turn_id.as_str(),
            "provider-approval-7",
            Some("tool-7".to_string()),
            serde_json::json!({"tool":"shell","command":"echo ok"}),
        )
        .await
        .expect("provider approval");
    let duplicate = manager
        .record_provider_approval(
            session.id.as_str(),
            accepted.turn_id.as_str(),
            "provider-approval-7",
            Some("tool-7".to_string()),
            serde_json::json!({"tool":"shell","command":"echo ok"}),
        )
        .await
        .expect("duplicate provider approval");
    assert_eq!(first.id, duplicate.id);
    assert_eq!(first.origin, "provider");
    assert_eq!(
        first.provider_approval_ref.as_deref(),
        Some("provider-approval-7")
    );
    let matching = manager
        .approvals
        .read()
        .await
        .values()
        .filter(|approval| approval.provider_approval_ref.as_deref() == Some("provider-approval-7"))
        .count();
    assert_eq!(matching, 1);
    let journal = store.events.lock().unwrap();
    let requests = journal
        .iter()
        .filter(|row| row.kind == "approval.requested")
        .collect::<Vec<_>>();
    assert_eq!(
        requests.len(),
        1,
        "native callback must not emit a second critical event"
    );
    assert_eq!(
        requests[0].event_id,
        format!("evt_approval_{}_requested", first.id)
    );
    assert_eq!(
        requests[0].payload["provider_approval_ref"],
        "provider-approval-7"
    );
}

#[tokio::test]
async fn native_approval_journal_failure_never_publishes_partial_caches_or_durable_authority() {
    let (manager, store, _send_count) =
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
    let accepted = manager
        .send_turn(
            &session.id,
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"approval request"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
                turn_settings: Default::default(),
            },
        )
        .await
        .unwrap();
    let previous_session = manager.get_session(&session.id).await.unwrap();
    let previous_turn = manager.turns.read().await[&accepted.turn_id].clone();
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("approval.requested".into());
    assert!(manager
        .record_provider_approval(
            &session.id,
            &accepted.turn_id,
            "native-ref-failed-once",
            Some("tool-req".into()),
            serde_json::json!({"command":"true"}),
        )
        .await
        .is_err());
    assert_eq!(
        manager.get_session(&session.id).await.unwrap(),
        previous_session
    );
    assert_eq!(manager.turns.read().await[&accepted.turn_id], previous_turn);
    assert!(manager.approvals.read().await.is_empty());
    assert_eq!(
        store.sessions.lock().unwrap()[&session.id],
        previous_session
    );
    assert_eq!(
        store.turns.lock().unwrap()[&accepted.turn_id],
        previous_turn
    );
    assert!(store.approvals.lock().unwrap().is_empty());
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|row| row.kind != "approval.requested"));

    let admitted = manager
        .record_provider_approval(
            &session.id,
            &accepted.turn_id,
            "native-ref-failed-once",
            Some("tool-req".into()),
            serde_json::json!({"command":"true"}),
        )
        .await
        .unwrap();
    assert_eq!(
        manager.get_session(&session.id).await.unwrap().status,
        "waiting_for_approval"
    );
    assert_eq!(
        manager.turns.read().await[&accepted.turn_id].status,
        "waiting_for_approval"
    );
    assert_eq!(store.approvals.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.kind == "approval.requested")
            .count(),
        1
    );
    assert_eq!(admitted.origin, "provider");
}

#[tokio::test]
async fn native_decision_reservation_event_fault_prevents_provider_side_effect_and_cache_replay() {
    let (manager, store, _send_count) =
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
    let accepted = manager
        .send_turn(
            &session.id,
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"requires native approval"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
                turn_settings: Default::default(),
            },
        )
        .await
        .unwrap();
    let approval = manager
        .record_provider_approval(
            &session.id,
            &accepted.turn_id,
            "native-decision-gate",
            Some("tool-gated".into()),
            serde_json::json!({"command":"true"}),
        )
        .await
        .unwrap();
    *store.fail_next_event_append_for_kind.lock().unwrap() =
        Some("approval.decision_reserved".into());
    assert!(manager
        .respond_approval(
            &session.id,
            &approval.id,
            ApprovalResponseInput {
                decision: "accept".into(),
                payload: Some(serde_json::json!({"visible":"payload"})),
            },
        )
        .await
        .is_err());
    assert_eq!(manager.approvals.read().await[&approval.id], approval);
    assert_eq!(store.approvals.lock().unwrap()[&approval.id], approval);
    assert_eq!(
        manager.get_session(&session.id).await.unwrap().status,
        "waiting_for_approval"
    );
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|e| { e.kind != "approval.decision_reserved" && e.kind != "approval.resolved" }));

    let resolved = manager
        .respond_approval(
            &session.id,
            &approval.id,
            ApprovalResponseInput {
                decision: "accept".into(),
                payload: Some(serde_json::json!({"visible":"payload"})),
            },
        )
        .await
        .unwrap();
    assert_eq!(resolved.status, "accept");
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == "approval.decision_reserved")
            .count(),
        1,
    );
    let journal = store.events.lock().unwrap();
    let reserved = journal
        .iter()
        .find(|e| e.kind == "approval.decision_reserved")
        .unwrap();
    assert_eq!(reserved.payload["approval_id"], approval.id);
    assert_eq!(reserved.payload["decision"], "accept");
    assert!(reserved.payload["decision_attempt_id"].as_str().is_some());
}

#[tokio::test]
async fn provider_approval_emitted_before_ack_is_durable_and_deduplicated() {
    let (manager, store, wait_count) = manager_with_provider_approval_events();
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
                input: vec![serde_json::json!({"type":"text","text":"use a tool"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
                turn_settings: Default::default(),
            },
        )
        .await
        .expect("send turn");

    for _ in 0..20 {
        if store
            .approvals
            .lock()
            .expect("approvals")
            .values()
            .any(|approval| {
                approval.provider_approval_ref.as_deref() == Some("provider-approval-before-ack")
            })
        {
            break;
        }
        sleep(Duration::from_millis(5)).await;
    }
    let approvals = store.approvals.lock().expect("approvals");
    let matching = approvals
        .values()
        .filter(|approval| {
            approval.provider_approval_ref.as_deref() == Some("provider-approval-before-ack")
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    let approval = matching[0].clone();
    drop(approvals);
    assert_eq!(approval.origin, "provider");
    assert_eq!(approval.turn_id, accepted.turn_id);
    assert_eq!(wait_count.load(Ordering::SeqCst), 1);

    manager
        .respond_approval(
            session.id.as_str(),
            approval.id.as_str(),
            ApprovalResponseInput {
                decision: "accept".to_string(),
                payload: None,
            },
        )
        .await
        .expect("respond provider approval");
    sleep(Duration::from_millis(20)).await;
    assert_eq!(
        wait_count.load(Ordering::SeqCst),
        1,
        "approval resolution must not spawn a duplicate provider waiter"
    );
}

#[tokio::test]
async fn startup_recovery_restores_provider_approval_turn_without_resending() {
    let store = std::sync::Arc::new(MockStore::default());
    let now = now_ms();
    let session_id = "sess_provider_approval_restart".to_string();
    let turn_id = "turn_provider_approval_restart".to_string();
    let provider_native_turn_id = "native_provider_approval_restart".to_string();

    store
        .upsert_session(&SessionRecord {
            id: session_id.clone(),
            provider: "codex".to_string(),
            status: "waiting_for_approval".to_string(),
            cwd: None,
            model: None,
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some(format!("mock:{session_id}")),
            canonical_provider_session_ref: None,
            active_turn_id: Some(turn_id.clone()),
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        })
        .expect("seed provider approval session");
    store
        .upsert_turn(&TurnRecord {
            id: turn_id.clone(),
            session_id: session_id.clone(),
            provider_turn_ref: None,
            status: "waiting_for_approval".to_string(),
            input: serde_json::json!([{ "type": "text", "text": "provider approval" }]),
            source: Some("user_visible".to_string()),
            started_at: Some(now),
            completed_at: None,
            usage: None,
            error: None,
        })
        .expect("seed provider approval turn");
    store
        .upsert_turn_admission(&TurnAdmissionRecord {
            turn_id: turn_id.clone(),
            session_id: session_id.clone(),
            provider: "codex".to_string(),
            projection_source: TurnInputProjectionSource::UserVisible,
            user_input_snapshot: PersistedUserInputSnapshot {
                prompt_text: "provider approval".to_string(),
                image_refs: Vec::new(),
                invocation: None,
            },
            dispatch_policy: TurnDispatchPolicySnapshot {
                permission_mode: None,
                pre_dispatch_approval_required: false,
                turn_settings: Default::default(),
            },
            correlation: TurnCorrelationState::default(),
            dispatch_state: TurnDispatchState::Dispatched,
            provider_native_turn_id: Some(provider_native_turn_id.clone()),
            dispatch_error: None,
            admitted_at: now,
            updated_at: now,
        })
        .expect("seed provider approval admission");
    store
        .upsert_approval(&ApprovalRecord {
            id: "apr_provider_restart".to_string(),
            session_id: session_id.clone(),
            turn_id: turn_id.clone(),
            origin: "provider".to_string(),
            tool_call_id: Some("tool-provider-restart".to_string()),
            provider_approval_ref: Some("provider-approval-restart".to_string()),
            status: "pending".to_string(),
            request: serde_json::json!({"tool":"shell","command":"echo restart"}),
            response: None,
            created_at: now,
            resolved_at: None,
        })
        .expect("seed provider approval");

    let (manager, send_count, restored_turn_mappings) =
        observable_manager_for_store_with_restore_probe(
            store.clone(),
            MockSendBehavior::Success,
            None,
        );
    let summary = manager.recover_startup().await.expect("startup recovery");

    assert_eq!(send_count.load(Ordering::SeqCst), 0);
    assert_eq!(summary.resumed_waits, 1);
    assert_eq!(
        restored_turn_mappings
            .lock()
            .expect("restored turn mappings")
            .as_slice(),
        &[(session_id.clone(), turn_id.clone(), provider_native_turn_id,)]
    );
    let recovered = manager
        .get_session(session_id.as_str())
        .await
        .expect("recovered provider approval session");
    assert_eq!(recovered.status, "waiting_for_approval");
    let approval = manager
        .approvals
        .read()
        .await
        .get("apr_provider_restart")
        .cloned()
        .expect("recovered provider approval");
    assert_eq!(approval.origin, "provider");
    assert_eq!(approval.status, "pending");
}
