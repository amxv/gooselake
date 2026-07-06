use serde_json::Value;
use tokio::time::{sleep, Duration};

use crate::{
    ApprovalRecord, ProviderKind, ProviderTurnResult, ProviderTurnStatus, RuntimeError,
    RuntimeStore, SessionRecord, TurnRecord,
};

use super::helpers::{append_session_transcript, extract_assistant_text_from_usage, now_ms};
use super::test_support::{
    manager_with_failing_send_provider, manager_with_permission_capture_provider,
    manager_with_provider, manager_with_provider_and_store,
};
use super::{ApprovalResponseInput, CreateSessionInput, ResumeSessionInput, SendTurnInput};

#[test]
fn assistant_text_extraction_supports_snake_and_camel_fields() {
    let usage_snake = serde_json::json!({ "last_message": "snake" });
    let usage_camel = serde_json::json!({ "lastMessage": "camel" });
    let usage_assistant_text = serde_json::json!({ "assistant_text": "provider-neutral" });
    assert_eq!(
        extract_assistant_text_from_usage(&usage_snake).as_deref(),
        Some("snake")
    );
    assert_eq!(
        extract_assistant_text_from_usage(&usage_camel).as_deref(),
        Some("camel")
    );
    assert_eq!(
        extract_assistant_text_from_usage(&usage_assistant_text).as_deref(),
        Some("provider-neutral")
    );
}

#[test]
fn append_session_transcript_migrates_legacy_key() {
    let mut metadata = serde_json::json!({
        "codex_transcript": [{"role":"assistant","text":"old"}]
    });
    append_session_transcript(&mut metadata, "assistant", "new");
    let rows = metadata
        .get("session_transcript")
        .and_then(Value::as_array)
        .expect("session transcript rows");
    assert_eq!(rows.len(), 2);
    assert!(metadata.get("codex_transcript").is_none());
}

#[tokio::test]
async fn one_active_turn_per_session_is_enforced() {
    let manager = manager_with_provider(200);
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

    let _ = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"first"})],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await
        .expect("first turn");

    let second = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"second"})],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await;
    assert!(matches!(second, Err(RuntimeError::InvalidState(_))));
}

#[tokio::test]
async fn duplicate_terminal_event_is_idempotent_and_conflict_fails_closed() {
    let manager = manager_with_provider(0);
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
                input: vec![serde_json::json!({"type":"text","text":"hello"})],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await
        .expect("send turn");

    // Let spawned wait complete first.
    sleep(Duration::from_millis(20)).await;

    manager
        .apply_terminal_result(ProviderTurnResult {
            runtime_session_id: session.id.clone(),
            turn_id: accepted.turn_id.clone(),
            status: ProviderTurnStatus::Completed,
            usage: None,
            error: None,
        })
        .await
        .expect("idempotent duplicate terminal");

    let conflict = manager
        .apply_terminal_result(ProviderTurnResult {
            runtime_session_id: session.id.clone(),
            turn_id: accepted.turn_id.clone(),
            status: ProviderTurnStatus::Failed,
            usage: None,
            error: None,
        })
        .await;
    assert!(matches!(conflict, Err(RuntimeError::ProtocolViolation(_))));

    let updated = manager
        .get_session(session.id.as_str())
        .await
        .expect("session");
    assert_eq!(updated.status, "failed");
    assert_eq!(updated.failure_code.as_deref(), Some("terminal_conflict"));
}

#[tokio::test]
async fn provider_turn_ownership_mismatch_is_rejected() {
    let manager = manager_with_provider(200);
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
                input: vec![serde_json::json!({"type":"text","text":"hello"})],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await
        .expect("send turn");

    let mismatched = manager
        .apply_terminal_result(ProviderTurnResult {
            runtime_session_id: "sess_other".to_string(),
            turn_id: accepted.turn_id,
            status: ProviderTurnStatus::Completed,
            usage: None,
            error: None,
        })
        .await;
    assert!(matches!(
        mismatched,
        Err(RuntimeError::ProtocolViolation(_))
    ));
}

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
            },
        )
        .await;
    assert!(matches!(send, Err(RuntimeError::Io(_))));

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
            },
        )
        .await;
    assert!(matches!(second, Err(RuntimeError::Io(_))));
}

#[tokio::test]
async fn send_turn_inherits_session_permission_mode_when_turn_omits_it() {
    let (manager, captured_permission_modes) = manager_with_permission_capture_provider();
    let session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: None,
            cwd: None,
            permission_mode: Some("full_auto".to_string()),
            metadata: None,
        })
        .await
        .expect("create session");

    let _ = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"inherit mode"})],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await
        .expect("send turn");

    let captured = captured_permission_modes
        .lock()
        .expect("captured permission modes")
        .clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].as_deref(), Some("full_auto"));
}

#[tokio::test]
async fn approval_requested_and_resolution_transitions_turn() {
    let manager = manager_with_provider(0);
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
                input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
            },
        )
        .await
        .expect("accepted waiting approval");
    assert_eq!(accepted.status, "waiting_for_approval");

    let events = manager
        .replay_session_events(session.id.as_str(), None, 50)
        .expect("events");
    let approval_event = events
        .iter()
        .find(|event| event.kind == "approval.requested")
        .expect("approval requested event");
    let approval_id = approval_event
        .payload
        .get("approval_id")
        .and_then(Value::as_str)
        .expect("approval id payload")
        .to_string();
    {
        let approvals = manager.approvals.read().await;
        let persisted = approvals
            .get(approval_id.as_str())
            .expect("pending approval");
        assert_eq!(persisted.status, "pending");
    }

    let resolved = manager
        .respond_approval(
            session.id.as_str(),
            approval_id.as_str(),
            ApprovalResponseInput {
                decision: "decline".to_string(),
                payload: Some(serde_json::json!({"reason":"not now"})),
            },
        )
        .await
        .expect("resolve approval");
    assert_eq!(resolved.status, "decline");
    {
        let approvals = manager.approvals.read().await;
        let persisted = approvals
            .get(approval_id.as_str())
            .expect("resolved approval");
        assert_eq!(persisted.status, "decline");
        assert!(persisted.resolved_at.is_some());
    }

    let updated = manager
        .get_session(session.id.as_str())
        .await
        .expect("session");
    assert_eq!(updated.active_turn_id, None);
    assert_eq!(updated.status, "ready");
}

#[tokio::test]
async fn approval_accept_is_case_insensitive_and_advances_turn() {
    let manager = manager_with_provider(0);
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
                input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
            },
        )
        .await
        .expect("accepted waiting approval");
    assert_eq!(accepted.status, "waiting_for_approval");

    let events = manager
        .replay_session_events(session.id.as_str(), None, 50)
        .expect("events");
    let approval_id = events
        .iter()
        .find(|event| event.kind == "approval.requested")
        .and_then(|event| event.payload.get("approval_id"))
        .and_then(Value::as_str)
        .expect("approval id")
        .to_string();

    let resolved = manager
        .respond_approval(
            session.id.as_str(),
            approval_id.as_str(),
            ApprovalResponseInput {
                decision: "Accept".to_string(),
                payload: None,
            },
        )
        .await
        .expect("resolve approval");
    assert_eq!(resolved.status, "accept");

    sleep(Duration::from_millis(20)).await;
    let updated = manager
        .get_session(session.id.as_str())
        .await
        .expect("session");
    assert_eq!(updated.active_turn_id, None);
    assert_eq!(updated.status, "ready");
}

#[tokio::test]
async fn approval_invalid_decision_is_rejected() {
    let manager = manager_with_provider(0);
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

    let _ = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
            },
        )
        .await
        .expect("accepted waiting approval");

    let events = manager
        .replay_session_events(session.id.as_str(), None, 50)
        .expect("events");
    let approval_id = events
        .iter()
        .find(|event| event.kind == "approval.requested")
        .and_then(|event| event.payload.get("approval_id"))
        .and_then(Value::as_str)
        .expect("approval id")
        .to_string();

    let result = manager
        .respond_approval(
            session.id.as_str(),
            approval_id.as_str(),
            ApprovalResponseInput {
                decision: "maybe".to_string(),
                payload: None,
            },
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::InvalidState(_))));

    let approvals = manager.approvals.read().await;
    let persisted = approvals
        .get(approval_id.as_str())
        .expect("pending approval still stored");
    assert_eq!(persisted.status, "pending");
    assert!(persisted.resolved_at.is_none());
}

#[tokio::test]
async fn explicit_resume_path_updates_session_and_emits_event() {
    let manager = manager_with_provider(0);
    let session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: None,
            cwd: Some("/tmp".to_string()),
            permission_mode: None,
            metadata: Some(serde_json::json!({"a":1})),
        })
        .await
        .expect("create session");

    let resumed = manager
        .resume_session(
            session.id.as_str(),
            ResumeSessionInput {
                provider_session_ref: None,
                canonical_provider_session_ref: None,
            },
        )
        .await
        .expect("resume session");
    assert_eq!(resumed.status, "ready");

    let events = manager
        .replay_session_events(session.id.as_str(), None, 20)
        .expect("events");
    assert!(
        events.iter().any(|event| event.kind == "session.resumed"),
        "session.resumed event missing"
    );
}

#[tokio::test]
async fn startup_recovery_clears_stale_active_turn_and_orphaned_pending_approval() {
    let (manager, store) = manager_with_provider_and_store(0);
    let now = now_ms();
    store
        .upsert_session(&SessionRecord {
            id: "sess_recover".to_string(),
            provider: "codex".to_string(),
            status: "turn_running".to_string(),
            cwd: None,
            model: None,
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("provider_ref_1".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: Some("turn_missing".to_string()),
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        })
        .expect("seed session");
    store
        .upsert_approval(&ApprovalRecord {
            id: "apr_orphan".to_string(),
            session_id: "sess_recover".to_string(),
            turn_id: "turn_missing".to_string(),
            tool_call_id: None,
            provider_approval_ref: None,
            status: "pending".to_string(),
            request: serde_json::json!({"reason":"manual"}),
            response: None,
            created_at: now,
            resolved_at: None,
        })
        .expect("seed approval");
    {
        let mut sessions = manager.sessions.write().await;
        sessions.insert(
            "sess_recover".to_string(),
            store
                .sessions
                .lock()
                .expect("sessions")
                .get("sess_recover")
                .cloned()
                .expect("session seeded"),
        );
    }
    {
        let mut approvals = manager.approvals.write().await;
        approvals.insert(
            "apr_orphan".to_string(),
            store
                .approvals
                .lock()
                .expect("approvals")
                .get("apr_orphan")
                .cloned()
                .expect("approval seeded"),
        );
    }

    let summary = manager.recover_startup().await.expect("startup recovery");
    assert!(summary.sessions_reconciled >= 1);
    assert!(summary.approvals_reconciled >= 1);
    let repaired = manager
        .get_session("sess_recover")
        .await
        .expect("repaired session");
    assert_eq!(repaired.active_turn_id, None);
    assert_eq!(repaired.status, "ready");
    let approvals = manager.approvals.read().await;
    let approval = approvals.get("apr_orphan").expect("approval present");
    assert_eq!(approval.status, "decline");
    assert!(approval.resolved_at.is_some());
}

#[tokio::test]
async fn startup_recovery_preserves_waiting_for_approval_turn_without_spawning_wait() {
    let (manager, store) = manager_with_provider_and_store(0);
    let now = now_ms();
    store
        .upsert_session(&SessionRecord {
            id: "sess_pending".to_string(),
            provider: "codex".to_string(),
            status: "waiting_for_approval".to_string(),
            cwd: None,
            model: None,
            permission_mode: Some("require_approval".to_string()),
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("provider_ref_pending".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: Some("turn_pending".to_string()),
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        })
        .expect("seed waiting session");
    store
        .upsert_turn(&TurnRecord {
            id: "turn_pending".to_string(),
            session_id: "sess_pending".to_string(),
            provider_turn_ref: None,
            status: "waiting_for_approval".to_string(),
            input: serde_json::json!([{ "type": "text", "text": "needs approval" }]),
            source: Some("user".to_string()),
            started_at: Some(now),
            completed_at: None,
            usage: None,
            error: None,
        })
        .expect("seed waiting turn");
    store
        .upsert_approval(&ApprovalRecord {
            id: "apr_pending".to_string(),
            session_id: "sess_pending".to_string(),
            turn_id: "turn_pending".to_string(),
            tool_call_id: None,
            provider_approval_ref: None,
            status: "pending".to_string(),
            request: serde_json::json!({"reason":"manual"}),
            response: None,
            created_at: now,
            resolved_at: None,
        })
        .expect("seed pending approval");
    {
        let mut sessions = manager.sessions.write().await;
        sessions.insert(
            "sess_pending".to_string(),
            store
                .sessions
                .lock()
                .expect("sessions")
                .get("sess_pending")
                .cloned()
                .expect("session seeded"),
        );
    }
    {
        let mut turns = manager.turns.write().await;
        turns.insert(
            "turn_pending".to_string(),
            store
                .turns
                .lock()
                .expect("turns")
                .get("turn_pending")
                .cloned()
                .expect("turn seeded"),
        );
    }
    {
        let mut approvals = manager.approvals.write().await;
        approvals.insert(
            "apr_pending".to_string(),
            store
                .approvals
                .lock()
                .expect("approvals")
                .get("apr_pending")
                .cloned()
                .expect("approval seeded"),
        );
    }

    let summary = manager.recover_startup().await.expect("startup recovery");
    assert_eq!(summary.resumed_waits, 0);
    let repaired = manager
        .get_session("sess_pending")
        .await
        .expect("repaired session");
    assert_eq!(repaired.status, "waiting_for_approval");
    assert_eq!(repaired.active_turn_id.as_deref(), Some("turn_pending"));

    let approvals = manager.approvals.read().await;
    let approval = approvals.get("apr_pending").expect("approval present");
    assert_eq!(approval.status, "pending");
    assert!(approval.resolved_at.is_none());

    let events = store.events.lock().expect("events lock").clone();
    assert!(
        !events.iter().any(|event| {
            event.session_id.as_deref() == Some("sess_pending")
                && matches!(
                    event.kind.as_str(),
                    "turn.completed" | "turn.interrupted" | "turn.failed" | "provider.error"
                )
        }),
        "startup recovery must not terminally reconcile waiting-for-approval turns"
    );
}
