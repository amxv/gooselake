use std::time::Duration;

use runtime_core::{
    ProviderApprovalResponseRequest, ProviderCreateSessionRequest, ProviderKind,
    ProviderRuntimeEvent, ProviderSendTurnRequest, ProviderTurnStatus, ProviderWaitTurnRequest,
    RuntimeError, RuntimeProvider,
};
use serde_json::json;

use super::support::FakeAgentHarness;

fn create(id: &str) -> ProviderCreateSessionRequest {
    ProviderCreateSessionRequest {
        runtime_session_id: id.into(),
        model: None,
        cwd: None,
        permission_mode: None,
        setting_sources: Vec::new(),
        system_prompt: None,
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        harness_version_slot: None,
        metadata: None,
    }
}

fn send(session: &str, turn: &str, text: &str) -> ProviderSendTurnRequest {
    ProviderSendTurnRequest {
        runtime_session_id: session.into(),
        turn_id: turn.into(),
        input: vec![json!({"type":"text", "text":text})],
        expected_turn_id: None,
        permission_mode: None,
        approval_id: None,
    }
}

async fn request_approval(
    provider: &impl RuntimeProvider,
    events: &mut tokio::sync::broadcast::Receiver<ProviderRuntimeEvent>,
    session: &str,
    turn: &str,
) -> String {
    provider
        .send_turn(send(session, turn, "permission please"))
        .await
        .expect("send turn");
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(ProviderRuntimeEvent::ApprovalRequested {
                runtime_session_id,
                turn_id,
                provider_approval_ref,
                tool_call_id,
                request,
            }) = events.recv().await
            {
                assert_eq!(runtime_session_id, session);
                assert_eq!(turn_id, turn);
                assert_eq!(tool_call_id.as_deref(), Some("call_permission"));
                assert!(request["options"].is_array());
                break provider_approval_ref;
            }
        }
    })
    .await
    .expect("approval event timeout");
    assert!(observed.starts_with("acp:"));
    observed
}

#[tokio::test]
async fn native_permission_accept_and_decline_reach_exact_rpc_requests() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let mut events = provider.subscribe_events().unwrap();
    provider
        .create_session(create("agent-native"))
        .await
        .unwrap();

    for (turn, decision, expected) in [
        ("turn-allow", "accept", "Permission approved."),
        ("turn-reject", "decline", "Permission rejected."),
    ] {
        let approval_ref = request_approval(&provider, &mut events, "agent-native", turn).await;
        provider
            .respond_approval(ProviderApprovalResponseRequest {
                runtime_session_id: "agent-native".into(),
                turn_id: turn.into(),
                approval_id: approval_ref,
                decision: decision.into(),
                payload: None,
            })
            .await
            .expect("respond native approval");
        let result = provider
            .wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: "agent-native".into(),
                turn_id: turn.into(),
                timeout_ms: Some(5000),
            })
            .await
            .expect("wait native turn");
        assert_eq!(result.status, ProviderTurnStatus::Completed);
        assert_eq!(result.usage.unwrap()["last_message"], expected);
    }
}

#[tokio::test]
async fn duplicated_native_permission_event_converges() {
    let harness = FakeAgentHarness::new("duplicate_permission");
    let provider = harness.provider();
    let mut events = provider.subscribe_events().unwrap();
    provider
        .create_session(create("agent-duplicates"))
        .await
        .unwrap();
    let ref_id = request_approval(&provider, &mut events, "agent-duplicates", "dup-turn").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err()
    );
    provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "agent-duplicates".into(),
            turn_id: "dup-turn".into(),
            approval_id: ref_id,
            decision: "accept".into(),
            payload: None,
        })
        .await
        .unwrap();
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "agent-duplicates".into(),
            turn_id: "dup-turn".into(),
            timeout_ms: Some(5000),
        })
        .await
        .unwrap();
    assert_eq!(result.status, ProviderTurnStatus::Completed);
}

#[tokio::test]
async fn reused_native_rpc_id_is_scoped_by_logical_turn() {
    let harness = FakeAgentHarness::new("reuse_permission_id");
    let provider = harness.provider();
    let mut events = provider.subscribe_events().unwrap();
    provider
        .create_session(create("reuse-agent"))
        .await
        .unwrap();
    let mut approval_refs = Vec::new();

    for turn in ["first-turn", "second-turn"] {
        let approval_ref = request_approval(&provider, &mut events, "reuse-agent", turn).await;
        approval_refs.push(approval_ref.clone());
        provider
            .respond_approval(ProviderApprovalResponseRequest {
                runtime_session_id: "reuse-agent".into(),
                turn_id: turn.into(),
                approval_id: approval_ref,
                decision: "accept".into(),
                payload: None,
            })
            .await
            .unwrap();
        assert_eq!(
            provider
                .wait_for_turn(ProviderWaitTurnRequest {
                    runtime_session_id: "reuse-agent".into(),
                    turn_id: turn.into(),
                    timeout_ms: Some(5000),
                })
                .await
                .unwrap()
                .status,
            ProviderTurnStatus::Completed
        );
    }
    assert_ne!(approval_refs[0], approval_refs[1]);
}

#[tokio::test]
async fn invalid_option_and_unoffered_selection_fail_closed() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let mut events = provider.subscribe_events().unwrap();
    provider
        .create_session(create("agent-options"))
        .await
        .unwrap();
    let ref_id = request_approval(&provider, &mut events, "agent-options", "options-turn").await;
    let error = provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "agent-options".into(),
            turn_id: "options-turn".into(),
            approval_id: ref_id.clone(),
            decision: "accept".into(),
            payload: Some(json!({"optionId":"not-offered"})),
        })
        .await
        .expect_err("must not invent selection");
    assert_eq!(
        error.provider_dispatch_code(),
        Some("invalid_acp_permission_option")
    );
    assert_eq!(
        error.provider_dispatch_outcome(),
        runtime_core::ProviderDispatchOutcome::NotDispatched
    );
    let malformed = provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "agent-options".into(),
            turn_id: "options-turn".into(),
            approval_id: ref_id.clone(),
            decision: "accept".into(),
            payload: Some(json!({"optionId": 9})),
        })
        .await
        .expect_err("a non-string optionId cannot silently select default allow");
    assert_eq!(
        malformed.provider_dispatch_outcome(),
        runtime_core::ProviderDispatchOutcome::NotDispatched
    );
    provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "agent-options".into(),
            turn_id: "options-turn".into(),
            approval_id: ref_id,
            decision: "accept".into(),
            payload: None,
        })
        .await
        .expect("a valid choice can follow a rejected local option");
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "agent-options".into(),
            turn_id: "options-turn".into(),
            timeout_ms: Some(5000),
        })
        .await
        .unwrap();
    assert_eq!(result.status, ProviderTurnStatus::Completed);
    assert_eq!(
        result.usage.unwrap()["last_message"],
        "Permission approved."
    );
}

#[tokio::test]
async fn invalid_decision_can_be_corrected_without_orphaning_native_permission() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let mut events = provider.subscribe_events().unwrap();
    provider
        .create_session(create("agent-retry"))
        .await
        .unwrap();
    let approval_ref = request_approval(&provider, &mut events, "agent-retry", "retry-turn").await;
    let invalid = provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "agent-retry".into(),
            turn_id: "retry-turn".into(),
            approval_id: approval_ref.clone(),
            decision: "unknown-decision".into(),
            payload: None,
        })
        .await
        .expect_err("unrecognized decisions must fail closed");
    assert!(matches!(invalid, RuntimeError::InvalidState(_)));

    provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "agent-retry".into(),
            turn_id: "retry-turn".into(),
            approval_id: approval_ref,
            decision: "accept".into(),
            payload: None,
        })
        .await
        .expect("corrected native decision must still reach the child");
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "agent-retry".into(),
            turn_id: "retry-turn".into(),
            timeout_ms: Some(5000),
        })
        .await
        .unwrap();
    assert_eq!(result.status, ProviderTurnStatus::Completed);
    assert_eq!(
        result.usage.unwrap()["last_message"],
        "Permission approved."
    );
}

#[tokio::test]
async fn malformed_permission_request_cancels_without_hanging() {
    let harness = FakeAgentHarness::new("malformed_permission");
    let provider = harness.provider();
    let _events = provider.subscribe_events().unwrap();
    provider
        .create_session(create("agent-malformed"))
        .await
        .unwrap();
    provider
        .send_turn(send(
            "agent-malformed",
            "malformed-turn",
            "permission please",
        ))
        .await
        .unwrap();
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "agent-malformed".into(),
            turn_id: "malformed-turn".into(),
            timeout_ms: Some(5000),
        })
        .await
        .unwrap();
    assert_eq!(result.status, ProviderTurnStatus::Failed);
}

#[tokio::test]
async fn negotiated_native_images_and_unsupported_input_are_truthful() {
    let harness = FakeAgentHarness::new("image_support");
    let provider = harness.provider();
    provider.create_session(create("images")).await.unwrap();
    let input = vec![json!({"type":"image","mimeType":"image/png","data":"YWJj"})];
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "images".into(),
            turn_id: "img-turn".into(),
            input,
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "images".into(),
            turn_id: "img-turn".into(),
            timeout_ms: Some(5000),
        })
        .await
        .unwrap();
    assert_eq!(result.status, ProviderTurnStatus::Completed);
    assert!(result.usage.unwrap()["last_message"]
        .as_str()
        .unwrap()
        .contains("image"));

    let unsupported = provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "images".into(),
            turn_id: "local-path".into(),
            input: vec![json!({"type":"local_image","path":"/private/image.png"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect_err("must not flatten native file");
    assert_eq!(
        unsupported.provider_dispatch_code(),
        Some("unsupported_acp_input")
    );
    assert_eq!(
        unsupported.provider_dispatch_outcome(),
        runtime_core::ProviderDispatchOutcome::NotDispatched
    );
    assert_eq!(provider.kind(), ProviderKind::Acp);
}

#[tokio::test]
async fn runtime_manager_durably_admits_provider_permission_before_reply() {
    use std::sync::Arc;

    use runtime_core::{
        ApprovalResponseInput, CreateSessionInput, ProviderRegistry, RuntimeSessionManager,
        RuntimeStore, SendTurnInput,
    };
    use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};

    let harness = FakeAgentHarness::new("normal");
    let provider = Arc::new(harness.provider());
    let mut registry = ProviderRegistry::new();
    registry.register(provider).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("acp-approvals.sqlite3"),
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
                input: vec![json!({"type":"text","text":"permission please"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .unwrap();

    let approval = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let hydrated = store.hydrate_runtime_state().unwrap();
            if let Some(approval) = hydrated.approvals.into_iter().find(|approval| {
                approval.origin == "provider" && approval.turn_id == admitted.turn_id
            }) {
                break approval;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("durable approval admission timed out");
    assert_eq!(approval.status, "pending");
    assert!(approval
        .provider_approval_ref
        .as_deref()
        .unwrap()
        .starts_with("acp:"));

    let invalid = manager
        .respond_approval(
            session.id.as_str(),
            approval.id.as_str(),
            ApprovalResponseInput {
                decision: "accept".into(),
                payload: Some(json!({"optionId":"never-offered"})),
            },
        )
        .await
        .expect_err("local ACP option validation must reject before provider dispatch");
    assert_eq!(
        invalid.provider_dispatch_outcome(),
        runtime_core::ProviderDispatchOutcome::NotDispatched
    );
    let after_rejection = store.hydrate_runtime_state().unwrap();
    assert!(after_rejection
        .approvals
        .iter()
        .any(|row| row.id == approval.id && row.status == "pending"));

    manager
        .respond_approval(
            session.id.as_str(),
            approval.id.as_str(),
            ApprovalResponseInput {
                decision: "accept".into(),
                payload: None,
            },
        )
        .await
        .expect("resolve durable provider approval");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let turns = manager
                .list_session_turns(session.id.as_str())
                .await
                .unwrap();
            if turns
                .iter()
                .any(|turn| turn.id == admitted.turn_id && turn.status == "completed")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("runtime turn completion timed out");
    let persisted = store.hydrate_runtime_state().unwrap();
    let provider_approvals = persisted
        .approvals
        .iter()
        .filter(|row| row.origin == "provider" && row.turn_id == admitted.turn_id)
        .collect::<Vec<_>>();
    assert_eq!(provider_approvals.len(), 1);
    assert_eq!(provider_approvals[0].status, "accept");
}

#[tokio::test]
async fn native_session_identity_cannot_be_claimed_by_another_active_agent() {
    use runtime_core::ProviderResumeSessionRequest;

    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let first = provider
        .create_session(create("first-agent"))
        .await
        .unwrap();
    let stolen = provider
        .resume_session(ProviderResumeSessionRequest {
            runtime_session_id: "second-agent".into(),
            provider_session_ref: first.provider_session_ref.clone(),
            canonical_provider_session_ref: first.canonical_provider_session_ref.clone(),
            cwd: None,
            model: None,
            permission_mode: None,
            setting_sources: Vec::new(),
            system_prompt: None,
            allowed_tools: Vec::new(),
            disallowed_tools: Vec::new(),
            harness_version_slot: None,
            metadata: None,
        })
        .await
        .expect_err("a second agent cannot attach to a live native session");
    assert!(matches!(stolen, RuntimeError::Conflict(_)));
    assert!(!provider
        .inner
        .sessions
        .read()
        .await
        .contains_key("second-agent"));
    provider
        .create_session(create("second-agent"))
        .await
        .expect("failed resume releases capacity");
}

#[tokio::test]
async fn replacement_child_cannot_reuse_stale_session_or_receive_its_close() {
    use runtime_core::{ProviderCloseSessionRequest, ProviderDispatchOutcome};

    let harness = FakeAgentHarness::new("strict_native_session");
    let provider = harness.provider();
    let stale = provider
        .create_session(create("stale-agent"))
        .await
        .unwrap();
    let original = provider.current_connection().await.unwrap();
    let original_connection_id = original.instance_id;
    original.shutdown(true).await;

    let fresh = provider
        .create_session(create("fresh-agent"))
        .await
        .unwrap();
    let fresh_connection_id = provider.current_connection().await.unwrap().instance_id;
    assert_ne!(original_connection_id, fresh_connection_id);
    // A newly launched child can legitimately reuse sess_1, but that must
    // never make another runtime agent's former native session current.
    assert_eq!(stale.provider_session_ref, fresh.provider_session_ref);

    let error = provider
        .send_turn(send("stale-agent", "unsafe-turn", "should never run"))
        .await
        .expect_err("stale ACP native session was not resumed on the new transport");
    assert_eq!(error.provider_dispatch_code(), Some("session_not_found"));
    assert_eq!(
        error.provider_dispatch_outcome(),
        ProviderDispatchOutcome::NotDispatched
    );

    // Closing a stale session is only local cleanup; sending session/close
    // with its reused native ID would close the fresh agent's real session.
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "stale-agent".into(),
            reason: Some("discard orphaned attachment".into()),
        })
        .await
        .unwrap();
    provider
        .send_turn(send("fresh-agent", "safe-turn", "split please"))
        .await
        .expect("new session must remain usable after stale cleanup");
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "fresh-agent".into(),
            turn_id: "safe-turn".into(),
            timeout_ms: Some(5_000),
        })
        .await
        .unwrap();
    assert_eq!(result.status, ProviderTurnStatus::Completed);
    assert_eq!(result.usage.unwrap()["last_message"], "Hello world");
}

#[tokio::test]
async fn idle_session_requires_successful_native_resume_after_child_replacement() {
    use runtime_core::{ProviderDispatchOutcome, ProviderResumeSessionRequest};

    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let original = provider.create_session(create("idle-agent")).await.unwrap();
    provider
        .send_turn(send("idle-agent", "original-turn", "split please"))
        .await
        .unwrap();
    assert_eq!(
        provider
            .wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: "idle-agent".into(),
                turn_id: "original-turn".into(),
                timeout_ms: Some(5_000),
            })
            .await
            .unwrap()
            .status,
        ProviderTurnStatus::Completed
    );

    let old_child = provider.current_connection().await.unwrap();
    let old_connection_id = old_child.instance_id;
    old_child.shutdown(true).await;
    let error = provider
        .send_turn(send("idle-agent", "before-resume", "split please"))
        .await
        .expect_err("a stale attachment cannot dispatch before native resume");
    assert_eq!(error.provider_dispatch_code(), Some("session_not_found"));
    assert_eq!(
        error.provider_dispatch_outcome(),
        ProviderDispatchOutcome::NotDispatched
    );

    let restored = provider
        .resume_session(ProviderResumeSessionRequest {
            runtime_session_id: "idle-agent".into(),
            provider_session_ref: original.provider_session_ref.clone(),
            canonical_provider_session_ref: original.canonical_provider_session_ref.clone(),
            cwd: None,
            model: None,
            permission_mode: None,
            setting_sources: Vec::new(),
            system_prompt: None,
            allowed_tools: Vec::new(),
            disallowed_tools: Vec::new(),
            harness_version_slot: None,
            metadata: None,
        })
        .await
        .expect("successful ACP resume rebinds the same runtime agent");
    assert_eq!(restored.provider_session_ref, original.provider_session_ref);
    assert_ne!(
        provider.current_connection().await.unwrap().instance_id,
        old_connection_id
    );

    provider
        .send_turn(send("idle-agent", "after-resume", "split please"))
        .await
        .unwrap();
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "idle-agent".into(),
            turn_id: "after-resume".into(),
            timeout_ms: Some(5_000),
        })
        .await
        .unwrap();
    assert_eq!(result.status, ProviderTurnStatus::Completed);
    assert_eq!(result.usage.unwrap()["last_message"], "Hello world");
}

#[tokio::test]
async fn ambiguous_native_prompt_outcome_cannot_be_rebound_as_idle() {
    use runtime_core::ProviderResumeSessionRequest;

    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let session = provider
        .create_session(create("unknown-agent"))
        .await
        .unwrap();
    provider
        .send_turn(send("unknown-agent", "unknown-turn", "crash now"))
        .await
        .unwrap();
    let error = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "unknown-agent".into(),
            turn_id: "unknown-turn".into(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect_err("ACP dispatch outcome cannot be inferred from child death");
    assert_eq!(
        error.provider_dispatch_code(),
        Some("acp_prompt_dispatch_unknown")
    );

    let result = provider
        .resume_session(ProviderResumeSessionRequest {
            runtime_session_id: "unknown-agent".into(),
            provider_session_ref: session.provider_session_ref,
            canonical_provider_session_ref: session.canonical_provider_session_ref,
            cwd: None,
            model: None,
            permission_mode: None,
            setting_sources: Vec::new(),
            system_prompt: None,
            allowed_tools: Vec::new(),
            disallowed_tools: Vec::new(),
            harness_version_slot: None,
            metadata: None,
        })
        .await;
    assert!(matches!(result, Err(RuntimeError::InvalidState(_))));
}

#[tokio::test]
async fn runtime_manager_lazily_resumes_idle_agent_after_acp_child_replacement() {
    use std::sync::Arc;

    use runtime_core::{
        CreateSessionInput, ProviderRegistry, RuntimeSessionManager, RuntimeStore, SendTurnInput,
    };
    use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};

    let harness = FakeAgentHarness::new("normal");
    let provider = Arc::new(harness.provider());
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("acp-rebind.sqlite3"),
    }));
    store.initialize().await.unwrap();
    let manager = Arc::new(RuntimeSessionManager::new(store, Arc::new(registry), 256).unwrap());
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

    for (index, restart) in [false, true].into_iter().enumerate() {
        if restart {
            let old_child = provider.current_connection().await.unwrap();
            old_child.shutdown(true).await;
        }
        let admitted = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![json!({"type":"text","text":format!("split please {index}")})],
                    expected_turn_id: None,
                    permission_mode: None,
                    projection_source: None,
                    user_input_snapshot: None,
                    correlation_id: None,
                },
            )
            .await
            .expect("native resume is proven before manager re-dispatch");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let turns = manager
                    .list_session_turns(session.id.as_str())
                    .await
                    .unwrap();
                if turns
                    .iter()
                    .any(|turn| turn.id == admitted.turn_id && turn.status == "completed")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("admitted turn must complete after native resume");
    }
}

#[tokio::test]
async fn private_acp_model_and_harness_policy_cannot_be_silently_overridden() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    let mut request = create("policy-agent");
    request.model = Some("unadvertised-model".into());
    assert!(matches!(
        provider.create_session(request).await,
        Err(RuntimeError::Unsupported(_))
    ));

    let mut request = create("policy-agent");
    request.system_prompt = Some("hidden synthetic developer instructions".into());
    assert!(matches!(
        provider.create_session(request).await,
        Err(RuntimeError::Unsupported(_))
    ));
    let mut request = create("policy-agent");
    request.setting_sources = vec!["worktree".into()];
    assert!(matches!(
        provider.create_session(request).await,
        Err(RuntimeError::Unsupported(_))
    ));
    let mut request = create("policy-agent");
    request.harness_version_slot = Some("runtime-private-harness".into());
    assert!(matches!(
        provider.create_session(request).await,
        Err(RuntimeError::Unsupported(_))
    ));
    assert!(provider.inner.sessions.read().await.is_empty());
}

#[tokio::test]
async fn acp_rejects_unsupported_per_turn_permission_override_without_dispatch() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(create("turn-permission-agent"))
        .await
        .unwrap();
    let mut turn = send("turn-permission-agent", "unsupported-turn", "split please");
    turn.permission_mode = Some("bypass_permissions".into());
    let error = provider
        .send_turn(turn)
        .await
        .expect_err("unsupported private ACP permission mode cannot be ignored");
    assert_eq!(
        error.provider_dispatch_code(),
        Some("unsupported_acp_permission_mode")
    );
    assert_eq!(
        error.provider_dispatch_outcome(),
        runtime_core::ProviderDispatchOutcome::NotDispatched
    );
    provider
        .send_turn(send("turn-permission-agent", "valid-turn", "split please"))
        .await
        .unwrap();
    assert_eq!(
        provider
            .wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: "turn-permission-agent".into(),
                turn_id: "valid-turn".into(),
                timeout_ms: Some(5000),
            })
            .await
            .unwrap()
            .status,
        ProviderTurnStatus::Completed
    );
}
