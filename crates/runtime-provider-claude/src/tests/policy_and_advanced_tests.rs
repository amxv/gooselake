use super::*;
use runtime_core::*;
use serde_json::json;

fn request(id: &str) -> ProviderCreateSessionPolicyRequest {
    ProviderCreateSessionPolicyRequest {
        runtime_session_id: id.into(),
        model: Some("claude-sonnet-5-5".into()),
        cwd: Some("/tmp".into()),
        launch_policy: ProviderSessionLaunchPolicy {
            permission_intent: ProviderPermissionIntent::Explicit {
                mode: "plan".into(),
            },
            setting_sources_intent: ProviderSettingSourcesIntent::Standard,
            system_prompt: Some("caller instructions".into()),
            allowed_tools: vec!["Read".into()],
            disallowed_tools: vec!["Write".into()],
            harness_version_slot: Some(HARNESS_VERSION.into()),
        },
        current_preferences: ProviderSessionPreferences {
            thinking_effort: Some(ProviderThinkingEffort::Max),
        },
        metadata: None,
    }
}

#[tokio::test]
async fn typed_recreation_send_and_skills_preserve_policy_and_current_preferences() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    let req = request("typed");
    let created = provider
        .create_session_with_policy(req.clone())
        .await
        .unwrap();
    provider
        .resume_session_with_policy(ProviderResumeSessionPolicyRequest {
            runtime_session_id: req.runtime_session_id.clone(),
            model: req.model.clone(),
            cwd: req.cwd.clone(),
            provider_session_ref: created.provider_session_ref,
            canonical_provider_session_ref: created.canonical_provider_session_ref,
            launch_policy: req.launch_policy.clone(),
            current_preferences: req.current_preferences.clone(),
            metadata: None,
        })
        .await
        .unwrap();
    let skills = provider
        .list_skills(ProviderSkillDiscoveryRequest {
            cwd: Some("/tmp".into()),
            setting_sources_intent: ProviderSettingSourcesIntent::Isolated,
            force_refresh: true,
        })
        .await
        .unwrap();
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].argument_hint.as_deref(), Some("<path>"));
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "typed".into(),
            turn_id: "logical".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    let requests = harness.read_requests();
    let create = &requests_for_method(&requests, "session.create")[0]["params"];
    let resume = &requests_for_method(&requests, "session.resume")[0]["params"];
    for field in [
        "cwd",
        "model",
        "settingSourcesIntent",
        "systemPrompt",
        "harnessInstructions",
        "harnessVersion",
        "allowedTools",
        "disallowedTools",
        "thinkingEffort",
    ] {
        assert_eq!(create[field], resume[field], "{field}");
    }
    assert_eq!(
        create["harnessInstructions"],
        provider_harness_text(ProviderKind::Claude)
            .unwrap()
            .unwrap()
    );
    assert_eq!(create["systemPrompt"], "caller instructions");
    assert_eq!(create["thinkingEffort"], "max");
    assert!(create.get("permissionIntent").is_none());
    assert!(resume.get("permissionIntent").is_none());
    let send = &requests_for_method(&requests, "session.send")[0]["params"];
    assert_eq!(
        send["permissionIntent"],
        serde_json::to_value(&req.launch_policy.permission_intent).unwrap()
    );
    assert_eq!(send["thinkingEffort"], "max");
    assert!(send.get("permissionMode").is_none());
    assert_eq!(
        requests_for_method(&requests, "session.supported_commands")[0]["params"]
            ["settingSourcesIntent"],
        json!({"kind":"isolated"})
    );
}

#[tokio::test]
async fn advanced_primitives_have_evidence_and_busy_guards() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("advanced"))
        .await
        .unwrap();
    assert!(provider.observe_context_limit("advanced").await.is_err());
    let rebound = provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "advanced".into(),
            cwd: "/tmp".into(),
        })
        .await
        .unwrap();
    assert_eq!(rebound.binding_generation, Some(1));
    assert_eq!(
        rebound.canonical_provider_session_ref.as_deref(),
        Some("canonical-session-1")
    );
    assert_eq!(
        provider
            .compact_session(ProviderCompactSessionRequest {
                runtime_session_id: "advanced".into()
            })
            .await
            .unwrap(),
        ProviderCompactSessionOutcome::Accepted
    );
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "advanced".into(),
            turn_id: "turn".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    assert!(provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "advanced".into(),
            cwd: "/tmp".into()
        })
        .await
        .is_err());
    assert!(provider
        .compact_session(ProviderCompactSessionRequest {
            runtime_session_id: "advanced".into()
        })
        .await
        .is_err());
    provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "advanced".into(),
            turn_id: "turn".into(),
            timeout_ms: Some(500),
        })
        .await
        .unwrap();
    assert_eq!(
        provider.observe_context_limit("advanced").await.unwrap(),
        ProviderContextLimitObservation {
            model_context_window: 1000,
            last_total_tokens: 150,
            remaining_percentage: 85,
        }
    );
    let child = provider
        .hard_fork_edit_rerun(ProviderHardForkEditRerunRequest {
            runtime_session_id: "advanced".into(),
            target_turn_id: "turn".into(),
            edited_input: vec![json!({"type":"text","text":"edited"})],
        })
        .await
        .unwrap();
    assert_eq!(child.provider_session_ref, "child-provider");
    assert_eq!(
        child.canonical_provider_session_ref.as_deref(),
        Some("child-native")
    );
    let requests = harness.read_requests();
    let hard_fork_requests = requests_for_method(&requests, "session.hard_fork");
    assert_eq!(
        hard_fork_requests[0]["params"]["rollbackBoundaryId"],
        "bridge-turn-1"
    );
    assert!(provider.observe_context_limit("advanced").await.is_err());
    assert!(matches!(
        provider
            .hard_fork_edit_rerun(ProviderHardForkEditRerunRequest {
                runtime_session_id: "advanced".into(),
                target_turn_id: "turn".into(),
                edited_input: vec![json!({"type":"text","text":"edited again"})],
            })
            .await,
        Err(RuntimeError::NotFound(_))
    ));
    assert_eq!(
        provider.capabilities().permission_mutation,
        ProviderCapabilitySupport::Supported
    );
    assert_eq!(
        provider.capabilities().hard_fork_edit_rerun,
        ProviderCapabilitySupport::Supported
    );
}

#[tokio::test]
async fn advanced_controls_fail_fast_behind_a_pending_turn_wait() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("busy-control"))
        .await
        .unwrap();
    let session = provider.get_session("busy-control").await.unwrap();
    let _wait_owns_session = session.operation_lock.lock().await;

    let controls = async {
        assert!(provider
            .rebind_workspace(ProviderWorkspaceRebindRequest {
                runtime_session_id: "busy-control".into(),
                cwd: "/tmp".into(),
            })
            .await
            .is_err());
        assert!(provider
            .compact_session(ProviderCompactSessionRequest {
                runtime_session_id: "busy-control".into(),
            })
            .await
            .is_err());
        assert!(provider
            .hard_fork_at_boundary("busy-control", "historical-boundary")
            .await
            .is_err());
    };
    tokio::time::timeout(std::time::Duration::from_millis(250), controls)
        .await
        .expect("controls must fail promptly without waiting for turn completion");
    assert!(requests_for_method(&harness.read_requests(), "session.rebind").is_empty());
    assert!(requests_for_method(&harness.read_requests(), "session.compact").is_empty());
    assert!(requests_for_method(&harness.read_requests(), "session.hard_fork").is_empty());
}

#[tokio::test]
async fn queued_send_cannot_dispatch_from_a_quarantined_session_handle() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("quarantine"))
        .await
        .unwrap();
    let session = provider.get_session("quarantine").await.unwrap();
    let operation = session.operation_lock.lock().await;
    let provider_for_send = provider.clone();
    let pending_send = tokio::spawn(async move {
        provider_for_send
            .send_turn(ProviderSendTurnRequest {
                runtime_session_id: "quarantine".into(),
                turn_id: "queued-turn".into(),
                input: vec![json!({"type":"text","text":"never dispatch"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
    });
    // The turn can have resolved its Arc before the rebind is invalidated;
    // quarantine must still defeat dispatch after the operation lock opens.
    tokio::task::yield_now().await;
    session
        .quarantined
        .store(true, std::sync::atomic::Ordering::SeqCst);
    drop(operation);
    assert!(pending_send.await.unwrap().is_err());
    assert!(requests_for_method(&harness.read_requests(), "session.send").is_empty());

    let detached = provider.remove_session("quarantine").await.unwrap();
    assert!(detached
        .quarantined
        .load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn fail_closed_policy_identity_and_rebind_evidence() {
    let harness = FakeClaudeBridgeHarness::new("resume_mismatch");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    let mut req = request("invalid");
    req.model = Some("unsupported-model".into());
    assert!(provider.create_session_with_policy(req).await.is_err());
    for canonical in [None, Some("native".to_string())] {
        assert!(provider
            .resume_session_with_policy(ProviderResumeSessionPolicyRequest {
                runtime_session_id: "resume".into(),
                provider_session_ref: "bridge".into(),
                canonical_provider_session_ref: canonical,
                model: request("resume").model,
                cwd: Some("/tmp".into()),
                launch_policy: request("resume").launch_policy,
                current_preferences: request("resume").current_preferences,
                metadata: None,
            })
            .await
            .is_err());
    }
    let harness = FakeClaudeBridgeHarness::new("rebind_mismatch");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("rebind"))
        .await
        .unwrap();
    assert!(provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "rebind".into(),
            cwd: "/tmp".into()
        })
        .await
        .is_err());
    assert!(matches!(
        provider.get_session("rebind").await,
        Err(RuntimeError::NotFound(_))
    ));
    assert!(provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "rebind".into(),
            turn_id: "unsafe-followup".into(),
            input: vec![json!({"type":"text","text":"must not dispatch"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .is_err());
    assert!(requests_for_method(&harness.read_requests(), "session.send").is_empty());
    let harness = FakeClaudeBridgeHarness::new("compact_noop");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("noop"))
        .await
        .unwrap();
    assert_eq!(
        provider
            .compact_session(ProviderCompactSessionRequest {
                runtime_session_id: "noop".into()
            })
            .await
            .unwrap(),
        ProviderCompactSessionOutcome::NotPerformed
    );
}

#[tokio::test]
async fn unverified_hard_fork_quarantines_native_identity_before_followup_turn() {
    let harness = FakeClaudeBridgeHarness::new("hard_fork_missing_identity");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("uncertain-fork"))
        .await
        .unwrap();
    assert!(provider
        .hard_fork_at_boundary("uncertain-fork", "recorded-boundary")
        .await
        .is_err());
    assert!(matches!(
        provider.get_session("uncertain-fork").await,
        Err(RuntimeError::NotFound(_))
    ));
    assert!(provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "uncertain-fork".into(),
            turn_id: "unsafe-followup".into(),
            input: vec![json!({"type":"text","text":"must not dispatch"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .is_err());
    assert!(requests_for_method(&harness.read_requests(), "session.send").is_empty());
}

#[test]
fn usage_observation_requires_real_window_and_counts_cache() {
    assert!(
        crate::advanced::context_from_usage(&json!({"inputTokens":1,"outputTokens":2})).is_none()
    );
    assert_eq!(
        crate::advanced::context_from_usage(
            &json!({"inputTokens":100,"outputTokens":100,"contextWindowSize":100})
        )
        .unwrap()
        .remaining_percentage,
        0
    );
}

#[tokio::test]
async fn bridge_terminal_event_observes_context_without_wait() {
    let harness = FakeClaudeBridgeHarness::new("event_usage");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("event"))
        .await
        .unwrap();
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "event".into(),
            turn_id: "logical-event".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(observed) = provider.observe_context_limit("event").await {
                assert_eq!(observed.last_total_tokens, 50);
                assert_eq!(observed.remaining_percentage, 50);
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bridge_compaction_event_preserves_logical_turn_and_updates_context() {
    let harness = FakeClaudeBridgeHarness::new("compaction_event");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    let mut events = provider.subscribe_events().expect("provider event stream");
    provider
        .create_session_with_policy(request("compaction-event"))
        .await
        .unwrap();
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "compaction-event".into(),
            turn_id: "logical-compaction".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();

    let mut observed = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while observed.len() < 2 {
            if let ProviderRuntimeEvent::ContextCompactionObserved {
                runtime_session_id,
                turn_id,
                phase,
                trigger,
                pre_tokens,
                post_tokens,
                context_window_size,
            } = events.recv().await.expect("provider event")
            {
                assert_eq!(runtime_session_id, "compaction-event");
                assert_eq!(turn_id.as_deref(), Some("logical-compaction"));
                observed.push((phase, trigger, pre_tokens, post_tokens, context_window_size));
            }
        }
    })
    .await
    .expect("compaction observations");
    assert_eq!(observed[0].0, "started");
    assert_eq!(observed[1].0, "completed");
    assert_eq!(observed[1].1.as_deref(), Some("auto"));
    assert_eq!(observed[1].2, Some(80));
    assert_eq!(observed[1].3, Some(25));
    assert_eq!(observed[1].4, Some(100));

    let context = provider
        .observe_context_limit("compaction-event")
        .await
        .expect("context observation");
    assert_eq!(context.model_context_window, 100);
    assert_eq!(context.last_total_tokens, 25);
    assert_eq!(context.remaining_percentage, 75);
}

#[tokio::test]
async fn native_model_discovery_keeps_builtin_catalog_semantics_and_current_effort_metadata() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    let result = provider
        .discover_models(ProviderModelDiscoveryRequest {
            startup_mode: ProviderModelDiscoveryStartupMode::StartRuntime,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.mode, ProviderDiscoveryMode::Catalog);
    assert_eq!(
        result.models[0].capabilities.supported_thinking_efforts,
        vec![ProviderThinkingEffort::High, ProviderThinkingEffort::Max]
    );
}

#[tokio::test]
async fn permission_mutation_is_revisioned_busy_safe_and_authoritative_for_future_turns() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("mutate-permission"))
        .await
        .unwrap();

    let updated = provider
        .mutate_session_permission(ProviderPermissionMutationRequest {
            runtime_session_id: "mutate-permission".into(),
            expected_revision: Some(0),
            permission_intent: ProviderPermissionIntent::Explicit {
                mode: "acceptEdits".into(),
            },
        })
        .await
        .unwrap();
    assert_eq!(updated.revision, 1);
    assert_eq!(
        updated.permission_intent,
        ProviderPermissionIntent::Explicit {
            mode: "acceptEdits".into()
        }
    );
    assert!(provider
        .mutate_session_permission(ProviderPermissionMutationRequest {
            runtime_session_id: "mutate-permission".into(),
            expected_revision: Some(0),
            permission_intent: ProviderPermissionIntent::InheritProviderConfiguration,
        })
        .await
        .is_err());

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "mutate-permission".into(),
            turn_id: "after-mutation".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    assert!(provider
        .mutate_session_permission(ProviderPermissionMutationRequest {
            runtime_session_id: "mutate-permission".into(),
            expected_revision: Some(1),
            permission_intent: ProviderPermissionIntent::InheritProviderConfiguration,
        })
        .await
        .is_err());

    let requests = harness.read_requests();
    let send = &requests_for_method(&requests, "session.send")[0]["params"];
    assert_eq!(
        send["permissionIntent"],
        json!({"kind":"explicit","mode":"acceptEdits"})
    );
}

#[tokio::test]
async fn thinking_preference_mutation_is_revisioned_busy_safe_and_authoritative_for_future_turns() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("mutate-thinking"))
        .await
        .unwrap();

    let updated = provider
        .mutate_session_preferences(runtime_core::ProviderSessionPreferencesMutationRequest {
            runtime_session_id: "mutate-thinking".into(),
            expected_revision: Some(0),
            current_preferences: ProviderSessionPreferences {
                thinking_effort: Some(ProviderThinkingEffort::High),
            },
        })
        .await
        .unwrap();
    assert_eq!(updated.revision, 1);
    assert_eq!(
        updated.current_preferences.thinking_effort,
        Some(ProviderThinkingEffort::High)
    );
    assert!(provider
        .mutate_session_preferences(runtime_core::ProviderSessionPreferencesMutationRequest {
            runtime_session_id: "mutate-thinking".into(),
            expected_revision: Some(0),
            current_preferences: ProviderSessionPreferences {
                thinking_effort: Some(ProviderThinkingEffort::Max),
            },
        })
        .await
        .is_err());

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "mutate-thinking".into(),
            turn_id: "after-thinking-mutation".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    assert!(provider
        .mutate_session_preferences(runtime_core::ProviderSessionPreferencesMutationRequest {
            runtime_session_id: "mutate-thinking".into(),
            expected_revision: Some(1),
            current_preferences: ProviderSessionPreferences {
                thinking_effort: Some(ProviderThinkingEffort::Max),
            },
        })
        .await
        .is_err());

    let requests = harness.read_requests();
    let send = &requests_for_method(&requests, "session.send")[0]["params"];
    assert_eq!(send["thinkingEffort"], "high");
}

#[tokio::test]
async fn provider_observed_permission_event_is_preserved_with_runtime_turn_identity() {
    let harness = FakeClaudeBridgeHarness::new("permission_event");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session_with_policy(request("permission-observed"))
        .await
        .unwrap();
    let mut events = provider.subscribe_events().unwrap();
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "permission-observed".into(),
            turn_id: "runtime-turn".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ProviderRuntimeEvent::PermissionObserved {
            runtime_session_id,
            turn_id,
            permission_mode,
            resolved_turn_selection,
        } => {
            assert_eq!(runtime_session_id, "permission-observed");
            assert_eq!(turn_id, "runtime-turn");
            assert_eq!(permission_mode, "plan");
            assert_eq!(resolved_turn_selection.as_deref(), Some("plan"));
        }
        other => panic!("unexpected provider event: {other:?}"),
    }
}
