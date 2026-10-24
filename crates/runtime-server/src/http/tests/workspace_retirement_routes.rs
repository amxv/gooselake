use super::workspace_retirement_support::*;
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn retirement_observes_delivery_claim_before_a_racing_native_interrupt() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("interrupt-race-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let recipient = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let sender = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, turn) = fixture.call("POST",
        &format!("/v2/agents/{}/turns", recipient.agent_id),
        Some(serde_json::json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})),
        None).await;
    assert_eq!(status, StatusCode::OK, "{turn}");
    let gate = provider.pause_next_interrupt().await;
    let router = fixture.router.clone();
    let token = fixture.token.clone();
    let input = serde_json::json!({
        "mode":"direct", "sender_agent_id":sender.agent_id,
        "recipient_agent_id":recipient.agent_id, "policy":"immediate_interrupt",
        "input":[{"type":"text","text":"retirement_interrupt_race_message"}],
    });
    let send = tokio::spawn(async move {
        super::workspace_worktree_routes::call(
            &router,
            &token,
            "POST",
            "/v2/messages",
            Some(input),
            Some("racing-interrupt-message"),
        )
        .await
    });
    timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("message did not reach the native interrupt boundary");
    let deliveries = fixture
        .store
        .list_agent_delivery_records(&runtime_core::AgentDeliveryListRequest {
            message_id: None,
            recipient_agent_id: Some(recipient.agent_id.clone()),
        })
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(
        deliveries[0].status, "injecting",
        "the native interrupt must have durable delivery ownership before dispatch"
    );

    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    // Exercise the durable fence independently of the manager's policy lock,
    // while the already-claimed native interrupt is still in flight.
    let command = runtime_core::prepare_workspace_retirement(
        &workspace.workspace_id,
        runtime_core::WorkspaceRetirementRequest {
            expected_revision: revision,
        },
        runtime_core::OperationActor::operator("runtime_operator"),
        Some("racing-interrupt-retirement".into()),
    )
    .unwrap();
    fixture.store.admit_workspace_retirement(&command).unwrap();
    fixture
        .store
        .settle_passive_workspace_retirement(&command.operation_id, command.requested_at + 1)
        .unwrap();
    let frozen = fixture
        .store
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        frozen.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    let effect = frozen
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_delivery_settle")
        .unwrap();
    assert_eq!(effect.phase, "manual_review");
    assert_eq!(provider.interrupts.load(Ordering::SeqCst), 1);
    assert_eq!(provider.closes.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .store
            .get_agent_delivery(&deliveries[0].id)
            .unwrap()
            .unwrap()
            .status,
        "injecting"
    );

    gate.release();
    let (status, message) = timeout(Duration::from_secs(5), send)
        .await
        .expect("racing message request remained blocked")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{message}");
    assert_ne!(message["deliveries"][0]["status"], "injected");
    assert_eq!(
        provider
            .turns_containing("retirement_interrupt_race_message")
            .await,
        0
    );
    let (status, blocked_interrupt) = fixture
        .call(
            "POST",
            &format!(
                "/v1/sessions/{}/turns/{}/interrupt",
                recipient.agent_id,
                turn["turn_id"].as_str().unwrap()
            ),
            None,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked_interrupt}");
    assert_eq!(provider.interrupts.load(Ordering::SeqCst), 1);
    let replay = fixture
        .advance(
            &workspace.workspace_id,
            revision,
            "racing-interrupt-retirement",
        )
        .await;
    assert_eq!(replay.operation.operation_id, frozen.operation.operation_id);
    assert_eq!(
        replay.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(provider.interrupts.load(Ordering::SeqCst), 1);
    assert_eq!(provider.closes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn populated_retirement_settles_mixed_native_turns_processes_and_deliveries_before_closure() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    fs::create_dir_all(&root).unwrap();
    let codex = RetirementProvider::new(ProviderKind::Codex);
    let claude = RetirementProvider::new(ProviderKind::Claude);
    let acp = RetirementProvider::new(ProviderKind::Acp);
    let fixture =
        retirement_fixture(temp.path(), &[codex.clone(), claude.clone(), acp.clone()]).await;
    let workspace = fixture.register(&root).await;
    let active = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let sender = fixture
        .agent(&workspace.workspace_id, ProviderKind::Claude)
        .await;
    let idle = fixture
        .agent(&workspace.workspace_id, ProviderKind::Acp)
        .await;
    let current = fixture.workspace(&workspace.workspace_id).await;
    let (status, lead) = fixture.call("POST", &format!("/v2/workspaces/{}/lead", workspace.workspace_id),
        Some(serde_json::json!({"lead_agent_id":active.agent_id,"expected_revision":current.revision})), Some("retirement-lead")).await;
    assert_eq!(status, StatusCode::OK, "{lead}");
    let (status, turn) = fixture.call("POST", &format!("/v2/agents/{}/turns", active.agent_id),
        Some(serde_json::json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})), None).await;
    assert_eq!(status, StatusCode::OK, "{turn}");
    let turn_id = turn["turn_id"].as_str().unwrap().to_string();
    let (status, message) = fixture.call("POST", "/v2/messages", Some(serde_json::json!({
        "mode":"direct","sender_agent_id":sender.agent_id,"recipient_agent_id":active.agent_id,
        "input":[{"type":"text","text":"must not be delivered after retirement freezes ingress"}],
    })), Some("retirement-pending-message")).await;
    assert_eq!(status, StatusCode::OK, "{message}");
    assert!(
        matches!(
            message["deliveries"][0]["status"].as_str(),
            Some("pending" | "deferred")
        ),
        "{message}"
    );
    let message_id = message["message"]["id"].as_str().unwrap();
    let stale_delivery: runtime_core::AgentDeliveryRecord =
        serde_json::from_value(message["deliveries"][0].clone()).unwrap();
    let (status, process) = fixture
        .call(
            "POST",
            "/v1/processes",
            Some(serde_json::json!({
                "command":"/bin/sleep 20","session_id":active.agent_id,"cwd":root,
            })),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{process}");
    let process_id = process["process"]["process_id"]
        .as_str()
        .unwrap()
        .to_string();
    timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            let record = fixture
                .store
                .get_managed_process(&process_id)
                .unwrap()
                .unwrap();
            if record.status == "running" {
                break;
            }
            assert!(
                matches!(record.status.as_str(), "queued" | "launch_reserved"),
                "{record:?}"
            );
            tick.tick().await;
        }
    })
    .await
    .expect("native process did not start");

    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let completed = fixture
        .finish_retirement(&workspace.workspace_id, revision, "mixed-retirement")
        .await;
    assert!(completed.claims.is_empty());
    assert!(completed
        .effects
        .iter()
        .all(|effect| effect.phase == "finalized"));
    assert_eq!(
        completed
            .effects
            .iter()
            .filter(|effect| effect.effect_kind == "workspace_provider_close")
            .count(),
        3
    );
    assert_eq!(codex.interrupts.load(Ordering::SeqCst), 1);
    for provider in [&codex, &claude, &acp] {
        assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    }
    for agent in [&active, &sender, &idle] {
        let archived = fixture
            .runtime
            .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
            .unwrap();
        assert_eq!(
            archived.lifecycle_state,
            runtime_core::WorkspaceAgentLifecycleState::Archived
        );
        assert_eq!(
            fixture
                .runtime
                .get_session(&agent.agent_id)
                .await
                .unwrap()
                .status,
            "closed"
        );
    }
    let state = fixture.store.hydrate_runtime_state().unwrap();
    assert_eq!(
        state
            .turns
            .iter()
            .find(|row| row.id == turn_id)
            .unwrap()
            .status,
        "interrupted"
    );
    let native = fixture
        .store
        .get_managed_process(&process_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        native.completion_state, "pending",
        "terminal result must survive retirement"
    );
    assert_eq!(
        native.completion_last_error.as_deref(),
        Some("workspace_retirement_deferred_completion")
    );
    assert_eq!(codex.turns_containing(&process_id).await, 0);
    assert!(
        !matches!(
            native.status.as_str(),
            "queued" | "launch_reserved" | "running"
        ),
        "{native:?}"
    );
    let (status, deliveries) = fixture
        .call(
            "GET",
            &format!("/v2/messages/{message_id}/deliveries"),
            None,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{deliveries}");
    assert_eq!(deliveries[0]["status"], "cancelled");
    assert!(
        fixture
            .store
            .upsert_agent_delivery(&stale_delivery)
            .is_err(),
        "stale worker state must never overwrite a retirement cancellation"
    );
    let (retry_status, _) = fixture
        .call(
            "POST",
            &format!("/v2/deliveries/{}/retry", stale_delivery.id),
            Some(serde_json::json!({})),
            None,
        )
        .await;
    assert!(
        !retry_status.is_success(),
        "cancelled retirement delivery must not be retried"
    );
    assert_eq!(
        fixture
            .store
            .get_agent_delivery(&stale_delivery.id)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    assert_eq!(
        fixture
            .workspace(&workspace.workspace_id)
            .await
            .lead_agent_id,
        None
    );
    let replay = fixture
        .advance(&workspace.workspace_id, revision, "mixed-retirement")
        .await;
    assert_eq!(
        replay.operation.exact_terminal_result,
        completed.operation.exact_terminal_result
    );
    assert_eq!(codex.closes.load(Ordering::SeqCst), 1);

    fixture.register(&root).await;
    assert_eq!(
        codex.turns_containing(&process_id).await,
        0,
        "reactivation alone cannot dispatch to an archived owner"
    );
    let (status, restored) = fixture
        .call(
            "POST",
            &format!(
                "/v2/workspaces/{}/agents/{}/restore",
                workspace.workspace_id, active.agent_id
            ),
            Some(serde_json::json!({})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            let record = fixture
                .store
                .get_managed_process(&process_id)
                .unwrap()
                .unwrap();
            if record.completion_state == "delivered" {
                assert!(record.completion_turn_id.is_some());
                break;
            }
            assert!(
                matches!(record.completion_state.as_str(), "pending" | "injecting"),
                "{record:?}"
            );
            tick.tick().await;
        }
    })
    .await
    .expect("preserved process result was not delivered after explicit owner restore");
    assert_eq!(codex.turns_containing(&process_id).await, 1);
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    fixture
        .finish_retirement(
            &workspace.workspace_id,
            revision,
            "after-completion-restoration",
        )
        .await;
    assert_eq!(
        codex.turns_containing(&process_id).await,
        1,
        "repeated retirement cannot redeliver completion"
    );
}

#[tokio::test]
async fn retirement_cancels_predispatch_approval_without_a_native_interrupt() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("approval-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, pending) = fixture
        .call(
            "POST",
            &format!("/v2/agents/{}/turns", agent.agent_id),
            Some(
                serde_json::json!({"input":[{"type":"text","text":"never dispatch this turn"}],
            "permission_mode":"require_approval"}),
            ),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{pending}");
    assert_eq!(pending["status"], "waiting_for_approval");
    let turn_id = pending["turn_id"].as_str().unwrap();
    let before = fixture.store.hydrate_runtime_state().unwrap();
    let approval = before
        .approvals
        .iter()
        .find(|row| row.turn_id == turn_id)
        .unwrap();
    assert_eq!(approval.status, "pending");
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    fixture
        .finish_retirement(
            &workspace.workspace_id,
            revision,
            "pending-approval-retirement",
        )
        .await;
    assert_eq!(provider.interrupts.load(Ordering::SeqCst), 0);
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    let after = fixture.store.hydrate_runtime_state().unwrap();
    assert_eq!(
        after
            .approvals
            .iter()
            .find(|row| row.id == approval.id)
            .unwrap()
            .status,
        "cancelled"
    );
    let turn = after.turns.iter().find(|row| row.id == turn_id).unwrap();
    assert_eq!(turn.status, "cancelled");
    assert!(turn.completed_at.is_some());
    assert!(fixture
        .runtime
        .get_session(&agent.agent_id)
        .await
        .unwrap()
        .active_turn_id
        .is_none());
}

#[tokio::test]
async fn retirement_cancels_native_approval_receipts_and_prevents_post_restore_replay() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("native-approval-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, turn) = fixture.call("POST",
        &format!("/v2/agents/{}/turns", agent.agent_id),
        Some(serde_json::json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})),
        None).await;
    assert_eq!(status, StatusCode::OK, "{turn}");
    let turn_id = turn["turn_id"].as_str().unwrap();
    let pending = fixture
        .runtime
        .record_provider_approval(
            &agent.agent_id,
            turn_id,
            "native-pending-approval",
            Some("fixture-tool".into()),
            serde_json::json!({"reason":"fixture native approval"}),
        )
        .await
        .unwrap();
    let mut uncertain = fixture
        .runtime
        .record_provider_approval(
            &agent.agent_id,
            turn_id,
            "native-decision-approval",
            None,
            serde_json::json!({"reason":"fixture uncertain native decision"}),
        )
        .await
        .unwrap();
    let prior_decision = serde_json::json!({"decision":"accept","payload":{"fixture":"retained"}});
    uncertain.status = "decision_pending".into();
    uncertain.response = Some(prior_decision.clone());
    fixture.store.upsert_approval(&uncertain).unwrap();
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let retired = fixture
        .finish_retirement(
            &workspace.workspace_id,
            revision,
            "native-approval-retirement",
        )
        .await;
    let interrupt_proof = retired
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_turn_interrupt")
        .and_then(|effect| effect.evidence.as_ref())
        .unwrap();
    assert_eq!(interrupt_proof["outcome"], "terminal_turn_observed");
    assert_eq!(
        interrupt_proof["prior_evidence"]["outcome"],
        "native_interrupt_accepted_not_terminal"
    );
    assert_eq!(
        interrupt_proof["prior_evidence"]["prior_evidence"]["outcome"],
        "native_interrupt_intended"
    );
    let durable = fixture.store.hydrate_runtime_state().unwrap();
    for (id, prior_status) in [
        (&pending.id, "pending"),
        (&uncertain.id, "decision_pending"),
    ] {
        let approval = durable.approvals.iter().find(|row| &row.id == id).unwrap();
        assert_eq!(approval.status, "cancelled");
        assert!(approval.resolved_at.is_some());
        let response = approval.response.as_ref().unwrap();
        assert_eq!(response["code"], "workspace_retirement");
        assert_eq!(response["reason"], "terminal_turn_verified");
        assert_eq!(response["turn_status"], "interrupted");
        assert_eq!(response["prior_status"], prior_status);
        if id == &uncertain.id {
            assert_eq!(response["prior_response"], prior_decision);
        }
    }
    fixture.register(&root).await;
    let (status, restored) = fixture
        .call(
            "POST",
            &format!(
                "/v2/workspaces/{}/agents/{}/restore",
                workspace.workspace_id, agent.agent_id
            ),
            Some(serde_json::json!({})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    for approval_id in [&pending.id, &uncertain.id] {
        let (status, rejected) = fixture
            .call(
                "POST",
                &format!("/v1/sessions/{}/approvals/{approval_id}", agent.agent_id),
                Some(serde_json::json!({"decision":"accept"})),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    }
    assert_eq!(provider.approval_responses.load(Ordering::SeqCst), 0);

    // A terminal native turn is also sufficient to reject an otherwise still
    // pending approval before the next retirement has cancelled its receipt.
    let (status, current_turn) = fixture.call("POST",
        &format!("/v2/agents/{}/turns", agent.agent_id),
        Some(serde_json::json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})),
        None).await;
    assert_eq!(status, StatusCode::OK, "{current_turn}");
    let current_turn_id = current_turn["turn_id"].as_str().unwrap();
    let current_approval = fixture
        .runtime
        .record_provider_approval(
            &agent.agent_id,
            current_turn_id,
            "native-terminal-approval",
            None,
            serde_json::json!({"reason":"terminal turn must not accept this"}),
        )
        .await
        .unwrap();
    fixture
        .runtime
        .interrupt_turn(&agent.agent_id, current_turn_id)
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        while fixture
            .runtime
            .get_session(&agent.agent_id)
            .await
            .unwrap()
            .active_turn_id
            .is_some()
        {
            tick.tick().await;
        }
    })
    .await
    .expect("native turn did not become terminal");
    let (status, rejected) = fixture
        .call(
            "POST",
            &format!(
                "/v1/sessions/{}/approvals/{}",
                agent.agent_id, current_approval.id
            ),
            Some(serde_json::json!({"decision":"accept"})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    assert_eq!(provider.approval_responses.load(Ordering::SeqCst), 0);
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    fixture
        .finish_retirement(
            &workspace.workspace_id,
            revision,
            "after-native-approval-restore",
        )
        .await;
}

#[tokio::test]
async fn retirement_native_close_failure_never_retries_or_reactivates_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("unknown-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    provider.fail_close.store(true, Ordering::SeqCst);
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let first = fixture
        .advance(&workspace.workspace_id, revision, "unknown-native-close")
        .await;
    assert_eq!(
        first.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    let replay = fixture
        .advance(&workspace.workspace_id, revision, "unknown-native-close")
        .await;
    assert_eq!(
        replay.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .workspace(&workspace.workspace_id)
            .await
            .lifecycle_state,
        runtime_core::WorkspaceLifecycleState::Active
    );
    let (status, blocked) = fixture
        .call(
            "POST",
            "/v2/workspaces",
            Some(serde_json::json!({
                "canonical_root":root,"display_name":"must not bypass retirement",
            })),
            Some("blocked-reactivation"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked}");
    let restarted_provider = RetirementProvider::new(ProviderKind::Codex);
    let restarted = retirement_fixture(temp.path(), &[restarted_provider.clone()]).await;
    let recovered = restarted.runtime.recover_startup().await.unwrap();
    assert!(
        recovered
            .notes
            .iter()
            .any(|note| note.contains(&agent.agent_id) && note.contains("retirement")),
        "{recovered:?}"
    );
    assert_eq!(restarted_provider.resumes.load(Ordering::SeqCst), 0);
    let replay = restarted
        .advance(&workspace.workspace_id, revision, "unknown-native-close")
        .await;
    assert_eq!(replay.operation.operation_id, first.operation.operation_id);
    assert_eq!(
        replay.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(restarted_provider.closes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retirement_reactivation_reuses_verified_archives_but_restored_members_need_new_close() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("reactivation-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let (status, lead) = fixture
        .call(
            "POST",
            &format!("/v2/workspaces/{}/lead", workspace.workspace_id),
            Some(serde_json::json!({"lead_agent_id":agent.agent_id,"expected_revision":revision})),
            Some("lead-before-retirement"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{lead}");
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let first = fixture
        .finish_retirement(&workspace.workspace_id, revision, "first-retirement")
        .await;
    let (status, rejected_restore) = fixture
        .call(
            "POST",
            &format!(
                "/v2/workspaces/{}/agents/{}/restore",
                workspace.workspace_id, agent.agent_id
            ),
            Some(serde_json::json!({})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected_restore}");
    assert_eq!(
        provider.resumes.load(Ordering::SeqCst),
        0,
        "a retired workspace must reject before native resume"
    );
    let reactivated = fixture.register(&root).await;
    assert_eq!(reactivated.workspace_id, workspace.workspace_id);
    assert_eq!(reactivated.lead_agent_id, None);
    fixture
        .finish_retirement(
            &workspace.workspace_id,
            reactivated.revision,
            "archived-only-retirement",
        )
        .await;
    assert_eq!(
        provider.closes.load(Ordering::SeqCst),
        1,
        "unchanged closed attachment must reuse verified evidence"
    );
    fixture.register(&root).await;
    let (status, restored) = fixture
        .call(
            "POST",
            &format!(
                "/v2/workspaces/{}/agents/{}/restore",
                workspace.workspace_id, agent.agent_id
            ),
            Some(serde_json::json!({})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    assert_eq!(restored["agent_id"], agent.agent_id);
    assert_eq!(
        fixture
            .workspace(&workspace.workspace_id)
            .await
            .lead_agent_id,
        None
    );
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 1);
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    fixture
        .finish_retirement(&workspace.workspace_id, revision, "restored-retirement")
        .await;
    assert_eq!(
        provider.closes.load(Ordering::SeqCst),
        2,
        "restore invalidates earlier close evidence"
    );
    let restarted_provider = RetirementProvider::new(ProviderKind::Codex);
    let restarted = retirement_fixture(temp.path(), &[restarted_provider.clone()]).await;
    restarted.runtime.recover_startup().await.unwrap();
    assert_eq!(restarted_provider.resumes.load(Ordering::SeqCst), 0);
    let replay = restarted
        .advance(
            &workspace.workspace_id,
            first.operation.normalized_request["expected_revision"]
                .as_u64()
                .unwrap(),
            "first-retirement",
        )
        .await;
    assert_eq!(
        replay.operation.exact_terminal_result,
        first.operation.exact_terminal_result
    );
    assert_eq!(restarted_provider.closes.load(Ordering::SeqCst), 0);
}
