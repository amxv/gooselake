use super::*;
use crate::tests::provider_native_approval_events::submit;
use crate::tests::workspace_agent::{registered_workspace, workspace_agent_fixture};
use crate::tests::workspace_retirement::retirement_command;

fn workspace(
    repository: &SqliteRuntimeRepository,
    temp: &tempfile::TempDir,
    name: &str,
) -> WorkspaceRecord {
    let root = temp.path().join(name);
    fs::create_dir(&root).unwrap();
    registered_workspace(repository, &root)
}
fn agent(
    repository: &SqliteRuntimeRepository,
    workspace: &WorkspaceRecord,
    id: &str,
) -> (SessionRecord, WorkspaceAgentRecord) {
    let (session, agent) = workspace_agent_fixture(workspace, id, id);
    repository.create_workspace_agent(&session, &agent).unwrap();
    let saved = repository
        .get_workspace_agent(&workspace.workspace_id, id)
        .unwrap()
        .unwrap();
    (session, saved)
}
fn admission(
    session: &SessionRecord,
    turn: &TurnRecord,
    projection: TurnInputProjectionSource,
    correlation: Option<String>,
    state: TurnDispatchState,
) -> TurnAdmissionRecord {
    TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: session.id.clone(),
        provider: session.provider.clone(),
        projection_source: projection,
        user_input_snapshot: PersistedUserInputSnapshot::from_input(turn.input.as_array().unwrap()),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState {
            expected_turn_id: None,
            correlation_id: correlation,
        },
        dispatch_state: state,
        provider_native_turn_id: turn.provider_turn_ref.clone(),
        dispatch_error: None,
        admitted_at: 110,
        updated_at: 110,
    }
}
fn dispatching(
    repository: &SqliteRuntimeRepository,
    session: &SessionRecord,
    id: &str,
    projection: TurnInputProjectionSource,
    correlation: Option<String>,
) -> (TurnRecord, SessionRecord, TurnAdmissionRecord) {
    let turn = TurnRecord {
        id: id.into(),
        session_id: session.id.clone(),
        provider_turn_ref: None,
        status: "admitted".into(),
        input: json!([{"type":"text","text":"inert snapshot input"}]),
        source: Some(projection.as_str().into()),
        started_at: Some(110),
        completed_at: None,
        usage: None,
        error: None,
    };
    let mut attached = session.clone();
    attached.status = "turn_admitted".into();
    attached.active_turn_id = Some(id.into());
    attached.updated_at = 110;
    let mut admitted = admission(
        session,
        &turn,
        projection,
        correlation,
        TurnDispatchState::Pending,
    );
    repository
        .admit_turn(&admitted, &turn, &attached, None)
        .unwrap();
    admitted.dispatch_state = TurnDispatchState::Dispatching;
    admitted.updated_at = 111;
    repository.upsert_turn_admission(&admitted).unwrap();
    (turn, attached, admitted)
}
pub(super) struct ApprovalSeed {
    pub pending: ApprovalRecord,
    pub turn: TurnRecord,
    pub session: SessionRecord,
}
pub(super) fn approval_decision(
    pending: &ApprovalRecord,
    attempt: &str,
) -> (ApprovalRecord, NewRuntimeEvent) {
    let mut reserved = pending.clone();
    reserved.status = "decision_pending".into();
    reserved.response = Some(
        json!({"decision":"accept","payload":{"snapshot_fixture":true},"decision_attempt_id":attempt}),
    );
    let event = NewRuntimeEvent {
        event_id: format!("evt_approval_{}_decision_reserved_{attempt}", pending.id),
        scope: RuntimeEventScope::Session,
        scope_id: pending.session_id.clone(),
        session_id: Some(pending.session_id.clone()),
        team_id: None,
        turn_id: Some(pending.turn_id.clone()),
        kind: "approval.decision_reserved".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({"approval_id":pending.id,"decision_attempt_id":attempt,"decision":"accept","origin":"provider"}),
        provider: None,
        provider_seq: None,
        created_at: 300,
    };
    (reserved, event)
}
fn seed_approval(repository: &SqliteRuntimeRepository, session: &SessionRecord) -> ApprovalSeed {
    let (mut turn, mut attached, mut admitted) = dispatching(
        repository,
        session,
        "n07_approval_turn",
        TurnInputProjectionSource::UserVisible,
        None,
    );
    turn.status = "in_progress".into();
    turn.provider_turn_ref = Some("fixture-native-approval-turn".into());
    attached.status = "turn_running".into();
    admitted.dispatch_state = TurnDispatchState::Dispatched;
    admitted.provider_native_turn_id = turn.provider_turn_ref.clone();
    admitted.updated_at = 112;
    repository.upsert_turn(&turn).unwrap();
    repository.upsert_session(&attached).unwrap();
    repository.upsert_turn_admission(&admitted).unwrap();
    let pending = submit(repository, &attached, &turn, "n07_approval").unwrap();
    let seed = ApprovalSeed {
        pending: pending.approval,
        turn: pending.turn,
        session: pending.session,
    };
    let (reserved, event) = approval_decision(&seed.pending, "n07_attempt_1");
    assert_eq!(
        repository
            .claim_provider_approval_decision(
                &seed.pending,
                &reserved,
                &seed.turn,
                &seed.session,
                &event
            )
            .unwrap()
            .approval
            .status,
        "decision_pending"
    );
    seed
}
fn seed_process(
    repository: &SqliteRuntimeRepository,
    workspace: &WorkspaceRecord,
    session: &SessionRecord,
    temp: &tempfile::TempDir,
) -> TurnAdmissionRecord {
    repository
        .admit_managed_process(&ManagedProcessAdmission {
            process_id: "n07_process".into(),
            owner_session_id: Some(session.id.clone()),
            workspace_id: Some(workspace.workspace_id.clone()),
            tool_call_id: Some("n07_process_call".into()),
            command: json!({"command":"fixture-never-executed"}),
            cwd: session.cwd.clone(),
            timeout_ms: Some(1000),
            stdout_path: temp
                .path()
                .join("never-created.stdout")
                .display()
                .to_string(),
            stderr_path: temp
                .path()
                .join("never-created.stderr")
                .display()
                .to_string(),
            capture_limit_bytes: 1024,
            admitted_at: 100,
        })
        .unwrap();
    // Synthetic known-terminal carrier; no process is launched.
    repository
        .terminalize_managed_process(
            "n07_process",
            &ManagedProcessTerminalUpdate {
                status: "completed".into(),
                terminal_reason: Some("synthetic_fixture_terminal".into()),
                exit_code: Some(0),
                signal: None,
                ended_at: 105,
                execution_duration_ms: Some(0),
                stdout_captured_bytes: 0,
                stderr_captured_bytes: 0,
                stdout_truncated: false,
                stderr_truncated: false,
                completion_required: true,
            },
        )
        .unwrap()
        .unwrap();
    repository
        .update_managed_process_completion(
            "n07_process",
            &ProcessCompletionUpdate {
                state: "injecting".into(),
                turn_id: None,
                attempt_count: 1,
                last_error: None,
                updated_at: 106,
            },
        )
        .unwrap()
        .unwrap();
    dispatching(
        repository,
        session,
        "n07_process_completion_turn",
        TurnInputProjectionSource::AutomationContext,
        Some("process_completion:n07_process".into()),
    )
    .2
}
fn seed_hook(
    repository: &SqliteRuntimeRepository,
    workspace: &WorkspaceRecord,
) -> TeamHookEffectRecord {
    let hook = TeamHookResolvedEntry {
        event_name: TeamHookEventName::PostTeammateAdd,
        name: "n07_inert_hook".into(),
        command: "fixture-never-executed".into(),
        timeout_ms: 500,
        scope: TeamHookScope::Project,
    };
    repository
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: workspace.workspace_id.clone(),
            expected_revision: 0,
            hooks: vec![hook.clone()],
            approved_at_ms: 100,
        })
        .unwrap();
    repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "n07_hook_parent".into(),
            scope: RuntimeEventScope::Workspace,
            scope_id: workspace.workspace_id.clone(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "workspace.member_added".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({"workspace_id":workspace.workspace_id}),
            provider: None,
            provider_seq: None,
            created_at: 101,
        })
        .unwrap();
    repository
        .admit_team_hook_effect(&TeamHookEffectAdmission {
            workspace_id: workspace.workspace_id.clone(),
            parent_event_id: "n07_hook_parent".into(),
            hook: hook.clone(),
            input: json!({"event_id":"n07_hook_parent","hook_event":"post_teammate_add"}),
            created_at_ms: 102,
        })
        .unwrap();
    repository
        .claim_team_hook_effect(
            &workspace.workspace_id,
            "n07_hook_parent",
            hook.event_name,
            &hook.name,
            103,
        )
        .unwrap()
}
fn seed_edit(
    repository: &SqliteRuntimeRepository,
    session: &SessionRecord,
    agent: &WorkspaceAgentRecord,
) -> SessionEditRerunOperation {
    let turn = TurnRecord {
        id: "n07_edit_original".into(),
        session_id: session.id.clone(),
        provider_turn_ref: Some("fixture-original-native-turn".into()),
        status: "completed".into(),
        input: json!([{"type":"text","text":"original immutable input"}]),
        source: Some("user".into()),
        started_at: Some(101),
        completed_at: Some(102),
        usage: None,
        error: None,
    };
    repository.upsert_turn(&turn).unwrap();
    repository
        .upsert_turn_admission(&admission(
            session,
            &turn,
            TurnInputProjectionSource::UserVisible,
            None,
            TurnDispatchState::Dispatched,
        ))
        .unwrap();
    let operation = SessionEditRerunOperation {
        operation_id: "n07_edit".into(),
        agent_id: session.id.clone(),
        target_turn_id: turn.id,
        provider: ProviderKind::Codex,
        source_provider_session_ref: session.provider_session_ref.clone().unwrap(),
        source_canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        agent_revision: agent.revision,
        expected_session_updated_at: session.updated_at,
        request_hash: "7".repeat(64),
        idempotency_key: Some("n07_edit_key".into()),
        edited_snapshot: PersistedUserInputSnapshot {
            prompt_text: "inert replacement request".into(),
            ..Default::default()
        },
        phase: "intended".into(),
        child_provider_session_ref: None,
        child_canonical_provider_session_ref: None,
        replacement_turn_id: None,
        error_code: None,
        review_evidence: None,
        legacy_native_recovery_evidence: None,
        created_at_ms: 104,
        updated_at_ms: 104,
    };
    repository.begin_session_edit_rerun(&operation).unwrap();
    repository
        .advance_session_edit_rerun(
            &operation.operation_id,
            "native_started",
            None,
            None,
            None,
            105,
        )
        .unwrap();
    operation
}
pub(super) struct RebindSeed {
    pub operation: WorkspaceAgentRebindOperation,
    pub session: SessionRecord,
    pub agent: WorkspaceAgentRecord,
}
fn seed_rebind(
    repository: &SqliteRuntimeRepository,
    workspace: &WorkspaceRecord,
    temp: &tempfile::TempDir,
) -> RebindSeed {
    let base = temp.path().join("inert-worktrees");
    let source = ManagedWorktreeRecord {
        id: "n07_worktree_source".into(),
        repo_root: workspace.canonical_root.clone(),
        worktree_root: base.display().to_string(),
        worktree_cwd: base.join("source").display().to_string(),
        branch_name: "fixture/source".into(),
        worktree_name: "source".into(),
        unified_workspace_path: "n07".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 100,
        updated_at: 100,
    };
    let destination = ManagedWorktreeRecord {
        id: "n07_worktree_destination".into(),
        worktree_cwd: base.join("destination").display().to_string(),
        branch_name: "fixture/destination".into(),
        worktree_name: "destination".into(),
        ..source.clone()
    };
    repository.upsert_managed_worktree(&source).unwrap();
    repository.upsert_managed_worktree(&destination).unwrap();
    let (mut session, mut agent) =
        workspace_agent_fixture(workspace, "n07_rebind_agent", "n07-rebind");
    session.cwd = Some(source.worktree_cwd.clone());
    session.worktree_id = Some(source.id.clone());
    agent.recreation_policy.authoritative_cwd = source.worktree_cwd.clone();
    repository.create_workspace_agent(&session, &agent).unwrap();
    let agent = repository
        .get_workspace_agent(&workspace.workspace_id, &session.id)
        .unwrap()
        .unwrap();
    assert_eq!(
        repository
            .get_managed_worktree_claim(&source.id, &session.id)
            .unwrap()
            .unwrap()
            .claim_generation,
        Some(1)
    );
    let operation = WorkspaceAgentRebindOperation {
        operation_id: "n07_rebind".into(),
        workspace_id: workspace.workspace_id.clone(),
        agent_id: session.id.clone(),
        idempotency_key: Some("n07_rebind_key".into()),
        expected_revision: agent.revision,
        previous_worktree_id: Some(source.id),
        destination_worktree_id: Some(destination.id),
        previous_cwd: source.worktree_cwd,
        destination_cwd: destination.worktree_cwd,
        cleanup_previous_worktree: false,
        previous_cleanup_status: None,
        previous_cleanup_diagnostic: None,
        phase: "intended".into(),
        provider_evidence: None,
        error_code: None,
        created_at: 200,
        updated_at: 200,
    };
    repository.begin_workspace_agent_rebind(&operation).unwrap();
    let reservation = repository
        .get_managed_worktree_claim(
            operation.destination_worktree_id.as_deref().unwrap(),
            &session.id,
        )
        .unwrap()
        .unwrap();
    assert_eq!(reservation.claim_role, "rebind_reservation");
    assert_eq!(reservation.claim_generation, Some(1));
    RebindSeed {
        operation,
        session,
        agent,
    }
}
pub(super) struct Seeds {
    pub turn: TurnAdmissionRecord,
    pub process_turn: TurnAdmissionRecord,
    pub approval: ApprovalSeed,
    pub hook: TeamHookEffectRecord,
    pub edit: SessionEditRerunOperation,
    pub rebind: RebindSeed,
    pub retirement: WorkspaceRetirementCommand,
    pub retiring_session: String,
}
pub(super) fn seed_all(repository: &SqliteRuntimeRepository, temp: &tempfile::TempDir) -> Seeds {
    let ws = workspace(repository, temp, "turn-root");
    let (s, _) = agent(repository, &ws, "n07_turn_agent");
    let turn = dispatching(
        repository,
        &s,
        "n07_turn",
        TurnInputProjectionSource::UserVisible,
        None,
    )
    .2;
    let ws = workspace(repository, temp, "process-root");
    let (s, _) = agent(repository, &ws, "n07_process_agent");
    let process_turn = seed_process(repository, &ws, &s, temp);
    let ws = workspace(repository, temp, "approval-root");
    let (s, _) = agent(repository, &ws, "n07_approval_agent");
    let approval = seed_approval(repository, &s);
    let ws = workspace(repository, temp, "hook-root");
    let hook = seed_hook(repository, &ws);
    let ws = workspace(repository, temp, "edit-root");
    let (s, a) = agent(repository, &ws, "n07_edit_agent");
    let edit = seed_edit(repository, &s, &a);
    let ws = workspace(repository, temp, "rebind-root");
    let rebind = seed_rebind(repository, &ws, temp);
    // Separate ownership prevents a retirement fence masking other families.
    let ws = workspace(repository, temp, "retirement-root");
    let (s, _) = agent(repository, &ws, "n07_retiring_agent");
    let current = repository.get_workspace(&ws.workspace_id).unwrap().unwrap();
    let retirement = retirement_command(&current, "n07_retirement_key");
    let plan = match repository.admit_workspace_retirement(&retirement).unwrap() {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected {other:?}"),
    };
    repository
        .claim_workspace_retirement_native_close(&plan.operation_id, &s.id, 300)
        .unwrap();
    Seeds {
        turn,
        process_turn,
        approval,
        hook,
        edit,
        rebind,
        retirement,
        retiring_session: s.id,
    }
}
