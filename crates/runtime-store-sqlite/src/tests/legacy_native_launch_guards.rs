//! Disposable database authority tests. No provider, Git or shell is run.
use super::legacy_native_turn_guards::{legacy, raw_state, LEGACY_ID};
use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::setup;
use super::*;
use runtime_core::{
    OperationActor, OperatorAgentCreateAdmission, OperatorAgentCreateCommand,
    OperatorAgentCreateLaunchSnapshot, RuntimeError, TeamHookApprovalUpdate,
    TeamHookEffectAdmission, TeamHookEffectPhase, TeamHookEffectReceipt, TeamHookEventName,
    TeamHookResolvedEntry, TeamHookScope, WorkspaceAgentRebindOperation,
    WorkspaceWorktreeCreationAdmission, WorkspaceWorktreeCreationCommand, WorktreeCreateResponse,
};

fn blocked(error: RuntimeError) {
    assert!(
        matches!(error, RuntimeError::LegacyNativeObligationBlocked { ref coordinate, .. }
        if coordinate.operation_id == LEGACY_ID),
        "{error:?}"
    );
}

fn git_fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    WorkspaceWorktreeCreationCommand,
) {
    let (temp, repo, workspace) = setup();
    let request = serde_json::json!({"workspace_id":workspace.workspace_id,"worktree_name":"feature",
        "branch_prefix":null,"base_ref":null,"deletion_policy":"retain_on_last_claim","run_init_script":false});
    let command = WorkspaceWorktreeCreationCommand {
        operation_id: "op_legacy_guard_git".into(),
        workspace_id: workspace.workspace_id,
        actor: OperationActor::operator("runtime_operator"),
        idempotency_key: "legacy-guard-git".into(),
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        repository_root: workspace.canonical_root,
        branch_name: "gg/feature".into(),
        checkout_path: temp.path().join("feature").to_string_lossy().into_owned(),
        requested_at: 100,
    };
    (temp, repo, command)
}

fn git_result(command: &WorkspaceWorktreeCreationCommand) -> WorktreeCreateResponse {
    WorktreeCreateResponse {
        worktree: ManagedWorktreeRecord {
            id: "wt_legacy_guard_git".into(),
            repo_root: command.repository_root.clone(),
            worktree_root: std::path::Path::new(&command.checkout_path)
                .parent()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            worktree_cwd: command.checkout_path.clone(),
            branch_name: command.branch_name.clone(),
            worktree_name: "feature".into(),
            unified_workspace_path: "feature".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: Some(command.operation_id.clone()),
            created_at: 101,
            updated_at: 101,
        },
        created: true,
        init_script_status: "skipped_disabled".into(),
    }
}

#[test]
fn native_git_admission_refuses_late_history_before_claims_or_events() {
    let (temp, repo, command) = git_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "rolled_back", "{}", false);
    let before = raw_state(&db);
    blocked(
        repo.admit_workspace_worktree_creation(&command)
            .unwrap_err(),
    );
    assert_eq!(raw_state(&db), before);
}

#[test]
fn known_git_result_settlement_and_original_key_replay_remain_eligible() {
    let (temp, repo, command) = git_fixture();
    assert!(matches!(
        repo.admit_workspace_worktree_creation(&command).unwrap(),
        WorkspaceWorktreeCreationAdmission::Execute { .. }
    ));
    // Synthetic observed-history receipt, not a claim that this test ran Git.
    let result = git_result(&command);
    repo.upsert_managed_worktree(&result.worktree).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "planned", "not-json", false);
    repo.complete_workspace_worktree_creation(&command.operation_id, &result, 103)
        .unwrap();
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    match reopened
        .admit_workspace_worktree_creation(&command)
        .unwrap()
    {
        WorkspaceWorktreeCreationAdmission::Replay {
            operation_id,
            response,
        } => {
            assert_eq!(operation_id, command.operation_id);
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                serde_json::to_value(result).unwrap()
            );
        }
        other => panic!("expected immutable Git receipt: {other:?}"),
    }
    assert_eq!(raw_state(&db), before);
}

#[test]
fn first_provider_launch_freeze_checks_root_checkout_even_for_disjoint_session() {
    for with_history in [false, true] {
        let (temp, repo, workspace) = setup();
        let id = "sess_codex_legacy_guard_reserved_provider";
        let (mut session, mut agent) = workspace_agent_fixture(&workspace, id, "reserved-heron");
        session.provider_session_ref = None;
        session.canonical_provider_session_ref = None;
        agent.provider_session_ref = None;
        agent.canonical_provider_session_ref = None;
        let typed: runtime_core::WorkspaceAgentCreateRequest =
            serde_json::from_value(serde_json::json!({
                "provider":"codex","model":"gpt-test"
            }))
            .unwrap();
        let request = serde_json::json!({"workspace_id":workspace.workspace_id,"action":"create","request":typed});
        let command = OperatorAgentCreateCommand {
            operation_id: "op_legacy_guard_launch".into(),
            workspace_id: workspace.workspace_id,
            reserved_agent_id: id.into(),
            actor: OperationActor::operator("runtime_operator"),
            idempotency_key: "legacy-guard-launch".into(),
            normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
            normalized_request: request,
            requested_at: 100,
            worktree_operation_id: None,
        };
        assert!(matches!(
            repo.admit_operator_agent_create(&command).unwrap(),
            OperatorAgentCreateAdmission::Execute { .. }
        ));
        let snapshot = OperatorAgentCreateLaunchSnapshot {
            session,
            agent,
            onboarding: Default::default(),
            hooks: None,
            worktree_operation_id: None,
        };
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        if with_history {
            // The Session axis is positively disjoint. Root creation still
            // has a checkout boundary even though it does not create Git.
            legacy(
                &db,
                "session_created",
                "{\"spawned_session_id\":\"different-known-child\"}",
                false,
            );
        }
        let before = raw_state(&db);
        let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
        let outcome = reopened.freeze_operator_agent_create_launch(&command, &snapshot);
        if with_history {
            blocked(outcome.unwrap_err());
            assert_eq!(raw_state(&db), before);
        } else {
            outcome.unwrap();
            let session_count: i64 = db
                .query_row("SELECT COUNT(*) FROM sessions WHERE id=?1", [id], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(session_count, 0);
            let details = repo.get_operation(&command.operation_id).unwrap().unwrap();
            assert_eq!(
                details.effects[0].evidence.as_ref().unwrap()["stage"],
                "launch_frozen_before_provider"
            );
        }
    }
}

fn rebind_fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    WorkspaceAgentRebindOperation,
) {
    let (temp, repo, workspace) = setup();
    let (session, agent) =
        workspace_agent_fixture(&workspace, "legacy-rebind-target", "rebind-heron");
    repo.create_workspace_agent(&session, &agent).unwrap();
    let operation = WorkspaceAgentRebindOperation {
        operation_id: "op_legacy_guard_rebind".into(),
        workspace_id: workspace.workspace_id,
        agent_id: agent.agent_id,
        idempotency_key: Some("legacy-guard-rebind".into()),
        expected_revision: 0,
        previous_worktree_id: None,
        destination_worktree_id: None,
        previous_cwd: workspace.canonical_root,
        destination_cwd: temp
            .path()
            .join("other-root")
            .to_string_lossy()
            .into_owned(),
        cleanup_previous_worktree: false,
        previous_cleanup_status: None,
        previous_cleanup_diagnostic: None,
        phase: "intended".into(),
        provider_evidence: None,
        error_code: None,
        created_at: 101,
        updated_at: 101,
    };
    (temp, repo, operation)
}

#[test]
fn rebind_admission_checks_checkout_scope_when_child_session_is_unrelated() {
    let (temp, repo, operation) = rebind_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(
        &db,
        "session_created",
        "{\"spawned_session_id\":\"another-child\"}",
        false,
    );
    let before = raw_state(&db);
    blocked(repo.begin_workspace_agent_rebind(&operation).unwrap_err());
    assert_eq!(raw_state(&db), before);
}

#[test]
fn original_rebind_intent_is_readable_after_later_legacy_obligation() {
    let (temp, repo, operation) = rebind_fixture();
    let admitted = repo.begin_workspace_agent_rebind(&operation).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "planned", "not-json", false);
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        reopened.begin_workspace_agent_rebind(&operation).unwrap(),
        admitted
    );
    assert_eq!(raw_state(&db), before);
}

fn hook_fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    TeamHookEffectAdmission,
) {
    let (temp, repo, workspace) = setup();
    let hook = TeamHookResolvedEntry {
        event_name: TeamHookEventName::PostTeammateAdd,
        name: "legacy-native-boundary".into(),
        command: "printf inert-test-command".into(),
        timeout_ms: 500,
        scope: TeamHookScope::Project,
    };
    repo.update_team_hook_approval(&TeamHookApprovalUpdate {
        workspace_id: workspace.workspace_id.clone(),
        expected_revision: 0,
        hooks: vec![hook.clone()],
        approved_at_ms: 7,
    })
    .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute("INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
        VALUES('evt_guard_hook','workspace',?1,101,'workspace.member_added',1,?2,8)",
        params![workspace.workspace_id,serde_json::json!({"workspace_id":workspace.workspace_id}).to_string()]).unwrap();
    let input = TeamHookEffectAdmission {
        workspace_id: workspace.workspace_id,
        parent_event_id: "evt_guard_hook".into(),
        hook,
        input: serde_json::json!({"schema_version":1,"event_id":"evt_guard_hook","hook_event":"post_teammate_add"}),
        created_at_ms: 9,
    };
    repo.admit_team_hook_effect(&input).unwrap();
    (temp, repo, input)
}

#[test]
fn first_hook_claim_checks_actual_workspace_execution_root_after_reopen() {
    let (temp, repo, input) = hook_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "rolled_back", "{}", false);
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    blocked(
        reopened
            .claim_team_hook_effect(
                &input.workspace_id,
                &input.parent_event_id,
                input.hook.event_name,
                &input.hook.name,
                10,
            )
            .unwrap_err(),
    );
    assert_eq!(raw_state(&db), before);
    assert_eq!(
        repo.list_team_hook_effects(&input.workspace_id, 10)
            .unwrap()[0]
            .phase,
        TeamHookEffectPhase::Intended
    );
}

#[test]
fn known_started_hook_result_and_terminal_replay_ignore_new_launch_fence() {
    let (temp, repo, input) = hook_fixture();
    repo.claim_team_hook_effect(
        &input.workspace_id,
        &input.parent_event_id,
        input.hook.event_name,
        &input.hook.name,
        10,
    )
    .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "planned", "not-json", false);
    let receipt = TeamHookEffectReceipt {
        validated_output: Some(serde_json::json!({})),
        error_code: None,
    };
    // Inject only a known shell-result receipt; the command is never executed.
    let terminal = repo
        .finish_team_hook_effect(
            &input.workspace_id,
            &input.parent_event_id,
            input.hook.event_name,
            &input.hook.name,
            TeamHookEffectPhase::Succeeded,
            receipt,
            11,
        )
        .unwrap();
    let before = raw_state(&db);
    assert_eq!(repo.admit_team_hook_effect(&input).unwrap(), terminal);
    assert_eq!(raw_state(&db), before);
}
