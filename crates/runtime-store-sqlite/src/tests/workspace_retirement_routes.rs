use super::*;
use runtime_core::{
    prepare_workspace_registration, prepare_workspace_retirement, OperationActor,
    PersistedUserInputSnapshot, TurnAdmissionRecord, TurnCorrelationState,
    TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
    WorkspaceRegisterRequest, WorkspaceRetirementCommand, WorkspaceRetirementRequest,
};

fn setup() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    runtime_core::WorkspaceRecord,
    ManagedWorktreeRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("repository");
    let external = temp.path().join("external-checkout");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(external.join("src")).unwrap();
    let workspace = repository
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.to_string_lossy().into_owned(),
                    display_name: Some("External routes".into()),
                },
                OperationActor::operator("route-test"),
                Some("register-routes".into()),
            )
            .unwrap(),
        )
        .unwrap()
        .workspace;
    let worktree = ManagedWorktreeRecord {
        id: "external-worktree".into(),
        repo_root: workspace.canonical_root.clone(),
        worktree_root: temp.path().display().to_string(),
        worktree_cwd: external.display().to_string(),
        branch_name: "gg/external".into(),
        worktree_name: "external".into(),
        unified_workspace_path: "external".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 1,
        updated_at: 1,
    };
    repository.upsert_managed_worktree(&worktree).unwrap();
    (temp, repository, workspace, worktree)
}

fn retire(workspace: &runtime_core::WorkspaceRecord) -> WorkspaceRetirementCommand {
    prepare_workspace_retirement(
        &workspace.workspace_id,
        WorkspaceRetirementRequest {
            expected_revision: workspace.revision,
        },
        OperationActor::operator("route-test"),
        Some("retire-routes".into()),
    )
    .unwrap()
}

#[test]
fn external_managed_routes_classify_legacy_obligations_and_fence_new_native_work() {
    let (temp, repository, workspace, worktree) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let cwd = format!("{}/src", worktree.worktree_cwd);
    let mut legacy = sample_session();
    legacy.cwd = Some(cwd.clone());
    legacy.active_turn_id = None;
    legacy.worktree_id = None;
    repository.upsert_session(&legacy).unwrap();
    db.execute(
        "INSERT INTO managed_processes(process_id,command_json,cwd,status,admission_order,
         queue_order,capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at)
         VALUES ('legacy-managed','{}',?1,'queued',1,1,1024,'out','err',1,1)",
        [&cwd],
    )
    .unwrap();
    db.execute(
        "INSERT INTO processes(id,command_json,cwd,status,started_at)
         VALUES ('legacy-process','{}',?1,'running',1)",
        [&cwd],
    )
    .unwrap();
    let preview = repository
        .workspace_retirement_preflight(&workspace.workspace_id)
        .unwrap();
    for code in [
        "unowned_provider_sessions",
        "unowned_managed_processes",
        "unowned_legacy_processes",
    ] {
        assert!(preview
            .blockers
            .iter()
            .any(|b| b.code == code && b.count == 1));
    }
    assert!(repository
        .admit_workspace_retirement(&retire(&workspace))
        .is_err());
    db.execute("DELETE FROM sessions WHERE id=?1", [&legacy.id])
        .unwrap();
    db.execute("DELETE FROM managed_processes", []).unwrap();
    db.execute("DELETE FROM processes", []).unwrap();
    repository
        .admit_workspace_retirement(&retire(&workspace))
        .unwrap();

    assert!(repository.upsert_session(&legacy).is_err());
    assert!(db
        .execute(
            "INSERT INTO managed_processes(process_id,command_json,cwd,status,admission_order,
         queue_order,capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at)
         VALUES ('late-managed','{}',?1,'queued',2,2,1024,'out','err',2,2)",
            [&cwd],
        )
        .is_err());
    assert!(db
        .execute(
            "INSERT INTO processes(id,command_json,cwd,status,started_at)
         VALUES ('late-process','{}',?1,'running',2)",
            [&cwd],
        )
        .is_err());
    legacy.cwd = Some(format!("{}-unrelated", worktree.worktree_cwd));
    repository.upsert_session(&legacy).unwrap();
    assert!(repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: worktree.id,
            session_id: legacy.id,
            claim_role: "owner".into(),
            created_at: 3,
            released_at: None,
        })
        .is_err());
}

#[test]
fn frozen_workspace_allows_native_settlement_upserts_but_rejects_reopening() {
    let (temp, repository, workspace, worktree) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut session = sample_session();
    session.cwd = Some(worktree.worktree_cwd.clone());
    session.worktree_id = Some(worktree.id.clone());
    repository.upsert_session(&session).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
         VALUES (?1,?2,'migration',1)",
        params![session.id, workspace.workspace_id],
    )
    .unwrap();
    let mut turn = sample_turn();
    repository.upsert_turn(&turn).unwrap();
    let mut admission = TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot::from_input(&[]),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Dispatching,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 1,
        updated_at: 1,
    };
    repository.upsert_turn_admission(&admission).unwrap();
    let mut claim = ManagedWorktreeClaimRecord {
        worktree_id: worktree.id.clone(),
        session_id: session.id.clone(),
        claim_role: "owner".into(),
        created_at: 1,
        released_at: None,
    };
    repository.upsert_managed_worktree_claim(&claim).unwrap();
    let command = retire(&workspace);
    repository.admit_workspace_retirement(&command).unwrap();
    assert_eq!(
        repository
            .retiring_operation_for_session(&session.id)
            .unwrap(),
        Some(command.operation_id)
    );

    // These are the real UPSERT methods used by native ACK/terminal handling.
    // BEFORE INSERT must not mistake an existing row for new admission.
    admission.dispatch_state = TurnDispatchState::Dispatched;
    admission.provider_native_turn_id = Some("native-turn".into());
    repository.upsert_turn_admission(&admission).unwrap();
    turn.status = "interrupted".into();
    turn.completed_at = Some(4);
    repository.upsert_turn(&turn).unwrap();
    session.status = "closed".into();
    session.active_turn_id = None;
    session.closed_at = Some(4);
    repository.upsert_session(&session).unwrap();
    claim.released_at = Some(4);
    repository.upsert_managed_worktree_claim(&claim).unwrap();
    repository.upsert_managed_worktree(&worktree).unwrap();

    turn.status = "running".into();
    turn.completed_at = None;
    assert!(repository.upsert_turn(&turn).is_err());
    session.status = "active".into();
    assert!(repository.upsert_session(&session).is_err());
    claim.released_at = None;
    assert!(repository.upsert_managed_worktree_claim(&claim).is_err());
}
