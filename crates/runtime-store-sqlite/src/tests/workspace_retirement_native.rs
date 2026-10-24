use super::workspace_retirement::{retirement_command, setup};
use super::*;
use runtime_core::{
    prepare_workspace_registration, OperationActor, OperationPhase,
    ProviderRetirementCloseEvidence, ProviderRetirementCloseProofKind, WorkspaceLifecycleState,
    WorkspaceRegisterRequest, WorkspaceRetirementAdmission,
};

#[test]
fn retirement_freeze_is_atomic_resumable_and_blocks_new_native_work() {
    let (temp, repo, workspace) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut owned = sample_session();
    owned.cwd = Some(workspace.canonical_root.clone());
    owned.status = "ready".into();
    owned.active_turn_id = None;
    owned.worktree_id = None;
    repo.upsert_session(&owned).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
         VALUES (?1,?2,'migration',1)",
        params![owned.id, workspace.workspace_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO managed_processes (process_id,workspace_id,owner_session_id,
            command_json,status,admission_order,queue_order,capture_limit_bytes,
            stdout_path,stderr_path,admitted_at,updated_at)
         VALUES ('pending-native',?1,?2,'{}','queued',1,1,1024,'out','err',1,1)",
        params![workspace.workspace_id, owned.id],
    )
    .unwrap();

    let command = retirement_command(&workspace, "freeze-first");
    let admitted = repo.admit_workspace_retirement(&command).unwrap();
    let plan = match admitted {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected retirement admission: {other:?}"),
    };
    assert_eq!(plan.agent_ids, vec![owned.id.clone()]);
    assert_eq!(plan.managed_process_ids, vec!["pending-native"]);
    assert_eq!(plan.operation_id, command.operation_id);

    let durable = repo.get_operation(&command.operation_id).unwrap().unwrap();
    assert_eq!(durable.operation.phase, OperationPhase::Requested);
    assert_eq!(durable.claims.len(), 1);
    assert_eq!(durable.effects.len(), 2);
    assert_eq!(durable.transitions.len(), 1);
    assert_eq!(
        durable
            .effects
            .iter()
            .filter(|e| e.phase == "intended")
            .count(),
        2
    );

    assert!(db
        .execute(
            "INSERT INTO turns(id,session_id,status,input_json)
         VALUES ('late-turn',?1,'running','{}')",
            [&owned.id],
        )
        .is_err());
    assert!(db.execute(
        "UPDATE managed_processes SET status='launch_reserved' WHERE process_id='pending-native'", [],
    ).is_err());
    assert!(db.execute(
        "INSERT INTO managed_processes (process_id,workspace_id,command_json,status,
            admission_order,queue_order,capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at)
         VALUES ('new-native',?1,'{}','queued',2,2,1024,'out','err',1,1)",
        [&workspace.workspace_id],
    ).is_err());
    let mut late = sample_session();
    late.id = "late-session".into();
    late.cwd = Some(workspace.canonical_root.clone());
    late.active_turn_id = None;
    late.worktree_id = None;
    assert!(repo.upsert_session(&late).is_err());
    assert!(db
        .execute(
            "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
         VALUES (?1,?2,'migration',1)",
            params!["late-session", workspace.workspace_id],
        )
        .is_err());
    assert!(format!(
        "{}",
        repo.register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: workspace.canonical_root.clone(),
                    display_name: Some(workspace.display_name.clone()),
                },
                OperationActor::operator("retirement-test"),
                Some("register-frozen".into()),
            )
            .unwrap(),
        )
        .unwrap_err()
    )
    .contains("undergoing retirement"));
    assert!(repo
        .retire_empty_workspace(&retirement_command(&workspace, "quiescent-during-freeze"))
        .is_err());
    assert_eq!(
        repo.get_workspace(&workspace.workspace_id)
            .unwrap()
            .unwrap(),
        workspace
    );

    // Allow native settlement, without allowing a process launch.
    db.execute(
        "UPDATE managed_processes SET status='canceled' WHERE process_id='pending-native'",
        [],
    )
    .unwrap();
    let second = repo
        .admit_workspace_retirement(&retirement_command(&workspace, "freeze-first"))
        .unwrap();
    assert_eq!(second, WorkspaceRetirementAdmission::Resume(plan.clone()));
    assert_eq!(
        repo.workspace_retirement_preflight(&workspace.workspace_id)
            .unwrap()
            .blockers
            .iter()
            .find(|b| b.code == "retirement_in_progress")
            .unwrap()
            .count,
        1
    );
    assert!(repo
        .admit_workspace_retirement(&retirement_command(&workspace, "another-key"))
        .is_err());
    assert_eq!(
        repo.get_operation(&command.operation_id)
            .unwrap()
            .unwrap()
            .effects
            .len(),
        2
    );
}

#[test]
fn retirement_freeze_rejects_unowned_legacy_sessions_without_partial_operation() {
    let (_temp, repo, workspace) = setup();
    let mut legacy = sample_session();
    legacy.cwd = Some(workspace.canonical_root.clone());
    legacy.status = "closed".into();
    legacy.active_turn_id = None;
    legacy.worktree_id = None;
    repo.upsert_session(&legacy).unwrap();
    let command = retirement_command(&workspace, "legacy-fail");
    assert!(
        format!("{}", repo.admit_workspace_retirement(&command).unwrap_err())
            .contains("unowned provider sessions")
    );
    assert!(repo.get_operation(&command.operation_id).unwrap().is_none());
    assert!(repo
        .workspace_retirement_preflight(&workspace.workspace_id)
        .unwrap()
        .blockers
        .iter()
        .all(|b| b.code != "retirement_in_progress"));
}

#[test]
fn verified_native_close_and_passive_settlement_are_required_for_retirement_finalization() {
    let (temp, repo, workspace) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut native = sample_session();
    native.cwd = Some(workspace.canonical_root.clone());
    native.status = "ready".into();
    native.provider_session_ref = Some("native-session-retirement".into());
    native.active_turn_id = None;
    native.worktree_id = None;
    repo.upsert_session(&native).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
        VALUES (?1,?2,'migration',1)",
        params![native.id, workspace.workspace_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO managed_processes (process_id,workspace_id,owner_session_id,
        command_json,status,admission_order,queue_order,capture_limit_bytes,
        stdout_path,stderr_path,admitted_at,updated_at)
        VALUES ('queued-for-retirement',?1,?2,'{}','queued',1,1,1024,'out','err',1,1)",
        params![workspace.workspace_id, native.id],
    )
    .unwrap();

    let command = retirement_command(&workspace, "close-verified");
    let plan = match repo.admit_workspace_retirement(&command).unwrap() {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected admission {other:?}"),
    };
    assert_eq!(plan.agent_ids, vec![native.id.clone()]);
    assert!(repo
        .finalize_settled_workspace_retirement(&plan.operation_id, 2)
        .unwrap()
        .is_none());
    repo.settle_passive_workspace_retirement(&plan.operation_id, 3)
        .unwrap();
    let process_status: String = db
        .query_row(
            "SELECT status FROM managed_processes
        WHERE process_id='queued-for-retirement'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(process_status, "canceled");
    assert!(repo
        .finalize_settled_workspace_retirement(&plan.operation_id, 4)
        .unwrap()
        .is_none());
    let attempt = repo
        .claim_workspace_retirement_native_close(&plan.operation_id, &native.id, 5)
        .unwrap();
    assert_eq!(attempt.provider_session_ref, "native-session-retirement");
    assert!(repo
        .claim_workspace_retirement_native_close(&plan.operation_id, &native.id, 6)
        .is_err());
    assert!(repo
        .finalize_settled_workspace_retirement(&plan.operation_id, 7)
        .unwrap()
        .is_none());

    let forged = ProviderRetirementCloseEvidence {
        runtime_session_id: native.id.clone(),
        provider_session_ref: "other-native".into(),
        proof_kind: ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
    };
    assert!(repo
        .finalize_workspace_retirement_native_close(&attempt, &forged, 8)
        .is_err());
    let evidence = ProviderRetirementCloseEvidence {
        runtime_session_id: native.id.clone(),
        provider_session_ref: "native-session-retirement".into(),
        proof_kind: ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
    };
    repo.finalize_workspace_retirement_native_close(&attempt, &evidence, 9)
        .unwrap();
    let receipt = repo
        .finalize_settled_workspace_retirement(&plan.operation_id, 10)
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.workspace.lifecycle_state,
        WorkspaceLifecycleState::Retired
    );
    assert_eq!(receipt.workspace.workspace_id, workspace.workspace_id);
    let durable = repo.get_operation(&plan.operation_id).unwrap().unwrap();
    assert_eq!(durable.operation.phase, OperationPhase::Completed);
    assert_eq!(
        durable.operation.exact_terminal_result,
        Some(serde_json::to_value(&receipt).unwrap())
    );
    assert!(durable.claims.is_empty());
    assert_eq!(durable.effects.len(), 2);
    assert!(durable
        .effects
        .iter()
        .all(|effect| effect.phase == "finalized"));
    assert!(repo
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: workspace.canonical_root.clone(),
                    display_name: Some(workspace.display_name.clone())
                },
                OperationActor::operator("retirement-test"),
                Some("reopen-verified".into()),
            )
            .unwrap()
        )
        .is_ok());
}

#[test]
fn unknown_provider_close_stays_frozen_and_cannot_claim_terminal_success() {
    let (temp, repo, workspace) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut native = sample_session();
    native.cwd = Some(workspace.canonical_root.clone());
    native.active_turn_id = None;
    native.worktree_id = None;
    native.provider_session_ref = Some("native-unknown".into());
    repo.upsert_session(&native).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
        VALUES (?1,?2,'migration',1)",
        params![native.id, workspace.workspace_id],
    )
    .unwrap();
    let command = retirement_command(&workspace, "unknown-native");
    let plan = match repo.admit_workspace_retirement(&command).unwrap() {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected {other:?}"),
    };
    let attempt = repo
        .claim_workspace_retirement_native_close(&plan.operation_id, &native.id, 3)
        .unwrap();
    repo.quarantine_workspace_retirement_native_close(&attempt, "transport_timeout", 4)
        .unwrap();
    assert!(repo
        .finalize_settled_workspace_retirement(&plan.operation_id, 5)
        .is_err());
    assert!(repo
        .claim_workspace_retirement_native_close(&plan.operation_id, &native.id, 6)
        .is_err());
    assert_eq!(
        repo.get_operation(&plan.operation_id)
            .unwrap()
            .unwrap()
            .operation
            .phase,
        OperationPhase::ManualReview
    );
    assert_eq!(
        repo.get_workspace(&workspace.workspace_id)
            .unwrap()
            .unwrap()
            .lifecycle_state,
        WorkspaceLifecycleState::Active
    );
    assert!(repo
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: workspace.canonical_root.clone(),
                    display_name: Some(workspace.display_name.clone())
                },
                OperationActor::operator("retirement-test"),
                Some("reopen-unknown".into()),
            )
            .unwrap()
        )
        .is_err());
}

#[test]
fn restart_never_replays_an_unreceipted_provider_close_and_recovers_clean_admissions() {
    let (temp, repository, workspace) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut native = sample_session();
    native.cwd = Some(workspace.canonical_root.clone());
    native.active_turn_id = None;
    native.worktree_id = None;
    native.provider_session_ref = Some("native-restart-unknown".into());
    repository.upsert_session(&native).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
        VALUES (?1,?2,'migration',1)",
        params![native.id, workspace.workspace_id],
    )
    .unwrap();
    let admitted = retirement_command(&workspace, "restart-close-key");
    let plan = match repository.admit_workspace_retirement(&admitted).unwrap() {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(
        repository
            .list_recoverable_workspace_retirements()
            .unwrap()
            .len(),
        1
    );
    repository
        .claim_workspace_retirement_native_close(&plan.operation_id, &native.id, 2)
        .unwrap();
    drop(repository);
    let reopened = repo(&temp);
    reopened.initialize_schema().unwrap();
    assert_eq!(
        reopened
            .quarantine_ambiguous_retirement_effects_after_restart(3)
            .unwrap(),
        1
    );
    assert_eq!(
        reopened
            .quarantine_ambiguous_retirement_effects_after_restart(4)
            .unwrap(),
        0
    );
    assert!(reopened
        .list_recoverable_workspace_retirements()
        .unwrap()
        .is_empty());
    let details = reopened.get_operation(&plan.operation_id).unwrap().unwrap();
    assert_eq!(details.operation.phase, OperationPhase::ManualReview);
    assert_eq!(details.effects[0].phase, "manual_review");
    assert_eq!(
        reopened.retiring_operation_for_session(&native.id).unwrap(),
        Some(plan.operation_id.clone())
    );
    assert!(reopened
        .claim_workspace_retirement_native_close(&plan.operation_id, &native.id, 5)
        .is_err());
    assert_eq!(
        reopened
            .get_workspace(&workspace.workspace_id)
            .unwrap()
            .unwrap()
            .lifecycle_state,
        WorkspaceLifecycleState::Active
    );
}

#[test]
fn restart_resumes_only_unstarted_retirements_using_original_idempotency_identity() {
    let (temp, repository, workspace) = setup();
    let admitted = retirement_command(&workspace, "restart-safe-key");
    let plan = match repository.admit_workspace_retirement(&admitted).unwrap() {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected {other:?}"),
    };
    drop(repository);
    let reopened = repo(&temp);
    reopened.initialize_schema().unwrap();
    assert_eq!(
        reopened
            .quarantine_ambiguous_retirement_effects_after_restart(2)
            .unwrap(),
        0
    );
    let commands = reopened.list_recoverable_workspace_retirements().unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].operation_id, plan.operation_id);
    assert_eq!(commands[0].actor, admitted.actor);
    assert_eq!(
        commands[0].normalized_request_hash,
        admitted.normalized_request_hash
    );
    assert_eq!(commands[0].idempotency_key, admitted.idempotency_key);
    assert_eq!(
        reopened.admit_workspace_retirement(&commands[0]).unwrap(),
        WorkspaceRetirementAdmission::Resume(plan.clone())
    );
    reopened
        .settle_passive_workspace_retirement(&plan.operation_id, 3)
        .unwrap();
    let result = reopened
        .finalize_settled_workspace_retirement(&plan.operation_id, 4)
        .unwrap()
        .unwrap();
    assert_eq!(
        result.workspace.lifecycle_state,
        WorkspaceLifecycleState::Retired
    );
    assert!(reopened
        .list_recoverable_workspace_retirements()
        .unwrap()
        .is_empty());
}
