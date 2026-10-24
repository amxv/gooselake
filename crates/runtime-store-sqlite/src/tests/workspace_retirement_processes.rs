use super::workspace_retirement::{register, retirement_command, setup};
use super::*;
use runtime_core::{
    OperationPhase, ProcessCompletionUpdate, ProviderRetirementCloseEvidence,
    ProviderRetirementCloseProofKind,
};
use std::sync::{Arc, Barrier};

fn terminal_process(
    temp: &tempfile::TempDir,
    repository: &SqliteRuntimeRepository,
    workspace: &runtime_core::WorkspaceRecord,
) -> SessionRecord {
    let mut session = sample_session();
    session.cwd = Some(workspace.canonical_root.clone());
    session.active_turn_id = None;
    session.worktree_id = None;
    session.status = "ready".into();
    repository.upsert_session(&session).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
        VALUES (?1,?2,'migration',1)",
        params![session.id, workspace.workspace_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO managed_processes(process_id,workspace_id,owner_session_id,
        command_json,status,admission_order,queue_order,capture_limit_bytes,stdout_path,
        stderr_path,admitted_at,updated_at,completion_state,terminal_recorded_at,ended_at)
        VALUES ('terminal-output',?1,?2,'{}','completed',1,1,1024,'out','err',1,2,'pending',2,2)",
        params![workspace.workspace_id, session.id],
    )
    .unwrap();
    session
}

fn injecting(attempt_count: i64) -> ProcessCompletionUpdate {
    ProcessCompletionUpdate {
        state: "injecting".into(),
        turn_id: None,
        attempt_count,
        last_error: None,
        updated_at: 12,
    }
}

fn verified_close(
    repository: &SqliteRuntimeRepository,
    operation_id: &str,
    session: &SessionRecord,
) {
    let attempt = repository
        .claim_workspace_retirement_native_close(operation_id, &session.id, 20)
        .unwrap();
    repository
        .finalize_workspace_retirement_native_close(
            &attempt,
            &ProviderRetirementCloseEvidence {
                runtime_session_id: session.id.clone(),
                provider_session_ref: session.provider_session_ref.clone().unwrap(),
                proof_kind: ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
            },
            21,
        )
        .unwrap();
}

#[test]
fn stale_completion_worker_cannot_reopen_retired_authority_and_output_survives_reactivation() {
    let (temp, repository, workspace) = setup();
    let session = terminal_process(&temp, &repository, &workspace);
    let selected = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let worker_repo = repository.clone();
    let worker_selected = selected.clone();
    let worker_resume = resume.clone();
    let worker = std::thread::spawn(move || {
        let stale = worker_repo
            .get_managed_process("terminal-output")
            .unwrap()
            .unwrap();
        worker_selected.wait();
        worker_resume.wait();
        worker_repo
            .update_managed_process_completion(
                &stale.process_id,
                &injecting(stale.completion_attempt_count + 1),
            )
            .unwrap()
    });
    selected.wait();
    let command = retirement_command(&workspace, "completion-freeze");
    repository.admit_workspace_retirement(&command).unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 15)
        .unwrap();
    verified_close(&repository, &command.operation_id, &session);
    let retired = repository
        .finalize_settled_workspace_retirement(&command.operation_id, 22)
        .unwrap();
    assert!(
        retired.is_some(),
        "a deferred result is classified, not discarded"
    );
    resume.wait();
    assert!(
        worker.join().unwrap().is_none(),
        "stale completion claim must lose"
    );
    let output = repository
        .get_managed_process("terminal-output")
        .unwrap()
        .unwrap();
    assert_eq!(output.completion_state, "pending");
    assert_eq!(output.completion_attempt_count, 0);
    assert_eq!(
        output.completion_last_error.as_deref(),
        Some("workspace_retirement_deferred_completion")
    );
    let pending = ProcessCompletionUpdate {
        state: "pending".into(),
        ..injecting(0)
    };
    assert!(repository
        .update_managed_process_completion("terminal-output", &pending)
        .unwrap()
        .is_none());
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    assert!(db
        .execute(
            "UPDATE managed_processes SET completion_state='injecting',
        completion_last_error=NULL WHERE process_id='terminal-output'",
            []
        )
        .is_err());
    let reactivated = register(
        &repository,
        std::path::Path::new(&workspace.canonical_root),
        "restore-root",
    );
    assert_eq!(reactivated.workspace_id, workspace.workspace_id);
    assert_eq!(
        repository
            .get_managed_process("terminal-output")
            .unwrap()
            .unwrap()
            .completion_state,
        "pending"
    );
}

#[test]
fn pre_admission_completion_claim_is_locally_reconciled_without_false_unknown_outcome() {
    let (temp, repository, workspace) = setup();
    let session = terminal_process(&temp, &repository, &workspace);
    assert!(repository
        .update_managed_process_completion("terminal-output", &injecting(1))
        .unwrap()
        .is_some());
    let command = retirement_command(&workspace, "pre-admission-completion");
    repository.admit_workspace_retirement(&command).unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 15)
        .unwrap();
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(operation.operation.phase, OperationPhase::Requested);
    let effect = operation
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_process_settle")
        .unwrap();
    assert_eq!(effect.phase, "finalized");
    assert_eq!(
        effect.evidence.as_ref().unwrap()["outcome"],
        "completion_deferred_until_agent_restore"
    );
    verified_close(&repository, &command.operation_id, &session);
    assert!(repository
        .finalize_settled_workspace_retirement(&command.operation_id, 22)
        .unwrap()
        .is_some());
}

#[test]
fn competing_completion_claims_have_one_winner_and_stale_pending_cannot_rewind_it() {
    let (temp, repository, workspace) = setup();
    terminal_process(&temp, &repository, &workspace);
    let barrier = Arc::new(Barrier::new(6));
    let mut workers = Vec::new();
    for _ in 0..6 {
        let repo = repository.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            repo.update_managed_process_completion("terminal-output", &injecting(1))
                .unwrap()
                .is_some()
        }));
    }
    assert_eq!(
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count(),
        1
    );
    let stale = ProcessCompletionUpdate {
        state: "pending".into(),
        ..injecting(0)
    };
    assert!(repository
        .update_managed_process_completion("terminal-output", &stale)
        .unwrap()
        .is_none());
    let state = repository
        .get_managed_process("terminal-output")
        .unwrap()
        .unwrap();
    assert_eq!(state.completion_state, "injecting");
    assert_eq!(state.completion_attempt_count, 1);
}

#[test]
fn owned_legacy_process_without_workspace_column_is_settled_and_retains_output() {
    let (temp, repository, workspace) = setup();
    let session = terminal_process(&temp, &repository, &workspace);
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE managed_processes SET workspace_id=NULL,cwd='/outside/owned-process'
        WHERE process_id='terminal-output'",
        [],
    )
    .unwrap();
    let preflight = repository
        .workspace_retirement_preflight(&workspace.workspace_id)
        .unwrap();
    assert!(preflight
        .blockers
        .iter()
        .any(|blocker| blocker.code == "managed_processes_unsettled"));
    let command = retirement_command(&workspace, "owned-legacy-process");
    let plan = match repository.admit_workspace_retirement(&command).unwrap() {
        runtime_core::WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected admission: {other:?}"),
    };
    assert_eq!(plan.managed_process_ids, vec!["terminal-output"]);
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 15)
        .unwrap();
    verified_close(&repository, &command.operation_id, &session);
    assert!(repository
        .finalize_settled_workspace_retirement(&command.operation_id, 22)
        .unwrap()
        .is_some());
    let reactivated = register(
        &repository,
        std::path::Path::new(&workspace.canonical_root),
        "legacy-output-restore",
    );
    assert_eq!(reactivated.workspace_id, workspace.workspace_id);
    let output = repository
        .get_managed_process("terminal-output")
        .unwrap()
        .unwrap();
    assert_eq!(output.completion_state, "pending");
    assert_eq!(
        output.completion_last_error.as_deref(),
        Some("workspace_retirement_deferred_completion")
    );
}
