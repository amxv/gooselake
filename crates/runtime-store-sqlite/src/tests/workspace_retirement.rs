use super::*;
use runtime_core::{
    prepare_workspace_registration, prepare_workspace_retirement, OperationActor, OperationPhase,
    WorkspaceLifecycleState, WorkspaceRegisterRequest, WorkspaceRetirementRequest,
};
use std::sync::{Arc, Barrier};

pub(super) fn setup() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    runtime_core::WorkspaceRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let record = register(&repository, &root, "new-root");
    (temp, repository, record)
}

pub(super) fn register(
    repository: &SqliteRuntimeRepository,
    root: &std::path::Path,
    key: &str,
) -> runtime_core::WorkspaceRecord {
    repository
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.to_string_lossy().into_owned(),
                    display_name: Some("Retirement Test".into()),
                },
                OperationActor::operator("retirement-test"),
                Some(key.into()),
            )
            .unwrap(),
        )
        .unwrap()
        .workspace
}

pub(super) fn retirement_command(
    workspace: &runtime_core::WorkspaceRecord,
    key: &str,
) -> runtime_core::WorkspaceRetirementCommand {
    prepare_workspace_retirement(
        &workspace.workspace_id,
        WorkspaceRetirementRequest {
            expected_revision: workspace.revision,
        },
        OperationActor::operator("retirement-test"),
        Some(key.into()),
    )
    .unwrap()
}

#[test]
fn preflight_snapshots_independent_blocker_counts_without_mutating_workspace_authority() {
    let (temp, repo, workspace) = setup();
    let ready = repo
        .workspace_retirement_preflight(&workspace.workspace_id)
        .unwrap();
    assert_eq!(ready.workspace, workspace);
    assert!(ready.eligible_for_quiescent_retirement);
    assert!(ready.blockers.is_empty());

    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    for id in ["first", "second"] {
        db.execute(
            "INSERT INTO managed_processes
              (process_id,workspace_id,command_json,status,admission_order,queue_order,
               capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at)
             VALUES (?1,?2,'{}','queued',1+?3,1+?3,1024,'out','err',1,1)",
            params![
                id,
                workspace.workspace_id,
                if id == "first" { 0 } else { 1 }
            ],
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO runtime_operations(operation_id,workspace_id,kind,actor_kind,actor_id,
                normalized_request_hash,normalized_request_json,phase,created_at,updated_at)
         VALUES ('review',?1,'external','system','worker','input','{}','manual_review',1,1)",
        [&workspace.workspace_id],
    )
    .unwrap();
    let mut legacy = sample_session();
    legacy.cwd = Some(workspace.canonical_root.clone());
    legacy.worktree_id = None;
    legacy.active_turn_id = None;
    repo.upsert_session(&legacy).unwrap();
    let blocked = repo
        .workspace_retirement_preflight(&workspace.workspace_id)
        .unwrap();
    assert!(!blocked.eligible_for_quiescent_retirement);
    let counts = blocked
        .blockers
        .iter()
        .map(|blocker| (blocker.code.as_str(), blocker.count))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(counts.len(), 3);
    assert_eq!(counts["managed_processes_unsettled"], 2);
    assert_eq!(counts["operations_unresolved"], 1);
    assert_eq!(counts["unowned_provider_sessions"], 1);
    assert_eq!(
        repo.get_workspace(&workspace.workspace_id).unwrap(),
        Some(workspace.clone())
    );
    let refusal = repo.retire_empty_workspace(&retirement_command(&workspace, "preflight-refusal"));
    assert!(refusal.is_err());
    assert_eq!(
        repo.get_workspace(&workspace.workspace_id).unwrap(),
        Some(workspace)
    );
}

#[test]
fn preflight_after_retirement_is_ineligible_even_with_no_remaining_obligations() {
    let (_temp, repo, workspace) = setup();
    repo.retire_empty_workspace(&retirement_command(&workspace, "retire"))
        .unwrap();
    let result = repo
        .workspace_retirement_preflight(&workspace.workspace_id)
        .unwrap();
    assert!(!result.eligible_for_quiescent_retirement);
    assert_eq!(result.blockers.len(), 1);
    assert_eq!(result.blockers[0].code, "workspace_not_active");
    assert_eq!(result.blockers[0].count, 1);
    assert!(repo
        .workspace_retirement_preflight("unknown-workspace")
        .is_err());
}

#[test]
fn quiescent_workspace_retirement_and_root_reactivation_preserve_identity_and_history() {
    let (temp, repo, original) = setup();
    let command = retirement_command(&original, "retire-once");
    let receipt = repo.retire_empty_workspace(&command).unwrap();
    assert_eq!(
        receipt.workspace.lifecycle_state,
        WorkspaceLifecycleState::Retired
    );
    assert_eq!(receipt.workspace.revision, original.revision + 1);
    assert_eq!(receipt.workspace.workspace_id, original.workspace_id);
    assert!(receipt.workspace.lead_agent_id.is_none());
    let durable = repo.get_operation(&receipt.operation_id).unwrap().unwrap();
    assert_eq!(durable.operation.phase, OperationPhase::Completed);
    assert_eq!(durable.transitions.len(), 2);
    assert!(durable.claims.is_empty());
    assert_eq!(
        durable.operation.exact_terminal_result,
        Some(serde_json::to_value(&receipt).unwrap())
    );
    assert_eq!(
        durable.transitions[1].evidence.as_ref().unwrap()["external_effects"],
        0
    );

    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let retired_operation: String = db
        .query_row(
            "SELECT retired_by_operation_id FROM retired_workspaces WHERE workspace_id=?1",
            [&original.workspace_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retired_operation, receipt.operation_id);

    // A lost HTTP response is replayable using the same principal/key/input.
    assert_eq!(
        repo.retire_empty_workspace(&retirement_command(&original, "retire-once"))
            .unwrap(),
        receipt
    );
    assert!(repo
        .retire_empty_workspace(&retirement_command(&original, "changed-key"))
        .is_err());

    let root = temp.path().join("workspace");
    let revived = register(&repo, &root, "reactivate");
    assert_eq!(revived.workspace_id, original.workspace_id);
    assert_eq!(revived.canonical_root, original.canonical_root);
    assert_eq!(revived.lifecycle_state, WorkspaceLifecycleState::Active);
    assert_eq!(revived.revision, original.revision + 2);
    assert_eq!(revived.lead_agent_id, None);
    let remaining: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM retired_workspaces WHERE workspace_id=?1",
            [&original.workspace_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
    // Repeating an old retirement key returns history, not a new retirement.
    assert_eq!(
        repo.retire_empty_workspace(&retirement_command(&original, "retire-once"))
            .unwrap(),
        receipt
    );
    assert_eq!(
        repo.get_workspace(&original.workspace_id).unwrap().unwrap(),
        revived
    );

    let second = repo
        .retire_empty_workspace(&retirement_command(&revived, "retire-again"))
        .unwrap();
    assert_eq!(second.workspace.revision, revived.revision + 1);
    assert_ne!(second.operation_id, receipt.operation_id);
}

#[test]
fn reactivation_rechecks_resource_obligations_admitted_after_retirement() {
    let (temp, repo, original) = setup();
    let retired = repo
        .retire_empty_workspace(&retirement_command(&original, "retire"))
        .unwrap()
        .workspace;
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let root = temp.path().join("workspace");
    let command = prepare_workspace_registration(
        WorkspaceRegisterRequest {
            canonical_root: root.to_string_lossy().into_owned(),
            display_name: Some("Retirement Test".into()),
        },
        OperationActor::operator("retirement-test"),
        Some("reactivate-after-settlement".into()),
    )
    .unwrap();

    // Current migration rejects new native work against a retired root.
    let late_process_sql = "INSERT INTO managed_processes
          (process_id,workspace_id,command_json,status,admission_order,queue_order,
           capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at)
         VALUES ('late-process',?1,'{}','queued',1,1,1024,'out','err',1,1)";
    assert!(db
        .execute(late_process_sql, [&original.workspace_id])
        .is_err());
    // Simulate a restored/legacy database that predates the new admission
    // trigger. Reactivation MUST still inspect live authority rather than
    // trusting the historical terminal receipt.
    db.execute_batch("DROP TRIGGER retirement_block_managed_process")
        .unwrap();
    db.execute(late_process_sql, [&original.workspace_id])
        .unwrap();
    assert!(
        format!("{}", repo.register_workspace(&command).unwrap_err())
            .contains("unsettled managed processes")
    );
    db.execute(
        "DELETE FROM managed_processes WHERE process_id='late-process'",
        [],
    )
    .unwrap();

    let mut legacy_session = sample_session();
    legacy_session.cwd = Some(original.canonical_root.clone());
    legacy_session.active_turn_id = None;
    legacy_session.worktree_id = None;
    legacy_session.status = "closed".into();
    assert!(repo.upsert_session(&legacy_session).is_err());
    db.execute_batch(
        "DROP TRIGGER retirement_block_session_create;
                      DROP TRIGGER retirement_block_session_rebind;",
    )
    .unwrap();
    repo.upsert_session(&legacy_session).unwrap();
    assert!(
        format!("{}", repo.register_workspace(&command).unwrap_err())
            .contains("unowned provider sessions")
    );
    assert_eq!(
        repo.get_workspace(&original.workspace_id).unwrap(),
        Some(retired)
    );
    assert!(repo.get_operation(&command.operation_id).unwrap().is_none());
    let tombstones: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM retired_workspaces WHERE workspace_id=?1",
            [&original.workspace_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tombstones, 1);

    legacy_session.cwd = None;
    repo.upsert_session(&legacy_session).unwrap();
    let reopened = repo.register_workspace(&command).unwrap();
    assert_eq!(reopened.workspace.workspace_id, original.workspace_id);
    assert_eq!(
        reopened.workspace.lifecycle_state,
        WorkspaceLifecycleState::Active
    );
    assert!(reopened.workspace.lead_agent_id.is_none());
}

#[test]
fn retirement_rejects_ownership_and_unsettled_obligations_without_any_state_change() {
    let (temp, repo, original) = setup();
    let path = temp.path().join("runtime.sqlite3");
    let db = open_connection(&path).unwrap();
    let mut session = sample_session();
    session.active_turn_id = None;
    session.worktree_id = None;
    session.status = "closed".into();
    repo.upsert_session(&session).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
         VALUES (?1,?2,'migration',1)",
        params![session.id, original.workspace_id],
    )
    .unwrap();
    let first = repo.retire_empty_workspace(&retirement_command(&original, "owned"));
    assert!(format!("{}", first.unwrap_err()).contains("session ownership"));
    db.execute(
        "DELETE FROM workspace_session_ownership WHERE session_id=?1",
        [&session.id],
    )
    .unwrap();

    // Pending background ops are first-class blockers even with no roster.
    db.execute(
        "INSERT INTO runtime_operations(operation_id,workspace_id,kind,actor_kind,actor_id,
                 normalized_request_hash,normalized_request_json,phase,created_at,updated_at)
         VALUES ('pending-op',?1,'external','system','worker','input','{}','manual_review',1,1)",
        [&original.workspace_id],
    )
    .unwrap();
    assert!(format!(
        "{}",
        repo.retire_empty_workspace(&retirement_command(&original, "blocked"))
            .unwrap_err()
    )
    .contains("unresolved workspace operations"));
    db.execute(
        "DELETE FROM runtime_operations WHERE operation_id='pending-op'",
        [],
    )
    .unwrap();

    assert_eq!(
        repo.get_workspace(&original.workspace_id).unwrap().unwrap(),
        original
    );
    let receipt = repo
        .retire_empty_workspace(&retirement_command(&original, "unblocked"))
        .unwrap();
    assert_eq!(
        receipt.workspace.lifecycle_state,
        WorkspaceLifecycleState::Retired
    );
}

#[test]
fn retirement_revision_cas_and_parallel_attempts_converge() {
    let (_temp, repo, original) = setup();
    let root = Arc::new(repo);
    let sync = Arc::new(Barrier::new(3));
    let tasks = (0..2)
        .map(|index| {
            let repo = Arc::clone(&root);
            let sync = Arc::clone(&sync);
            let workspace = original.clone();
            std::thread::spawn(move || {
                sync.wait();
                repo.retire_empty_workspace(&retirement_command(
                    &workspace,
                    &format!("attempt-{index}"),
                ))
            })
        })
        .collect::<Vec<_>>();
    sync.wait();
    let results = tasks
        .into_iter()
        .map(|task| task.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert_eq!(
        root.get_workspace(&original.workspace_id)
            .unwrap()
            .unwrap()
            .revision,
        original.revision + 1
    );
}

#[test]
fn running_process_pending_completion_and_messages_block_retirement_but_preserved_worktrees_do_not()
{
    let (temp, repo, original) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO managed_processes
          (process_id,workspace_id,command_json,status,admission_order,queue_order,
           capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at)
         VALUES ('process',?1,'{}','running',1,1,1024,'out','err',1,1)",
        [&original.workspace_id],
    )
    .unwrap();
    assert!(format!(
        "{}",
        repo.retire_empty_workspace(&retirement_command(&original, "running"))
            .unwrap_err()
    )
    .contains("unsettled managed processes"));
    db.execute("UPDATE managed_processes SET status='completed',completion_state='pending' WHERE process_id='process'", []).unwrap();
    assert!(repo
        .retire_empty_workspace(&retirement_command(&original, "completion"))
        .is_err());
    db.execute(
        "UPDATE managed_processes SET completion_state='delivered' WHERE process_id='process'",
        [],
    )
    .unwrap();

    db.execute(
        "INSERT INTO agent_messages
          (id,scope,context_kind,workspace_id,sender_agent_id,recipient_agent_ids_json,
           input_json,priority,policy,created_at)
         VALUES ('msg','broadcast','workspace_team',?1,'historical','[]','[]','normal','non_interrupting',1)",
        [&original.workspace_id],
    ).unwrap();
    db.execute(
        "INSERT INTO agent_deliveries
          (id,message_id,recipient_agent_id,provider,status,created_at,updated_at)
         VALUES ('delivery','msg','other','codex','pending',1,1)",
        [],
    )
    .unwrap();
    assert!(format!(
        "{}",
        repo.retire_empty_workspace(&retirement_command(&original, "messages"))
            .unwrap_err()
    )
    .contains("unfinished message deliveries"));
    db.execute(
        "UPDATE agent_deliveries SET status='injected' WHERE id='delivery'",
        [],
    )
    .unwrap();

    // The quiescent path makes no filesystem or managed-worktree changes.
    db.execute(
        "INSERT INTO managed_worktrees
         (id,repo_root,worktree_root,worktree_cwd,branch_name,worktree_name,
          unified_workspace_path,deletion_policy,created_at,updated_at)
         VALUES ('preserve',?1,'/outside','/outside/feature','feature/preserve','preserve',
                 'root','retain_on_last_claim',1,1)",
        [&original.canonical_root],
    )
    .unwrap();
    let receipt = repo
        .retire_empty_workspace(&retirement_command(&original, "settled"))
        .unwrap();
    assert_eq!(
        receipt.workspace.lifecycle_state,
        WorkspaceLifecycleState::Retired
    );
    let retained: String = db
        .query_row(
            "SELECT worktree_cwd FROM managed_worktrees WHERE id='preserve'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained, "/outside/feature");
}

#[test]
fn unproven_retirement_marker_never_reactivates() {
    let (temp, repo, original) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspaces SET lifecycle_state='retired' WHERE workspace_id=?1",
        [&original.workspace_id],
    )
    .unwrap();
    let command = prepare_workspace_registration(
        WorkspaceRegisterRequest {
            canonical_root: original.canonical_root.clone(),
            display_name: Some("Retirement Test".into()),
        },
        OperationActor::operator("retirement-test"),
        Some("new-after-fake-retirement".into()),
    )
    .unwrap();
    let result = repo.register_workspace(&command);
    assert!(format!("{}", result.unwrap_err()).contains("no proven terminal retirement"));
    assert_eq!(
        repo.get_workspace(&original.workspace_id)
            .unwrap()
            .unwrap()
            .lifecycle_state,
        WorkspaceLifecycleState::Retired
    );
}

#[test]
fn failed_identity_receipt_write_rolls_back_lifecycle_operation_and_resource_claim() {
    let (temp, repo, original) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    // Simulate failure late in the same transaction, after workspace update.
    db.execute(
        "INSERT INTO retired_workspaces(workspace_id,canonical_root,retired_at)
         VALUES (?1,?2,1)",
        params![original.workspace_id, original.canonical_root],
    )
    .unwrap();
    let command = retirement_command(&original, "transactional-failure");
    assert!(repo.retire_empty_workspace(&command).is_err());
    assert_eq!(
        repo.get_workspace(&original.workspace_id).unwrap().unwrap(),
        original
    );
    assert!(repo.get_operation(&command.operation_id).unwrap().is_none());
    let claim_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE resource_id=?1",
            [&original.workspace_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claim_count, 0);
    db.execute(
        "DELETE FROM retired_workspaces WHERE workspace_id=?1",
        [&original.workspace_id],
    )
    .unwrap();
    assert!(repo.retire_empty_workspace(&command).is_ok());
}

#[test]
fn reactivation_failure_preserves_retired_state_and_tombstone_atomically() {
    let (temp, repo, original) = setup();
    let retired = repo
        .retire_empty_workspace(&retirement_command(&original, "retire"))
        .unwrap()
        .workspace;
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_reactivation BEFORE DELETE ON retired_workspaces
         BEGIN SELECT RAISE(ABORT, 'injected tombstone removal failure'); END;",
    )
    .unwrap();
    let register_command = prepare_workspace_registration(
        WorkspaceRegisterRequest {
            canonical_root: original.canonical_root.clone(),
            display_name: Some("Retirement Test".into()),
        },
        OperationActor::operator("retirement-test"),
        Some("reactivation-failpoint".into()),
    )
    .unwrap();
    assert!(repo.register_workspace(&register_command).is_err());
    assert_eq!(
        repo.get_workspace(&original.workspace_id).unwrap().unwrap(),
        retired
    );
    assert!(repo
        .get_operation(&register_command.operation_id)
        .unwrap()
        .is_none());
    db.execute_batch("DROP TRIGGER fail_reactivation").unwrap();
    let recovered = repo.register_workspace(&register_command).unwrap();
    assert_eq!(recovered.workspace.workspace_id, original.workspace_id);
    assert_eq!(
        recovered.workspace.lifecycle_state,
        WorkspaceLifecycleState::Active
    );
}

#[test]
fn empty_roster_does_not_hide_unowned_legacy_provider_activity_in_the_same_root() {
    let (_temp, repo, original) = setup();
    let mut legacy = sample_session();
    legacy.cwd = Some(original.canonical_root.clone());
    legacy.active_turn_id = None;
    legacy.worktree_id = None;
    repo.upsert_session(&legacy).unwrap();
    let result = repo.retire_empty_workspace(&retirement_command(&original, "unmapped"));
    assert!(format!("{}", result.unwrap_err()).contains("unowned provider session"));
    assert_eq!(
        repo.get_workspace(&original.workspace_id).unwrap().unwrap(),
        original
    );

    legacy.cwd = Some("/somewhere-else".into());
    repo.upsert_session(&legacy).unwrap();
    assert!(repo
        .retire_empty_workspace(&retirement_command(&original, "reconciled"))
        .is_ok());
}
