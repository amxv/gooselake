use std::path::Path;
use std::process::Command;

use super::*;
use runtime_core::{
    plan_legacy_workspace_migration, prepare_legacy_workspace_migration_apply,
    prepare_legacy_workspace_migration_resolution, LegacyWorkspaceMigrationClassification,
    LegacyWorkspaceMigrationResolutionAction, LegacyWorkspaceMigrationResolutionRequest,
    LegacyWorkspaceMigrationSubjectKind, OperationActor, SessionRecord, TeamMemberRecord,
    TeamRecord,
};

#[test]
fn deterministic_preview_apply_preserves_legacy_rows_and_converges() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let git_root = temp_dir.path().join("repo");
    init_git_repo(&git_root);

    let session = migration_session("session_mapped", Some(&git_root));
    repository.upsert_session(&session).expect("session");
    repository
        .upsert_team(&TeamRecord {
            id: "team_mapped".to_string(),
            name: "Display name is not authority".to_string(),
            lead_agent_id: session.id.clone(),
            created_by: "user".to_string(),
            created_at: 11,
            updated_at: 11,
            deleted_at: None,
        })
        .expect("team");
    repository
        .upsert_team_member(&TeamMemberRecord {
            team_id: "team_mapped".to_string(),
            agent_id: session.id.clone(),
            title: Some("legacy title".to_string()),
            joined_at: 12,
            added_by: "user".to_string(),
            creator_agent_id: None,
            creator_compaction_subscription: "auto".to_string(),
            worktree_id: None,
        })
        .expect("member");

    let legacy_before = repository.hydrate_runtime_state().expect("legacy before");
    let preview = plan_legacy_workspace_migration(
        &legacy_before,
        &repository.list_workspaces().expect("workspaces"),
    );
    assert_eq!(preview.unresolved_subjects, 0);
    let persisted = repository
        .persist_workspace_migration_preview(&preview)
        .expect("persist preview");
    assert_eq!(persisted.mapped_subjects, 3);
    assert!(!persisted.cutover_blocked);

    let command = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("apply-once".to_string()),
    )
    .expect("apply command");
    let first = repository
        .apply_workspace_migration(&command)
        .expect("apply migration");
    assert_eq!(first.created_workspaces, 1);
    assert_eq!(first.mapped_subjects_applied, 3);
    assert_eq!(first.owned_sessions_created, 1);
    assert_eq!(first.unresolved_subjects, 0);
    assert!(!first.cutover_blocked);

    assert_eq!(
        repository.hydrate_runtime_state().expect("legacy after"),
        legacy_before,
        "new authority must not rewrite generation-one rows"
    );
    let connection = open_connection(&repository.database_path).expect("connection");
    let workspace_id: String = connection
        .query_row(
            "SELECT workspace_id FROM workspace_session_ownership WHERE session_id = ?1",
            params![session.id],
            |row| row.get(0),
        )
        .expect("ownership");
    let profile_workspace_id: String = connection
        .query_row(
            "SELECT workspace_id FROM workspace_agent_profiles WHERE session_id = ?1",
            params![session.id],
            |row| row.get(0),
        )
        .expect("profile");
    assert_eq!(profile_workspace_id, workspace_id);
    assert!(connection
        .execute(
            "UPDATE workspace_session_ownership SET workspace_id = 'workspace_other' WHERE session_id = ?1",
            params![session.id],
        )
        .is_err());
    drop(connection);

    let replay_command = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("apply-once".to_string()),
    )
    .expect("replay command");
    assert_eq!(
        repository
            .apply_workspace_migration(&replay_command)
            .expect("exact replay"),
        first
    );

    let rerun = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("apply-again".to_string()),
    )
    .expect("rerun command");
    let rerun_result = repository
        .apply_workspace_migration(&rerun)
        .expect("rerun migration");
    assert_eq!(rerun_result.created_workspaces, 0);
    assert_eq!(rerun_result.mapped_subjects_applied, 0);
    assert_eq!(rerun_result.owned_sessions_created, 0);

    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    let restart_rerun = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("apply-after-restart".to_string()),
    )
    .expect("restart command");
    let restart_result = reopened
        .apply_workspace_migration(&restart_rerun)
        .expect("restart rerun");
    assert_eq!(restart_result.created_workspaces, 0);
    assert_eq!(restart_result.owned_sessions_created, 0);
}

#[test]
fn unresolved_sessions_require_explicit_map_or_archive_and_survive_preview_refresh() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let git_root = temp_dir.path().join("repo");
    init_git_repo(&git_root);

    let mapped = migration_session("mapped", Some(&git_root));
    let orphan_map = migration_session("orphan_map", None);
    let orphan_archive = migration_session("orphan_archive", None);
    for session in [&mapped, &orphan_map, &orphan_archive] {
        repository.upsert_session(session).expect("session");
    }

    let state = repository.hydrate_runtime_state().expect("hydrate");
    let preview = plan_legacy_workspace_migration(&state, &[]);
    let persisted = repository
        .persist_workspace_migration_preview(&preview)
        .expect("preview");
    assert_eq!(persisted.unresolved_active_sessions, 2);
    assert!(persisted.cutover_blocked);

    let apply = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("apply-partial".to_string()),
    )
    .expect("apply");
    let applied = repository
        .apply_workspace_migration(&apply)
        .expect("partial apply");
    assert_eq!(applied.owned_sessions_created, 1);
    assert_eq!(applied.unresolved_subjects, 2);
    assert!(applied.cutover_blocked);
    let workspace = repository
        .list_workspaces()
        .expect("workspaces")
        .into_iter()
        .next()
        .expect("mapped workspace");

    let connection = open_connection(&repository.database_path).expect("connection");
    let orphan_ownerships: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM workspace_session_ownership WHERE session_id IN ('orphan_map', 'orphan_archive')",
            [],
            |row| row.get(0),
        )
        .expect("orphan ownership count");
    assert_eq!(
        orphan_ownerships, 0,
        "unresolved rows must receive zero guessed authority"
    );
    drop(connection);

    let map_command = prepare_legacy_workspace_migration_resolution(
        LegacyWorkspaceMigrationSubjectKind::Session,
        orphan_map.id.clone(),
        LegacyWorkspaceMigrationResolutionRequest {
            action: LegacyWorkspaceMigrationResolutionAction::Map,
            workspace_id: Some(workspace.workspace_id.clone()),
        },
        OperationActor::operator("migration-test"),
        Some("resolve-map".to_string()),
    )
    .expect("map command");
    let mapped_resolution = repository
        .resolve_workspace_migration_subject(&map_command)
        .expect("map resolution");
    assert_eq!(
        mapped_resolution.subject.classification,
        LegacyWorkspaceMigrationClassification::Mapped
    );
    assert_eq!(
        mapped_resolution.subject.workspace_id.as_deref(),
        Some(workspace.workspace_id.as_str())
    );

    let archive_command = prepare_legacy_workspace_migration_resolution(
        LegacyWorkspaceMigrationSubjectKind::Session,
        orphan_archive.id.clone(),
        LegacyWorkspaceMigrationResolutionRequest {
            action: LegacyWorkspaceMigrationResolutionAction::Archive,
            workspace_id: None,
        },
        OperationActor::operator("migration-test"),
        Some("resolve-archive".to_string()),
    )
    .expect("archive command");
    let archived = repository
        .resolve_workspace_migration_subject(&archive_command)
        .expect("archive resolution");
    assert_eq!(
        archived.subject.classification,
        LegacyWorkspaceMigrationClassification::ArchivedHistory
    );

    let refreshed_plan = plan_legacy_workspace_migration(
        &repository.hydrate_runtime_state().expect("hydrate refresh"),
        &repository.list_workspaces().expect("workspaces refresh"),
    );
    let refreshed = repository
        .persist_workspace_migration_preview(&refreshed_plan)
        .expect("refresh preview");
    assert_eq!(refreshed.unresolved_subjects, 0);
    assert!(!refreshed.cutover_blocked);
    assert_eq!(
        find_subject(
            &refreshed,
            LegacyWorkspaceMigrationSubjectKind::Session,
            "orphan_map"
        )
        .classification,
        LegacyWorkspaceMigrationClassification::Mapped
    );
    assert_eq!(
        find_subject(
            &refreshed,
            LegacyWorkspaceMigrationSubjectKind::Session,
            "orphan_archive"
        )
        .classification,
        LegacyWorkspaceMigrationClassification::ArchivedHistory
    );
}

#[test]
fn failed_apply_rolls_back_authority_and_retry_converges() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let git_root = temp_dir.path().join("repo");
    init_git_repo(&git_root);
    repository
        .upsert_session(&migration_session("session_retry", Some(&git_root)))
        .expect("session");
    let plan =
        plan_legacy_workspace_migration(&repository.hydrate_runtime_state().expect("hydrate"), &[]);
    repository
        .persist_workspace_migration_preview(&plan)
        .expect("preview");

    let connection = open_connection(&repository.database_path).expect("connection");
    connection
        .execute_batch(
            "CREATE TRIGGER fail_migrated_profile
             BEFORE INSERT ON workspace_agent_profiles
             BEGIN
               SELECT RAISE(ABORT, 'forced profile failure');
             END;",
        )
        .expect("failure trigger");
    drop(connection);

    let command = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("retryable-apply".to_string()),
    )
    .expect("apply command");
    assert!(repository.apply_workspace_migration(&command).is_err());

    let connection = open_connection(&repository.database_path).expect("after failure");
    for table in [
        "workspaces",
        "workspace_session_ownership",
        "workspace_agent_profiles",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 0, "{table} must roll back atomically");
    }
    let operation_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE kind = 'legacy_workspace_migration_apply'",
            [],
            |row| row.get(0),
        )
        .expect("operation count");
    assert_eq!(operation_count, 0);
    connection
        .execute_batch("DROP TRIGGER fail_migrated_profile")
        .expect("remove trigger");
    drop(connection);

    let retry = repository
        .apply_workspace_migration(&command)
        .expect("retry succeeds");
    assert_eq!(retry.created_workspaces, 1);
    assert_eq!(retry.owned_sessions_created, 1);
}

#[test]
fn apply_requires_an_explicit_preview_and_empty_preview_is_a_valid_noop() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");

    let before_preview = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("before-preview".to_string()),
    )
    .expect("apply command");
    assert!(repository
        .apply_workspace_migration(&before_preview)
        .is_err());

    let preview = plan_legacy_workspace_migration(
        &repository.hydrate_runtime_state().expect("hydrate"),
        &repository.list_workspaces().expect("workspaces"),
    );
    assert!(preview.subjects.is_empty());
    assert!(!preview.cutover_blocked);
    repository
        .persist_workspace_migration_preview(&preview)
        .expect("persist empty preview");

    let after_preview = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("after-preview".to_string()),
    )
    .expect("apply command");
    let result = repository
        .apply_workspace_migration(&after_preview)
        .expect("empty apply succeeds");
    assert_eq!(result.created_workspaces, 0);
    assert_eq!(result.mapped_subjects_applied, 0);
    assert_eq!(result.archived_subjects_applied, 0);
    assert_eq!(result.owned_sessions_created, 0);
    assert_eq!(result.unresolved_subjects, 0);
    assert!(!result.cutover_blocked);
}

#[test]
fn repository_identity_change_after_preview_blocks_apply_without_partial_authority() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let git_root = temp_dir.path().join("repo");
    let moved_root = temp_dir.path().join("repo-moved");
    init_git_repo(&git_root);
    repository
        .upsert_session(&migration_session("session_stale", Some(&git_root)))
        .expect("session");
    let preview = plan_legacy_workspace_migration(
        &repository.hydrate_runtime_state().expect("hydrate"),
        &repository.list_workspaces().expect("workspaces"),
    );
    repository
        .persist_workspace_migration_preview(&preview)
        .expect("preview");

    std::fs::rename(&git_root, &moved_root).expect("move repository after preview");
    let command = prepare_legacy_workspace_migration_apply(
        OperationActor::operator("migration-test"),
        Some("stale-repository".to_string()),
    )
    .expect("apply command");
    assert!(repository.apply_workspace_migration(&command).is_err());

    let connection = open_connection(&repository.database_path).expect("connection");
    let workspaces: i64 = connection
        .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
        .expect("workspace count");
    let ownerships: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM workspace_session_ownership",
            [],
            |row| row.get(0),
        )
        .expect("ownership count");
    assert_eq!(workspaces, 0);
    assert_eq!(ownerships, 0);
    drop(connection);

    std::fs::rename(&moved_root, &git_root).expect("restore repository");
    let retry = repository
        .apply_workspace_migration(&command)
        .expect("retry after restoring evidence");
    assert_eq!(retry.created_workspaces, 1);
    assert_eq!(retry.owned_sessions_created, 1);
}

fn migration_session(id: &str, cwd: Option<&Path>) -> SessionRecord {
    SessionRecord {
        id: id.to_string(),
        provider: "codex".to_string(),
        status: "active".to_string(),
        cwd: cwd.map(|path| path.to_string_lossy().to_string()),
        model: Some("test-model".to_string()),
        permission_mode: None,
        system_prompt: None,
        metadata: serde_json::json!({}),
        provider_session_ref: None,
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 10,
        updated_at: 10,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

fn init_git_repo(root: &Path) {
    std::fs::create_dir_all(root).expect("repo dir");
    run_git(root, &["init"]);
    run_git(root, &["config", "user.email", "runtime@example.invalid"]);
    run_git(root, &["config", "user.name", "Runtime Test"]);
    std::fs::write(root.join("README.md"), "migration fixture\n").expect("seed");
    run_git(root, &["add", "."]);
    run_git(root, &["commit", "-m", "seed"]);
}

fn run_git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn find_subject<'a>(
    status: &'a runtime_core::LegacyWorkspaceMigrationStatus,
    kind: LegacyWorkspaceMigrationSubjectKind,
    id: &str,
) -> &'a runtime_core::LegacyWorkspaceMigrationSubject {
    status
        .subjects
        .iter()
        .find(|subject| subject.subject_kind == kind && subject.subject_id == id)
        .expect("subject")
}
