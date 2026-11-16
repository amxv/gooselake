use super::workspace_agent::apply_schema_through;
use super::*;
use runtime_core::{
    ManagedWorktreeCleanupAdmission, ManagedWorktreeCleanupCommand, OperationPhase, RuntimeError,
    WorktreeCleanupResponse,
};

fn worktree() -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: "wt_cleanup".into(),
        repo_root: "/disposable/git-root".into(),
        worktree_root: "/disposable/worktrees".into(),
        worktree_cwd: "/disposable/worktrees/feature".into(),
        branch_name: "gg/cleanup".into(),
        worktree_name: "cleanup".into(),
        unified_workspace_path: "git-root".into(),
        deletion_policy: "delete_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 10,
        updated_at: 10,
    }
}

fn fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    ManagedWorktreeCleanupCommand,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    repository.upsert_session(&sample_session()).unwrap();
    let worktree = worktree();
    repository.upsert_managed_worktree(&worktree).unwrap();
    (
        temp,
        repository,
        ManagedWorktreeCleanupCommand {
            operation_id: "op_cleanup_1".into(),
            worktree,
            reason: Some("test_release".into()),
            requested_at: 100,
        },
    )
}

fn success() -> WorktreeCleanupResponse {
    WorktreeCleanupResponse {
        worktree_id: "wt_cleanup".into(),
        status: "deleted".into(),
        deletion_policy: "delete_on_last_claim".into(),
        active_claim_count: 0,
        worktree_path_deleted: true,
        branch_deleted: true,
        diagnostics: Vec::new(),
    }
}

#[test]
fn cleanup_admission_rejects_later_live_nested_cwd_binding() {
    let (temp, repository, command) = fixture();
    // Another SQLite process binds a live provider session to a descendant of
    // the checkout without using the optional worktree_id column. Native Git
    // must not be admitted for cleanup after this binding.
    let writer = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    writer
        .upsert_session(&SessionRecord {
            id: "nested_cleanup_session".into(),
            cwd: Some(format!("{}/nested/source", command.worktree.worktree_cwd)),
            worktree_id: None,
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
    let result = repository.admit_managed_worktree_cleanup(&command);
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "nested live session must block native cleanup admission: {result:?}"
    );
    assert!(repository
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
    assert!(repository
        .replay_managed_worktree_cleanup(&command.worktree)
        .unwrap()
        .is_none());
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn cleanup_admission_does_not_confuse_sibling_cwd_prefix_with_native_binding() {
    let (temp, repository, command) = fixture();
    let writer = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    writer
        .upsert_session(&SessionRecord {
            id: "sibling_cleanup_session".into(),
            cwd: Some(format!("{}-other/subdir", command.worktree.worktree_cwd)),
            worktree_id: None,
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
    assert!(matches!(
        repository.admit_managed_worktree_cleanup(&command).unwrap(),
        ManagedWorktreeCleanupAdmission::Execute { .. }
    ));
    assert_eq!(
        repository
            .list_runtime_events(
                Some((RuntimeEventScope::Worktree, &command.worktree.id)),
                None,
                10,
            )
            .unwrap()
            .len(),
        1
    );
}

const CLEANUP_CHILD_DB: &str = "GOOSELAKE_NATIVE_CLEANUP_OS_CHILD_DB";

// The real test executable is spawned by the parent test below. In ordinary
// `cargo test` this function does nothing and cannot touch a user's database.
#[test]
fn child_os_writer_observes_postadmission_checkout_binding_fence() {
    let Some(path) = std::env::var_os(CLEANUP_CHILD_DB) else {
        return;
    };
    let writer = SqliteRuntimeRepository::new(std::path::PathBuf::from(path));
    let checkout = worktree().worktree_cwd;
    let bound = SessionRecord {
        id: "os_child_live_nested".into(),
        worktree_id: None,
        cwd: Some(format!("{checkout}/nested/source")),
        status: "ready".into(),
        ..sample_session()
    };
    let error = writer
        .upsert_session(&bound)
        .expect_err("independent OS writer cannot bind to an admitted native checkout");
    assert!(
        error
            .to_string()
            .contains("managed worktree native cleanup fences session binding"),
        "the native cleanup trigger, not another constraint, must refuse: {error}"
    );

    let closed = SessionRecord {
        id: "os_child_closed_nested".into(),
        status: "closed".into(),
        ..bound.clone()
    };
    writer.upsert_session(&closed).unwrap();
    let sibling = SessionRecord {
        id: "os_child_live_sibling".into(),
        cwd: Some(format!("{checkout}-other/nested/source")),
        ..bound
    };
    writer.upsert_session(&sibling).unwrap();
}

#[test]
fn admitted_native_cleanup_fences_later_nested_cwd_from_independent_os_process() {
    let (temp, repository, command) = fixture();
    assert!(matches!(
        repository.admit_managed_worktree_cleanup(&command).unwrap(),
        ManagedWorktreeCleanupAdmission::Execute { .. }
    ));
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::worktree_cleanup_receipts::child_os_writer_observes_postadmission_checkout_binding_fence",
            "--nocapture",
        ])
        .env(CLEANUP_CHILD_DB, temp.path().join("runtime.sqlite3"))
        .output()
        .expect("spawn independent OS process with only disposable SQLite");
    assert!(
        output.status.success(),
        "child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
        "child fixture must actually execute its exact test: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    let sessions = repository.hydrate_runtime_state().unwrap().sessions;
    assert!(!sessions
        .iter()
        .any(|session| session.id == "os_child_live_nested"));
    assert!(sessions
        .iter()
        .any(|session| session.id == "os_child_closed_nested" && session.status == "closed"));
    assert!(sessions
        .iter()
        .any(|session| session.id == "os_child_live_sibling" && session.status == "ready"));
    assert!(matches!(
        repository
            .replay_managed_worktree_cleanup(&command.worktree)
            .unwrap(),
        Some(ManagedWorktreeCleanupAdmission::Unresolved { operation_id })
            if operation_id == command.operation_id
    ));
    assert_eq!(
        repository
            .list_runtime_events(
                Some((RuntimeEventScope::Worktree, &command.worktree.id)),
                None,
                10,
            )
            .unwrap()
            .len(),
        1,
        "late rejected binding must not forge another native event"
    );
}

#[test]
fn cleanup_admission_fault_rolls_back_intent_journal_and_git_claims() {
    let (temp, repository, command) = fixture();
    let subscriber = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let mut rx = subscriber.subscribe_event_commits().unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER reject_worktree_cleanup_intent
        BEFORE INSERT ON runtime_events WHEN NEW.kind='worktree.cleanup_admitted'
        BEGIN SELECT RAISE(FAIL, 'injected cleanup intent failure'); END;",
    )
    .unwrap();
    assert!(repository.admit_managed_worktree_cleanup(&command).is_err());
    assert!(repository
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
    assert!(repository
        .replay_managed_worktree_cleanup(&command.worktree)
        .unwrap()
        .is_none());
    assert!(repository
        .list_runtime_events(None, None, 100)
        .unwrap()
        .is_empty());
    assert_eq!(
        repository
            .native_worktree_cleanup_owner(
                &command.worktree.repo_root,
                &command.worktree.worktree_cwd,
                &command.worktree.branch_name
            )
            .unwrap(),
        None
    );
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    db.execute_batch("DROP TRIGGER reject_worktree_cleanup_intent;")
        .unwrap();
    assert!(matches!(
        repository.admit_managed_worktree_cleanup(&command).unwrap(),
        ManagedWorktreeCleanupAdmission::Execute { .. }
    ));
    assert!(rx.try_recv().is_ok());
    assert_eq!(
        repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap()
            .claims
            .len(),
        2
    );
    let new_instance = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert!(
        matches!(new_instance.replay_managed_worktree_cleanup(&command.worktree).unwrap(),
        Some(ManagedWorktreeCleanupAdmission::Unresolved { operation_id }) if operation_id == command.operation_id)
    );
    assert!(matches!(
        new_instance
            .admit_managed_worktree_cleanup(&command)
            .unwrap(),
        ManagedWorktreeCleanupAdmission::Unresolved { .. }
    ));
    assert_eq!(
        new_instance
            .native_worktree_cleanup_owner(
                &command.worktree.repo_root,
                &command.worktree.worktree_cwd,
                &command.worktree.branch_name
            )
            .unwrap()
            .as_deref(),
        Some(command.operation_id.as_str())
    );

    // Database triggers also forbid unrelated writers from binding an agent
    // after cleanup admission, independent of service-local Git mutexes.
    assert!(new_instance
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: command.worktree.id.clone(),
            session_id: "session_1".into(),
            claim_role: "owner".into(),
            created_at: 200,
            released_at: None,
        })
        .is_err());
    assert!(new_instance
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims
        .is_empty());

    // A legacy writer may try to bind a session directly, without first
    // acquiring a managed claim. The native cleanup fence covers both the
    // worktree ID and descendants of the admitted native checkout path.
    let mut bound = sample_session();
    bound.id = "session_fenced".into();
    bound.worktree_id = Some(command.worktree.id.clone());
    bound.cwd = None;
    assert!(new_instance.upsert_session(&bound).is_err());
    bound.worktree_id = None;
    bound.cwd = Some(format!("{}/nested", command.worktree.worktree_cwd));
    assert!(new_instance.upsert_session(&bound).is_err());
    let mut old = sample_session();
    old.cwd = Some(command.worktree.worktree_cwd.clone());
    assert!(new_instance.upsert_session(&old).is_err());
    old.status = "closed".into();
    new_instance.upsert_session(&old).unwrap();

    let mut changed_identity = command.worktree.clone();
    changed_identity.created_at += 1;
    assert!(new_instance
        .upsert_managed_worktree(&changed_identity)
        .is_err());
    assert_eq!(
        new_instance
            .get_managed_worktree(&command.worktree.id)
            .unwrap(),
        Some(command.worktree.clone())
    );
}

#[test]
fn failed_terminal_event_commit_preserves_intent_and_reconciles_once_after_restart() {
    let (temp, repository, command) = fixture();
    repository.admit_managed_worktree_cleanup(&command).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER reject_worktree_cleanup_receipt
        BEFORE INSERT ON runtime_events WHEN NEW.kind='worktree.cleaned_up'
        BEGIN SELECT RAISE(FAIL, 'injected native cleanup terminal failure'); END;",
    )
    .unwrap();
    assert!(repository
        .complete_managed_worktree_cleanup(&command.operation_id, &success(), 200)
        .is_err());
    let pending = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(pending.operation.phase, OperationPhase::Requested);
    assert_eq!(pending.claims.len(), 2);
    assert_eq!(pending.effects[0].phase, "intended");
    assert!(matches!(
        repository
            .replay_managed_worktree_cleanup(&command.worktree)
            .unwrap(),
        Some(ManagedWorktreeCleanupAdmission::Unresolved { .. })
    ));
    assert_eq!(
        repository
            .list_runtime_events(Some((RuntimeEventScope::Worktree, "wt_cleanup")), None, 10)
            .unwrap()
            .len(),
        1
    );

    db.execute_batch("DROP TRIGGER reject_worktree_cleanup_receipt;")
        .unwrap();
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    restarted
        .complete_managed_worktree_cleanup(&command.operation_id, &success(), 300)
        .unwrap();
    let terminal = restarted
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(terminal.operation.phase, OperationPhase::Completed);
    assert_eq!(terminal.claims.len(), 0);
    assert_eq!(terminal.effects[0].phase, "finalized");
    let replayed = restarted
        .replay_managed_worktree_cleanup(&command.worktree)
        .unwrap();
    assert!(
        matches!(replayed, Some(ManagedWorktreeCleanupAdmission::Replay { operation_id, response })
        if operation_id == command.operation_id && response.status == "deleted" && response.branch_deleted)
    );
    assert_eq!(
        restarted
            .native_worktree_cleanup_owner(
                &command.worktree.repo_root,
                &command.worktree.worktree_cwd,
                &command.worktree.branch_name
            )
            .unwrap(),
        None
    );
    let worktree_events = restarted
        .list_runtime_events(Some((RuntimeEventScope::Worktree, "wt_cleanup")), None, 10)
        .unwrap();
    assert_eq!(
        worktree_events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        ["worktree.cleanup_admitted", "worktree.cleaned_up"]
    );
    assert_eq!(
        worktree_events[0].event_id,
        format!("{}:event:worktree:admitted", command.operation_id)
    );
    assert_eq!(
        worktree_events[1].event_id,
        format!("{}:event:worktree:settled", command.operation_id)
    );
    assert_eq!(worktree_events[0].seq, 1);
    assert_eq!(worktree_events[1].seq, 2);
    assert!(restarted
        .complete_managed_worktree_cleanup(&command.operation_id, &success(), 400)
        .is_err());
    assert_eq!(
        restarted
            .list_runtime_events(Some((RuntimeEventScope::Worktree, "wt_cleanup")), None, 10)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn partial_git_removal_is_manual_review_until_independent_absence_evidence() {
    let (temp, repository, command) = fixture();
    repository.admit_managed_worktree_cleanup(&command).unwrap();
    let partial = WorktreeCleanupResponse {
        status: "cleanup_failed".into(),
        worktree_path_deleted: true,
        branch_deleted: false,
        diagnostics: vec!["branch has unmerged history".into()],
        ..success()
    };
    repository
        .complete_managed_worktree_cleanup(&command.operation_id, &partial, 200)
        .unwrap();
    let review = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(review.operation.phase, OperationPhase::ManualReview);
    assert_eq!(review.claims.len(), 2);
    assert_eq!(review.effects[0].phase, "uncertain");
    assert!(repository
        .complete_managed_worktree_cleanup(&command.operation_id, &partial, 300)
        .is_err());
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert!(matches!(
        restarted
            .replay_managed_worktree_cleanup(&command.worktree)
            .unwrap(),
        Some(ManagedWorktreeCleanupAdmission::Unresolved { .. })
    ));
    // Only the service's independent Git inspection may supply this final
    // success receipt; SQLite never infers removal from a partial response.
    restarted
        .complete_managed_worktree_cleanup(&command.operation_id, &success(), 400)
        .unwrap();
    let final_state = restarted
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(final_state.operation.phase, OperationPhase::Completed);
    assert!(final_state.claims.is_empty());
    assert_eq!(
        final_state
            .transitions
            .iter()
            .map(|step| step.to_phase)
            .collect::<Vec<_>>(),
        [
            OperationPhase::Requested,
            OperationPhase::ManualReview,
            OperationPhase::Completed
        ]
    );
    let kinds = restarted
        .list_runtime_events(Some((RuntimeEventScope::Worktree, "wt_cleanup")), None, 10)
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            "worktree.cleanup_admitted",
            "worktree.cleanup_failed",
            "worktree.cleaned_up"
        ]
    );
}

#[test]
fn schema_upgrade_from_27_retains_legacy_worktrees_claims_and_events() {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&database_path, 27);
    let repository = SqliteRuntimeRepository::new(database_path.clone());
    repository.upsert_session(&sample_session()).unwrap();
    let worktree = worktree();
    repository.upsert_managed_worktree(&worktree).unwrap();
    let legacy_claim = ManagedWorktreeClaimRecord {
        worktree_id: worktree.id.clone(),
        session_id: "session_1".into(),
        claim_role: "owner".into(),
        created_at: 30,
        released_at: Some(40),
    };
    repository
        .upsert_managed_worktree_claim(&legacy_claim)
        .unwrap();
    repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_pre_upgrade".into(),
            scope: RuntimeEventScope::Worktree,
            scope_id: worktree.id.clone(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "worktree.legacy".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"legacy":true}),
            provider: None,
            provider_seq: None,
            created_at: 40,
        })
        .unwrap();
    repository.initialize_schema().unwrap();
    assert_eq!(
        repository.get_managed_worktree(&worktree.id).unwrap(),
        Some(worktree.clone())
    );
    assert_eq!(
        repository
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        [legacy_claim]
    );
    assert_eq!(
        repository
            .list_runtime_events(Some((RuntimeEventScope::Worktree, "wt_cleanup")), None, 10)
            .unwrap()
            .len(),
        1
    );
    let db = Connection::open(&database_path).unwrap();
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert!(matches!(
        repository
            .admit_managed_worktree_cleanup(&ManagedWorktreeCleanupCommand {
                operation_id: "op_upgrade_cleanup".into(),
                worktree,
                reason: None,
                requested_at: 100,
            })
            .unwrap(),
        ManagedWorktreeCleanupAdmission::Execute { .. }
    ));
}
