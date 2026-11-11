use super::*;
use runtime_core::{
    prepare_workspace_registration, OperationActor, OperationPhase, RuntimeEventScope,
    WorkspaceRegisterRequest, WorkspaceWorktreeCreateRequest, WorktreeClaimRequest,
    WorktreeCleanupRequest, WorktreeService,
};
use std::path::{Path, PathBuf};
use std::process::Command;

fn git(root: &Path, args: &[&str]) -> (bool, String) {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
    )
}

struct Fixture {
    temp: tempfile::TempDir,
    root: PathBuf,
    store: Arc<SqliteRuntimeStore>,
    service: Arc<RuntimeWorktreeService>,
    worktree: ManagedWorktreeRecord,
    workspace_id: String,
}

async fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("repository");
    std::fs::create_dir_all(&root).unwrap();
    assert!(git(&root, &["init", "--quiet"]).0);
    assert!(
        git(
            &root,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "--allow-empty",
                "--quiet",
                "-m",
                "Initial",
            ]
        )
        .0
    );
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.unwrap();
    let (runtime, comms) = build_runtime_and_team_comms(store.clone()).await;
    let workspace_id = store
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.display().to_string(),
                    display_name: Some("Fixture".into()),
                },
                OperationActor::operator("disposable-worktree-test"),
                Some("register-fixture".into()),
            )
            .unwrap(),
        )
        .unwrap()
        .workspace
        .workspace_id;
    let service = RuntimeWorktreeService::new(
        store.clone(),
        runtime,
        comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp.path().join("managed").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "retain_on_last_claim".into(),
        },
    )
    .unwrap();
    let worktree = service
        .create_workspace_worktree(WorkspaceWorktreeCreateRequest {
            workspace_id: workspace_id.clone(),
            worktree_name: "recoverable".into(),
            branch_prefix: None,
            base_ref: None,
            deletion_policy: Some("delete_on_last_claim".into()),
            run_init_script: Some(false),
        })
        .await
        .unwrap()
        .worktree;
    assert!(Path::new(&worktree.worktree_cwd).is_dir());
    assert!(
        git(
            &root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", worktree.branch_name)
            ]
        )
        .0
    );
    Fixture {
        temp,
        root,
        store,
        service,
        worktree,
        workspace_id,
    }
}

fn request(worktree_id: &str) -> WorktreeCleanupRequest {
    WorktreeCleanupRequest {
        worktree_id: worktree_id.into(),
        reason: Some("disposable_test".into()),
    }
}

fn cleanup_operation(store: &SqliteRuntimeStore) -> String {
    store
        .repository()
        .list_operations_page(None, None, 101)
        .unwrap()
        .into_iter()
        .find(|op| op.kind == "managed_worktree_cleanup")
        .expect("canonical native cleanup operation")
        .operation_id
}

#[tokio::test]
async fn native_worktree_cleanup_is_exactly_receipted_and_replayed_across_restart() {
    let fixture = fixture().await;
    // A historical released claim must survive cleanup. Startup repair
    // re-upserts this released history; the database fence must reject only
    // new active claims, not recovery of already-released evidence.
    let independent_session =
        create_test_session(&fixture.service.runtime, fixture.root.to_str().unwrap()).await;
    fixture
        .store
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: fixture.worktree.id.clone(),
            session_id: independent_session.id.clone(),
            claim_role: "owner".into(),
            created_at: 10,
            released_at: Some(20),
        })
        .unwrap();
    let original = fixture
        .service
        .cleanup_worktree(request(&fixture.worktree.id))
        .await
        .unwrap();
    assert_eq!(original.status, "deleted");
    assert!(original.worktree_path_deleted && original.branch_deleted);
    let rejected_claim = fixture
        .service
        .claim_worktree(WorktreeClaimRequest {
            worktree_id: fixture.worktree.id.clone(),
            session_id: independent_session.id.clone(),
            claim_role: "owner".into(),
        })
        .await;
    assert!(
        matches!(rejected_claim, Err(RuntimeError::Conflict(_))),
        "completed cleanup cannot resurrect a stale claim: {rejected_claim:?}"
    );
    assert!(!Path::new(&fixture.worktree.worktree_cwd).exists());
    assert!(
        !git(
            &fixture.root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", fixture.worktree.branch_name)
            ]
        )
        .0
    );
    let operation_id = cleanup_operation(&fixture.store);
    let operation = fixture
        .store
        .repository()
        .get_operation(&operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(operation.operation.phase, OperationPhase::Completed);
    assert!(operation.claims.is_empty());
    assert_eq!(operation.effects[0].phase, "finalized");

    let restarted = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: fixture.temp.path().join("runtime.sqlite3"),
    }));
    restarted.initialize().await.unwrap();
    let (runtime, comms) = build_runtime_and_team_comms(restarted.clone()).await;
    let service = RuntimeWorktreeService::new(
        restarted.clone(),
        runtime,
        comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: fixture.temp.path().join("managed").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "retain_on_last_claim".into(),
        },
    )
    .unwrap();
    assert_eq!(
        restarted
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims
            .iter()
            .filter(
                |claim| claim.session_id == independent_session.id && claim.released_at == Some(20)
            )
            .count(),
        1
    );
    let replayed = service
        .cleanup_worktree(request(&fixture.worktree.id))
        .await
        .unwrap();
    assert_eq!(replayed.status, "deleted");
    assert_eq!(
        replayed.worktree_path_deleted,
        original.worktree_path_deleted
    );
    let lifecycle = restarted
        .list_runtime_events(
            Some((RuntimeEventScope::Worktree, &fixture.worktree.id)),
            None,
            30,
        )
        .unwrap();
    assert_eq!(
        lifecycle
            .iter()
            .filter(|event| event.kind == "worktree.cleanup_admitted")
            .count(),
        1
    );
    assert_eq!(
        lifecycle
            .iter()
            .filter(|event| event.kind == "worktree.cleaned_up")
            .count(),
        1
    );
    assert_eq!(cleanup_operation(&restarted), operation_id);
}

#[tokio::test]
async fn lost_terminal_sqlite_receipt_never_replays_git_and_reconciles_by_absence() {
    let fixture = fixture().await;
    let db = rusqlite::Connection::open(fixture.temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_worktree_terminal BEFORE INSERT ON runtime_events
        WHEN NEW.kind='worktree.cleaned_up'
        BEGIN SELECT RAISE(FAIL, 'lost terminal receipt'); END;",
    )
    .unwrap();
    let failure = fixture
        .service
        .cleanup_worktree(request(&fixture.worktree.id))
        .await
        .unwrap_err();
    assert!(
        matches!(failure, RuntimeError::OperationUnresolved { .. }),
        "{failure:?}"
    );
    assert!(
        !Path::new(&fixture.worktree.worktree_cwd).exists(),
        "Git worktree was removed once"
    );
    assert!(
        !git(
            &fixture.root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", fixture.worktree.branch_name)
            ]
        )
        .0
    );
    let operation_id = cleanup_operation(&fixture.store);
    let pending = fixture
        .store
        .repository()
        .get_operation(&operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(pending.operation.phase, OperationPhase::Requested);
    assert_eq!(pending.claims.len(), 2);
    assert!(matches!(
        fixture
            .service
            .create_workspace_worktree(WorkspaceWorktreeCreateRequest {
                workspace_id: fixture.workspace_id.clone(),
                worktree_name: "recoverable".into(),
                branch_prefix: None,
                base_ref: None,
                deletion_policy: Some("delete_on_last_claim".into()),
                run_init_script: Some(false),
            })
            .await,
        Err(RuntimeError::OperationUnresolved { .. })
    ));

    // A dangling symlink is not proof of a removed checkout. The replay
    // guard must not accept it even with no Git branch left behind.
    std::os::unix::fs::symlink("missing-worktree", &fixture.worktree.worktree_cwd).unwrap();
    assert!(matches!(
        fixture
            .service
            .cleanup_worktree(request(&fixture.worktree.id))
            .await,
        Err(RuntimeError::OperationUnresolved { .. })
    ));
    std::fs::remove_file(&fixture.worktree.worktree_cwd).unwrap();

    db.execute_batch("DROP TRIGGER fail_worktree_terminal;")
        .unwrap();
    let restarted = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: fixture.temp.path().join("runtime.sqlite3"),
    }));
    restarted.initialize().await.unwrap();
    let (runtime, comms) = build_runtime_and_team_comms(restarted.clone()).await;
    let service = RuntimeWorktreeService::new(
        restarted.clone(),
        runtime,
        comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: fixture.temp.path().join("managed").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "retain_on_last_claim".into(),
        },
    )
    .unwrap();
    let recovered = service
        .cleanup_worktree(request(&fixture.worktree.id))
        .await
        .unwrap();
    assert_eq!(recovered.status, "deleted");
    assert_eq!(
        restarted
            .repository()
            .get_operation(&operation_id)
            .unwrap()
            .unwrap()
            .operation
            .phase,
        OperationPhase::Completed
    );
    assert!(restarted
        .repository()
        .get_operation(&operation_id)
        .unwrap()
        .unwrap()
        .claims
        .is_empty());
    let lifecycle = restarted
        .list_runtime_events(
            Some((RuntimeEventScope::Worktree, &fixture.worktree.id)),
            None,
            30,
        )
        .unwrap();
    assert_eq!(
        lifecycle
            .iter()
            .filter(|event| event.kind == "worktree.cleaned_up")
            .count(),
        1
    );
    assert_eq!(
        lifecycle
            .iter()
            .filter(|event| event.kind == "worktree.cleanup_admitted")
            .count(),
        1
    );
    assert_eq!(cleanup_operation(&restarted), operation_id);
}

#[tokio::test]
async fn failed_pre_native_admission_preserves_git_and_dirty_checkout_is_never_removed() {
    let fixture = fixture().await;
    let db = rusqlite::Connection::open(fixture.temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_worktree_admit BEFORE INSERT ON runtime_events
        WHEN NEW.kind='worktree.cleanup_admitted'
        BEGIN SELECT RAISE(FAIL, 'lost native admission'); END;",
    )
    .unwrap();
    assert!(fixture
        .service
        .cleanup_worktree(request(&fixture.worktree.id))
        .await
        .is_err());
    assert!(Path::new(&fixture.worktree.worktree_cwd).is_dir());
    assert!(
        git(
            &fixture.root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", fixture.worktree.branch_name)
            ]
        )
        .0
    );
    assert!(fixture
        .store
        .repository()
        .list_operations_page(None, None, 101)
        .unwrap()
        .into_iter()
        .all(|op| op.kind != "managed_worktree_cleanup"));
    db.execute_batch("DROP TRIGGER fail_worktree_admit;")
        .unwrap();

    std::fs::write(
        Path::new(&fixture.worktree.worktree_cwd).join("preserve-me.txt"),
        "user data",
    )
    .unwrap();
    let blocked = fixture
        .service
        .cleanup_worktree(request(&fixture.worktree.id))
        .await
        .unwrap();
    assert_eq!(blocked.status, "skipped_dirty_worktree");
    assert!(Path::new(&fixture.worktree.worktree_cwd)
        .join("preserve-me.txt")
        .exists());
    assert!(fixture
        .store
        .repository()
        .list_operations_page(None, None, 101)
        .unwrap()
        .into_iter()
        .all(|op| op.kind != "managed_worktree_cleanup"));
}
