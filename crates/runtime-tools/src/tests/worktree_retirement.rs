use super::*;
use runtime_core::{
    prepare_workspace_registration, prepare_workspace_retirement, OperationActor,
    WorkspaceRegisterRequest, WorkspaceRetirementRequest, WorkspaceWorktreeCreateRequest,
    WorktreeCleanupRequest, WorktreeCreateRequest, WorktreeService,
};
use std::path::Path;
use std::process::Command;

fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test]
async fn retirement_freeze_prevents_native_worktrees_and_legacy_external_session_creation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("repository");
    let outside = temp.path().join("unrelated");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    git(&root, &["init", "--quiet"]);
    git(
        &root,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "Fixture",
        ],
    );
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.unwrap();
    let provider = Arc::new(WorktreeTestProvider::default());
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let runtime =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512).unwrap());
    let team_comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .unwrap();
    let source = create_test_session(&runtime, outside.to_str().unwrap()).await;
    let workspace = store
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.display().to_string(),
                    display_name: Some("Frozen native routes".into()),
                },
                OperationActor::operator("worktree-retirement-test"),
                Some("register".into()),
            )
            .unwrap(),
        )
        .unwrap()
        .workspace;
    let service = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        team_comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp.path().join("managed-checkouts").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "retain_on_last_claim".into(),
        },
    )
    .unwrap();
    let retained = service
        .create_workspace_worktree(WorkspaceWorktreeCreateRequest {
            workspace_id: workspace.workspace_id.clone(),
            worktree_name: "retained".into(),
            branch_prefix: None,
            base_ref: None,
            deletion_policy: Some("delete_on_last_claim".into()),
            run_init_script: Some(false),
        })
        .await
        .unwrap()
        .worktree;
    assert!(!Path::new(&retained.worktree_cwd).starts_with(&root));
    store
        .admit_workspace_retirement(
            &prepare_workspace_retirement(
                &workspace.workspace_id,
                WorkspaceRetirementRequest {
                    expected_revision: workspace.revision,
                },
                OperationActor::operator("worktree-retirement-test"),
                Some("retire".into()),
            )
            .unwrap(),
        )
        .unwrap();

    let before_worktrees = git(&root, &["worktree", "list", "--porcelain"]);
    let before_provider_count = provider.state.lock().await.sessions.len();
    assert!(matches!(
        service
            .cleanup_worktree(WorktreeCleanupRequest {
                worktree_id: retained.id.clone(),
                reason: Some("competing-cleanup".into()),
            })
            .await
            .unwrap_err(),
        RuntimeError::Conflict(_)
    ));
    let current_error = service
        .create_workspace_worktree(WorkspaceWorktreeCreateRequest {
            workspace_id: workspace.workspace_id,
            worktree_name: "late-current".into(),
            branch_prefix: None,
            base_ref: None,
            deletion_policy: None,
            run_init_script: Some(true),
        })
        .await
        .unwrap_err();
    assert!(matches!(current_error, RuntimeError::Conflict(_)));
    let legacy_error = service
        .create_worktree(WorktreeCreateRequest {
            team_id: None,
            source_session_id: source.id,
            repo_root: Some(root.display().to_string()),
            worktree_name: "late-legacy".into(),
            branch_prefix: None,
            base_ref: None,
            deletion_policy: None,
            run_init_script: Some(true),
            created_by_session_id: None,
            operation_id: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(legacy_error, RuntimeError::Conflict(_)));
    let native_error = runtime
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: Some("test-model".into()),
            cwd: Some(retained.worktree_cwd.clone()),
            permission_mode: None,
            metadata: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(native_error, RuntimeError::Conflict(_)));
    assert_eq!(
        provider.state.lock().await.sessions.len(),
        before_provider_count
    );
    assert_eq!(
        git(&root, &["worktree", "list", "--porcelain"]),
        before_worktrees
    );
    assert!(git(
        &root,
        &[
            "for-each-ref",
            "--format=%(refname)",
            "refs/heads/gg/late-current",
            "refs/heads/gg/late-legacy"
        ]
    )
    .is_empty());
    assert!(Path::new(&retained.worktree_cwd).is_dir());
}
