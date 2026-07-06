use super::*;

#[tokio::test]
async fn startup_repair_normalizes_identity_and_repairs_conflicting_claims() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("init store");

    let seed_session = |id: &str| SessionRecord {
        id: id.to_string(),
        provider: "codex".to_string(),
        status: "idle".to_string(),
        cwd: Some("/tmp/repo".to_string()),
        model: Some("test-model".to_string()),
        permission_mode: None,
        system_prompt: None,
        metadata: serde_json::json!({}),
        provider_session_ref: Some(format!("thread-{id}")),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 1,
        updated_at: 1,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    };
    store
        .upsert_session(&seed_session("session_a"))
        .expect("seed a");
    store
        .upsert_session(&seed_session("session_b"))
        .expect("seed b");

    let winner = ManagedWorktreeRecord {
        id: "wt_1".to_string(),
        repo_root: "/tmp/repo".to_string(),
        worktree_root: "/tmp/worktrees/repo".to_string(),
        worktree_cwd: "/tmp/worktrees/repo/gg--feature".to_string(),
        branch_name: "gg/feature".to_string(),
        worktree_name: "feature".to_string(),
        unified_workspace_path: "tmp__repo".to_string(),
        deletion_policy: "retain_on_last_claim".to_string(),
        created_by_session_id: Some("session_a".to_string()),
        created_by_operation_id: Some("op_a".to_string()),
        created_at: 10,
        updated_at: 10,
    };
    let loser = ManagedWorktreeRecord {
        id: "wt_2".to_string(),
        repo_root: " /tmp/repo ".to_string(),
        worktree_root: "/tmp/worktrees/repo".to_string(),
        worktree_cwd: " /tmp/worktrees/repo/gg--feature ".to_string(),
        branch_name: " gg/feature ".to_string(),
        worktree_name: "feature-dup".to_string(),
        unified_workspace_path: "tmp__repo".to_string(),
        deletion_policy: "delete_on_last_claim".to_string(),
        created_by_session_id: Some("session_b".to_string()),
        created_by_operation_id: Some("op_b".to_string()),
        created_at: 20,
        updated_at: 20,
    };
    store.upsert_managed_worktree(&winner).expect("winner");
    store.upsert_managed_worktree(&loser).expect("loser");
    store
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_1".to_string(),
            session_id: "session_a".to_string(),
            claim_role: "owner".to_string(),
            created_at: 30,
            released_at: None,
        })
        .expect("claim winner");
    store
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_2".to_string(),
            session_id: "session_a".to_string(),
            claim_role: "consumer".to_string(),
            created_at: 31,
            released_at: None,
        })
        .expect("claim loser");

    let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
    let _service = RuntimeWorktreeService::new(
        store.clone(),
        runtime,
        team_comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("service");

    let hydrated = store.hydrate_runtime_state().expect("hydrate repaired");
    let live_records = hydrated
        .managed_worktrees
        .iter()
        .filter(|row| !RuntimeWorktreeService::is_record_tombstoned(row))
        .collect::<Vec<_>>();
    assert_eq!(live_records.len(), 1);
}
