use super::*;
use runtime_core::WorktreeService;

#[tokio::test]
async fn get_worktree_reads_only_its_id_despite_unrelated_malformed_session_history() {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("runtime.sqlite3");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: database_path.clone(),
    }));
    store.initialize().await.unwrap();
    let (runtime, comms) = build_runtime_and_team_comms(store.clone()).await;
    let service = RuntimeWorktreeService::new(
        store.clone(),
        runtime,
        comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "delete_on_last_claim".into(),
        },
    )
    .unwrap();

    let live = ManagedWorktreeRecord {
        id: "wt_point_read_live".into(),
        repo_root: "/disposable/repo".into(),
        worktree_root: "/disposable/worktrees".into(),
        worktree_cwd: "/disposable/worktrees/live".into(),
        branch_name: "gg/live".into(),
        worktree_name: "live".into(),
        unified_workspace_path: "disposable__repo".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 101,
        updated_at: 101,
    };
    store.upsert_managed_worktree(&live).unwrap();
    let tombstoned = ManagedWorktreeRecord {
        id: "wt_point_read_tombstone".into(),
        repo_root: "__gg_tombstoned__/old_repo".into(),
        worktree_cwd: "__gg_tombstoned__/old_checkout".into(),
        branch_name: "__gg_tombstoned__/old_branch".into(),
        ..live.clone()
    };
    store.upsert_managed_worktree(&tombstoned).unwrap();

    // A broken, unrelated session projection can make whole-database
    // hydration fail. One worktree's indexed primary-key read should not
    // depend on parsing every session, event, process and delivery.
    let db = rusqlite::Connection::open(&database_path).unwrap();
    db.execute(
        "INSERT INTO sessions (id, provider, status, metadata_json, created_at, updated_at)
         VALUES ('unrelated_session', 'codex', 'idle', '{malformed', 1, 1)",
        [],
    )
    .unwrap();
    assert!(store.hydrate_runtime_state().is_err());

    assert_eq!(service.get_worktree(&live.id).await.unwrap(), live);
    for id in [tombstoned.id.as_str(), "unknown_worktree"] {
        assert!(
            matches!(
                service.get_worktree(id).await,
                Err(RuntimeError::NotFound(_))
            ),
            "missing/tombstoned IDs cannot be made visible by a point-read"
        );
    }
    let plan: String = db
        .query_row(
            "EXPLAIN QUERY PLAN
             SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                    worktree_name, unified_workspace_path, deletion_policy,
                    created_by_session_id, created_by_operation_id, created_at, updated_at
             FROM managed_worktrees WHERE id=?1",
            [&live.id],
            |row| row.get(3),
        )
        .unwrap();
    assert!(plan.contains("SEARCH managed_worktrees"), "{plan}");
    assert!(!plan.contains("SCAN managed_worktrees"), "{plan}");
}

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

#[tokio::test]
async fn healthy_worktree_startup_does_not_rewrite_unchanged_authority_or_claims() {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("runtime.sqlite3");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: database_path.clone(),
    }));
    store.initialize().await.unwrap();
    let (runtime, comms) = build_runtime_and_team_comms(store.clone()).await;
    let session = create_test_session(runtime.as_ref(), temp.path().to_str().unwrap()).await;

    let worktree = ManagedWorktreeRecord {
        id: "wt_clean_startup".into(),
        repo_root: temp.path().display().to_string(),
        worktree_root: temp.path().join("worktrees").display().to_string(),
        worktree_cwd: temp.path().join("worktrees/clean").display().to_string(),
        branch_name: "gg/clean".into(),
        worktree_name: "clean".into(),
        unified_workspace_path: "clean_fixture".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: Some(session.id.clone()),
        created_by_operation_id: None,
        created_at: 101,
        updated_at: 101,
    };
    store.upsert_managed_worktree(&worktree).unwrap();
    let claim = ManagedWorktreeClaimRecord {
        worktree_id: worktree.id.clone(),
        session_id: session.id,
        claim_role: "owner".into(),
        created_at: 102,
        released_at: None,
    };
    store.upsert_managed_worktree_claim(&claim).unwrap();
    let before_revision = store.managed_worktree_revision(&worktree.id).unwrap();

    // Actual independent SQLite triggers make no-op writes observable.
    // A healthy restart must not increment revision, churn ETags, or rewrite
    // historical claims; in particular stale hydration cannot rewrite a
    // just-released claim if no normalization was required.
    let db = rusqlite::Connection::open(&database_path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_unchanged_worktree_rewrite BEFORE UPDATE ON managed_worktrees
         WHEN OLD.id='wt_clean_startup'
         BEGIN SELECT RAISE(FAIL,'unexpected startup worktree revision rewrite'); END;
         CREATE TRIGGER fail_unchanged_claim_rewrite BEFORE UPDATE ON managed_worktree_claims
         WHEN OLD.worktree_id='wt_clean_startup'
         BEGIN SELECT RAISE(FAIL,'unexpected startup claim rewrite'); END;",
    )
    .unwrap();
    let _service = RuntimeWorktreeService::new(
        store.clone(),
        runtime,
        comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "delete_on_last_claim".into(),
        },
    )
    .expect("healthy startup should not write unchanged worktree and claim rows");
    assert_eq!(
        store.managed_worktree_revision(&worktree.id).unwrap(),
        before_revision
    );
    assert_eq!(
        store
            .get_managed_worktree_claim(&worktree.id, &claim.session_id)
            .unwrap(),
        Some(claim)
    );
}
