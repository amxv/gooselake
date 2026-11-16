use super::*;
use runtime_core::{RuntimeEventScope, RuntimeStore};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};

struct StartupFixture {
    _temp: tempfile::TempDir,
    path: std::path::PathBuf,
    store: SqliteRuntimeStore,
    winner: ManagedWorktreeRecord,
    loser: ManagedWorktreeRecord,
    claim: ManagedWorktreeClaimRecord,
}

async fn legacy_record_and_claim_fixture() -> StartupFixture {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("startup-record-claim-atomic.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    store.initialize().await.unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "INSERT INTO sessions (id,provider,status,metadata_json,created_at,updated_at)
             VALUES ('atomic_claim_session','codex','idle','{}',1,1);",
        )
        .unwrap();
    let winner = ManagedWorktreeRecord {
        id: "wt_atomic_winner".into(),
        repo_root: temp.path().display().to_string(),
        worktree_root: temp.path().join("worktrees").display().to_string(),
        worktree_cwd: temp.path().join("worktrees/shared").display().to_string(),
        branch_name: "gg/atomic".into(),
        worktree_name: "winner".into(),
        unified_workspace_path: "atomic-winner".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 10,
        updated_at: 10,
    };
    // Distinct raw keys normalize to one checkout. Startup must merge the
    // winner, tombstone the loser, and normalize the claim in one transaction.
    let loser = ManagedWorktreeRecord {
        id: "wt_atomic_loser".into(),
        repo_root: format!(" {} ", winner.repo_root),
        deletion_policy: "delete_on_last_claim".into(),
        created_at: 20,
        updated_at: 20,
        ..winner.clone()
    };
    store.upsert_managed_worktree(&winner).unwrap();
    store.upsert_managed_worktree(&loser).unwrap();
    let claim = ManagedWorktreeClaimRecord {
        worktree_id: winner.id.clone(),
        session_id: "atomic_claim_session".into(),
        claim_role: " owner ".into(),
        created_at: 30,
        released_at: None,
    };
    store.upsert_managed_worktree_claim(&claim).unwrap();
    StartupFixture {
        _temp: temp,
        path,
        store,
        winner,
        loser,
        claim,
    }
}

fn assert_original_records(f: &StartupFixture) {
    assert_eq!(
        f.store.get_managed_worktree(&f.winner.id).unwrap(),
        Some(f.winner.clone()),
        "a claim failure must not strand a merged winner"
    );
    assert_eq!(
        f.store.get_managed_worktree(&f.loser.id).unwrap(),
        Some(f.loser.clone()),
        "a claim failure must not strand a tombstoned loser"
    );
    assert!(f
        .store
        .list_runtime_events(Some((RuntimeEventScope::Worktree, &f.winner.id)), None, 10,)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn stale_claim_conflict_must_not_commit_earlier_legacy_worktree_normalization() {
    let f = legacy_record_and_claim_fixture().await;
    let stale = f.store.hydrate_runtime_state().unwrap();
    // A separate SQLite connection writes a newer, unjournaled legacy claim
    // after startup hydration. The old record-first two-transaction service
    // committed the winner/loser plan, then rejected this stale claim source.
    let writer = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: f.path.clone(),
    });
    let later = ManagedWorktreeClaimRecord {
        claim_role: "consumer".into(),
        created_at: 40,
        ..f.claim.clone()
    };
    writer.upsert_managed_worktree_claim(&later).unwrap();
    let result = RuntimeWorktreeService::repair_startup_state(&f.store, &stale);
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "stale claim source must conflict: {result:?}"
    );
    assert_original_records(&f);
    assert_eq!(
        writer
            .get_managed_worktree_claim(&later.worktree_id, &later.session_id)
            .unwrap(),
        Some(later),
    );
}

#[tokio::test]
async fn legacy_record_and_claim_normalization_commits_together_and_retries_without_writes() {
    let f = legacy_record_and_claim_fixture().await;
    let stale = f.store.hydrate_runtime_state().unwrap();
    RuntimeWorktreeService::repair_startup_state(&f.store, &stale).unwrap();
    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: f.path.clone(),
    });
    let winner = reopened
        .get_managed_worktree(&f.winner.id)
        .unwrap()
        .unwrap();
    assert_eq!(winner.deletion_policy, "delete_on_last_claim");
    assert_eq!(winner.updated_at, 20);
    let loser = reopened.get_managed_worktree(&f.loser.id).unwrap().unwrap();
    assert!(loser.repo_root.starts_with("__gg_tombstoned__/"));
    let claim = reopened
        .get_managed_worktree_claim(&f.claim.worktree_id, &f.claim.session_id)
        .unwrap()
        .unwrap();
    assert_eq!(claim.claim_role, "owner");
    assert_eq!(claim.created_at, f.claim.created_at);

    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER forbid_repeat_worktree_update BEFORE UPDATE ON managed_worktrees
         BEGIN SELECT RAISE(ABORT,'repaired worktree must not UPDATE on restart'); END;
         CREATE TRIGGER forbid_repeat_claim_update BEFORE UPDATE ON managed_worktree_claims
         BEGIN SELECT RAISE(ABORT,'repaired claim must not UPDATE on restart'); END;",
    )
    .unwrap();
    let fresh = reopened.hydrate_runtime_state().unwrap();
    RuntimeWorktreeService::repair_startup_state(&reopened, &fresh).unwrap();
    assert_eq!(
        reopened.get_managed_worktree(&f.winner.id).unwrap(),
        Some(winner)
    );
    assert_eq!(
        reopened.get_managed_worktree(&f.loser.id).unwrap(),
        Some(loser)
    );
    assert_eq!(
        reopened
            .get_managed_worktree_claim(&f.claim.worktree_id, &f.claim.session_id)
            .unwrap(),
        Some(claim)
    );
}

#[tokio::test]
async fn claim_sql_fault_rolls_back_earlier_legacy_record_plan_and_allows_clean_retry() {
    let f = legacy_record_and_claim_fixture().await;
    let stale = f.store.hydrate_runtime_state().unwrap();
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_claim_repair AFTER UPDATE ON managed_worktree_claims
         BEGIN SELECT RAISE(ABORT, 'injected claim fault after record repairs'); END;",
    )
    .unwrap();
    let failed = RuntimeWorktreeService::repair_startup_state(&f.store, &stale);
    assert!(failed.is_err(), "injected SQL fault must abort: {failed:?}");
    assert_original_records(&f);
    assert_eq!(
        f.store
            .get_managed_worktree_claim(&f.claim.worktree_id, &f.claim.session_id)
            .unwrap(),
        Some(f.claim.clone())
    );
    db.execute_batch("DROP TRIGGER fail_claim_repair;").unwrap();
    RuntimeWorktreeService::repair_startup_state(&f.store, &stale).unwrap();
    assert_eq!(
        f.store
            .get_managed_worktree_claim(&f.claim.worktree_id, &f.claim.session_id)
            .unwrap()
            .unwrap()
            .claim_role,
        "owner"
    );
}
