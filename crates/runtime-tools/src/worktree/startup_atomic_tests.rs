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
        claim_generation: None,
        worktree_id: winner.id.clone(),
        session_id: "atomic_claim_session".into(),
        claim_role: " owner ".into(),
        created_at: 30,
        released_at: None,
    };
    store.upsert_managed_worktree_claim(&claim).unwrap();
    let claim = store
        .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
        .unwrap()
        .unwrap();
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
    f.store
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: f.loser.id.clone(),
            created_at: 40,
            claim_generation: None,
            ..f.claim.clone()
        })
        .unwrap();
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

    let expected_claims = reopened
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims;
    let winner_revision = reopened.managed_worktree_revision(&f.winner.id).unwrap();
    let loser_revision = reopened.managed_worktree_revision(&f.loser.id).unwrap();
    assert!(expected_claims.iter().any(|c| c.worktree_id == f.loser.id
        && c.released_at.is_some()
        && c.claim_generation == Some(1)));
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER forbid_repeat_worktree_update BEFORE UPDATE ON managed_worktrees
         BEGIN SELECT RAISE(ABORT,'repaired worktree must not UPDATE on restart'); END;
         CREATE TRIGGER forbid_repeat_claim_update BEFORE UPDATE ON managed_worktree_claims
         BEGIN SELECT RAISE(ABORT,'repaired claim must not UPDATE on restart'); END;
         CREATE TRIGGER forbid_repeat_claim_insert BEFORE INSERT ON managed_worktree_claims BEGIN SELECT RAISE(ABORT,'restart INSERT'); END;
         CREATE TRIGGER forbid_repeat_claim_delete BEFORE DELETE ON managed_worktree_claims BEGIN SELECT RAISE(ABORT,'restart DELETE'); END;",
    )
    .unwrap();
    let fresh = reopened.hydrate_runtime_state().unwrap();
    RuntimeWorktreeService::repair_startup_state(&reopened, &fresh).unwrap();
    assert_eq!(
        reopened
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        expected_claims
    );
    assert_eq!(
        reopened.managed_worktree_revision(&f.winner.id).unwrap(),
        winner_revision
    );
    assert_eq!(
        reopened.managed_worktree_revision(&f.loser.id).unwrap(),
        loser_revision
    );
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

#[tokio::test]
async fn unchanged_merge_winner_route_is_fenced_before_loser_claim_remap() {
    let mut f = legacy_record_and_claim_fixture().await;
    f.winner.deletion_policy = f.loser.deletion_policy.clone();
    f.winner.updated_at = 100;
    f.store.upsert_managed_worktree(&f.winner).unwrap();
    f.store
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: f.loser.id.clone(),
            claim_role: "owner".into(),
            created_at: 40,
            claim_generation: None,
            ..f.claim.clone()
        })
        .unwrap();
    let stale = f.store.hydrate_runtime_state().unwrap();
    let expected_claims = stale.managed_worktree_claims.clone();
    let writer = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: f.path.clone(),
    });
    let moved = ManagedWorktreeRecord {
        worktree_cwd: f
            ._temp
            .path()
            .join("moved-after-hydration")
            .display()
            .to_string(),
        updated_at: 101,
        ..f.winner.clone()
    };
    writer.upsert_managed_worktree(&moved).unwrap();
    let revision = writer.managed_worktree_revision(&moved.id).unwrap();
    let result = RuntimeWorktreeService::repair_startup_state(&f.store, &stale);
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "{result:?}"
    );
    assert_eq!(
        writer.get_managed_worktree(&f.winner.id).unwrap(),
        Some(moved)
    );
    assert_eq!(
        writer.managed_worktree_revision(&f.winner.id).unwrap(),
        revision
    );
    assert_eq!(
        writer.get_managed_worktree(&f.loser.id).unwrap(),
        Some(f.loser)
    );
    assert_eq!(
        writer
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        expected_claims
    );
    assert!(writer
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn new_unrelated_claim_after_empty_hydration_blocks_record_only_repair() {
    let f = legacy_record_and_claim_fixture().await;
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("DELETE FROM managed_worktree_claims;
        INSERT INTO sessions(id,provider,status,metadata_json,created_at,updated_at) VALUES('late-session','codex','idle','{}',1,1);").unwrap();
    let unrelated = ManagedWorktreeRecord {
        id: "wt-unrelated".into(),
        worktree_cwd: f._temp.path().join("unrelated").display().to_string(),
        branch_name: "gg/unrelated".into(),
        ..f.winner.clone()
    };
    f.store.upsert_managed_worktree(&unrelated).unwrap();
    let stale = f.store.hydrate_runtime_state().unwrap();
    assert!(stale.managed_worktree_claims.is_empty());
    let writer = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: f.path.clone(),
    });
    let late = writer
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                worktree_id: unrelated.id,
                session_id: "late-session".into(),
                claim_role: "owner".into(),
                created_at: 90,
                released_at: None,
                claim_generation: None,
            },
            runtime_core::ManagedWorktreeClaimTransition::Claim,
        )
        .unwrap();
    let claims = writer
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims;
    let events = writer.list_runtime_events(None, None, 10).unwrap();
    assert!(matches!(
        RuntimeWorktreeService::repair_startup_state(&f.store, &stale),
        Err(RuntimeError::Conflict(_))
    ));
    assert_original_records(&f);
    assert_eq!(
        writer
            .get_managed_worktree_claim(&late.worktree_id, &late.session_id)
            .unwrap(),
        Some(late)
    );
    assert_eq!(
        writer
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        claims
    );
    assert_eq!(writer.list_runtime_events(None, None, 10).unwrap(), events);
}

#[tokio::test]
async fn absent_destination_gets_its_own_first_generation_and_terminal_destination_refuses() {
    for terminal in [false, true] {
        let f = legacy_record_and_claim_fixture().await;
        let db = rusqlite::Connection::open(&f.path).unwrap();
        if terminal {
            db.execute("UPDATE managed_worktree_claims SET released_at=90", [])
                .unwrap();
        } else {
            db.execute("DELETE FROM managed_worktree_claims", [])
                .unwrap();
        }
        f.store
            .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
                worktree_id: f.loser.id.clone(),
                claim_generation: None,
                ..f.claim.clone()
            })
            .unwrap();
        db.execute(
            "UPDATE managed_worktree_claims SET claim_generation=9 WHERE worktree_id=?1",
            [&f.loser.id],
        )
        .unwrap();
        let before = f.store.hydrate_runtime_state().unwrap();
        let result = RuntimeWorktreeService::repair_startup_state(&f.store, &before);
        if terminal {
            assert!(matches!(result, Err(RuntimeError::Conflict(_))));
            assert_eq!(f.store.hydrate_runtime_state().unwrap(), before);
        } else {
            result.unwrap();
            let target = f
                .store
                .get_managed_worktree_claim(&f.winner.id, &f.claim.session_id)
                .unwrap()
                .unwrap();
            let source = f
                .store
                .get_managed_worktree_claim(&f.loser.id, &f.claim.session_id)
                .unwrap()
                .unwrap();
            assert_eq!(target.claim_generation, Some(1));
            assert_eq!(target.released_at, None);
            assert_eq!(target.claim_role, "owner");
            assert_eq!(source.claim_generation, Some(9));
            assert!(source.released_at.is_some());
            assert_eq!(source.claim_role, f.claim.claim_role);
        }
    }
}

#[tokio::test]
async fn whitespace_only_worktree_metadata_compares_against_raw_original() {
    let f = legacy_record_and_claim_fixture().await;
    RuntimeWorktreeService::repair_startup_state(
        &f.store,
        &f.store.hydrate_runtime_state().unwrap(),
    )
    .unwrap();
    let mut row = f.store.get_managed_worktree(&f.winner.id).unwrap().unwrap();
    row.worktree_name = " winner ".into();
    f.store.upsert_managed_worktree(&row).unwrap();
    RuntimeWorktreeService::repair_startup_state(
        &f.store,
        &f.store.hydrate_runtime_state().unwrap(),
    )
    .unwrap();
    assert_eq!(
        f.store
            .get_managed_worktree(&f.winner.id)
            .unwrap()
            .unwrap()
            .worktree_name,
        "winner"
    );
}

#[tokio::test]
async fn later_carrier_fault_cannot_invalidate_an_earlier_startup_plan() {
    for sql in [
        "CREATE TRIGGER n06_late_record AFTER UPDATE ON managed_worktree_claims BEGIN UPDATE managed_worktrees SET worktree_name='changed-after-record-write' WHERE id='wt_atomic_winner';END;",
        "CREATE TRIGGER n06_new_claim AFTER UPDATE ON managed_worktrees WHEN NEW.id='wt_atomic_winner' BEGIN INSERT INTO managed_worktree_claims(worktree_id,session_id,claim_role,created_at,released_at,claim_generation) VALUES('wt_atomic_loser','atomic_claim_session','owner',40,NULL,1);END;"
    ]{
        let f=legacy_record_and_claim_fixture().await;let before=f.store.hydrate_runtime_state().unwrap();
        let revision=f.store.managed_worktree_revision(&f.winner.id).unwrap();
        rusqlite::Connection::open(&f.path).unwrap().execute_batch(sql).unwrap();
        let result=RuntimeWorktreeService::repair_startup_state(&f.store,&before);
        assert!(matches!(result,Err(RuntimeError::Conflict(_))),"{result:?}");
        assert_eq!(f.store.hydrate_runtime_state().unwrap(),before);
        assert_eq!(f.store.managed_worktree_revision(&f.winner.id).unwrap(),revision);
        assert!(f.store.list_runtime_events(None,None,10).unwrap().is_empty());
    }
}
