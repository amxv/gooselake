use super::*;
use crate::repository_worktree_diagnostics::{
    ACTIVE_ORPHAN_COUNTS_SQL, DELETION_POLICY_COUNTS_SQL,
};
use std::collections::{BTreeMap, HashSet};

fn hydrated_counts(
    state: &runtime_core::RuntimeHydratedState,
) -> runtime_core::ManagedWorktreeDiagnosticCounts {
    let mut policies = BTreeMap::new();
    for worktree in &state.managed_worktrees {
        *policies
            .entry(worktree.deletion_policy.clone())
            .or_insert(0) += 1;
    }
    let sessions = state
        .sessions
        .iter()
        .map(|s| s.id.as_str())
        .collect::<HashSet<_>>();
    let worktrees = state
        .managed_worktrees
        .iter()
        .map(|w| w.id.as_str())
        .collect::<HashSet<_>>();
    let active = state
        .managed_worktree_claims
        .iter()
        .filter(|c| c.released_at.is_none())
        .collect::<Vec<_>>();
    runtime_core::ManagedWorktreeDiagnosticCounts {
        worktree_total: state.managed_worktrees.len(),
        active_claim_total: active.len(),
        orphaned_claim_total: active
            .iter()
            .filter(|c| {
                !sessions.contains(c.session_id.as_str())
                    || !worktrees.contains(c.worktree_id.as_str())
            })
            .count(),
        deletion_policy_counts: policies,
    }
}

#[test]
fn indexed_worktree_diagnostics_match_full_hydration_orphans_and_one_wal_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    // Bare connection intentionally models historical foreign-key-violating
    // rows without changing runtime foreign-key enforcement or user data.
    let mut seed = Connection::open(&path).unwrap();
    seed.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=OFF;")
        .unwrap();
    let transaction = seed.transaction().unwrap();
    let private_path = format!("/private/{}", "s".repeat(1536));
    let policies = [
        "retain_on_last_claim",
        "delete_if_clean",
        "retain_forever",
        "legacy_unknown",
    ];
    for i in 0..2_400i64 {
        transaction
            .execute(
                "INSERT INTO managed_worktrees (
               id, repo_root, worktree_root, worktree_cwd, branch_name,
               worktree_name, unified_workspace_path, deletion_policy, created_at, updated_at
             ) VALUES (?1, '/demo', ?2, ?2, ?1, ?1, ?2, ?3, ?4, ?4)",
                params![
                    format!("wt_{i:04}"),
                    private_path,
                    policies[i as usize % 4],
                    i + 1
                ],
            )
            .unwrap();
    }
    for i in 0..1_800i64 {
        transaction
            .execute(
                "INSERT INTO sessions (id, provider, status, created_at, updated_at)
             VALUES (?1, 'inert', 'ready', ?2, ?2)",
                params![format!("session_{i:04}"), i + 1],
            )
            .unwrap();
        // Both missing-session and missing-worktree orphans occur in legacy
        // snapshots. Released claims must not be counted as active/orphaned.
        let worktree = if i % 37 == 0 {
            format!("lost_wt_{i:04}")
        } else {
            format!("wt_{i:04}")
        };
        let session = if i % 31 == 0 {
            format!("lost_session_{i:04}")
        } else {
            format!("session_{i:04}")
        };
        transaction.execute(
            "INSERT INTO managed_worktree_claims (worktree_id, session_id, claim_role, created_at, released_at)
             VALUES (?1, ?2, 'primary', ?3, ?4)",
            params![worktree, session, i + 1, if i % 9 == 0 { Some(i + 2) } else { None }],
        ).unwrap();
    }
    transaction.commit().unwrap();

    let old = hydrated_counts(&repository.hydrate_runtime_state().unwrap());
    assert_eq!(old.worktree_total, 2_400);
    assert!(old.orphaned_claim_total > 50);
    assert_eq!(
        repository.managed_worktree_diagnostic_counts().unwrap(),
        old
    );

    let policy_plan = seed
        .prepare(&format!("EXPLAIN QUERY PLAN {DELETION_POLICY_COUNTS_SQL}"))
        .unwrap()
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" ");
    assert!(
        policy_plan.contains("COVERING INDEX idx_managed_worktrees_deletion_policy"),
        "{policy_plan}"
    );
    assert!(!policy_plan.contains("USE TEMP B-TREE"), "{policy_plan}");
    let claim_plan = seed
        .prepare(&format!("EXPLAIN QUERY PLAN {ACTIVE_ORPHAN_COUNTS_SQL}"))
        .unwrap()
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" ");
    // All three partial indexes restrict the scan to active claims. Schema
    // 48's session-first index can be chosen over either earlier index for
    // this unordered count without changing cardinality or join semantics.
    // This assertion must allow legitimate SQLite cost-model choices.
    assert!(
        claim_plan.contains("idx_managed_worktree_claims_active_worktree")
            || claim_plan.contains("idx_worktree_claims_active_diagnostics")
            || claim_plan.contains("idx_worktree_claims_active_session_order"),
        "{claim_plan}"
    );
    assert!(
        claim_plan.contains("sqlite_autoindex_sessions_1"),
        "{claim_plan}"
    );
    assert!(
        claim_plan.contains("sqlite_autoindex_managed_worktrees_1"),
        "{claim_plan}"
    );

    // A second connection's commit between the two aggregate statements
    // must not mix worktrees from one revision with claims from another.
    let mut reader = Connection::open(&path).unwrap();
    let snapshot = reader.transaction().unwrap();
    let before: i64 = snapshot
        .query_row("SELECT COUNT(*) FROM managed_worktrees", [], |r| r.get(0))
        .unwrap();
    let writer = Connection::open(&path).unwrap();
    writer.execute("INSERT INTO managed_worktrees (
        id, repo_root, worktree_root, worktree_cwd, branch_name, worktree_name,
        unified_workspace_path, deletion_policy, created_at, updated_at
      ) VALUES ('late_wt', '/demo', '/private/late', '/private/late', 'late', 'late', '/private/late', 'new_policy', 9999, 9999)", []).unwrap();
    writer
        .execute(
            "UPDATE managed_worktree_claims SET released_at=9999 WHERE worktree_id='wt_0001'",
            [],
        )
        .unwrap();
    let during: i64 = snapshot
        .query_row("SELECT COUNT(*) FROM managed_worktrees", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, during);
    assert_eq!(before, 2_400);
    let (active_during, _): (i64, i64) = snapshot
        .query_row(ACTIVE_ORPHAN_COUNTS_SQL, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!(active_during as usize, old.active_claim_total);
    snapshot.commit().unwrap();

    let reopened = SqliteRuntimeRepository::new(path.clone());
    let current = hydrated_counts(&reopened.hydrate_runtime_state().unwrap());
    assert_eq!(current.worktree_total, 2_401);
    assert_eq!(current.active_claim_total + 1, old.active_claim_total);
    assert_eq!(
        reopened.managed_worktree_diagnostic_counts().unwrap(),
        current
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path,
    });
    assert_eq!(
        RuntimeStore::managed_worktree_diagnostic_counts(&store).unwrap(),
        Some(current)
    );
}
