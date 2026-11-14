use std::collections::BTreeMap;

use runtime_core::{ManagedWorktreeDiagnosticCounts, RuntimeError};
use rusqlite::TransactionBehavior;

use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

pub(crate) const DELETION_POLICY_COUNTS_SQL: &str =
    "SELECT deletion_policy, COUNT(*) FROM managed_worktrees GROUP BY deletion_policy";

pub(crate) const ACTIVE_ORPHAN_COUNTS_SQL: &str = "
    SELECT COUNT(*), COUNT(*) FILTER (WHERE s.id IS NULL OR w.id IS NULL)
    FROM managed_worktree_claims c
    LEFT JOIN sessions s ON s.id = c.session_id
    LEFT JOIN managed_worktrees w ON w.id = c.worktree_id
    WHERE c.released_at IS NULL";

fn checked_count(value: i64) -> Result<usize, RuntimeError> {
    usize::try_from(value).map_err(|_| {
        RuntimeError::ProtocolViolation("negative or oversized worktree diagnostic count".into())
    })
}

impl SqliteRuntimeRepository {
    /// Two aggregate queries in one SQLite read snapshot. The deletion-policy
    /// index and existing live-claim partial index avoid full-row hydration,
    /// while PK joins distinguish missing parents for historical/orphan data.
    pub fn managed_worktree_diagnostic_counts(
        &self,
    ) -> Result<ManagedWorktreeDiagnosticCounts, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let snapshot = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting worktree diagnostic snapshot", error))?;

        let mut result = ManagedWorktreeDiagnosticCounts {
            deletion_policy_counts: BTreeMap::new(),
            ..Default::default()
        };
        {
            let mut statement = snapshot
                .prepare(DELETION_POLICY_COUNTS_SQL)
                .map_err(|error| {
                    db_error("failed preparing worktree deletion-policy counts", error)
                })?;
            let mut rows = statement.query([]).map_err(|error| {
                db_error("failed querying worktree deletion-policy counts", error)
            })?;
            while let Some(row) = rows.next().map_err(|error| {
                db_error("failed reading worktree deletion-policy counts", error)
            })? {
                let policy = row
                    .get::<_, String>(0)
                    .map_err(|error| db_error("failed decoding worktree deletion policy", error))?;
                let count =
                    checked_count(row.get::<_, i64>(1).map_err(|error| {
                        db_error("failed decoding worktree policy count", error)
                    })?)?;
                result.worktree_total =
                    result.worktree_total.checked_add(count).ok_or_else(|| {
                        RuntimeError::ProtocolViolation("worktree diagnostic total overflow".into())
                    })?;
                result.deletion_policy_counts.insert(policy, count);
            }
        }

        let (active, orphaned): (i64, i64) = snapshot
            .query_row(ACTIVE_ORPHAN_COUNTS_SQL, [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|error| db_error("failed aggregating active and orphaned claims", error))?;
        result.active_claim_total = checked_count(active)?;
        result.orphaned_claim_total = checked_count(orphaned)?;
        snapshot
            .commit()
            .map_err(|error| db_error("failed closing worktree diagnostic snapshot", error))?;
        Ok(result)
    }
}
