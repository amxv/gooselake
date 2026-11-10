use std::collections::HashMap;
use std::path::Path;

use runtime_core::{ManagedWorktreeRecord, ManagedWorktreeStatusProjection, RuntimeError};
use rusqlite::{params_from_iter, OptionalExtension, Row, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Point lookup for native worktree route and post-provider evidence
    /// validation. Missing rows remain distinct from storage failure.
    pub fn get_managed_worktree(
        &self,
        worktree_id: &str,
    ) -> Result<Option<ManagedWorktreeRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                        worktree_name, unified_workspace_path, deletion_policy,
                        created_by_session_id, created_by_operation_id, created_at, updated_at
                 FROM managed_worktrees WHERE id=?1",
                [worktree_id],
                worktree_from_row,
            )
            .optional()
            .map_err(|error| db_error("failed reading managed worktree by ID", error))
    }

    /// Read only worktrees belonging to this canonical Git repository. Legacy
    /// records may contain a symlink/`..` spelling of repo_root, so enumerate
    /// indexed *distinct roots* rather than assuming old paths were canonical.
    /// Each matching root is then fetched by the repo_root index, along with
    /// revisions and only active claims, under one SQLite read snapshot.
    pub fn list_managed_worktree_status_for_root(
        &self,
        canonical_repo_root: &str,
    ) -> Result<Vec<ManagedWorktreeStatusProjection>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed beginning worktree status snapshot", error))?;

        let roots = {
            let mut statement = transaction
                .prepare("SELECT DISTINCT repo_root FROM managed_worktrees ORDER BY repo_root")
                .map_err(|error| db_error("failed preparing distinct worktree roots", error))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| db_error("failed querying distinct worktree roots", error))?;
            collect_rows(rows)?
        };
        let requested = Path::new(canonical_repo_root);
        let matching_roots = roots
            .into_iter()
            .filter(|root| {
                std::fs::canonicalize(root)
                    .ok()
                    .is_some_and(|canonical| canonical == requested)
            })
            .collect::<Vec<_>>();

        let mut output = Vec::new();
        // A native legacy database may have many spellings of one repo root.
        // Bound every prepared statement's SQLite bind count without dropping
        // any matching aliases or worktrees.
        for chunk in matching_roots.chunks(200) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let mut active_claims: HashMap<String, Vec<String>> = HashMap::new();
            {
                let sql = format!(
                    "SELECT c.worktree_id, c.session_id
                     FROM managed_worktrees w CROSS JOIN managed_worktree_claims c
                     WHERE w.repo_root IN ({placeholders})
                       AND c.worktree_id = w.id AND c.released_at IS NULL
                     ORDER BY c.worktree_id ASC, c.session_id ASC"
                );
                let mut statement = transaction
                    .prepare(&sql)
                    .map_err(|error| db_error("failed preparing live worktree claims", error))?;
                let rows = statement
                    .query_map(params_from_iter(chunk), |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(|error| db_error("failed querying live worktree claims", error))?;
                for (worktree_id, session_id) in collect_rows(rows)? {
                    active_claims
                        .entry(worktree_id)
                        .or_default()
                        .push(session_id);
                }
            }
            let sql = format!(
                "SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                        worktree_name, unified_workspace_path, deletion_policy,
                        created_by_session_id, created_by_operation_id,
                        created_at, updated_at, revision
                 FROM managed_worktrees
                 WHERE repo_root IN ({placeholders})
                 ORDER BY worktree_name ASC, id ASC"
            );
            let mut statement = transaction
                .prepare(&sql)
                .map_err(|error| db_error("failed preparing scoped worktree status", error))?;
            let rows = statement
                .query_map(params_from_iter(chunk), |row| {
                    Ok((worktree_from_row(row)?, row.get::<_, i64>(12)?))
                })
                .map_err(|error| db_error("failed querying scoped worktree status", error))?;
            for (worktree, revision) in collect_rows(rows)? {
                let revision = u64::try_from(revision).map_err(|_| {
                    RuntimeError::ProtocolViolation(
                        "negative managed worktree status revision".into(),
                    )
                })?;
                let active_claim_session_ids =
                    active_claims.remove(&worktree.id).unwrap_or_default();
                output.push(ManagedWorktreeStatusProjection {
                    worktree,
                    revision,
                    active_claim_session_ids,
                });
            }
        }
        output.sort_by(|left, right| {
            left.worktree
                .worktree_name
                .cmp(&right.worktree.worktree_name)
                .then(left.worktree.id.cmp(&right.worktree.id))
        });
        Ok(output)
    }
}

fn worktree_from_row(row: &Row<'_>) -> rusqlite::Result<ManagedWorktreeRecord> {
    Ok(ManagedWorktreeRecord {
        id: row.get(0)?,
        repo_root: row.get(1)?,
        worktree_root: row.get(2)?,
        worktree_cwd: row.get(3)?,
        branch_name: row.get(4)?,
        worktree_name: row.get(5)?,
        unified_workspace_path: row.get(6)?,
        deletion_policy: row.get(7)?,
        created_by_session_id: row.get(8)?,
        created_by_operation_id: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}
