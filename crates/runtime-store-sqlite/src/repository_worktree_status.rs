use std::collections::HashMap;
use std::path::Path;

use runtime_core::{ManagedWorktreeRecord, ManagedWorktreeStatusProjection, RuntimeError};
use rusqlite::{params_from_iter, OptionalExtension, Row, ToSql, TransactionBehavior};

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

    /// Fetch at most `limit` worktree records and their live claims from one
    /// read snapshot. The cursor point lookup must belong to the same actual
    /// canonical repo, including historical alias spellings, and must still
    /// be visible. A missing cursor is reported to the caller, not skipped.
    pub fn list_managed_worktree_status_for_root_page(
        &self,
        canonical_repo_root: &str,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<(Vec<ManagedWorktreeStatusProjection>, bool), RuntimeError> {
        if !(1..=101).contains(&limit) {
            return Err(RuntimeError::InvalidState(
                "worktree status read limit must be between 1 and 101".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed beginning paged worktree snapshot", error))?;
        let requested_root = Path::new(canonical_repo_root);
        let cursor_valid = if let Some((name, id)) = after {
            let record: Option<(String, String, String)> = transaction
                .query_row(
                    "SELECT repo_root, worktree_cwd, worktree_name
                     FROM managed_worktrees WHERE id=?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(|error| db_error("failed checking worktree page cursor", error))?;
            record.is_some_and(|(root, cwd, persisted_name)| {
                persisted_name == name
                    && !cwd.starts_with("__gg_tombstoned__/")
                    && std::fs::canonicalize(root).ok().as_deref() == Some(requested_root)
            })
        } else {
            true
        };
        if !cursor_valid {
            return Ok((Vec::new(), false));
        }

        let page_size = i64::try_from(limit).expect("bounded worktree page fits i64");
        let mut selected = Vec::new();
        // A legacy alias may resolve differently after a filesystem symlink
        // changes, so a stale persisted canonical-root lookup is not safe.
        // Stream DISTINCT spellings rather than materializing a potentially
        // enormous global root catalog (or even all matching aliases). Only
        // 200 aliases and at most one page of candidates reside in memory.
        {
            let mut statement = transaction
                .prepare("SELECT DISTINCT repo_root FROM managed_worktrees ORDER BY repo_root")
                .map_err(|error| db_error("failed preparing paged worktree roots", error))?;
            let mut rows = statement
                .query([])
                .map_err(|error| db_error("failed querying paged worktree roots", error))?;
            let mut matching_roots = Vec::with_capacity(200);
            while let Some(row) = rows
                .next()
                .map_err(|error| db_error("failed reading paged worktree roots", error))?
            {
                let root: String = row
                    .get(0)
                    .map_err(|error| db_error("failed decoding paged worktree root", error))?;
                if std::fs::canonicalize(&root).ok().as_deref() != Some(requested_root) {
                    continue;
                }
                matching_roots.push(root);
                if matching_roots.len() == 200 {
                    append_worktree_page_candidates(
                        &transaction,
                        &matching_roots,
                        after,
                        page_size,
                        limit,
                        &mut selected,
                    )?;
                    matching_roots.clear();
                }
            }
            if !matching_roots.is_empty() {
                append_worktree_page_candidates(
                    &transaction,
                    &matching_roots,
                    after,
                    page_size,
                    limit,
                    &mut selected,
                )?;
            }
        }

        let mut active_claims: HashMap<String, Vec<String>> = HashMap::new();
        if !selected.is_empty() {
            let ids = selected
                .iter()
                .map(|(row, _)| row.id.as_str())
                .collect::<Vec<_>>();
            let placeholders = vec!["?"; ids.len()].join(", ");
            let sql = format!(
                "SELECT worktree_id, session_id FROM managed_worktree_claims
                 WHERE worktree_id IN ({placeholders}) AND released_at IS NULL
                 ORDER BY worktree_id ASC, session_id ASC"
            );
            let mut statement = transaction
                .prepare(&sql)
                .map_err(|error| db_error("failed preparing paged worktree claims", error))?;
            let rows = statement
                .query_map(params_from_iter(ids), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| db_error("failed querying paged worktree claims", error))?;
            for (worktree_id, session_id) in collect_rows(rows)? {
                active_claims
                    .entry(worktree_id)
                    .or_default()
                    .push(session_id);
            }
        }
        let page = selected
            .into_iter()
            .map(|(worktree, revision)| {
                let revision = u64::try_from(revision).map_err(|_| {
                    RuntimeError::ProtocolViolation(
                        "negative managed worktree status revision".into(),
                    )
                })?;
                let active_claim_session_ids =
                    active_claims.remove(&worktree.id).unwrap_or_default();
                Ok(ManagedWorktreeStatusProjection {
                    worktree,
                    revision,
                    active_claim_session_ids,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        Ok((page, true))
    }
}

// Per-alias-chunk indexed walks stop after one page; merge winning candidates
// across all chunks before fetching only their live claims. This deliberately
// does not cache filesystem alias identity or change cursor membership rules.
fn append_worktree_page_candidates(
    transaction: &rusqlite::Transaction<'_>,
    roots: &[String],
    after: Option<(&str, &str)>,
    page_size: i64,
    limit: usize,
    selected: &mut Vec<(ManagedWorktreeRecord, i64)>,
) -> Result<(), RuntimeError> {
    let placeholders = vec!["?"; roots.len()].join(", ");
    let mut arguments: Vec<&dyn ToSql> = roots.iter().map(|root| root as &dyn ToSql).collect();
    let after_clause = if let Some((name, id)) = after.as_ref() {
        arguments.extend([name as &dyn ToSql, id as &dyn ToSql]);
        "AND (worktree_name, id) > (?, ?)"
    } else {
        ""
    };
    arguments.push(&page_size);
    let sql = format!(
        "SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                worktree_name, unified_workspace_path, deletion_policy,
                created_by_session_id, created_by_operation_id,
                created_at, updated_at, revision
         FROM managed_worktrees
         WHERE repo_root IN ({placeholders})
           AND worktree_cwd NOT GLOB '__gg_tombstoned__/*'
           {after_clause}
         ORDER BY worktree_name ASC, id ASC LIMIT ?"
    );
    let mut statement = transaction
        .prepare(&sql)
        .map_err(|error| db_error("failed preparing paged worktree records", error))?;
    let rows = statement
        .query_map(params_from_iter(arguments), |row| {
            Ok((worktree_from_row(row)?, row.get::<_, i64>(12)?))
        })
        .map_err(|error| db_error("failed querying paged worktree records", error))?;
    selected.extend(collect_rows(rows)?);
    selected.sort_by(|left, right| {
        left.0
            .worktree_name
            .cmp(&right.0.worktree_name)
            .then(left.0.id.cmp(&right.0.id))
    });
    selected.truncate(limit);
    Ok(())
}

pub(crate) fn worktree_from_row(row: &Row<'_>) -> rusqlite::Result<ManagedWorktreeRecord> {
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
