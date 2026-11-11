use runtime_core::{
    ManagedWorktreeRecord, NewRuntimeEvent, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::SqliteRuntimeRepository;

/// The legacy normalizer, native worktree creation, and recovery path must all
/// agree on the same `(repo_root, cwd, branch)` authority key. Return the
/// effective persisted ID, not a temporary proposed ID, for event identity.
pub(crate) fn upsert_managed_worktree_in_transaction(
    tx: &Transaction<'_>,
    record: &ManagedWorktreeRecord,
) -> Result<ManagedWorktreeRecord, RuntimeError> {
    let existing_id: Option<String> = tx
        .query_row(
            "SELECT id FROM managed_worktrees
             WHERE repo_root = ?1 AND worktree_cwd = ?2 AND branch_name = ?3",
            params![record.repo_root, record.worktree_cwd, record.branch_name],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed querying managed worktree by identity key", error))?;
    let effective = ManagedWorktreeRecord {
        id: existing_id.clone().unwrap_or_else(|| record.id.clone()),
        ..record.clone()
    };
    if existing_id.is_some() {
        tx.execute(
            "UPDATE managed_worktrees
             SET repo_root = ?2, worktree_root = ?3, worktree_cwd = ?4,
                 branch_name = ?5, worktree_name = ?6, unified_workspace_path = ?7,
                 deletion_policy = ?8, created_by_session_id = ?9,
                 created_by_operation_id = ?10, created_at = ?11,
                 updated_at = ?12, revision = revision + 1 WHERE id = ?1",
            params![
                effective.id,
                effective.repo_root,
                effective.worktree_root,
                effective.worktree_cwd,
                effective.branch_name,
                effective.worktree_name,
                effective.unified_workspace_path,
                effective.deletion_policy,
                effective.created_by_session_id,
                effective.created_by_operation_id,
                effective.created_at,
                effective.updated_at,
            ],
        )
        .map_err(|error| db_error("failed updating managed worktree by identity key", error))?;
    } else {
        tx.execute(
            "INSERT INTO managed_worktrees (
                id, repo_root, worktree_root, worktree_cwd, branch_name, worktree_name,
                unified_workspace_path, deletion_policy, created_by_session_id,
                created_by_operation_id, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(id) DO UPDATE SET
                repo_root = excluded.repo_root,
                worktree_root = excluded.worktree_root,
                worktree_cwd = excluded.worktree_cwd,
                branch_name = excluded.branch_name,
                worktree_name = excluded.worktree_name,
                unified_workspace_path = excluded.unified_workspace_path,
                deletion_policy = excluded.deletion_policy,
                created_by_session_id = excluded.created_by_session_id,
                created_by_operation_id = excluded.created_by_operation_id,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                revision = managed_worktrees.revision + 1",
            params![
                effective.id,
                effective.repo_root,
                effective.worktree_root,
                effective.worktree_cwd,
                effective.branch_name,
                effective.worktree_name,
                effective.unified_workspace_path,
                effective.deletion_policy,
                effective.created_by_session_id,
                effective.created_by_operation_id,
                effective.created_at,
                effective.updated_at,
            ],
        )
        .map_err(|error| db_error("failed upserting managed worktree", error))?;
    }
    Ok(effective)
}

impl SqliteRuntimeRepository {
    /// Git has already created or independently verified this checkout. The
    /// native effect itself cannot be rolled back by SQLite. Its managed row
    /// and evidence event, however, must never commit separately. Callers with
    /// a native-effect operation ID mark unknown commit failures unresolved.
    pub fn record_managed_worktree_event(
        &self,
        record: &ManagedWorktreeRecord,
        init_script_status: &str,
        recovered: bool,
        session_id: Option<&str>,
        team_id: Option<&str>,
    ) -> Result<ManagedWorktreeRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning managed worktree evidence", error))?;
        let effective = upsert_managed_worktree_in_transaction(&tx, record)?;
        let next_seq: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events
                 WHERE scope = 'worktree' AND scope_id = ?1",
                [&effective.id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed computing worktree evidence identity", error))?;
        insert_runtime_event_in_transaction(
            &tx,
            &NewRuntimeEvent {
                event_id: format!(
                    "evt_wt_record_{}_{}_{}",
                    effective.id.len(),
                    effective.id,
                    next_seq
                ),
                scope: RuntimeEventScope::Worktree,
                scope_id: effective.id.clone(),
                session_id: session_id.map(str::to_owned),
                team_id: team_id.map(str::to_owned),
                turn_id: None,
                kind: if recovered {
                    "worktree.recovered"
                } else {
                    "worktree.created"
                }
                .into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({
                    "worktree": effective,
                    "init_script_status": init_script_status,
                }),
                provider: None,
                provider_seq: None,
                created_at: effective.updated_at,
            },
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing managed worktree event", error))?;
        Ok(effective)
    }
}
