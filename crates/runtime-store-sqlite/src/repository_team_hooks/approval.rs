use std::collections::HashSet;

use runtime_core::{RuntimeError, TeamHookApproval, TeamHookApprovalUpdate, TeamHookResolvedEntry};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn get_team_hook_approval(
        &self,
        workspace_id: &str,
    ) -> Result<TeamHookApproval, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = ?1)",
                [workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking hook approval workspace", error))?;
        if !exists {
            return Err(RuntimeError::NotFound(format!("workspace {workspace_id}")));
        }
        let existing: Option<(i64, String, i64)> = connection
            .query_row(
                "SELECT revision, approved_hooks_json, approved_at_ms
                 FROM workspace_hook_approvals WHERE workspace_id = ?1",
                [workspace_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading hook approval", error))?;
        approval_from_row(workspace_id, existing)
    }

    /// This mutation must only be called by the authenticated operator path.
    /// Store exact resolved commands, not user-submitted shell strings.
    pub fn update_team_hook_approval(
        &self,
        update: &TeamHookApprovalUpdate,
    ) -> Result<TeamHookApproval, RuntimeError> {
        let mut seen = HashSet::new();
        if update.hooks.len() > 6 * 32
            || update.hooks.iter().any(|hook| {
                hook.name.is_empty()
                    || hook.name.len() > 128
                    || hook.command.is_empty()
                    || hook.command.len() > 4096
                    || !(100..=120_000).contains(&hook.timeout_ms)
                    || !seen.insert((hook.event_name, hook.name.clone()))
            })
        {
            return Err(RuntimeError::InvalidState(
                "invalid or duplicate hook approval snapshot".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting hook approval transaction", error))?;
        let lifecycle: Option<String> = transaction
            .query_row(
                "SELECT lifecycle_state FROM workspaces WHERE workspace_id = ?1",
                [&update.workspace_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading hook approval workspace", error))?;
        match lifecycle.as_deref() {
            None => {
                return Err(RuntimeError::NotFound(format!(
                    "workspace {}",
                    update.workspace_id
                )))
            }
            Some("active") => {}
            _ => {
                return Err(RuntimeError::Conflict(
                    "cannot approve hooks for a retired workspace".into(),
                ))
            }
        }
        let existing: Option<(i64, String, i64)> = transaction
            .query_row(
                "SELECT revision, approved_hooks_json, approved_at_ms
                 FROM workspace_hook_approvals WHERE workspace_id = ?1",
                [&update.workspace_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading previous hook approval", error))?;
        let current = approval_from_row(&update.workspace_id, existing)?;
        if current.revision != update.expected_revision {
            return Err(RuntimeError::Conflict(format!(
                "hook approval revision changed from {} to {}",
                update.expected_revision, current.revision
            )));
        }
        let revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| RuntimeError::InvalidState("hook approval revision overflow".into()))?;
        transaction
            .execute(
                "INSERT INTO workspace_hook_approvals (workspace_id,revision,approved_hooks_json,approved_at_ms)
                 VALUES (?1,?2,?3,?4)
                 ON CONFLICT(workspace_id) DO UPDATE SET revision=excluded.revision,
                   approved_hooks_json=excluded.approved_hooks_json,approved_at_ms=excluded.approved_at_ms",
                params![
                    update.workspace_id,
                    i64::try_from(revision).map_err(|_| RuntimeError::InvalidState("hook approval revision overflow".into()))?,
                    json_to_string(&serde_json::to_value(&update.hooks)
                        .map_err(|error| RuntimeError::Bootstrap(format!("invalid hook snapshot: {error}")))?)?,
                    update.approved_at_ms,
                ],
            )
            .map_err(|error| db_error("failed recording hook approval", error))?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing hook approval", error))?;
        Ok(TeamHookApproval {
            workspace_id: update.workspace_id.clone(),
            revision,
            hooks: update.hooks.clone(),
            approved_at_ms: Some(update.approved_at_ms),
        })
    }
}

fn approval_from_row(
    workspace_id: &str,
    row: Option<(i64, String, i64)>,
) -> Result<TeamHookApproval, RuntimeError> {
    let Some((revision, hooks_json, approved_at_ms)) = row else {
        return Ok(TeamHookApproval {
            workspace_id: workspace_id.into(),
            revision: 0,
            hooks: Vec::new(),
            approved_at_ms: None,
        });
    };
    let hooks =
        serde_json::from_str::<Vec<TeamHookResolvedEntry>>(&hooks_json).map_err(|error| {
            RuntimeError::Bootstrap(format!("corrupt hook approval snapshot: {error}"))
        })?;
    Ok(TeamHookApproval {
        workspace_id: workspace_id.into(),
        revision: u64::try_from(revision)
            .map_err(|_| RuntimeError::Bootstrap("negative hook approval revision".into()))?,
        hooks,
        approved_at_ms: Some(approved_at_ms),
    })
}
