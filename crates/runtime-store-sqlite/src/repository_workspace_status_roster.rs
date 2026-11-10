use std::collections::{BTreeSet, HashMap};

use runtime_core::{RuntimeError, WorkspaceStatusRosterSnapshot};
use rusqlite::{params, params_from_iter, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection};
use crate::repository_workspace_agent::{workspace_agent_from_row, AGENT_SELECT};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Read the first N active members in the exact model-visible alias order,
    /// then resolve only lead and displayed-creator aliases (even when they
    /// fall outside that page). Count, roster and aliases share one snapshot.
    pub fn list_workspace_status_roster(
        &self,
        workspace_id: &str,
        lead_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<WorkspaceStatusRosterSnapshot, RuntimeError> {
        if !(1..=256).contains(&limit) {
            return Err(RuntimeError::InvalidState(
                "workspace status roster limit must be 1..=256".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting workspace roster snapshot", error))?;
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM workspace_agents
                 WHERE workspace_id=?1 AND lifecycle_state='active'",
                [workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed counting active workspace members", error))?;
        let total_active_members = usize::try_from(count).map_err(|_| {
            RuntimeError::ProtocolViolation("invalid active workspace member count".into())
        })?;

        let sql = format!(
            "{AGENT_SELECT} WHERE a.workspace_id=?1 AND a.lifecycle_state='active'
             ORDER BY a.identity_alias ASC, a.session_id ASC LIMIT ?2"
        );
        let members = {
            let mut statement = tx
                .prepare(&sql)
                .map_err(|error| db_error("failed preparing bounded workspace roster", error))?;
            let rows = statement
                .query_map(
                    params![workspace_id, limit as i64],
                    workspace_agent_from_row,
                )
                .map_err(|error| db_error("failed reading bounded workspace roster", error))?;
            collect_rows(rows)?
        };
        let mut alias_by_agent_id = members
            .iter()
            .map(|member| (member.agent_id.clone(), member.alias.clone()))
            .collect::<HashMap<_, _>>();
        let mut required = BTreeSet::new();
        if let Some(lead_id) = lead_agent_id {
            required.insert(lead_id.to_string());
        }
        for member in &members {
            if member.profile.added_by != "runtime_operator" {
                required.insert(member.profile.added_by.clone());
            }
        }
        required.retain(|id| !alias_by_agent_id.contains_key(id));
        if !required.is_empty() {
            // At most 257 binds; no history walk or one-query-per-creator.
            let placeholders = (2..required.len() + 2)
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT session_id, identity_alias FROM workspace_agents
                 WHERE workspace_id=?1 AND lifecycle_state='active'
                   AND session_id IN ({placeholders})"
            );
            let mut statement = tx
                .prepare(&sql)
                .map_err(|error| db_error("failed preparing status alias lookup", error))?;
            let ids = std::iter::once(workspace_id).chain(required.iter().map(String::as_str));
            let rows = statement
                .query_map(params_from_iter(ids), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| db_error("failed reading status alias lookup", error))?;
            alias_by_agent_id.extend(collect_rows(rows)?);
        }

        Ok(WorkspaceStatusRosterSnapshot {
            members,
            total_active_members,
            alias_by_agent_id,
        })
    }
}
