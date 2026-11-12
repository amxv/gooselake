use runtime_core::{
    ProviderKind, RuntimeError, WorkspaceActiveAgentRoute, WorkspaceAgentRecreationPolicy,
};
use rusqlite::{params, params_from_iter, types::Value};

use crate::db::{collect_rows, db_error, open_connection};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Thin SQLite read projection for worktree inventory/status. Joins retain
    /// the same authoritative roster/profile/session presence constraints as
    /// list_workspace_agents, but avoid loading their unrelated fields.
    pub fn list_workspace_active_agent_routes(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceActiveAgentRoute>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let mut statement = connection
            .prepare(
                "SELECT a.session_id, a.recreation_policy_json
                 FROM workspace_agents a
                 JOIN workspace_agent_profiles p
                   ON p.session_id=a.session_id AND p.workspace_id=a.workspace_id
                 JOIN sessions s ON s.id=a.session_id
                 WHERE a.workspace_id=?1 AND a.lifecycle_state='active'
                 ORDER BY a.created_at ASC, a.session_id ASC",
            )
            .map_err(|error| db_error("failed preparing active worktree routes", error))?;
        let rows = statement
            .query_map(params![workspace_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| db_error("failed querying active worktree routes", error))?;
        collect_rows(rows)?
            .into_iter()
            .map(decode_active_route)
            .collect()
    }

    /// A page's attached agent IDs depend only on this exact set of CWDs.
    /// Use the persisted policy JSON *only as an indexed candidate filter*,
    /// then decode and validate the unchanged authoritative model. The joins
    /// retain the full roster's profile/session presence requirements.
    pub fn list_workspace_active_agent_routes_for_cwds(
        &self,
        workspace_id: &str,
        cwds: &[String],
    ) -> Result<Vec<WorkspaceActiveAgentRoute>, RuntimeError> {
        if cwds.is_empty() {
            return Ok(Vec::new());
        }
        // HTTP selects at most 101 worktrees including one continuation row.
        if cwds.len() > 101 {
            return Err(RuntimeError::InvalidState(
                "worktree status attachment lookup exceeds page limit".into(),
            ));
        }
        let connection = open_connection(&self.database_path)?;
        let placeholders = vec!["?"; cwds.len()].join(", ");
        let sql = format!(
            "SELECT a.session_id, a.recreation_policy_json
             FROM workspace_agents a
             JOIN workspace_agent_profiles p
               ON p.session_id=a.session_id AND p.workspace_id=a.workspace_id
             JOIN sessions s ON s.id=a.session_id
             WHERE a.workspace_id=? AND a.lifecycle_state='active'
               AND json_extract(a.recreation_policy_json, '$.authoritative_cwd') IN ({placeholders})
             ORDER BY a.created_at ASC, a.session_id ASC"
        );
        let mut bindings = Vec::with_capacity(cwds.len() + 1);
        bindings.push(Value::Text(workspace_id.to_owned()));
        bindings.extend(cwds.iter().cloned().map(Value::Text));
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| db_error("failed preparing selected active worktree routes", error))?;
        let rows = statement
            .query_map(params_from_iter(&bindings), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| db_error("failed querying selected active worktree routes", error))?;
        collect_rows(rows)?
            .into_iter()
            .map(decode_active_route)
            .collect()
    }
}

pub(crate) fn decode_active_route(
    (agent_id, policy_json): (String, String),
) -> Result<WorkspaceActiveAgentRoute, RuntimeError> {
    let policy: WorkspaceAgentRecreationPolicy =
        serde_json::from_str(&policy_json).map_err(|error| {
            RuntimeError::ProtocolViolation(format!(
                "invalid persisted agent worktree route for {agent_id}: {error}"
            ))
        })?;
    if ProviderKind::from_str(policy.provider.as_str()).is_none() {
        return Err(RuntimeError::ProtocolViolation(format!(
            "invalid persisted agent provider for {agent_id}"
        )));
    }
    Ok(WorkspaceActiveAgentRoute {
        agent_id,
        authoritative_cwd: policy.authoritative_cwd,
    })
}
