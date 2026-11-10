use runtime_core::{
    ProviderContextLimitObservation, ProviderKind, RuntimeError, SessionContextLimitSnapshot,
};
use rusqlite::{params, params_from_iter};

use crate::db::{collect_rows, db_error, open_connection};
use crate::SqliteRuntimeRepository;

fn as_sql_i64(label: &str, value: u64) -> Result<i64, RuntimeError> {
    i64::try_from(value)
        .map_err(|_| RuntimeError::InvalidState(format!("{label} exceeds SQLite integer range")))
}

impl SqliteRuntimeRepository {
    /// The write is admitted only while the exact provider identity, agent
    /// revision, and session update observed BEFORE contacting the provider
    /// still match durable authority. A delayed provider response cannot
    /// overwrite a resumed/rebound attachment or newer usage observation.
    pub fn record_session_context_limit(
        &self,
        snapshot: &SessionContextLimitSnapshot,
        expected_session_updated_at: i64,
    ) -> Result<bool, RuntimeError> {
        snapshot.validate()?;
        let connection = open_connection(&self.database_path)?;
        let changed = connection.execute(
            "INSERT INTO session_context_limit_snapshots (
                session_id, provider, provider_session_ref, canonical_provider_session_ref,
                agent_revision, model_context_window, last_total_tokens, remaining_percentage,
                observed_at_ms, observed_turn_id
             )
             SELECT s.id, s.provider, s.provider_session_ref, s.canonical_provider_session_ref,
                    a.revision, ?6, ?7, ?8, ?9, ?10
             FROM sessions s
             JOIN workspace_agents a ON a.session_id = s.id
             JOIN workspaces w ON w.workspace_id = a.workspace_id
             WHERE s.id = ?1 AND s.provider = ?2
               AND s.provider_session_ref = ?3
               AND s.canonical_provider_session_ref IS ?4
               AND a.revision = ?5 AND a.lifecycle_state = 'active'
               AND w.lifecycle_state = 'active'
               AND s.updated_at = ?11
               AND s.status = 'ready' AND s.active_turn_id IS NULL
             ON CONFLICT(session_id) DO UPDATE SET
               provider = excluded.provider,
               provider_session_ref = excluded.provider_session_ref,
               canonical_provider_session_ref = excluded.canonical_provider_session_ref,
               agent_revision = excluded.agent_revision,
               model_context_window = excluded.model_context_window,
               last_total_tokens = excluded.last_total_tokens,
               remaining_percentage = excluded.remaining_percentage,
               observed_at_ms = excluded.observed_at_ms,
               observed_turn_id = excluded.observed_turn_id
             WHERE excluded.agent_revision != session_context_limit_snapshots.agent_revision
                OR excluded.provider_session_ref != session_context_limit_snapshots.provider_session_ref
                OR excluded.canonical_provider_session_ref IS NOT session_context_limit_snapshots.canonical_provider_session_ref
                OR excluded.observed_at_ms > session_context_limit_snapshots.observed_at_ms",
            params![
                snapshot.agent_id,
                snapshot.provider.as_str(),
                snapshot.provider_session_ref,
                snapshot.canonical_provider_session_ref,
                as_sql_i64("agent revision", snapshot.agent_revision)?,
                as_sql_i64("context window", snapshot.observation.model_context_window)?,
                as_sql_i64("token usage", snapshot.observation.last_total_tokens)?,
                i64::from(snapshot.observation.remaining_percentage),
                snapshot.observed_at_ms,
                snapshot.observed_turn_id,
                expected_session_updated_at,
            ],
        ).map_err(|error| db_error("failed recording verified context snapshot", error))?;
        Ok(changed == 1)
    }

    /// Stale rows are retained for audit but never projected into a current
    /// agent status, including after provider reattach and workspace rebind.
    pub fn get_session_context_limit(
        &self,
        agent_id: &str,
    ) -> Result<Option<SessionContextLimitSnapshot>, RuntimeError> {
        Ok(self
            .list_session_context_limits(&[agent_id.to_owned()])?
            .into_iter()
            .next())
    }

    /// One SQL statement for a bounded status page. Provider identity,
    /// revision, lifecycle and current session status use exactly the same
    /// durable fences as a point lookup. The primary-key IN search excludes
    /// unrelated agents, even when a database contains large historic rosters.
    pub fn list_session_context_limits(
        &self,
        agent_ids: &[String],
    ) -> Result<Vec<SessionContextLimitSnapshot>, RuntimeError> {
        if agent_ids.is_empty() {
            return Ok(Vec::new());
        }
        if agent_ids.len() > 256 {
            return Err(RuntimeError::InvalidState(
                "context status batch exceeds 256 agents".into(),
            ));
        }
        let placeholders = vec!["?"; agent_ids.len()].join(", ");
        let connection = open_connection(&self.database_path)?;
        let sql = format!(
            "SELECT c.session_id, c.provider, c.provider_session_ref,
                    c.canonical_provider_session_ref, c.agent_revision,
                    c.model_context_window, c.last_total_tokens,
                    c.remaining_percentage, c.observed_at_ms, c.observed_turn_id
             FROM session_context_limit_snapshots c
             JOIN sessions s ON s.id = c.session_id
             JOIN workspace_agents a ON a.session_id = s.id
             JOIN workspaces w ON w.workspace_id = a.workspace_id
             WHERE c.session_id IN ({placeholders}) AND c.provider = s.provider
               AND c.provider_session_ref = s.provider_session_ref
               AND c.canonical_provider_session_ref IS s.canonical_provider_session_ref
               AND c.agent_revision = a.revision
               AND a.lifecycle_state = 'active'
               AND w.lifecycle_state = 'active'
               AND s.status NOT IN ('closed', 'failed')
             ORDER BY c.session_id ASC"
        );
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| db_error("failed preparing current context status batch", error))?;
        let rows = statement
            .query_map(params_from_iter(agent_ids), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            })
            .map_err(|error| db_error("failed querying current context status batch", error))?;
        collect_rows(rows)?
            .into_iter()
            .map(
                |(
                    agent_id,
                    provider,
                    provider_session_ref,
                    canonical_provider_session_ref,
                    revision,
                    window,
                    total,
                    remaining,
                    observed_at_ms,
                    observed_turn_id,
                )| {
                    let nonnegative = |label: &str, value: i64| -> Result<u64, RuntimeError> {
                        u64::try_from(value).map_err(|_| {
                            RuntimeError::Bootstrap(format!(
                                "invalid stored {label} for agent {agent_id}"
                            ))
                        })
                    };
                    let provider = ProviderKind::from_str(&provider).ok_or_else(|| {
                        RuntimeError::Bootstrap(format!(
                            "invalid stored provider for agent {agent_id}"
                        ))
                    })?;
                    let remaining_percentage = u8::try_from(remaining).map_err(|_| {
                        RuntimeError::Bootstrap(format!(
                            "invalid context percentage for agent {agent_id}"
                        ))
                    })?;
                    let snapshot = SessionContextLimitSnapshot {
                        agent_id: agent_id.clone(),
                        provider,
                        provider_session_ref,
                        canonical_provider_session_ref,
                        agent_revision: nonnegative("agent revision", revision)?,
                        observation: ProviderContextLimitObservation {
                            model_context_window: nonnegative("context window", window)?,
                            last_total_tokens: nonnegative("total tokens", total)?,
                            remaining_percentage,
                        },
                        observed_at_ms,
                        observed_turn_id,
                    };
                    snapshot.validate()?;
                    Ok(snapshot)
                },
            )
            .collect()
    }
}
