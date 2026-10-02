use runtime_core::{
    ProviderKind, RuntimeError, SessionRecord, WorkspaceAgentLifecycleState, WorkspaceAgentProfile,
    WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy,
};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection, string_to_json};
use crate::SqliteRuntimeRepository;

const AGENT_SELECT: &str = "SELECT
    a.session_id, a.workspace_id, a.identity_alias, a.lifecycle_state,
    p.title, p.title_provenance, p.added_by, p.creator_session_id,
    p.creator_compaction_subscription, p.joined_at, a.recreation_policy_json,
    s.provider_session_ref, s.canonical_provider_session_ref, s.metadata_json,
    a.archived_at, a.archive_reason, a.revision, a.created_at, a.updated_at
 FROM workspace_agents a
 JOIN workspace_agent_profiles p ON p.session_id = a.session_id AND p.workspace_id = a.workspace_id
 JOIN sessions s ON s.id = a.session_id";

impl SqliteRuntimeRepository {
    pub fn create_workspace_agent(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
    ) -> Result<(), RuntimeError> {
        if session.id != agent.agent_id
            || session.provider != agent.recreation_policy.provider.as_str()
            || session.cwd.as_deref() != Some(agent.recreation_policy.authoritative_cwd.as_str())
        {
            return Err(RuntimeError::ProtocolViolation(
                "workspace agent session does not match recreation policy".to_string(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting workspace agent transaction", error))?;
        let lifecycle = tx
            .query_row(
                "SELECT lifecycle_state FROM workspaces WHERE workspace_id = ?1",
                params![agent.workspace_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace for agent create", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", agent.workspace_id)))?;
        if lifecycle != "active" {
            return Err(RuntimeError::InvalidState(format!(
                "workspace {} is not active",
                agent.workspace_id
            )));
        }

        insert_session(&tx, session)?;
        tx.execute(
            "INSERT INTO workspace_session_ownership (
                session_id, workspace_id, source, source_operation_id, created_at
             ) VALUES (?1, ?2, 'v2_workspace_agent_create', NULL, ?3)",
            params![agent.agent_id, agent.workspace_id, agent.created_at],
        )
        .map_err(|error| db_error("failed inserting workspace session ownership", error))?;
        tx.execute(
            "INSERT INTO workspace_agent_profiles (
                session_id, workspace_id, title, title_provenance, added_by,
                creator_session_id, creator_compaction_subscription, joined_at,
                revision, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10)",
            params![
                agent.agent_id,
                agent.workspace_id,
                agent.profile.title,
                agent.profile.title_provenance,
                agent.profile.added_by,
                agent.profile.creator_session_id,
                agent.profile.creator_compaction_subscription,
                agent.profile.joined_at,
                agent.created_at,
                agent.updated_at,
            ],
        )
        .map_err(|error| db_error("failed inserting workspace agent profile", error))?;
        tx.execute(
            "INSERT INTO workspace_agents (
                session_id, workspace_id, identity_alias, lifecycle_state,
                recreation_policy_json, archived_at, archive_reason, revision,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                agent.agent_id,
                agent.workspace_id,
                agent.alias,
                agent.lifecycle_state.as_str(),
                serde_json::to_string(&agent.recreation_policy).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "failed serializing workspace agent recreation policy: {error}"
                    ))
                })?,
                agent.archived_at,
                agent.archive_reason,
                i64::try_from(agent.revision).map_err(|_| RuntimeError::Bootstrap(
                    "workspace agent revision overflow".to_string()
                ))?,
                agent.created_at,
                agent.updated_at,
            ],
        )
        .map_err(|error| {
            let message = error.to_string();
            if message.contains("workspace_agents.identity_alias") {
                RuntimeError::Conflict(format!(
                    "workspace agent alias {} already exists",
                    agent.alias
                ))
            } else {
                db_error("failed inserting workspace agent authority", error)
            }
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing workspace agent create", error))?;
        Ok(())
    }

    pub fn list_workspace_agents(
        &self,
        workspace_id: &str,
        lifecycle: Option<WorkspaceAgentLifecycleState>,
    ) -> Result<Vec<WorkspaceAgentRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let (query, lifecycle_arg) = match lifecycle {
            Some(lifecycle) => (
                format!(
                    "{AGENT_SELECT} WHERE a.workspace_id = ?1 AND a.lifecycle_state = ?2 ORDER BY a.created_at, a.session_id"
                ),
                Some(lifecycle.as_str()),
            ),
            None => (
                format!(
                    "{AGENT_SELECT} WHERE a.workspace_id = ?1 ORDER BY a.created_at, a.session_id"
                ),
                None,
            ),
        };
        let mut statement = connection
            .prepare(query.as_str())
            .map_err(|error| db_error("failed preparing workspace agent list", error))?;
        let mut records = Vec::new();
        if let Some(lifecycle) = lifecycle_arg {
            let rows = statement
                .query_map(params![workspace_id, lifecycle], workspace_agent_from_row)
                .map_err(|error| db_error("failed listing workspace agents", error))?;
            for row in rows {
                records.push(row.map_err(|error| db_error("invalid workspace agent row", error))?);
            }
        } else {
            let rows = statement
                .query_map(params![workspace_id], workspace_agent_from_row)
                .map_err(|error| db_error("failed listing workspace agents", error))?;
            for row in rows {
                records.push(row.map_err(|error| db_error("invalid workspace agent row", error))?);
            }
        }
        Ok(records)
    }

    pub fn get_workspace_agent(
        &self,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let query = format!("{AGENT_SELECT} WHERE a.workspace_id = ?1 AND a.session_id = ?2");
        connection
            .query_row(
                query.as_str(),
                params![workspace_id, agent_id],
                workspace_agent_from_row,
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace agent", error))
    }

    pub fn get_workspace_agent_by_id(
        &self,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let query = format!("{AGENT_SELECT} WHERE a.session_id = ?1");
        connection
            .query_row(query.as_str(), params![agent_id], workspace_agent_from_row)
            .optional()
            .map_err(|error| db_error("failed reading workspace agent by id", error))
    }

    pub fn set_workspace_agent_lifecycle(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        lifecycle: WorkspaceAgentLifecycleState,
        archive_reason: Option<&str>,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        if session.id != agent_id {
            return Err(RuntimeError::ProtocolViolation(
                "workspace agent lifecycle session mismatch".to_string(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed starting workspace agent lifecycle transaction",
                    error,
                )
            })?;
        let workspace_id = tx
            .query_row(
                "SELECT workspace_id FROM workspace_agents WHERE session_id = ?1",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace agent lifecycle", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        update_session(&tx, session)?;
        let (archived_at, archive_reason) = match lifecycle {
            WorkspaceAgentLifecycleState::Active => (None, None),
            WorkspaceAgentLifecycleState::Archived => {
                tx.execute(
                    "UPDATE workspaces
                     SET lead_agent_id = NULL, revision = revision + 1, updated_at = ?3
                     WHERE workspace_id = ?1 AND lead_agent_id = ?2",
                    params![workspace_id, agent_id, changed_at],
                )
                .map_err(|error| db_error("failed clearing archived workspace lead", error))?;
                (Some(changed_at), archive_reason.map(str::to_string))
            }
        };
        tx.execute(
            "UPDATE workspace_agents
             SET lifecycle_state = ?2, archived_at = ?3, archive_reason = ?4,
                 revision = revision + 1, updated_at = ?5
             WHERE session_id = ?1",
            params![
                agent_id,
                lifecycle.as_str(),
                archived_at,
                archive_reason,
                changed_at
            ],
        )
        .map_err(|error| db_error("failed updating workspace agent lifecycle", error))?;
        tx.commit()
            .map_err(|error| db_error("failed committing workspace agent lifecycle", error))?;
        self.get_workspace_agent(workspace_id.as_str(), agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))
    }
}

fn insert_session(connection: &Connection, record: &SessionRecord) -> Result<(), RuntimeError> {
    connection
        .execute(
            "INSERT INTO sessions (
                id, provider, status, cwd, model, permission_mode, system_prompt, metadata_json,
                provider_session_ref, canonical_provider_session_ref, active_turn_id, worktree_id,
                created_at, updated_at, closed_at, failure_code, failure_message
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            rusqlite::params_from_iter(session_params(record)?),
        )
        .map_err(|error| db_error("failed inserting workspace agent session", error))?;
    Ok(())
}

fn update_session(connection: &Connection, record: &SessionRecord) -> Result<(), RuntimeError> {
    let changed = connection
        .execute(
            "UPDATE sessions SET
                provider = ?2, status = ?3, cwd = ?4, model = ?5, permission_mode = ?6,
                system_prompt = ?7, metadata_json = ?8, provider_session_ref = ?9,
                canonical_provider_session_ref = ?10, active_turn_id = ?11, worktree_id = ?12,
                updated_at = ?14, closed_at = ?15, failure_code = ?16, failure_message = ?17
             WHERE id = ?1",
            rusqlite::params_from_iter(session_params(record)?),
        )
        .map_err(|error| db_error("failed updating workspace agent session", error))?;
    if changed != 1 {
        return Err(RuntimeError::NotFound(format!("session {}", record.id)));
    }
    Ok(())
}

fn session_params(record: &SessionRecord) -> Result<Vec<rusqlite::types::Value>, RuntimeError> {
    use rusqlite::types::Value as SqlValue;
    let optional = |value: &Option<String>| match value {
        Some(value) => SqlValue::Text(value.clone()),
        None => SqlValue::Null,
    };
    Ok(vec![
        SqlValue::Text(record.id.clone()),
        SqlValue::Text(record.provider.clone()),
        SqlValue::Text(record.status.clone()),
        optional(&record.cwd),
        optional(&record.model),
        optional(&record.permission_mode),
        optional(&record.system_prompt),
        SqlValue::Text(json_to_string(&record.metadata)?),
        optional(&record.provider_session_ref),
        optional(&record.canonical_provider_session_ref),
        optional(&record.active_turn_id),
        optional(&record.worktree_id),
        SqlValue::Integer(record.created_at),
        SqlValue::Integer(record.updated_at),
        record
            .closed_at
            .map(SqlValue::Integer)
            .unwrap_or(SqlValue::Null),
        optional(&record.failure_code),
        optional(&record.failure_message),
    ])
}

fn workspace_agent_from_row(row: &Row<'_>) -> rusqlite::Result<WorkspaceAgentRecord> {
    let lifecycle_raw: String = row.get(3)?;
    let lifecycle_state = WorkspaceAgentLifecycleState::from_str(lifecycle_raw.as_str())
        .ok_or_else(|| conversion_error(3, format!("invalid lifecycle state {lifecycle_raw}")))?;
    let policy_json: String = row.get(10)?;
    let recreation_policy = serde_json::from_str::<WorkspaceAgentRecreationPolicy>(&policy_json)
        .map_err(|error| conversion_error(10, error.to_string()))?;
    if ProviderKind::from_str(recreation_policy.provider.as_str()).is_none() {
        return Err(conversion_error(10, "invalid workspace agent provider"));
    }
    let revision: i64 = row.get(16)?;
    Ok(WorkspaceAgentRecord {
        agent_id: row.get(0)?,
        workspace_id: row.get(1)?,
        alias: row.get(2)?,
        lifecycle_state,
        profile: WorkspaceAgentProfile {
            title: row.get(4)?,
            title_provenance: row.get(5)?,
            added_by: row.get(6)?,
            creator_session_id: row.get(7)?,
            creator_compaction_subscription: row.get(8)?,
            joined_at: row.get(9)?,
        },
        recreation_policy,
        provider_session_ref: row.get(11)?,
        canonical_provider_session_ref: row.get(12)?,
        metadata: string_to_json(row.get(13)?)?,
        archived_at: row.get(14)?,
        archive_reason: row.get(15)?,
        revision: u64::try_from(revision).map_err(|_| conversion_error(16, "negative revision"))?,
        created_at: row.get(17)?,
        updated_at: row.get(18)?,
    })
}

fn conversion_error(index: usize, message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message.into(),
        )),
    )
}
