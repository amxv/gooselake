use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use runtime_core::{
    ApprovalRecord, CredentialRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
    NewRuntimeEvent, ProcessRecord, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord,
    RuntimeEventScope, RuntimeHydratedState, RuntimeStore, SessionRecord, TeamDeliveryRecord,
    TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord, TeamOperationJournalRecord,
    TeamRecord, TurnRecord,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const SCHEMA_VERSION: i64 = 1;

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  cwd TEXT,
  model TEXT,
  permission_mode TEXT,
  system_prompt TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  provider_session_ref TEXT,
  canonical_provider_session_ref TEXT,
  active_turn_id TEXT,
  worktree_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  closed_at INTEGER,
  failure_code TEXT,
  failure_message TEXT
);

CREATE TABLE IF NOT EXISTS turns (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  provider_turn_ref TEXT,
  status TEXT NOT NULL,
  input_json TEXT NOT NULL,
  source TEXT,
  started_at INTEGER,
  completed_at INTEGER,
  usage_json TEXT,
  error_json TEXT
);

CREATE TABLE IF NOT EXISTS approvals (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  turn_id TEXT NOT NULL REFERENCES turns(id),
  tool_call_id TEXT,
  provider_approval_ref TEXT,
  status TEXT NOT NULL,
  request_json TEXT NOT NULL,
  response_json TEXT,
  created_at INTEGER NOT NULL,
  resolved_at INTEGER
);

CREATE TABLE IF NOT EXISTS runtime_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  scope TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  session_id TEXT,
  team_id TEXT,
  turn_id TEXT,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  critical INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  provider TEXT,
  provider_seq INTEGER,
  created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_scope_seq
ON runtime_events(scope, scope_id, seq);

CREATE TABLE IF NOT EXISTS teams (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  lead_agent_id TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  deleted_at INTEGER
);

CREATE TABLE IF NOT EXISTS team_members (
  team_id TEXT NOT NULL REFERENCES teams(id),
  agent_id TEXT NOT NULL REFERENCES sessions(id),
  title TEXT,
  joined_at INTEGER NOT NULL,
  added_by TEXT NOT NULL,
  creator_agent_id TEXT,
  creator_compaction_subscription TEXT NOT NULL DEFAULT 'auto',
  worktree_id TEXT,
  PRIMARY KEY (team_id, agent_id)
);

CREATE TABLE IF NOT EXISTS team_messages (
  id TEXT PRIMARY KEY,
  team_id TEXT NOT NULL REFERENCES teams(id),
  scope TEXT NOT NULL,
  sender_agent_id TEXT NOT NULL,
  recipient_agent_ids_json TEXT NOT NULL,
  input_json TEXT NOT NULL,
  image_paths_json TEXT NOT NULL DEFAULT '[]',
  priority TEXT NOT NULL,
  policy TEXT NOT NULL,
  correlation_id TEXT,
  reply_to_message_id TEXT,
  idempotency_key TEXT,
  created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_team_message_idempotency
ON team_messages(team_id, sender_agent_id, scope, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS team_deliveries (
  id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL REFERENCES team_messages(id),
  team_id TEXT NOT NULL REFERENCES teams(id),
  recipient_agent_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  effective_policy TEXT,
  injection_strategy TEXT,
  injected_turn_id TEXT,
  last_error_code TEXT,
  last_error_message TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS managed_worktrees (
  id TEXT PRIMARY KEY,
  repo_root TEXT NOT NULL,
  worktree_root TEXT NOT NULL,
  worktree_cwd TEXT NOT NULL,
  branch_name TEXT NOT NULL,
  worktree_name TEXT NOT NULL,
  unified_workspace_path TEXT NOT NULL,
  deletion_policy TEXT NOT NULL,
  created_by_session_id TEXT,
  created_by_operation_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(repo_root, worktree_cwd, branch_name)
);

CREATE TABLE IF NOT EXISTS managed_worktree_claims (
  worktree_id TEXT NOT NULL REFERENCES managed_worktrees(id),
  session_id TEXT NOT NULL REFERENCES sessions(id),
  claim_role TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  released_at INTEGER,
  PRIMARY KEY (worktree_id, session_id)
);

CREATE TABLE IF NOT EXISTS processes (
  id TEXT PRIMARY KEY,
  session_id TEXT,
  tool_call_id TEXT,
  pid INTEGER,
  command_json TEXT NOT NULL,
  cwd TEXT,
  status TEXT NOT NULL,
  exit_code INTEGER,
  signal INTEGER,
  stdout_path TEXT,
  stderr_path TEXT,
  started_at INTEGER NOT NULL,
  ended_at INTEGER,
  timeout_ms INTEGER
);

CREATE TABLE IF NOT EXISTS credentials (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  profile TEXT NOT NULL,
  kind TEXT NOT NULL,
  encrypted_secret TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(provider, profile, kind)
);

CREATE TABLE IF NOT EXISTS team_operation_journal (
  operation_id TEXT PRIMARY KEY,
  team_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  stage TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS team_operation_diagnostics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id TEXT,
  team_id TEXT,
  code TEXT NOT NULL,
  message TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS diagnostics_journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subsystem TEXT NOT NULL,
  severity TEXT NOT NULL,
  code TEXT NOT NULL,
  message TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteStoreConfig {
    pub database_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SqliteRuntimeRepository {
    database_path: PathBuf,
}

impl SqliteRuntimeRepository {
    pub fn new(database_path: PathBuf) -> Self {
        Self { database_path }
    }

    pub fn initialize_schema(&self) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        apply_schema(&mut connection)?;
        Ok(())
    }

    pub fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed to start event append transaction with immediate lock",
                    error,
                )
            })?;

        if let Some(existing) = fetch_runtime_event_by_event_id(&transaction, &event.event_id)? {
            transaction
                .commit()
                .map_err(|error| db_error("failed committing idempotent event append", error))?;
            return Ok(existing);
        }

        let next_seq = transaction
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events WHERE scope = ?1 AND scope_id = ?2",
                params![event.scope.as_str(), event.scope_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| db_error("failed computing next event sequence", error))?;

        let payload_json = json_to_string(&event.payload)?;
        transaction
            .execute(
                "INSERT INTO runtime_events (
                    event_id, scope, scope_id, session_id, team_id, turn_id,
                    seq, kind, critical, payload_json, provider, provider_seq, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    event.event_id,
                    event.scope.as_str(),
                    event.scope_id,
                    event.session_id,
                    event.team_id,
                    event.turn_id,
                    next_seq,
                    event.kind,
                    event.criticality.as_i64(),
                    payload_json,
                    event.provider,
                    event.provider_seq,
                    event.created_at,
                ],
            )
            .map_err(|error| db_error("failed inserting runtime event", error))?;

        let inserted =
            fetch_runtime_event_by_event_id(&transaction, &event.event_id)?.ok_or_else(|| {
                RuntimeError::Bootstrap("inserted event missing after insert".to_string())
            })?;

        transaction
            .commit()
            .map_err(|error| db_error("failed committing event append", error))?;

        Ok(inserted)
    }

    pub fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let limit = i64::try_from(limit).map_err(|_| {
            RuntimeError::Bootstrap("runtime event query limit overflow".to_string())
        })?;

        match (scope, after_seq) {
            (Some((scope_value, scope_id)), Some(after)) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, event_id, scope, scope_id, session_id, team_id, turn_id,
                                seq, kind, critical, payload_json, provider, provider_seq, created_at
                         FROM runtime_events
                         WHERE scope = ?1 AND scope_id = ?2 AND seq > ?3
                         ORDER BY seq ASC
                         LIMIT ?4",
                    )
                    .map_err(|error| db_error("failed preparing scoped event query", error))?;
                let rows = statement
                    .query_map(
                        params![scope_value.as_str(), scope_id, after, limit],
                        |row| runtime_event_from_row(row),
                    )
                    .map_err(|error| db_error("failed querying scoped events", error))?;
                collect_rows(rows)
            }
            (Some((scope_value, scope_id)), None) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, event_id, scope, scope_id, session_id, team_id, turn_id,
                                seq, kind, critical, payload_json, provider, provider_seq, created_at
                         FROM runtime_events
                         WHERE scope = ?1 AND scope_id = ?2
                         ORDER BY seq ASC
                         LIMIT ?3",
                    )
                    .map_err(|error| db_error("failed preparing scoped event query", error))?;
                let rows = statement
                    .query_map(params![scope_value.as_str(), scope_id, limit], |row| {
                        runtime_event_from_row(row)
                    })
                    .map_err(|error| db_error("failed querying scoped events", error))?;
                collect_rows(rows)
            }
            (None, Some(after)) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, event_id, scope, scope_id, session_id, team_id, turn_id,
                                seq, kind, critical, payload_json, provider, provider_seq, created_at
                         FROM runtime_events
                         WHERE id > ?1
                         ORDER BY id ASC
                         LIMIT ?2",
                    )
                    .map_err(|error| db_error("failed preparing global event query", error))?;
                let rows = statement
                    .query_map(params![after, limit], |row| runtime_event_from_row(row))
                    .map_err(|error| db_error("failed querying global events", error))?;
                collect_rows(rows)
            }
            (None, None) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, event_id, scope, scope_id, session_id, team_id, turn_id,
                                seq, kind, critical, payload_json, provider, provider_seq, created_at
                         FROM runtime_events
                         ORDER BY id ASC
                         LIMIT ?1",
                    )
                    .map_err(|error| db_error("failed preparing global event query", error))?;
                let rows = statement
                    .query_map(params![limit], |row| runtime_event_from_row(row))
                    .map_err(|error| db_error("failed querying global events", error))?;
                collect_rows(rows)
            }
        }
    }

    pub fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO sessions (
                    id, provider, status, cwd, model, permission_mode, system_prompt, metadata_json,
                    provider_session_ref, canonical_provider_session_ref, active_turn_id, worktree_id,
                    created_at, updated_at, closed_at, failure_code, failure_message
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                 ON CONFLICT(id) DO UPDATE SET
                    provider = excluded.provider,
                    status = excluded.status,
                    cwd = excluded.cwd,
                    model = excluded.model,
                    permission_mode = excluded.permission_mode,
                    system_prompt = excluded.system_prompt,
                    metadata_json = excluded.metadata_json,
                    provider_session_ref = excluded.provider_session_ref,
                    canonical_provider_session_ref = excluded.canonical_provider_session_ref,
                    active_turn_id = excluded.active_turn_id,
                    worktree_id = excluded.worktree_id,
                    updated_at = excluded.updated_at,
                    closed_at = excluded.closed_at,
                    failure_code = excluded.failure_code,
                    failure_message = excluded.failure_message",
                params![
                    record.id,
                    record.provider,
                    record.status,
                    record.cwd,
                    record.model,
                    record.permission_mode,
                    record.system_prompt,
                    json_to_string(&record.metadata)?,
                    record.provider_session_ref,
                    record.canonical_provider_session_ref,
                    record.active_turn_id,
                    record.worktree_id,
                    record.created_at,
                    record.updated_at,
                    record.closed_at,
                    record.failure_code,
                    record.failure_message,
                ],
            )
            .map_err(|error| db_error("failed upserting session", error))?;
        Ok(())
    }

    pub fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO turns (
                    id, session_id, provider_turn_ref, status, input_json, source,
                    started_at, completed_at, usage_json, error_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(id) DO UPDATE SET
                    session_id = excluded.session_id,
                    provider_turn_ref = excluded.provider_turn_ref,
                    status = excluded.status,
                    input_json = excluded.input_json,
                    source = excluded.source,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    usage_json = excluded.usage_json,
                    error_json = excluded.error_json",
                params![
                    record.id,
                    record.session_id,
                    record.provider_turn_ref,
                    record.status,
                    json_to_string(&record.input)?,
                    record.source,
                    record.started_at,
                    record.completed_at,
                    opt_json_to_string(record.usage.as_ref())?,
                    opt_json_to_string(record.error.as_ref())?,
                ],
            )
            .map_err(|error| db_error("failed upserting turn", error))?;
        Ok(())
    }

    pub fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO approvals (
                    id, session_id, turn_id, tool_call_id, provider_approval_ref, status,
                    request_json, response_json, created_at, resolved_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(id) DO UPDATE SET
                    session_id = excluded.session_id,
                    turn_id = excluded.turn_id,
                    tool_call_id = excluded.tool_call_id,
                    provider_approval_ref = excluded.provider_approval_ref,
                    status = excluded.status,
                    request_json = excluded.request_json,
                    response_json = excluded.response_json,
                    resolved_at = excluded.resolved_at",
                params![
                    record.id,
                    record.session_id,
                    record.turn_id,
                    record.tool_call_id,
                    record.provider_approval_ref,
                    record.status,
                    json_to_string(&record.request)?,
                    opt_json_to_string(record.response.as_ref())?,
                    record.created_at,
                    record.resolved_at,
                ],
            )
            .map_err(|error| db_error("failed upserting approval", error))?;
        Ok(())
    }

    pub fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO teams (id, name, lead_agent_id, created_by, created_at, updated_at, deleted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    lead_agent_id = excluded.lead_agent_id,
                    created_by = excluded.created_by,
                    updated_at = excluded.updated_at,
                    deleted_at = excluded.deleted_at",
                params![
                    record.id,
                    record.name,
                    record.lead_agent_id,
                    record.created_by,
                    record.created_at,
                    record.updated_at,
                    record.deleted_at,
                ],
            )
            .map_err(|error| db_error("failed upserting team", error))?;
        Ok(())
    }

    pub fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO team_members (
                    team_id, agent_id, title, joined_at, added_by, creator_agent_id,
                    creator_compaction_subscription, worktree_id
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(team_id, agent_id) DO UPDATE SET
                    title = excluded.title,
                    joined_at = excluded.joined_at,
                    added_by = excluded.added_by,
                    creator_agent_id = excluded.creator_agent_id,
                    creator_compaction_subscription = excluded.creator_compaction_subscription,
                    worktree_id = excluded.worktree_id",
                params![
                    record.team_id,
                    record.agent_id,
                    record.title,
                    record.joined_at,
                    record.added_by,
                    record.creator_agent_id,
                    record.creator_compaction_subscription,
                    record.worktree_id,
                ],
            )
            .map_err(|error| db_error("failed upserting team member", error))?;
        Ok(())
    }

    pub fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "DELETE FROM team_members
                 WHERE team_id = ?1 AND agent_id = ?2",
                params![team_id, agent_id],
            )
            .map_err(|error| db_error("failed deleting team member", error))?;
        Ok(())
    }

    pub fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction()
            .map_err(|error| db_error("failed to start team message upsert transaction", error))?;

        if let Some(idempotency_key) = record.idempotency_key.as_deref() {
            let existing_id: Option<String> = transaction
                .query_row(
                    "SELECT id
                     FROM team_messages
                     WHERE team_id = ?1
                       AND sender_agent_id = ?2
                       AND scope = ?3
                       AND idempotency_key = ?4",
                    params![
                        record.team_id,
                        record.sender_agent_id,
                        record.scope,
                        idempotency_key
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| {
                    db_error(
                        "failed querying existing team message by idempotency key",
                        error,
                    )
                })?;

            if let Some(existing_id) = existing_id {
                transaction
                    .execute(
                        "UPDATE team_messages
                         SET team_id = ?2,
                             scope = ?3,
                             sender_agent_id = ?4,
                             recipient_agent_ids_json = ?5,
                             input_json = ?6,
                             image_paths_json = ?7,
                             priority = ?8,
                             policy = ?9,
                             correlation_id = ?10,
                             reply_to_message_id = ?11,
                             idempotency_key = ?12,
                             created_at = ?13
                         WHERE id = ?1",
                        params![
                            existing_id,
                            record.team_id,
                            record.scope,
                            record.sender_agent_id,
                            json_to_string(&record.recipient_agent_ids)?,
                            json_to_string(&record.input)?,
                            json_to_string(&record.image_paths)?,
                            record.priority,
                            record.policy,
                            record.correlation_id,
                            record.reply_to_message_id,
                            record.idempotency_key,
                            record.created_at,
                        ],
                    )
                    .map_err(|error| {
                        db_error("failed updating team message by idempotency key", error)
                    })?;
                transaction.commit().map_err(|error| {
                    db_error("failed committing team message logical upsert", error)
                })?;
                return Ok(());
            }
        }

        transaction
            .execute(
                "INSERT INTO team_messages (
                    id, team_id, scope, sender_agent_id, recipient_agent_ids_json,
                    input_json, image_paths_json, priority, policy, correlation_id,
                    reply_to_message_id, idempotency_key, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(id) DO UPDATE SET
                    team_id = excluded.team_id,
                    scope = excluded.scope,
                    sender_agent_id = excluded.sender_agent_id,
                    recipient_agent_ids_json = excluded.recipient_agent_ids_json,
                    input_json = excluded.input_json,
                    image_paths_json = excluded.image_paths_json,
                    priority = excluded.priority,
                    policy = excluded.policy,
                    correlation_id = excluded.correlation_id,
                    reply_to_message_id = excluded.reply_to_message_id,
                    idempotency_key = excluded.idempotency_key,
                    created_at = excluded.created_at",
                params![
                    record.id,
                    record.team_id,
                    record.scope,
                    record.sender_agent_id,
                    json_to_string(&record.recipient_agent_ids)?,
                    json_to_string(&record.input)?,
                    json_to_string(&record.image_paths)?,
                    record.priority,
                    record.policy,
                    record.correlation_id,
                    record.reply_to_message_id,
                    record.idempotency_key,
                    record.created_at,
                ],
            )
            .map_err(|error| db_error("failed upserting team message", error))?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing team message upsert", error))?;
        Ok(())
    }

    pub fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO team_deliveries (
                    id, message_id, team_id, recipient_agent_id, provider, status,
                    effective_policy, injection_strategy, injected_turn_id, last_error_code,
                    last_error_message, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(id) DO UPDATE SET
                    message_id = excluded.message_id,
                    team_id = excluded.team_id,
                    recipient_agent_id = excluded.recipient_agent_id,
                    provider = excluded.provider,
                    status = excluded.status,
                    effective_policy = excluded.effective_policy,
                    injection_strategy = excluded.injection_strategy,
                    injected_turn_id = excluded.injected_turn_id,
                    last_error_code = excluded.last_error_code,
                    last_error_message = excluded.last_error_message,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at",
                params![
                    record.id,
                    record.message_id,
                    record.team_id,
                    record.recipient_agent_id,
                    record.provider,
                    record.status,
                    record.effective_policy,
                    record.injection_strategy,
                    record.injected_turn_id,
                    record.last_error_code,
                    record.last_error_message,
                    record.created_at,
                    record.updated_at,
                ],
            )
            .map_err(|error| db_error("failed upserting team delivery", error))?;
        Ok(())
    }

    pub fn upsert_managed_worktree(
        &self,
        record: &ManagedWorktreeRecord,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection.transaction().map_err(|error| {
            db_error("failed to start managed worktree upsert transaction", error)
        })?;

        let existing_id: Option<String> = transaction
            .query_row(
                "SELECT id
                 FROM managed_worktrees
                 WHERE repo_root = ?1
                   AND worktree_cwd = ?2
                   AND branch_name = ?3",
                params![record.repo_root, record.worktree_cwd, record.branch_name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed querying managed worktree by identity key", error))?;

        if let Some(existing_id) = existing_id {
            transaction
                .execute(
                    "UPDATE managed_worktrees
                     SET repo_root = ?2,
                         worktree_root = ?3,
                         worktree_cwd = ?4,
                         branch_name = ?5,
                         worktree_name = ?6,
                         unified_workspace_path = ?7,
                         deletion_policy = ?8,
                         created_by_session_id = ?9,
                         created_by_operation_id = ?10,
                         created_at = ?11,
                         updated_at = ?12
                     WHERE id = ?1",
                    params![
                        existing_id,
                        record.repo_root,
                        record.worktree_root,
                        record.worktree_cwd,
                        record.branch_name,
                        record.worktree_name,
                        record.unified_workspace_path,
                        record.deletion_policy,
                        record.created_by_session_id,
                        record.created_by_operation_id,
                        record.created_at,
                        record.updated_at,
                    ],
                )
                .map_err(|error| {
                    db_error("failed updating managed worktree by identity key", error)
                })?;
            transaction.commit().map_err(|error| {
                db_error("failed committing managed worktree logical upsert", error)
            })?;
            return Ok(());
        }

        transaction
            .execute(
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
                    updated_at = excluded.updated_at",
                params![
                    record.id,
                    record.repo_root,
                    record.worktree_root,
                    record.worktree_cwd,
                    record.branch_name,
                    record.worktree_name,
                    record.unified_workspace_path,
                    record.deletion_policy,
                    record.created_by_session_id,
                    record.created_by_operation_id,
                    record.created_at,
                    record.updated_at,
                ],
            )
            .map_err(|error| db_error("failed upserting managed worktree", error))?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing managed worktree upsert", error))?;
        Ok(())
    }

    pub fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO managed_worktree_claims (
                    worktree_id, session_id, claim_role, created_at, released_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(worktree_id, session_id) DO UPDATE SET
                    claim_role = excluded.claim_role,
                    created_at = excluded.created_at,
                    released_at = excluded.released_at",
                params![
                    record.worktree_id,
                    record.session_id,
                    record.claim_role,
                    record.created_at,
                    record.released_at,
                ],
            )
            .map_err(|error| db_error("failed upserting managed worktree claim", error))?;
        Ok(())
    }

    pub fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO processes (
                    id, session_id, tool_call_id, pid, command_json, cwd, status,
                    exit_code, signal, stdout_path, stderr_path, started_at, ended_at, timeout_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                 ON CONFLICT(id) DO UPDATE SET
                    session_id = excluded.session_id,
                    tool_call_id = excluded.tool_call_id,
                    pid = excluded.pid,
                    command_json = excluded.command_json,
                    cwd = excluded.cwd,
                    status = excluded.status,
                    exit_code = excluded.exit_code,
                    signal = excluded.signal,
                    stdout_path = excluded.stdout_path,
                    stderr_path = excluded.stderr_path,
                    started_at = excluded.started_at,
                    ended_at = excluded.ended_at,
                    timeout_ms = excluded.timeout_ms",
                params![
                    record.id,
                    record.session_id,
                    record.tool_call_id,
                    record.pid,
                    json_to_string(&record.command)?,
                    record.cwd,
                    record.status,
                    record.exit_code,
                    record.signal,
                    record.stdout_path,
                    record.stderr_path,
                    record.started_at,
                    record.ended_at,
                    record.timeout_ms,
                ],
            )
            .map_err(|error| db_error("failed upserting process", error))?;
        Ok(())
    }

    pub fn upsert_team_operation_journal(
        &self,
        record: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO team_operation_journal (
                    operation_id, team_id, kind, stage, payload_json, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(operation_id) DO UPDATE SET
                    team_id = excluded.team_id,
                    kind = excluded.kind,
                    stage = excluded.stage,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at",
                params![
                    record.operation_id,
                    record.team_id,
                    record.kind,
                    record.stage,
                    json_to_string(&record.payload)?,
                    record.created_at,
                    record.updated_at,
                ],
            )
            .map_err(|error| db_error("failed upserting team operation journal row", error))?;
        Ok(())
    }

    pub fn append_team_operation_diagnostic(
        &self,
        operation_id: Option<&str>,
        team_id: Option<&str>,
        code: &str,
        message: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection.transaction().map_err(|error| {
            db_error(
                "failed to start team operation diagnostic transaction",
                error,
            )
        })?;
        transaction
            .execute(
                "INSERT INTO team_operation_diagnostics (
                    operation_id, team_id, code, message, payload_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    operation_id,
                    team_id,
                    code.trim(),
                    message.trim(),
                    json_to_string(payload)?,
                    created_at,
                ],
            )
            .map_err(|error| db_error("failed inserting team operation diagnostic row", error))?;
        let id = transaction.last_insert_rowid();
        let row = transaction
            .query_row(
                "SELECT id, operation_id, team_id, code, message, payload_json, created_at
                 FROM team_operation_diagnostics
                 WHERE id = ?1",
                params![id],
                |row| {
                    Ok(TeamOperationDiagnosticRecord {
                        id: row.get(0)?,
                        operation_id: row.get(1)?,
                        team_id: row.get(2)?,
                        code: row.get(3)?,
                        message: row.get(4)?,
                        payload: string_to_json(row.get(5)?)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .map_err(|error| {
                db_error(
                    "failed loading inserted team operation diagnostic row",
                    error,
                )
            })?;
        transaction.commit().map_err(|error| {
            db_error(
                "failed committing team operation diagnostic transaction",
                error,
            )
        })?;
        Ok(row)
    }

    pub fn list_team_operation_journal(
        &self,
        team_id: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        if let Some(team_id) = team_id {
            let mut statement = connection
                .prepare(
                    "SELECT operation_id, team_id, kind, stage, payload_json, created_at, updated_at
                     FROM team_operation_journal
                     WHERE team_id = ?1
                     ORDER BY updated_at ASC, operation_id ASC",
                )
                .map_err(|error| db_error("failed preparing scoped team operation journal query", error))?;
            let rows = statement
                .query_map(params![team_id], |row| {
                    Ok(TeamOperationJournalRecord {
                        operation_id: row.get(0)?,
                        team_id: row.get(1)?,
                        kind: row.get(2)?,
                        stage: row.get(3)?,
                        payload: string_to_json(row.get(4)?)?,
                        created_at: row.get(5)?,
                        updated_at: row.get(6)?,
                    })
                })
                .map_err(|error| {
                    db_error("failed running scoped team operation journal query", error)
                })?;
            return collect_rows(rows);
        }

        let mut statement = connection
            .prepare(
                "SELECT operation_id, team_id, kind, stage, payload_json, created_at, updated_at
                 FROM team_operation_journal
                 ORDER BY updated_at ASC, operation_id ASC",
            )
            .map_err(|error| db_error("failed preparing team operation journal query", error))?;
        let rows = statement
            .query_map([], |row| {
                Ok(TeamOperationJournalRecord {
                    operation_id: row.get(0)?,
                    team_id: row.get(1)?,
                    kind: row.get(2)?,
                    stage: row.get(3)?,
                    payload: string_to_json(row.get(4)?)?,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .map_err(|error| db_error("failed running team operation journal query", error))?;
        collect_rows(rows)
    }

    pub fn list_team_operation_diagnostics(
        &self,
        team_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        match (team_id, operation_id) {
            (Some(team_id), Some(operation_id)) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, operation_id, team_id, code, message, payload_json, created_at
                         FROM team_operation_diagnostics
                         WHERE team_id = ?1 AND operation_id = ?2
                         ORDER BY id ASC",
                    )
                    .map_err(|error| {
                        db_error("failed preparing team+operation diagnostics query", error)
                    })?;
                let rows = statement
                    .query_map(params![team_id, operation_id], |row| {
                        Ok(TeamOperationDiagnosticRecord {
                            id: row.get(0)?,
                            operation_id: row.get(1)?,
                            team_id: row.get(2)?,
                            code: row.get(3)?,
                            message: row.get(4)?,
                            payload: string_to_json(row.get(5)?)?,
                            created_at: row.get(6)?,
                        })
                    })
                    .map_err(|error| {
                        db_error("failed running team+operation diagnostics query", error)
                    })?;
                collect_rows(rows)
            }
            (Some(team_id), None) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, operation_id, team_id, code, message, payload_json, created_at
                         FROM team_operation_diagnostics
                         WHERE team_id = ?1
                         ORDER BY id ASC",
                    )
                    .map_err(|error| db_error("failed preparing team diagnostics query", error))?;
                let rows = statement
                    .query_map(params![team_id], |row| {
                        Ok(TeamOperationDiagnosticRecord {
                            id: row.get(0)?,
                            operation_id: row.get(1)?,
                            team_id: row.get(2)?,
                            code: row.get(3)?,
                            message: row.get(4)?,
                            payload: string_to_json(row.get(5)?)?,
                            created_at: row.get(6)?,
                        })
                    })
                    .map_err(|error| db_error("failed running team diagnostics query", error))?;
                collect_rows(rows)
            }
            (None, Some(operation_id)) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, operation_id, team_id, code, message, payload_json, created_at
                         FROM team_operation_diagnostics
                         WHERE operation_id = ?1
                         ORDER BY id ASC",
                    )
                    .map_err(|error| {
                        db_error("failed preparing operation diagnostics query", error)
                    })?;
                let rows = statement
                    .query_map(params![operation_id], |row| {
                        Ok(TeamOperationDiagnosticRecord {
                            id: row.get(0)?,
                            operation_id: row.get(1)?,
                            team_id: row.get(2)?,
                            code: row.get(3)?,
                            message: row.get(4)?,
                            payload: string_to_json(row.get(5)?)?,
                            created_at: row.get(6)?,
                        })
                    })
                    .map_err(|error| {
                        db_error("failed running operation diagnostics query", error)
                    })?;
                collect_rows(rows)
            }
            (None, None) => {
                let mut statement = connection
                    .prepare(
                        "SELECT id, operation_id, team_id, code, message, payload_json, created_at
                         FROM team_operation_diagnostics
                         ORDER BY id ASC",
                    )
                    .map_err(|error| db_error("failed preparing diagnostics query", error))?;
                let rows = statement
                    .query_map([], |row| {
                        Ok(TeamOperationDiagnosticRecord {
                            id: row.get(0)?,
                            operation_id: row.get(1)?,
                            team_id: row.get(2)?,
                            code: row.get(3)?,
                            message: row.get(4)?,
                            payload: string_to_json(row.get(5)?)?,
                            created_at: row.get(6)?,
                        })
                    })
                    .map_err(|error| db_error("failed running diagnostics query", error))?;
                collect_rows(rows)
            }
        }
    }

    pub fn upsert_credential(&self, record: &CredentialRecord) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction()
            .map_err(|error| db_error("failed to start credential upsert transaction", error))?;

        let existing_id: Option<String> = transaction
            .query_row(
                "SELECT id
                 FROM credentials
                 WHERE provider = ?1
                   AND profile = ?2
                   AND kind = ?3",
                params![record.provider, record.profile, record.kind],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed querying credential by logical key", error))?;

        if let Some(existing_id) = existing_id {
            transaction
                .execute(
                    "UPDATE credentials
                     SET provider = ?2,
                         profile = ?3,
                         kind = ?4,
                         encrypted_secret = ?5,
                         metadata_json = ?6,
                         created_at = ?7,
                         updated_at = ?8
                     WHERE id = ?1",
                    params![
                        existing_id,
                        record.provider,
                        record.profile,
                        record.kind,
                        record.encrypted_secret,
                        json_to_string(&record.metadata)?,
                        record.created_at,
                        record.updated_at,
                    ],
                )
                .map_err(|error| db_error("failed updating credential by logical key", error))?;
            transaction
                .commit()
                .map_err(|error| db_error("failed committing credential logical upsert", error))?;
            return Ok(());
        }

        transaction
            .execute(
                "INSERT INTO credentials (
                    id, provider, profile, kind, encrypted_secret, metadata_json, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                    provider = excluded.provider,
                    profile = excluded.profile,
                    kind = excluded.kind,
                    encrypted_secret = excluded.encrypted_secret,
                    metadata_json = excluded.metadata_json,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at",
                params![
                    record.id,
                    record.provider,
                    record.profile,
                    record.kind,
                    record.encrypted_secret,
                    json_to_string(&record.metadata)?,
                    record.created_at,
                    record.updated_at,
                ],
            )
            .map_err(|error| db_error("failed upserting credential", error))?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing credential upsert", error))?;
        Ok(())
    }

    pub fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError> {
        let connection = open_connection(&self.database_path)?;

        let sessions = {
            let mut statement = connection
                .prepare(
                    "SELECT id, provider, status, cwd, model, permission_mode, system_prompt,
                            metadata_json, provider_session_ref, canonical_provider_session_ref,
                            active_turn_id, worktree_id, created_at, updated_at, closed_at,
                            failure_code, failure_message
                     FROM sessions
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing session hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(SessionRecord {
                        id: row.get(0)?,
                        provider: row.get(1)?,
                        status: row.get(2)?,
                        cwd: row.get(3)?,
                        model: row.get(4)?,
                        permission_mode: row.get(5)?,
                        system_prompt: row.get(6)?,
                        metadata: string_to_json(row.get(7)?)?,
                        provider_session_ref: row.get(8)?,
                        canonical_provider_session_ref: row.get(9)?,
                        active_turn_id: row.get(10)?,
                        worktree_id: row.get(11)?,
                        created_at: row.get(12)?,
                        updated_at: row.get(13)?,
                        closed_at: row.get(14)?,
                        failure_code: row.get(15)?,
                        failure_message: row.get(16)?,
                    })
                })
                .map_err(|error| db_error("failed running session hydration query", error))?;
            collect_rows(rows)?
        };

        let turns = {
            let mut statement = connection
                .prepare(
                    "SELECT id, session_id, provider_turn_ref, status, input_json, source,
                            started_at, completed_at, usage_json, error_json
                     FROM turns
                     ORDER BY rowid ASC",
                )
                .map_err(|error| db_error("failed preparing turn hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    let usage_json: Option<String> = row.get(8)?;
                    let error_json: Option<String> = row.get(9)?;
                    Ok(TurnRecord {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        provider_turn_ref: row.get(2)?,
                        status: row.get(3)?,
                        input: string_to_json(row.get(4)?)?,
                        source: row.get(5)?,
                        started_at: row.get(6)?,
                        completed_at: row.get(7)?,
                        usage: opt_string_to_json(usage_json)?,
                        error: opt_string_to_json(error_json)?,
                    })
                })
                .map_err(|error| db_error("failed running turn hydration query", error))?;
            collect_rows(rows)?
        };

        let approvals = {
            let mut statement = connection
                .prepare(
                    "SELECT id, session_id, turn_id, tool_call_id, provider_approval_ref,
                            status, request_json, response_json, created_at, resolved_at
                     FROM approvals
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing approval hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    let response_json: Option<String> = row.get(7)?;
                    Ok(ApprovalRecord {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        turn_id: row.get(2)?,
                        tool_call_id: row.get(3)?,
                        provider_approval_ref: row.get(4)?,
                        status: row.get(5)?,
                        request: string_to_json(row.get(6)?)?,
                        response: opt_string_to_json(response_json)?,
                        created_at: row.get(8)?,
                        resolved_at: row.get(9)?,
                    })
                })
                .map_err(|error| db_error("failed running approval hydration query", error))?;
            collect_rows(rows)?
        };

        let teams = {
            let mut statement = connection
                .prepare(
                    "SELECT id, name, lead_agent_id, created_by, created_at, updated_at, deleted_at
                     FROM teams
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing team hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamRecord {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        lead_agent_id: row.get(2)?,
                        created_by: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                        deleted_at: row.get(6)?,
                    })
                })
                .map_err(|error| db_error("failed running team hydration query", error))?;
            collect_rows(rows)?
        };

        let team_members = {
            let mut statement = connection
                .prepare(
                    "SELECT team_id, agent_id, title, joined_at, added_by, creator_agent_id,
                            creator_compaction_subscription, worktree_id
                     FROM team_members
                     ORDER BY joined_at ASC, team_id ASC, agent_id ASC",
                )
                .map_err(|error| db_error("failed preparing team member hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamMemberRecord {
                        team_id: row.get(0)?,
                        agent_id: row.get(1)?,
                        title: row.get(2)?,
                        joined_at: row.get(3)?,
                        added_by: row.get(4)?,
                        creator_agent_id: row.get(5)?,
                        creator_compaction_subscription: row.get(6)?,
                        worktree_id: row.get(7)?,
                    })
                })
                .map_err(|error| db_error("failed running team member hydration query", error))?;
            collect_rows(rows)?
        };

        let team_messages = {
            let mut statement = connection
                .prepare(
                    "SELECT id, team_id, scope, sender_agent_id, recipient_agent_ids_json,
                            input_json, image_paths_json, priority, policy, correlation_id,
                            reply_to_message_id, idempotency_key, created_at
                     FROM team_messages
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing team message hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamMessageRecord {
                        id: row.get(0)?,
                        team_id: row.get(1)?,
                        scope: row.get(2)?,
                        sender_agent_id: row.get(3)?,
                        recipient_agent_ids: string_to_json(row.get(4)?)?,
                        input: string_to_json(row.get(5)?)?,
                        image_paths: string_to_json(row.get(6)?)?,
                        priority: row.get(7)?,
                        policy: row.get(8)?,
                        correlation_id: row.get(9)?,
                        reply_to_message_id: row.get(10)?,
                        idempotency_key: row.get(11)?,
                        created_at: row.get(12)?,
                    })
                })
                .map_err(|error| db_error("failed running team message hydration query", error))?;
            collect_rows(rows)?
        };

        let team_deliveries = {
            let mut statement = connection
                .prepare(
                    "SELECT id, message_id, team_id, recipient_agent_id, provider, status,
                            effective_policy, injection_strategy, injected_turn_id,
                            last_error_code, last_error_message, created_at, updated_at
                     FROM team_deliveries
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing team delivery hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamDeliveryRecord {
                        id: row.get(0)?,
                        message_id: row.get(1)?,
                        team_id: row.get(2)?,
                        recipient_agent_id: row.get(3)?,
                        provider: row.get(4)?,
                        status: row.get(5)?,
                        effective_policy: row.get(6)?,
                        injection_strategy: row.get(7)?,
                        injected_turn_id: row.get(8)?,
                        last_error_code: row.get(9)?,
                        last_error_message: row.get(10)?,
                        created_at: row.get(11)?,
                        updated_at: row.get(12)?,
                    })
                })
                .map_err(|error| db_error("failed running team delivery hydration query", error))?;
            collect_rows(rows)?
        };

        let managed_worktrees = {
            let mut statement = connection
                .prepare(
                    "SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                            worktree_name, unified_workspace_path, deletion_policy,
                            created_by_session_id, created_by_operation_id, created_at, updated_at
                     FROM managed_worktrees
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing managed worktree hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
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
                })
                .map_err(|error| {
                    db_error("failed running managed worktree hydration query", error)
                })?;
            collect_rows(rows)?
        };

        let managed_worktree_claims = {
            let mut statement = connection
                .prepare(
                    "SELECT worktree_id, session_id, claim_role, created_at, released_at
                     FROM managed_worktree_claims
                     ORDER BY created_at ASC, worktree_id ASC, session_id ASC",
                )
                .map_err(|error| {
                    db_error(
                        "failed preparing managed worktree claim hydration query",
                        error,
                    )
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(ManagedWorktreeClaimRecord {
                        worktree_id: row.get(0)?,
                        session_id: row.get(1)?,
                        claim_role: row.get(2)?,
                        created_at: row.get(3)?,
                        released_at: row.get(4)?,
                    })
                })
                .map_err(|error| {
                    db_error(
                        "failed running managed worktree claim hydration query",
                        error,
                    )
                })?;
            collect_rows(rows)?
        };

        let team_operation_journal = {
            let mut statement = connection
                .prepare(
                    "SELECT operation_id, team_id, kind, stage, payload_json, created_at, updated_at
                     FROM team_operation_journal
                     ORDER BY updated_at ASC, operation_id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing team operation journal hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamOperationJournalRecord {
                        operation_id: row.get(0)?,
                        team_id: row.get(1)?,
                        kind: row.get(2)?,
                        stage: row.get(3)?,
                        payload: string_to_json(row.get(4)?)?,
                        created_at: row.get(5)?,
                        updated_at: row.get(6)?,
                    })
                })
                .map_err(|error| {
                    db_error(
                        "failed running team operation journal hydration query",
                        error,
                    )
                })?;
            collect_rows(rows)?
        };

        let team_operation_diagnostics = {
            let mut statement = connection
                .prepare(
                    "SELECT id, operation_id, team_id, code, message, payload_json, created_at
                     FROM team_operation_diagnostics
                     ORDER BY id ASC",
                )
                .map_err(|error| {
                    db_error(
                        "failed preparing team operation diagnostics hydration query",
                        error,
                    )
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamOperationDiagnosticRecord {
                        id: row.get(0)?,
                        operation_id: row.get(1)?,
                        team_id: row.get(2)?,
                        code: row.get(3)?,
                        message: row.get(4)?,
                        payload: string_to_json(row.get(5)?)?,
                        created_at: row.get(6)?,
                    })
                })
                .map_err(|error| {
                    db_error(
                        "failed running team operation diagnostics hydration query",
                        error,
                    )
                })?;
            collect_rows(rows)?
        };

        let processes = {
            let mut statement = connection
                .prepare(
                    "SELECT id, session_id, tool_call_id, pid, command_json, cwd, status,
                            exit_code, signal, stdout_path, stderr_path, started_at, ended_at, timeout_ms
                     FROM processes
                     ORDER BY started_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing process hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(ProcessRecord {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        tool_call_id: row.get(2)?,
                        pid: row.get(3)?,
                        command: string_to_json(row.get(4)?)?,
                        cwd: row.get(5)?,
                        status: row.get(6)?,
                        exit_code: row.get(7)?,
                        signal: row.get(8)?,
                        stdout_path: row.get(9)?,
                        stderr_path: row.get(10)?,
                        started_at: row.get(11)?,
                        ended_at: row.get(12)?,
                        timeout_ms: row.get(13)?,
                    })
                })
                .map_err(|error| db_error("failed running process hydration query", error))?;
            collect_rows(rows)?
        };

        let credentials = {
            let mut statement = connection
                .prepare(
                    "SELECT id, provider, profile, kind, encrypted_secret, metadata_json,
                            created_at, updated_at
                     FROM credentials
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing credential hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(CredentialRecord {
                        id: row.get(0)?,
                        provider: row.get(1)?,
                        profile: row.get(2)?,
                        kind: row.get(3)?,
                        encrypted_secret: row.get(4)?,
                        metadata: string_to_json(row.get(5)?)?,
                        created_at: row.get(6)?,
                        updated_at: row.get(7)?,
                    })
                })
                .map_err(|error| db_error("failed running credential hydration query", error))?;
            collect_rows(rows)?
        };

        Ok(RuntimeHydratedState {
            sessions,
            turns,
            approvals,
            teams,
            team_members,
            team_messages,
            team_deliveries,
            managed_worktrees,
            managed_worktree_claims,
            team_operation_journal,
            team_operation_diagnostics,
            processes,
            credentials,
        })
    }
}

#[derive(Debug)]
pub struct SqliteRuntimeStore {
    config: SqliteStoreConfig,
    repository: SqliteRuntimeRepository,
}

impl SqliteRuntimeStore {
    pub fn new(config: SqliteStoreConfig) -> Self {
        let repository = SqliteRuntimeRepository::new(config.database_path.clone());
        Self { config, repository }
    }

    pub fn database_path(&self) -> &Path {
        &self.config.database_path
    }

    pub fn repository(&self) -> &SqliteRuntimeRepository {
        &self.repository
    }

    pub fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError> {
        self.repository.hydrate_runtime_state()
    }

    async fn ensure_parent_dir(path: &Path) -> Result<(), RuntimeError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl RuntimeStore for SqliteRuntimeStore {
    async fn initialize(&self) -> Result<(), RuntimeError> {
        Self::ensure_parent_dir(self.database_path()).await?;
        self.repository.initialize_schema()
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        let connection = open_connection(self.database_path())?;
        let _: i64 = connection
            .query_row("SELECT 1", [], |row| row.get(0))
            .map_err(|error| db_error("sqlite healthcheck query failed", error))?;
        Ok(())
    }

    fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.repository.append_runtime_event(event)
    }

    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.repository.list_runtime_events(scope, after_seq, limit)
    }

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_session(record)
    }

    fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_turn(record)
    }

    fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_approval(record)
    }

    fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team(record)
    }

    fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team_member(record)
    }

    fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError> {
        self.repository.delete_team_member(team_id, agent_id)
    }

    fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team_message(record)
    }

    fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team_delivery(record)
    }

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_managed_worktree(record)
    }

    fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        self.repository.upsert_managed_worktree_claim(record)
    }

    fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_process(record)
    }

    fn upsert_team_operation_journal(
        &self,
        record: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError> {
        self.repository.upsert_team_operation_journal(record)
    }

    fn append_team_operation_diagnostic(
        &self,
        operation_id: Option<&str>,
        team_id: Option<&str>,
        code: &str,
        message: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
        self.repository.append_team_operation_diagnostic(
            operation_id,
            team_id,
            code,
            message,
            payload,
            created_at,
        )
    }

    fn list_team_operation_journal(
        &self,
        team_id: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
        self.repository.list_team_operation_journal(team_id)
    }

    fn list_team_operation_diagnostics(
        &self,
        team_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
        self.repository
            .list_team_operation_diagnostics(team_id, operation_id)
    }

    fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError> {
        self.repository.hydrate_runtime_state()
    }
}

fn open_connection(path: &Path) -> Result<Connection, RuntimeError> {
    let connection = Connection::open(path).map_err(|error| {
        db_error(
            format!("failed to open sqlite database {}", path.display()),
            error,
        )
    })?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| db_error("failed to set sqlite busy timeout", error))?;
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )
        .map_err(|error| db_error("failed to configure sqlite pragmas", error))?;
    Ok(connection)
}

fn apply_schema(connection: &mut Connection) -> Result<(), RuntimeError> {
    let transaction = connection
        .transaction()
        .map_err(|error| db_error("failed to start sqlite schema transaction", error))?;

    transaction
        .execute_batch(SCHEMA_SQL)
        .map_err(|error| db_error("failed applying sqlite schema", error))?;

    let existing: Option<i64> = transaction
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = ?1",
            params![SCHEMA_VERSION],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading schema_migrations", error))?;

    if existing.is_none() {
        transaction
            .execute(
                "INSERT INTO schema_migrations (version, applied_at)
                 VALUES (?1, strftime('%s','now'))",
                params![SCHEMA_VERSION],
            )
            .map_err(|error| db_error("failed writing schema migration row", error))?;
    }

    transaction
        .commit()
        .map_err(|error| db_error("failed committing schema transaction", error))?;
    Ok(())
}

fn fetch_runtime_event_by_event_id(
    connection: &Connection,
    event_id: &str,
) -> Result<Option<RuntimeEventRecord>, RuntimeError> {
    connection
        .query_row(
            "SELECT id, event_id, scope, scope_id, session_id, team_id, turn_id,
                    seq, kind, critical, payload_json, provider, provider_seq, created_at
             FROM runtime_events
             WHERE event_id = ?1",
            params![event_id],
            runtime_event_from_row,
        )
        .optional()
        .map_err(|error| db_error("failed querying runtime event by event_id", error))
}

fn runtime_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RuntimeEventRecord> {
    let scope_text: String = row.get(2)?;
    let critical_value: i64 = row.get(9)?;

    let scope = RuntimeEventScope::from_str(&scope_text).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid runtime event scope '{scope_text}'"),
            )),
        )
    })?;

    let criticality = RuntimeEventCriticality::from_i64(critical_value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            9,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid runtime event criticality value '{critical_value}'"),
            )),
        )
    })?;

    let payload_json: String = row.get(10)?;
    let payload = serde_json::from_str::<Value>(&payload_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(10, rusqlite::types::Type::Text, Box::new(error))
    })?;

    Ok(RuntimeEventRecord {
        row_id: row.get(0)?,
        event_id: row.get(1)?,
        scope,
        scope_id: row.get(3)?,
        session_id: row.get(4)?,
        team_id: row.get(5)?,
        turn_id: row.get(6)?,
        seq: row.get(7)?,
        kind: row.get(8)?,
        criticality,
        payload,
        provider: row.get(11)?,
        provider_seq: row.get(12)?,
        created_at: row.get(13)?,
    })
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>, RuntimeError> {
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed collecting sqlite rows", error))
}

fn json_to_string(value: &Value) -> Result<String, RuntimeError> {
    serde_json::to_string(value)
        .map_err(|error| RuntimeError::Bootstrap(format!("failed serializing JSON value: {error}")))
}

fn opt_json_to_string(value: Option<&Value>) -> Result<Option<String>, RuntimeError> {
    match value {
        Some(value) => Ok(Some(json_to_string(value)?)),
        None => Ok(None),
    }
}

fn string_to_json(value: String) -> rusqlite::Result<Value> {
    serde_json::from_str::<Value>(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn opt_string_to_json(value: Option<String>) -> rusqlite::Result<Option<Value>> {
    match value {
        Some(raw) => Ok(Some(string_to_json(raw)?)),
        None => Ok(None),
    }
}

fn db_error(context: impl AsRef<str>, error: rusqlite::Error) -> RuntimeError {
    RuntimeError::Bootstrap(format!("{}: {error}", context.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(temp_dir: &tempfile::TempDir) -> SqliteRuntimeRepository {
        SqliteRuntimeRepository::new(temp_dir.path().join("runtime.sqlite3"))
    }

    fn sample_session() -> SessionRecord {
        SessionRecord {
            id: "session_1".to_string(),
            provider: "codex".to_string(),
            status: "active".to_string(),
            cwd: Some("/tmp/repo".to_string()),
            model: Some("gpt-5.4".to_string()),
            permission_mode: Some("workspace_write".to_string()),
            system_prompt: Some("you are helpful".to_string()),
            metadata: serde_json::json!({"source": "test"}),
            provider_session_ref: Some("prov_sess_1".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: Some("turn_1".to_string()),
            worktree_id: Some("wt_1".to_string()),
            created_at: 100,
            updated_at: 101,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        }
    }

    fn sample_turn() -> TurnRecord {
        TurnRecord {
            id: "turn_1".to_string(),
            session_id: "session_1".to_string(),
            provider_turn_ref: Some("prov_turn_1".to_string()),
            status: "running".to_string(),
            input: serde_json::json!([{"type": "text", "text": "hello"}]),
            source: Some("user".to_string()),
            started_at: Some(101),
            completed_at: None,
            usage: None,
            error: None,
        }
    }

    #[test]
    fn initialize_schema_creates_all_phase_two_tables() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);

        repository.initialize_schema().expect("initialize schema");

        let connection = open_connection(&temp_dir.path().join("runtime.sqlite3")).expect("open");
        let mut statement = connection
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .expect("prepare");
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");

        let table_names = rows.into_iter().collect::<std::collections::BTreeSet<_>>();
        for expected in [
            "schema_migrations",
            "sessions",
            "turns",
            "approvals",
            "runtime_events",
            "teams",
            "team_members",
            "team_messages",
            "team_deliveries",
            "managed_worktrees",
            "managed_worktree_claims",
            "processes",
            "credentials",
            "team_operation_journal",
            "team_operation_diagnostics",
            "diagnostics_journal",
        ] {
            assert!(table_names.contains(expected), "missing table {expected}");
        }
    }

    #[test]
    fn initialize_schema_migrates_partially_populated_database_without_reset() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let db_path = temp_dir.path().join("runtime.sqlite3");

        {
            let connection = Connection::open(&db_path).expect("open raw");
            connection
                .execute_batch(
                    "CREATE TABLE sessions (
                        id TEXT PRIMARY KEY,
                        provider TEXT NOT NULL,
                        status TEXT NOT NULL,
                        cwd TEXT,
                        model TEXT,
                        permission_mode TEXT,
                        system_prompt TEXT,
                        metadata_json TEXT NOT NULL DEFAULT '{}',
                        provider_session_ref TEXT,
                        canonical_provider_session_ref TEXT,
                        active_turn_id TEXT,
                        worktree_id TEXT,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        closed_at INTEGER,
                        failure_code TEXT,
                        failure_message TEXT
                    );",
                )
                .expect("create partial schema");
            connection
                .execute(
                    "INSERT INTO sessions (id, provider, status, metadata_json, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params!["existing_session", "codex", "created", "{}", 1_i64, 1_i64],
                )
                .expect("insert session");
        }

        let repository = SqliteRuntimeRepository::new(db_path.clone());
        repository.initialize_schema().expect("schema migration");

        let hydrated = repository.hydrate_runtime_state().expect("hydrate");
        assert_eq!(hydrated.sessions.len(), 1);
        assert_eq!(hydrated.sessions[0].id, "existing_session");

        let connection = open_connection(&db_path).expect("open");
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='runtime_events'",
                [],
                |row| row.get(0),
            )
            .expect("query");
        assert_eq!(count, 1);
    }

    #[test]
    fn append_runtime_event_assigns_monotonic_scope_sequence() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        let first = repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: "evt_1".to_string(),
                scope: RuntimeEventScope::Session,
                scope_id: "session_1".to_string(),
                session_id: Some("session_1".to_string()),
                team_id: None,
                turn_id: Some("turn_1".to_string()),
                kind: "turn.started".to_string(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"status": "started"}),
                provider: Some("codex".to_string()),
                provider_seq: Some(11),
                created_at: 100,
            })
            .expect("append first event");
        let second = repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: "evt_2".to_string(),
                scope: RuntimeEventScope::Session,
                scope_id: "session_1".to_string(),
                session_id: Some("session_1".to_string()),
                team_id: None,
                turn_id: Some("turn_1".to_string()),
                kind: "turn.completed".to_string(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"status": "completed"}),
                provider: Some("codex".to_string()),
                provider_seq: Some(12),
                created_at: 101,
            })
            .expect("append second event");
        let third = repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: "evt_3".to_string(),
                scope: RuntimeEventScope::Team,
                scope_id: "team_1".to_string(),
                session_id: None,
                team_id: Some("team_1".to_string()),
                turn_id: None,
                kind: "team.created".to_string(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"name": "alpha"}),
                provider: None,
                provider_seq: None,
                created_at: 102,
            })
            .expect("append third event");

        assert_eq!(first.seq, 1);
        assert_eq!(second.seq, 2);
        assert_eq!(third.seq, 1);
    }

    #[test]
    fn append_runtime_event_is_idempotent_for_existing_event_id() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        let first = repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: "evt_same".to_string(),
                scope: RuntimeEventScope::Session,
                scope_id: "session_1".to_string(),
                session_id: Some("session_1".to_string()),
                team_id: None,
                turn_id: None,
                kind: "session.created".to_string(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"a": 1}),
                provider: Some("codex".to_string()),
                provider_seq: Some(1),
                created_at: 1,
            })
            .expect("append first event");

        let second = repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: "evt_same".to_string(),
                scope: RuntimeEventScope::Session,
                scope_id: "session_ignored".to_string(),
                session_id: Some("session_ignored".to_string()),
                team_id: None,
                turn_id: None,
                kind: "session.changed".to_string(),
                criticality: RuntimeEventCriticality::Droppable,
                payload: serde_json::json!({"a": 2}),
                provider: Some("codex".to_string()),
                provider_seq: Some(2),
                created_at: 2,
            })
            .expect("append second event");

        assert_eq!(first.row_id, second.row_id);
        assert_eq!(first.scope_id, second.scope_id);
        assert_eq!(first.kind, second.kind);

        let events = repository
            .list_runtime_events(None, None, 10)
            .expect("list events");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn concurrent_appends_same_scope_allocate_distinct_monotonic_sequences() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        let scope_id = "session_concurrent";
        let writer_count = 20usize;
        let barrier = Arc::new(Barrier::new(writer_count));

        let mut handles = Vec::with_capacity(writer_count);
        for writer_idx in 0..writer_count {
            let barrier = barrier.clone();
            let repository = repository.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                repository.append_runtime_event(&NewRuntimeEvent {
                    event_id: format!("evt_concurrent_{writer_idx}"),
                    scope: RuntimeEventScope::Session,
                    scope_id: scope_id.to_string(),
                    session_id: Some(scope_id.to_string()),
                    team_id: None,
                    turn_id: None,
                    kind: "turn.delta".to_string(),
                    criticality: RuntimeEventCriticality::Droppable,
                    payload: serde_json::json!({"writer_idx": writer_idx}),
                    provider: Some("codex".to_string()),
                    provider_seq: Some(writer_idx as i64),
                    created_at: 1_000 + writer_idx as i64,
                })
            }));
        }

        let mut appended = Vec::with_capacity(writer_count);
        for handle in handles {
            let event = handle
                .join()
                .expect("writer thread panicked")
                .expect("append should succeed");
            appended.push(event);
        }
        assert_eq!(appended.len(), writer_count);

        let events = repository
            .list_runtime_events(
                Some((RuntimeEventScope::Session, scope_id)),
                None,
                writer_count + 5,
            )
            .expect("list scoped events");
        assert_eq!(events.len(), writer_count);

        let sequences = events.iter().map(|event| event.seq).collect::<Vec<_>>();
        assert_eq!(
            sequences,
            (1_i64..=(writer_count as i64)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn team_message_idempotency_key_retry_converges_with_different_id() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        let session = sample_session();
        repository.upsert_session(&session).expect("insert session");
        repository
            .upsert_team(&TeamRecord {
                id: "team_1".to_string(),
                name: "Team Alpha".to_string(),
                lead_agent_id: session.id.clone(),
                created_by: "user".to_string(),
                created_at: 100,
                updated_at: 100,
                deleted_at: None,
            })
            .expect("insert team");

        repository
            .upsert_team_message(&TeamMessageRecord {
                id: "msg_1".to_string(),
                team_id: "team_1".to_string(),
                scope: "direct".to_string(),
                sender_agent_id: session.id.clone(),
                recipient_agent_ids: serde_json::json!([session.id.clone()]),
                input: serde_json::json!([{"type":"text","text":"first"}]),
                image_paths: serde_json::json!([]),
                priority: "normal".to_string(),
                policy: "non_interrupting".to_string(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("idem_same".to_string()),
                created_at: 100,
            })
            .expect("insert message");
        repository
            .upsert_team_message(&TeamMessageRecord {
                id: "msg_2".to_string(),
                team_id: "team_1".to_string(),
                scope: "direct".to_string(),
                sender_agent_id: session.id.clone(),
                recipient_agent_ids: serde_json::json!([session.id.clone()]),
                input: serde_json::json!([{"type":"text","text":"retry"}]),
                image_paths: serde_json::json!(["/tmp/a.png"]),
                priority: "high".to_string(),
                policy: "immediate_interrupt".to_string(),
                correlation_id: Some("corr_2".to_string()),
                reply_to_message_id: None,
                idempotency_key: Some("idem_same".to_string()),
                created_at: 101,
            })
            .expect("logical retry upsert");

        let hydrated = repository.hydrate_runtime_state().expect("hydrate");
        assert_eq!(hydrated.team_messages.len(), 1);
        let message = &hydrated.team_messages[0];
        assert_eq!(message.id, "msg_1");
        assert_eq!(message.priority, "high");
        assert_eq!(message.policy, "immediate_interrupt");
        assert_eq!(
            message.input,
            serde_json::json!([{"type":"text","text":"retry"}])
        );
    }

    #[test]
    fn managed_worktree_natural_key_retry_converges_with_different_id() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        repository
            .upsert_managed_worktree(&ManagedWorktreeRecord {
                id: "wt_1".to_string(),
                repo_root: "/tmp/repo".to_string(),
                worktree_root: "/tmp/worktrees".to_string(),
                worktree_cwd: "/tmp/worktrees/repo-feature".to_string(),
                branch_name: "gg/feature".to_string(),
                worktree_name: "repo-feature".to_string(),
                unified_workspace_path: "repo_feature".to_string(),
                deletion_policy: "retain_on_last_claim".to_string(),
                created_by_session_id: Some("session_1".to_string()),
                created_by_operation_id: Some("op_1".to_string()),
                created_at: 100,
                updated_at: 100,
            })
            .expect("insert worktree");
        repository
            .upsert_managed_worktree(&ManagedWorktreeRecord {
                id: "wt_2".to_string(),
                repo_root: "/tmp/repo".to_string(),
                worktree_root: "/tmp/worktrees".to_string(),
                worktree_cwd: "/tmp/worktrees/repo-feature".to_string(),
                branch_name: "gg/feature".to_string(),
                worktree_name: "repo-feature-updated".to_string(),
                unified_workspace_path: "repo_feature_v2".to_string(),
                deletion_policy: "delete_on_last_claim".to_string(),
                created_by_session_id: Some("session_2".to_string()),
                created_by_operation_id: Some("op_2".to_string()),
                created_at: 100,
                updated_at: 101,
            })
            .expect("logical retry upsert");

        let hydrated = repository.hydrate_runtime_state().expect("hydrate");
        assert_eq!(hydrated.managed_worktrees.len(), 1);
        let worktree = &hydrated.managed_worktrees[0];
        assert_eq!(worktree.id, "wt_1");
        assert_eq!(worktree.worktree_name, "repo-feature-updated");
        assert_eq!(worktree.unified_workspace_path, "repo_feature_v2");
        assert_eq!(worktree.deletion_policy, "delete_on_last_claim");
        assert_eq!(worktree.created_by_session_id.as_deref(), Some("session_2"));
    }

    #[test]
    fn credential_logical_key_retry_converges_with_different_id() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        repository
            .upsert_credential(&CredentialRecord {
                id: "cred_1".to_string(),
                provider: "codex".to_string(),
                profile: "default".to_string(),
                kind: "api_key".to_string(),
                encrypted_secret: "enc:first".to_string(),
                metadata: serde_json::json!({"source":"first"}),
                created_at: 100,
                updated_at: 100,
            })
            .expect("insert credential");
        repository
            .upsert_credential(&CredentialRecord {
                id: "cred_2".to_string(),
                provider: "codex".to_string(),
                profile: "default".to_string(),
                kind: "api_key".to_string(),
                encrypted_secret: "enc:second".to_string(),
                metadata: serde_json::json!({"source":"retry"}),
                created_at: 100,
                updated_at: 101,
            })
            .expect("logical retry upsert");

        let hydrated = repository.hydrate_runtime_state().expect("hydrate");
        assert_eq!(hydrated.credentials.len(), 1);
        let credential = &hydrated.credentials[0];
        assert_eq!(credential.id, "cred_1");
        assert_eq!(credential.encrypted_secret, "enc:second");
        assert_eq!(credential.metadata, serde_json::json!({"source":"retry"}));
        assert_eq!(credential.updated_at, 101);
    }

    #[test]
    fn hydrate_runtime_state_round_trips_core_entities() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        let session = sample_session();
        let turn = sample_turn();

        repository.upsert_session(&session).expect("insert session");
        repository.upsert_turn(&turn).expect("insert turn");
        repository
            .upsert_approval(&ApprovalRecord {
                id: "apr_1".to_string(),
                session_id: session.id.clone(),
                turn_id: turn.id.clone(),
                tool_call_id: Some("tool_1".to_string()),
                provider_approval_ref: Some("prov_apr_1".to_string()),
                status: "pending".to_string(),
                request: serde_json::json!({"question": "allow?"}),
                response: None,
                created_at: 102,
                resolved_at: None,
            })
            .expect("insert approval");
        repository
            .upsert_team(&TeamRecord {
                id: "team_1".to_string(),
                name: "Team Alpha".to_string(),
                lead_agent_id: session.id.clone(),
                created_by: "user".to_string(),
                created_at: 103,
                updated_at: 103,
                deleted_at: None,
            })
            .expect("insert team");
        repository
            .upsert_team_member(&TeamMemberRecord {
                team_id: "team_1".to_string(),
                agent_id: session.id.clone(),
                title: Some("Lead".to_string()),
                joined_at: 103,
                added_by: "user".to_string(),
                creator_agent_id: None,
                creator_compaction_subscription: "auto".to_string(),
                worktree_id: Some("wt_1".to_string()),
            })
            .expect("insert team member");
        repository
            .upsert_team_message(&TeamMessageRecord {
                id: "msg_1".to_string(),
                team_id: "team_1".to_string(),
                scope: "direct".to_string(),
                sender_agent_id: session.id.clone(),
                recipient_agent_ids: serde_json::json!([session.id.clone()]),
                input: serde_json::json!([{"type": "text", "text": "hello"}]),
                image_paths: serde_json::json!([]),
                priority: "normal".to_string(),
                policy: "non_interrupting".to_string(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("idem_1".to_string()),
                created_at: 104,
            })
            .expect("insert team message");
        repository
            .upsert_team_delivery(&TeamDeliveryRecord {
                id: "dlv_1".to_string(),
                message_id: "msg_1".to_string(),
                team_id: "team_1".to_string(),
                recipient_agent_id: session.id.clone(),
                provider: "codex".to_string(),
                status: "pending".to_string(),
                effective_policy: Some("non_interrupting".to_string()),
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: None,
                last_error_message: None,
                created_at: 104,
                updated_at: 104,
            })
            .expect("insert team delivery");
        repository
            .upsert_managed_worktree(&ManagedWorktreeRecord {
                id: "wt_1".to_string(),
                repo_root: "/tmp/repo".to_string(),
                worktree_root: "/tmp/worktrees".to_string(),
                worktree_cwd: "/tmp/worktrees/repo-feature".to_string(),
                branch_name: "gg/feature".to_string(),
                worktree_name: "repo-feature".to_string(),
                unified_workspace_path: "repo_feature".to_string(),
                deletion_policy: "retain_on_last_claim".to_string(),
                created_by_session_id: Some(session.id.clone()),
                created_by_operation_id: Some("op_1".to_string()),
                created_at: 105,
                updated_at: 105,
            })
            .expect("insert worktree");
        repository
            .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
                worktree_id: "wt_1".to_string(),
                session_id: session.id.clone(),
                claim_role: "owner".to_string(),
                created_at: 106,
                released_at: None,
            })
            .expect("insert worktree claim");
        repository
            .upsert_process(&ProcessRecord {
                id: "proc_1".to_string(),
                session_id: Some(session.id.clone()),
                tool_call_id: Some("tool_1".to_string()),
                pid: Some(4242),
                command: serde_json::json!(["bash", "-lc", "echo hello"]),
                cwd: Some("/tmp/repo".to_string()),
                status: "running".to_string(),
                exit_code: None,
                signal: None,
                stdout_path: Some("/tmp/stdout.log".to_string()),
                stderr_path: Some("/tmp/stderr.log".to_string()),
                started_at: 107,
                ended_at: None,
                timeout_ms: Some(30_000),
            })
            .expect("insert process");
        repository
            .upsert_credential(&CredentialRecord {
                id: "cred_1".to_string(),
                provider: "codex".to_string(),
                profile: "default".to_string(),
                kind: "api_key".to_string(),
                encrypted_secret: "enc:abc".to_string(),
                metadata: serde_json::json!({"origin": "test"}),
                created_at: 108,
                updated_at: 108,
            })
            .expect("insert credential");

        let hydrated = repository.hydrate_runtime_state().expect("hydrate state");
        assert_eq!(hydrated.sessions.len(), 1);
        assert_eq!(hydrated.turns.len(), 1);
        assert_eq!(hydrated.approvals.len(), 1);
        assert_eq!(hydrated.teams.len(), 1);
        assert_eq!(hydrated.team_members.len(), 1);
        assert_eq!(hydrated.team_messages.len(), 1);
        assert_eq!(hydrated.team_deliveries.len(), 1);
        assert_eq!(hydrated.managed_worktrees.len(), 1);
        assert_eq!(hydrated.managed_worktree_claims.len(), 1);
        assert_eq!(hydrated.processes.len(), 1);
        assert_eq!(hydrated.credentials.len(), 1);

        assert_eq!(hydrated.sessions[0].id, "session_1");
        assert_eq!(hydrated.turns[0].id, "turn_1");
        assert_eq!(hydrated.teams[0].id, "team_1");
    }

    #[test]
    fn delete_team_member_persists_membership_removal() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let repository = repo(&temp_dir);
        repository.initialize_schema().expect("initialize schema");

        let lead = sample_session();
        let member = SessionRecord {
            id: "session_2".to_string(),
            provider: "codex".to_string(),
            status: "idle".to_string(),
            cwd: None,
            model: Some("gpt-5.2-codex".to_string()),
            permission_mode: Some("default".to_string()),
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("thread_2".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: 200,
            updated_at: 200,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        };
        repository
            .upsert_session(&lead)
            .expect("insert lead session");
        repository
            .upsert_session(&member)
            .expect("insert member session");

        repository
            .upsert_team(&TeamRecord {
                id: "team_1".to_string(),
                name: "Team Alpha".to_string(),
                lead_agent_id: lead.id.clone(),
                created_by: "user".to_string(),
                created_at: 201,
                updated_at: 201,
                deleted_at: None,
            })
            .expect("insert team");

        for agent_id in [&lead.id, &member.id] {
            repository
                .upsert_team_member(&TeamMemberRecord {
                    team_id: "team_1".to_string(),
                    agent_id: agent_id.to_string(),
                    title: None,
                    joined_at: 202,
                    added_by: "user".to_string(),
                    creator_agent_id: None,
                    creator_compaction_subscription: "auto".to_string(),
                    worktree_id: None,
                })
                .expect("insert team member");
        }

        repository
            .delete_team_member("team_1", member.id.as_str())
            .expect("delete team member");

        let hydrated = repository.hydrate_runtime_state().expect("hydrate");
        assert_eq!(hydrated.team_members.len(), 1);
        assert_eq!(hydrated.team_members[0].agent_id, lead.id);
    }

    #[tokio::test]
    async fn store_initialize_and_healthcheck_use_schema() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        });

        store.initialize().await.expect("initialize store");
        store.healthcheck().await.expect("healthcheck");

        let hydrated = store.hydrate_runtime_state().expect("hydrate");
        assert!(hydrated.sessions.is_empty());
    }
}
