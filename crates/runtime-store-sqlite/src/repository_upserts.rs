use runtime_core::{
    ApprovalRecord, CredentialRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
    ProcessRecord, RuntimeError, SessionRecord, TeamDeliveryRecord, TeamMemberRecord,
    TeamMessageRecord, TeamOperationDiagnosticRecord, TeamOperationJournalRecord, TeamRecord,
    TurnRecord,
};
use rusqlite::{params, OptionalExtension};
use serde_json::Value;

use crate::db::{
    collect_rows, db_error, json_to_string, open_connection, opt_json_to_string, string_to_json,
};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
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
}
