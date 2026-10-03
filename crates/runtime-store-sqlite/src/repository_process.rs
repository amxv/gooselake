use std::collections::{BTreeMap, HashMap};

use runtime_core::{
    process_status_is_terminal, ManagedProcessAdmission, ManagedProcessRecord,
    ManagedProcessTerminalUpdate, ProcessCompletionUpdate, ProcessSchedulerSettings, RuntimeError,
    PROCESS_COMPLETION_NOT_REQUIRED, PROCESS_COMPLETION_PENDING,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection, string_to_json};
use crate::SqliteRuntimeRepository;

const PROCESS_COLUMNS: &str = r#"
process_id, owner_session_id, workspace_id, tool_call_id, command_json, cwd,
timeout_ms, status, admission_order, queue_order, claim_generation, claimed_at,
capture_limit_bytes, pid, os_start_identity, execution_started_at, ended_at,
execution_duration_ms, terminal_recorded_at, terminal_reason, exit_code, signal,
stdout_path, stderr_path, stdout_captured_bytes, stderr_captured_bytes,
stdout_truncated, stderr_truncated, cancel_requested, completion_state,
completion_turn_id, completion_attempt_count, completion_last_error,
completion_updated_at, admitted_at, updated_at
"#;

impl SqliteRuntimeRepository {
    pub fn admit_managed_process(
        &self,
        admission: &ManagedProcessAdmission,
    ) -> Result<ManagedProcessRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "managed-process admission")?;
        let admission_order = transaction
            .query_row(
                "SELECT COALESCE(MAX(admission_order), 0) + 1 FROM managed_processes",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| db_error("failed allocating process admission order", error))?;
        let queue_order = transaction
            .query_row(
                "SELECT COALESCE(MAX(queue_order), 0) + 1 FROM managed_processes",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| db_error("failed allocating process queue order", error))?;
        let command_json = json_to_string(&admission.command)?;
        transaction
            .execute(
                "INSERT INTO managed_processes (
                    process_id, owner_session_id, workspace_id, tool_call_id, command_json, cwd,
                    timeout_ms, status, admission_order, queue_order, claim_generation, claimed_at,
                    capture_limit_bytes, pid, os_start_identity, execution_started_at, ended_at,
                    execution_duration_ms, terminal_recorded_at, terminal_reason, exit_code, signal,
                    stdout_path, stderr_path, stdout_captured_bytes, stderr_captured_bytes,
                    stdout_truncated, stderr_truncated, cancel_requested, completion_state,
                    completion_turn_id, completion_attempt_count, completion_last_error,
                    completion_updated_at, admitted_at, updated_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', ?8, ?9, 0, NULL,
                    ?10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    ?11, ?12, 0, 0, 0, 0, 0, 'not_required', NULL, 0, NULL, NULL, ?13, ?13
                 )",
                params![
                    admission.process_id,
                    admission.owner_session_id,
                    admission.workspace_id,
                    admission.tool_call_id,
                    command_json,
                    admission.cwd,
                    admission.timeout_ms,
                    admission_order,
                    queue_order,
                    admission.capture_limit_bytes,
                    admission.stdout_path,
                    admission.stderr_path,
                    admission.admitted_at,
                ],
            )
            .map_err(|error| db_error("failed inserting managed-process admission", error))?;
        let record = process_by_id(&transaction, &admission.process_id)?.ok_or_else(|| {
            RuntimeError::Bootstrap(
                "admitted managed process disappeared before commit".to_string(),
            )
        })?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing managed-process admission", error))?;
        Ok(record)
    }

    pub fn list_managed_processes(&self) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let sql = format!(
            "SELECT {PROCESS_COLUMNS} FROM managed_processes ORDER BY admission_order ASC, process_id ASC"
        );
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| db_error("failed preparing managed-process list", error))?;
        let rows = statement
            .query_map([], managed_process_from_row)
            .map_err(|error| db_error("failed querying managed processes", error))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| db_error("failed decoding managed processes", error))
    }

    pub fn get_managed_process(
        &self,
        process_id: &str,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        process_by_id(&connection, process_id)
    }

    pub fn claim_managed_processes(
        &self,
        settings: &ProcessSchedulerSettings,
        claimed_at: i64,
    ) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        let settings = settings.clone().normalized();
        if settings.paused {
            return Ok(Vec::new());
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "managed-process scheduler claim")?;
        let active_global = transaction
            .query_row(
                "SELECT COUNT(*) FROM managed_processes WHERE status IN ('launch_reserved', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| db_error("failed counting active managed processes", error))?
            .max(0) as usize;
        let mut slots = settings.max_concurrent.saturating_sub(active_global);
        if slots == 0 {
            transaction.commit().map_err(|error| {
                db_error(
                    "failed closing empty managed-process scheduler claim",
                    error,
                )
            })?;
            return Ok(Vec::new());
        }

        let mut workspace_active = HashMap::<String, usize>::new();
        {
            let mut statement = transaction
                .prepare(
                    "SELECT workspace_id, COUNT(*) FROM managed_processes
                     WHERE status IN ('launch_reserved', 'running') AND workspace_id IS NOT NULL
                     GROUP BY workspace_id",
                )
                .map_err(|error| db_error("failed preparing workspace process count", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(|error| db_error("failed querying workspace process count", error))?;
            for row in rows {
                let (workspace_id, count) = row
                    .map_err(|error| db_error("failed decoding workspace process count", error))?;
                workspace_active.insert(workspace_id, count.max(0) as usize);
            }
        }
        let candidates = {
            let mut statement = transaction
                .prepare(
                    "SELECT process_id, workspace_id FROM managed_processes
                     WHERE status = 'queued'
                     ORDER BY queue_order ASC, admission_order ASC, process_id ASC",
                )
                .map_err(|error| db_error("failed preparing process claim candidates", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })
                .map_err(|error| db_error("failed querying process claim candidates", error))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed decoding process claim candidates", error))?
        };

        let mut claimed = Vec::new();
        for (process_id, workspace_id) in candidates {
            if slots == 0 {
                break;
            }
            if let Some(workspace_id) = workspace_id.as_deref() {
                if let Some(limit) = settings.workspace_max_concurrent.get(workspace_id) {
                    let active = workspace_active.get(workspace_id).copied().unwrap_or(0);
                    if active >= *limit {
                        continue;
                    }
                }
            }
            let changed = transaction
                .execute(
                    "UPDATE managed_processes
                     SET status = 'launch_reserved', claim_generation = claim_generation + 1,
                         claimed_at = ?2, capture_limit_bytes = ?3, updated_at = ?2
                     WHERE process_id = ?1 AND status = 'queued'",
                    params![process_id, claimed_at, settings.capture_limit_bytes as i64],
                )
                .map_err(|error| db_error("failed claiming managed process", error))?;
            if changed == 0 {
                continue;
            }
            let record = process_by_id(&transaction, &process_id)?.ok_or_else(|| {
                RuntimeError::Bootstrap(
                    "claimed process disappeared inside transaction".to_string(),
                )
            })?;
            if let Some(workspace_id) = record.workspace_id.as_deref() {
                *workspace_active
                    .entry(workspace_id.to_string())
                    .or_default() += 1;
            }
            slots -= 1;
            claimed.push(record);
        }
        transaction.commit().map_err(|error| {
            db_error("failed committing managed-process scheduler claim", error)
        })?;
        Ok(claimed)
    }

    pub fn mark_managed_process_running(
        &self,
        process_id: &str,
        claim_generation: i64,
        pid: i64,
        os_start_identity: &str,
        started_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "managed-process running authorization")?;
        let changed = transaction
            .execute(
                "UPDATE managed_processes
                 SET status = 'running', pid = ?3, os_start_identity = ?4,
                     execution_started_at = ?5, updated_at = ?5
                 WHERE process_id = ?1 AND status = 'launch_reserved' AND claim_generation = ?2",
                params![
                    process_id,
                    claim_generation,
                    pid,
                    os_start_identity,
                    started_at
                ],
            )
            .map_err(|error| db_error("failed authorizing managed process running", error))?;
        let record = if changed == 1 {
            process_by_id(&transaction, process_id)?
        } else {
            None
        };
        transaction.commit().map_err(|error| {
            db_error(
                "failed committing managed-process running authorization",
                error,
            )
        })?;
        Ok(record)
    }

    pub fn requeue_managed_process_claim(
        &self,
        process_id: &str,
        claim_generation: i64,
        updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "managed-process claim requeue")?;
        let changed = transaction
            .execute(
                "UPDATE managed_processes
                 SET status = 'queued', claimed_at = NULL, updated_at = ?3
                 WHERE process_id = ?1 AND status = 'launch_reserved' AND claim_generation = ?2",
                params![process_id, claim_generation, updated_at],
            )
            .map_err(|error| db_error("failed requeueing managed-process claim", error))?;
        let record = if changed == 1 {
            process_by_id(&transaction, process_id)?
        } else {
            None
        };
        transaction
            .commit()
            .map_err(|error| db_error("failed committing managed-process requeue", error))?;
        Ok(record)
    }

    pub fn terminalize_managed_process(
        &self,
        process_id: &str,
        update: &ManagedProcessTerminalUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        if !process_status_is_terminal(&update.status) {
            return Err(RuntimeError::InvalidState(format!(
                "managed process terminal update has non-terminal status {}",
                update.status
            )));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "managed-process terminalization")?;
        let Some(current) = process_by_id(&transaction, process_id)? else {
            transaction.commit().map_err(|error| {
                db_error("failed closing missing process terminalization", error)
            })?;
            return Ok(None);
        };
        if process_status_is_terminal(&current.status) {
            transaction.commit().map_err(|error| {
                db_error("failed closing idempotent process terminalization", error)
            })?;
            return Ok(Some(current));
        }
        let completion_state = if update.completion_required {
            PROCESS_COMPLETION_PENDING
        } else {
            PROCESS_COMPLETION_NOT_REQUIRED
        };
        transaction
            .execute(
                "UPDATE managed_processes
                 SET status = ?2, terminal_reason = ?3, exit_code = ?4, signal = ?5,
                     ended_at = ?6, execution_duration_ms = ?7, terminal_recorded_at = ?6,
                     stdout_captured_bytes = ?8, stderr_captured_bytes = ?9,
                     stdout_truncated = ?10, stderr_truncated = ?11,
                     completion_state = ?12, completion_turn_id = NULL,
                     completion_last_error = NULL, completion_updated_at = ?6,
                     updated_at = ?6
                 WHERE process_id = ?1",
                params![
                    process_id,
                    update.status,
                    update.terminal_reason,
                    update.exit_code,
                    update.signal,
                    update.ended_at,
                    update.execution_duration_ms,
                    update.stdout_captured_bytes,
                    update.stderr_captured_bytes,
                    bool_i64(update.stdout_truncated),
                    bool_i64(update.stderr_truncated),
                    completion_state,
                ],
            )
            .map_err(|error| db_error("failed terminalizing managed process", error))?;
        let record = process_by_id(&transaction, process_id)?;
        transaction.commit().map_err(|error| {
            db_error("failed committing managed-process terminalization", error)
        })?;
        Ok(record)
    }

    pub fn update_managed_process_capture_progress(
        &self,
        process_id: &str,
        stream: &str,
        captured_bytes: i64,
        truncated: bool,
        updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        if captured_bytes < 0 {
            return Err(RuntimeError::InvalidState(
                "captured process bytes cannot be negative".to_string(),
            ));
        }
        let connection = open_connection(&self.database_path)?;
        let sql = match stream {
            "stdout" => {
                "UPDATE managed_processes
                 SET stdout_captured_bytes = MAX(stdout_captured_bytes, ?2),
                     stdout_truncated = MAX(stdout_truncated, ?3), updated_at = MAX(updated_at, ?4)
                 WHERE process_id = ?1 AND status = 'running'"
            }
            "stderr" => {
                "UPDATE managed_processes
                 SET stderr_captured_bytes = MAX(stderr_captured_bytes, ?2),
                     stderr_truncated = MAX(stderr_truncated, ?3), updated_at = MAX(updated_at, ?4)
                 WHERE process_id = ?1 AND status = 'running'"
            }
            other => {
                return Err(RuntimeError::InvalidState(format!(
                    "unsupported managed-process capture stream {other}"
                )))
            }
        };
        let changed = connection
            .execute(
                sql,
                params![process_id, captured_bytes, bool_i64(truncated), updated_at],
            )
            .map_err(|error| db_error("failed updating managed-process capture progress", error))?;
        if changed == 0 {
            return self.get_managed_process(process_id);
        }
        process_by_id(&connection, process_id)
    }

    pub fn cancel_queued_managed_process(
        &self,
        process_id: &str,
        owner_session_id: Option<&str>,
        canceled_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "queued managed-process cancellation")?;
        let Some(current) = process_by_id(&transaction, process_id)? else {
            transaction.commit().map_err(|error| {
                db_error("failed closing missing queued process cancellation", error)
            })?;
            return Ok(None);
        };
        if !owner_matches(&current, owner_session_id) {
            transaction.commit().map_err(|error| {
                db_error(
                    "failed closing unauthorized queued process cancellation",
                    error,
                )
            })?;
            return Ok(None);
        }
        if current.status == "canceled" {
            transaction.commit().map_err(|error| {
                db_error(
                    "failed closing idempotent queued process cancellation",
                    error,
                )
            })?;
            return Ok(Some(current));
        }
        if !matches!(current.status.as_str(), "queued" | "launch_reserved") {
            transaction.commit().map_err(|error| {
                db_error("failed closing non-queued process cancellation", error)
            })?;
            return Ok(None);
        }
        transaction
            .execute(
                "UPDATE managed_processes
                 SET status = 'canceled', terminal_reason = 'canceled_before_launch',
                     ended_at = ?2, terminal_recorded_at = ?2,
                     completion_state = 'not_required', completion_turn_id = NULL,
                     completion_last_error = NULL, completion_updated_at = ?2, updated_at = ?2
                 WHERE process_id = ?1 AND status IN ('queued', 'launch_reserved')",
                params![process_id, canceled_at],
            )
            .map_err(|error| db_error("failed canceling queued managed process", error))?;
        let record = process_by_id(&transaction, process_id)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing queued process cancellation", error))?;
        Ok(record)
    }

    pub fn request_managed_process_cancel(
        &self,
        process_id: &str,
        owner_session_id: Option<&str>,
        updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "running managed-process cancel request")?;
        let Some(current) = process_by_id(&transaction, process_id)? else {
            transaction.commit().map_err(|error| {
                db_error(
                    "failed closing missing running process cancel request",
                    error,
                )
            })?;
            return Ok(None);
        };
        if !owner_matches(&current, owner_session_id) {
            transaction.commit().map_err(|error| {
                db_error(
                    "failed closing unauthorized running process cancel request",
                    error,
                )
            })?;
            return Ok(None);
        }
        if process_status_is_terminal(&current.status) {
            transaction.commit().map_err(|error| {
                db_error(
                    "failed closing terminal running process cancel request",
                    error,
                )
            })?;
            return Ok(Some(current));
        }
        if current.status != "running" {
            transaction.commit().map_err(|error| {
                db_error("failed closing non-running process cancel request", error)
            })?;
            return Ok(None);
        }
        transaction
            .execute(
                "UPDATE managed_processes SET cancel_requested = 1, updated_at = ?2
                 WHERE process_id = ?1 AND status = 'running'",
                params![process_id, updated_at],
            )
            .map_err(|error| db_error("failed persisting running process cancel request", error))?;
        let record = process_by_id(&transaction, process_id)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing running process cancel request", error))?;
        Ok(record)
    }

    pub fn update_managed_process_completion(
        &self,
        process_id: &str,
        update: &ProcessCompletionUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let changed = connection
            .execute(
                "UPDATE managed_processes
                 SET completion_state = ?2, completion_turn_id = ?3,
                     completion_attempt_count = ?4, completion_last_error = ?5,
                     completion_updated_at = ?6, updated_at = MAX(updated_at, ?6)
                 WHERE process_id = ?1",
                params![
                    process_id,
                    update.state,
                    update.turn_id,
                    update.attempt_count,
                    update.last_error,
                    update.updated_at,
                ],
            )
            .map_err(|error| db_error("failed updating process completion state", error))?;
        if changed == 0 {
            return Ok(None);
        }
        process_by_id(&connection, process_id)
    }

    pub fn load_or_initialize_process_scheduler_settings(
        &self,
        defaults: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "process scheduler settings initialization")?;
        let stored = scheduler_settings(&transaction)?;
        let result = if stored.updated_at == 0 {
            let normalized = defaults.clone().normalized();
            write_scheduler_settings(&transaction, &normalized)?;
            normalized
        } else {
            stored.normalized()
        };
        transaction.commit().map_err(|error| {
            db_error(
                "failed committing process scheduler settings initialization",
                error,
            )
        })?;
        Ok(result)
    }

    pub fn replace_process_scheduler_settings(
        &self,
        settings: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        let normalized = settings.clone().normalized();
        let connection = open_connection(&self.database_path)?;
        write_scheduler_settings(&connection, &normalized)?;
        Ok(normalized)
    }

    pub fn reorder_queued_managed_process(
        &self,
        process_id: &str,
        before_process_id: Option<&str>,
        after_process_id: Option<&str>,
        updated_at: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        if before_process_id.is_some() == after_process_id.is_some() {
            return Err(RuntimeError::InvalidState(
                "exactly one of before_process_id or after_process_id is required".to_string(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = immediate(&mut connection, "managed-process queue reorder")?;
        let mut ids = {
            let mut statement = transaction
                .prepare(
                    "SELECT process_id FROM managed_processes WHERE status = 'queued'
                     ORDER BY queue_order ASC, admission_order ASC, process_id ASC",
                )
                .map_err(|error| db_error("failed preparing managed-process reorder", error))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| db_error("failed querying managed-process reorder", error))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed decoding managed-process reorder", error))?
        };
        let Some(current_index) = ids.iter().position(|id| id == process_id) else {
            return Err(RuntimeError::NotFound(format!(
                "queued process {process_id}"
            )));
        };
        let moving = ids.remove(current_index);
        let target = before_process_id
            .or(after_process_id)
            .expect("validated target");
        let Some(target_index) = ids.iter().position(|id| id == target) else {
            return Err(RuntimeError::NotFound(format!("queued process {target}")));
        };
        let insert_at = if before_process_id.is_some() {
            target_index
        } else {
            target_index + 1
        };
        ids.insert(insert_at, moving);
        for (index, id) in ids.iter().enumerate() {
            transaction
                .execute(
                    "UPDATE managed_processes SET queue_order = ?2, updated_at = ?3
                     WHERE process_id = ?1 AND status = 'queued'",
                    params![id, (index as i64) + 1, updated_at],
                )
                .map_err(|error| db_error("failed updating managed-process queue order", error))?;
        }
        transaction
            .commit()
            .map_err(|error| db_error("failed committing managed-process queue reorder", error))?;
        Ok(ids)
    }
}

fn immediate<'a>(
    connection: &'a mut Connection,
    action: &str,
) -> Result<Transaction<'a>, RuntimeError> {
    connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| db_error(format!("failed starting {action} transaction"), error))
}

fn process_by_id(
    connection: &Connection,
    process_id: &str,
) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
    let sql = format!("SELECT {PROCESS_COLUMNS} FROM managed_processes WHERE process_id = ?1");
    connection
        .query_row(&sql, params![process_id], managed_process_from_row)
        .optional()
        .map_err(|error| db_error("failed querying managed process", error))
}

fn managed_process_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ManagedProcessRecord> {
    Ok(ManagedProcessRecord {
        process_id: row.get(0)?,
        owner_session_id: row.get(1)?,
        workspace_id: row.get(2)?,
        tool_call_id: row.get(3)?,
        command: string_to_json(row.get(4)?)?,
        cwd: row.get(5)?,
        timeout_ms: row.get(6)?,
        status: row.get(7)?,
        admission_order: row.get(8)?,
        queue_order: row.get(9)?,
        claim_generation: row.get(10)?,
        claimed_at: row.get(11)?,
        capture_limit_bytes: row.get(12)?,
        pid: row.get(13)?,
        os_start_identity: row.get(14)?,
        execution_started_at: row.get(15)?,
        ended_at: row.get(16)?,
        execution_duration_ms: row.get(17)?,
        terminal_recorded_at: row.get(18)?,
        terminal_reason: row.get(19)?,
        exit_code: row.get(20)?,
        signal: row.get(21)?,
        stdout_path: row.get(22)?,
        stderr_path: row.get(23)?,
        stdout_captured_bytes: row.get(24)?,
        stderr_captured_bytes: row.get(25)?,
        stdout_truncated: row.get::<_, i64>(26)? != 0,
        stderr_truncated: row.get::<_, i64>(27)? != 0,
        cancel_requested: row.get::<_, i64>(28)? != 0,
        completion_state: row.get(29)?,
        completion_turn_id: row.get(30)?,
        completion_attempt_count: row.get(31)?,
        completion_last_error: row.get(32)?,
        completion_updated_at: row.get(33)?,
        admitted_at: row.get(34)?,
        updated_at: row.get(35)?,
    })
}

fn scheduler_settings(connection: &Connection) -> Result<ProcessSchedulerSettings, RuntimeError> {
    let (max_concurrent, workspace_json, capture_limit_bytes, paused, pause_reason, updated_at) =
        connection
            .query_row(
                "SELECT max_concurrent, workspace_max_concurrent_json, capture_limit_bytes,
                        paused, pause_reason, updated_at
                 FROM process_scheduler_state WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .map_err(|error| db_error("failed reading process scheduler settings", error))?;
    let workspace_max_concurrent = serde_json::from_str::<BTreeMap<String, usize>>(&workspace_json)
        .map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "invalid persisted process workspace concurrency settings: {error}"
            ))
        })?;
    Ok(ProcessSchedulerSettings {
        max_concurrent: usize::try_from(max_concurrent).unwrap_or_default(),
        workspace_max_concurrent,
        capture_limit_bytes: usize::try_from(capture_limit_bytes).unwrap_or_default(),
        paused: paused != 0,
        pause_reason,
        updated_at,
    })
}

fn write_scheduler_settings(
    connection: &Connection,
    settings: &ProcessSchedulerSettings,
) -> Result<(), RuntimeError> {
    let workspace_json =
        serde_json::to_string(&settings.workspace_max_concurrent).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed serializing process workspace concurrency settings: {error}"
            ))
        })?;
    connection
        .execute(
            "UPDATE process_scheduler_state
             SET max_concurrent = ?1, workspace_max_concurrent_json = ?2,
                 capture_limit_bytes = ?3, paused = ?4, pause_reason = ?5, updated_at = ?6
             WHERE singleton = 1",
            params![
                settings.max_concurrent as i64,
                workspace_json,
                settings.capture_limit_bytes as i64,
                bool_i64(settings.paused),
                settings.pause_reason,
                settings.updated_at,
            ],
        )
        .map_err(|error| db_error("failed writing process scheduler settings", error))?;
    Ok(())
}

fn owner_matches(record: &ManagedProcessRecord, requested: Option<&str>) -> bool {
    requested.is_none_or(|requested| record.owner_session_id.as_deref() == Some(requested))
}

const fn bool_i64(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}
