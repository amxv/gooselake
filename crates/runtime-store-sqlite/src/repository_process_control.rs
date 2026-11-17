//! Local scheduler controls and their atomic Critical observations.
//! This module adds no HTTP key, revision, or native-dispatch authority.
use super::{
    immediate, managed_process_from_row, scheduler_settings, write_scheduler_settings,
    PROCESS_COLUMNS,
};
use crate::db::{db_error, json_to_string, open_connection};
use crate::SqliteRuntimeRepository;
use runtime_core::{ManagedProcessRecord, ProcessSchedulerSettings, RuntimeError};
use rusqlite::{params, Transaction};
use serde_json::{json, Value};
use std::collections::HashMap;

const CONTROL_SCOPE: &str = "process_scheduler";

fn append_control_event(
    tx: &Transaction<'_>,
    kind: &str,
    payload: &Value,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    let previous: Option<i64> = tx
        .query_row(
            "SELECT MAX(seq) FROM runtime_events WHERE scope='system' AND scope_id=?1",
            [CONTROL_SCOPE],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading scheduler event sequence", error))?;
    let previous = previous.unwrap_or(0);
    let sequence = previous
        .checked_add(1)
        .filter(|_| previous >= 0)
        .ok_or_else(|| {
            RuntimeError::Conflict("exhausted or invalid process scheduler event sequence".into())
        })?;
    // IMMEDIATE serializes sequence allocation and the authority mutation.
    // A conflicting event ID fails the transaction; it is never replay proof.
    let event_id = format!("evt_process_scheduler_control_{sequence}");
    let payload_json = json_to_string(payload)?;
    let inserted = tx
        .execute(
            "INSERT INTO runtime_events
         (event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES(?1,'system',?2,?3,?4,1,?5,?6)",
            params![event_id, CONTROL_SCOPE, sequence, kind, payload_json, at_ms],
        )
        .map_err(|error| db_error("failed atomically recording scheduler control", error))?;
    if inserted != 1 {
        return Err(RuntimeError::Conflict(
            "scheduler control event insertion did not produce exactly one receipt".into(),
        ));
    }
    let exact: bool = tx
        .query_row(
            "SELECT EXISTS(
           SELECT 1 FROM runtime_events WHERE event_id=?1
             AND scope='system' AND scope_id=?2
             AND session_id IS NULL AND team_id IS NULL AND turn_id IS NULL
             AND seq=?3 AND kind=?4 AND critical=1 AND payload_json=?5
             AND provider IS NULL AND provider_seq IS NULL AND created_at=?6)",
            params![event_id, CONTROL_SCOPE, sequence, kind, payload_json, at_ms],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed verifying scheduler control event receipt", error))?;
    if !exact {
        return Err(RuntimeError::Conflict(
            "scheduler control event receipt changed before commit".into(),
        ));
    }
    Ok(())
}

fn queued_records(tx: &Transaction<'_>) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
    let sql = format!(
        "SELECT {PROCESS_COLUMNS} FROM managed_processes WHERE status='queued'
         ORDER BY queue_order ASC, admission_order ASC, process_id ASC"
    );
    let mut statement = tx
        .prepare(&sql)
        .map_err(|error| db_error("failed preparing exact process queue carrier", error))?;
    let rows = statement
        .query_map([], managed_process_from_row)
        .map_err(|error| db_error("failed reading exact process queue carrier", error))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed decoding exact process queue carrier", error))
}

impl SqliteRuntimeRepository {
    /// Full replacement by the trusted caller, not a revision-CAS API.
    /// A timestamp alone is not a settings change or a new event.
    pub fn replace_process_scheduler_settings(
        &self,
        settings: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        let normalized = settings.clone().normalized();
        let mut connection = open_connection(&self.database_path)?;
        let tx = immediate(&mut connection, "process scheduler settings update")?;
        let previous = scheduler_settings(&tx)?;
        let mut comparable = normalized.clone();
        comparable.updated_at = previous.updated_at;
        if comparable == previous {
            return Ok(previous);
        }
        write_scheduler_settings(&tx, &normalized)?;
        append_control_event(
            &tx,
            "process.scheduler_settings_changed",
            &json!({
                "previous_settings": previous,
                "settings": normalized,
            }),
            normalized.updated_at,
        )?;
        if scheduler_settings(&tx)? != normalized {
            return Err(RuntimeError::Conflict(
                "scheduler control event carrier changed before commit".into(),
            ));
        }
        tx.commit().map_err(|error| {
            db_error("failed committing process scheduler settings update", error)
        })?;
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
        let tx = immediate(&mut connection, "managed-process queue reorder")?;
        let before_rows = queued_records(&tx)?;
        let mut ids = before_rows
            .iter()
            .map(|row| row.process_id.clone())
            .collect::<Vec<_>>();
        let previous_order = ids.clone();
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
        if ids == previous_order {
            return Ok(ids);
        }
        let mut expected_by_id: HashMap<_, _> = before_rows
            .into_iter()
            .map(|row| (row.process_id.clone(), row))
            .collect();
        let mut expected_rows = Vec::with_capacity(ids.len());
        for (index, id) in ids.iter().enumerate() {
            let position = i64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| RuntimeError::Conflict("process queue order exhausted".into()))?;
            let changed = tx
                .execute(
                    "UPDATE managed_processes SET queue_order = ?2, updated_at = ?3
                     WHERE process_id = ?1 AND status = 'queued'",
                    params![id, position, updated_at],
                )
                .map_err(|error| db_error("failed updating managed-process queue order", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "queued process changed while recording its order".into(),
                ));
            }
            let mut expected = expected_by_id
                .remove(id)
                .expect("queue identity came from this transaction's original rows");
            expected.queue_order = position;
            expected.updated_at = updated_at;
            expected_rows.push(expected);
        }
        append_control_event(
            &tx,
            "process.queue_reordered",
            &json!({
                "process_id": process_id,
                "before_process_id": before_process_id,
                "after_process_id": after_process_id,
                "previous_order": previous_order,
                "order": ids,
            }),
            updated_at,
        )?;
        if queued_records(&tx)? != expected_rows {
            return Err(RuntimeError::Conflict(
                "process queue control event carrier changed before commit".into(),
            ));
        }
        tx.commit()
            .map_err(|error| db_error("failed committing managed-process queue reorder", error))?;
        Ok(ids)
    }
}
