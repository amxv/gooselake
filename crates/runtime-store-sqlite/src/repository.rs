use std::path::PathBuf;

use runtime_core::{NewRuntimeEvent, RuntimeError, RuntimeEventRecord, RuntimeEventScope};
use rusqlite::{params, Transaction, TransactionBehavior};

use crate::db::{
    apply_schema, collect_rows, db_error, fetch_runtime_event_by_event_id, json_to_string,
    open_connection, runtime_event_from_row,
};

#[derive(Debug, Clone)]
pub struct SqliteRuntimeRepository {
    pub(crate) database_path: PathBuf,
}

/// Shared insertion primitive for an already-immediate SQLite transaction.
/// Callers with associated authority mutations must commit both together;
/// the connection hook wakes SSE subscribers only after a successful commit.
pub(crate) fn insert_runtime_event_in_transaction(
    transaction: &Transaction<'_>,
    event: &NewRuntimeEvent,
) -> Result<RuntimeEventRecord, RuntimeError> {
    if let Some(existing) = fetch_runtime_event_by_event_id(transaction, &event.event_id)? {
        return Ok(existing);
    }
    let next_seq = transaction
        .query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events WHERE scope = ?1 AND scope_id = ?2",
            params![event.scope.as_str(), event.scope_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| db_error("failed computing next event sequence", error))?;
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
                json_to_string(&event.payload)?,
                event.provider,
                event.provider_seq,
                event.created_at,
            ],
        )
        .map_err(|error| db_error("failed inserting runtime event", error))?;
    fetch_runtime_event_by_event_id(transaction, &event.event_id)?
        .ok_or_else(|| RuntimeError::Bootstrap("inserted event missing after insert".to_string()))
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

        let inserted = insert_runtime_event_in_transaction(&transaction, event)?;

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

        // Subject views retain their parent's canonical event identity and
        // `seq`, but page by the stable global row ID instead. A single event
        // may be linked to multiple subjects; no duplicate journal writes.
        if let Some((kind, subject_id)) = scope.filter(|(kind, _)| kind.is_subject_projection()) {
            let mut statement = connection
                .prepare(
                    "SELECT e.id, e.event_id, e.scope, e.scope_id, e.session_id,
                            e.team_id, e.turn_id, e.seq, e.kind, e.critical,
                            e.payload_json, e.provider, e.provider_seq, e.created_at
                     FROM runtime_event_subjects s
                     JOIN runtime_events e ON e.id = s.event_row_id
                     WHERE s.subject_scope = ?1 AND s.subject_id = ?2 AND s.event_row_id > ?3
                     ORDER BY s.event_row_id ASC LIMIT ?4",
                )
                .map_err(|error| db_error("failed preparing subject event replay", error))?;
            let rows = statement
                .query_map(
                    params![kind.as_str(), subject_id, after_seq.unwrap_or(0), limit],
                    runtime_event_from_row,
                )
                .map_err(|error| db_error("failed querying subject event replay", error))?;
            return collect_rows(rows);
        }

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
}
