use std::path::PathBuf;

use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventRecord, RuntimeEventScope, RuntimeRecordMutation,
};
use rusqlite::{params, TransactionBehavior};

use crate::db::{
    apply_schema, checkpoint_source_identity_at, collect_rows, db_error,
    fetch_runtime_event_by_event_id, initialize_source_identity, json_to_string, open_connection,
    runtime_event_from_row,
};

#[derive(Debug, Clone)]
pub struct SqliteRuntimeRepository {
    pub(crate) database_path: PathBuf,
    pub(crate) authority_root: Option<PathBuf>,
}

impl SqliteRuntimeRepository {
    pub fn new(database_path: PathBuf) -> Self {
        Self {
            database_path,
            authority_root: None,
        }
    }

    pub fn new_with_authority_root(database_path: PathBuf, authority_root: PathBuf) -> Self {
        Self {
            database_path,
            authority_root: Some(authority_root),
        }
    }

    pub fn initialize_schema(&self) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        apply_schema(&mut connection)?;
        initialize_source_identity(
            &mut connection,
            &self.database_path,
            self.authority_root.as_deref(),
        )?;
        Ok(())
    }

    pub fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.append_runtime_event_with_mutations(event, &[])
    }

    pub fn append_runtime_event_with_mutations(
        &self,
        event: &NewRuntimeEvent,
        mutations: &[RuntimeRecordMutation],
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
            let high_watermark = transaction
                .query_row(
                    "SELECT COALESCE(MAX(id), 0) FROM runtime_events",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|error| db_error("failed reading idempotent event watermark", error))?;
            checkpoint_source_identity_at(
                &transaction,
                &self.database_path,
                self.authority_root.as_deref(),
                high_watermark,
            )?;
            transaction
                .commit()
                .map_err(|error| db_error("failed committing idempotent event append", error))?;
            return Ok(existing);
        }

        Self::apply_runtime_mutations_on(&transaction, mutations)?;

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

        checkpoint_source_identity_at(
            &transaction,
            &self.database_path,
            self.authority_root.as_deref(),
            inserted.row_id,
        )?;

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
}
