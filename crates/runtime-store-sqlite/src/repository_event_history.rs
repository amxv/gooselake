use runtime_core::{
    OperatorEventSummary, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord,
    RuntimeEventScope,
};
use rusqlite::{params, types::Type};

use crate::db::{collect_rows, db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// A single global row-ID cursor spans scopes and remains stable when
    /// unrelated scopes append events. The selected index covers the filter
    /// and order, with no payload or provider details in the SELECT list.
    pub fn list_operator_events_page(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<OperatorEventSummary>, RuntimeError> {
        if !(1..=101).contains(&limit) {
            return Err(RuntimeError::InvalidState(
                "operator event history limit must be between 1 and 101".into(),
            ));
        }
        if after_id.is_some_and(|value| value < 0) {
            return Err(RuntimeError::InvalidState(
                "event history cursor must be nonnegative".into(),
            ));
        }
        let connection = open_connection(&self.database_path)?;
        let where_clause = match (scope.is_some(), after_id.is_some()) {
            (true, true) => "WHERE scope = ?1 AND scope_id = ?2 AND id > ?3",
            (true, false) => "WHERE scope = ?1 AND scope_id = ?2",
            (false, true) => "WHERE id > ?3",
            (false, false) => "",
        };
        let sql = format!(
            "SELECT id, event_id, scope, scope_id, seq, kind, critical, created_at \
             FROM runtime_events {where_clause} ORDER BY id ASC LIMIT ?4"
        );
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| db_error("failed preparing operator event history", error))?;
        let rows = statement
            .query_map(
                params![
                    scope.as_ref().map(|(kind, _)| kind.as_str()),
                    scope.map(|(_, id)| id),
                    after_id,
                    limit as i64,
                ],
                |row| {
                    let scope_text: String = row.get(2)?;
                    let scope = RuntimeEventScope::from_str(&scope_text).ok_or_else(|| {
                        invalid_enum(2, format!("unknown event scope {scope_text}"))
                    })?;
                    let critical: i64 = row.get(6)?;
                    let criticality =
                        RuntimeEventCriticality::from_i64(critical).ok_or_else(|| {
                            invalid_enum(6, format!("invalid criticality {critical}"))
                        })?;
                    Ok(OperatorEventSummary {
                        row_id: row.get(0)?,
                        event_id: row.get(1)?,
                        scope,
                        scope_id: row.get(3)?,
                        seq: row.get(4)?,
                        kind: row.get(5)?,
                        criticality,
                        created_at: row.get(7)?,
                    })
                },
            )
            .map_err(|error| db_error("failed querying operator event history", error))?;
        collect_rows(rows)
    }

    pub fn get_operator_event(
        &self,
        event_id: &str,
    ) -> Result<Option<RuntimeEventRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        fetch_runtime_event_by_event_id(&connection, event_id)
    }
}

fn invalid_enum(index: usize, reason: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        Type::Text,
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, reason)),
    )
}
