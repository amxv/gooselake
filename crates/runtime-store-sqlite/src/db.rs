use std::path::Path;
use std::time::Duration;

use runtime_core::{RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::schema::{SCHEMA_SQL, SCHEMA_VERSION};

pub(crate) fn open_connection(path: &Path) -> Result<Connection, RuntimeError> {
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

pub(crate) fn apply_schema(connection: &mut Connection) -> Result<(), RuntimeError> {
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

pub(crate) fn fetch_runtime_event_by_event_id(
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

pub(crate) fn runtime_event_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RuntimeEventRecord> {
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

pub(crate) fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>, RuntimeError> {
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed collecting sqlite rows", error))
}

pub(crate) fn json_to_string(value: &Value) -> Result<String, RuntimeError> {
    serde_json::to_string(value)
        .map_err(|error| RuntimeError::Bootstrap(format!("failed serializing JSON value: {error}")))
}

pub(crate) fn opt_json_to_string(value: Option<&Value>) -> Result<Option<String>, RuntimeError> {
    match value {
        Some(value) => Ok(Some(json_to_string(value)?)),
        None => Ok(None),
    }
}

pub(crate) fn string_to_json(value: String) -> rusqlite::Result<Value> {
    serde_json::from_str::<Value>(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

pub(crate) fn opt_string_to_json(value: Option<String>) -> rusqlite::Result<Option<Value>> {
    match value {
        Some(raw) => Ok(Some(string_to_json(raw)?)),
        None => Ok(None),
    }
}

pub(crate) fn db_error(context: impl AsRef<str>, error: rusqlite::Error) -> RuntimeError {
    RuntimeError::Bootstrap(format!("{}: {error}", context.as_ref()))
}
