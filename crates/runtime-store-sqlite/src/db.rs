use std::path::Path;
use std::time::Duration;

use runtime_core::{RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;

use crate::schema::{MIGRATIONS, SCHEMA_VERSION};

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
    reject_newer_schema(connection)?;
    for migration in MIGRATIONS {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed to start sqlite migration transaction", error))?;

        let already_applied = if table_exists(&transaction, "schema_migrations")? {
            transaction
                .query_row(
                    "SELECT version FROM schema_migrations WHERE version = ?1",
                    params![migration.version],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(|error| db_error("failed reading schema_migrations", error))?
                .is_some()
        } else {
            false
        };

        if !already_applied {
            transaction.execute_batch(migration.sql).map_err(|error| {
                db_error(
                    format!("failed applying sqlite migration {}", migration.version),
                    error,
                )
            })?;
            transaction
                .execute(
                    "INSERT INTO schema_migrations (version, applied_at)
                     VALUES (?1, CAST(strftime('%s','now') AS INTEGER))",
                    params![migration.version],
                )
                .map_err(|error| {
                    db_error(
                        format!("failed recording sqlite migration {}", migration.version),
                        error,
                    )
                })?;
        }

        transaction.commit().map_err(|error| {
            db_error(
                format!("failed committing sqlite migration {}", migration.version),
                error,
            )
        })?;
    }

    verify_migration_chain(connection)?;
    Ok(())
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, RuntimeError> {
    let count = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| db_error("failed checking sqlite table existence", error))?;
    Ok(count == 1)
}

fn reject_newer_schema(connection: &Connection) -> Result<(), RuntimeError> {
    if !table_exists(connection, "schema_migrations")? {
        return Ok(());
    }
    let max_version = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .map_err(|error| db_error("failed reading current sqlite schema version", error))?
        .unwrap_or(0);
    if max_version > SCHEMA_VERSION {
        return Err(RuntimeError::Bootstrap(format!(
            "database schema version {max_version} is newer than supported version {SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn verify_migration_chain(connection: &Connection) -> Result<(), RuntimeError> {
    let mut statement = connection
        .prepare("SELECT version FROM schema_migrations ORDER BY version ASC")
        .map_err(|error| db_error("failed preparing migration verification", error))?;
    let versions = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|error| db_error("failed reading applied migrations", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed collecting applied migrations", error))?;
    let expected = (1..=SCHEMA_VERSION).collect::<Vec<_>>();
    if versions != expected {
        return Err(RuntimeError::Bootstrap(format!(
            "sqlite migration chain is incomplete: expected {expected:?}, found {versions:?}"
        )));
    }
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
