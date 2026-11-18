//! Epoch and anchor reads within the caller's one SQLite read snapshot.
use crate::db::db_error;
use runtime_core::{
    EventCollection, EventCursor, EventEpoch, EventPositionKind, EventReadError, RuntimeError,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

pub(crate) fn read_journal_epoch(connection: &Connection) -> Result<EventEpoch, RuntimeError> {
    // Caller-owned read/write snapshots stay intact. Standalone checks (in
    // particular the replay postguard) bind schema and rows in one snapshot.
    let snapshot = if connection.is_autocommit() {
        Some(
            Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
                .map_err(|error| db_error("failed starting journal identity snapshot", error))?,
        )
    } else {
        None
    };
    // A canonical-looking table without its complete applied chain is not
    // initialized authority. Bound readers do not adopt or upgrade it.
    let versions = connection
        .prepare("SELECT version FROM main.schema_migrations ORDER BY version LIMIT ?1")
        .map_err(|error| db_error("failed inspecting journal identity migration chain", error))?
        .query_map([crate::schema::SCHEMA_VERSION + 1], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| db_error("failed reading journal identity migration chain", error))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| db_error("invalid journal identity migration marker", error))?;
    if versions != (1..=crate::schema::SCHEMA_VERSION).collect::<Vec<_>>() {
        return Err(RuntimeError::Bootstrap("journal identity requires the complete supported migration chain; no read-time adoption is permitted".into()));
    }
    let migration = include_str!("schema_migration_53.sql");
    let start = migration
        .find("CREATE TABLE runtime_event_journal_identity (")
        .expect("schema53 identity table");
    let end = start
        + migration[start..]
            .find("\n);")
            .expect("schema53 identity table ending")
        + "\n);".len();
    let canonical = migration[start..end].trim().trim_end_matches(';');
    let installed: Option<String> = connection.query_row(
        "SELECT sql FROM main.sqlite_master WHERE type='table' AND name='runtime_event_journal_identity' AND tbl_name='runtime_event_journal_identity'",
        [], |row| row.get(0),
    ).optional().map_err(|error| db_error("failed attesting journal identity schema", error))?;
    if installed
        .as_deref()
        .map(|sql| sql.trim().trim_end_matches(';'))
        != Some(canonical)
    {
        return Err(RuntimeError::Bootstrap("journal identity requires canonical schema53 constraints; no repair or adoption is permitted".into()));
    }
    let mut statement = connection
        .prepare(
            "SELECT singleton,epoch FROM main.runtime_event_journal_identity ORDER BY singleton LIMIT 2",
        )
        .map_err(|e| db_error("failed reading required journal identity", e))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| db_error("failed querying required journal identity", e))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| db_error("invalid journal identity carrier", e))?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(RuntimeError::Bootstrap("journal identity requires exactly its original singleton; no read-time repair is permitted".into()));
    }
    let epoch = EventEpoch::from_persisted(&rows[0].1)?;
    drop(statement);
    if let Some(snapshot) = snapshot {
        snapshot
            .rollback()
            .map_err(|error| db_error("failed closing journal identity snapshot", error))?;
    }
    Ok(epoch)
}

/// Epoch, anchor and subsequent page must be read inside the caller's same
/// transaction. This function never changes an observer's shared health:
/// client cursor rejection is not a database-generation failure.
pub(crate) fn validate_cursor(
    connection: &Connection,
    epoch: &EventEpoch,
    collection: &EventCollection,
    after: Option<&EventCursor>,
) -> Result<(), EventReadError> {
    let Some(cursor) = after else {
        return Ok(());
    };
    cursor.validate_context(epoch, collection)?;
    let position = cursor.position();
    let event_id: Option<String> = match collection.scope() {
        Some((kind, id)) if kind.is_subject_projection() => connection.query_row(
            "SELECT e.event_id FROM runtime_event_subjects s JOIN runtime_events e ON e.id=s.event_row_id
             WHERE s.subject_scope=?1 AND s.subject_id=?2 AND s.event_row_id=?3",
            params![kind.as_str(), id, position], |row| row.get(0),
        ).optional(),
        Some((kind, id)) if collection.position_kind() == EventPositionKind::ScopeSeq => connection.query_row(
            "SELECT event_id FROM runtime_events WHERE scope=?1 AND scope_id=?2 AND seq=?3",
            params![kind.as_str(), id, position], |row| row.get(0),
        ).optional(),
        Some((kind, id)) => connection.query_row(
            "SELECT event_id FROM runtime_events WHERE scope=?1 AND scope_id=?2 AND id=?3",
            params![kind.as_str(), id, position], |row| row.get(0),
        ).optional(),
        None => connection.query_row(
            "SELECT event_id FROM runtime_events WHERE id=?1", [position], |row| row.get(0),
        ).optional(),
    }.map_err(|e| db_error("failed checking bound event cursor anchor", e))?;
    cursor.validate_anchor(event_id.as_deref())?;
    Ok(())
}
