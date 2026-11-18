//! One positive signed lifetime token per exact managed-claim primary key.
use crate::db::{collect_rows, db_error};
use runtime_core::{ManagedWorktreeClaimRecord, RuntimeError};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use std::collections::BTreeMap;

pub(crate) const SELECT: &str = "SELECT worktree_id,session_id,claim_role,created_at,released_at,claim_generation FROM managed_worktree_claims";

pub(crate) fn from_row(row: &Row<'_>) -> rusqlite::Result<ManagedWorktreeClaimRecord> {
    let raw: i64 = row.get(5)?;
    if raw <= 0 {
        return Err(rusqlite::Error::IntegralValueOutOfRange(5, raw));
    }
    let token = u64::try_from(raw).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, raw))?;
    Ok(ManagedWorktreeClaimRecord {
        worktree_id: row.get(0)?,
        session_id: row.get(1)?,
        claim_role: row.get(2)?,
        created_at: row.get(3)?,
        released_at: row.get(4)?,
        claim_generation: Some(token),
    })
}

/// Historical inspection may precede schema migration. A complete recorded
/// pre-52 schema has no lifetime token; returning None preserves that absence
/// without granting any mutation authority. A current damaged schema must not
/// silently become historical data. Schema selection and rows share a snapshot.
pub(crate) fn hydrate(
    connection: &Connection,
) -> Result<Vec<ManagedWorktreeClaimRecord>, RuntimeError> {
    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
        .map_err(|e| db_error("failed starting claim hydration snapshot", e))?;
    let has_generation: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('managed_worktree_claims') WHERE name='claim_generation')",
        [], |row| row.get(0),
    ).map_err(|e| db_error("failed inspecting claim hydration schema", e))?;
    if !has_generation {
        let mut statement = transaction
            .prepare("SELECT version FROM schema_migrations ORDER BY version LIMIT 52")
            .map_err(|e| db_error("failed checking historical claim schema", e))?;
        let versions = statement
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(|e| db_error("failed reading historical claim schema", e))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| db_error("invalid historical claim schema versions", e))?;
        let last = versions.last().copied().unwrap_or(0);
        if !(1..52).contains(&last) || versions != (1..=last).collect::<Vec<_>>() {
            return Err(RuntimeError::Bootstrap(
                "missing claim_generation requires a complete recorded pre-52 schema".into(),
            ));
        }
    }
    let sql = format!(
        "SELECT worktree_id,session_id,claim_role,created_at,released_at,{} \
         FROM managed_worktree_claims ORDER BY created_at,worktree_id,session_id",
        if has_generation {
            "claim_generation"
        } else {
            "NULL"
        }
    );
    let mut statement = transaction
        .prepare(&sql)
        .map_err(|e| db_error("failed preparing managed worktree claim hydration", e))?;
    let rows = statement
        .query_map([], |row| {
            if has_generation {
                from_row(row)
            } else {
                Ok(ManagedWorktreeClaimRecord {
                    worktree_id: row.get(0)?,
                    session_id: row.get(1)?,
                    claim_role: row.get(2)?,
                    created_at: row.get(3)?,
                    released_at: row.get(4)?,
                    claim_generation: None,
                })
            }
        })
        .map_err(|e| db_error("failed reading managed worktree claim hydration", e))?;
    let records = collect_rows(rows)?;
    drop(statement);
    transaction
        .commit()
        .map_err(|e| db_error("failed completing claim hydration snapshot", e))?;
    Ok(records)
}

pub(crate) fn read(
    connection: &Connection,
    worktree: &str,
    session: &str,
) -> Result<Option<ManagedWorktreeClaimRecord>, RuntimeError> {
    connection
        .query_row(
            &format!("{SELECT} WHERE worktree_id=?1 AND session_id=?2"),
            params![worktree, session],
            from_row,
        )
        .optional()
        .map_err(|e| db_error("failed reading managed claim lifetime", e))
}

pub(crate) fn generation(record: &ManagedWorktreeClaimRecord) -> Result<i64, RuntimeError> {
    record
        .claim_generation
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
        .ok_or_else(|| {
            RuntimeError::Conflict(
                "managed worktree claim has no valid observed lifetime token".into(),
            )
        })
}

pub(crate) fn require_observed(
    record: &ManagedWorktreeClaimRecord,
    current: &ManagedWorktreeClaimRecord,
) -> Result<(), RuntimeError> {
    if record.worktree_id != current.worktree_id
        || record.session_id != current.session_id
        || generation(record)? != generation(current)?
    {
        return Err(RuntimeError::Conflict(
            "worktree claim lifetime changed since observation".into(),
        ));
    }
    Ok(())
}

pub(crate) fn next(prior: Option<&ManagedWorktreeClaimRecord>) -> Result<u64, RuntimeError> {
    let previous = prior.map(generation).transpose()?.unwrap_or(0);
    let value = previous.checked_add(1).ok_or_else(|| {
        RuntimeError::InvalidState(
            "worktree claim generation cannot advance beyond SQLite signed integer range".into(),
        )
    })?;
    u64::try_from(value)
        .map_err(|_| RuntimeError::InvalidState("invalid next claim generation".into()))
}

/// Combined startup owns a complete hydration snapshot. New keys matter even
/// when its plan changes only worktree records; subset repair APIs do not use this.
pub(crate) fn verify_full_snapshot(
    connection: &Transaction<'_>,
    originals: &[ManagedWorktreeClaimRecord],
) -> Result<(), RuntimeError> {
    let mut expected = BTreeMap::new();
    for row in originals {
        generation(row)?;
        if expected
            .insert((row.worktree_id.as_str(), row.session_id.as_str()), row)
            .is_some()
        {
            return Err(RuntimeError::Conflict(
                "startup claim snapshot contains duplicate raw keys".into(),
            ));
        }
    }
    let mut statement = connection
        .prepare(SELECT)
        .map_err(|e| db_error("failed preparing complete startup claim snapshot", e))?;
    let rows = statement
        .query_map([], from_row)
        .map_err(|e| db_error("failed reading complete startup claim snapshot", e))?;
    let current = collect_rows(rows)?;
    let actual = current
        .iter()
        .map(|row| ((row.worktree_id.as_str(), row.session_id.as_str()), row))
        .collect::<BTreeMap<_, _>>();
    if actual != expected {
        return Err(RuntimeError::Conflict(
            "startup claim keyset or lifetime changed since hydration".into(),
        ));
    }
    Ok(())
}

/// Caller must already hold its IMMEDIATE transaction and establish mutation
/// authority. This write preserves its chosen token and verifies the carrier.
pub(crate) fn write(
    connection: &Transaction<'_>,
    record: &ManagedWorktreeClaimRecord,
) -> Result<(), RuntimeError> {
    let token = generation(record)?;
    let changed = connection.execute(
        "INSERT INTO managed_worktree_claims(worktree_id,session_id,claim_role,created_at,released_at,claim_generation)
         VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(worktree_id,session_id) DO UPDATE SET
         claim_role=excluded.claim_role,created_at=excluded.created_at,released_at=excluded.released_at,
         claim_generation=excluded.claim_generation",
        params![record.worktree_id, record.session_id, record.claim_role, record.created_at, record.released_at, token])
        .map_err(|e| db_error("failed persisting managed claim lifetime", e))?;
    if changed != 1
        || read(connection, &record.worktree_id, &record.session_id)?.as_ref() != Some(record)
    {
        return Err(RuntimeError::Conflict(
            "managed claim lifetime write was ignored or changed".into(),
        ));
    }
    Ok(())
}
