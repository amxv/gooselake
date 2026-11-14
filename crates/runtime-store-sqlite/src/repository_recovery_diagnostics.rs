use runtime_core::RuntimeError;
use rusqlite::TransactionBehavior;

use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

// Match hydration's ordering and its exact anomaly-precedence rules. Only
// active/pending rows and their indexed parent identities/statuses are needed.
pub(crate) const ACTIVE_TURNS_DIAGNOSTICS_SQL: &str = "
    SELECT s.id, s.active_turn_id, t.session_id, t.status
    FROM sessions s LEFT JOIN turns t ON t.id = s.active_turn_id
    WHERE s.active_turn_id IS NOT NULL
    ORDER BY s.created_at, s.id";
pub(crate) const PENDING_APPROVALS_DIAGNOSTICS_SQL: &str = "
    SELECT a.id, a.turn_id, t.status
    FROM approvals a LEFT JOIN turns t ON t.id = a.turn_id
    WHERE a.status = 'pending'
    ORDER BY a.created_at, a.id";
pub(crate) const ORPHAN_CLAIMS_DIAGNOSTICS_SQL: &str = "
    SELECT c.worktree_id, c.session_id
    FROM managed_worktree_claims c LEFT JOIN sessions s ON s.id = c.session_id
    WHERE c.released_at IS NULL AND s.id IS NULL
    ORDER BY c.created_at, c.worktree_id, c.session_id";

impl SqliteRuntimeRepository {
    /// Legacy recovery anomaly strings, with the same session/approval/claim
    /// order as full hydration. SQLite's managed-process status projection is
    /// present, so the legacy `processes` fallback anomalies are intentionally
    /// suppressed just as in diagnostics_from_hydrated_state.
    /// A single deferred read transaction prevents cross-statement WAL drift;
    /// no inputs, credentials, worktree paths, or provider JSON are hydrated.
    pub fn recovery_diagnostic_anomalies(&self) -> Result<Vec<String>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let snapshot = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting recovery diagnostic snapshot", error))?;
        let mut anomalies = Vec::new();

        {
            let mut stmt = snapshot
                .prepare(ACTIVE_TURNS_DIAGNOSTICS_SQL)
                .map_err(|e| db_error("failed preparing active-turn diagnostics", e))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })
                .map_err(|e| db_error("failed querying active-turn diagnostics", e))?;
            for row in rows {
                let (session, active, turn_session, turn_status) =
                    row.map_err(|e| db_error("failed reading active-turn diagnostic", e))?;
                match (turn_session, turn_status) {
                    (None, _) => anomalies.push(format!(
                        "session {session} references missing active turn {active}"
                    )),
                    (Some(owner), _) if owner != session => anomalies.push(format!(
                        "session {session} active turn {active} belongs to {owner}"
                    )),
                    (_, Some(status)) if is_terminal(&status) => anomalies.push(format!(
                        "session {session} retains terminal active turn {active} ({status})"
                    )),
                    _ => {}
                }
            }
        }

        {
            let mut stmt = snapshot
                .prepare(PENDING_APPROVALS_DIAGNOSTICS_SQL)
                .map_err(|e| db_error("failed preparing pending-approval diagnostics", e))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(|e| db_error("failed querying pending-approval diagnostics", e))?;
            for row in rows {
                let (approval, turn, status) =
                    row.map_err(|e| db_error("failed reading pending-approval diagnostic", e))?;
                match status {
                    None => anomalies.push(format!(
                        "pending approval {approval} references missing turn {turn}"
                    )),
                    Some(status) if is_terminal(&status) => anomalies.push(format!(
                        "pending approval {approval} references terminal turn {turn} ({status})"
                    )),
                    _ => {}
                }
            }
        }

        {
            let mut stmt = snapshot
                .prepare(ORPHAN_CLAIMS_DIAGNOSTICS_SQL)
                .map_err(|e| db_error("failed preparing orphan-claim diagnostics", e))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|e| db_error("failed querying orphan-claim diagnostics", e))?;
            for row in rows {
                let (worktree, session) =
                    row.map_err(|e| db_error("failed reading orphan-claim diagnostic", e))?;
                anomalies.push(format!(
                    "active worktree claim {worktree} -> {session} references missing session"
                ));
            }
        }

        snapshot
            .commit()
            .map_err(|e| db_error("failed closing recovery diagnostic snapshot", e))?;
        Ok(anomalies)
    }
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "completed" | "interrupted" | "failed")
}
