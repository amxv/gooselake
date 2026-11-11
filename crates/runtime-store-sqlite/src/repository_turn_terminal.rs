use runtime_core::{
    NewRuntimeEvent, ProviderTerminalCommit, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, SessionRecord, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{
    db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection, opt_json_to_string,
    opt_string_to_json, string_to_json,
};
use crate::repository::insert_runtime_event_in_transaction;
use crate::SqliteRuntimeRepository;

fn read_turn(tx: &Transaction<'_>, id: &str) -> Result<TurnRecord, RuntimeError> {
    tx.query_row(
        "SELECT id, session_id, provider_turn_ref, status, input_json, source,
                started_at, completed_at, usage_json, error_json
         FROM turns WHERE id=?1",
        [id],
        |row| {
            Ok(TurnRecord {
                id: row.get(0)?,
                session_id: row.get(1)?,
                provider_turn_ref: row.get(2)?,
                status: row.get(3)?,
                input: string_to_json(row.get(4)?)?,
                source: row.get(5)?,
                started_at: row.get(6)?,
                completed_at: row.get(7)?,
                usage: opt_string_to_json(row.get(8)?)?,
                error: opt_string_to_json(row.get(9)?)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading committed provider turn", error))?
    .ok_or_else(|| RuntimeError::NotFound(format!("turn {id}")))
}

fn read_session(tx: &Transaction<'_>, id: &str) -> Result<SessionRecord, RuntimeError> {
    tx.query_row(
        "SELECT id, provider, status, cwd, model, permission_mode, system_prompt,
                metadata_json, provider_session_ref, canonical_provider_session_ref,
                active_turn_id, worktree_id, created_at, updated_at, closed_at,
                failure_code, failure_message
         FROM sessions WHERE id=?1",
        [id],
        |row| {
            Ok(SessionRecord {
                id: row.get(0)?,
                provider: row.get(1)?,
                status: row.get(2)?,
                cwd: row.get(3)?,
                model: row.get(4)?,
                permission_mode: row.get(5)?,
                system_prompt: row.get(6)?,
                metadata: string_to_json(row.get(7)?)?,
                provider_session_ref: row.get(8)?,
                canonical_provider_session_ref: row.get(9)?,
                active_turn_id: row.get(10)?,
                worktree_id: row.get(11)?,
                created_at: row.get(12)?,
                updated_at: row.get(13)?,
                closed_at: row.get(14)?,
                failure_code: row.get(15)?,
                failure_message: row.get(16)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading committed provider session", error))?
    .ok_or_else(|| RuntimeError::NotFound(format!("session {id}")))
}

impl SqliteRuntimeRepository {
    /// A provider result is already observed, so the canonical event receipt
    /// and *both* projections must either commit together or not change at
    /// all. The turn ID is the permanent receipt key across restart/replay.
    pub fn commit_provider_turn_result(
        &self,
        previous_turn: &TurnRecord,
        updated_turn: &TurnRecord,
        previous_session: Option<&SessionRecord>,
        updated_session: Option<&SessionRecord>,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderTerminalCommit, RuntimeError> {
        let terminal_kind = match updated_turn.status.as_str() {
            "completed" => "turn.completed",
            "interrupted" => "turn.interrupted",
            "failed" => "turn.failed",
            _ => {
                return Err(RuntimeError::InvalidState(
                    "provider result is not terminal".into(),
                ));
            }
        };
        if previous_turn.id != updated_turn.id
            || previous_turn.session_id != updated_turn.session_id
            || updated_turn.completed_at.is_none()
            || event.event_id != format!("evt_turn_{}_terminal", updated_turn.id)
            || event.scope != RuntimeEventScope::Session
            || event.scope_id != updated_turn.session_id
            || event.session_id.as_deref() != Some(updated_turn.session_id.as_str())
            || event.turn_id.as_deref() != Some(updated_turn.id.as_str())
            || event.kind != terminal_kind
            || event.criticality != RuntimeEventCriticality::Critical
            || event.payload["status"] != updated_turn.status
            || previous_session.is_some() != updated_session.is_some()
            || previous_session
                .zip(updated_session)
                .is_some_and(|(before, after)| {
                    before.id != updated_turn.session_id
                        || after.id != before.id
                        || before.active_turn_id.as_deref() != Some(updated_turn.id.as_str())
                        || after.active_turn_id.is_some()
                })
        {
            return Err(RuntimeError::ProtocolViolation(
                "terminal turn authority/event bundle mismatch".into(),
            ));
        }

        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error("failed starting terminal turn journal transaction", error)
            })?;

        if let Some(existing) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if existing.scope != event.scope
                || existing.scope_id != event.scope_id
                || existing.session_id != event.session_id
                || existing.turn_id != event.turn_id
                || existing.kind != event.kind
                || existing.criticality != event.criticality
                || existing.payload != event.payload
            {
                return Err(RuntimeError::Conflict(
                    "terminal turn receipt ID collides with different event authority".into(),
                ));
            }
            let persisted_turn = read_turn(&tx, &updated_turn.id)?;
            if persisted_turn.status != updated_turn.status
                || persisted_turn.completed_at.is_none()
                || persisted_turn.usage != updated_turn.usage
                || persisted_turn.error != updated_turn.error
            {
                return Err(RuntimeError::Conflict(
                    "terminal turn receipt does not match durable turn state".into(),
                ));
            }
            let persisted_session = updated_session
                .map(|_| read_session(&tx, &updated_turn.session_id))
                .transpose()?;
            return Ok(ProviderTerminalCommit {
                turn: persisted_turn,
                session: persisted_session,
                event: existing,
            });
        }

        let changed = tx
            .execute(
                "UPDATE turns SET status=?1, completed_at=?2, usage_json=?3, error_json=?4
                 WHERE id=?5 AND session_id=?6 AND status=?7 AND completed_at IS ?8",
                params![
                    updated_turn.status,
                    updated_turn.completed_at,
                    opt_json_to_string(updated_turn.usage.as_ref())?,
                    opt_json_to_string(updated_turn.error.as_ref())?,
                    updated_turn.id,
                    updated_turn.session_id,
                    previous_turn.status,
                    previous_turn.completed_at,
                ],
            )
            .map_err(|error| db_error("failed terminalizing provider turn", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "provider turn changed before terminal result could commit".into(),
            ));
        }

        if let Some((before, after)) = previous_session.zip(updated_session) {
            let changed = tx
                .execute(
                    "UPDATE sessions
                     SET status=?1, active_turn_id=?2, metadata_json=?3, updated_at=?4
                     WHERE id=?5 AND status=?6 AND active_turn_id=?7 AND updated_at=?8",
                    params![
                        after.status,
                        after.active_turn_id,
                        json_to_string(&after.metadata)?,
                        after.updated_at,
                        before.id,
                        before.status,
                        before.active_turn_id,
                        before.updated_at,
                    ],
                )
                .map_err(|error| db_error("failed terminalizing provider session", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "provider session attachment changed before terminal result commit".into(),
                ));
            }
        }

        let inserted = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit()
            .map_err(|error| db_error("failed committing terminal turn journal", error))?;
        Ok(ProviderTerminalCommit {
            turn: updated_turn.clone(),
            session: updated_session.cloned(),
            event: inserted,
        })
    }
}
