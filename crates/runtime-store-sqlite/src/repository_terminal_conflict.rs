use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    SessionRecord, TurnRecord,
};
use rusqlite::{params, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

fn conflict_message(turn: &TurnRecord, incoming: &str) -> String {
    format!(
        "conflicting terminal state for turn {} (stored={}, incoming={})",
        turn.id, turn.status, incoming
    )
}

fn verify_conflict_bundle(
    turn: &TurnRecord,
    before: &SessionRecord,
    failed: &SessionRecord,
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let incoming = event.payload["status"].as_str().unwrap_or_default();
    let terminal = matches!(
        turn.status.as_str(),
        "completed" | "interrupted" | "failed" | "cancelled" | "superseded"
    );
    if !terminal
        || !matches!(incoming, "completed" | "interrupted" | "failed")
        || incoming == turn.status
        || before.id != turn.session_id
        || before.status == "closed"
        || before.status == "turn_recovery_required"
        || before.closed_at.is_some()
        || before
            .active_turn_id
            .as_deref()
            .is_some_and(|owner| owner != turn.id)
        || failed.updated_at < before.updated_at
        || event.created_at != failed.updated_at
        || event.event_id != format!("evt_turn_{}_terminal_conflict", turn.id)
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != before.id
        || event.session_id.as_deref() != Some(before.id.as_str())
        || event.turn_id.as_deref() != Some(turn.id.as_str())
        || event.team_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.kind != "provider.terminal_conflict"
        || event.criticality != RuntimeEventCriticality::Critical
        || event.payload
            != serde_json::json!({
                "stored_status": turn.status,
                "status": incoming,
                "reason": "idle_terminal_conflict",
                "state_preserved": false,
            })
    {
        return Err(RuntimeError::ProtocolViolation(
            "terminal conflict lacks idle original-turn authority or Critical receipt".into(),
        ));
    }
    let mut expected = before.clone();
    expected.status = "failed".into();
    expected.failure_code = Some("terminal_conflict".into());
    expected.failure_message = Some(conflict_message(turn, incoming));
    expected.updated_at = failed.updated_at;
    if *failed != expected {
        return Err(RuntimeError::ProtocolViolation(
            "terminal conflict session projection differs from receipt".into(),
        ));
    }
    Ok(())
}

fn same_receipt(receipt: &RuntimeEventRecord, event: &NewRuntimeEvent) -> bool {
    receipt.event_id == event.event_id
        && receipt.scope == event.scope
        && receipt.scope_id == event.scope_id
        && receipt.session_id == event.session_id
        && receipt.turn_id == event.turn_id
        && receipt.team_id == event.team_id
        && receipt.kind == event.kind
        && receipt.criticality == event.criticality
        && receipt.payload == event.payload
        && receipt.provider == event.provider
        && receipt.provider_seq == event.provider_seq
}

impl SqliteRuntimeRepository {
    /// A contradictory *late* callback cannot choose the current session
    /// authority from a stale manager cache. The verified original terminal
    /// turn, unclaimed session, failure projection and Critical observation
    /// are one SQLite IMMEDIATE transaction. A forged/colliding receipt or
    /// newer owner fails closed; no native work is ever retried here.
    pub fn commit_provider_terminal_conflict(
        &self,
        previous_turn: &TurnRecord,
        previous_session: &SessionRecord,
        failed_session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<(SessionRecord, Option<RuntimeEventRecord>), RuntimeError> {
        verify_conflict_bundle(previous_turn, previous_session, failed_session, event)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning terminal conflict journal", error))?;
        if read_turn(&tx, &previous_turn.id)? != *previous_turn {
            return Err(RuntimeError::Conflict(
                "original terminal turn changed before conflict observation".into(),
            ));
        }
        let current = read_session(&tx, &previous_session.id)?;
        if let Some(receipt) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if !same_receipt(&receipt, event) {
                return Err(RuntimeError::Conflict(
                    "terminal conflict receipt collides with different native evidence".into(),
                ));
            }
            let original_failure = current.status == "failed"
                && current.failure_code == failed_session.failure_code
                && current.failure_message == failed_session.failure_message
                && current.updated_at >= receipt.created_at;
            let later_authority = current.updated_at > receipt.created_at
                && (current.status == "closed"
                    || current
                        .active_turn_id
                        .as_deref()
                        .is_some_and(|id| id != previous_turn.id));
            if !original_failure && !later_authority {
                return Err(RuntimeError::Conflict(
                    "terminal conflict receipt lacks its committed failure or newer owner".into(),
                ));
            }
            return Ok((current, None));
        }
        if current != *previous_session {
            return Err(RuntimeError::Conflict(
                "session owner changed before terminal conflict could commit".into(),
            ));
        }
        let updated = tx
            .execute(
                "UPDATE sessions SET status=?1, failure_code=?2, failure_message=?3,
                 updated_at=?4 WHERE id=?5 AND status=?6 AND active_turn_id IS ?7
                 AND updated_at=?8",
                params![
                    failed_session.status,
                    failed_session.failure_code,
                    failed_session.failure_message,
                    failed_session.updated_at,
                    previous_session.id,
                    previous_session.status,
                    previous_session.active_turn_id,
                    previous_session.updated_at,
                ],
            )
            .map_err(|error| db_error("failed applying terminal conflict CAS", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "terminal conflict session CAS lost".into(),
            ));
        }
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit()
            .map_err(|error| db_error("failed committing terminal conflict journal", error))?;
        Ok((failed_session.clone(), Some(receipt)))
    }
}
