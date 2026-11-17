use crate::db::{db_error, fetch_runtime_event_by_event_id};
use crate::repository::insert_runtime_event_in_transaction;
use runtime_core::{
    EditRerunTransitionReceipt, NewRuntimeEvent, RuntimeError, RuntimeEventCriticality,
    RuntimeEventRecord, RuntimeEventScope, SessionControlCoordinate, SessionEditRerunOperation,
};
use rusqlite::Transaction;
use serde_json::{json, Value};

pub(crate) fn unchanged(operation: SessionEditRerunOperation) -> EditRerunTransitionReceipt {
    EditRerunTransitionReceipt {
        operation,
        changed: false,
        journal_event: None,
        committed_session: None,
    }
}

pub(crate) fn journal_edit_transition_tx(
    tx: &Transaction<'_>,
    operation: &SessionEditRerunOperation,
    previous_phase: Option<&str>,
    origin: &'static str,
) -> Result<RuntimeEventRecord, RuntimeError> {
    let kind = match (origin, operation.phase.as_str()) {
        ("operator_review", "failed") => "turn.edit_reviewed",
        (_, "intended") => "turn.edit_admitted",
        (_, "native_started") => "turn.edit_native_started",
        (_, "fork_observed") => "turn.edit_fork_observed",
        (_, "completed") => "turn.edit_completed",
        (_, "manual_review") => "turn.edit_manual_review",
        (_, "failed") => "turn.edit_failed",
        _ => {
            return Err(RuntimeError::InvalidState(
                "unsupported edit receipt phase".into(),
            ))
        }
    };
    let owner = SessionControlCoordinate::EditRerun {
        agent_id: operation.agent_id.clone(),
        operation_id: operation.operation_id.clone(),
    };
    // The allowed phase graph never re-enters a phase. Historical replay
    // returns before this helper and never fabricates a missing old receipt.
    let event_id = format!(
        "evt_edit_state_{}",
        runtime_core::normalized_json_hash(&json!({"owner":owner,"phase":operation.phase}),)?
    );
    if fetch_runtime_event_by_event_id(tx, &event_id)?.is_some() {
        return Err(RuntimeError::Conflict(
            "fresh edit transition event identity is already occupied".into(),
        ));
    }
    let expected = NewRuntimeEvent {
        event_id,
        scope: RuntimeEventScope::Session,
        scope_id: operation.agent_id.clone(),
        session_id: Some(operation.agent_id.clone()),
        team_id: None,
        turn_id: Some(operation.target_turn_id.clone()),
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({
            "owner":owner,"operation_id":operation.operation_id,
            "target_turn_id":operation.target_turn_id,"phase":operation.phase,
            "previous_phase":previous_phase,"origin":origin,
            "child_provider_session_ref":operation.child_provider_session_ref,
            "child_canonical_provider_session_ref":operation.child_canonical_provider_session_ref,
            "replacement_turn_id":operation.replacement_turn_id,
            "error_code":operation.error_code,"review_evidence":operation.review_evidence,
        }),
        provider: None,
        provider_seq: None,
        created_at: operation.updated_at_ms,
    };
    let prior_sequence: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(seq),0) FROM runtime_events
             WHERE scope='session' AND scope_id=?1",
            [&operation.agent_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading edit event sequence", error))?;
    let sequence = prior_sequence
        .checked_add(1)
        .filter(|next| *next > 0)
        .ok_or_else(|| {
            RuntimeError::Conflict("edit transition event sequence is invalid or exhausted".into())
        })?;
    let event = insert_runtime_event_in_transaction(tx, &expected)?;
    if event.seq != sequence
        || event.event_id != expected.event_id
        || event.scope != expected.scope
        || event.scope_id != expected.scope_id
        || event.session_id != expected.session_id
        || event.team_id != expected.team_id
        || event.turn_id != expected.turn_id
        || event.kind != expected.kind
        || event.criticality != expected.criticality
        || event.payload != expected.payload
        || event.provider != expected.provider
        || event.provider_seq != expected.provider_seq
        || event.created_at != expected.created_at
    {
        return Err(RuntimeError::Conflict(
            "edit transition lacks its exact Critical event".into(),
        ));
    }
    verify_edit_transition_receipt_tx(tx, operation, &event)?;
    Ok(event)
}

/// Recheck only a receipt already proved by this transaction. Recovery retains
/// these exact rows until all later events have been inserted; no historical
/// receipt or mutable native authority is reconstructed here.
pub(crate) fn verify_edit_transition_receipt_tx(
    tx: &Transaction<'_>,
    operation: &SessionEditRerunOperation,
    event: &RuntimeEventRecord,
) -> Result<(), RuntimeError> {
    if fetch_runtime_event_by_event_id(tx, &event.event_id)?.as_ref() != Some(event) {
        return Err(RuntimeError::Conflict(
            "edit transition receipt changed before commit".into(),
        ));
    }
    let stored: (String, String, String, String, i64, i64) = tx
        .query_row(
            "SELECT session_id,target_turn_id,phase,record_json,created_at_ms,updated_at_ms
         FROM session_edit_reruns WHERE operation_id=?1",
            [&operation.operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .map_err(|error| db_error("failed verifying edit authority after its event", error))?;
    let record: Value = serde_json::from_str(&stored.3).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid edit receipt carrier: {error}"))
    })?;
    let expected_record = serde_json::to_value(operation).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid edit receipt record: {error}"))
    })?;
    if stored.0 != operation.agent_id
        || stored.1 != operation.target_turn_id
        || stored.2 != operation.phase
        || record != expected_record
        || stored.4 != operation.created_at_ms
        || stored.5 != operation.updated_at_ms
    {
        return Err(RuntimeError::Conflict(
            "edit authority changed while journaling its receipt".into(),
        ));
    }
    Ok(())
}
