//! Positive native dispatch acknowledgements must not leave a dispatched
//! admission, a running turn, and its Critical start event on separate commits.
use runtime_core::{
    NewRuntimeEvent, ProviderDispatchStartedCommit, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, SessionRecord, TurnAdmissionRecord, TurnDispatchState, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_turn_authority::turn_admission_from_row;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

fn read_admission(tx: &Transaction<'_>, id: &str) -> Result<TurnAdmissionRecord, RuntimeError> {
    tx.query_row(
        "SELECT turn_id, session_id, provider, projection_source,
                user_input_snapshot_json, dispatch_policy_json, correlation_json,
                dispatch_state, provider_native_turn_id, dispatch_error_json,
                admitted_at, updated_at
         FROM turn_admissions WHERE turn_id=?1",
        [id],
        turn_admission_from_row,
    )
    .optional()
    .map_err(|error| db_error("failed reading provider dispatch admission", error))?
    .ok_or_else(|| RuntimeError::NotFound(format!("turn admission {id}")))
}

fn verify_start(
    before_admission: &TurnAdmissionRecord,
    after_admission: &TurnAdmissionRecord,
    before_turn: &TurnRecord,
    after_turn: &TurnRecord,
    before_session: &SessionRecord,
    after_session: &SessionRecord,
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let mut expected_admission = before_admission.clone();
    expected_admission.dispatch_state = TurnDispatchState::Dispatched;
    expected_admission.provider_native_turn_id = after_admission.provider_native_turn_id.clone();
    expected_admission.dispatch_error = None;
    expected_admission.updated_at = after_admission.updated_at;
    let mut expected_turn = before_turn.clone();
    expected_turn.status = "in_progress".into();
    expected_turn.error = None;
    let mut expected_session = before_session.clone();
    expected_session.status = "turn_running".into();
    expected_session.failure_code = None;
    expected_session.failure_message = None;
    expected_session.updated_at = after_session.updated_at;

    if before_admission.dispatch_state != TurnDispatchState::Dispatching
        || before_admission.dispatch_error.is_some()
        || after_admission.updated_at < before_admission.updated_at
        || after_session.updated_at < before_session.updated_at
        || after_admission != &expected_admission
        || after_turn != &expected_turn
        || after_session != &expected_session
        || before_admission.turn_id != before_turn.id
        || before_admission.session_id != before_turn.session_id
        || before_session.id != before_turn.session_id
        || before_admission.provider != before_session.provider
        || before_session.active_turn_id.as_deref() != Some(before_turn.id.as_str())
        || before_session.closed_at.is_some()
        || !matches!(
            before_turn.status.as_str(),
            "admitted" | "waiting_for_approval"
        )
        || !matches!(
            before_session.status.as_str(),
            "turn_admitted" | "waiting_for_approval"
        )
        || before_turn.completed_at.is_some()
        || before_admission.provider_native_turn_id.is_some()
        || after_admission
            .provider_native_turn_id
            .as_deref()
            .is_some_and(|id| id.is_empty() || id.trim() != id)
        || event.event_id != format!("evt_turn_{}_started", before_turn.id)
        || event.kind != "turn.started"
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != before_session.id
        || event.session_id.as_deref() != Some(before_session.id.as_str())
        || event.turn_id.as_deref() != Some(before_turn.id.as_str())
        || event.team_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || !matches!(event.payload.as_object(), Some(map)
            if map.is_empty() || (map.len() == 1 && matches!(map.get("source").and_then(|v| v.as_str()), Some("pre_dispatch_approval.accepted"))))
    {
        return Err(RuntimeError::ProtocolViolation(
            "native dispatch start has mismatched state, owner or Critical event".into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn commit_provider_turn_started(
        &self,
        before_admission: &TurnAdmissionRecord,
        after_admission: &TurnAdmissionRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderDispatchStartedCommit, RuntimeError> {
        verify_start(
            before_admission,
            after_admission,
            before_turn,
            after_turn,
            before_session,
            after_session,
            event,
        )?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native dispatch start journal", error))?;

        let saved_admission = read_admission(&tx, &before_admission.turn_id)?;
        let saved_turn = read_turn(&tx, &before_turn.id)?;
        let saved_session = read_session(&tx, &before_session.id)?;
        if let Some(existing) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if existing.scope != event.scope
                || existing.scope_id != event.scope_id
                || existing.session_id != event.session_id
                || existing.team_id != event.team_id
                || existing.turn_id != event.turn_id
                || existing.kind != event.kind
                || existing.criticality != event.criticality
                || existing.payload != event.payload
                || existing.provider != event.provider
                || existing.provider_seq != event.provider_seq
                || saved_admission != *after_admission
                || saved_turn != *after_turn
                || saved_session != *after_session
            {
                return Err(RuntimeError::Conflict(
                    "native dispatch Critical start receipt does not match committed state".into(),
                ));
            }
            return Ok(ProviderDispatchStartedCommit {
                admission: saved_admission,
                turn: saved_turn,
                session: saved_session,
                event: existing,
            });
        }
        if saved_admission != *before_admission
            || saved_turn != *before_turn
            || saved_session != *before_session
        {
            return Err(RuntimeError::Conflict(
                "native dispatch owner changed before atomic start receipt".into(),
            ));
        }
        tx.execute(
            "UPDATE turn_admissions
             SET dispatch_state='dispatched', provider_native_turn_id=?1,
                 dispatch_error_json=NULL, updated_at=?2 WHERE turn_id=?3",
            params![
                after_admission.provider_native_turn_id,
                after_admission.updated_at,
                after_admission.turn_id,
            ],
        )
        .map_err(|error| db_error("failed committing native dispatch mapping", error))?;
        tx.execute(
            "UPDATE turns SET status='in_progress', error_json=NULL WHERE id=?1",
            [&after_turn.id],
        )
        .map_err(|error| db_error("failed committing native turn running state", error))?;
        tx.execute(
            "UPDATE sessions SET status='turn_running', failure_code=NULL,
                 failure_message=NULL, updated_at=?1 WHERE id=?2",
            params![after_session.updated_at, after_session.id],
        )
        .map_err(|error| db_error("failed committing native session running state", error))?;
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit()
            .map_err(|error| db_error("failed committing native dispatch start journal", error))?;
        Ok(ProviderDispatchStartedCommit {
            admission: after_admission.clone(),
            turn: after_turn.clone(),
            session: after_session.clone(),
            event: receipt,
        })
    }
}
