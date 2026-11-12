use runtime_core::{
    NewRuntimeEvent, ProviderTerminalCommit, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, SessionRecord, TurnRecord,
};
use rusqlite::{params, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection, opt_json_to_string};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

fn verify_failure_bundle(
    before_turn: &TurnRecord,
    after_turn: &TurnRecord,
    before_session: &SessionRecord,
    after_session: &SessionRecord,
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let unknown = match event.kind.as_str() {
        "provider.event_stream_unknown" => true,
        "provider.error" => false,
        _ => {
            return Err(RuntimeError::ProtocolViolation(
                "provider failure receipt kind is unsupported".into(),
            ));
        }
    };
    let expected_id = if unknown {
        format!("evt_turn_{}_event_stream_unknown", before_turn.id)
    } else {
        format!("evt_turn_{}_wait_failure", before_turn.id)
    };
    if event.event_id != expected_id
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != before_turn.session_id
        || event.session_id.as_deref() != Some(before_turn.session_id.as_str())
        || event.turn_id.as_deref() != Some(before_turn.id.as_str())
        || event.team_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || before_session.id != before_turn.session_id
        || before_session.active_turn_id.as_deref() != Some(before_turn.id.as_str())
        || before_session.closed_at.is_some()
        || matches!(
            before_session.status.as_str(),
            "closed" | "failed" | "turn_recovery_required"
        )
        || before_turn.completed_at.is_some()
        || matches!(
            before_turn.status.as_str(),
            "completed"
                | "interrupted"
                | "failed"
                | "cancelled"
                | "superseded"
                | "provider_event_recovery_required"
                | "approval_decision_unknown"
        )
        || after_session.updated_at < before_session.updated_at
    {
        return Err(RuntimeError::ProtocolViolation(
            "provider failure does not own an active nonterminal turn".into(),
        ));
    }
    let mut expected_turn = before_turn.clone();
    let mut expected_session = before_session.clone();
    expected_session.updated_at = after_session.updated_at;
    if unknown {
        let code = event.payload["code"].as_str().unwrap_or_default();
        let message = event.payload["message"].as_str().unwrap_or_default();
        if code.is_empty()
            || message.is_empty()
            || event.payload != serde_json::json!({"code":code,"message":message})
        {
            return Err(RuntimeError::ProtocolViolation(
                "provider unknown stream receipt lacks exact code and message".into(),
            ));
        }
        expected_turn.status = "provider_event_recovery_required".into();
        expected_turn.error = Some(serde_json::json!({"code":code,"message":message}));
        expected_session.status = "turn_recovery_required".into();
        expected_session.failure_code = Some(code.into());
        expected_session.failure_message = Some(message.into());
    } else {
        let message = event.payload["error"].as_str().unwrap_or_default();
        if message.is_empty()
            || event.payload != serde_json::json!({"error":message})
            || after_turn.completed_at.is_none()
            || after_turn.completed_at < before_turn.started_at
        {
            return Err(RuntimeError::ProtocolViolation(
                "provider wait failure lacks terminal proof and exact error".into(),
            ));
        }
        expected_turn.status = "failed".into();
        expected_turn.completed_at = after_turn.completed_at;
        expected_turn.error = Some(serde_json::json!({"message":message}));
        expected_session.status = "failed".into();
        expected_session.active_turn_id = None;
        expected_session.failure_code = Some("provider_wait_failure".into());
        expected_session.failure_message = Some(message.into());
    }
    if *after_turn != expected_turn || *after_session != expected_session {
        return Err(RuntimeError::ProtocolViolation(
            "provider failure turn/session and event payload are not one canonical transition"
                .into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// The provider wait or event stream may already have crossed the native
    /// dispatch boundary. Persist its observation and all affected projections
    /// under one exclusive journal transaction, never under independent upserts.
    pub fn commit_provider_failure_result(
        &self,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderTerminalCommit, RuntimeError> {
        verify_failure_bundle(
            before_turn,
            after_turn,
            before_session,
            after_session,
            event,
        )?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting provider failure journal", error))?;

        if let Some(receipt) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if receipt.scope != event.scope
                || receipt.scope_id != event.scope_id
                || receipt.session_id != event.session_id
                || receipt.turn_id != event.turn_id
                || receipt.team_id != event.team_id
                || receipt.kind != event.kind
                || receipt.criticality != event.criticality
                || receipt.payload != event.payload
            {
                return Err(RuntimeError::Conflict(
                    "provider failure receipt ID collides with different authority".into(),
                ));
            }
            let turn = read_turn(&tx, &before_turn.id)?;
            let session = read_session(&tx, &before_session.id)?;
            if turn.status != after_turn.status
                || turn.error != after_turn.error
                || (event.kind == "provider.error" && turn.completed_at.is_none())
                || (event.kind == "provider.event_stream_unknown" && turn.completed_at.is_some())
            {
                return Err(RuntimeError::Conflict(
                    "provider failure receipt lacks corresponding durable turn result".into(),
                ));
            }
            // A later explicit reattachment or closure may legitimately
            // change the session. Do not treat a forged event accompanied
            // only by a separately changed turn as proof of this bundle.
            // Always return current durable state, never replay an old
            // projection over a newer owner.
            let session_is_failure_result = session.status == after_session.status
                && session.active_turn_id == after_session.active_turn_id
                && session.failure_code == after_session.failure_code
                && session.failure_message == after_session.failure_message
                && session.updated_at >= after_session.updated_at;
            let session_has_newer_owner = session.active_turn_id.as_deref().is_some_and(|id| {
                id != before_turn.id.as_str() && session.updated_at > after_session.updated_at
            });
            let session_explicitly_closed = session.status == "closed"
                && session.closed_at.is_some()
                && session.updated_at > after_session.updated_at;
            if !(session_is_failure_result || session_has_newer_owner || session_explicitly_closed)
            {
                return Err(RuntimeError::Conflict(
                    "provider failure receipt lacks corresponding session quarantine".into(),
                ));
            }
            return Ok(ProviderTerminalCommit {
                turn,
                session: Some(session),
                event: receipt,
            });
        }

        if read_turn(&tx, &before_turn.id)? != *before_turn
            || read_session(&tx, &before_session.id)? != *before_session
        {
            return Err(RuntimeError::Conflict(
                "provider failure turn/session changed before critical receipt".into(),
            ));
        }
        let changed_turn = tx
            .execute(
                "UPDATE turns SET status=?1,completed_at=?2,error_json=?3
             WHERE id=?4 AND session_id=?5 AND status=?6 AND completed_at IS ?7",
                params![
                    after_turn.status,
                    after_turn.completed_at,
                    opt_json_to_string(after_turn.error.as_ref())?,
                    before_turn.id,
                    before_turn.session_id,
                    before_turn.status,
                    before_turn.completed_at
                ],
            )
            .map_err(|error| db_error("failed projecting provider failure turn", error))?;
        if changed_turn != 1 {
            return Err(RuntimeError::Conflict(
                "provider failure turn CAS lost".into(),
            ));
        }
        let changed_session = tx
            .execute(
                "UPDATE sessions SET status=?1,active_turn_id=?2,failure_code=?3,
                 failure_message=?4,updated_at=?5
             WHERE id=?6 AND status=?7 AND active_turn_id=?8 AND updated_at=?9",
                params![
                    after_session.status,
                    after_session.active_turn_id,
                    after_session.failure_code,
                    after_session.failure_message,
                    after_session.updated_at,
                    before_session.id,
                    before_session.status,
                    before_session.active_turn_id,
                    before_session.updated_at
                ],
            )
            .map_err(|error| db_error("failed projecting provider failure session", error))?;
        if changed_session != 1 {
            return Err(RuntimeError::Conflict(
                "provider failure session CAS lost".into(),
            ));
        }
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit()
            .map_err(|error| db_error("failed committing provider failure journal", error))?;
        Ok(ProviderTerminalCommit {
            turn: after_turn.clone(),
            session: Some(after_session.clone()),
            event: receipt,
        })
    }
}
