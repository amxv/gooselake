use runtime_core::{
    ApprovalRecord, NewRuntimeEvent, ProviderApprovalFinalization, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionRecord, TurnRecord,
};
use rusqlite::{params, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection, opt_json_to_string};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_provider_approval::existing_provider_approval;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

fn validate_native_resolution(
    reserved: &ApprovalRecord,
    resolved: &ApprovalRecord,
    before_turn: &TurnRecord,
    after_turn: &TurnRecord,
    before_session: &SessionRecord,
    after_session: &SessionRecord,
    event: &NewRuntimeEvent,
) -> Result<String, RuntimeError> {
    let receipt = reserved.response.as_ref().ok_or_else(|| {
        RuntimeError::ProtocolViolation("native approval decision lacked reservation".into())
    })?;
    let attempt = receipt["decision_attempt_id"].as_str().ok_or_else(|| {
        RuntimeError::ProtocolViolation("native approval decision lacked attempt ID".into())
    })?;
    let decision = receipt["decision"].as_str().unwrap_or_default();
    let mut allowed_turn = before_turn.clone();
    let mut allowed_session = before_session.clone();
    let attached = before_session.active_turn_id.as_deref() == Some(reserved.turn_id.as_str())
        && matches!(
            before_session.status.as_str(),
            "turn_running" | "waiting_for_approval"
        )
        && matches!(
            before_turn.status.as_str(),
            "waiting_for_approval" | "in_progress"
        )
        && before_turn.completed_at.is_none();
    if after_turn != before_turn || after_session != before_session {
        if !attached {
            return Err(RuntimeError::ProtocolViolation(
                "native decision result cannot resume an unattached or quarantined turn".into(),
            ));
        }
        allowed_turn.status = "in_progress".into();
        allowed_turn.error = None;
        allowed_session.status = "turn_running".into();
        allowed_session.updated_at = after_session.updated_at;
        if after_session.updated_at < before_session.updated_at {
            return Err(RuntimeError::ProtocolViolation(
                "native approval resolution rewound session timestamp".into(),
            ));
        }
    } else if (!attached && !terminal_finished(before_turn))
        || (attached
            && (before_turn.status == "waiting_for_approval"
                || before_session.status == "waiting_for_approval"))
    {
        return Err(RuntimeError::ProtocolViolation(
            "native approval result cannot leave a waiting turn unresolved".into(),
        ));
    }
    let requested_payload = &receipt["payload"];
    if reserved.origin != "provider"
        || reserved.status != "decision_pending"
        || reserved.resolved_at.is_some()
        || reserved.id.is_empty()
        || attempt.is_empty()
        || !matches!(decision, "accept" | "decline")
        || resolved.id != reserved.id
        || resolved.session_id != reserved.session_id
        || resolved.turn_id != reserved.turn_id
        || resolved.origin != reserved.origin
        || resolved.tool_call_id != reserved.tool_call_id
        || resolved.provider_approval_ref != reserved.provider_approval_ref
        || resolved.request != reserved.request
        || resolved.created_at != reserved.created_at
        || resolved.status != decision
        || resolved.resolved_at.is_none()
        || resolved
            .response
            .as_ref()
            .is_some_and(|payload| payload != requested_payload)
        || (resolved.response.is_none() && !requested_payload.is_null())
        || before_turn.id != reserved.turn_id
        || before_turn.session_id != reserved.session_id
        || before_session.id != reserved.session_id
        || after_turn != &allowed_turn
        || after_session != &allowed_session
        || event.event_id != format!("evt_approval_{}_resolved", reserved.id)
        || event.kind != "approval.resolved"
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != reserved.session_id
        || event.session_id.as_deref() != Some(reserved.session_id.as_str())
        || event.turn_id.as_deref() != Some(reserved.turn_id.as_str())
        || event.criticality != RuntimeEventCriticality::Critical
        || event.payload["approval_id"] != reserved.id
        || event.payload["decision"] != decision
        || event.payload["decision_attempt_id"] != attempt
        || event.payload["origin"] != "provider"
    {
        return Err(RuntimeError::ProtocolViolation(
            "native provider approval resolution and critical event bundle mismatch".into(),
        ));
    }
    Ok(attempt.to_string())
}

fn terminal_finished(turn: &TurnRecord) -> bool {
    turn.completed_at.is_some()
        && matches!(
            turn.status.as_str(),
            "completed" | "failed" | "interrupted" | "cancelled" | "superseded"
        )
}

fn validate_unknown_result(
    reserved: &ApprovalRecord,
    before_turn: &TurnRecord,
    after_turn: &TurnRecord,
    before_session: &SessionRecord,
    after_session: &SessionRecord,
    event: &NewRuntimeEvent,
) -> Result<String, RuntimeError> {
    let body = reserved.response.as_ref().ok_or_else(|| {
        RuntimeError::ProtocolViolation("ambiguous native decision missing reservation".into())
    })?;
    let attempt = body["decision_attempt_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RuntimeError::ProtocolViolation("native decision attempt absent".into()))?;
    let decision = body["decision"].as_str().unwrap_or_default();
    let message = event.payload["error"].as_str().unwrap_or_default();
    let attached = before_session.active_turn_id.as_deref() == Some(reserved.turn_id.as_str())
        && matches!(
            before_session.status.as_str(),
            "turn_running" | "waiting_for_approval"
        )
        && matches!(
            before_turn.status.as_str(),
            "waiting_for_approval" | "in_progress"
        )
        && before_turn.completed_at.is_none();
    let mut expected_turn = before_turn.clone();
    let mut expected_session = before_session.clone();
    if attached {
        expected_turn.status = "approval_decision_unknown".into();
        expected_turn.error = Some(serde_json::json!({
            "message": message,
            "approval_id": reserved.id,
            "decision_attempt_id": attempt,
        }));
        expected_session.status = "turn_recovery_required".into();
        expected_session.updated_at = after_session.updated_at;
    }
    if reserved.origin != "provider"
        || reserved.status != "decision_pending"
        || reserved.resolved_at.is_some()
        || !matches!(decision, "accept" | "decline")
        || !attached && !terminal_finished(before_turn)
        || before_turn.id != reserved.turn_id
        || before_turn.session_id != reserved.session_id
        || before_session.id != reserved.session_id
        || after_turn != &expected_turn
        || after_session != &expected_session
        || after_session.updated_at < before_session.updated_at
        || message.is_empty()
        || event.event_id != format!("evt_approval_{}_decision_unknown_{attempt}", reserved.id)
        || event.kind != "approval.decision_unknown"
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != reserved.session_id
        || event.session_id.as_deref() != Some(reserved.session_id.as_str())
        || event.turn_id.as_deref() != Some(reserved.turn_id.as_str())
        || event.criticality != RuntimeEventCriticality::Critical
        || event.payload["approval_id"] != reserved.id
        || event.payload["decision_attempt_id"] != attempt
        || event.payload["decision"] != decision
        || event.payload["origin"] != "provider"
    {
        return Err(RuntimeError::ProtocolViolation(
            "ambiguous native provider decision and critical journal mismatch".into(),
        ));
    }
    Ok(attempt.to_string())
}

fn verify_reservation(
    tx: &rusqlite::Transaction<'_>,
    reserved: &ApprovalRecord,
    attempt: &str,
) -> Result<(), RuntimeError> {
    let claim_id = format!("evt_approval_{}_decision_reserved_{}", reserved.id, attempt);
    if fetch_runtime_event_by_event_id(tx, &claim_id)?.is_none_or(|row| {
        row.kind != "approval.decision_reserved"
            || row.scope != RuntimeEventScope::Session
            || row.session_id.as_deref() != Some(reserved.session_id.as_str())
            || row.turn_id.as_deref() != Some(reserved.turn_id.as_str())
            || row.criticality != RuntimeEventCriticality::Critical
            || row.scope_id != reserved.session_id
            || row.payload["approval_id"] != reserved.id
            || row.payload["origin"] != "provider"
            || row.payload["decision_attempt_id"] != attempt
            || row.payload["decision"] != reserved.response.as_ref().unwrap()["decision"]
    }) {
        return Err(RuntimeError::Conflict(
            "native decision outcome missing the exact pre-effect reservation".into(),
        ));
    }
    Ok(())
}

fn apply_decision_turn_session(
    tx: &rusqlite::Transaction<'_>,
    before_turn: &TurnRecord,
    after_turn: &TurnRecord,
    before_session: &SessionRecord,
    after_session: &SessionRecord,
) -> Result<(), RuntimeError> {
    let actual_turn = read_turn(tx, &before_turn.id)?;
    let actual_session = read_session(tx, &before_session.id)?;
    // Provider completion may race the acknowledgement across two processes.
    // Its separately committed terminal receipt outranks turn-resume or
    // quarantine projections. Still journal the approval result below.
    if terminal_finished(&actual_turn) {
        return Ok(());
    }
    if actual_turn != *before_turn || actual_session != *before_session {
        return Err(RuntimeError::Conflict(
            "provider-native decision turn/session authority changed during native call".into(),
        ));
    }
    if after_turn != before_turn {
        let count = tx
            .execute(
                "UPDATE turns SET status=?1,error_json=?2
             WHERE id=?3 AND session_id=?4 AND status=?5 AND completed_at IS NULL",
                params![
                    after_turn.status,
                    opt_json_to_string(after_turn.error.as_ref())?,
                    before_turn.id,
                    before_turn.session_id,
                    before_turn.status
                ],
            )
            .map_err(|error| db_error("failed applying native decision turn outcome", error))?;
        if count != 1 {
            return Err(RuntimeError::Conflict(
                "native approval turn CAS lost".into(),
            ));
        }
    }
    if after_session != before_session {
        let count = tx
            .execute(
                "UPDATE sessions SET status=?1,updated_at=?2
             WHERE id=?3 AND status=?4 AND active_turn_id=?5 AND updated_at=?6",
                params![
                    after_session.status,
                    after_session.updated_at,
                    before_session.id,
                    before_session.status,
                    before_session.active_turn_id,
                    before_session.updated_at
                ],
            )
            .map_err(|error| db_error("failed applying native decision session outcome", error))?;
        if count != 1 {
            return Err(RuntimeError::Conflict(
                "native approval session CAS lost".into(),
            ));
        }
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// The provider has already acknowledged the decision. This receipt is
    /// durable independently of a response lost after the SQLite COMMIT,
    /// and cannot be used as permission to call the provider again.
    pub fn finalize_provider_approval_decision(
        &self,
        reserved: &ApprovalRecord,
        resolved: &ApprovalRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderApprovalFinalization, RuntimeError> {
        let attempt = validate_native_resolution(
            reserved,
            resolved,
            before_turn,
            after_turn,
            before_session,
            after_session,
            event,
        )?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native approval resolution", error))?;

        verify_reservation(&tx, reserved, &attempt)?;

        let actual = existing_provider_approval(&tx, reserved)?;
        if let Some(previous) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if previous.kind != event.kind
                || previous.scope != event.scope
                || previous.scope_id != event.scope_id
                || previous.session_id != event.session_id
                || previous.turn_id != event.turn_id
                || previous.criticality != event.criticality
                || previous.payload != event.payload
                || actual.as_ref().is_none_or(|row| {
                    row.id != resolved.id
                        || row.status != resolved.status
                        || row.response != resolved.response
                        || row.resolved_at.is_none()
                })
            {
                return Err(RuntimeError::Conflict(
                    "native approval resolution receipt conflicts with current durable authority"
                        .into(),
                ));
            }
            return Ok(ProviderApprovalFinalization {
                approval: actual.unwrap(),
                turn: read_turn(&tx, &reserved.turn_id)?,
                session: read_session(&tx, &reserved.session_id)?,
                event: None,
            });
        }
        if actual.as_ref() != Some(reserved) {
            return Err(RuntimeError::Conflict(
                "native approval decision changed after provider acknowledgment".into(),
            ));
        }
        apply_decision_turn_session(&tx, before_turn, after_turn, before_session, after_session)?;
        let changed = tx
            .execute(
                "UPDATE approvals SET status=?1,response_json=?2,resolved_at=?3
                 WHERE id=?4 AND session_id=?5 AND turn_id=?6
                   AND origin='provider' AND status='decision_pending'
                   AND response_json=?7 AND resolved_at IS NULL",
                params![
                    resolved.status,
                    opt_json_to_string(resolved.response.as_ref())?,
                    resolved.resolved_at,
                    reserved.id,
                    reserved.session_id,
                    reserved.turn_id,
                    opt_json_to_string(reserved.response.as_ref())?,
                ],
            )
            .map_err(|error| db_error("failed recording acknowledged provider approval", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "provider approval resolution lost reserved decision authority".into(),
            ));
        }
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        let result = ProviderApprovalFinalization {
            approval: resolved.clone(),
            turn: read_turn(&tx, &resolved.turn_id)?,
            session: read_session(&tx, &resolved.session_id)?,
            event: Some(receipt),
        };
        tx.commit().map_err(|error| {
            db_error("failed committing provider approval result journal", error)
        })?;
        Ok(result)
    }

    /// An ambiguous provider response *never* releases its reserved attempt.
    /// The irreversible possibility of a native effect and its quarantine
    /// projection must be recorded with one critical journal receipt.
    pub fn quarantine_provider_approval_decision(
        &self,
        reserved: &ApprovalRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderApprovalFinalization, RuntimeError> {
        let attempt = validate_unknown_result(
            reserved,
            before_turn,
            after_turn,
            before_session,
            after_session,
            event,
        )?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting ambiguous approval quarantine", error))?;
        verify_reservation(&tx, reserved, &attempt)?;
        let actual = existing_provider_approval(&tx, reserved)?;
        if let Some(previous) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if previous.kind != event.kind
                || previous.scope != event.scope
                || previous.scope_id != event.scope_id
                || previous.session_id != event.session_id
                || previous.turn_id != event.turn_id
                || previous.criticality != event.criticality
                || previous.payload != event.payload
                || actual.as_ref() != Some(reserved)
            {
                return Err(RuntimeError::Conflict(
                    "ambiguous decision receipt collides with changed authority".into(),
                ));
            }
            let persisted_turn = read_turn(&tx, &reserved.turn_id)?;
            let persisted_session = read_session(&tx, &reserved.session_id)?;
            if !terminal_finished(&persisted_turn)
                && (persisted_turn.status != "approval_decision_unknown"
                    || persisted_session.status != "turn_recovery_required"
                    || persisted_session.active_turn_id.as_deref()
                        != Some(reserved.turn_id.as_str()))
            {
                return Err(RuntimeError::Conflict(
                    "ambiguous approval receipt lacks durable quarantine state".into(),
                ));
            }
            return Ok(ProviderApprovalFinalization {
                approval: reserved.clone(),
                turn: persisted_turn,
                session: persisted_session,
                event: None,
            });
        }
        if actual.as_ref() != Some(reserved) {
            return Err(RuntimeError::Conflict(
                "ambiguous native decision lost exact reservation".into(),
            ));
        }
        apply_decision_turn_session(&tx, before_turn, after_turn, before_session, after_session)?;
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        let result = ProviderApprovalFinalization {
            approval: reserved.clone(),
            turn: read_turn(&tx, &reserved.turn_id)?,
            session: read_session(&tx, &reserved.session_id)?,
            event: Some(receipt),
        };
        tx.commit().map_err(|error| {
            db_error(
                "failed committing ambiguous provider approval quarantine",
                error,
            )
        })?;
        Ok(result)
    }
}
