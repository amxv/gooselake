use runtime_core::{
    ApprovalRecord, NewRuntimeEvent, ProviderApprovalDecisionReceipt, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionRecord, TurnRecord,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_provider_approval::existing_provider_approval;
use crate::SqliteRuntimeRepository;

fn claimed_attempt(record: &ApprovalRecord) -> Result<&str, RuntimeError> {
    record
        .response
        .as_ref()
        .and_then(|value| value.get("decision_attempt_id"))
        .and_then(|value| value.as_str())
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "provider decision requires durable nonempty attempt identity".into(),
            )
        })
}

fn validate_decision_state(
    pending: &ApprovalRecord,
    reserved: &ApprovalRecord,
    event: &NewRuntimeEvent,
    is_release: bool,
) -> Result<(), RuntimeError> {
    let attempt_id = claimed_attempt(reserved)?;
    let decision = reserved.response.as_ref().map(|body| &body["decision"]);
    if pending.id.is_empty()
        || pending.id != reserved.id
        || pending.session_id != reserved.session_id
        || pending.turn_id != reserved.turn_id
        || pending.origin != "provider"
        || reserved.origin != "provider"
        || pending.tool_call_id != reserved.tool_call_id
        || pending.provider_approval_ref != reserved.provider_approval_ref
        || pending.request != reserved.request
        || pending.status != "pending"
        || reserved.status != "decision_pending"
        || pending.response.is_some()
        || pending.resolved_at.is_some()
        || reserved.resolved_at.is_some()
        || !matches!(
            decision.and_then(|s| s.as_str()),
            Some("accept" | "decline")
        )
        || event.event_id
            != format!(
                "evt_approval_{}_decision_{}_{}",
                pending.id,
                if is_release {
                    "not_dispatched"
                } else {
                    "reserved"
                },
                attempt_id
            )
        || event.kind
            != if is_release {
                "approval.decision_not_dispatched"
            } else {
                "approval.decision_reserved"
            }
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != pending.session_id
        || event.session_id.as_deref() != Some(pending.session_id.as_str())
        || event.turn_id.as_deref() != Some(pending.turn_id.as_str())
        || event.criticality != RuntimeEventCriticality::Critical
        || event.payload["approval_id"] != pending.id
        || event.payload["decision_attempt_id"] != attempt_id
        || event.payload["decision"] != reserved.response.as_ref().unwrap()["decision"]
        || event.payload["origin"] != "provider"
    {
        return Err(RuntimeError::ProtocolViolation(
            "provider-native approval decision state and event authority mismatch".into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// The unique pre-native reservation. A lost commit acknowledgment or a
    /// concurrent caller MUST see Conflict: neither may send the native
    /// decision merely because the same reservation is already durable.
    pub fn claim_provider_approval_decision(
        &self,
        pending: &ApprovalRecord,
        reserved: &ApprovalRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderApprovalDecisionReceipt, RuntimeError> {
        validate_decision_state(pending, reserved, event, false)?;
        if turn.id != pending.turn_id
            || turn.session_id != pending.session_id
            || !matches!(turn.status.as_str(), "waiting_for_approval" | "in_progress")
            || turn.completed_at.is_some()
            || session.id != pending.session_id
            || !matches!(
                session.status.as_str(),
                "waiting_for_approval" | "turn_running"
            )
            || session.active_turn_id.as_deref() != Some(turn.id.as_str())
        {
            return Err(RuntimeError::InvalidState(
                "provider-native approval decision requires attached waiting turn".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting approval decision claim", error))?;
        let actual = existing_provider_approval(&tx, pending)?;
        if actual.as_ref() != Some(pending) {
            return Err(RuntimeError::Conflict(
                "approval already changed or a different native decision is in flight".into(),
            ));
        }
        if fetch_runtime_event_by_event_id(&tx, &event.event_id)?.is_some() {
            return Err(RuntimeError::Conflict(
                "native approval decision attempt was previously reserved and cannot be resent"
                    .into(),
            ));
        }
        let live_ownership: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT s.status, s.active_turn_id
                 FROM sessions s JOIN turns t ON t.session_id=s.id
                 JOIN turn_admissions a ON a.turn_id=t.id AND a.session_id=s.id
                 WHERE s.id=?1 AND t.id=?2
                   AND s.status IN ('waiting_for_approval','turn_running')
                   AND t.status IN ('waiting_for_approval','in_progress') AND t.completed_at IS NULL
                   AND s.updated_at=?3 AND a.dispatch_state='dispatched'",
                params![pending.session_id, pending.turn_id, session.updated_at],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| {
                db_error("failed checking native approval decision ownership", error)
            })?;
        if live_ownership
            .as_ref()
            .is_none_or(|(_, active)| active.as_deref() != Some(turn.id.as_str()))
        {
            return Err(RuntimeError::Conflict(
                "native approval decision lost dispatched turn ownership".into(),
            ));
        }

        let changed = tx
            .execute(
                "UPDATE approvals SET status='decision_pending', response_json=?1
                 WHERE id=?2 AND session_id=?3 AND turn_id=?4 AND origin='provider'
                   AND status='pending' AND response_json IS NULL AND resolved_at IS NULL",
                params![
                    json_to_string(reserved.response.as_ref().unwrap())?,
                    pending.id,
                    pending.session_id,
                    pending.turn_id,
                ],
            )
            .map_err(|error| db_error("failed reserving native provider decision", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "native approval decision already reserved by another process".into(),
            ));
        }
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit().map_err(|error| {
            db_error(
                "failed committing native approval decision reservation",
                error,
            )
        })?;
        Ok(ProviderApprovalDecisionReceipt {
            approval: reserved.clone(),
            event: Some(receipt),
        })
    }

    /// Only an adapter's *proven not-dispatched* response can use this
    /// transition. Both the durable decision reservation and its outcome
    /// event are atomically released; ambiguous native effects cannot call it.
    pub fn release_unexecuted_provider_approval_decision(
        &self,
        reserved: &ApprovalRecord,
        pending: &ApprovalRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderApprovalDecisionReceipt, RuntimeError> {
        validate_decision_state(pending, reserved, event, true)?;
        let attempt_id = claimed_attempt(reserved)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting no-effect approval release", error))?;
        let claim_id = format!(
            "evt_approval_{}_decision_reserved_{}",
            reserved.id, attempt_id
        );
        if fetch_runtime_event_by_event_id(&tx, &claim_id)?.is_none_or(|row| {
            row.kind != "approval.decision_reserved"
                || row.payload["approval_id"] != reserved.id
                || row.payload["decision_attempt_id"] != attempt_id
        }) {
            return Err(RuntimeError::Conflict(
                "cannot release native provider decision without exact committed reservation"
                    .into(),
            ));
        }

        let actual = existing_provider_approval(&tx, pending)?;
        if let Some(previous_receipt) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if previous_receipt.kind != event.kind
                || previous_receipt.scope_id != event.scope_id
                || previous_receipt.payload != event.payload
                || actual.as_ref() != Some(pending)
            {
                return Err(RuntimeError::Conflict(
                    "no-effect decision release conflicts with newer provider authority".into(),
                ));
            }
            return Ok(ProviderApprovalDecisionReceipt {
                approval: pending.clone(),
                event: None,
            });
        }
        if actual.as_ref() != Some(reserved) {
            return Err(RuntimeError::Conflict(
                "native decision reservation no longer matches no-effect response".into(),
            ));
        }
        let still_attached: bool = tx
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sessions s JOIN turns t ON t.session_id=s.id
                    WHERE s.id=?1 AND t.id=?2 AND s.active_turn_id=t.id
                      AND s.status IN ('waiting_for_approval','turn_running')
                      AND t.status IN ('waiting_for_approval','in_progress') AND t.completed_at IS NULL
                )",
                params![reserved.session_id, reserved.turn_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed checking owner before no-effect approval release",
                    error,
                )
            })?;
        if !still_attached {
            return Err(RuntimeError::Conflict(
                "cannot restore pending native approval after turn ownership changed".into(),
            ));
        }
        let changed = tx
            .execute(
                "UPDATE approvals SET status='pending', response_json=NULL
                 WHERE id=?1 AND session_id=?2 AND turn_id=?3 AND origin='provider'
                   AND status='decision_pending' AND response_json=?4 AND resolved_at IS NULL",
                params![
                    reserved.id,
                    reserved.session_id,
                    reserved.turn_id,
                    json_to_string(reserved.response.as_ref().unwrap())?,
                ],
            )
            .map_err(|error| {
                db_error(
                    "failed restoring provider approval after no-effect result",
                    error,
                )
            })?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "native decision reservation changed before no-effect release".into(),
            ));
        }
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit()
            .map_err(|error| db_error("failed committing no-effect approval release", error))?;
        Ok(ProviderApprovalDecisionReceipt {
            approval: pending.clone(),
            event: Some(receipt),
        })
    }
}
