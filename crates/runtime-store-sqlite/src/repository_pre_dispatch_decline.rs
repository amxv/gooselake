//! A local policy denial never dispatches native work. Its approval, admission,
//! turn, session and two Critical events share one compare-and-swap transaction.
use runtime_core::{
    ApprovalRecord, NewRuntimeEvent, PreDispatchApprovalDeclineCommit, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionRecord, TurnAdmissionRecord,
    TurnDispatchState, TurnRecord,
};
use rusqlite::{params, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection, opt_json_to_string};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_turn_dispatch_outcome::read_approval;
use crate::repository_turn_start::read_admission;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

#[allow(clippy::too_many_arguments)]
fn validate_bundle(
    before_admission: &TurnAdmissionRecord,
    after_admission: &TurnAdmissionRecord,
    before_approval: &ApprovalRecord,
    after_approval: &ApprovalRecord,
    before_turn: &TurnRecord,
    after_turn: &TurnRecord,
    before_session: &SessionRecord,
    after_session: &SessionRecord,
    events: &[NewRuntimeEvent],
) -> Result<(), RuntimeError> {
    let at = after_approval.resolved_at.ok_or_else(|| {
        RuntimeError::ProtocolViolation("local denial has no resolution timestamp".into())
    })?;
    let mut expected_approval = before_approval.clone();
    expected_approval.status = "decline".into();
    expected_approval.response = after_approval.response.clone();
    expected_approval.resolved_at = Some(at);
    let mut expected_admission = before_admission.clone();
    expected_admission.dispatch_state = TurnDispatchState::NotDispatched;
    expected_admission.updated_at = after_admission.updated_at;
    expected_admission.dispatch_error = Some(serde_json::json!({
        "code":"pre_dispatch_approval_declined",
        "message":"runtime pre-dispatch approval declined",
    }));
    let mut expected_turn = before_turn.clone();
    expected_turn.status = "interrupted".into();
    expected_turn.completed_at = Some(at);
    expected_turn.error = Some(serde_json::json!({
        "message":"approval declined before provider dispatch",
    }));
    let mut expected_session = before_session.clone();
    expected_session.status = "ready".into();
    expected_session.active_turn_id = None;
    expected_session.updated_at = after_session.updated_at;

    if before_approval.origin != "runtime_pre_dispatch_policy"
        || before_approval.status != "pending"
        || before_approval.resolved_at.is_some()
        || before_approval.response.is_some()
        || before_approval.provider_approval_ref.is_some()
        || before_admission.dispatch_state != TurnDispatchState::Pending
        || before_admission.dispatch_error.is_some()
        || before_admission.provider_native_turn_id.is_some()
        || !before_admission
            .dispatch_policy
            .pre_dispatch_approval_required
        || before_turn.provider_turn_ref.is_some()
        || before_turn.status != "waiting_for_approval"
        || before_turn.completed_at.is_some()
        || before_session.status != "waiting_for_approval"
        || before_session.closed_at.is_some()
        || before_session.active_turn_id.as_deref() != Some(before_turn.id.as_str())
        || before_approval.turn_id != before_turn.id
        || before_approval.session_id != before_session.id
        || before_admission.turn_id != before_turn.id
        || before_admission.session_id != before_session.id
        || before_admission.provider != before_session.provider
        || before_turn.session_id != before_session.id
        || at < before_approval.created_at
        || before_turn.started_at.is_some_and(|start| at < start)
        || after_admission.updated_at < before_admission.updated_at
        || after_session.updated_at < before_session.updated_at
        || after_approval != &expected_approval
        || after_admission != &expected_admission
        || after_turn != &expected_turn
        || after_session != &expected_session
        || events.len() != 2
    {
        return Err(RuntimeError::ProtocolViolation(
            "local pre-dispatch denial is not one guarded unexecuted turn".into(),
        ));
    }
    for (index, event) in events.iter().enumerate() {
        let (id, kind, payload) = if index == 0 {
            (
                format!("evt_approval_{}_resolved", before_approval.id),
                "approval.resolved",
                serde_json::json!({
                    "approval_id":before_approval.id,
                    "origin":"runtime_pre_dispatch_policy",
                    "decision":"decline",
                }),
            )
        } else {
            (
                format!("evt_turn_{}_pre_dispatch_declined", before_turn.id),
                "turn.interrupted",
                serde_json::json!({"source":"pre_dispatch_approval.declined"}),
            )
        };
        if event.event_id != id
            || event.kind != kind
            || event.payload != payload
            || event.scope != RuntimeEventScope::Session
            || event.scope_id != before_session.id
            || event.session_id.as_deref() != Some(before_session.id.as_str())
            || event.turn_id.as_deref() != Some(before_turn.id.as_str())
            || event.team_id.is_some()
            || event.provider.is_some()
            || event.provider_seq.is_some()
            || event.criticality != RuntimeEventCriticality::Critical
            || event.created_at != at
        {
            return Err(RuntimeError::ProtocolViolation(
                "local denial Critical event identity, scope or payload mismatch".into(),
            ));
        }
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    #[allow(clippy::too_many_arguments)]
    pub fn decline_pre_dispatch_approval(
        &self,
        before_admission: &TurnAdmissionRecord,
        after_admission: &TurnAdmissionRecord,
        before_approval: &ApprovalRecord,
        after_approval: &ApprovalRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        events: &[NewRuntimeEvent],
    ) -> Result<PreDispatchApprovalDeclineCommit, RuntimeError> {
        validate_bundle(
            before_admission,
            after_admission,
            before_approval,
            after_approval,
            before_turn,
            after_turn,
            before_session,
            after_session,
            events,
        )?;
        let mut db = open_connection(&self.database_path)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| db_error("failed starting local denial transaction", err))?;
        let actual_admission = read_admission(&tx, &before_turn.id)?;
        let actual_approval = read_approval(&tx, &before_approval.id)?;
        let actual_turn = read_turn(&tx, &before_turn.id)?;
        let actual_session = read_session(&tx, &before_session.id)?;
        let first = fetch_runtime_event_by_event_id(&tx, &events[0].event_id)?;
        let second = fetch_runtime_event_by_event_id(&tx, &events[1].event_id)?;
        if let (Some(first), Some(second)) = (&first, &second) {
            if actual_admission == *after_admission
                && actual_approval == *after_approval
                && actual_turn == *after_turn
                && actual_session == *after_session
                && [&events[0], &events[1]]
                    .iter()
                    .zip([first, second])
                    .all(|(want, got)| {
                        got.event_id == want.event_id
                            && got.kind == want.kind
                            && got.scope == want.scope
                            && got.scope_id == want.scope_id
                            && got.session_id == want.session_id
                            && got.turn_id == want.turn_id
                            && got.team_id == want.team_id
                            && got.criticality == want.criticality
                            && got.payload == want.payload
                            && got.provider == want.provider
                            && got.provider_seq == want.provider_seq
                    })
            {
                return Ok(PreDispatchApprovalDeclineCommit {
                    admission: actual_admission,
                    approval: actual_approval,
                    turn: actual_turn,
                    session: actual_session,
                    events: vec![first.clone(), second.clone()],
                });
            }
            return Err(RuntimeError::Conflict(
                "local denial Critical receipts disagree with current authority".into(),
            ));
        }
        if first.is_some()
            || second.is_some()
            || actual_admission != *before_admission
            || actual_approval != *before_approval
            || actual_turn != *before_turn
            || actual_session != *before_session
        {
            return Err(RuntimeError::Conflict(
                "local denial has an incomplete receipt or another owner".into(),
            ));
        }
        // Only a newly admitted, indexed policy gate may be denied. Do not
        // silently convert historic partial / legacy rows to safe authority.
        let admitted =
            fetch_runtime_event_by_event_id(&tx, &format!("evt_turn_{}_admitted", before_turn.id))?;
        let requested = fetch_runtime_event_by_event_id(
            &tx,
            &format!("evt_approval_{}_requested", before_approval.id),
        )?;
        if admitted.as_ref().is_none_or(|row| {
            row.kind != "turn.admitted"
                || row.scope != RuntimeEventScope::Session
                || row.scope_id != before_session.id
                || row.criticality != RuntimeEventCriticality::Critical
                || row.session_id.as_deref() != Some(before_session.id.as_str())
                || row.turn_id.as_deref() != Some(before_turn.id.as_str())
                || row.payload["projection_source"] != before_admission.projection_source.as_str()
                || row.payload["requires_approval"] != true
        }) || requested.as_ref().is_none_or(|row| {
            row.kind != "approval.requested"
                || row.scope != RuntimeEventScope::Session
                || row.scope_id != before_session.id
                || row.criticality != RuntimeEventCriticality::Critical
                || row.session_id.as_deref() != Some(before_session.id.as_str())
                || row.turn_id.as_deref() != Some(before_turn.id.as_str())
                || row.payload["approval_id"] != before_approval.id
                || row.payload["origin"] != "runtime_pre_dispatch_policy"
        }) {
            return Err(RuntimeError::Conflict(
                "local denial is missing original Critical admission/request receipts".into(),
            ));
        }
        let pending: i64 = tx.query_row(
            "SELECT COUNT(*) FROM approvals WHERE session_id=?1 AND turn_id=?2 AND status='pending'",
            params![before_session.id, before_turn.id], |row| row.get(0),
        ).map_err(|err| db_error("failed inspecting competing pending approvals", err))?;
        if pending != 1 {
            return Err(RuntimeError::Conflict(
                "local denial has unexpected competing pending approvals".into(),
            ));
        }
        let updated = tx
            .execute(
                "UPDATE approvals SET status='decline', response_json=?1, resolved_at=?2
             WHERE id=?3 AND status='pending' AND resolved_at IS NULL",
                params![
                    opt_json_to_string(after_approval.response.as_ref())?,
                    after_approval.resolved_at,
                    before_approval.id
                ],
            )
            .map_err(|err| db_error("failed resolving denied approval", err))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict("local approval CAS lost".into()));
        }
        let updated = tx
            .execute(
                "UPDATE turn_admissions SET dispatch_state='not_dispatched',
                dispatch_error_json=?1, updated_at=?2
             WHERE turn_id=?3 AND dispatch_state='pending' AND updated_at=?4",
                params![
                    opt_json_to_string(after_admission.dispatch_error.as_ref())?,
                    after_admission.updated_at,
                    before_turn.id,
                    before_admission.updated_at
                ],
            )
            .map_err(|err| db_error("failed releasing denied turn admission", err))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "local denial admission CAS lost".into(),
            ));
        }
        let updated = tx
            .execute(
                "UPDATE turns SET status='interrupted',completed_at=?1,error_json=?2
             WHERE id=?3 AND status='waiting_for_approval' AND completed_at IS NULL",
                params![
                    after_turn.completed_at,
                    opt_json_to_string(after_turn.error.as_ref())?,
                    before_turn.id
                ],
            )
            .map_err(|err| db_error("failed interrupting denied turn", err))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict("local denial turn CAS lost".into()));
        }
        let updated = tx
            .execute(
                "UPDATE sessions SET status='ready',active_turn_id=NULL,updated_at=?1
             WHERE id=?2 AND status='waiting_for_approval' AND active_turn_id=?3 AND updated_at=?4",
                params![
                    after_session.updated_at,
                    before_session.id,
                    before_turn.id,
                    before_session.updated_at
                ],
            )
            .map_err(|err| db_error("failed releasing denied session", err))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "local denial session CAS lost".into(),
            ));
        }
        let mut receipts = Vec::with_capacity(2);
        for event in events {
            receipts.push(insert_runtime_event_in_transaction(&tx, event)?);
        }
        tx.commit()
            .map_err(|err| db_error("failed committing local denial Critical events", err))?;
        Ok(PreDispatchApprovalDeclineCommit {
            admission: after_admission.clone(),
            approval: after_approval.clone(),
            turn: after_turn.clone(),
            session: after_session.clone(),
            events: receipts,
        })
    }
}
