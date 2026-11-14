//! Native send failures must not separately persist a dispatch fence, turn,
//! session, approvals and Critical outcome receipt. Never retry UNKNOWN sends.
use runtime_core::{
    ApprovalRecord, NewRuntimeEvent, ProviderDispatchOutcomeCommit, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionRecord, TurnAdmissionRecord,
    TurnDispatchState, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{
    db_error, fetch_runtime_event_by_event_id, open_connection, opt_json_to_string,
    opt_string_to_json, string_to_json,
};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_turn_start::read_admission;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

pub(crate) fn read_approval(
    tx: &Transaction<'_>,
    id: &str,
) -> Result<ApprovalRecord, RuntimeError> {
    tx.query_row(
        "SELECT id,session_id,turn_id,origin,tool_call_id,provider_approval_ref,
                status,request_json,response_json,created_at,resolved_at
         FROM approvals WHERE id=?1",
        [id],
        |row| {
            Ok(ApprovalRecord {
                id: row.get(0)?,
                session_id: row.get(1)?,
                turn_id: row.get(2)?,
                origin: row.get(3)?,
                tool_call_id: row.get(4)?,
                provider_approval_ref: row.get(5)?,
                status: row.get(6)?,
                request: string_to_json(row.get(7)?)?,
                response: opt_string_to_json(row.get(8)?)?,
                created_at: row.get(9)?,
                resolved_at: row.get(10)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed inspecting dispatch outcome approval", error))?
    .ok_or_else(|| RuntimeError::NotFound(format!("approval {id}")))
}

fn verify_bundle(
    before_admission: &TurnAdmissionRecord,
    after_admission: &TurnAdmissionRecord,
    before_turn: &TurnRecord,
    after_turn: &TurnRecord,
    before_session: &SessionRecord,
    after_session: &SessionRecord,
    approvals: &[(ApprovalRecord, ApprovalRecord)],
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let not_dispatched = match after_admission.dispatch_state {
        TurnDispatchState::NotDispatched => true,
        TurnDispatchState::Unknown => false,
        _ => {
            return Err(RuntimeError::ProtocolViolation(
                "invalid provider send outcome".into(),
            ))
        }
    };
    let outcome = if not_dispatched {
        "not_dispatched"
    } else {
        "unknown"
    };
    let (kind, id_suffix) = if not_dispatched {
        ("turn.failed", "not_dispatched")
    } else {
        ("turn.dispatch_unknown", "dispatch_unknown")
    };
    let error = after_admission.dispatch_error.as_ref().ok_or_else(|| {
        RuntimeError::ProtocolViolation("missing provider dispatch outcome evidence".into())
    })?;
    let message = error["message"].as_str().unwrap_or_default();
    let code = error["code"].as_str();
    let mut expected_admission = before_admission.clone();
    expected_admission.dispatch_state = after_admission.dispatch_state;
    expected_admission.updated_at = after_admission.updated_at;
    expected_admission.dispatch_error = Some(serde_json::json!({"code":code,"message":message}));
    let mut expected_turn = before_turn.clone();
    expected_turn.status = if not_dispatched {
        "failed"
    } else {
        "dispatch_unknown"
    }
    .into();
    expected_turn.error =
        Some(serde_json::json!({"message":message,"provider_dispatch_outcome":outcome}));
    if not_dispatched {
        expected_turn.completed_at = after_turn.completed_at;
    }
    let mut expected_session = before_session.clone();
    expected_session.status = if not_dispatched {
        "ready"
    } else {
        "turn_recovery_required"
    }
    .into();
    expected_session.updated_at = after_session.updated_at;
    if not_dispatched {
        expected_session.active_turn_id = None;
    }
    let allowed_start = if not_dispatched {
        matches!(
            before_admission.dispatch_state,
            TurnDispatchState::Pending
                | TurnDispatchState::Dispatching
                | TurnDispatchState::Unknown
                | TurnDispatchState::NotDispatched
        )
    } else {
        before_admission.dispatch_state == TurnDispatchState::Dispatching
    };
    if !allowed_start
        || message.is_empty()
        || error != &serde_json::json!({"code":code,"message":message})
        || after_admission != &expected_admission
        || after_turn != &expected_turn
        || after_session != &expected_session
        || after_admission.updated_at < before_admission.updated_at
        || after_session.updated_at < before_session.updated_at
        || before_admission.turn_id != before_turn.id
        || before_admission.session_id != before_turn.session_id
        || before_session.id != before_turn.session_id
        || before_admission.provider != before_session.provider
        || before_session.active_turn_id.as_deref() != Some(before_turn.id.as_str())
        || before_session.closed_at.is_some()
        || matches!(before_session.status.as_str(), "closed" | "failed")
        || !matches!(
            before_turn.status.as_str(),
            "admitted" | "waiting_for_approval" | "dispatch_unknown"
        )
        || before_turn.completed_at.is_some()
        || before_admission.provider_native_turn_id.is_some()
        || (not_dispatched && after_turn.completed_at.is_none())
        || (not_dispatched && after_turn.completed_at < before_turn.started_at)
        || event.event_id != format!("evt_turn_{}_{}", before_turn.id, id_suffix)
        || event.kind != kind
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != before_session.id
        || event.session_id.as_deref() != Some(before_session.id.as_str())
        || event.turn_id.as_deref() != Some(before_turn.id.as_str())
        || event.team_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || event.payload != serde_json::json!({"error":message,"provider_dispatch_outcome":outcome})
        || (!not_dispatched && !approvals.is_empty())
    {
        return Err(RuntimeError::ProtocolViolation(
            "provider dispatch outcome does not own one exact turn/session/event transition".into(),
        ));
    }
    let mut previous_id: Option<&str> = None;
    for (before, after) in approvals {
        let mut expected = before.clone();
        expected.status = "cancelled".into();
        expected.response =
            Some(serde_json::json!({"reason":"provider dispatch was proven not to have occurred"}));
        expected.resolved_at = after.resolved_at;
        if !not_dispatched
            || before.session_id != before_session.id
            || before.turn_id != before_turn.id
            || before.status != "pending"
            || before.resolved_at.is_some()
            || after.resolved_at.is_none()
            || after.resolved_at < Some(before.created_at)
            || after != &expected
            || previous_id.is_some_and(|id| id >= before.id.as_str())
        {
            return Err(RuntimeError::ProtocolViolation(
                "dispatch outcome approvals are not an exact ordered pending-to-cancelled set"
                    .into(),
            ));
        }
        previous_id = Some(&before.id);
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn commit_provider_dispatch_outcome(
        &self,
        before_admission: &TurnAdmissionRecord,
        after_admission: &TurnAdmissionRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        approvals: &[(ApprovalRecord, ApprovalRecord)],
        event: &NewRuntimeEvent,
    ) -> Result<ProviderDispatchOutcomeCommit, RuntimeError> {
        verify_bundle(
            before_admission,
            after_admission,
            before_turn,
            after_turn,
            before_session,
            after_session,
            approvals,
            event,
        )?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting dispatch outcome transaction", error))?;
        let saved_admission = read_admission(&tx, &before_turn.id)?;
        let saved_turn = read_turn(&tx, &before_turn.id)?;
        let saved_session = read_session(&tx, &before_session.id)?;
        if let Some(existing) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if existing.scope != event.scope
                || existing.scope_id != event.scope_id
                || existing.session_id != event.session_id
                || existing.turn_id != event.turn_id
                || existing.team_id != event.team_id
                || existing.kind != event.kind
                || existing.criticality != event.criticality
                || existing.payload != event.payload
                || existing.provider != event.provider
                || existing.provider_seq != event.provider_seq
                || saved_admission != *after_admission
                || saved_turn != *after_turn
                || saved_session != *after_session
                || approvals
                    .iter()
                    .any(|(_, after)| read_approval(&tx, &after.id).ok().as_ref() != Some(after))
            {
                return Err(RuntimeError::Conflict(
                    "dispatch outcome Critical receipt does not match durable state".into(),
                ));
            }
            return Ok(ProviderDispatchOutcomeCommit {
                admission: saved_admission,
                turn: saved_turn,
                session: saved_session,
                approvals: approvals.iter().map(|(_, after)| after.clone()).collect(),
                event: existing,
            });
        }
        if saved_admission != *before_admission
            || saved_turn != *before_turn
            || saved_session != *before_session
        {
            return Err(RuntimeError::Conflict(
                "dispatch outcome owner changed".into(),
            ));
        }
        // An in-memory caller may have omitted a freshly admitted pending
        // approval. That omission must not allow a terminal failed turn to
        // retain a dangling operator decision, even across separate runtimes.
        let mut pending_statement = tx
            .prepare("SELECT id FROM approvals WHERE session_id=?1 AND turn_id=?2 AND status='pending' ORDER BY id")
            .map_err(|error| db_error("failed querying pending dispatch approvals", error))?;
        let pending = pending_statement
            .query_map(params![before_session.id, before_turn.id], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| db_error("failed reading pending approval IDs", error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| db_error("failed collecting pending approval IDs", error))?;
        drop(pending_statement);
        let expected: Vec<_> = approvals
            .iter()
            .map(|(before, _)| before.id.clone())
            .collect();
        if (after_admission.dispatch_state == TurnDispatchState::NotDispatched
            && pending != expected)
            || (after_admission.dispatch_state == TurnDispatchState::Unknown && !pending.is_empty())
        {
            return Err(RuntimeError::Conflict(
                "pending approval set changed before dispatch outcome journal".into(),
            ));
        }
        for (before, _) in approvals {
            if read_approval(&tx, &before.id)? != *before {
                return Err(RuntimeError::Conflict(
                    "dispatch outcome approval changed before commit".into(),
                ));
            }
        }
        let changed = tx
            .execute(
                "UPDATE turn_admissions SET dispatch_state=?1, dispatch_error_json=?2,updated_at=?3
             WHERE turn_id=?4 AND dispatch_state=?5 AND updated_at=?6",
                params![
                    after_admission.dispatch_state.as_str(),
                    opt_json_to_string(after_admission.dispatch_error.as_ref())?,
                    after_admission.updated_at,
                    before_admission.turn_id,
                    before_admission.dispatch_state.as_str(),
                    before_admission.updated_at
                ],
            )
            .map_err(|error| db_error("failed committing dispatch outcome admission", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict("dispatch admission CAS lost".into()));
        }
        let changed = tx.execute(
            "UPDATE turns SET status=?1,completed_at=?2,error_json=?3 WHERE id=?4 AND status=?5",
            params![after_turn.status, after_turn.completed_at,
                opt_json_to_string(after_turn.error.as_ref())?, before_turn.id, before_turn.status],
        ).map_err(|error| db_error("failed committing dispatch outcome turn", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict("dispatch turn CAS lost".into()));
        }
        let changed = tx
            .execute(
                "UPDATE sessions SET status=?1,active_turn_id=?2,updated_at=?3
             WHERE id=?4 AND status=?5 AND active_turn_id=?6 AND updated_at=?7",
                params![
                    after_session.status,
                    after_session.active_turn_id,
                    after_session.updated_at,
                    before_session.id,
                    before_session.status,
                    before_session.active_turn_id,
                    before_session.updated_at
                ],
            )
            .map_err(|error| db_error("failed committing dispatch outcome session", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict("dispatch session CAS lost".into()));
        }
        for (before, after) in approvals {
            let changed = tx
                .execute(
                    "UPDATE approvals SET status='cancelled',response_json=?1,resolved_at=?2
                 WHERE id=?3 AND status='pending' AND resolved_at IS NULL",
                    params![
                        opt_json_to_string(after.response.as_ref())?,
                        after.resolved_at,
                        before.id
                    ],
                )
                .map_err(|error| db_error("failed cancelling pending dispatch approval", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict("dispatch approval CAS lost".into()));
            }
        }
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit()
            .map_err(|error| db_error("failed committing dispatch outcome event", error))?;
        Ok(ProviderDispatchOutcomeCommit {
            admission: after_admission.clone(),
            turn: after_turn.clone(),
            session: after_session.clone(),
            approvals: approvals.iter().map(|(_, after)| after.clone()).collect(),
            event: receipt,
        })
    }
}
