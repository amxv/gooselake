use runtime_core::{RuntimeError, WorkspaceRetirementTurnStep};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Reconcile native turn state without redispatching a previously started
    /// interrupt. Even successful interrupt admission isn't terminal evidence.
    pub fn progress_workspace_retirement_turn(
        &self,
        operation_id: &str,
        turn_id: &str,
        at_ms: i64,
    ) -> Result<WorkspaceRetirementTurnStep, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning retirement turn reconciliation", error))?;
        let row: Option<(String,String,String,Option<String>,Option<i64>)> = tx.query_row(
            "SELECT e.phase,t.session_id,t.status,s.active_turn_id,t.completed_at
             FROM runtime_operation_effects e
             JOIN workspace_retirement_fences f ON f.operation_id=e.operation_id
             JOIN runtime_operations op ON op.operation_id=e.operation_id
             JOIN turns t ON t.id=e.target_id
             JOIN sessions s ON s.id=t.session_id
             JOIN workspace_session_ownership o ON o.session_id=s.id AND o.workspace_id=f.workspace_id
             WHERE e.operation_id=?1 AND e.effect_kind='workspace_turn_interrupt' AND e.target_id=?2
               AND f.phase IN ('frozen','manual_review') AND op.phase IN ('requested','manual_review')",
            params![operation_id,turn_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
        ).optional().map_err(|error|db_error("failed inspecting admitted retirement turn",error))?;
        let (phase, session_id, status, active_turn_id, completed_at) = row.ok_or_else(|| {
            RuntimeError::Conflict(format!(
                "turn {turn_id} is not an admitted retirement target"
            ))
        })?;
        if phase == "finalized" {
            return Ok(WorkspaceRetirementTurnStep::Finalized);
        }
        if phase == "manual_review" {
            return Ok(WorkspaceRetirementTurnStep::NeedsReview);
        }
        let dispatch: Option<(String, Option<String>)> = tx.query_row(
            "SELECT dispatch_state,provider_native_turn_id FROM turn_admissions WHERE turn_id=?1",
            [turn_id], |row| Ok((row.get(0)?,row.get(1)?)),
        ).optional().map_err(|error| db_error("failed inspecting retirement turn dispatch",error))?;
        // An admitted active turn can already have an uncertain native
        // dispatch. Snapshot it so an operator can prove attachment closure,
        // but never infer that a fresh interrupt is safe to send.
        if phase == "intended"
            && dispatch
                .as_ref()
                .is_some_and(|(state, _)| matches!(state.as_str(), "unknown" | "dispatching"))
        {
            return Ok(WorkspaceRetirementTurnStep::NeedsReview);
        }
        if phase == "intended"
            && active_turn_id.as_deref() == Some(turn_id)
            && dispatch.as_ref().is_some_and(|(state, native)| {
                matches!(state.as_str(), "pending" | "not_dispatched") && native.is_none()
            })
        {
            let reason = json_to_string(&serde_json::json!({
                "code":"workspace_retirement","provider_dispatch_outcome":"not_dispatched",
            }))?;
            tx.execute(
                "UPDATE turn_admissions SET dispatch_state='not_dispatched',
                dispatch_error_json=json_set(?2,'$.prior_error',json(dispatch_error_json)),
                updated_at=?3 WHERE turn_id=?1",
                params![turn_id, reason, at_ms],
            )
            .map_err(|error| db_error("failed cancelling undispatched retirement input", error))?;
            tx.execute(
                "UPDATE turns SET status='cancelled',completed_at=?2,error_json=?3
                WHERE id=?1",
                params![turn_id, at_ms, reason],
            )
            .map_err(|error| db_error("failed settling never-dispatched turn", error))?;
            tx.execute(
                "UPDATE approvals SET status='cancelled',resolved_at=?2,
                 response_json=json_set(?3,'$.prior_status',status,'$.prior_response',json(response_json))
                WHERE turn_id=?1 AND status='pending'",
                params![turn_id, at_ms, reason],
            )
            .map_err(|error| db_error("failed cancelling retired pre-dispatch approvals", error))?;
            tx.execute(
                "UPDATE sessions SET active_turn_id=NULL,
                status=CASE WHEN status IN ('closed','failed') THEN status ELSE 'ready' END,
                updated_at=?3 WHERE id=?1 AND active_turn_id=?2",
                params![session_id, turn_id, at_ms],
            )
            .map_err(|error| {
                db_error("failed releasing never-dispatched retirement turn", error)
            })?;
            tx.execute(
                "UPDATE runtime_operation_effects SET phase='finalized',
                evidence_json=json_set(?3,'$.prior_evidence',json(evidence_json)),
                updated_at=?4 WHERE operation_id=?1 AND effect_kind='workspace_turn_interrupt'
                AND target_id=?2 AND phase='intended'",
                params![
                    operation_id,
                    turn_id,
                    json_to_string(&serde_json::json!({
                        "outcome":"never_dispatched_cancelled","native_effect":false,
                        "turn_id":turn_id,"verified_at":at_ms,
                    }))?,
                    at_ms
                ],
            )
            .map_err(|error| {
                db_error(
                    "failed receipting local retirement turn cancellation",
                    error,
                )
            })?;
            tx.commit().map_err(|error| {
                db_error(
                    "failed committing local retirement turn cancellation",
                    error,
                )
            })?;
            return Ok(WorkspaceRetirementTurnStep::Finalized);
        }
        let terminal = completed_at.is_some()
            && matches!(
                status.as_str(),
                "completed" | "failed" | "interrupted" | "cancelled"
            );
        if terminal && active_turn_id.as_deref() != Some(turn_id) {
            tx.execute(
                "UPDATE approvals SET status='cancelled',resolved_at=?2,
                 response_json=json_object('code','workspace_retirement',
                   'reason','terminal_turn_verified','turn_status',?3,
                   'prior_status',status,'prior_response',json(response_json))
                 WHERE turn_id=?1 AND status IN ('pending','decision_pending')",
                params![turn_id, at_ms, status],
            )
            .map_err(|error| {
                db_error(
                    "failed cancelling approvals for retired terminal turn",
                    error,
                )
            })?;
            let updated = tx
                .execute(
                    "UPDATE runtime_operation_effects SET phase='finalized',
                 evidence_json=json_set(?3,'$.prior_evidence',json(evidence_json)),
                 updated_at=?4 WHERE operation_id=?1
                 AND effect_kind='workspace_turn_interrupt' AND target_id=?2
                 AND phase IN ('intended','started','observed')",
                    params![
                        operation_id,
                        turn_id,
                        json_to_string(&serde_json::json!({
                            "outcome":"terminal_turn_observed","turn_status":status,
                            "completed_at":completed_at,"verified_at":at_ms,
                        }))?,
                        at_ms
                    ],
                )
                .map_err(|error| db_error("failed finalizing terminal retirement turn", error))?;
            if updated != 1 {
                return Err(RuntimeError::Conflict(
                    "retirement turn effect changed during finalization".into(),
                ));
            }
            tx.commit()
                .map_err(|error| db_error("failed committing terminal retirement turn", error))?;
            return Ok(WorkspaceRetirementTurnStep::Finalized);
        }
        if phase != "intended" {
            // An unreceipted 'started' interrupt might already have reached the
            // provider; never resend it after a crash or concurrent retry.
            return Ok(if phase == "observed" {
                WorkspaceRetirementTurnStep::Waiting
            } else {
                WorkspaceRetirementTurnStep::NeedsReview
            });
        }
        if active_turn_id.as_deref() != Some(turn_id) {
            return Ok(WorkspaceRetirementTurnStep::NeedsReview);
        }
        let updated = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='started',
             attempt_count=attempt_count+1,
             evidence_json=json_set(?3,'$.prior_evidence',json(evidence_json)),updated_at=?4
             WHERE operation_id=?1 AND effect_kind='workspace_turn_interrupt'
               AND target_id=?2 AND phase='intended'",
                params![
                    operation_id,
                    turn_id,
                    json_to_string(&serde_json::json!({
                        "outcome":"native_interrupt_intended","session_id":session_id,
                        "turn_id":turn_id,"at_ms":at_ms,
                    }))?,
                    at_ms
                ],
            )
            .map_err(|error| db_error("failed claiming retirement turn interrupt", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "retirement turn interrupt was concurrently claimed".into(),
            ));
        }
        tx.commit()
            .map_err(|error| db_error("failed persisting native turn interrupt intent", error))?;
        Ok(WorkspaceRetirementTurnStep::Interrupt {
            session_id,
            turn_id: turn_id.into(),
        })
    }

    pub fn acknowledge_workspace_retirement_interrupt(
        &self,
        operation_id: &str,
        turn_id: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let conn = open_connection(&self.database_path)?;
        let updated = conn
            .execute(
                "UPDATE runtime_operation_effects SET phase='observed',
             evidence_json=json_set(?3,'$.prior_evidence',json(evidence_json)),updated_at=?4
             WHERE operation_id=?1 AND effect_kind='workspace_turn_interrupt'
               AND target_id=?2 AND phase='started'",
                params![
                    operation_id,
                    turn_id,
                    json_to_string(&serde_json::json!({
                        "outcome":"native_interrupt_accepted_not_terminal","at_ms":at_ms,
                    }))?,
                    at_ms
                ],
            )
            .map_err(|error| {
                db_error(
                    "failed acknowledging native retirement turn interrupt",
                    error,
                )
            })?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "retirement turn interrupt wasn't started".into(),
            ));
        }
        Ok(())
    }

    pub fn quarantine_workspace_retirement_interrupt(
        &self,
        operation_id: &str,
        turn_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning retirement interrupt quarantine", error))?;
        let updated = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='manual_review',
             evidence_json=json_set(?3,'$.prior_evidence',json(evidence_json)),
             updated_at=?4 WHERE operation_id=?1 AND target_id=?2
             AND effect_kind='workspace_turn_interrupt' AND phase IN ('intended','started')",
                params![
                    operation_id,
                    turn_id,
                    json_to_string(&serde_json::json!({
                        "outcome":"native_interrupt_unknown","reason":reason,
                    }))?,
                    at_ms
                ],
            )
            .map_err(|error| db_error("failed quarantining ambiguous native interrupt", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "native interrupt wasn't started".into(),
            ));
        }
        tx.execute(
            "UPDATE workspace_retirement_fences SET phase='manual_review',updated_at=?2
             WHERE operation_id=?1 AND phase='frozen'",
            params![operation_id, at_ms],
        )
        .map_err(|error| db_error("failed quarantining workspace retirement fence", error))?;
        tx.execute(
            "UPDATE runtime_operations SET phase='manual_review',updated_at=?2
             WHERE operation_id=?1 AND phase='requested'",
            params![operation_id, at_ms],
        )
        .map_err(|error| db_error("failed quarantining retirement operation", error))?;
        tx.commit()
            .map_err(|error| db_error("failed committing ambiguous native interrupt", error))?;
        Ok(())
    }
}
