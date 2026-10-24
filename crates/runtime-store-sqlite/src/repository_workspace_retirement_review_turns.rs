use runtime_core::{
    RuntimeError, WorkspaceRetirementCloseAttempt, WorkspaceRetirementNativeIdentity,
};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, string_to_json};

/// Explicit operator verification of the exact native attachment's closure
/// proves its snapshotted turns cannot still execute. Preserve unknown dispatch
/// history; only their logical liveness and approvals become terminal.
pub(crate) fn settle_closed_attachment_turns(
    tx: &Transaction<'_>,
    attempt: &WorkspaceRetirementCloseAttempt,
    identity: &WorkspaceRetirementNativeIdentity,
    close_evidence: &Value,
    at_ms: i64,
) -> Result<Vec<String>, RuntimeError> {
    let review = close_evidence.get("operator_review").ok_or_else(|| {
        RuntimeError::Conflict("operator close proof has no immutable review assertion".into())
    })?;
    if let Some(active) = &identity.active_turn_id {
        let exact: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM turns t
            JOIN runtime_operation_effects e ON e.target_id=t.id AND e.operation_id=?1
            WHERE e.effect_kind='workspace_turn_interrupt' AND e.phase<>'finalized'
              AND t.id=?2 AND t.session_id=?3)",
                params![attempt.operation_id, active, attempt.agent_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed binding operator close to exact snapshotted active turn",
                    error,
                )
            })?;
        if !exact {
            return Err(RuntimeError::Conflict(
                "asserted active turn is not an unresolved target of this retirement".into(),
            ));
        }
    }
    let mut stmt = tx
        .prepare(
            "SELECT e.effect_id,e.target_id,e.phase,e.evidence_json
        FROM runtime_operation_effects e JOIN turns t ON t.id=e.target_id
        WHERE e.operation_id=?1 AND e.effect_kind='workspace_turn_interrupt'
          AND t.session_id=?2 AND e.phase<>'finalized' ORDER BY e.effect_id",
        )
        .map_err(|error| {
            db_error(
                "failed preparing exact closed-attachment turn effects",
                error,
            )
        })?;
    let rows = stmt
        .query_map(params![attempt.operation_id, attempt.agent_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|error| db_error("failed reading closed-attachment turn effects", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed decoding closed-attachment turn effects", error))?;
    drop(stmt);
    let close_effect_id = format!(
        "{}:workspace_provider_close:{}",
        attempt.operation_id, attempt.agent_id
    );
    let mut settled = Vec::new();
    for (effect_id, turn_id, phase, prior) in rows {
        if !matches!(
            phase.as_str(),
            "intended" | "started" | "observed" | "manual_review"
        ) {
            return Err(RuntimeError::Conflict(
                "closed-attachment turn effect has an unsupported phase".into(),
            ));
        }
        let dispatch: Option<String> = tx
            .query_row(
                "SELECT dispatch_state FROM turn_admissions
            WHERE turn_id=?1 AND session_id=?2",
                params![turn_id, attempt.agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed preserving unknown native turn admission", error))?;
        let proof = json!({"code":"operator_verified_native_attachment_closed",
            "operation_id":attempt.operation_id,"provider_close_effect_id":close_effect_id,
            "native_identity":identity,"operator_review":review,"at_ms":at_ms});
        tx.execute(
            "UPDATE turns SET status='interrupted',completed_at=?3,
            error_json=json_object('code','operator_verified_native_attachment_closed',
                'native_result','unknown','proof',json(?4),'prior_status',status,'prior_error',json(error_json))
            WHERE id=?1 AND session_id=?2 AND (completed_at IS NULL
                OR status NOT IN ('completed','failed','interrupted','cancelled'))",
            params![turn_id, attempt.agent_id, at_ms, json_to_string(&proof)?],
        )
        .map_err(|error| {
            db_error(
                "failed settling logical turn after operator-verified native closure",
                error,
            )
        })?;
        tx.execute(
            "UPDATE turn_admissions SET dispatch_error_json=json_object(
            'native_attachment_closed',json(?3),'prior_dispatch_error',json(dispatch_error_json)),
            updated_at=?4 WHERE turn_id=?1 AND session_id=?2",
            params![turn_id, attempt.agent_id, json_to_string(&proof)?, at_ms],
        )
        .map_err(|error| {
            db_error(
                "failed preserving native dispatch uncertainty with closure classification",
                error,
            )
        })?;
        tx.execute(
            "UPDATE approvals SET status='cancelled',resolved_at=?3,
            response_json=json_object('code','operator_verified_native_attachment_closed',
                'proof',json(?4),'prior_status',status,'prior_response',json(response_json))
            WHERE turn_id=?1 AND session_id=?2 AND status IN ('pending','decision_pending')",
            params![turn_id, attempt.agent_id, at_ms, json_to_string(&proof)?],
        )
        .map_err(|error| {
            db_error(
                "failed cancelling stale native approvals after operator-verified close",
                error,
            )
        })?;
        let prior = prior
            .map(string_to_json)
            .transpose()
            .map_err(|error| db_error("failed decoding unknown native turn evidence", error))?;
        let changed = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='finalized',
            evidence_json=?3,updated_at=?4 WHERE operation_id=?1 AND effect_id=?2 AND phase=?5",
                params![
                    attempt.operation_id,
                    effect_id,
                    json_to_string(&json!({
                        "outcome":"operator_verified_native_attachment_closed",
                        "provider_close_effect_id":close_effect_id,"native_identity":identity,
                        "historical_dispatch_state":dispatch,"operator_review":review,
                        "prior_evidence":prior,"verified_at":at_ms,
                    }))?,
                    at_ms,
                    phase
                ],
            )
            .map_err(|error| {
                db_error(
                    "failed receipting exact turn settlement from native closure",
                    error,
                )
            })?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "operator-closed turn effect changed during receipt".into(),
            ));
        }
        settled.push(turn_id);
    }
    if let Some(active) = &identity.active_turn_id {
        if tx
            .execute(
                "UPDATE sessions SET active_turn_id=NULL WHERE id=?1 AND active_turn_id=?2",
                params![attempt.agent_id, active],
            )
            .map_err(|error| {
                db_error("failed releasing exact operator-closed logical turn", error)
            })?
            != 1
        {
            return Err(RuntimeError::Conflict(
                "operator-closed active turn identity changed".into(),
            ));
        }
    }
    Ok(settled)
}
