use runtime_core::RuntimeError;
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

use crate::db::db_error;
use crate::repository_workspace_retirement_settlement::settle_process_completion;

/// Review never redispatches work. Only already persisted native terminal or
/// canonical admission facts can release an uncertain local obligation.
pub(crate) fn independently_settled(
    tx: &Transaction<'_>,
    workspace_id: &str,
    kind: &str,
    target_id: &str,
    at_ms: i64,
) -> Result<Option<Value>, RuntimeError> {
    match kind {
        "workspace_turn_interrupt" => {
            let row:Option<(String,i64)> = tx.query_row(
                "SELECT t.status,t.completed_at FROM turns t JOIN sessions s ON s.id=t.session_id
                 JOIN workspace_session_ownership o ON o.session_id=s.id
                 JOIN turn_admissions a ON a.turn_id=t.id AND a.session_id=s.id
                 WHERE t.id=?1 AND o.workspace_id=?2 AND t.completed_at IS NOT NULL
                   AND t.status IN ('completed','failed','interrupted','cancelled')
                   AND s.active_turn_id IS NOT t.id AND a.dispatch_state IN ('dispatched','not_dispatched')",
                params![target_id,workspace_id],|row|Ok((row.get(0)?,row.get(1)?)),
            ).optional().map_err(|error|db_error("failed independently checking retired turn",error))?;
            Ok(row.map(
                |(status, completed_at)| json!({"turn_status":status,"completed_at":completed_at}),
            ))
        }
        "workspace_process_settle" => {
            let row:Option<(String,String)> = tx.query_row(
                "SELECT p.status,p.completion_state FROM managed_processes p
                 WHERE p.process_id=?1 AND (p.workspace_id=?2 OR EXISTS(
                    SELECT 1 FROM workspace_session_ownership o WHERE o.workspace_id=?2
                    AND o.session_id=p.owner_session_id))
                   AND p.status IN ('completed','failed','timed_out','killed','canceled','interrupted')
                   AND p.terminal_recorded_at IS NOT NULL",
                params![target_id,workspace_id],|row|Ok((row.get(0)?,row.get(1)?)),
            ).optional().map_err(|error|db_error("failed independently checking retired process",error))?;
            let Some((status, completion)) = row else {
                return Ok(None);
            };
            let outcome = if matches!(completion.as_str(), "pending" | "injecting") {
                let (phase, reason) = settle_process_completion(tx, target_id, at_ms)?;
                if phase != "finalized" {
                    return Ok(None);
                }
                reason
            } else if matches!(completion.as_str(), "delivered" | "not_required") {
                "process_completion_already_terminal"
            } else {
                return Ok(None);
            };
            Ok(Some(
                json!({"process_status":status,"completion_outcome":outcome}),
            ))
        }
        "workspace_delivery_settle" => delivery(tx, workspace_id, target_id, at_ms),
        "workspace_notice_settle" => notice(tx, workspace_id, target_id, at_ms),
        "workspace_hook_settle" => hook(tx, workspace_id, target_id, at_ms),
        _ => Err(RuntimeError::Conflict(
            "this retirement effect has no safe local reconciliation rule".into(),
        )),
    }
}

fn admitted_turn(
    tx: &Transaction<'_>,
    session_id: &str,
    correlation_id: &str,
) -> Result<Option<String>, RuntimeError> {
    // Exactly one dispatch receipt is sufficient; any other ambiguous dispatch
    // under the same recipient/correlation keeps the effect quarantined.
    let rows = tx
        .prepare(
            "SELECT turn_id,dispatch_state FROM turn_admissions
        WHERE session_id=?1 AND json_extract(correlation_json,'$.correlation_id')=?2",
        )
        .map_err(|error| {
            db_error(
                "failed preparing retirement delivery admission proof",
                error,
            )
        })?
        .query_map(params![session_id, correlation_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| db_error("failed reading retirement delivery admission proof", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed decoding retirement delivery admission proof", error))?;
    let dispatched: Vec<_> = rows
        .iter()
        .filter(|(_, state)| state == "dispatched")
        .collect();
    if dispatched.len() != 1
        || rows
            .iter()
            .any(|(_, state)| matches!(state.as_str(), "dispatching" | "unknown"))
    {
        return Ok(None);
    }
    Ok(Some(dispatched[0].0.clone()))
}

fn delivery(
    tx: &Transaction<'_>,
    workspace_id: &str,
    id: &str,
    at_ms: i64,
) -> Result<Option<Value>, RuntimeError> {
    let row: Option<(String, String, String)> = tx
        .query_row(
            "SELECT d.status,d.recipient_agent_id,d.message_id FROM agent_deliveries d
         JOIN agent_messages m ON m.id=d.message_id WHERE d.id=?1 AND (m.workspace_id=?2
           OR EXISTS(SELECT 1 FROM workspace_session_ownership o WHERE o.workspace_id=?2
             AND o.session_id IN (d.recipient_agent_id,m.sender_agent_id)))",
            params![id, workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| db_error("failed checking retired delivery terminal facts", error))?;
    let Some((status, recipient, message_id)) = row else {
        return Ok(None);
    };
    if matches!(status.as_str(), "injected" | "failed" | "cancelled") {
        return Ok(Some(json!({"delivery_status":status})));
    }
    let Some(turn_id) = admitted_turn(tx, &recipient, &message_id)? else {
        if !closed_without_dispatch(tx, workspace_id, &recipient, &message_id)? {
            return Ok(None);
        }
        tx.execute(
            "UPDATE agent_deliveries SET status='cancelled',last_error_code='workspace_retirement',
            updated_at=?2 WHERE id=?1",
            params![id, at_ms],
        )
        .map_err(|error| {
            db_error(
                "failed cancelling proven undispatched retirement delivery",
                error,
            )
        })?;
        tx.execute(
            "UPDATE team_deliveries SET status='cancelled',last_error_code='workspace_retirement',
            updated_at=?2 WHERE id=?1 AND status NOT IN ('injected','failed','cancelled')",
            params![id, at_ms],
        )
        .map_err(|error| {
            db_error(
                "failed cancelling undispatched retirement delivery mirror",
                error,
            )
        })?;
        return Ok(Some(
            json!({"delivery_status":"cancelled","proof":"canonical_no_dispatch_after_verified_close"}),
        ));
    };
    tx.execute(
        "UPDATE agent_deliveries SET status='injected',injected_turn_id=?2,
        last_error_code=NULL,last_error_message=NULL,updated_at=?3 WHERE id=?1",
        params![id, turn_id, at_ms],
    )
    .map_err(|error| {
        db_error(
            "failed recording independently proven message dispatch",
            error,
        )
    })?;
    tx.execute(
        "UPDATE team_deliveries SET status='injected',injected_turn_id=?2,
        last_error_code=NULL,last_error_message=NULL,updated_at=?3 WHERE id=?1
        AND status NOT IN ('injected','failed','cancelled')",
        params![id, turn_id, at_ms],
    )
    .map_err(|error| db_error("failed reconciling legacy delivery mirror", error))?;
    Ok(Some(
        json!({"delivery_status":"injected","canonical_turn_id":turn_id}),
    ))
}

fn notice(
    tx: &Transaction<'_>,
    workspace_id: &str,
    id: &str,
    at_ms: i64,
) -> Result<Option<Value>, RuntimeError> {
    let row: Option<(String, String, String)> = tx
        .query_row(
            "SELECT state,recipient_agent_id,correlation_id FROM workspace_system_notices
         WHERE notice_id=?1 AND workspace_id=?2",
            params![id, workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| db_error("failed checking retired notice terminal facts", error))?;
    let Some((state, recipient, correlation)) = row else {
        return Ok(None);
    };
    if matches!(state.as_str(), "delivered" | "cancelled") {
        return Ok(Some(json!({"notice_state":state})));
    }
    let Some(turn_id) = admitted_turn(tx, &recipient, &correlation)? else {
        if !closed_without_dispatch(tx, workspace_id, &recipient, &correlation)? {
            return Ok(None);
        }
        tx.execute("UPDATE workspace_system_notices SET state='cancelled',last_error='workspace_retirement',
            updated_at_ms=?2 WHERE notice_id=?1",params![id,at_ms])
            .map_err(|error|db_error("failed cancelling proven undispatched retirement notice",error))?;
        return Ok(Some(
            json!({"notice_state":"cancelled","proof":"canonical_no_dispatch_after_verified_close"}),
        ));
    };
    tx.execute(
        "UPDATE workspace_system_notices SET state='delivered',turn_id=?2,
        last_error=NULL,updated_at_ms=?3 WHERE notice_id=?1",
        params![id, turn_id, at_ms],
    )
    .map_err(|error| {
        db_error(
            "failed recording independently proven notice dispatch",
            error,
        )
    })?;
    Ok(Some(
        json!({"notice_state":"delivered","canonical_turn_id":turn_id}),
    ))
}

fn closed_without_dispatch(
    tx: &Transaction<'_>,
    workspace_id: &str,
    recipient: &str,
    correlation: &str,
) -> Result<bool, RuntimeError> {
    tx.query_row("SELECT EXISTS(SELECT 1 FROM sessions s
        JOIN workspace_session_ownership o ON o.session_id=s.id
        JOIN workspace_retirement_fences f ON f.workspace_id=o.workspace_id
        JOIN runtime_operation_effects e ON e.operation_id=f.operation_id AND e.target_id=s.id
        LEFT JOIN workspace_agents a ON a.session_id=s.id
        WHERE o.workspace_id=?1 AND s.id=?2 AND s.status='closed' AND s.active_turn_id IS NULL
          AND f.phase IN ('frozen','manual_review') AND (a.session_id IS NULL OR a.lifecycle_state='archived')
          AND e.effect_kind='workspace_provider_close' AND e.phase='finalized'
          AND json_extract(e.evidence_json,'$.verified')=1
          AND json_extract(e.evidence_json,'$.provider')=s.provider
          AND json_extract(e.evidence_json,'$.provider_session_ref')=s.provider_session_ref
          AND json_extract(e.evidence_json,'$.canonical_provider_session_ref') IS s.canonical_provider_session_ref
          AND json_extract(e.evidence_json,'$.agent_revision') IS a.revision
          AND json_extract(e.evidence_json,'$.closed_at')=s.closed_at
          AND json_extract(e.evidence_json,'$.session_updated_at')=s.updated_at)
        AND NOT EXISTS(SELECT 1 FROM turn_admissions WHERE session_id=?2
          AND json_extract(correlation_json,'$.correlation_id')=?3
          AND (dispatch_state NOT IN ('pending','not_dispatched') OR provider_native_turn_id IS NOT NULL))",
        params![workspace_id,recipient,correlation],|row|row.get(0))
        .map_err(|error|db_error("failed proving undispatched input behind verified native closure",error))
}

fn hook(
    tx: &Transaction<'_>,
    workspace_id: &str,
    id: &str,
    at_ms: i64,
) -> Result<Option<Value>, RuntimeError> {
    let keys: Vec<String> = serde_json::from_str(id).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid retirement hook identity: {error}"))
    })?;
    if keys.len() != 3 {
        return Err(RuntimeError::Bootstrap(
            "invalid retirement hook identity width".into(),
        ));
    }
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT phase,application_state FROM workspace_hook_effects WHERE workspace_id=?1
         AND parent_event_id=?2 AND event_name=?3 AND hook_name=?4",
            params![workspace_id, keys[0], keys[1], keys[2]],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| db_error("failed checking retired hook terminal facts", error))?;
    let Some((phase, application)) = row else {
        return Ok(None);
    };
    if !matches!(phase.as_str(), "succeeded" | "failed" | "timed_out") {
        return Ok(None);
    }
    if application == "pending" || application == "not_ready" {
        tx.execute(
            "UPDATE workspace_hook_effects SET application_state='rejected',
            application_error_code='workspace_retirement',updated_at_ms=?5 WHERE workspace_id=?1
            AND parent_event_id=?2 AND event_name=?3 AND hook_name=?4",
            params![workspace_id, keys[0], keys[1], keys[2], at_ms],
        )
        .map_err(|error| {
            db_error(
                "failed rejecting independently completed retirement hook output",
                error,
            )
        })?;
    }
    Ok(Some(
        json!({"hook_phase":phase,"prior_application_state":application}),
    ))
}
