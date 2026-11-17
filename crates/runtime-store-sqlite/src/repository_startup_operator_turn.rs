use crate::db::db_error;
use crate::repository_operator_user_turn_receipt::{
    complete_operator_user_turn_tx, fail_operator_user_turn_not_dispatched_tx,
};
use crate::repository_turn_authority::{
    OPERATOR_TURN_CLAIM, OPERATOR_TURN_EFFECT, OPERATOR_TURN_KIND,
};
use crate::repository_workspace::operation_by_id;
use runtime_core::{OperationPhase, RuntimeError, SendTurnAccepted};
use rusqlite::{params, OptionalExtension, Transaction};
pub(crate) fn startup_operator_turn_tx(
    tx: &Transaction<'_>,
    session: &runtime_core::SessionRecord,
    at_ms: i64,
    settle_known: bool,
) -> Result<Option<runtime_core::StartupPendingOperatorTurn>, RuntimeError> {
    use crate::repository_startup_reattachment::{claim_for, conflict, sole_effect, value};
    use crate::repository_turn_dispatch_outcome::read_approval;
    use crate::repository_turn_start::read_admission;
    use crate::repository_turn_terminal::read_turn;
    use runtime_core::{
        OperationActorKind, StartupPendingOperatorTurn, TurnDispatchState,
        TurnInputProjectionSource,
    };
    let claim = claim_for(tx, OPERATOR_TURN_CLAIM, &session.id)?;
    let unfinished:Option<String>=tx.query_row("SELECT op.operation_id FROM runtime_operations op WHERE op.kind=?1 AND op.phase IN ('requested','manual_review') AND (json_extract(op.normalized_request_json,'$.agent_id')=?2 OR EXISTS(SELECT 1 FROM runtime_operation_effects e JOIN turns t ON t.id=e.target_id WHERE e.operation_id=op.operation_id AND e.target_kind='logical_turn' AND t.session_id=?2)) AND (?3 IS NULL OR op.operation_id<>?3) ORDER BY op.operation_id LIMIT 1",params![OPERATOR_TURN_KIND,session.id,claim.as_ref().map(|c|c.owner_operation_id.as_str())],|r|r.get(0)).optional().map_err(|e|db_error("failed checking orphan operator turn authority",e))?;
    if let Some(operation_id) = unfinished {
        return Err(RuntimeError::operation_unresolved(
            operation_id,
            "an unfinished operator turn lacks this exact exclusive owner binding",
        ));
    }
    let Some(claim) = claim else {
        return Ok(None);
    };
    let operation = operation_by_id(tx, &claim.owner_operation_id)?
        .ok_or_else(|| conflict("operator turn claim has no operation"))?;
    let unresolved = || {
        RuntimeError::operation_unresolved(
            operation.operation_id.clone(),
            "original operator turn outcome or exact parent receipt remains unresolved",
        )
    };
    if operation.kind != OPERATOR_TURN_KIND
        || operation.phase != OperationPhase::Requested
        || operation.actor.kind != OperationActorKind::Operator
        || operation.actor.identifier.trim().is_empty()
        || operation.normalized_request_hash
            != runtime_core::normalized_json_hash(&operation.normalized_request)?
        || operation.normalized_request["agent_id"] != session.id
    {
        return Err(unresolved());
    }
    let effect = sole_effect(tx, &operation.operation_id)?;
    if effect.kind != OPERATOR_TURN_EFFECT
        || effect.target_kind != "logical_turn"
        || effect.phase != "intended"
        || effect.key != "provider_turn_send"
        || effect.attempts != 0
        || effect.evidence != serde_json::json!({"agent_id":session.id,"turn_id":effect.target_id})
    {
        return Err(unresolved());
    }
    let turn = read_turn(tx, &effect.target_id)?;
    let admission = read_admission(tx, &turn.id)?;
    let workspace_id:Option<String>=tx.query_row("SELECT a.workspace_id FROM workspace_agents a JOIN workspace_session_ownership o ON o.session_id=a.session_id AND o.workspace_id=a.workspace_id WHERE a.session_id=?1",[&session.id],|r|r.get(0)).optional().map_err(|e|db_error("failed checking original operator turn owner",e))?;
    if workspace_id.is_none()
        || operation.workspace_id != workspace_id
        || turn.session_id != session.id
        || admission.session_id != session.id
        || admission.provider != session.provider
        || admission.projection_source != TurnInputProjectionSource::UserVisible
        || admission.correlation.correlation_id.is_some()
        || operation.normalized_request["input"] != turn.input
        || operation.normalized_request["expected_turn_id"]
            != value(&admission.correlation.expected_turn_id)?
        || operation.normalized_request["turn_settings"]
            != value(&admission.dispatch_policy.turn_settings)?
        || !matches!(
            &operation.normalized_request["permission_mode"],
            serde_json::Value::Null | serde_json::Value::String(_)
        )
    {
        return Err(unresolved());
    }
    let at_ms = at_ms.max(operation.updated_at);
    match admission.dispatch_state {
        TurnDispatchState::Dispatched => {
            if !settle_known {
                return Err(unresolved());
            }
            complete_operator_user_turn_tx(
                tx,
                &operation.operation_id,
                &SendTurnAccepted {
                    session_id: session.id.clone(),
                    turn_id: turn.id,
                    status: "in_progress".into(),
                },
                at_ms,
            )?;
            return Ok(None);
        }
        TurnDispatchState::NotDispatched => {
            if !settle_known {
                return Err(unresolved());
            }
            let error = admission.dispatch_error.as_ref().ok_or_else(unresolved)?;
            let code = error["code"].as_str().ok_or_else(unresolved)?;
            let stored_message = error["message"].as_str().ok_or_else(unresolved)?;
            let message = stored_message
                .strip_prefix("provider dispatch not_dispatched: ")
                .unwrap_or(stored_message);
            fail_operator_user_turn_not_dispatched_tx(
                tx,
                &operation.operation_id,
                code,
                message,
                at_ms,
            )?;
            return Ok(None);
        }
        TurnDispatchState::Dispatching | TurnDispatchState::Unknown => return Err(unresolved()),
        TurnDispatchState::Pending => {}
    }
    if session.active_turn_id.as_deref() != Some(turn.id.as_str())
        || turn.completed_at.is_some()
        || turn.provider_turn_ref.is_some()
        || !matches!(turn.status.as_str(), "admitted" | "waiting_for_approval")
        || admission.provider_native_turn_id.is_some()
        || admission.dispatch_error.is_some()
    {
        return Err(unresolved());
    }
    let input = turn.input.as_array().ok_or_else(unresolved)?;
    if admission.user_input_snapshot != runtime_core::PersistedUserInputSnapshot::from_input(input)
        || admission.dispatch_policy.pre_dispatch_approval_required
            != (admission.dispatch_policy.permission_mode.as_deref() == Some("require_approval"))
    {
        return Err(unresolved());
    }
    match operation.normalized_request.get("permission_mode") {
        Some(serde_json::Value::String(requested))
            if admission.dispatch_policy.permission_mode.as_deref() == Some(requested.as_str()) => {
        }
        Some(serde_json::Value::Null) => {}
        _ => return Err(unresolved()),
    }
    crate::repository_turn_authority::verified_turn_admitted_receipt(tx, &admission)?
        .ok_or_else(unresolved)?;
    let mut statement=tx.prepare("SELECT id FROM approvals WHERE session_id=?1 AND turn_id=?2 AND origin='runtime_pre_dispatch_policy' ORDER BY id LIMIT 2").map_err(|e|db_error("failed preparing startup policy approval",e))?;
    let approval_ids = statement
        .query_map(params![session.id, turn.id], |r| r.get::<_, String>(0))
        .map_err(|e| db_error("failed reading startup policy approval", e))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| db_error("failed decoding startup policy approval", e))?;
    drop(statement);
    if admission.dispatch_policy.pre_dispatch_approval_required {
        if approval_ids.len() != 1 {
            return Err(unresolved());
        }
        let approval = read_approval(tx, &approval_ids[0])?;
        if approval.provider_approval_ref.is_some() || approval.tool_call_id.is_some() {
            return Err(unresolved());
        }
        match approval.status.as_str() {
            "pending" => {
                if !settle_known
                    || approval.response.is_some()
                    || approval.resolved_at.is_some()
                    || turn.status != "waiting_for_approval"
                    || session.status != "waiting_for_approval"
                {
                    return Err(unresolved());
                }
                complete_operator_user_turn_tx(
                    tx,
                    &operation.operation_id,
                    &SendTurnAccepted {
                        session_id: session.id.clone(),
                        turn_id: turn.id,
                        status: "waiting_for_approval".into(),
                    },
                    at_ms,
                )?;
                return Ok(None);
            }
            "accept" if approval.resolved_at.is_some() => {}
            _ => return Err(unresolved()),
        }
    } else if !approval_ids.is_empty() {
        return Err(unresolved());
    }
    Ok(Some(StartupPendingOperatorTurn {
        operation_id: operation.operation_id,
        normalized_request_hash: operation.normalized_request_hash,
        effect_id: effect.id,
        claim,
        admission,
    }))
}
