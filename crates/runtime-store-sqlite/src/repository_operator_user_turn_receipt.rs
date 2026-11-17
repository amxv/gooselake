use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_user_turn::{
    append_terminal_transition, operation_turn_id, release_operator_turn_claim,
    requested_operator_turn,
};
use crate::repository_turn_authority::OPERATOR_TURN_EFFECT;
use crate::SqliteRuntimeRepository;
use runtime_core::{OperationPhase, RuntimeError, SendTurnAccepted};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

impl SqliteRuntimeRepository {
    pub fn complete_operator_user_turn(
        &self,
        operation_id: &str,
        accepted: &SendTurnAccepted,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting operator accepted receipt", error))?;
        complete_operator_user_turn_tx(&tx, operation_id, accepted, at_ms)?;
        tx.commit()
            .map_err(|error| db_error("failed committing exact operator turn receipt", error))
    }
    pub fn fail_operator_user_turn_not_dispatched(
        &self,
        operation_id: &str,
        code: &str,
        message: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting operator non-dispatch receipt", error))?;
        fail_operator_user_turn_not_dispatched_tx(&tx, operation_id, code, message, at_ms)?;
        tx.commit()
            .map_err(|error| db_error("failed committing exact non-dispatch receipt", error))
    }
}
pub(crate) fn complete_operator_user_turn_tx(
    transaction: &Transaction<'_>,
    operation_id: &str,
    accepted: &SendTurnAccepted,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    if !matches!(
        accepted.status.as_str(),
        "in_progress" | "waiting_for_approval"
    ) {
        return Err(RuntimeError::InvalidState(
            "operator turn receipt must describe an accepted logical turn".into(),
        ));
    }
    let operation = requested_operator_turn(transaction, operation_id)?;
    let turn_id = operation_turn_id(transaction, operation_id)?;
    if accepted.turn_id != turn_id
        || operation.normalized_request["agent_id"] != accepted.session_id
    {
        return Err(RuntimeError::Conflict(
            "operator turn receipt does not match admitted logical turn".into(),
        ));
    }
    let persisted:Option<(String,String)>=transaction.query_row("SELECT t.session_id,a.dispatch_state FROM turns t JOIN turn_admissions a ON a.turn_id=t.id WHERE t.id=?1",[&turn_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(|e|db_error("failed verifying accepted logical turn",e))?;
    let Some((session_id, dispatch_state)) = persisted else {
        return Err(RuntimeError::Conflict(
            "operator turn receipt lacks durable turn admission".into(),
        ));
    };
    if session_id != accepted.session_id
        || (accepted.status == "in_progress" && dispatch_state != "dispatched")
        || (accepted.status == "waiting_for_approval" && dispatch_state == "not_dispatched")
    {
        return Err(RuntimeError::Conflict(
            "operator turn receipt has no matching accepted dispatch evidence".into(),
        ));
    }
    let exact = serde_json::to_value(accepted)
        .map_err(|e| RuntimeError::Bootstrap(format!("failed serializing accepted turn: {e}")))?;
    let changed=transaction.execute("UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?2,attempt_count=1,updated_at=?3 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",params![operation_id,json_to_string(&exact)?,at_ms,OPERATOR_TURN_EFFECT]).map_err(|e|db_error("failed recording accepted operator turn",e))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "operator native turn intent has already been settled".into(),
        ));
    }
    let authority = OperationAuthorityTransaction::new(transaction);
    authority.terminalize(
        operation_id,
        OperationPhase::Requested,
        OperationPhase::Completed,
        operation.workspace_id.as_deref(),
        &exact,
        None,
        None,
        at_ms,
    )?;
    append_terminal_transition(&authority, operation_id, OperationPhase::Completed, at_ms)?;
    release_operator_turn_claim(transaction, operation_id, &session_id)
}
pub(crate) fn fail_operator_user_turn_not_dispatched_tx(
    transaction: &Transaction<'_>,
    operation_id: &str,
    code: &str,
    message: &str,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    let operation = requested_operator_turn(transaction, operation_id)?;
    let turn_id = operation_turn_id(transaction, operation_id)?;
    let state:Option<(String,String)>=transaction.query_row("SELECT t.session_id,a.dispatch_state FROM turns t JOIN turn_admissions a ON a.turn_id=t.id WHERE t.id=?1",[&turn_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(|e|db_error("failed checking rejected provider dispatch",e))?;
    let Some((session_id, state)) = state else {
        return Err(RuntimeError::Conflict(
            "operator non-dispatch lacks logical turn evidence".into(),
        ));
    };
    if state != "not_dispatched" || operation.normalized_request["agent_id"] != session_id {
        return Err(RuntimeError::Conflict(
            "provider native non-dispatch was not durably proven".into(),
        ));
    }
    let exact = serde_json::json!({"code":code,"message":message,"dispatch_outcome":"not_dispatched","logical_turn_id":turn_id});
    let changed=transaction.execute("UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?2,attempt_count=1,updated_at=?3 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",params![operation_id,json_to_string(&exact)?,at_ms,OPERATOR_TURN_EFFECT]).map_err(|e|db_error("failed recording provider non-dispatch",e))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "operator turn non-dispatch receipt was already settled".into(),
        ));
    }
    let authority = OperationAuthorityTransaction::new(transaction);
    authority.terminalize(
        operation_id,
        OperationPhase::Requested,
        OperationPhase::Failed,
        operation.workspace_id.as_deref(),
        &exact,
        Some("not_dispatched"),
        Some(message),
        at_ms,
    )?;
    append_terminal_transition(&authority, operation_id, OperationPhase::Failed, at_ms)?;
    release_operator_turn_claim(transaction, operation_id, &session_id)
}
