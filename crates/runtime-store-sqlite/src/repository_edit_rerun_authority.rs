//! Original edit ownership is checked at replacement admission even after
//! recovery or review makes the edit terminal. A correlation is not a grant.
use crate::db::db_error;
use crate::repository_turn_authority::{turn_admission_from_row, verified_turn_admitted_receipt};
use crate::repository_turn_terminal::{read_session, read_turn};
use runtime_core::{
    ProviderKind, RuntimeError, SessionEditRerunOperation, SessionRecord, TurnAdmissionRecord,
    TurnInputProjectionSource, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

fn conflict(message: &str) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}

fn unresolved(
    agent_id: &str,
    operation_id: &str,
    code: &str,
    message: impl std::fmt::Display,
    replacement_turn_id: Option<String>,
) -> RuntimeError {
    RuntimeError::SessionControlUnresolved {
        coordinate: runtime_core::SessionControlCoordinate::EditRerun {
            agent_id: agent_id.into(),
            operation_id: operation_id.into(),
        },
        code: code.into(),
        message: message.to_string(),
        legacy_recovery: None,
        replacement_turn_id,
        dispatch_outcome: None,
    }
}

// Reproduce the input already checked by the runtime without touching the
// filesystem inside the SQLite transaction. Display metadata stays in the
// immutable snapshot and does not become a native request field.
fn native_input(operation: &SessionEditRerunOperation) -> Result<Value, RuntimeError> {
    let snapshot = &operation.edited_snapshot;
    let mut input = Vec::new();
    if !snapshot.prompt_text.is_empty() {
        input.push(json!({"type":"text","text":snapshot.prompt_text}));
    }
    for image in &snapshot.image_refs {
        let path = image
            .persisted_absolute_path
            .as_deref()
            .ok_or_else(|| conflict("edited image lacks its original native path"))?;
        input.push(json!({"type":"local_image","path":path,"media_type":image.media_type}));
    }
    if let Some(invocation) = &snapshot.invocation {
        if operation.provider != ProviderKind::Codex || invocation.provider != "codex" {
            return Err(conflict("edited invocation has a different provider"));
        }
        let path = invocation
            .path
            .as_deref()
            .ok_or_else(|| conflict("edited invocation lacks its original native path"))?;
        input.push(json!({"type":"skill","name":invocation.name,"path":path}));
    }
    if input.is_empty() {
        return Err(conflict("edit replacement has no original native input"));
    }
    Ok(Value::Array(input))
}

fn exact_replacement(
    operation: &SessionEditRerunOperation,
    admission: &TurnAdmissionRecord,
    turn: &TurnRecord,
) -> Result<bool, RuntimeError> {
    Ok(admission.session_id == operation.agent_id
        && admission.provider == operation.provider.as_str()
        && admission.projection_source == TurnInputProjectionSource::UserVisible
        && admission.user_input_snapshot == operation.edited_snapshot
        && admission.correlation.correlation_id.as_deref() == Some(operation.operation_id.as_str())
        && admission.turn_id == turn.id
        && turn.session_id == operation.agent_id
        && turn.source.as_deref() == Some("user")
        && turn.input == native_input(operation)?)
}

pub(crate) fn guard_edit_replacement_admission(
    tx: &Transaction<'_>,
    admission: &TurnAdmissionRecord,
    turn: &TurnRecord,
    submitted: &SessionRecord,
) -> Result<(), RuntimeError> {
    let correlated = if let Some(id) = admission.correlation.correlation_id.as_deref() {
        tx.query_row(
            "SELECT session_id,phase,record_json FROM session_edit_reruns WHERE operation_id=?1",
            [id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|error| db_error("failed reading original edit correlation", error))?
    } else {
        None
    };
    let operation = correlated
        .map(|(agent_id, phase, raw)| {
            let operation: SessionEditRerunOperation =
                serde_json::from_str(&raw).map_err(|error| {
                    RuntimeError::Bootstrap(format!("invalid original edit authority: {error}"))
                })?;
            operation.validate()?;
            if operation.agent_id != agent_id
                || operation.phase != phase
                || admission.correlation.correlation_id.as_deref()
                    != Some(operation.operation_id.as_str())
            {
                return Err(conflict("edit authority and indexed owner disagree"));
            }
            Ok(operation)
        })
        .transpose()?;
    let pending: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM session_edit_reruns WHERE session_id=?1
         AND phase IN ('intended','native_started','fork_observed','manual_review')",
            [&submitted.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed checking edit replacement owner", error))?;
    if let Some(pending) = pending {
        if operation.as_ref().map(|op| op.operation_id.as_str()) != Some(pending.as_str()) {
            return Err(unresolved(
                &submitted.id,
                &pending,
                "edit_operation_unresolved",
                "another edit owns this session; inspect its original operation",
                None,
            ));
        }
    }
    let Some(operation) = operation else {
        return Ok(());
    };
    let refused = |message: &str| {
        unresolved(
            &operation.agent_id,
            &operation.operation_id,
            "edit_replacement_not_authorized",
            message,
            operation.replacement_turn_id.clone(),
        )
    };
    if operation.phase != "fork_observed"
        || operation.replacement_turn_id.is_some()
        || !exact_replacement(&operation, admission, turn)
            .map_err(|error| refused(&error.to_string()))?
        || submitted.id != operation.agent_id
        || submitted.provider != operation.provider.as_str()
    {
        return Err(refused(
            "original edit no longer authorizes this exact replacement admission",
        ));
    }
    let child = operation
        .child_provider_session_ref
        .as_deref()
        .filter(|child| !child.is_empty() && *child != operation.source_provider_session_ref)
        .ok_or_else(|| refused("edit replacement lacks an acknowledged child identity"))?;
    let persisted = read_session(tx, &operation.agent_id)?;
    if persisted.provider != operation.provider.as_str()
        || persisted.provider_session_ref.as_deref() != Some(child)
        || persisted.canonical_provider_session_ref
            != operation.child_canonical_provider_session_ref
        || submitted.provider_session_ref != persisted.provider_session_ref
        || submitted.canonical_provider_session_ref != persisted.canonical_provider_session_ref
        || persisted.status != "ready"
        || persisted.active_turn_id.is_some()
    {
        return Err(refused(
            "original edit child is no longer the current idle attachment",
        ));
    }
    let already_admitted: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM turn_admissions WHERE session_id=?1
         AND json_extract(correlation_json,'$.correlation_id')=?2)",
            params![operation.agent_id, operation.operation_id],
            |row| row.get(0),
        )
        .map_err(|error| {
            db_error(
                "failed checking original edit replacement uniqueness",
                error,
            )
        })?;
    if already_admitted {
        return Err(refused(
            "the original edit already owns an admitted replacement turn",
        ));
    }
    Ok(())
}

/// Positive admission evidence only. None means no unique verified proof;
/// it never proves native absence and must not authorize review settlement.
/// Known dispatch/terminal outcomes are intentionally not reclassified here.
pub(crate) fn verified_edit_replacement(
    tx: &Transaction<'_>,
    operation: &SessionEditRerunOperation,
) -> Result<Option<TurnAdmissionRecord>, RuntimeError> {
    if operation
        .child_provider_session_ref
        .as_deref()
        .is_none_or(|child| child.is_empty() || child == operation.source_provider_session_ref)
    {
        return Ok(None);
    }
    let records = {
        let mut statement = tx
            .prepare(
                "SELECT turn_id,session_id,provider,projection_source,user_input_snapshot_json,
                    dispatch_policy_json,correlation_json,dispatch_state,
                    provider_native_turn_id,dispatch_error_json,admitted_at,updated_at
             FROM turn_admissions WHERE session_id=?1
               AND json_extract(correlation_json,'$.correlation_id')=?2
             ORDER BY turn_id LIMIT 2",
            )
            .map_err(|error| db_error("failed preparing exact edit replacement evidence", error))?;
        let records = statement
            .query_map(
                params![operation.agent_id, operation.operation_id],
                turn_admission_from_row,
            )
            .map_err(|error| db_error("failed reading exact edit replacement evidence", error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| db_error("failed decoding exact edit replacement evidence", error))?;
        records
    };
    if records.len() != 1 {
        return Ok(None);
    }
    let admission = records.into_iter().next().expect("one exact candidate");
    let turn = match read_turn(tx, &admission.turn_id) {
        Ok(turn) => turn,
        Err(RuntimeError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    if !exact_replacement(operation, &admission, &turn)?
        || verified_turn_admitted_receipt(tx, &admission)?.is_none()
    {
        return Ok(None);
    }
    Ok(Some(admission))
}
