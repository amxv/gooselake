//! An observation on an existing native/target effect, never a new authority.
use crate::db::{db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_workspace::operation_by_id;
use crate::SqliteRuntimeRepository;
use runtime_core::legacy_spawn_obligations::LegacyNativeRecovery;
use runtime_core::{
    NewRuntimeEvent, OperationPhase, OperationRecord, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

fn event_id(operation_id: &str, effect_id: &str) -> Result<String, RuntimeError> {
    Ok(format!(
        "evt_legacy_guard_{}",
        runtime_core::normalized_json_hash(&json!([operation_id, effect_id]))?
    ))
}

fn expected_event(
    operation: &OperationRecord,
    effect_id: &str,
    recovery: &LegacyNativeRecovery,
    at_ms: i64,
) -> Result<NewRuntimeEvent, RuntimeError> {
    let (scope, scope_id) = operation
        .workspace_id
        .as_ref()
        .map(|id| (RuntimeEventScope::Workspace, id.clone()))
        .unwrap_or((RuntimeEventScope::Operation, operation.operation_id.clone()));
    Ok(NewRuntimeEvent {
        event_id: event_id(&operation.operation_id, effect_id)?,
        scope,
        scope_id,
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: "operation.legacy_native_recovery_observed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({"operation_id":operation.operation_id,"effect_id":effect_id,
            "legacy_recovery":recovery,"native_retry_authorized":false}),
        provider: None,
        provider_seq: None,
        created_at: at_ms,
    })
}

fn exact_carrier(
    connection: &Connection,
    operation_id: &str,
    effect_id: &str,
    effect_kind: &str,
    target_id: &str,
) -> Result<(OperationRecord, String, Value), RuntimeError> {
    let operation = operation_by_id(connection, operation_id)?
        .ok_or_else(|| RuntimeError::NotFound("admitted recovery observation owner".into()))?;
    let old: Option<String> = connection
        .query_row(
            "SELECT evidence_json FROM runtime_operation_effects
         WHERE operation_id=?1 AND effect_id=?2 AND effect_kind=?3 AND target_id=?4",
            params![operation_id, effect_id, effect_kind, target_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed locating exact recovery observation carrier", error))?
        .ok_or_else(|| {
            RuntimeError::Conflict(
                "legacy recovery observation has no exact admitted effect carrier".into(),
            )
        })?;
    let old = old.unwrap_or_else(|| "null".into());
    let evidence: Value = serde_json::from_str(&old).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid original effect evidence: {error}"))
    })?;
    Ok((operation, old, evidence))
}

/// Read the original observation time alongside its known prior-evidence carrier.
/// A retry time never replaces this immutable coordinate.
fn observation_time(evidence: &Value) -> Result<i64, RuntimeError> {
    let mut node = evidence;
    let mut saved = None;
    for _ in 0..32 {
        let Some(object) = node.as_object() else {
            break;
        };
        if object.contains_key("legacy_native_recovery") {
            let at = object
                .get("legacy_native_recovery_observed_at")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    RuntimeError::Conflict(
                        "legacy recovery carrier lacks its original observation time".into(),
                    )
                })?;
            if saved.is_some_and(|prior| prior != at) {
                return Err(RuntimeError::Conflict(
                    "legacy recovery observation times conflict".into(),
                ));
            }
            saved = Some(at);
        }
        let Some(prior) = object.get("prior_evidence") else {
            break;
        };
        node = prior;
    }
    saved.ok_or_else(|| {
        RuntimeError::Conflict("legacy recovery carrier has no observation time".into())
    })
}

fn verify_saved_pair(
    connection: &Connection,
    operation: &OperationRecord,
    effect_id: &str,
    evidence: &Value,
    recovery: &LegacyNativeRecovery,
) -> Result<(), RuntimeError> {
    let expected = expected_event(operation, effect_id, recovery, observation_time(evidence)?)?;
    let saved =
        fetch_runtime_event_by_event_id(connection, &expected.event_id)?.ok_or_else(|| {
            RuntimeError::Conflict(
                "legacy recovery carrier lacks its original Critical observation event".into(),
            )
        })?;
    if saved.event_id != expected.event_id
        || saved.scope != expected.scope
        || saved.scope_id != expected.scope_id
        || saved.kind != expected.kind
        || saved.criticality != RuntimeEventCriticality::Critical
        || saved.payload != expected.payload
        || saved.created_at != expected.created_at
        || saved.session_id.is_some()
        || saved.team_id.is_some()
        || saved.turn_id.is_some()
        || saved.provider.is_some()
        || saved.provider_seq.is_some()
    {
        return Err(RuntimeError::Conflict(
            "legacy recovery carrier and Critical observation event disagree".into(),
        ));
    }
    Ok(())
}

/// Guard owners can use this in their existing IMMEDIATE failure/quarantine
/// transaction. No native phase, attempt, frozen request or claim changes.
pub(crate) fn record_operation_effect_legacy_recovery_tx(
    tx: &Transaction<'_>,
    operation_id: &str,
    effect_id: &str,
    effect_kind: &str,
    target_id: &str,
    recovery: &LegacyNativeRecovery,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    let serialized = serde_json::to_value(recovery).map_err(|error| {
        RuntimeError::InvalidState(format!("invalid recovery observation: {error}"))
    })?;
    if serde_json::to_vec(&serialized)
        .map_err(|error| RuntimeError::InvalidState(error.to_string()))?
        .len()
        > 32 * 1024
    {
        return Err(RuntimeError::InvalidState(
            "legacy recovery observation exceeds bounded receipt size".into(),
        ));
    }
    let (operation, old, mut evidence) =
        exact_carrier(tx, operation_id, effect_id, effect_kind, target_id)?;
    if !evidence.is_object() {
        return Err(RuntimeError::Conflict(
            "legacy recovery observation carrier is not an evidence object".into(),
        ));
    }
    if let Some(saved) = LegacyNativeRecovery::from_effect_evidence(&evidence)? {
        if saved != *recovery {
            return Err(RuntimeError::Conflict(
                "original legacy recovery observation is immutable".into(),
            ));
        }
        return verify_saved_pair(tx, &operation, effect_id, &evidence, &saved);
    }
    if !matches!(
        operation.phase,
        OperationPhase::Requested | OperationPhase::ManualReview
    ) {
        return Err(RuntimeError::Conflict(
            "cannot add a recovery refusal to a terminal operation".into(),
        ));
    }
    let event = expected_event(&operation, effect_id, recovery, at_ms)?;
    if fetch_runtime_event_by_event_id(tx, &event.event_id)?.is_some() {
        return Err(RuntimeError::Conflict(
            "legacy recovery event exists without its exact carrier receipt".into(),
        ));
    }
    evidence["legacy_native_recovery"] = serialized;
    evidence["legacy_native_recovery_observed_at"] = json!(at_ms);
    let changed = tx.execute(
        "UPDATE runtime_operation_effects SET evidence_json=?5
         WHERE operation_id=?1 AND effect_id=?2 AND effect_kind=?3 AND target_id=?4 AND evidence_json=?6",
        params![operation_id,effect_id,effect_kind,target_id,json_to_string(&evidence)?,old],
    ).map_err(|error| db_error("failed recording exact legacy recovery observation", error))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "legacy recovery observation carrier changed".into(),
        ));
    }
    insert_runtime_event_in_transaction(tx, &event)?;
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn read_operation_effect_legacy_recovery(
        &self,
        operation_id: &str,
        effect_id: &str,
        effect_kind: &str,
        target_id: &str,
    ) -> Result<Option<LegacyNativeRecovery>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting legacy recovery observation read", error))?;
        let (operation, _, evidence) =
            exact_carrier(&tx, operation_id, effect_id, effect_kind, target_id)?;
        let saved = LegacyNativeRecovery::from_effect_evidence(&evidence)?;
        if let Some(recovery) = saved.as_ref() {
            verify_saved_pair(&tx, &operation, effect_id, &evidence, recovery)?;
        } else if fetch_runtime_event_by_event_id(&tx, &event_id(operation_id, effect_id)?)?
            .is_some()
        {
            return Err(RuntimeError::Conflict(
                "legacy recovery event exists without its exact carrier receipt".into(),
            ));
        }
        tx.commit().map_err(|error| {
            db_error("failed completing legacy recovery observation read", error)
        })?;
        Ok(saved)
    }

    pub fn record_operation_effect_legacy_recovery(
        &self,
        operation_id: &str,
        effect_id: &str,
        effect_kind: &str,
        target_id: &str,
        recovery: &LegacyNativeRecovery,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting legacy recovery observation", error))?;
        record_operation_effect_legacy_recovery_tx(
            &tx,
            operation_id,
            effect_id,
            effect_kind,
            target_id,
            recovery,
            at_ms,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing legacy recovery observation", error))
    }
}

#[cfg(test)]
mod tests;
