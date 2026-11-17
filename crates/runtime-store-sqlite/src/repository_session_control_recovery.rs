//! Immutable diagnostic observation on an existing edit/compaction record.
//! This never grants native authority or rewrites a known native outcome.
use crate::db::{db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::SqliteRuntimeRepository;
use runtime_core::legacy_spawn_obligations::LegacyNativeRecovery;
use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    SessionControlCoordinate,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const SLOT: &str = "legacy_native_recovery_evidence";
const MAX_OBSERVATION_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    recovery: LegacyNativeRecovery,
    observed_at_ms: i64,
}

struct FamilyRow {
    table: &'static str,
    id_column: &'static str,
    phase: String,
    original: String,
    evidence: Option<Value>,
}

fn load(
    connection: &Connection,
    owner: &SessionControlCoordinate,
) -> Result<FamilyRow, RuntimeError> {
    let (table, id_column) = match owner {
        SessionControlCoordinate::EditRerun { .. } => ("session_edit_reruns", "operation_id"),
        SessionControlCoordinate::Compaction { .. } => ("session_compaction_runs", "run_id"),
    };
    let sql = format!("SELECT session_id,phase,record_json FROM {table} WHERE {id_column}=?1");
    let (agent_id, phase, original): (String, String, String) = connection
        .query_row(&sql, [owner.id()], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .optional()
        .map_err(|error| db_error("failed reading session-control observation owner", error))?
        .ok_or_else(|| {
            RuntimeError::NotFound("original session-control observation owner".into())
        })?;
    let record: Value = serde_json::from_str(&original).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid session-control owner: {error}"))
    })?;
    if agent_id != owner.agent_id()
        || record["agent_id"].as_str() != Some(owner.agent_id())
        || record[id_column].as_str() != Some(owner.id())
        || record["phase"].as_str() != Some(phase.as_str())
    {
        return Err(RuntimeError::Conflict(
            "session-control observation owner identity changed".into(),
        ));
    }
    let evidence = record.get(SLOT).filter(|value| !value.is_null()).cloned();
    Ok(FamilyRow {
        table,
        id_column,
        phase,
        original,
        evidence,
    })
}

fn event_id(owner: &SessionControlCoordinate) -> Result<String, RuntimeError> {
    Ok(format!(
        "evt_session_control_legacy_{}",
        runtime_core::normalized_json_hash(&json!(owner))?
    ))
}

fn expected_event(
    owner: &SessionControlCoordinate,
    observation: &Observation,
) -> Result<NewRuntimeEvent, RuntimeError> {
    Ok(NewRuntimeEvent {
        event_id: event_id(owner)?,
        scope: RuntimeEventScope::Session,
        scope_id: owner.agent_id().into(),
        session_id: Some(owner.agent_id().into()),
        team_id: None,
        turn_id: None,
        kind: "session_control.legacy_native_recovery_observed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({"owner":owner,"legacy_recovery":observation.recovery,"native_retry_authorized":false}),
        provider: None,
        provider_seq: None,
        created_at: observation.observed_at_ms,
    })
}

fn decode(value: &Value) -> Result<Observation, RuntimeError> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| RuntimeError::InvalidState(error.to_string()))?;
    if bytes.len() > MAX_OBSERVATION_BYTES {
        return Err(RuntimeError::InvalidState(
            "session-control recovery observation exceeds bounded size".into(),
        ));
    }
    let observation: Observation = serde_json::from_value(value.clone()).map_err(|error| {
        RuntimeError::Conflict(format!(
            "invalid saved session-control observation: {error}"
        ))
    })?;
    if observation.observed_at_ms < 0 {
        return Err(RuntimeError::Conflict(
            "invalid session-control observation time".into(),
        ));
    }
    if serde_json::to_value(&observation)
        .map_err(|error| RuntimeError::InvalidState(error.to_string()))?
        != *value
    {
        return Err(RuntimeError::Conflict(
            "saved session-control observation contains unverified fields".into(),
        ));
    }
    Ok(observation)
}

fn verify_pair(
    connection: &Connection,
    owner: &SessionControlCoordinate,
    observation: &Observation,
) -> Result<(), RuntimeError> {
    let expected = expected_event(owner, observation)?;
    let event =
        fetch_runtime_event_by_event_id(connection, &expected.event_id)?.ok_or_else(|| {
            RuntimeError::Conflict(
                "session-control observation lacks its original Critical event".into(),
            )
        })?;
    if event.event_id != expected.event_id
        || event.scope != expected.scope
        || event.scope_id != expected.scope_id
        || event.session_id != expected.session_id
        || event.team_id.is_some()
        || event.turn_id.is_some()
        || event.kind != expected.kind
        || event.criticality != RuntimeEventCriticality::Critical
        || event.payload != expected.payload
        || event.created_at != expected.created_at
        || event.provider.is_some()
        || event.provider_seq.is_some()
    {
        return Err(RuntimeError::Conflict(
            "session-control observation and Critical event disagree".into(),
        ));
    }
    Ok(())
}

fn unresolved(owner: &SessionControlCoordinate, phase: &str) -> bool {
    match owner {
        SessionControlCoordinate::EditRerun { .. } => matches!(
            phase,
            "intended" | "native_started" | "fork_observed" | "manual_review"
        ),
        SessionControlCoordinate::Compaction { .. } => matches!(
            phase,
            "intended" | "accepted" | "started" | "compacting" | "manual_review"
        ),
    }
}

impl SqliteRuntimeRepository {
    pub fn record_session_control_legacy_recovery(
        &self,
        owner: &SessionControlCoordinate,
        recovery: &LegacyNativeRecovery,
        at_ms: i64,
    ) -> Result<Option<RuntimeEventRecord>, RuntimeError> {
        let proposed = Observation {
            recovery: recovery.clone(),
            observed_at_ms: at_ms,
        };
        let value = serde_json::to_value(&proposed)
            .map_err(|error| RuntimeError::InvalidState(error.to_string()))?;
        decode(&value)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed beginning session-control recovery observation",
                    error,
                )
            })?;
        let row = load(&tx, owner)?;
        if let Some(evidence) = row.evidence.as_ref() {
            let saved = decode(evidence)?;
            if saved.recovery != *recovery {
                return Err(RuntimeError::Conflict(
                    "original session-control recovery observation is immutable".into(),
                ));
            }
            verify_pair(&tx, owner, &saved)?;
            tx.commit().map_err(|error| {
                db_error(
                    "failed completing session-control observation replay",
                    error,
                )
            })?;
            return Ok(None);
        }
        if !unresolved(owner, &row.phase) {
            return Err(RuntimeError::Conflict(
                "cannot add a refusal to a terminal session-control result".into(),
            ));
        }
        if fetch_runtime_event_by_event_id(&tx, &event_id(owner)?)?.is_some() {
            return Err(RuntimeError::Conflict(
                "session-control event exists without its original observation".into(),
            ));
        }
        // Keep every original native field and the column timestamps. This
        // diagnostic is not a phase, native attempt or receipt transition.
        let sql = format!(
            "UPDATE {} SET record_json=json_set(record_json,'$.{SLOT}',json(?2))
            WHERE {}=?1 AND session_id=?3 AND phase=?4 AND record_json=?5",
            row.table, row.id_column
        );
        let changed = tx
            .execute(
                &sql,
                params![
                    owner.id(),
                    json_to_string(&value)?,
                    owner.agent_id(),
                    row.phase,
                    row.original
                ],
            )
            .map_err(|error| db_error("failed saving exact session-control observation", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "session-control observation owner changed".into(),
            ));
        }
        let event = insert_runtime_event_in_transaction(&tx, &expected_event(owner, &proposed)?)?;
        let saved_row = load(&tx, owner)?;
        if saved_row.phase != row.phase {
            return Err(RuntimeError::Conflict(
                "session-control observation changed its owner's phase".into(),
            ));
        }
        let saved = saved_row.evidence.as_ref().ok_or_else(|| {
            RuntimeError::Conflict("session-control observation disappeared before commit".into())
        })?;
        if decode(saved)? != proposed {
            return Err(RuntimeError::Conflict(
                "session-control observation changed before commit".into(),
            ));
        }
        verify_pair(&tx, owner, &proposed)?;
        tx.commit()
            .map_err(|error| db_error("failed committing session-control observation", error))?;
        Ok(Some(event))
    }

    pub fn read_session_control_legacy_recovery(
        &self,
        owner: &SessionControlCoordinate,
    ) -> Result<Option<LegacyNativeRecovery>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| {
                db_error("failed beginning session-control observation read", error)
            })?;
        let row = load(&tx, owner)?;
        let recovery = if let Some(evidence) = row.evidence.as_ref() {
            let saved = decode(evidence)?;
            verify_pair(&tx, owner, &saved)?;
            Some(saved.recovery)
        } else {
            if fetch_runtime_event_by_event_id(&tx, &event_id(owner)?)?.is_some() {
                return Err(RuntimeError::Conflict(
                    "session-control event exists without its original observation".into(),
                ));
            }
            None
        };
        tx.commit().map_err(|error| {
            db_error("failed completing session-control observation read", error)
        })?;
        Ok(recovery)
    }
}
