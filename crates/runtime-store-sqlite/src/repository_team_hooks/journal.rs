//! Critical receipts for exact hook changes. Payloads carry only identity,
//! bounded state and hashes; approved commands, input and output stay private.
use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    TeamHookEffectPhase, TeamHookEffectRecord, TeamHookEventName, TeamHookResolvedEntry,
};
use rusqlite::{params, Transaction};
use serde_json::{json, Value};

use super::effects::read_effect;
use crate::db::{db_error, fetch_runtime_event_by_event_id};
use crate::repository::insert_runtime_event_in_transaction;

pub(crate) fn next_critical_sequence_tx(
    tx: &Transaction<'_>,
    scope: &RuntimeEventScope,
    scope_id: &str,
) -> Result<i64, RuntimeError> {
    let previous: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(seq),0) FROM runtime_events WHERE scope=?1 AND scope_id=?2",
            params![scope.as_str(), scope_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed allocating Critical observation sequence", error))?;
    previous
        .checked_add(1)
        .filter(|next| *next > 0)
        .ok_or_else(|| RuntimeError::InvalidState("Critical observation sequence exhausted".into()))
}

pub(crate) fn verify_critical_receipt_tx(
    tx: &Transaction<'_>,
    expected: &NewRuntimeEvent,
    expected_sequence: i64,
) -> Result<RuntimeEventRecord, RuntimeError> {
    let actual = fetch_runtime_event_by_event_id(tx, &expected.event_id)?
        .ok_or_else(|| RuntimeError::Conflict("Critical observation receipt missing".into()))?;
    if actual.event_id != expected.event_id
        || actual.seq != expected_sequence
        || actual.scope != expected.scope
        || actual.scope_id != expected.scope_id
        || actual.session_id != expected.session_id
        || actual.team_id != expected.team_id
        || actual.turn_id != expected.turn_id
        || actual.kind != expected.kind
        || actual.criticality != expected.criticality
        || actual.payload != expected.payload
        || actual.provider != expected.provider
        || actual.provider_seq != expected.provider_seq
        || actual.created_at != expected.created_at
    {
        return Err(RuntimeError::Conflict(
            "Critical observation receipt differs from its authority".into(),
        ));
    }
    Ok(actual)
}

pub(crate) fn insert_critical_receipt_tx(
    tx: &Transaction<'_>,
    expected: &NewRuntimeEvent,
) -> Result<RuntimeEventRecord, RuntimeError> {
    if fetch_runtime_event_by_event_id(tx, &expected.event_id)?.is_some() {
        return Err(RuntimeError::Conflict(
            "fresh observation receipt identity is occupied".into(),
        ));
    }
    let sequence = next_critical_sequence_tx(tx, &expected.scope, &expected.scope_id)?;
    insert_runtime_event_in_transaction(tx, expected)?;
    verify_critical_receipt_tx(tx, expected, sequence)
}

pub(crate) struct CriticalEventProof {
    pub event: NewRuntimeEvent,
    pub sequence: i64,
}
pub(crate) fn verify_critical_proof_tx(
    tx: &Transaction<'_>,
    proof: &CriticalEventProof,
) -> Result<(), RuntimeError> {
    verify_critical_receipt_tx(tx, &proof.event, proof.sequence).map(|_| ())
}
pub(crate) struct HookJournalProof {
    workspace_id: String,
    parent_event_id: String,
    event_name: TeamHookEventName,
    hook_name: String,
    expected_hash: String,
    event: Option<CriticalEventProof>,
}
pub(crate) struct HookTriggerReceipt {
    pub hooks: Vec<TeamHookResolvedEntry>,
    pub proofs: Vec<HookJournalProof>,
}
pub(crate) fn verify_hook_proofs_tx(
    tx: &Transaction<'_>,
    proofs: &[HookJournalProof],
) -> Result<(), RuntimeError> {
    for proof in proofs {
        let effect = read_effect(
            tx,
            &proof.workspace_id,
            &proof.parent_event_id,
            proof.event_name,
            &proof.hook_name,
        )?
        .ok_or_else(|| RuntimeError::Conflict("hook authority disappeared before commit".into()))?;
        if record_hash(&effect)? != proof.expected_hash {
            return Err(RuntimeError::Conflict(
                "hook authority changed before its transaction committed".into(),
            ));
        }
        if let Some(event) = &proof.event {
            verify_critical_proof_tx(tx, event)?;
        }
    }
    Ok(())
}

pub(crate) fn intended_hook(
    workspace_id: &str,
    parent_event_id: &str,
    hook: &TeamHookResolvedEntry,
    input: &Value,
    at_ms: i64,
) -> TeamHookEffectRecord {
    TeamHookEffectRecord {
        workspace_id: workspace_id.into(),
        parent_event_id: parent_event_id.into(),
        hook: hook.clone(),
        input: input.clone(),
        phase: TeamHookEffectPhase::Intended,
        attempt_count: 0,
        receipt: None,
        created_at_ms: at_ms,
        updated_at_ms: at_ms,
        application_state: "not_ready".into(),
        application_error_code: None,
        applied_at_ms: None,
    }
}

fn record_hash(effect: &TeamHookEffectRecord) -> Result<String, RuntimeError> {
    runtime_core::normalized_json_hash(&serde_json::to_value(effect).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid hook receipt projection: {error}"))
    })?)
}

fn state(effect: &TeamHookEffectRecord) -> Value {
    json!({
        "phase":effect.phase.as_str(), "attempt_count":effect.attempt_count,
        "application_state":effect.application_state,
        "created_at_ms":effect.created_at_ms, "updated_at_ms":effect.updated_at_ms,
        "applied_at_ms":effect.applied_at_ms,
    })
}

pub(crate) fn journal_hook_change_tx(
    tx: &Transaction<'_>,
    before: Option<&TeamHookEffectRecord>,
    expected: &TeamHookEffectRecord,
    origin: &'static str,
    at_ms: i64,
) -> Result<HookJournalProof, RuntimeError> {
    let read = || {
        read_effect(
            tx,
            &expected.workspace_id,
            &expected.parent_event_id,
            expected.hook.event_name,
            &expected.hook.name,
        )
    };
    if read()?.as_ref() != Some(expected) {
        return Err(RuntimeError::Conflict(
            "hook authority differs from its intended receipt".into(),
        ));
    }
    let mut proof = HookJournalProof {
        workspace_id: expected.workspace_id.clone(),
        parent_event_id: expected.parent_event_id.clone(),
        event_name: expected.hook.event_name,
        hook_name: expected.hook.name.clone(),
        expected_hash: record_hash(expected)?,
        event: None,
    };
    if before == Some(expected) {
        return Ok(proof);
    }
    if before.is_some_and(|prior| {
        prior.workspace_id != expected.workspace_id
            || prior.parent_event_id != expected.parent_event_id
            || prior.hook != expected.hook
            || prior.input != expected.input
            || prior.created_at_ms != expected.created_at_ms
    }) {
        return Err(RuntimeError::Conflict(
            "hook receipt cannot change its admitted identity".into(),
        ));
    }
    let before_hash = before.map(record_hash).transpose()?;
    let after_hash = record_hash(expected)?;
    let payload = json!({
        "workspace_id":expected.workspace_id, "parent_event_id":expected.parent_event_id,
        "event_name":expected.hook.event_name.as_str(), "hook_name":expected.hook.name,
        "origin":origin, "before":before.map(state), "after":state(expected),
        "before_record_hash":before_hash, "after_record_hash":after_hash,
        "hook_fingerprint":runtime_core::team_hook_fingerprint(&expected.hook),
        "input_hash":runtime_core::normalized_json_hash(&expected.input)?,
        "receipt_hash":expected.receipt.as_ref().map(|receipt|
            serde_json::to_value(receipt).map_err(|error|
                RuntimeError::Bootstrap(format!("invalid bounded hook receipt: {error}")))
                .and_then(|value| runtime_core::normalized_json_hash(&value))).transpose()?,
    });
    let event = NewRuntimeEvent {
        event_id: format!(
            "evt_hook_state_{}",
            runtime_core::normalized_json_hash(&payload)?
        ),
        scope: RuntimeEventScope::Workspace,
        scope_id: expected.workspace_id.clone(),
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: "workspace.hook_effect_transitioned".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: at_ms,
    };
    let committed = insert_critical_receipt_tx(tx, &event)?;
    proof.event = Some(CriticalEventProof {
        event,
        sequence: committed.seq,
    });
    if read()?.as_ref() != Some(expected) {
        return Err(RuntimeError::Conflict(
            "hook authority changed while journaling its receipt".into(),
        ));
    }
    Ok(proof)
}
