//! Per-target workspace interrupt receipts. These functions do not authorize
//! native work; first-dispatch and known-ACK recovery predicates remain outside.
use super::{INTERRUPT_EFFECT_KIND, WORKSPACE_INTERRUPT_KIND};
use crate::db::{db_error, json_to_string, open_connection};
use crate::SqliteRuntimeRepository;
use runtime_core::RuntimeError;
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

#[derive(Clone, PartialEq)]
pub(crate) struct Target {
    pub(crate) workspace_id: String,
    pub(crate) effect_id: String,
    pub(crate) phase: String,
    pub(crate) evidence: Value,
    pub(crate) attempt_count: i64,
    pub(crate) updated_at: i64,
}

pub(crate) fn target(
    tx: &Transaction<'_>,
    operation_id: &str,
    agent_id: &str,
    turn_id: &str,
) -> Result<Target, RuntimeError> {
    let row: Option<(String, String, String, String, i64, i64)> = tx
        .query_row(
            "SELECT op.workspace_id,e.effect_id,e.phase,e.evidence_json,
                    e.attempt_count,e.updated_at
             FROM runtime_operation_effects e
             JOIN runtime_operations op ON op.operation_id=e.operation_id
             WHERE e.operation_id=?1 AND e.effect_kind=?2 AND e.target_id=?3
               AND e.target_kind='workspace_agent' AND op.kind=?4",
            params![
                operation_id,
                INTERRUPT_EFFECT_KIND,
                agent_id,
                WORKSPACE_INTERRUPT_KIND
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| db_error("failed reading exact workspace interrupt target", error))?;
    let (workspace_id, effect_id, phase, raw, attempt_count, updated_at) = row
        .ok_or_else(|| RuntimeError::Conflict("workspace interrupt target is not owned".into()))?;
    let evidence: Value = serde_json::from_str(&raw).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid interrupt target evidence: {error}"))
    })?;
    if workspace_id.is_empty()
        || attempt_count < 0
        || evidence["turn_id"].as_str() != Some(turn_id)
        || turn_id.is_empty()
    {
        return Err(RuntimeError::Conflict(
            "workspace interrupt target evidence does not match its original turn".into(),
        ));
    }
    Ok(Target {
        workspace_id,
        effect_id,
        phase,
        evidence,
        attempt_count,
        updated_at,
    })
}

pub(crate) struct TargetEventReceipt {
    operation_id: String,
    agent_id: String,
    turn_id: String,
    target: Target,
    event_id: String,
    sequence: i64,
    kind: String,
    payload_json: String,
}

impl TargetEventReceipt {
    pub(crate) fn verify(&self, tx: &Transaction<'_>) -> Result<(), RuntimeError> {
        let exact: bool = tx
            .query_row(
                "SELECT EXISTS(
               SELECT 1 FROM runtime_events WHERE event_id=?1
                 AND scope='workspace' AND scope_id=?2 AND session_id=?3
                 AND team_id IS NULL AND turn_id=?4
                 AND seq=?5 AND kind=?6 AND critical=1 AND payload_json=?7
                 AND provider IS NULL AND provider_seq IS NULL AND created_at=?8)",
                params![
                    self.event_id,
                    self.target.workspace_id,
                    self.agent_id,
                    self.turn_id,
                    self.sequence,
                    self.kind,
                    self.payload_json,
                    self.target.updated_at
                ],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed verifying workspace interrupt target event receipt",
                    error,
                )
            })?;
        if !exact {
            return Err(RuntimeError::Conflict(
                "workspace interrupt target event receipt changed before commit".into(),
            ));
        }
        if target(tx, &self.operation_id, &self.agent_id, &self.turn_id)? != self.target {
            return Err(RuntimeError::Conflict(
                "workspace interrupt target event carrier changed before commit".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn append_target_event(
    tx: &Transaction<'_>,
    operation_id: &str,
    agent_id: &str,
    turn_id: &str,
    from_phase: &str,
    to_phase: &str,
    expected: &Target,
) -> Result<TargetEventReceipt, RuntimeError> {
    if !matches!(
        (from_phase, to_phase),
        ("intended", "started")
            | ("started", "finalized")
            | ("started", "uncertain")
            | ("uncertain", "finalized")
    ) {
        return Err(RuntimeError::Conflict(
            "invalid interrupt target event transition".into(),
        ));
    }
    let stored = target(tx, operation_id, agent_id, turn_id)?;
    if &stored != expected {
        return Err(RuntimeError::Conflict(
            "workspace interrupt target event carrier differs from intended mutation".into(),
        ));
    }
    if stored.phase != to_phase {
        return Err(RuntimeError::Conflict(
            "interrupt target event differs from stored phase".into(),
        ));
    }
    let previous: Option<i64> = tx
        .query_row(
            "SELECT MAX(seq) FROM runtime_events WHERE scope='workspace' AND scope_id=?1",
            [&stored.workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed allocating interrupt target event sequence", error))?;
    let previous = previous.unwrap_or(0);
    let sequence = previous
        .checked_add(1)
        .filter(|_| previous >= 0)
        .ok_or_else(|| {
            RuntimeError::Conflict("exhausted or invalid workspace interrupt event sequence".into())
        })?;
    let kind = format!("workspace.interrupt_target_{to_phase}");
    let event_id = format!("{}:phase:{to_phase}", stored.effect_id);
    let payload = json!({
        "operation_id": operation_id,
        "workspace_id": stored.workspace_id,
        "effect_id": stored.effect_id,
        "agent_id": agent_id,
        "turn_id": turn_id,
        "from_phase": from_phase,
        "phase": to_phase,
        "attempt_count": stored.attempt_count,
        "evidence": stored.evidence,
    });
    // A duplicate identity during a fresh state transition is a conflict,
    // never evidence that an uncommitted mutation already happened.
    let payload_json = json_to_string(&payload)?;
    let inserted = tx
        .execute(
            "INSERT INTO runtime_events
         (event_id,scope,scope_id,session_id,turn_id,seq,kind,critical,payload_json,created_at)
         VALUES(?1,'workspace',?2,?3,?4,?5,?6,1,?7,?8)",
            params![
                event_id,
                stored.workspace_id,
                agent_id,
                turn_id,
                sequence,
                kind,
                payload_json,
                stored.updated_at
            ],
        )
        .map_err(|error| {
            db_error(
                "failed atomically recording workspace interrupt target",
                error,
            )
        })?;
    if inserted != 1 {
        return Err(RuntimeError::Conflict(
            "workspace interrupt target event insertion did not produce exactly one receipt".into(),
        ));
    }
    let receipt = TargetEventReceipt {
        operation_id: operation_id.to_owned(),
        agent_id: agent_id.to_owned(),
        turn_id: turn_id.to_owned(),
        target: stored,
        event_id,
        sequence,
        kind,
        payload_json,
    };
    receipt.verify(tx)?;
    Ok(receipt)
}

impl SqliteRuntimeRepository {
    #[allow(clippy::too_many_arguments)]
    pub fn finalize_workspace_interrupt_effect(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        interrupted: bool,
        reason: &str,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        self.record_workspace_interrupt_receipt(
            operation_id,
            agent_id,
            turn_id,
            "finalized",
            json!({
                "turn_id":turn_id,
                "outcome":if interrupted { "interrupted" } else { "skipped" },
                "reason":reason,
            }),
            changed_at,
        )
    }

    pub fn mark_workspace_interrupt_uncertain(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        error: &Value,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        self.record_workspace_interrupt_receipt(
            operation_id,
            agent_id,
            turn_id,
            "uncertain",
            json!({
                "turn_id":turn_id,
                "outcome":null,
                "reason":"provider_interrupt_outcome_uncertain",
                "error":error,
            }),
            changed_at,
        )
    }

    fn record_workspace_interrupt_receipt(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        phase: &str,
        receipt: Value,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting workspace interrupt receipt", error))?;
        let prior = target(&tx, operation_id, agent_id, turn_id)?;
        let mut comparable = prior.evidence.clone();
        if let Some(fields) = comparable.as_object_mut() {
            fields.remove("prior_evidence");
        }
        // This is read-only replay of an existing outcome. It does not repeat
        // the first-dispatch claim, resample mutable guards, or invent an event
        // for historical pre-event receipts.
        if prior.phase == phase && comparable == receipt {
            return Ok(());
        }
        if prior.phase != "started" {
            return Err(RuntimeError::Conflict(
                "workspace interrupt target is not in started state for this exact receipt".into(),
            ));
        }
        let mut expected = prior.clone();
        let mut evidence = receipt;
        evidence["prior_evidence"] = prior.evidence;
        expected.phase = phase.to_owned();
        expected.evidence = evidence.clone();
        expected.updated_at = changed_at;
        let changed = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase=?4,evidence_json=?5,updated_at=?6
             WHERE operation_id=?1 AND effect_kind=?7 AND target_id=?2
               AND phase='started' AND json_extract(evidence_json,'$.turn_id')=?3",
                params![
                    operation_id,
                    agent_id,
                    turn_id,
                    phase,
                    json_to_string(&evidence)?,
                    changed_at,
                    INTERRUPT_EFFECT_KIND
                ],
            )
            .map_err(|error| {
                db_error("failed recording exact workspace interrupt receipt", error)
            })?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "workspace interrupt receipt lost its target CAS".into(),
            ));
        }
        append_target_event(
            &tx,
            operation_id,
            agent_id,
            turn_id,
            "started",
            phase,
            &expected,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing workspace interrupt receipt", error))?;
        Ok(())
    }
}
