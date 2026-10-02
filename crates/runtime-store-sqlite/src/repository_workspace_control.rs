use runtime_core::{
    OperationEffectRecord, OperationPhase, OperationRecord, OperationTransitionRecord,
    RuntimeError, WorkspaceInterruptAdmission, WorkspaceInterruptCommand, WorkspaceInterruptPlan,
    WorkspaceInterruptResponse, WorkspaceInterruptTarget, WorkspaceLeadTransitionCommand,
    WorkspaceLeadTransitionResponse, WorkspaceLifecycleState,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{collect_rows, db_error, json_to_string, open_connection, string_to_json};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::{operation_by_idempotency, workspace_by_id};
use crate::SqliteRuntimeRepository;

const WORKSPACE_LEAD_KIND: &str = "workspace_lead_transition";
const WORKSPACE_INTERRUPT_KIND: &str = "workspace_interrupt";
const WORKSPACE_RESOURCE_KIND: &str = "workspace";
const INTERRUPT_EFFECT_KIND: &str = "workspace_turn_interrupt";

impl SqliteRuntimeRepository {
    pub fn transition_workspace_lead(
        &self,
        command: &WorkspaceLeadTransitionCommand,
    ) -> Result<WorkspaceLeadTransitionResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting workspace lead transaction", error))?;

        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            command.actor.identifier.as_str(),
            command.idempotency_key.as_deref(),
        )? {
            ensure_operation_replay_matches(
                &existing,
                WORKSPACE_LEAD_KIND,
                command.normalized_request_hash.as_str(),
            )?;
            let result = existing.exact_terminal_result.ok_or_else(|| {
                RuntimeError::Conflict(format!(
                    "workspace lead operation {} is not terminal",
                    existing.operation_id
                ))
            })?;
            let response = serde_json::from_value(result).map_err(|error| {
                RuntimeError::Bootstrap(format!(
                    "failed decoding workspace lead replay result: {error}"
                ))
            })?;
            tx.commit()
                .map_err(|error| db_error("failed committing workspace lead replay", error))?;
            return Ok(response);
        }

        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: WORKSPACE_LEAD_KIND.to_string(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: None,
            created_at: command.requested_at,
        })?;
        let claim = authority.acquire_claim(
            &command.operation_id,
            WORKSPACE_RESOURCE_KIND,
            &command.workspace_id,
            command.requested_at,
        )?;

        let workspace = workspace_by_id(&tx, &command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", command.workspace_id)))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::InvalidState(format!(
                "workspace {} is not active",
                command.workspace_id
            )));
        }
        if workspace.revision != command.expected_revision {
            return Err(RuntimeError::Conflict(format!(
                "workspace {} revision changed: expected {}, actual {}",
                command.workspace_id, command.expected_revision, workspace.revision
            )));
        }
        if let Some(lead_agent_id) = command.lead_agent_id.as_deref() {
            require_active_workspace_member(&tx, &command.workspace_id, lead_agent_id)?;
        }

        if workspace.lead_agent_id != command.lead_agent_id {
            let changed = tx
                .execute(
                    "UPDATE workspaces
                     SET lead_agent_id = ?3, revision = revision + 1, updated_at = ?4
                     WHERE workspace_id = ?1 AND revision = ?2",
                    params![
                        command.workspace_id,
                        i64::try_from(command.expected_revision).map_err(|_| {
                            RuntimeError::Bootstrap("workspace revision overflow".to_string())
                        })?,
                        command.lead_agent_id,
                        command.requested_at,
                    ],
                )
                .map_err(|error| db_error("failed compare-and-swap workspace lead", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(format!(
                    "workspace {} lead revision changed concurrently",
                    command.workspace_id
                )));
            }
        }

        let workspace = workspace_by_id(&tx, &command.workspace_id)?.ok_or_else(|| {
            RuntimeError::Bootstrap("workspace disappeared during lead transition".to_string())
        })?;
        let response = WorkspaceLeadTransitionResponse {
            operation_id: command.operation_id.clone(),
            workspace: workspace.clone(),
        };
        let exact = serde_json::to_value(&response).map_err(|error| {
            RuntimeError::Bootstrap(format!("failed serializing workspace lead result: {error}"))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&command.workspace_id),
            &exact,
            None,
            None,
            command.requested_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({
                "lead_agent_id": workspace.lead_agent_id,
                "workspace_revision": workspace.revision,
            })),
            created_at: command.requested_at,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing workspace lead transition", error))?;
        Ok(response)
    }

    pub fn begin_workspace_interrupt(
        &self,
        command: &WorkspaceInterruptCommand,
    ) -> Result<WorkspaceInterruptAdmission, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting workspace interrupt transaction", error))?;

        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            command.actor.identifier.as_str(),
            command.idempotency_key.as_deref(),
        )? {
            ensure_operation_replay_matches(
                &existing,
                WORKSPACE_INTERRUPT_KIND,
                command.normalized_request_hash.as_str(),
            )?;
            if let Some(result) = existing.exact_terminal_result {
                let response = serde_json::from_value(result).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "failed decoding workspace interrupt replay result: {error}"
                    ))
                })?;
                tx.commit().map_err(|error| {
                    db_error("failed committing workspace interrupt replay", error)
                })?;
                return Ok(WorkspaceInterruptAdmission::Replay(response));
            }
            if existing.phase != OperationPhase::Requested {
                return Err(RuntimeError::Conflict(format!(
                    "workspace interrupt operation {} requires manual recovery in phase {}",
                    existing.operation_id,
                    existing.phase.as_str()
                )));
            }
            let plan = recover_interrupt_plan(&tx, &existing.operation_id, &command.workspace_id)?;
            tx.commit().map_err(|error| {
                db_error(
                    "failed committing workspace interrupt recovery admission",
                    error,
                )
            })?;
            return Ok(WorkspaceInterruptAdmission::Execute(plan));
        }

        let workspace = workspace_by_id(&tx, &command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", command.workspace_id)))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::InvalidState(format!(
                "workspace {} is not active",
                command.workspace_id
            )));
        }

        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: WORKSPACE_INTERRUPT_KIND.to_string(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: Some(serde_json::json!({"workspace_revision": workspace.revision})),
            created_at: command.requested_at,
        })?;
        authority.acquire_claim(
            &command.operation_id,
            WORKSPACE_RESOURCE_KIND,
            &command.workspace_id,
            command.requested_at,
        )?;

        let members = interrupt_roster(&tx, &command.workspace_id)?;
        let mut targets = Vec::new();
        for (agent_id, turn_id) in members {
            let effect_id = format!("{}:interrupt:{agent_id}", command.operation_id);
            let (phase, evidence) = match turn_id {
                Some(turn_id) => {
                    targets.push(WorkspaceInterruptTarget {
                        agent_id: agent_id.clone(),
                        turn_id: turn_id.clone(),
                    });
                    (
                        "intended",
                        serde_json::json!({"turn_id": turn_id, "outcome": null}),
                    )
                }
                None => (
                    "finalized",
                    serde_json::json!({
                        "turn_id": null,
                        "outcome": "skipped",
                        "reason": "idle",
                    }),
                ),
            };
            authority.insert_effect(&OperationEffectRecord {
                effect_id,
                operation_id: command.operation_id.clone(),
                effect_kind: INTERRUPT_EFFECT_KIND.to_string(),
                target_kind: "workspace_agent".to_string(),
                target_id: agent_id.clone(),
                phase: phase.to_string(),
                idempotency_key: format!("interrupt:{agent_id}"),
                evidence: Some(evidence),
                attempt_count: 0,
                created_at: command.requested_at,
                updated_at: command.requested_at,
            })?;
        }
        tx.commit()
            .map_err(|error| db_error("failed committing workspace interrupt admission", error))?;
        Ok(WorkspaceInterruptAdmission::Execute(
            WorkspaceInterruptPlan {
                operation_id: command.operation_id.clone(),
                workspace_id: command.workspace_id.clone(),
                targets,
            },
        ))
    }

    pub fn mark_workspace_interrupt_started(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let changed = connection
            .execute(
                "UPDATE runtime_operation_effects
                 SET phase = 'started', attempt_count = attempt_count + 1, updated_at = ?4
                 WHERE operation_id = ?1 AND effect_kind = ?5 AND target_id = ?2
                   AND phase = 'intended' AND json_extract(evidence_json, '$.turn_id') = ?3",
                params![
                    operation_id,
                    agent_id,
                    turn_id,
                    changed_at,
                    INTERRUPT_EFFECT_KIND
                ],
            )
            .map_err(|error| db_error("failed marking workspace interrupt started", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "workspace interrupt effect for agent {agent_id} is not safely dispatchable"
            )));
        }
        Ok(())
    }

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
        let connection = open_connection(&self.database_path)?;
        let outcome = if interrupted {
            "interrupted"
        } else {
            "skipped"
        };
        let evidence = json_to_string(&serde_json::json!({
            "turn_id": turn_id,
            "outcome": outcome,
            "reason": reason,
        }))?;
        let changed = connection
            .execute(
                "UPDATE runtime_operation_effects
                 SET phase = 'finalized', evidence_json = ?4, updated_at = ?5
                 WHERE operation_id = ?1 AND effect_kind = ?6 AND target_id = ?2
                   AND phase = 'started' AND json_extract(evidence_json, '$.turn_id') = ?3",
                params![
                    operation_id,
                    agent_id,
                    turn_id,
                    evidence,
                    changed_at,
                    INTERRUPT_EFFECT_KIND,
                ],
            )
            .map_err(|error| db_error("failed finalizing workspace interrupt effect", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "workspace interrupt effect for agent {agent_id} is not in started state"
            )));
        }
        Ok(())
    }

    pub fn mark_workspace_interrupt_uncertain(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        error: &serde_json::Value,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let evidence = json_to_string(&serde_json::json!({
            "turn_id": turn_id,
            "outcome": null,
            "reason": "provider_interrupt_outcome_uncertain",
            "error": error,
        }))?;
        let changed = connection
            .execute(
                "UPDATE runtime_operation_effects
                 SET phase = 'uncertain', evidence_json = ?4, updated_at = ?5
                 WHERE operation_id = ?1 AND effect_kind = ?6 AND target_id = ?2
                   AND phase = 'started' AND json_extract(evidence_json, '$.turn_id') = ?3",
                params![
                    operation_id,
                    agent_id,
                    turn_id,
                    evidence,
                    changed_at,
                    INTERRUPT_EFFECT_KIND,
                ],
            )
            .map_err(|db| db_error("failed recording uncertain workspace interrupt", db))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "workspace interrupt effect for agent {agent_id} could not enter uncertain state"
            )));
        }
        Ok(())
    }

    pub fn complete_workspace_interrupt(
        &self,
        operation_id: &str,
        completed_at: i64,
    ) -> Result<WorkspaceInterruptResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting workspace interrupt completion", error))?;
        let operation = operation_by_id(&tx, operation_id)?.ok_or_else(|| {
            RuntimeError::NotFound(format!("workspace interrupt operation {operation_id}"))
        })?;
        if operation.kind != WORKSPACE_INTERRUPT_KIND {
            return Err(RuntimeError::Conflict(format!(
                "operation {operation_id} is not a workspace interrupt"
            )));
        }
        if let Some(exact) = operation.exact_terminal_result {
            let response = serde_json::from_value(exact).map_err(|error| {
                RuntimeError::Bootstrap(format!(
                    "failed decoding completed workspace interrupt: {error}"
                ))
            })?;
            tx.commit().map_err(|error| {
                db_error(
                    "failed committing completed workspace interrupt replay",
                    error,
                )
            })?;
            return Ok(response);
        }
        let workspace_id = operation.workspace_id.clone().ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!(
                "workspace interrupt operation {operation_id} has no workspace"
            ))
        })?;
        let outcomes = interrupt_outcomes(&tx, operation_id)?;
        if outcomes.iter().any(|(_, phase, _)| phase != "finalized") {
            return Err(RuntimeError::Conflict(format!(
                "workspace interrupt operation {operation_id} still has unresolved effects"
            )));
        }
        let mut interrupted_agent_ids = Vec::new();
        let mut skipped_agent_ids = Vec::new();
        for (agent_id, _, evidence) in outcomes {
            match evidence.get("outcome").and_then(serde_json::Value::as_str) {
                Some("interrupted") => interrupted_agent_ids.push(agent_id),
                Some("skipped") => skipped_agent_ids.push(agent_id),
                other => {
                    return Err(RuntimeError::ProtocolViolation(format!(
                        "workspace interrupt effect has invalid outcome {other:?}"
                    )))
                }
            }
        }
        interrupted_agent_ids.sort();
        skipped_agent_ids.sort();
        let response = WorkspaceInterruptResponse {
            operation_id: operation_id.to_string(),
            workspace_id: workspace_id.clone(),
            interrupted_agent_ids,
            skipped_agent_ids,
        };
        let exact = serde_json::to_value(&response).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed serializing workspace interrupt result: {error}"
            ))
        })?;
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&workspace_id),
            &exact,
            None,
            None,
            completed_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: operation_id.to_string(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({
                "interrupted_count": response.interrupted_agent_ids.len(),
                "skipped_count": response.skipped_agent_ids.len(),
            })),
            created_at: completed_at,
        })?;
        let claim = operation_workspace_claim(&tx, operation_id)?.ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!(
                "workspace interrupt operation {operation_id} lost its workspace claim"
            ))
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing workspace interrupt completion", error))?;
        Ok(response)
    }
}

fn ensure_operation_replay_matches(
    operation: &OperationRecord,
    expected_kind: &str,
    expected_hash: &str,
) -> Result<(), RuntimeError> {
    if operation.kind != expected_kind || operation.normalized_request_hash != expected_hash {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key already belongs to operation {} with different input",
            operation.operation_id
        )));
    }
    Ok(())
}

fn require_active_workspace_member(
    tx: &Transaction<'_>,
    workspace_id: &str,
    agent_id: &str,
) -> Result<(), RuntimeError> {
    let found = tx
        .query_row(
            "SELECT 1 FROM workspace_agents
             WHERE workspace_id = ?1 AND session_id = ?2 AND lifecycle_state = 'active'",
            params![workspace_id, agent_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| db_error("failed validating workspace lead member", error))?;
    found.ok_or_else(|| {
        RuntimeError::InvalidState(format!(
            "workspace lead {agent_id} must be an active member of workspace {workspace_id}"
        ))
    })
}

fn interrupt_roster(
    tx: &Transaction<'_>,
    workspace_id: &str,
) -> Result<Vec<(String, Option<String>)>, RuntimeError> {
    let mut statement = tx
        .prepare(
            "SELECT agent.session_id, session.active_turn_id
             FROM workspace_agents agent
             JOIN sessions session ON session.id = agent.session_id
             WHERE agent.workspace_id = ?1 AND agent.lifecycle_state = 'active'
             ORDER BY agent.created_at ASC, agent.session_id ASC",
        )
        .map_err(|error| db_error("failed preparing workspace interrupt roster", error))?;
    let rows = statement
        .query_map(params![workspace_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|error| db_error("failed querying workspace interrupt roster", error))?;
    collect_rows(rows)
}

fn recover_interrupt_plan(
    tx: &Transaction<'_>,
    operation_id: &str,
    workspace_id: &str,
) -> Result<WorkspaceInterruptPlan, RuntimeError> {
    let effects = interrupt_outcomes(tx, operation_id)?;
    let mut targets = Vec::new();
    for (agent_id, phase, evidence) in effects {
        let turn_id = evidence
            .get("turn_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        match phase.as_str() {
            "intended" => {
                let turn_id = turn_id.ok_or_else(|| {
                    RuntimeError::ProtocolViolation(format!(
                        "intended workspace interrupt for {agent_id} has no turn id"
                    ))
                })?;
                targets.push(WorkspaceInterruptTarget { agent_id, turn_id });
            }
            "started" | "uncertain" => {
                let turn_id = turn_id.ok_or_else(|| {
                    RuntimeError::ProtocolViolation(format!(
                        "started workspace interrupt for {agent_id} has no turn id"
                    ))
                })?;
                if interrupt_requested_event_exists(tx, &agent_id, &turn_id)? {
                    recover_finalize_effect(
                        tx,
                        operation_id,
                        &agent_id,
                        &turn_id,
                        true,
                        "recovered_interrupt_request_event",
                    )?;
                } else if current_active_turn(tx, &agent_id)?.as_deref() != Some(turn_id.as_str()) {
                    recover_finalize_effect(
                        tx,
                        operation_id,
                        &agent_id,
                        &turn_id,
                        false,
                        "turn_no_longer_active_during_recovery",
                    )?;
                } else {
                    return Err(RuntimeError::Conflict(format!(
                        "workspace interrupt outcome is unresolved for agent {agent_id}; refusing duplicate provider interrupt"
                    )));
                }
            }
            "finalized" => {}
            other => {
                return Err(RuntimeError::Conflict(format!(
                    "workspace interrupt effect for agent {agent_id} requires recovery from phase {other}"
                )))
            }
        }
    }
    Ok(WorkspaceInterruptPlan {
        operation_id: operation_id.to_string(),
        workspace_id: workspace_id.to_string(),
        targets,
    })
}

fn recover_finalize_effect(
    tx: &Transaction<'_>,
    operation_id: &str,
    agent_id: &str,
    turn_id: &str,
    interrupted: bool,
    reason: &str,
) -> Result<(), RuntimeError> {
    let outcome = if interrupted {
        "interrupted"
    } else {
        "skipped"
    };
    let evidence = json_to_string(&serde_json::json!({
        "turn_id": turn_id,
        "outcome": outcome,
        "reason": reason,
    }))?;
    tx.execute(
        "UPDATE runtime_operation_effects
         SET phase = 'finalized', evidence_json = ?4, updated_at = updated_at + 1
         WHERE operation_id = ?1 AND effect_kind = ?5 AND target_id = ?2
           AND phase IN ('started', 'uncertain')
           AND json_extract(evidence_json, '$.turn_id') = ?3",
        params![
            operation_id,
            agent_id,
            turn_id,
            evidence,
            INTERRUPT_EFFECT_KIND
        ],
    )
    .map_err(|error| db_error("failed recovering workspace interrupt effect", error))?;
    Ok(())
}

fn interrupt_outcomes(
    tx: &Transaction<'_>,
    operation_id: &str,
) -> Result<Vec<(String, String, serde_json::Value)>, RuntimeError> {
    let mut statement = tx
        .prepare(
            "SELECT target_id, phase, evidence_json
             FROM runtime_operation_effects
             WHERE operation_id = ?1 AND effect_kind = ?2
             ORDER BY target_id ASC",
        )
        .map_err(|error| db_error("failed preparing workspace interrupt effects", error))?;
    let rows = statement
        .query_map(params![operation_id, INTERRUPT_EFFECT_KIND], |row| {
            let evidence: Option<String> = row.get(2)?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                evidence
                    .map(string_to_json)
                    .transpose()?
                    .unwrap_or_else(|| serde_json::json!({})),
            ))
        })
        .map_err(|error| db_error("failed querying workspace interrupt effects", error))?;
    collect_rows(rows)
}

fn interrupt_requested_event_exists(
    tx: &Transaction<'_>,
    agent_id: &str,
    turn_id: &str,
) -> Result<bool, RuntimeError> {
    tx.query_row(
        "SELECT 1 FROM runtime_events
         WHERE session_id = ?1 AND turn_id = ?2 AND kind = 'turn.interrupt_requested'
         LIMIT 1",
        params![agent_id, turn_id],
        |_| Ok(()),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(|error| db_error("failed reading workspace interrupt recovery event", error))
}

fn current_active_turn(
    tx: &Transaction<'_>,
    agent_id: &str,
) -> Result<Option<String>, RuntimeError> {
    tx.query_row(
        "SELECT active_turn_id FROM sessions WHERE id = ?1",
        params![agent_id],
        |row| row.get(0),
    )
    .optional()
    .map(|value| value.flatten())
    .map_err(|error| db_error("failed reading workspace interrupt active turn", error))
}

fn operation_by_id(
    tx: &Transaction<'_>,
    operation_id: &str,
) -> Result<Option<OperationRecord>, RuntimeError> {
    tx.query_row(
        "SELECT operation_id, workspace_id, kind, actor_kind, actor_id, idempotency_key,
                normalized_request_hash, normalized_request_json, phase,
                exact_terminal_result_json, error_code, error_message, created_at, updated_at
         FROM runtime_operations WHERE operation_id = ?1",
        params![operation_id],
        |row| {
            let actor_kind = runtime_core::OperationActorKind::from_str(&row.get::<_, String>(3)?)
                .ok_or_else(|| conversion_error(3, "invalid operation actor kind"))?;
            let phase = OperationPhase::from_str(&row.get::<_, String>(8)?)
                .ok_or_else(|| conversion_error(8, "invalid operation phase"))?;
            let exact: Option<String> = row.get(9)?;
            Ok(OperationRecord {
                operation_id: row.get(0)?,
                workspace_id: row.get(1)?,
                kind: row.get(2)?,
                actor: runtime_core::OperationActor {
                    kind: actor_kind,
                    identifier: row.get(4)?,
                },
                idempotency_key: row.get(5)?,
                normalized_request_hash: row.get(6)?,
                normalized_request: string_to_json(row.get(7)?)?,
                phase,
                exact_terminal_result: exact.map(string_to_json).transpose()?,
                error_code: row.get(10)?,
                error_message: row.get(11)?,
                created_at: row.get(12)?,
                updated_at: row.get(13)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading workspace interrupt operation", error))
}

fn operation_workspace_claim(
    tx: &Transaction<'_>,
    operation_id: &str,
) -> Result<Option<runtime_core::OperationResourceClaimRecord>, RuntimeError> {
    tx.query_row(
        "SELECT resource_kind, resource_id, claim_mode, owner_operation_id,
                fence_generation, acquired_at
         FROM runtime_operation_resource_claims
         WHERE owner_operation_id = ?1 AND resource_kind = ?2",
        params![operation_id, WORKSPACE_RESOURCE_KIND],
        |row| {
            let generation: i64 = row.get(4)?;
            Ok(runtime_core::OperationResourceClaimRecord {
                resource_kind: row.get(0)?,
                resource_id: row.get(1)?,
                claim_mode: row.get(2)?,
                owner_operation_id: row.get(3)?,
                fence_generation: u64::try_from(generation)
                    .map_err(|_| conversion_error(4, "negative fence generation"))?,
                acquired_at: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading workspace interrupt claim", error))
}

fn conversion_error(index: usize, message: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message.to_string(),
        )),
    )
}
