use runtime_core::{
    OperationActor, OperationActorKind, OperationPhase, OperationTransitionRecord,
    ProviderRetirementCloseProofKind, RuntimeError, WorkspaceRetirementCloseAttempt,
    WorkspaceRetirementCommand, WorkspaceRetirementNativeIdentity,
    WorkspaceRetirementReviewCommand, WorkspaceRetirementReviewOutcome,
    WorkspaceRetirementReviewReceipt, WorkspaceRetirementReviewResolution,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, open_connection, string_to_json};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace_retirement_close_receipt::{
    archive_verified_close, current_native_identity,
};
use crate::repository_workspace_retirement_review_settlement::independently_settled;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Operator evidence settles one exact effect. This transaction never calls
    /// native code, deletes a fence, resamples a retirement, or creates new work.
    pub fn reconcile_workspace_retirement(
        &self,
        command: &WorkspaceRetirementReviewCommand,
    ) -> Result<WorkspaceRetirementReviewOutcome, RuntimeError> {
        command.validate()?;
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error("failed starting operator retirement reconciliation", error)
            })?;
        if let Some(receipt) = review_replay(&tx, command)? {
            return Ok(WorkspaceRetirementReviewOutcome {
                receipt,
                resume_command: None,
            });
        }
        let admitted: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_operations op
             JOIN workspace_retirement_fences f ON f.operation_id=op.operation_id
             JOIN workspaces w ON w.workspace_id=f.workspace_id
             JOIN runtime_operation_resource_claims c ON c.owner_operation_id=op.operation_id
               AND c.resource_kind='workspace' AND c.resource_id=w.workspace_id
             WHERE op.operation_id=?1 AND op.workspace_id=?2 AND f.workspace_id=?2
               AND op.kind='workspace_retirement' AND op.phase='manual_review'
               AND f.phase='manual_review' AND w.lifecycle_state='active')",
                params![command.operation_id, command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error("failed verifying quarantined retirement authority", error)
            })?;
        if !admitted {
            return Err(RuntimeError::Conflict(
                "retirement review requires the exact quarantined operation and workspace claim"
                    .into(),
            ));
        }
        let row:Option<(String,String,String,Option<String>,i64)>=tx.query_row(
            "SELECT effect_kind,target_id,phase,evidence_json,attempt_count FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_id=?2",
            params![command.operation_id,command.request.effect_id],
            |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
        ).optional().map_err(|error|db_error("failed reading exact retirement review effect",error))?;
        let (kind, target, phase, raw, attempts) = row.ok_or_else(|| {
            RuntimeError::Conflict(
                "review effect is not owned by the requested retirement operation".into(),
            )
        })?;
        if !matches!(
            phase.as_str(),
            "intended" | "started" | "observed" | "manual_review"
        ) {
            return Err(RuntimeError::Conflict(
                "retirement effect is already settled; replay its original review key".into(),
            ));
        }
        let prior = raw
            .map(string_to_json)
            .transpose()
            .map_err(|error| db_error("failed decoding prior retirement evidence", error))?
            .unwrap_or(Value::Null);
        let review = json!({"review_id":command.review_id,"operator":command.actor.identifier,
            "evidence":command.request.evidence,"resolution":command.request.resolution,
            "reviewed_at":command.reviewed_at});
        let settled = match &command.request.resolution {
            WorkspaceRetirementReviewResolution::ProviderClosed { identity } => {
                if kind != "workspace_provider_close" || identity.agent_id != target {
                    return Err(RuntimeError::Conflict(
                        "provider closure assertion does not match this exact retirement effect"
                            .into(),
                    ));
                }
                let current = current_native_identity(&tx, &command.operation_id, &target)?;
                if current != *identity {
                    return Err(RuntimeError::Conflict("operator assertion does not match current native identity and archive revision".into()));
                }
                if attempts > 0 {
                    verify_claimed_identity(&prior, &current)?;
                }
                archive_verified_close(
                    &tx,
                    &WorkspaceRetirementCloseAttempt {
                        operation_id: command.operation_id.clone(),
                        agent_id: target,
                        provider: identity.provider.as_str().into(),
                        provider_session_ref: identity.provider_session_ref.clone(),
                    },
                    identity,
                    &phase,
                    ProviderRetirementCloseProofKind::OperatorVerifiedClosed,
                    json!({"prior_evidence":prior,"operator_review":review}),
                    command.reviewed_at,
                )?;
                true
            }
            WorkspaceRetirementReviewResolution::RecheckSettled => {
                let observed = independently_settled(
                    &tx,
                    &command.workspace_id,
                    &kind,
                    &target,
                    command.reviewed_at,
                )?;
                let settled = observed.is_some();
                let outcome = observed
                    .as_ref()
                    .and_then(|value| value.get("completion_outcome"))
                    .and_then(Value::as_str)
                    .unwrap_or(if settled {
                        "independent_terminal_facts_verified"
                    } else {
                        "operator_recheck_still_unresolved"
                    });
                let changed = tx
                    .execute(
                        "UPDATE runtime_operation_effects SET phase=?3,evidence_json=?4,
                    updated_at=?5 WHERE operation_id=?1 AND effect_id=?2 AND phase=?6",
                        params![
                            command.operation_id,
                            command.request.effect_id,
                            if settled { "finalized" } else { phase.as_str() },
                            json_to_string(&json!({"outcome":outcome,"observed":observed,
                            "operator_review":review,"prior_evidence":prior}))?,
                            command.reviewed_at,
                            phase
                        ],
                    )
                    .map_err(|error| {
                        db_error("failed recording retirement local reconciliation", error)
                    })?;
                if changed != 1 {
                    return Err(RuntimeError::Conflict(
                        "retirement review effect changed concurrently".into(),
                    ));
                }
                settled
            }
        };
        let unresolved = unresolved_effects(&tx, &command.operation_id)?;
        let resume = unresolved.is_empty();
        let receipt = WorkspaceRetirementReviewReceipt {
            review_id: command.review_id.clone(),
            workspace_id: command.workspace_id.clone(),
            operation_id: command.operation_id.clone(),
            effect_id: command.request.effect_id.clone(),
            settled,
            operation_phase: if resume {
                OperationPhase::Requested
            } else {
                OperationPhase::ManualReview
            },
            unresolved_effect_ids: unresolved,
            reviewed_at: command.reviewed_at,
        };
        let resume_command = if resume {
            release_review(&tx, command, &receipt)?;
            Some(original_command(&tx, &command.operation_id)?)
        } else {
            None
        };
        tx.execute(
            "INSERT INTO workspace_retirement_reviews
            (review_id,operation_id,effect_id,actor_id,idempotency_key,normalized_request_hash,
             normalized_request_json,receipt_json,reviewed_at)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                command.review_id,
                command.operation_id,
                command.request.effect_id,
                command.actor.identifier,
                command.idempotency_key,
                command.normalized_request_hash,
                json_to_string(&command.normalized_request)?,
                json_to_string(&serde_json::to_value(&receipt).map_err(|error| {
                    RuntimeError::Bootstrap(format!("invalid retirement review receipt: {error}"))
                })?)?,
                command.reviewed_at
            ],
        )
        .map_err(|error| {
            db_error(
                "failed recording immutable retirement review receipt",
                error,
            )
        })?;
        tx.commit().map_err(|error| {
            db_error(
                "failed committing operator retirement reconciliation",
                error,
            )
        })?;
        Ok(WorkspaceRetirementReviewOutcome {
            receipt,
            resume_command,
        })
    }
}

fn review_replay(
    tx: &Transaction<'_>,
    command: &WorkspaceRetirementReviewCommand,
) -> Result<Option<WorkspaceRetirementReviewReceipt>, RuntimeError> {
    let row:Option<(String,String,String)>=tx.query_row(
        "SELECT normalized_request_hash,normalized_request_json,receipt_json FROM workspace_retirement_reviews
         WHERE operation_id=?1 AND actor_id=?2 AND idempotency_key=?3",
        params![command.operation_id,command.actor.identifier,command.idempotency_key],
        |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    ).optional().map_err(|error|db_error("failed checking retirement review replay",error))?;
    let Some((hash, request, receipt)) = row else {
        return Ok(None);
    };
    if hash != command.normalized_request_hash
        || string_to_json(request)
            .map_err(|error| db_error("failed decoding original retirement review", error))?
            != command.normalized_request
    {
        return Err(RuntimeError::Conflict(
            "retirement review idempotency key was reused with different input".into(),
        ));
    }
    Ok(Some(serde_json::from_str(&receipt).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid stored retirement review receipt: {error}"))
    })?))
}

fn verify_claimed_identity(
    prior: &Value,
    current: &WorkspaceRetirementNativeIdentity,
) -> Result<(), RuntimeError> {
    let mut item = prior;
    while let Some(previous) = item.get("prior_evidence") {
        if item.get("native_identity").is_some() {
            break;
        }
        item = previous;
    }
    let mut admitted: WorkspaceRetirementNativeIdentity =
        serde_json::from_value(item.get("native_identity").cloned().ok_or_else(|| {
            RuntimeError::Conflict(
                "retirement native close lacks its immutable dispatch identity".into(),
            )
        })?)
        .map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid admitted native close identity: {error}"))
        })?;
    // Current timestamp is independently compare-and-set by the request. A
    // benign session update cannot change the immutable native/archive binding.
    admitted.session_updated_at = current.session_updated_at;
    if admitted != *current {
        return Err(RuntimeError::Conflict(
            "native attachment changed since the original retirement close intent".into(),
        ));
    }
    Ok(())
}

fn unresolved_effects(
    tx: &Transaction<'_>,
    operation_id: &str,
) -> Result<Vec<String>, RuntimeError> {
    let mut stmt = tx
        .prepare(
            "SELECT effect_id FROM runtime_operation_effects WHERE operation_id=?1
        AND phase NOT IN ('intended','finalized') ORDER BY effect_id",
        )
        .map_err(|error| db_error("failed preparing unresolved retirement review scan", error))?;
    let rows = stmt
        .query_map([operation_id], |row| row.get::<_, String>(0))
        .map_err(|error| {
            db_error(
                "failed scanning unresolved retirement review effects",
                error,
            )
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            db_error(
                "failed decoding unresolved retirement review effects",
                error,
            )
        })?;
    Ok(rows)
}

fn release_review(
    tx: &Transaction<'_>,
    command: &WorkspaceRetirementReviewCommand,
    receipt: &WorkspaceRetirementReviewReceipt,
) -> Result<(), RuntimeError> {
    for sql in [
        "UPDATE workspace_retirement_fences SET phase='frozen',updated_at=?2 WHERE operation_id=?1 AND phase='manual_review'",
        "UPDATE runtime_operations SET phase='requested',updated_at=?2 WHERE operation_id=?1 AND phase='manual_review'",
    ] {
        if tx.execute(sql,params![command.operation_id,command.reviewed_at])
            .map_err(|error|db_error("failed releasing verified retirement quarantine",error))?!=1 {
            return Err(RuntimeError::Conflict("retirement quarantine changed during operator review".into()));
        }
    }
    let sequence: u64 = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_operation_transitions
        WHERE operation_id=?1",
            [&command.operation_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed allocating operator review transition", error))?;
    OperationAuthorityTransaction::new(tx).append_transition(&OperationTransitionRecord {
        operation_id: command.operation_id.clone(),
        sequence,
        from_phase: Some(OperationPhase::ManualReview),
        to_phase: OperationPhase::Requested,
        evidence: Some(json!({"operator_review":receipt,
            "operator":command.actor.identifier,"evidence":command.request.evidence})),
        created_at: command.reviewed_at,
    })
}

fn original_command(
    tx: &Transaction<'_>,
    operation_id: &str,
) -> Result<WorkspaceRetirementCommand, RuntimeError> {
    let (actor_kind,actor_id,key,hash,raw,workspace_id,requested_at):(String,String,String,String,String,String,i64)=tx.query_row(
        "SELECT actor_kind,actor_id,idempotency_key,normalized_request_hash,normalized_request_json,
            workspace_id,created_at FROM runtime_operations WHERE operation_id=?1
            AND kind='workspace_retirement' AND phase='requested'",[operation_id],
        |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?)),
    ).map_err(|error|db_error("failed recovering original retirement command after review",error))?;
    let normalized_request = string_to_json(raw)
        .map_err(|error| db_error("failed decoding original retirement command", error))?;
    let expected_revision = normalized_request
        .get("expected_revision")
        .and_then(Value::as_u64)
        .ok_or_else(|| RuntimeError::Bootstrap("original retirement revision is missing".into()))?;
    Ok(WorkspaceRetirementCommand {
        operation_id: operation_id.into(),
        actor: OperationActor {
            kind: OperationActorKind::from_str(&actor_kind).ok_or_else(|| {
                RuntimeError::Bootstrap("original retirement actor is invalid".into())
            })?,
            identifier: actor_id,
        },
        idempotency_key: Some(key),
        normalized_request_hash: hash,
        normalized_request,
        workspace_id,
        expected_revision,
        requested_at,
    })
}
