use runtime_core::{
    OperationActorKind, OperationPhase, OperationResourceClaimRecord, OperationTransitionRecord,
    OperatorAgentArchiveResolution, OperatorAgentArchiveReviewCommand,
    OperatorAgentArchiveReviewResponse, RuntimeError, WorkspaceAgentLifecycleState,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_agent_archive::{CLAIM_KIND, EFFECT_KIND, OPERATION_KIND};
use crate::repository_operator_agent_restore::{restore_state, restored_session_snapshot};
use crate::repository_workspace::operation_by_id;
use crate::repository_workspace_agent::update_session;
use crate::repository_workspace_agent_lifecycle::apply_workspace_agent_lifecycle_tx;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn operator_agent_archive_reattachment_pending(
        &self,
        agent_id: &str,
    ) -> Result<bool, RuntimeError> {
        let conn = open_connection(&self.database_path)?;
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1 AND status='archive_recovery_required')",
            [agent_id], |row| row.get(0),
        ).map_err(|error| db_error("failed checking independently reviewed archive reattachment", error))
    }

    /// Independent evidence settles one quarantined provider close without
    /// dispatching any native provider work. A closed attachment may archive
    /// the roster atomically. A verified-not-closed attachment remains active,
    /// but new native work is frozen until a *fresh* startup proves exact
    /// provider identity again.
    pub fn resolve_operator_agent_archive_review(
        &self,
        command: &OperatorAgentArchiveReviewCommand,
    ) -> Result<OperatorAgentArchiveReviewResponse, RuntimeError> {
        let input = &command.input;
        let evidence = input.evidence.trim();
        if command.actor.kind != OperationActorKind::Operator
            || command.actor.identifier.trim().is_empty()
            || command.operation_id.trim().is_empty()
            || command.agent_id.trim().is_empty()
            || command.workspace_id.trim().is_empty()
            || !(8..=2048).contains(&evidence.len())
            || input.provider.trim().is_empty()
            || input.original_provider_session_ref.trim().is_empty()
            || (input.resolution == OperatorAgentArchiveResolution::NotClosed
                && input
                    .observed_provider_session_ref
                    .as_deref()
                    .is_none_or(|v| v.trim().is_empty()))
            || (input.resolution == OperatorAgentArchiveResolution::Closed
                && (input.observed_provider_session_ref.is_some()
                    || input.observed_canonical_provider_session_ref.is_some()))
        {
            return Err(RuntimeError::InvalidState(
                "operator archive review requires exact original attachment and independently verified evidence".into(),
            ));
        }
        let mut normalized_input = input.clone();
        normalized_input.evidence = evidence.into();
        let input_json = serde_json::to_value(&normalized_input)
            .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting independent archive review", error))?;
        let record = operation_by_id(&tx, &command.operation_id)?.ok_or_else(|| {
            RuntimeError::NotFound(format!("archive operation {}", command.operation_id))
        })?;
        if record.kind != OPERATION_KIND
            || record.actor != command.actor
            || record.workspace_id.as_deref() != Some(&command.workspace_id)
            || record.normalized_request["workspace_id"] != command.workspace_id
            || record.normalized_request["agent_id"] != command.agent_id
            || record.normalized_request["action"] != "archive"
            || runtime_core::normalized_json_hash(&record.normalized_request)?
                != record.normalized_request_hash
        {
            return Err(RuntimeError::Conflict(
                "archive review does not match immutable operator authority".into(),
            ));
        }
        let (phase, raw_effect): (String, String) = tx
            .query_row(
                "SELECT phase,evidence_json FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3",
                params![command.operation_id, EFFECT_KIND, command.agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed loading exact archive review target", error))?
            .ok_or_else(|| {
                RuntimeError::Conflict("no native close effect exists for this archive".into())
            })?;
        let effect: Value = serde_json::from_str(&raw_effect).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid stored archive effect: {error}"))
        })?;
        if matches!(
            record.phase,
            OperationPhase::Completed | OperationPhase::Failed
        ) {
            let review = &effect["operator_review"];
            if phase == "finalized" && review["input"] == input_json {
                return serde_json::from_value(review["response"].clone()).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "invalid stored archive review receipt: {error}"
                    ))
                });
            }
            return Err(RuntimeError::Conflict(
                "archive operation has a different terminal result".into(),
            ));
        }
        if record.phase != OperationPhase::ManualReview || phase != "uncertain" {
            return Err(RuntimeError::Conflict(
                "only quarantined native archives accept independent review".into(),
            ));
        }
        if effect["provider"] != input.provider
            || effect["original_provider_session_ref"] != input.original_provider_session_ref
            || effect["original_canonical_provider_session_ref"]
                != json!(input.original_canonical_provider_session_ref)
            || effect["expected_revision"] != input.expected_revision
            || effect["session_updated_at"] != input.session_updated_at
        {
            return Err(RuntimeError::Conflict(
                "operator evidence names a different native archive intent".into(),
            ));
        }
        let state = restore_state(&tx, &command.agent_id)?;
        if state.workspace_id != command.workspace_id
            || state.workspace_lifecycle != "active"
            || state.agent_lifecycle != "active"
            || state.session_status != "ready"
            || state.active_turn_id.is_some()
            || state.revision
                != i64::try_from(input.expected_revision).map_err(|_| {
                    RuntimeError::InvalidState("invalid archive review revision".into())
                })?
            || state.provider != input.provider
            || state.policy.provider.as_str() != input.provider
            || state.provider_session_ref.as_deref()
                != Some(input.original_provider_session_ref.as_str())
            || state.canonical_provider_session_ref != input.original_canonical_provider_session_ref
            || state.session_updated_at != input.session_updated_at
            || state.cwd.as_deref() != Some(state.policy.authoritative_cwd.as_str())
            || state.model != state.policy.model
        {
            return Err(RuntimeError::Conflict(
                "review target changed since native close intent".into(),
            ));
        }
        let retirement_frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking archive review retirement freeze", error))?;
        if retirement_frozen {
            return Err(RuntimeError::Conflict(
                "workspace is frozen by retirement".into(),
            ));
        }
        let (generation, acquired_at): (i64, i64) = tx.query_row(
            "SELECT fence_generation,acquired_at FROM runtime_operation_resource_claims
             WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3 AND claim_mode='exclusive'",
            params![CLAIM_KIND, command.agent_id, command.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional().map_err(|error| db_error("failed reading archive review fence", error))?
            .ok_or_else(|| RuntimeError::Conflict("archive review no longer owns native claim".into()))?;
        let claim = OperationResourceClaimRecord {
            resource_kind: CLAIM_KIND.into(),
            resource_id: command.agent_id.clone(),
            claim_mode: "exclusive".into(),
            owner_operation_id: command.operation_id.clone(),
            fence_generation: u64::try_from(generation).map_err(|_| {
                RuntimeError::Bootstrap("invalid native archive fence generation".into())
            })?,
            acquired_at,
        };
        let closed = input.resolution == OperatorAgentArchiveResolution::Closed;
        let changed_at = command
            .reviewed_at
            .max(state.session_updated_at.saturating_add(1));
        let mut session = restored_session_snapshot(&tx, &command.agent_id)?;
        let (new_revision, terminal_result) = if closed {
            session.status = "closed".into();
            session.closed_at = Some(changed_at);
            session.active_turn_id = None;
            session.updated_at = changed_at;
            let reason = record.normalized_request["reason"].as_str();
            let updated = apply_workspace_agent_lifecycle_tx(
                &tx,
                &session,
                &command.agent_id,
                WorkspaceAgentLifecycleState::Archived,
                reason,
                changed_at,
                None,
                Some(&command.operation_id),
            )?;
            (
                updated.revision,
                serde_json::to_value(updated).map_err(|error| {
                    RuntimeError::Bootstrap(format!("invalid archive review result: {error}"))
                })?,
            )
        } else {
            session.status = "archive_recovery_required".into();
            session.provider_session_ref = input.observed_provider_session_ref.clone();
            session.canonical_provider_session_ref =
                input.observed_canonical_provider_session_ref.clone();
            session.closed_at = None;
            session.active_turn_id = None;
            session.failure_code = Some("operator_verified_archive_reattachment_pending".into());
            session.failure_message = Some(
                "independently verified provider remained open; require fresh startup reattachment"
                    .into(),
            );
            session.updated_at = changed_at;
            update_session(&tx, &session)?;
            (
                input.expected_revision,
                json!({
                    "operation_id":command.operation_id,
                    "code":"operator_verified_not_closed",
                    "agent_revision":input.expected_revision,
                    "restart_required":true,
                }),
            )
        };
        let response = OperatorAgentArchiveReviewResponse {
            operation_id: command.operation_id.clone(),
            agent_id: command.agent_id.clone(),
            resolution: input.resolution,
            agent_revision: new_revision,
            restart_required: !closed,
            reviewed_at_ms: command.reviewed_at,
        };
        let review = json!({
            "input": input_json, "response": response,
            "actor": command.actor, "reviewed_at_ms": command.reviewed_at,
        });
        let effect_update = tx.execute(
            "UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?2,updated_at=?3
             WHERE operation_id=?1 AND effect_kind=?4 AND phase='uncertain'",
            params![command.operation_id, json_to_string(&json!({
                "original_intent": effect, "operator_review": review,
            }))?, changed_at, EFFECT_KIND],
        ).map_err(|error| db_error("failed recording exact independent archive evidence", error))?;
        if effect_update != 1 {
            return Err(RuntimeError::Conflict(
                "native archive review effect raced".into(),
            ));
        }
        let final_phase = if closed {
            OperationPhase::Completed
        } else {
            OperationPhase::Failed
        };
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            &command.operation_id,
            OperationPhase::ManualReview,
            final_phase,
            Some(&command.workspace_id),
            &terminal_result,
            (!closed).then_some("operator_verified_not_closed"),
            (!closed)
                .then_some("operator independently verified original native close never occurred"),
            changed_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 3,
            from_phase: Some(OperationPhase::ManualReview),
            to_phase: final_phase,
            evidence: Some(json!({
                "review_resolution":input.resolution,
                "restart_required":!closed,
            })),
            created_at: changed_at,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing exact native archive review", error))?;
        Ok(response)
    }
}
