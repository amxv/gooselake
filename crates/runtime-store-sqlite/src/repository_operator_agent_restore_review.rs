use runtime_core::{
    OperationActorKind, OperationPhase, OperationResourceClaimRecord,
    OperatorAgentRestoreResolution, OperatorAgentRestoreReviewCommand,
    OperatorAgentRestoreReviewResponse, RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, open_connection};
use crate::repository_operator_agent_restore::{
    restore_state, CLAIM_KIND, EFFECT_KIND, OPERATION_KIND,
};
use crate::repository_workspace::operation_by_id;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn operator_agent_restore_reattachment_pending(
        &self,
        agent_id: &str,
    ) -> Result<bool, RuntimeError> {
        let conn = open_connection(&self.database_path)?;
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1 AND status='restore_recovery_required')",
            [agent_id], |row| row.get(0),
        ).map_err(|error| db_error("failed checking reviewed native restore reattachment", error))
    }

    /// Return an exact retained historical review without creating new authority.
    /// Fresh prose preserves the original uncertain effect and its native claim.
    pub fn resolve_operator_agent_restore_review(
        &self,
        command: &OperatorAgentRestoreReviewCommand,
    ) -> Result<OperatorAgentRestoreReviewResponse, RuntimeError> {
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
            || (input.resolution == OperatorAgentRestoreResolution::Resumed
                && input
                    .observed_provider_session_ref
                    .as_deref()
                    .is_none_or(|v| v.trim().is_empty()))
            || (input.resolution == OperatorAgentRestoreResolution::NotResumed
                && (input.observed_provider_session_ref.is_some()
                    || input.observed_canonical_provider_session_ref.is_some()))
        {
            return Err(RuntimeError::InvalidState(
                "operator restore review requires exact original attachment and 8–2048 evidence bytes".into(),
            ));
        }
        let mut normalized_input = input.clone();
        normalized_input.evidence = evidence.into();
        let input_json = serde_json::to_value(&normalized_input)
            .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting independent restore review", error))?;
        let record = operation_by_id(&tx, &command.operation_id)?.ok_or_else(|| {
            RuntimeError::NotFound(format!("restore operation {}", command.operation_id))
        })?;
        if record.kind != OPERATION_KIND
            || record.actor != command.actor
            || record.workspace_id.as_deref() != Some(&command.workspace_id)
            || record.normalized_request["workspace_id"] != command.workspace_id
            || record.normalized_request["agent_id"] != command.agent_id
            || record.normalized_request["action"] != "restore"
            || runtime_core::normalized_json_hash(&record.normalized_request)?
                != record.normalized_request_hash
        {
            return Err(RuntimeError::Conflict(
                "restore review does not match immutable operator authority".into(),
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
            .map_err(|error| db_error("failed loading exact restore review target", error))?
            .ok_or_else(|| {
                RuntimeError::Conflict("no native effect exists for this restoration".into())
            })?;
        let effect: Value = serde_json::from_str(&raw_effect).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid stored restore effect: {error}"))
        })?;
        if matches!(
            record.phase,
            OperationPhase::Completed | OperationPhase::Failed
        ) {
            let review = &effect["operator_review"];
            if phase == "finalized" && review["input"] == input_json {
                return serde_json::from_value(review["response"].clone()).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "invalid stored restore review receipt: {error}"
                    ))
                });
            }
            return Err(RuntimeError::Conflict(
                "restore operation has a different terminal result".into(),
            ));
        }
        if record.phase != OperationPhase::ManualReview || phase != "uncertain" {
            return Err(RuntimeError::Conflict(
                "only quarantined native restores accept independent review".into(),
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
                "operator evidence names a different native restore intent".into(),
            ));
        }

        let state = restore_state(&tx, &command.agent_id)?;
        if state.workspace_id != command.workspace_id
            || state.workspace_lifecycle != "active"
            || state.agent_lifecycle != "archived"
            || state.session_status != "closed"
            || state.active_turn_id.is_some()
            || state.revision
                != i64::try_from(input.expected_revision).map_err(|_| {
                    RuntimeError::InvalidState("invalid restore review revision".into())
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
                "review target changed since the native restore intent".into(),
            ));
        }
        let retirement_frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking restore review retirement freeze", error))?;
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
        ).optional().map_err(|error| db_error("failed reading restore review fence", error))?
            .ok_or_else(|| RuntimeError::Conflict("restore review no longer owns native claim".into()))?;
        let _claim = OperationResourceClaimRecord {
            resource_kind: CLAIM_KIND.into(),
            resource_id: command.agent_id.clone(),
            claim_mode: "exclusive".into(),
            owner_operation_id: command.operation_id.clone(),
            fence_generation: u64::try_from(generation).map_err(|_| {
                RuntimeError::Bootstrap("invalid native restore fence generation".into())
            })?,
            acquired_at,
        };

        Err(RuntimeError::operation_unresolved(
            command.operation_id.clone(),
            "prose assertions cannot settle an uncertain native restore; inspect the original operation; native retry is not authorized",
        ))
    }
}
