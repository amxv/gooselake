use runtime_core::{
    OperationActorKind, OperationPhase, OperatorAgentCreateCommand, OperatorAgentCreateResolution,
    OperatorAgentCreateReviewCommand, OperatorAgentCreateReviewResponse, RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::Value;

use crate::db::{db_error, open_connection};
use crate::repository_operator_agent_create::{
    read_frozen_create_plan, EFFECT_KIND, OPERATION_KIND,
};
use crate::repository_operator_agent_create_receipt::exact_create_claim;
use crate::repository_workspace::operation_by_id;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Inspect exact historical review receipts. Fresh prose cannot settle an
    /// uncertain create, reconstruct an agent, consume hook output, or release claims.
    pub fn resolve_operator_agent_create_review(
        &self,
        command: &OperatorAgentCreateReviewCommand,
    ) -> Result<OperatorAgentCreateReviewResponse, RuntimeError> {
        let input = &command.input;
        let evidence = input.evidence.trim();
        if command.actor.kind != OperationActorKind::Operator
            || command.actor.identifier.trim().is_empty()
            || command.operation_id.trim().is_empty()
            || command.workspace_id.trim().is_empty()
            || input.reserved_agent_id.trim().is_empty()
            || input.provider.trim().is_empty()
            || !(8..=2048).contains(&evidence.len())
            || (input.resolution == OperatorAgentCreateResolution::Created
                && input
                    .observed_provider_session_ref
                    .as_deref()
                    .is_none_or(|v| v.trim().is_empty()))
            || (input.resolution == OperatorAgentCreateResolution::NotCreated
                && (input.observed_provider_session_ref.is_some()
                    || input.observed_canonical_provider_session_ref.is_some()))
        {
            return Err(RuntimeError::InvalidState(
                "native agent creation review requires 8–2048 evidence bytes and exact native identity".into(),
            ));
        }
        let mut normalized = input.clone();
        normalized.evidence = evidence.into();
        let request_json = serde_json::to_value(&normalized)
            .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;

        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native create evidence review", error))?;
        let record = operation_by_id(&tx, &command.operation_id)?.ok_or_else(|| {
            RuntimeError::NotFound(format!(
                "native creation operation {}",
                command.operation_id
            ))
        })?;
        let member = crate::repository_member_agent_create::command_for_child(&tx, &record)?;
        if record.kind != OPERATION_KIND
            || (record.actor != command.actor && member.is_none())
            || record.workspace_id.as_deref() != Some(&command.workspace_id)
            || record.normalized_request["workspace_id"] != command.workspace_id
            || record.normalized_request["action"] != "create"
            || record.normalized_request["request"]["provider"] != input.provider
            || runtime_core::normalized_json_hash(&record.normalized_request)?
                != record.normalized_request_hash
        {
            return Err(RuntimeError::Conflict(
                "native creation review does not match immutable operator request".into(),
            ));
        }
        let (effect_phase, raw_effect): (String, String) = tx
            .query_row(
                "SELECT phase,evidence_json FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3",
                params![command.operation_id, EFFECT_KIND, input.reserved_agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed loading exact reserved native create effect", error))?
            .ok_or_else(|| {
                RuntimeError::Conflict("requested native session was never reserved".into())
            })?;
        let original_effect: Value = serde_json::from_str(&raw_effect).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid native create effect evidence: {error}"))
        })?;
        if original_effect["reserved_agent_id"] != input.reserved_agent_id
            || original_effect["provider"] != input.provider
        {
            return Err(RuntimeError::Conflict(
                "independent evidence names a different reserved agent".into(),
            ));
        }
        if matches!(
            record.phase,
            OperationPhase::Completed | OperationPhase::Failed
        ) {
            let stored_review = &original_effect["operator_review"];
            if effect_phase == "finalized" && stored_review["input"] == request_json {
                return serde_json::from_value(stored_review["response"].clone()).map_err(
                    |error| {
                        RuntimeError::Bootstrap(format!(
                            "corrupt exact native create review replay: {error}"
                        ))
                    },
                );
            }
            return Err(RuntimeError::Conflict(
                "native agent creation already has a different terminal receipt".into(),
            ));
        }
        if record.phase != OperationPhase::ManualReview || effect_phase != "uncertain" {
            return Err(RuntimeError::Conflict(
                "only a quarantined native agent creation accepts operator evidence".into(),
            ));
        }
        let frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed checking creation review retirement authority",
                    error,
                )
            })?;
        let active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id=?1 AND lifecycle_state='active')",
            [&command.workspace_id],|row| row.get(0),
        ).map_err(|error| db_error("failed checking native create review workspace", error))?;
        if frozen || !active {
            return Err(RuntimeError::Conflict(
                "cannot settle native creation inside a retired/frozen workspace".into(),
            ));
        }
        let admitted = member
            .as_ref()
            .map(|member| member.operation.clone())
            .unwrap_or(OperatorAgentCreateCommand {
                operation_id: record.operation_id.clone(),
                actor: record.actor.clone(),
                idempotency_key: record.idempotency_key.clone().ok_or_else(|| {
                    RuntimeError::Bootstrap("native create lost its immutable key".into())
                })?,
                workspace_id: command.workspace_id.clone(),
                reserved_agent_id: input.reserved_agent_id.clone(),
                normalized_request: record.normalized_request.clone(),
                normalized_request_hash: record.normalized_request_hash.clone(),
                requested_at: record.created_at,
                worktree_operation_id: None,
            });
        exact_create_claim(&tx, &admitted)?;
        if input.resolution == OperatorAgentCreateResolution::Created {
            if let Some(member) = member.as_ref() {
                crate::repository_member_agent_create_routes::validate_route(&tx, member, true)?;
            }
            read_frozen_create_plan(&tx, &admitted)?.ok_or_else(|| {
                RuntimeError::Conflict(
                    "no durable pre-provider launch snapshot; cannot reconcile a created agent"
                        .into(),
                )
            })?;
        }
        Err(RuntimeError::operation_unresolved(
            command.operation_id.clone(),
            "prose assertions cannot settle uncertain native creation; inspect the original operation; native retry is not authorized",
        ))
    }
}
