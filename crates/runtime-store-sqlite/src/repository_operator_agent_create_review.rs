use runtime_core::{
    OperationActorKind, OperationPhase, OperationTransitionRecord, OperatorAgentCreateCommand,
    OperatorAgentCreateResolution, OperatorAgentCreateReviewCommand,
    OperatorAgentCreateReviewResponse, RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_agent_create::{
    read_frozen_create_plan, EFFECT_KIND, OPERATION_KIND,
};
use crate::repository_operator_agent_create_receipt::exact_create_claim;
use crate::repository_workspace::operation_by_id;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Evidence-only reconciliation for one opaque reserved agent identity.
    /// Never call Git, shell hooks or the provider from here; an operator must
    /// independently verify native session identity. Created sessions remain
    /// fenced until a fresh startup verifies exact native reattachment.
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
                "native agent creation review requires independently verified 8–2048-byte evidence and exact native identity".into(),
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
        if record.kind != OPERATION_KIND
            || record.actor != command.actor
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
        let admitted = OperatorAgentCreateCommand {
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
        };
        let claim = exact_create_claim(&tx, &admitted)?;
        let created = input.resolution == OperatorAgentCreateResolution::Created;
        let (exact, restart_required) = if created {
            // Without a durably frozen launch policy, claiming a native
            // attachment is not enough to reconstruct an authorized agent.
            let snapshot = read_frozen_create_plan(&tx, &admitted)?.ok_or_else(|| {
                RuntimeError::Conflict(
                    "no durable pre-provider launch snapshot; cannot reconcile a created agent"
                        .into(),
                )
            })?;
            let mut session = snapshot.session;
            let mut agent = snapshot.agent;
            session.provider_session_ref = input.observed_provider_session_ref.clone();
            session.canonical_provider_session_ref =
                input.observed_canonical_provider_session_ref.clone();
            session.status = "create_recovery_required".into();
            session.closed_at = None;
            session.failure_code = Some("operator_verified_create_reattachment_pending".into());
            session.failure_message = Some(
                "operator verified native creation; fresh exact provider reattachment required"
                    .into(),
            );
            session.updated_at = command
                .reviewed_at
                .max(session.created_at.saturating_add(1));
            agent.provider_session_ref = session.provider_session_ref.clone();
            agent.canonical_provider_session_ref = session.canonical_provider_session_ref.clone();
            agent.updated_at = session.updated_at;
            self.persist_workspace_agent_create_tx(
                &tx,
                &session,
                &agent,
                Some(&snapshot.onboarding),
                snapshot.hooks.as_ref(),
                Some(&command.operation_id),
            )?;
            let exact = serde_json::to_value(agent).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid reviewed agent creation: {error}"))
            })?;
            (exact, true)
        } else {
            // A previously completed child Git checkout is intentionally NOT
            // deleted as part of declaring this provider session absent.
            // Its independent operation/worktree ID remains inspectable.
            // A successfully executed pre-add script is historical evidence,
            // but its unconsumed output cannot ever target an agent whose
            // native creation was verified absent. Settle only that exact
            // reserved member's context application, retaining the script
            // receipt itself for auditing and Git/manual cleanup.
            tx.execute(
                "UPDATE workspace_hook_effects
                 SET application_state='rejected',
                     application_error_code='operator_verified_agent_not_created',
                     updated_at_ms=?3
                 WHERE workspace_id=?1 AND event_name='pre_teammate_add'
                   AND application_state='pending'
                   AND json_extract(input_json,'$.subject.member_agent_id')=?2",
                params![
                    command.workspace_id,
                    input.reserved_agent_id,
                    command.reviewed_at
                ],
            )
            .map_err(|error| {
                db_error(
                    "failed settling orphaned pre-add context for absent native agent",
                    error,
                )
            })?;
            (
                json!({"operation_id":command.operation_id,
                    "code":"operator_verified_not_created",
                    "reserved_agent_id":input.reserved_agent_id}),
                false,
            )
        };
        let final_phase = if created {
            OperationPhase::Completed
        } else {
            OperationPhase::Failed
        };
        let response = OperatorAgentCreateReviewResponse {
            operation_id: command.operation_id.clone(),
            reserved_agent_id: input.reserved_agent_id.clone(),
            resolution: input.resolution,
            restart_required,
            reviewed_at_ms: command.reviewed_at,
        };
        let review = json!({
            "input":request_json, "response":response,
            "actor":command.actor, "reviewed_at_ms":command.reviewed_at,
        });
        let effect = json!({
            "reserved_agent_id":input.reserved_agent_id,
            "provider":input.provider,
            "original_intent":original_effect,
            "operator_review":review,
        });
        let changed = tx.execute(
            "UPDATE runtime_operation_effects SET phase='finalized',updated_at=?2,evidence_json=?3
             WHERE operation_id=?1 AND effect_kind=?4 AND phase='uncertain'",
            params![command.operation_id,command.reviewed_at,json_to_string(&effect)?,EFFECT_KIND],
        ).map_err(|error| db_error("failed persisting independent native create evidence", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "creation evidence was already terminalized".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            &command.operation_id,
            OperationPhase::ManualReview,
            final_phase,
            Some(&command.workspace_id),
            &exact,
            (!created).then_some("operator_verified_not_created"),
            (!created).then_some(
                "operator independently verified native provider session was never created",
            ),
            command.reviewed_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 3,
            from_phase: Some(OperationPhase::ManualReview),
            to_phase: final_phase,
            evidence: Some(
                json!({"review_resolution":input.resolution,"restart_required":restart_required}),
            ),
            created_at: command.reviewed_at,
        })?;
        authority.release_claim(&claim)?;
        tx.commit().map_err(|error| {
            db_error(
                "failed committing independently verified agent creation",
                error,
            )
        })?;
        Ok(response)
    }
}
