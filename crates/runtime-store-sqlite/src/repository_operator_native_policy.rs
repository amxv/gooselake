use runtime_core::{
    OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord,
    OperationTransitionRecord, OperatorNativePolicyAdmission, OperatorNativePolicyCommand,
    OperatorNativePolicyKind, RuntimeError, WorkspaceAgentRecreationPolicy,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_turn_authority::OPERATOR_TURN_CLAIM;
use crate::repository_workspace::operation_by_idempotency;
use crate::SqliteRuntimeRepository;

pub(crate) const CLAIM_KIND: &str = "operator_provider_policy_native";
pub(crate) const EFFECT_KIND: &str = "provider_session_policy_mutation";
pub(crate) const OPERATION_KIND: &str = "operator_provider_policy_mutation";

fn classify(
    existing: OperationRecord,
    agent_id: &str,
    kind: OperatorNativePolicyKind,
    hash: &str,
) -> Result<OperatorNativePolicyAdmission, RuntimeError> {
    if existing.kind != OPERATION_KIND
        || existing.normalized_request_hash != hash
        || existing.normalized_request["agent_id"] != agent_id
        || existing.normalized_request["kind"] != kind.as_str()
    {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key belongs to operation {} with different input",
            existing.operation_id
        )));
    }
    match (existing.phase, existing.exact_terminal_result) {
        (OperationPhase::Completed, Some(result)) => Ok(OperatorNativePolicyAdmission::Replay {
            operation_id: existing.operation_id,
            exact_result: result,
        }),
        (OperationPhase::Failed, _)
            if existing.error_code.as_deref() == Some("operator_verified_not_applied") =>
        {
            Ok(OperatorNativePolicyAdmission::VerifiedNotApplied {
                operation_id: existing.operation_id,
            })
        }
        _ => Ok(OperatorNativePolicyAdmission::Unresolved {
            operation_id: existing.operation_id,
        }),
    }
}

impl SqliteRuntimeRepository {
    pub fn unresolved_operator_native_policy_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",
                params![CLAIM_KIND, agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading unresolved provider policy claim", error))
    }

    pub fn replay_operator_native_policy(
        &self,
        actor: &OperationActor,
        key: &str,
        agent_id: &str,
        kind: OperatorNativePolicyKind,
        hash: &str,
    ) -> Result<Option<OperatorNativePolicyAdmission>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        operation_by_idempotency(&connection, actor.kind, &actor.identifier, Some(key))?
            .map(|existing| classify(existing, agent_id, kind, hash))
            .transpose()
    }

    /// An IMMEDIATE transaction atomically binds the caller's key, immutable
    /// input, exact native session, exclusive agent claim and effect intent.
    /// No provider method may run before this transaction commits.
    pub fn admit_operator_native_policy(
        &self,
        command: &OperatorNativePolicyCommand,
    ) -> Result<OperatorNativePolicyAdmission, RuntimeError> {
        if command.actor.kind != OperationActorKind::Operator
            || command.actor.identifier.trim().is_empty()
            || command.idempotency_key.is_empty()
            || command.idempotency_key.trim() != command.idempotency_key
            || command.idempotency_key.len() > 512
            || command.agent_id.is_empty()
            || command.workspace_id.is_empty()
            || command.provider.is_empty()
            || command.operation_id.is_empty()
            || command.normalized_request["agent_id"] != command.agent_id
            || command.normalized_request["kind"] != command.kind.as_str()
            || command.normalized_request["expected_revision"] != command.expected_revision
            || command.normalized_request_hash
                != runtime_core::normalized_json_hash(&command.normalized_request)?
        {
            return Err(RuntimeError::InvalidState(
                "invalid trusted provider policy operation identity".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting provider policy admission", error))?;
        if let Some(existing) = operation_by_idempotency(
            &transaction,
            command.actor.kind,
            &command.actor.identifier,
            Some(&command.idempotency_key),
        )? {
            return classify(
                existing,
                &command.agent_id,
                command.kind,
                &command.normalized_request_hash,
            );
        }

        let state: Option<(
            String,
            String,
            String,
            i64,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
            Option<String>,
        )> = transaction
            .query_row(
                "SELECT a.workspace_id, a.lifecycle_state, w.lifecycle_state, a.revision,
                        s.provider, s.status, a.recreation_policy_json,
                        s.provider_session_ref, s.canonical_provider_session_ref,
                        s.updated_at, s.active_turn_id
                 FROM workspace_agents a JOIN workspaces w ON w.workspace_id=a.workspace_id
                 JOIN sessions s ON s.id=a.session_id WHERE a.session_id=?1",
                [&command.agent_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| db_error("failed verifying native policy agent authority", error))?;
        let Some((
            workspace_id,
            lifecycle,
            workspace_state,
            revision,
            provider,
            session_status,
            policy_json,
            session_ref,
            canonical_ref,
            updated_at,
            active_turn,
        )) = state
        else {
            return Err(RuntimeError::NotFound(format!(
                "workspace agent {}",
                command.agent_id
            )));
        };
        let policy: WorkspaceAgentRecreationPolicy =
            serde_json::from_str(&policy_json).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid durable agent policy: {error}"))
            })?;
        if workspace_id != command.workspace_id
            || lifecycle != "active"
            || workspace_state != "active"
            || matches!(
                session_status.as_str(),
                "closed"
                    | "failed"
                    | "policy_recovery_required"
                    | "restore_recovery_required"
                    | "archive_recovery_required"
            )
            || active_turn.is_some()
            || revision
                != i64::try_from(command.expected_revision)
                    .map_err(|_| RuntimeError::InvalidState("revision overflow".into()))?
            || provider != command.provider
            || policy.provider.as_str() != command.provider
            || session_ref != command.provider_session_ref
            || canonical_ref != command.canonical_provider_session_ref
            || updated_at != command.session_updated_at
        {
            return Err(RuntimeError::Conflict(
                "native provider policy session identity or agent revision changed before admission".into(),
            ));
        }
        let frozen: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking provider policy retirement fence", error))?;
        if frozen {
            return Err(RuntimeError::Conflict(
                "workspace is frozen by retirement".into(),
            ));
        }
        let native_blockers: i64 = transaction.query_row(
            "SELECT (SELECT COUNT(*) FROM workspace_agent_rebinds WHERE agent_id=?1 AND phase IN ('intended','manual_review'))
                  + (SELECT COUNT(*) FROM session_compaction_runs WHERE session_id=?1 AND phase IN ('intended','accepted','started','compacting','manual_review'))
                  + (SELECT COUNT(*) FROM session_edit_reruns WHERE session_id=?1 AND phase IN ('intended','native_started','fork_observed','manual_review'))
                  + (SELECT COUNT(*) FROM runtime_operation_resource_claims
                     WHERE resource_id=?1 AND claim_mode='exclusive' AND resource_kind IN (?2,?3,?4,?5))",
            params![command.agent_id, CLAIM_KIND, OPERATOR_TURN_CLAIM,
                crate::repository_operator_agent_restore::CLAIM_KIND,
                crate::repository_operator_agent_archive::CLAIM_KIND],
            |row| row.get(0),
        ).map_err(|error| db_error("failed checking competing provider native work", error))?;
        if native_blockers != 0 {
            return Err(RuntimeError::Conflict(
                "another unresolved native provider effect owns the agent; inspect its operation"
                    .into(),
            ));
        }

        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: OPERATION_KIND.into(),
            actor: command.actor.clone(),
            idempotency_key: Some(command.idempotency_key.clone()),
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
            evidence: Some(serde_json::json!({"native_policy": command.kind.as_str()})),
            created_at: command.requested_at,
        })?;
        authority.acquire_claim(
            &command.operation_id,
            CLAIM_KIND,
            &command.agent_id,
            command.requested_at,
        )?;
        authority.insert_effect(&OperationEffectRecord {
            effect_id: format!("fx_{}", command.operation_id),
            operation_id: command.operation_id.clone(),
            effect_kind: EFFECT_KIND.into(),
            target_kind: "provider_session".into(),
            target_id: command.agent_id.clone(),
            phase: "intended".into(),
            idempotency_key: "exact_native_provider_session".into(),
            evidence: Some(serde_json::json!({
                "kind":command.kind.as_str(), "provider":command.provider,
                "provider_session_ref":command.provider_session_ref,
                "canonical_provider_session_ref":command.canonical_provider_session_ref,
                "session_updated_at":command.session_updated_at,
                "expected_revision":command.expected_revision,
            })),
            attempt_count: 0,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing native policy intent", error))?;
        Ok(OperatorNativePolicyAdmission::Execute {
            operation_id: command.operation_id.clone(),
        })
    }
}
