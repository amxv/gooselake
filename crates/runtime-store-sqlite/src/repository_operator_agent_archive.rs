use runtime_core::{
    OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord,
    OperationTransitionRecord, OperatorAgentArchiveAdmission, OperatorAgentArchiveCommand,
    RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_agent_restore::{restore_state, RestoreState};
use crate::repository_operator_native_policy::CLAIM_KIND as POLICY_CLAIM;
use crate::repository_turn_authority::OPERATOR_TURN_CLAIM;
use crate::repository_workspace::operation_by_idempotency;
use crate::repository_workspace_agent::{workspace_agent_from_row, AGENT_SELECT};
use crate::SqliteRuntimeRepository;

pub(crate) const CLAIM_KIND: &str = "operator_agent_archive_native";
pub(crate) const EFFECT_KIND: &str = "provider_session_close";
pub(crate) const OPERATION_KIND: &str = "operator_agent_archive";

pub(crate) fn assert_archive_identity(
    state: &RestoreState,
    command: &OperatorAgentArchiveCommand,
    expected_lifecycle: &str,
    expected_session_status: &str,
) -> Result<(), RuntimeError> {
    if state.workspace_id != command.workspace_id
        || state.workspace_lifecycle != "active"
        || state.agent_lifecycle != expected_lifecycle
        || state.session_status != expected_session_status
        || state.active_turn_id.is_some()
        || state.revision
            != i64::try_from(command.expected_revision)
                .map_err(|_| RuntimeError::InvalidState("invalid expected agent revision".into()))?
        || state.provider != command.provider
        || state.policy.provider.as_str() != command.provider
        || state.provider_session_ref.as_deref() != Some(command.provider_session_ref.as_str())
        || state.canonical_provider_session_ref != command.canonical_provider_session_ref
        || state.session_updated_at != command.session_updated_at
        || state.cwd.as_deref() != Some(state.policy.authoritative_cwd.as_str())
        || state.model != state.policy.model
    {
        return Err(RuntimeError::Conflict(
            "operator archive native attachment, agent revision or workspace authority changed"
                .into(),
        ));
    }
    Ok(())
}

fn classify_archive(
    operation: OperationRecord,
    workspace_id: &str,
    agent_id: &str,
    hash: &str,
) -> Result<OperatorAgentArchiveAdmission, RuntimeError> {
    if operation.kind != OPERATION_KIND
        || operation.workspace_id.as_deref() != Some(workspace_id)
        || operation.normalized_request_hash != hash
        || operation.normalized_request["workspace_id"] != workspace_id
        || operation.normalized_request["agent_id"] != agent_id
        || operation.normalized_request["action"] != "archive"
    {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key is owned by operation {} for different input",
            operation.operation_id
        )));
    }
    match (operation.phase, operation.exact_terminal_result) {
        (OperationPhase::Completed, Some(exact)) => {
            let exact_result = serde_json::from_value(exact).map_err(|error| {
                RuntimeError::Bootstrap(format!(
                    "invalid stored exact agent archive receipt: {error}"
                ))
            })?;
            Ok(OperatorAgentArchiveAdmission::Replay {
                operation_id: operation.operation_id,
                exact_result,
            })
        }
        (OperationPhase::Failed, _)
            if operation.error_code.as_deref() == Some("operator_verified_not_closed") =>
        {
            Ok(OperatorAgentArchiveAdmission::VerifiedNotClosed {
                operation_id: operation.operation_id,
            })
        }
        _ => Ok(OperatorAgentArchiveAdmission::Unresolved {
            operation_id: operation.operation_id,
        }),
    }
}

impl SqliteRuntimeRepository {
    pub fn unresolved_operator_agent_archive_for_agent(
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
            .map_err(|error| db_error("failed reading unresolved native agent archive", error))
    }

    pub fn replay_operator_agent_archive(
        &self,
        actor: &OperationActor,
        key: &str,
        workspace_id: &str,
        agent_id: &str,
        hash: &str,
    ) -> Result<Option<OperatorAgentArchiveAdmission>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        operation_by_idempotency(&connection, actor.kind, &actor.identifier, Some(key))?
            .map(|record| classify_archive(record, workspace_id, agent_id, hash))
            .transpose()
    }

    pub fn admit_operator_agent_archive(
        &self,
        command: &OperatorAgentArchiveCommand,
    ) -> Result<OperatorAgentArchiveAdmission, RuntimeError> {
        if command.actor.kind != OperationActorKind::Operator
            || command.actor.identifier.trim().is_empty()
            || command.operation_id.trim().is_empty()
            || command.workspace_id.trim().is_empty()
            || command.agent_id.trim().is_empty()
            || command.idempotency_key.is_empty()
            || command.idempotency_key.len() > 512
            || command.idempotency_key.trim() != command.idempotency_key
            || !command
                .idempotency_key
                .bytes()
                .all(|byte| (32..=126).contains(&byte) && byte != b',')
            || command.provider_session_ref.trim().is_empty()
            || command.normalized_request["workspace_id"] != command.workspace_id
            || command.normalized_request["agent_id"] != command.agent_id
            || command.normalized_request["action"] != "archive"
            || command.normalized_request["reason"] != serde_json::json!(command.reason)
            || runtime_core::normalized_json_hash(&command.normalized_request)?
                != command.normalized_request_hash
        {
            return Err(RuntimeError::InvalidState(
                "invalid operator native archive identity".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting operator agent archive admission", error))?;
        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            Some(&command.idempotency_key),
        )? {
            return classify_archive(
                existing,
                &command.workspace_id,
                &command.agent_id,
                &command.normalized_request_hash,
            );
        }
        let state = restore_state(&tx, &command.agent_id)?;
        if state.workspace_id != command.workspace_id || state.workspace_lifecycle != "active" {
            return Err(RuntimeError::Conflict(
                "workspace is not active for agent archival".into(),
            ));
        }
        let retirement_frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking native archive retirement fence", error))?;
        if retirement_frozen {
            return Err(RuntimeError::Conflict(
                "workspace is frozen by retirement".into(),
            ));
        }
        let blockers: i64 = tx.query_row(
            "SELECT (SELECT COUNT(*) FROM workspace_agent_rebinds WHERE agent_id=?1 AND phase IN ('intended','manual_review'))
                  + (SELECT COUNT(*) FROM session_compaction_runs WHERE session_id=?1 AND phase IN ('intended','accepted','started','compacting','manual_review'))
                  + (SELECT COUNT(*) FROM session_edit_reruns WHERE session_id=?1 AND phase IN ('intended','native_started','fork_observed','manual_review'))
                  + (SELECT COUNT(*) FROM runtime_operation_resource_claims
                     WHERE resource_id=?1 AND claim_mode='exclusive' AND resource_kind IN (?2,?3,?4,?5))",
            params![command.agent_id, POLICY_CLAIM, OPERATOR_TURN_CLAIM,
                crate::repository_operator_agent_restore::CLAIM_KIND, CLAIM_KIND],
            |row| row.get(0),
        ).map_err(|error| db_error("failed checking competing archive native effects", error))?;
        if blockers > 0
            || matches!(
                state.session_status.as_str(),
                "policy_recovery_required"
                    | "restore_recovery_required"
                    | "archive_recovery_required"
            )
        {
            return Err(RuntimeError::Conflict(
                "agent has unresolved native provider attachment or pending reattachment".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&tx);
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
            evidence: Some(serde_json::json!({"agent_archive":"admitted"})),
            created_at: command.requested_at,
        })?;

        // Do not claim another provider close for an already archived agent.
        // This is a durable no-effect roster receipt, not native proof of the
        // historical archive performed by an older caller.
        if state.agent_lifecycle == "archived" && state.session_status == "closed" {
            if state.revision
                != i64::try_from(command.expected_revision)
                    .map_err(|_| RuntimeError::InvalidState("invalid agent revision".into()))?
                || state.provider != command.provider
                || state.provider_session_ref.as_deref()
                    != Some(command.provider_session_ref.as_str())
                || state.canonical_provider_session_ref != command.canonical_provider_session_ref
                || state.session_updated_at != command.session_updated_at
            {
                return Err(RuntimeError::Conflict(
                    "no-op archive snapshot changed before admission".into(),
                ));
            }
            let agent = tx
                .query_row(
                    &format!("{AGENT_SELECT} WHERE a.session_id=?1 AND a.workspace_id=?2"),
                    params![command.agent_id, command.workspace_id],
                    workspace_agent_from_row,
                )
                .map_err(|error| db_error("failed reading no-op archive receipt", error))?;
            let receipt = serde_json::to_value(&agent).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid archive receipt: {error}"))
            })?;
            authority.terminalize(
                &command.operation_id,
                OperationPhase::Requested,
                OperationPhase::Completed,
                Some(&command.workspace_id),
                &receipt,
                None,
                None,
                command.requested_at,
            )?;
            authority.append_transition(&OperationTransitionRecord {
                operation_id: command.operation_id.clone(),
                sequence: 2,
                from_phase: Some(OperationPhase::Requested),
                to_phase: OperationPhase::Completed,
                evidence: Some(serde_json::json!({"archive":"already_archived_no_native_call"})),
                created_at: command.requested_at,
            })?;
            tx.commit()
                .map_err(|error| db_error("failed committing exact no-op archive", error))?;
            return Ok(OperatorAgentArchiveAdmission::Replay {
                operation_id: command.operation_id.clone(),
                exact_result: agent,
            });
        }

        assert_archive_identity(&state, command, "active", "ready")?;
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
            idempotency_key: "exact_provider_close".into(),
            evidence: Some(serde_json::json!({
                "provider":command.provider,
                "original_provider_session_ref":command.provider_session_ref,
                "original_canonical_provider_session_ref":command.canonical_provider_session_ref,
                "expected_revision":command.expected_revision,
                "session_updated_at":command.session_updated_at,
                "archive_reason":command.reason,
            })),
            attempt_count: 0,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing durable archive intent", error))?;
        Ok(OperatorAgentArchiveAdmission::Execute {
            operation_id: command.operation_id.clone(),
        })
    }
}
