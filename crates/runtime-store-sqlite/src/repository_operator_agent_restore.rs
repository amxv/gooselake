use runtime_core::{
    OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord,
    OperationTransitionRecord, OperatorAgentRestoreAdmission, OperatorAgentRestoreCommand,
    RuntimeError, SessionRecord, WorkspaceAgentRecreationPolicy,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection, string_to_json};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_native_policy::CLAIM_KIND as POLICY_CLAIM;
use crate::repository_turn_authority::OPERATOR_TURN_CLAIM;
use crate::repository_workspace::operation_by_idempotency;
use crate::repository_workspace_agent::{workspace_agent_from_row, AGENT_SELECT};
use crate::SqliteRuntimeRepository;

pub(crate) const CLAIM_KIND: &str = "operator_agent_restore_native";
pub(crate) const EFFECT_KIND: &str = "provider_session_restore";
pub(crate) const OPERATION_KIND: &str = "operator_agent_restore";

#[derive(Debug)]
pub(crate) struct RestoreState {
    pub workspace_id: String,
    pub agent_lifecycle: String,
    pub workspace_lifecycle: String,
    pub revision: i64,
    pub provider: String,
    pub session_status: String,
    pub policy: WorkspaceAgentRecreationPolicy,
    pub provider_session_ref: Option<String>,
    pub canonical_provider_session_ref: Option<String>,
    pub session_updated_at: i64,
    pub active_turn_id: Option<String>,
    pub cwd: Option<String>,
    pub model: Option<String>,
}

pub(crate) fn restore_state(
    connection: &Connection,
    agent_id: &str,
) -> Result<RestoreState, RuntimeError> {
    let row: (
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
        Option<String>,
        Option<String>,
    ) = connection
        .query_row(
            "SELECT a.workspace_id,a.lifecycle_state,w.lifecycle_state,a.revision,
                s.provider,s.status,a.recreation_policy_json,s.provider_session_ref,
                s.canonical_provider_session_ref,s.updated_at,s.active_turn_id,s.cwd,s.model
             FROM workspace_agents a JOIN workspaces w ON w.workspace_id=a.workspace_id
             JOIN sessions s ON s.id=a.session_id WHERE a.session_id=?1",
            [agent_id],
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
                    row.get(11)?,
                    row.get(12)?,
                ))
            },
        )
        .optional()
        .map_err(|error| db_error("failed reading exact native restore authority", error))?
        .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
    let policy = serde_json::from_str(&row.6).map_err(|error| {
        RuntimeError::Bootstrap(format!("corrupt durable agent policy: {error}"))
    })?;
    Ok(RestoreState {
        workspace_id: row.0,
        agent_lifecycle: row.1,
        workspace_lifecycle: row.2,
        revision: row.3,
        provider: row.4,
        session_status: row.5,
        policy,
        provider_session_ref: row.7,
        canonical_provider_session_ref: row.8,
        session_updated_at: row.9,
        active_turn_id: row.10,
        cwd: row.11,
        model: row.12,
    })
}

/// Read the complete authoritative session *inside* the same transaction as
/// an independently reviewed restore. Do not use a stale in-memory manager
/// snapshot to overwrite metadata, route or other existing durable fields.
pub(crate) fn restored_session_snapshot(
    connection: &Connection,
    agent_id: &str,
) -> Result<SessionRecord, RuntimeError> {
    connection
        .query_row(
            "SELECT id,provider,status,cwd,model,permission_mode,system_prompt,metadata_json,
                provider_session_ref,canonical_provider_session_ref,active_turn_id,worktree_id,
                created_at,updated_at,closed_at,failure_code,failure_message
         FROM sessions WHERE id=?1",
            [agent_id],
            |row| {
                Ok(SessionRecord {
                    id: row.get(0)?,
                    provider: row.get(1)?,
                    status: row.get(2)?,
                    cwd: row.get(3)?,
                    model: row.get(4)?,
                    permission_mode: row.get(5)?,
                    system_prompt: row.get(6)?,
                    metadata: string_to_json(row.get(7)?)?,
                    provider_session_ref: row.get(8)?,
                    canonical_provider_session_ref: row.get(9)?,
                    active_turn_id: row.get(10)?,
                    worktree_id: row.get(11)?,
                    created_at: row.get(12)?,
                    updated_at: row.get(13)?,
                    closed_at: row.get(14)?,
                    failure_code: row.get(15)?,
                    failure_message: row.get(16)?,
                })
            },
        )
        .optional()
        .map_err(|error| db_error("failed reading exact restoration session", error))?
        .ok_or_else(|| RuntimeError::NotFound(format!("session {agent_id}")))
}

pub(crate) fn assert_restore_identity(
    state: &RestoreState,
    command: &OperatorAgentRestoreCommand,
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
            "operator restore native attachment, agent revision or workspace authority changed"
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn classify_restore(
    operation: OperationRecord,
    workspace_id: &str,
    agent_id: &str,
    hash: &str,
) -> Result<OperatorAgentRestoreAdmission, RuntimeError> {
    if operation.kind != OPERATION_KIND
        || operation.workspace_id.as_deref() != Some(workspace_id)
        || operation.normalized_request_hash != hash
        || operation.normalized_request["workspace_id"] != workspace_id
        || operation.normalized_request["agent_id"] != agent_id
        || operation.normalized_request["action"] != "restore"
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
                    "invalid stored exact agent restore receipt: {error}"
                ))
            })?;
            Ok(OperatorAgentRestoreAdmission::Replay {
                operation_id: operation.operation_id,
                exact_result,
            })
        }
        (OperationPhase::Failed, _)
            if operation.error_code.as_deref() == Some("operator_verified_not_resumed") =>
        {
            Ok(OperatorAgentRestoreAdmission::VerifiedNotResumed {
                operation_id: operation.operation_id,
            })
        }
        _ => Ok(OperatorAgentRestoreAdmission::Unresolved {
            operation_id: operation.operation_id,
        }),
    }
}

impl SqliteRuntimeRepository {
    pub fn unresolved_operator_agent_restore_for_agent(
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
            .map_err(|error| db_error("failed reading unresolved native agent restore", error))
    }

    pub fn replay_operator_agent_restore(
        &self,
        actor: &OperationActor,
        key: &str,
        workspace_id: &str,
        agent_id: &str,
        hash: &str,
    ) -> Result<Option<OperatorAgentRestoreAdmission>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        operation_by_idempotency(&connection, actor.kind, &actor.identifier, Some(key))?
            .map(|record| classify_restore(record, workspace_id, agent_id, hash))
            .transpose()
    }

    pub fn admit_operator_agent_restore(
        &self,
        command: &OperatorAgentRestoreCommand,
    ) -> Result<OperatorAgentRestoreAdmission, RuntimeError> {
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
            || command.normalized_request["action"] != "restore"
            || runtime_core::normalized_json_hash(&command.normalized_request)?
                != command.normalized_request_hash
        {
            return Err(RuntimeError::InvalidState(
                "invalid operator native restore identity".into(),
            ));
        }

        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting operator agent restore admission", error))?;
        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            Some(&command.idempotency_key),
        )? {
            return classify_restore(
                existing,
                &command.workspace_id,
                &command.agent_id,
                &command.normalized_request_hash,
            );
        }

        let state = restore_state(&tx, &command.agent_id)?;
        if state.workspace_id != command.workspace_id || state.workspace_lifecycle != "active" {
            return Err(RuntimeError::Conflict(
                "workspace is not active for agent restoration".into(),
            ));
        }
        let retirement_frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking native restore retirement fence", error))?;
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
                CLAIM_KIND, crate::repository_operator_agent_archive::CLAIM_KIND],
            |row| row.get(0),
        ).map_err(|error| db_error("failed checking competing restore native effects", error))?;
        if blockers > 0
            || state.session_status == "policy_recovery_required"
            || state.session_status == "restore_recovery_required"
            || state.session_status == "archive_recovery_required"
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
            evidence: Some(serde_json::json!({"agent_restore":"admitted"})),
            created_at: command.requested_at,
        })?;

        // A newly keyed restore of an already active agent is a durable no-op,
        // with its own exact receipt. No native provider resume is attempted.
        if state.agent_lifecycle == "active" && state.session_status == "ready" {
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
                    "no-op restore snapshot changed before durable admission".into(),
                ));
            }
            let agent = tx
                .query_row(
                    &format!("{AGENT_SELECT} WHERE a.session_id=?1 AND a.workspace_id=?2"),
                    params![command.agent_id, command.workspace_id],
                    workspace_agent_from_row,
                )
                .map_err(|error| db_error("failed reading no-op restore receipt", error))?;
            let receipt = serde_json::to_value(&agent).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid restore receipt: {error}"))
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
                evidence: Some(serde_json::json!({"restore":"already_active_no_native_call"})),
                created_at: command.requested_at,
            })?;
            tx.commit()
                .map_err(|error| db_error("failed committing exact no-op restore", error))?;
            return Ok(OperatorAgentRestoreAdmission::Replay {
                operation_id: command.operation_id.clone(),
                exact_result: agent,
            });
        }

        assert_restore_identity(&state, command, "archived", "closed")?;
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
            idempotency_key: "exact_provider_resume".into(),
            evidence: Some(serde_json::json!({
                "provider":command.provider,
                "original_provider_session_ref":command.provider_session_ref,
                "original_canonical_provider_session_ref":command.canonical_provider_session_ref,
                "expected_revision":command.expected_revision,
                "session_updated_at":command.session_updated_at,
            })),
            attempt_count: 0,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing durable restore intent", error))?;
        Ok(OperatorAgentRestoreAdmission::Execute {
            operation_id: command.operation_id.clone(),
        })
    }
}
