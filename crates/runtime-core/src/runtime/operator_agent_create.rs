use std::sync::Arc;

use crate::{
    OperationActor, OperatorAgentCreateAdmission, OperatorAgentCreateCommand,
    OperatorAgentCreateReviewCommand, OperatorAgentCreateReviewInput,
    OperatorAgentCreateReviewResponse, RuntimeError, WorkspaceAgentCreateRequest,
    WorkspaceAgentRecord,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// Bind the operator's exact request to one reserved session ID before
    /// Git worktree creation, shell hooks, or provider-native calls. Replay
    /// must happen before validating current workspace/provider capabilities:
    /// a completed receipt remains retrievable after later state changes.
    pub async fn admit_workspace_agent_create_operator(
        self: &Arc<Self>,
        workspace_id: &str,
        request: &WorkspaceAgentCreateRequest,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<
        (
            OperatorAgentCreateAdmission,
            Option<OperatorAgentCreateCommand>,
        ),
        RuntimeError,
    > {
        let workspace_id = workspace_id.trim();
        if workspace_id.is_empty() || actor.identifier.trim().is_empty() {
            return Err(RuntimeError::InvalidState(
                "agent creation requires exact workspace and operator identity".into(),
            ));
        }
        let normalized_request = serde_json::json!({
            "workspace_id":workspace_id,
            "action":"create",
            "request":request,
        });
        let hash = crate::normalized_json_hash(&normalized_request)?;
        let _mutation = self.session_policy_mutation_lock.lock().await;
        if let Some(replay) = self.store.replay_operator_agent_create(
            &actor,
            &idempotency_key,
            workspace_id,
            &hash,
        )? {
            return Ok((replay, None));
        }

        self.preflight_workspace_agent_creation_inputs(workspace_id, request, &actor.identifier)?;
        let command = OperatorAgentCreateCommand {
            operation_id: format!("op_agent_create_{:032x}", rand::random::<u128>()),
            actor,
            idempotency_key,
            workspace_id: workspace_id.into(),
            reserved_agent_id: self.allocate_id("sess", request.provider.as_str()),
            normalized_request,
            normalized_request_hash: hash,
            requested_at: now_ms(),
            worktree_operation_id: None,
        };
        let admission = self.store.admit_operator_agent_create(&command)?;
        match admission {
            OperatorAgentCreateAdmission::Execute { .. } => Ok((admission, Some(command))),
            replay => Ok((replay, None)),
        }
    }

    /// Replay the exact admitted receipt, retaining its verified historical
    /// cause when the original native outcome still requires inspection.
    pub fn workspace_agent_create_replay_result(
        &self,
        admission: OperatorAgentCreateAdmission,
    ) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        admission
            .into_replay_result()
            .map_err(|error| self.restore_legacy_native_recovery(error))
    }

    /// Only the caller holding the newly admitted operation may dispatch.
    /// A provider error, lost reply, or SQLite CAS failure is not proof of
    /// non-execution. Preserve the original immutable native claim for
    /// independent review rather than trying another session/rollback.
    pub async fn execute_workspace_agent_create_operator(
        self: &Arc<Self>,
        command: OperatorAgentCreateCommand,
        request: WorkspaceAgentCreateRequest,
    ) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        let result = self
            .create_workspace_agent_inner(
                &command.workspace_id,
                request,
                &command.actor.identifier,
                Some(&command),
            )
            .await;
        match result {
            Ok(agent) => Ok((agent, command.operation_id)),
            Err(error) => Err(self.quarantine_operator_agent_create_with_legacy_recovery(
                &command.operation_id,
                &command.reserved_agent_id,
                &format!("provider/Git/hooks/atomic creation outcome cannot be retried: {error}"),
                error.legacy_native_recovery(),
            )),
        }
    }

    pub fn quarantine_operator_agent_create_with_legacy_recovery(
        &self,
        operation_id: &str,
        reserved_agent_id: &str,
        reason: &str,
        recovery: Option<crate::legacy_spawn_obligations::LegacyNativeRecovery>,
    ) -> RuntimeError {
        if let Err(error) = self.preserve_legacy_native_recovery(
            operation_id,
            "provider_session_create",
            reserved_agent_id,
            recovery.clone(),
        ) {
            // The existing requested operation/claims remain unresolved.
            return error;
        }
        self.quarantine_operator_agent_create(operation_id, reason)
            .with_legacy_native_recovery(recovery)
    }

    pub fn quarantine_operator_agent_create(
        &self,
        operation_id: &str,
        reason: &str,
    ) -> RuntimeError {
        let persisted =
            self.store
                .mark_operator_agent_create_uncertain(operation_id, reason, now_ms());
        RuntimeError::operation_unresolved(
            operation_id,
            match persisted {
                Ok(()) => format!(
                    "{reason}; inspect the exact operation and independently verify native effects before resolving"
                ),
                Err(error) => format!(
                    "{reason}; durable quarantine also failed ({error}), so native outcome remains unresolved"
                ),
            },
        )
    }

    /// Historical review inspection or fresh prose refusal; no native call occurs.
    pub async fn resolve_operator_agent_create_review(
        self: &Arc<Self>,
        workspace_id: &str,
        operation_id: &str,
        input: OperatorAgentCreateReviewInput,
        actor: OperationActor,
    ) -> Result<OperatorAgentCreateReviewResponse, RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        let result =
            self.store
                .resolve_operator_agent_create_review(&OperatorAgentCreateReviewCommand {
                    operation_id: operation_id.trim().into(),
                    workspace_id: workspace_id.trim().into(),
                    actor,
                    input: input.clone(),
                    reviewed_at: now_ms(),
                })?;
        if result.restart_required {
            // The newly created, verified native attachment remains fenced in
            // this process until a *fresh* startup proves its exact identity.
            // The review has already committed. Only reload the reserved
            // session; unrelated invalid history cannot mask the receipt.
            if let Some(session) = self.store.get_persisted_session(&input.reserved_agent_id)? {
                self.sessions
                    .write()
                    .await
                    .insert(session.id.clone(), session);
            }
        }
        Ok(result)
    }
}
