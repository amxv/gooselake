use std::sync::Arc;

use super::helpers::now_ms;
use super::RuntimeSessionManager;
use crate::{
    OperationActorKind, OperatorAgentArchiveAdmission, OperatorAgentArchiveCommand, ProviderKind,
    RuntimeError, WorkspaceAgentRecord, WorkspaceMembershipAction, WorkspaceMembershipPolicy,
};

impl RuntimeSessionManager {
    pub async fn remove_workspace_agents_as_member(
        self: &Arc<Self>,
        workspace_id: &str,
        caller: &str,
        parent_operation_id: &str,
        policy: WorkspaceMembershipPolicy,
    ) -> Result<Vec<(WorkspaceAgentRecord, String)>, RuntimeError> {
        self.authorize_workspace_membership(
            workspace_id,
            caller,
            WorkspaceMembershipAction::Remove,
            policy,
        )?;
        let plans =
            self.store
                .plan_member_agent_archives(parent_operation_id, caller, policy, now_ms())?;
        let mut removed = Vec::with_capacity(plans.len());
        for command in plans {
            if command.workspace_id != workspace_id || command.actor.identifier != caller {
                return Err(RuntimeError::Conflict(
                    "member archive plan has different owner".into(),
                ));
            }
            match self.execute_member_agent_archive(command.clone()).await {
                Ok(receipt) => removed.push(receipt),
                Err(error) => {
                    self.preserve_legacy_native_recovery(
                        parent_operation_id,
                        "model_member_archive",
                        &command.agent_id,
                        error.legacy_native_recovery(),
                    )?;
                    // The store records this only when NO child was admitted.
                    // Once a child exists its native claim remains authoritative.
                    if let Err(record_error) = self.store.reject_member_agent_archive(
                        &command,
                        &error.to_string(),
                        now_ms(),
                    ) {
                        return Err(RuntimeError::operation_unresolved_with_legacy_recovery(parent_operation_id,
                            format!("{error}; target classification could not be persisted: {record_error}"),
                            error.legacy_native_recovery()));
                    }
                    return Err(error);
                }
            }
        }
        Ok(removed)
    }

    /// A frozen target plan is not a capability to redispatch. The new child
    /// must acquire the shared native claim under current member authority.
    pub async fn execute_member_agent_archive(
        self: &Arc<Self>,
        command: OperatorAgentArchiveCommand,
    ) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        if command.actor.kind != OperationActorKind::Agent {
            return Err(RuntimeError::InvalidState(
                "member archive requires Agent identity".into(),
            ));
        }
        let _mutation = self.session_policy_mutation_lock.lock().await;
        if let Some(existing) = self.store.replay_operator_agent_archive(
            &command.actor,
            &command.idempotency_key,
            &command.workspace_id,
            &command.agent_id,
            &command.normalized_request_hash,
        )? {
            return existing.into_replay_result();
        }
        let workspace = self
            .store
            .get_workspace(&command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound("member archive workspace".into()))?;
        let repository_lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
        let _repository_guard = repository_lock.read().await;
        let session = self.get_session(&command.agent_id).await?;
        if session.status != "ready"
            || session.active_turn_id.is_some()
            || session.provider != command.provider
            || session.provider_session_ref.as_deref()
                != Some(command.provider_session_ref.as_str())
            || session.canonical_provider_session_ref != command.canonical_provider_session_ref
            || session.updated_at != command.session_updated_at
            || serde_json::json!(session.cwd) != command.normalized_request["original_route"]["cwd"]
            || serde_json::json!(session.model)
                != command.normalized_request["original_route"]["model"]
        {
            return Err(RuntimeError::Conflict(
                "local provider attachment differs from frozen model removal target".into(),
            ));
        }
        let kind = ProviderKind::from_str(&command.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation("unknown planned archive provider".into())
        })?;
        let provider = self
            .providers
            .get_enabled(kind)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(command.provider.clone()))?;
        let admission = self.store.admit_member_agent_archive(&command)?;
        if !matches!(admission, OperatorAgentArchiveAdmission::Execute { .. }) {
            return admission.into_replay_result();
        }
        self.execute_admitted_agent_archive(command, session, provider)
            .await
    }
}
