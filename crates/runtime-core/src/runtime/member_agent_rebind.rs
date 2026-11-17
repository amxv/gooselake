use std::path::Path;
use std::sync::Arc;

use super::helpers::now_ms;
use super::RuntimeSessionManager;
use crate::{
    MemberAgentRebindAdmission, MemberAgentRebindCommand, OperationActorKind,
    ProviderCapabilitySupport, ProviderKind, RepositoryIdentity, RuntimeError,
    WorkspaceAgentRebindResponse, WorkspaceMembershipAction, WorkspaceMembershipPolicy,
};

fn replay(
    admission: MemberAgentRebindAdmission,
) -> Result<WorkspaceAgentRebindResponse, RuntimeError> {
    match admission {
        MemberAgentRebindAdmission::Replay { response } => Ok(response),
        MemberAgentRebindAdmission::Unresolved { operation_id } => Err(RuntimeError::operation_unresolved(operation_id,
            "original member rebind has no confirmed receipt; inspect it without repeating native work")),
        MemberAgentRebindAdmission::Execute => Err(RuntimeError::InvalidState("read-only member rebind lookup cannot authorize execution".into())),
    }
}

impl RuntimeSessionManager {
    pub async fn rebind_workspace_agent_as_member(
        self: &Arc<Self>,
        workspace_id: &str,
        caller: &str,
        parent_id: &str,
        policy: WorkspaceMembershipPolicy,
    ) -> Result<WorkspaceAgentRebindResponse, RuntimeError> {
        self.authorize_workspace_membership(
            workspace_id,
            caller,
            WorkspaceMembershipAction::Add,
            policy,
        )?;
        let parent = self
            .store
            .get_operation(parent_id)?
            .ok_or_else(|| RuntimeError::NotFound("model assignment parent".into()))?;
        let target = parent.operation.normalized_request["args"]["assign_worktree"]["agent_id"]
            .as_str()
            .ok_or_else(|| RuntimeError::InvalidState("model assignment target missing".into()))?;
        // Preserve the target-first lookup contract even for a non-Git root.
        // The durable planner still freezes and rechecks this target in its
        // own transaction before any native admission.
        self.get_workspace_agent(workspace_id, target)?;
        let name = parent.operation.normalized_request["args"]["assign_worktree"]["worktree_name"]
            .as_str()
            .ok_or_else(|| {
                RuntimeError::InvalidState("model assignment worktree name missing".into())
            })?;
        let inventory = self.list_workspace_worktree_inventory(workspace_id)?;
        let destination = inventory
            .worktrees
            .iter()
            .find(|entry| entry.normalized_name == name && entry.eligible_for_assignment)
            .ok_or_else(|| RuntimeError::NotFound("eligible worktree name".into()))?;
        let repository = RepositoryIdentity {
            canonical_root: inventory.canonical_repository_root.clone(),
            git_common_dir: inventory.git_common_dir.clone(),
            fingerprint: inventory.repository_fingerprint.clone(),
        };
        let command = self.store.plan_member_agent_rebind(
            parent_id,
            caller,
            policy,
            &destination.worktree_id,
            &repository,
            now_ms(),
        )?;
        match self.execute_member_agent_rebind(command.clone()).await {
            Ok(response) => Ok(response),
            Err(error) => {
                if let Err(record_error) =
                    self.store
                        .reject_member_agent_rebind(&command, &error.to_string(), now_ms())
                {
                    return Err(RuntimeError::operation_unresolved(
                        parent_id,
                        format!("{error}; target refusal could not be persisted: {record_error}"),
                    ));
                }
                Err(error)
            }
        }
    }

    pub async fn execute_member_agent_rebind(
        self: &Arc<Self>,
        command: MemberAgentRebindCommand,
    ) -> Result<WorkspaceAgentRebindResponse, RuntimeError> {
        if command.actor.kind != OperationActorKind::Agent {
            return Err(RuntimeError::InvalidState(
                "member rebind requires Agent identity".into(),
            ));
        }
        if let Some(existing) = self.store.replay_member_agent_rebind(&command)? {
            return replay(existing);
        }
        let _mutation = self.session_policy_mutation_lock.lock().await;
        self.fence_unresolved_operator_turn_native_effect(&command.operation.agent_id)?;
        let workspace = self
            .store
            .get_workspace(&command.operation.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound("member rebind workspace".into()))?;
        let lifecycle_lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
        let _lifecycle = lifecycle_lock.read().await;
        let identity = crate::resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        if identity != command.repository {
            return Err(RuntimeError::Conflict(
                "member rebind repository identity changed".into(),
            ));
        }
        let repository_lock = crate::repository_worktree_lock(&identity.canonical_root).await;
        let _repository = repository_lock.lock().await;
        let session = self.get_session(&command.operation.agent_id).await?;
        if session != command.original_session {
            return Err(RuntimeError::Conflict(
                "local native attachment differs from frozen member rebind target".into(),
            ));
        }
        let destination = self.store.get_managed_worktree(&command.destination.id)?;
        if destination.as_ref() != Some(&command.destination)
            || self.eligible_workspace_worktree_cwd(&workspace, &command.destination.id)?
                != command.destination.worktree_cwd
        {
            return Err(RuntimeError::Conflict(
                "member rebind destination identity changed".into(),
            ));
        }
        let kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation("unknown member rebind provider".into())
        })?;
        let provider = self
            .providers
            .get_enabled(kind)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(session.provider.clone()))?;
        if provider.capabilities().workspace_rebind != ProviderCapabilitySupport::Supported {
            return Err(RuntimeError::Unsupported(
                "provider cannot rebind workspace routes".into(),
            ));
        }
        match self.store.admit_member_agent_rebind(&command)? {
            MemberAgentRebindAdmission::Execute => {}
            existing => return replay(existing),
        }
        let result = self
            .execute_admitted_workspace_rebind(
                command.operation.clone(),
                session,
                command.original_agent.clone(),
                identity,
                provider,
            )
            .await;
        // The receipt commits the full historical response. A live roster
        // lookup, including after a lost commit acknowledgement, cannot replace it.
        match self.store.replay_member_agent_rebind(&command) {
            Ok(Some(MemberAgentRebindAdmission::Replay { response })) => Ok(response),
            Ok(_) => Err(result.err().unwrap_or_else(|| {
                RuntimeError::operation_unresolved(
                    &command.operation.operation_id,
                    "native rebind returned without its immutable child receipt",
                )
            })),
            Err(error) => Err(RuntimeError::operation_unresolved(
                &command.operation.operation_id,
                format!("native rebind receipt inspection unavailable: {error}"),
            )),
        }
    }
}
