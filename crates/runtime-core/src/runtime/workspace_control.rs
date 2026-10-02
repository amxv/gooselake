use std::sync::Arc;

use crate::{
    authorize_workspace_membership_mutation, RuntimeError, WorkspaceAgentCreateRequest,
    WorkspaceAgentRecord, WorkspaceInterruptAdmission, WorkspaceInterruptCommand,
    WorkspaceInterruptResponse, WorkspaceMembershipAction, WorkspaceMembershipPolicy,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    pub fn authorize_workspace_membership(
        &self,
        workspace_id: &str,
        caller_agent_id: &str,
        action: WorkspaceMembershipAction,
        policy: WorkspaceMembershipPolicy,
    ) -> Result<(), RuntimeError> {
        let workspace_id = workspace_id.trim();
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let active_members = self.store.list_workspace_agents(
            workspace_id,
            Some(crate::WorkspaceAgentLifecycleState::Active),
        )?;
        authorize_workspace_membership_mutation(
            &workspace,
            &active_members,
            caller_agent_id,
            action,
            policy,
        )
    }

    pub async fn create_workspace_agent_as_member(
        self: &Arc<Self>,
        workspace_id: &str,
        request: WorkspaceAgentCreateRequest,
        caller_agent_id: &str,
        policy: WorkspaceMembershipPolicy,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.authorize_workspace_membership(
            workspace_id,
            caller_agent_id,
            WorkspaceMembershipAction::Add,
            policy,
        )?;
        self.create_workspace_agent(workspace_id, request, caller_agent_id)
            .await
    }

    pub async fn archive_workspace_agent_as_member(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
        caller_agent_id: &str,
        reason: Option<&str>,
        policy: WorkspaceMembershipPolicy,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.authorize_workspace_membership(
            workspace_id,
            caller_agent_id,
            WorkspaceMembershipAction::Remove,
            policy,
        )?;
        self.archive_workspace_agent(workspace_id, agent_id, reason)
            .await
    }

    pub async fn interrupt_workspace(
        &self,
        command: WorkspaceInterruptCommand,
    ) -> Result<WorkspaceInterruptResponse, RuntimeError> {
        let plan = match self.store.begin_workspace_interrupt(&command)? {
            WorkspaceInterruptAdmission::Replay(response) => return Ok(response),
            WorkspaceInterruptAdmission::Execute(plan) => plan,
        };

        for target in plan.targets {
            self.store.mark_workspace_interrupt_started(
                &plan.operation_id,
                &target.agent_id,
                &target.turn_id,
                now_ms(),
            )?;
            match self
                .interrupt_turn(target.agent_id.as_str(), target.turn_id.as_str())
                .await
            {
                Ok(()) => self.store.finalize_workspace_interrupt_effect(
                    &plan.operation_id,
                    &target.agent_id,
                    &target.turn_id,
                    true,
                    "interrupt_requested",
                    now_ms(),
                )?,
                Err(RuntimeError::InvalidState(_)) | Err(RuntimeError::NotFound(_)) => {
                    self.store.finalize_workspace_interrupt_effect(
                        &plan.operation_id,
                        &target.agent_id,
                        &target.turn_id,
                        false,
                        "turn_no_longer_active",
                        now_ms(),
                    )?;
                }
                Err(error) => {
                    self.store.mark_workspace_interrupt_uncertain(
                        &plan.operation_id,
                        &target.agent_id,
                        &target.turn_id,
                        &serde_json::json!({"message": error.to_string()}),
                        now_ms(),
                    )?;
                    return Err(error);
                }
            }
        }

        self.store
            .complete_workspace_interrupt(&plan.operation_id, now_ms())
    }
}
