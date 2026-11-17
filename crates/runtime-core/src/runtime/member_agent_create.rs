use std::sync::Arc;

use crate::{
    MemberAgentCreateCommand, MemberAgentCreateIntent, MemberAgentCreatePreset,
    MemberAgentCreateRoute, OperationActor, OperationActorKind, OperatorAgentCreateAdmission,
    OperatorAgentCreateCommand, RuntimeError, WorkspaceAgentCreateRequest,
    WorkspaceAgentInitialRoute, WorkspaceMembershipPolicy, WorktreeService,
};

use super::helpers::now_ms;
use super::workspace_agents::normalize_recreation_policy;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    #[allow(clippy::too_many_arguments)]
    pub fn plan_workspace_agent_create_as_member(
        &self,
        workspace_id: &str,
        caller_id: &str,
        parent_id: &str,
        request: WorkspaceAgentCreateRequest,
        preset: Option<MemberAgentCreatePreset>,
        mut route: MemberAgentCreateRoute,
        policy: WorkspaceMembershipPolicy,
    ) -> Result<MemberAgentCreateCommand, RuntimeError> {
        self.preflight_workspace_agent_creation_inputs(workspace_id, &request, caller_id)?;
        let caller = self.get_workspace_agent(workspace_id, caller_id)?;
        if let MemberAgentCreateRoute::New { worktree, .. } = &mut route {
            worktree.created_by_session_id = Some(caller_id.into());
        }
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let parent = self
            .store
            .get_operation(parent_id)?
            .ok_or_else(|| RuntimeError::NotFound("model create parent".into()))?
            .operation;
        let mut recreation_policy =
            normalize_recreation_policy(&workspace.canonical_root, &request)?;
        recreation_policy.authoritative_cwd = route.cwd().to_owned();
        let git_idempotency_key = match &route {
            MemberAgentCreateRoute::New { worktree, .. } => {
                Some(crate::member_native_idempotency_key(
                    "create_git",
                    worktree.created_by_operation_id.as_deref().ok_or_else(|| {
                        RuntimeError::Conflict("new member route has no Git identity".into())
                    })?,
                )?)
            }
            _ => None,
        };
        let intent = MemberAgentCreateIntent {
            parent_operation_id: parent_id.into(),
            parent_request_hash: parent.normalized_request_hash,
            caller,
            canonical_root: workspace.canonical_root,
            request,
            recreation_policy,
            preset,
            route,
            allow_non_lead: policy.non_lead_can_add_members,
            git_idempotency_key,
        };
        let reserved_agent_id = self.allocate_id("sess", intent.request.provider.as_str());
        let identity =
            crate::normalized_json_hash(&serde_json::json!([parent_id, reserved_agent_id]))?;
        let normalized_request = serde_json::json!({"workspace_id":workspace_id,"action":"create",
            "request":intent.request,"member_intent":intent});
        let worktree_operation_id = match &intent.route {
            MemberAgentCreateRoute::New { worktree, .. } => {
                worktree.created_by_operation_id.clone()
            }
            _ => None,
        };
        let command = MemberAgentCreateCommand {
            operation: OperatorAgentCreateCommand {
                operation_id: format!("op_member_create_{identity}"),
                actor: OperationActor {
                    kind: OperationActorKind::Agent,
                    identifier: caller_id.into(),
                },
                idempotency_key: crate::member_native_idempotency_key("create", &identity)?,
                workspace_id: workspace_id.into(),
                reserved_agent_id,
                normalized_request_hash: crate::normalized_json_hash(&normalized_request)?,
                normalized_request,
                requested_at: now_ms(),
                worktree_operation_id,
            },
            intent,
        };
        self.store.plan_member_agent_create(&command)
    }

    /// Only the newly admitted child may proceed. Every later stage derives
    /// its input from this saved command; no fresh preset or route resolution.
    pub async fn execute_workspace_agent_create_as_member(
        self: &Arc<Self>,
        command: MemberAgentCreateCommand,
        worktrees: &dyn WorktreeService,
    ) -> Result<serde_json::Value, RuntimeError> {
        if let Some(prior) = self.store.replay_member_agent_create(&command)? {
            return self.member_create_replay(&command, prior);
        }
        let mutation = self.session_policy_mutation_lock.lock().await;
        if let Some(prior) = self.store.replay_member_agent_create(&command)? {
            return self.member_create_replay(&command, prior);
        }
        self.preflight_workspace_agent_creation_inputs(
            &command.operation.workspace_id,
            &command.intent.request,
            &command.operation.actor.identifier,
        )?;
        match &command.intent.route {
            MemberAgentCreateRoute::Root { canonical_root } => {
                let current = std::fs::canonicalize(canonical_root)?;
                if current.to_string_lossy() != canonical_root.as_str() || !current.is_dir() {
                    return Err(RuntimeError::Conflict(
                        "member root route changed before admission".into(),
                    ));
                }
            }
            MemberAgentCreateRoute::Existing {
                repository,
                worktree,
                ..
            } => {
                let current = crate::resolve_repository_identity(std::path::Path::new(
                    &command.intent.canonical_root,
                ))?;
                let workspace = self
                    .store
                    .get_workspace(&command.operation.workspace_id)?
                    .ok_or_else(|| RuntimeError::NotFound("member workspace".into()))?;
                if current != *repository
                    || self.eligible_workspace_worktree_cwd(&workspace, &worktree.id)?
                        != worktree.worktree_cwd
                    || std::fs::canonicalize(&worktree.worktree_cwd)?.to_string_lossy()
                        != command.intent.route.canonical_cwd()
                {
                    return Err(RuntimeError::Conflict(
                        "member managed route changed before admission".into(),
                    ));
                }
            }
            MemberAgentCreateRoute::New { .. } => {}
        }
        let admission = match self.store.admit_member_agent_create(&command) {
            Ok(admission) => admission,
            Err(error) => {
                self.preserve_legacy_native_recovery(
                    &command.intent.parent_operation_id,
                    "model_member_create",
                    &command.operation.reserved_agent_id,
                    error.legacy_native_recovery(),
                )?;
                return Err(error);
            }
        };
        match admission {
            OperatorAgentCreateAdmission::Execute { .. } => {}
            prior => return self.member_create_replay(&command, prior),
        }
        drop(mutation);
        let result = async {
            let mut request = command.intent.request.clone();
            if let MemberAgentCreateRoute::New { worktree, .. } = &command.intent.route {
                let receipt = worktrees
                    .execute_member_agent_create_worktree(&command)
                    .await?;
                if receipt.worktree != *worktree || !receipt.created {
                    return Err(RuntimeError::Conflict(
                        "member Git receipt differs from frozen route".into(),
                    ));
                }
                request.worktree = Some(WorkspaceAgentInitialRoute::Existing {
                    worktree_id: worktree.id.clone(),
                });
            }
            self.create_workspace_agent_inner(
                &command.operation.workspace_id,
                request,
                &command.operation.actor.identifier,
                Some(&command.operation),
            )
            .await?;
            self.store.member_agent_create_result(&command)
        }
        .await;
        match result {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                // A lost commit acknowledgement must not hide an exact saved
                // native/model receipt or cause another provider create call.
                if let Some(OperatorAgentCreateAdmission::Replay { .. }) =
                    self.store.replay_member_agent_create(&command)?
                {
                    return self.store.member_agent_create_result(&command);
                }
                self.preserve_legacy_native_recovery(
                    &command.intent.parent_operation_id,
                    "model_member_create",
                    &command.operation.reserved_agent_id,
                    error.legacy_native_recovery(),
                )?;
                Err(self.quarantine_operator_agent_create_with_legacy_recovery(
                    &command.operation.operation_id,
                    &command.operation.reserved_agent_id,
                    &format!("member creation stage outcome requires inspection: {error}"),
                    error.legacy_native_recovery(),
                ))
            }
        }
    }

    fn member_create_replay(
        &self,
        command: &MemberAgentCreateCommand,
        prior: OperatorAgentCreateAdmission,
    ) -> Result<serde_json::Value, RuntimeError> {
        match prior {
            OperatorAgentCreateAdmission::Replay { .. } => {
                self.store.member_agent_create_result(command)
            }
            other => {
                other
                    .into_replay_result()
                    .map_err(|error| self.restore_legacy_native_recovery(error))?;
                Err(RuntimeError::ProtocolViolation(
                    "unexpected member create replay".into(),
                ))
            }
        }
    }
}
