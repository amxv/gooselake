use serde::{Deserialize, Serialize};

use crate::{
    ManagedWorktreeRecord, OperatorAgentCreateCommand, ProviderKind, ProviderThinkingEffort,
    RepositoryIdentity, WorkspaceAgentCreateRequest, WorkspaceAgentInitialRoute,
    WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy,
};

/// Effective preset selection is frozen with the authenticated parent's input.
/// Replays never resolve the current preset catalog again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberAgentCreatePreset {
    pub requested_name: String,
    pub provider: ProviderKind,
    pub model: String,
    pub thinking_effort: Option<ProviderThinkingEffort>,
}

/// A read-only route plan. New checkout paths, branch and base commit are fixed
/// before either Git or provider admission. Root does not require a Git repo.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum MemberAgentCreateRoute {
    Root {
        canonical_root: String,
    },
    Existing {
        repository: RepositoryIdentity,
        worktree: ManagedWorktreeRecord,
        revision: u64,
        /// Preserve the record spelling as the claim/provider route while
        /// detecting retargeted aliases against this separately frozen path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        canonical_cwd: Option<String>,
    },
    New {
        repository: RepositoryIdentity,
        worktree: ManagedWorktreeRecord,
        configured_root: String,
        base_commit: String,
    },
}

impl MemberAgentCreateRoute {
    pub fn canonical_cwd(&self) -> &str {
        match self {
            Self::Existing {
                canonical_cwd: Some(cwd),
                ..
            } => cwd,
            _ => self.cwd(),
        }
    }

    pub fn cwd(&self) -> &str {
        match self {
            Self::Root { canonical_root } => canonical_root,
            Self::Existing { worktree, .. } | Self::New { worktree, .. } => &worktree.worktree_cwd,
        }
    }

    pub fn worktree(&self) -> Option<&ManagedWorktreeRecord> {
        match self {
            Self::Root { .. } => None,
            Self::Existing { worktree, .. } | Self::New { worktree, .. } => Some(worktree),
        }
    }

    pub fn request_route(&self) -> WorkspaceAgentInitialRoute {
        match self {
            Self::Root { .. } => WorkspaceAgentInitialRoute::Root,
            Self::Existing { worktree, .. } => WorkspaceAgentInitialRoute::Existing {
                worktree_id: worktree.id.clone(),
            },
            Self::New {
                worktree,
                base_commit,
                ..
            } => WorkspaceAgentInitialRoute::New {
                worktree_name: worktree.worktree_name.clone(),
                branch_prefix: Some("gg".into()),
                base_ref: Some(base_commit.clone()),
                deletion_policy: Some("retain_on_last_claim".into()),
                run_init_script: Some(false),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberAgentCreateIntent {
    pub parent_operation_id: String,
    pub parent_request_hash: String,
    pub caller: WorkspaceAgentRecord,
    pub canonical_root: String,
    pub request: WorkspaceAgentCreateRequest,
    pub recreation_policy: WorkspaceAgentRecreationPolicy,
    pub preset: Option<MemberAgentCreatePreset>,
    pub route: MemberAgentCreateRoute,
    pub allow_non_lead: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberAgentCreateCommand {
    pub operation: OperatorAgentCreateCommand,
    pub intent: MemberAgentCreateIntent,
}

impl MemberAgentCreateCommand {
    pub fn worktree_command(
        &self,
    ) -> Result<crate::WorkspaceWorktreeCreationCommand, crate::RuntimeError> {
        let MemberAgentCreateRoute::New {
            repository,
            worktree,
            ..
        } = &self.intent.route
        else {
            return Err(crate::RuntimeError::Conflict(
                "member create has no new Git intent".into(),
            ));
        };
        let operation_id = self
            .operation
            .worktree_operation_id
            .clone()
            .ok_or_else(|| {
                crate::RuntimeError::Conflict("member Git identity was not frozen".into())
            })?;
        let normalized_request = serde_json::json!({"workspace_id":self.operation.workspace_id,
            "action":"create_member_worktree","member_create_operation_id":self.operation.operation_id,
            "parent_operation_id":self.intent.parent_operation_id,"route":self.intent.route});
        Ok(crate::WorkspaceWorktreeCreationCommand {
            operation_id,
            workspace_id: self.operation.workspace_id.clone(),
            actor: self.operation.actor.clone(),
            idempotency_key: self
                .intent
                .git_idempotency_key
                .clone()
                .unwrap_or_else(|| format!("member-create-git:{}", self.operation.operation_id)),
            normalized_request_hash: crate::normalized_json_hash(&normalized_request)?,
            normalized_request,
            repository_root: repository.canonical_root.clone(),
            branch_name: worktree.branch_name.clone(),
            checkout_path: worktree.worktree_cwd.clone(),
            requested_at: self.operation.requested_at,
        })
    }
}
