use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::TeamWithMembers;
use crate::{
    ManagedWorktreeClaimRecord, ManagedWorktreeRecord, OperationActor, SessionRecord,
    TeamMemberRecord, TeamOperationDiagnosticRecord,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCreateRequest {
    pub team_id: Option<String>,
    pub source_session_id: String,
    pub repo_root: Option<String>,
    pub worktree_name: String,
    pub branch_prefix: Option<String>,
    pub base_ref: Option<String>,
    pub deletion_policy: Option<String>,
    pub run_init_script: Option<bool>,
    pub created_by_session_id: Option<String>,
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceWorktreeCreateRequest {
    pub workspace_id: String,
    pub worktree_name: String,
    pub branch_prefix: Option<String>,
    pub base_ref: Option<String>,
    pub deletion_policy: Option<String>,
    pub run_init_script: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCreateResponse {
    pub worktree: ManagedWorktreeRecord,
    pub created: bool,
    pub init_script_status: String,
}

/// Exact operator intent and expected native Git identity, committed before
/// worktree creation or an optional initialization script can execute.
#[derive(Debug, Clone)]
pub struct WorkspaceWorktreeCreationCommand {
    pub operation_id: String,
    pub workspace_id: String,
    pub actor: OperationActor,
    pub idempotency_key: String,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub repository_root: String,
    pub branch_name: String,
    pub checkout_path: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone)]
pub enum WorkspaceWorktreeCreationAdmission {
    Execute {
        operation_id: String,
    },
    Replay {
        operation_id: String,
        response: WorktreeCreateResponse,
    },
    /// An earlier execution may have reached Git or an init script. Never
    /// replay that native effect without independent operator evidence.
    Unresolved {
        operation_id: String,
    },
}

#[derive(Debug, Clone)]
pub struct WorkspaceWorktreeCreationResult {
    pub operation_id: String,
    pub response: WorktreeCreateResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeClaimRequest {
    pub worktree_id: String,
    pub session_id: String,
    pub claim_role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeClaimResponse {
    pub worktree: ManagedWorktreeRecord,
    pub claim: ManagedWorktreeClaimRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeReleaseRequest {
    pub worktree_id: String,
    pub session_id: String,
    pub cleanup_if_last_claim: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeReleaseResponse {
    pub worktree: ManagedWorktreeRecord,
    pub released_claim: ManagedWorktreeClaimRecord,
    pub active_claim_count: usize,
    pub cleanup: Option<WorktreeCleanupResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCleanupRequest {
    pub worktree_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCleanupResponse {
    pub worktree_id: String,
    pub status: String,
    pub deletion_policy: String,
    pub active_claim_count: usize,
    pub worktree_path_deleted: bool,
    pub branch_deleted: bool,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberSpawnWorktreeInput {
    pub mode: Option<String>,
    pub name: Option<String>,
    pub branch_prefix: Option<String>,
    pub base_ref: Option<String>,
    pub run_init_script: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberSpawnRequest {
    pub team_id: String,
    pub source_session_id: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub title: Option<String>,
    pub prompt: Option<String>,
    pub permission_mode: Option<String>,
    pub metadata: Option<Value>,
    pub worktree: Option<TeamMemberSpawnWorktreeInput>,
    pub creator_agent_id: Option<String>,
    pub creator_compaction_subscription: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberSpawnResponse {
    pub operation_id: String,
    pub team: TeamWithMembers,
    pub spawned_session: SessionRecord,
    pub spawned_member: TeamMemberRecord,
    pub worktree: Option<ManagedWorktreeRecord>,
    pub worktree_assignment_mode: String,
    pub worktree_created_by_operation: bool,
    pub onboarding: Value,
    pub journal_stage: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeMemberRemovedRequest {
    pub team_id: String,
    pub agent_id: String,
    pub removed_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeMemberRemovedResponse {
    pub released_claims: Vec<ManagedWorktreeClaimRecord>,
    pub cleanup_results: Vec<WorktreeCleanupResponse>,
    pub diagnostics: Vec<TeamOperationDiagnosticRecord>,
}
