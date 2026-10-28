use super::*;
use crate::OperationActor;

#[async_trait]
pub trait WorktreeService: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeError>;

    async fn get_worktree(&self, worktree_id: &str) -> Result<ManagedWorktreeRecord, RuntimeError>;

    async fn create_worktree(
        &self,
        request: WorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError>;

    async fn create_workspace_worktree(
        &self,
        _request: WorkspaceWorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace-scoped worktree creation is not supported by this service".into(),
        ))
    }

    async fn create_workspace_worktree_idempotent(
        &self,
        _request: WorkspaceWorktreeCreateRequest,
        _actor: OperationActor,
        _idempotency_key: String,
    ) -> Result<WorkspaceWorktreeCreationResult, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable workspace worktree creation is not supported by this service".into(),
        ))
    }

    async fn claim_worktree(
        &self,
        request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError>;

    async fn release_worktree(
        &self,
        request: WorktreeReleaseRequest,
    ) -> Result<WorktreeReleaseResponse, RuntimeError>;

    async fn cleanup_worktree(
        &self,
        request: WorktreeCleanupRequest,
    ) -> Result<WorktreeCleanupResponse, RuntimeError>;

    async fn spawn_team_member(
        &self,
        request: TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError>;

    async fn on_member_removed(
        &self,
        request: WorktreeMemberRemovedRequest,
    ) -> Result<WorktreeMemberRemovedResponse, RuntimeError>;
}
