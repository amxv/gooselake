use crate::{
    OperationActor, RuntimeError, WorkspaceWorktreeCreationAdmission,
    WorkspaceWorktreeCreationCommand, WorktreeCreateResponse,
};

/// Persist and recover operator worktree creation intent before Git or shell
/// code executes. Separated from RuntimeStore to keep each source file bounded.
pub trait WorkspaceWorktreeCreationStore: Send + Sync {
    fn admit_workspace_worktree_creation(
        &self,
        _command: &WorkspaceWorktreeCreationCommand,
    ) -> Result<WorkspaceWorktreeCreationAdmission, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable native worktree admission is unavailable".into(),
        ))
    }

    /// Historical exact receipts must be recoverable without mutable Git state.
    fn replay_workspace_worktree_creation(
        &self,
        _actor: &OperationActor,
        _idempotency_key: &str,
        _workspace_id: &str,
        _normalized_request_hash: &str,
    ) -> Result<Option<WorkspaceWorktreeCreationAdmission>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable native worktree replay lookup is unavailable".into(),
        ))
    }

    /// Legacy checkout creation and deletion must respect these reservations.
    fn native_worktree_creation_claim_owner(
        &self,
        _checkout_path: &str,
    ) -> Result<Option<String>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "native worktree effect claim lookup is unavailable".into(),
        ))
    }

    /// A branch is repository-unique even if the managed checkout root
    /// changes between processes. Unknown native effects fence both identities.
    fn native_worktree_creation_branch_owner(
        &self,
        _repository_root: &str,
        _branch_name: &str,
    ) -> Result<Option<String>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "native worktree branch reservation lookup is unavailable".into(),
        ))
    }

    fn complete_workspace_worktree_creation(
        &self,
        _operation_id: &str,
        _response: &WorktreeCreateResponse,
        _at_ms: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable native worktree completion is unavailable".into(),
        ))
    }

    fn mark_workspace_worktree_creation_uncertain(
        &self,
        _operation_id: &str,
        _reason: &str,
        _at_ms: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable native worktree review is unavailable".into(),
        ))
    }
}
