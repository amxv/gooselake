use runtime_core::{
    OperationActor, RuntimeError, WorkspaceWorktreeCreationAdmission,
    WorkspaceWorktreeCreationCommand, WorkspaceWorktreeCreationStore, WorktreeCreateResponse,
};

use crate::SqliteRuntimeStore;

impl WorkspaceWorktreeCreationStore for SqliteRuntimeStore {
    fn admit_workspace_worktree_creation(
        &self,
        command: &WorkspaceWorktreeCreationCommand,
    ) -> Result<WorkspaceWorktreeCreationAdmission, RuntimeError> {
        self.repository().admit_workspace_worktree_creation(command)
    }

    fn replay_workspace_worktree_creation(
        &self,
        actor: &OperationActor,
        idempotency_key: &str,
        workspace_id: &str,
        normalized_request_hash: &str,
    ) -> Result<Option<WorkspaceWorktreeCreationAdmission>, RuntimeError> {
        self.repository().replay_workspace_worktree_creation(
            actor,
            idempotency_key,
            workspace_id,
            normalized_request_hash,
        )
    }

    fn native_worktree_creation_claim_owner(
        &self,
        checkout_path: &str,
    ) -> Result<Option<String>, RuntimeError> {
        self.repository()
            .native_worktree_creation_claim_owner(checkout_path)
    }

    fn native_worktree_creation_branch_owner(
        &self,
        repository_root: &str,
        branch_name: &str,
    ) -> Result<Option<String>, RuntimeError> {
        self.repository()
            .native_worktree_creation_branch_owner(repository_root, branch_name)
    }

    fn complete_workspace_worktree_creation(
        &self,
        operation_id: &str,
        response: &WorktreeCreateResponse,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        self.repository()
            .complete_workspace_worktree_creation(operation_id, response, at_ms)
    }

    fn mark_workspace_worktree_creation_uncertain(
        &self,
        operation_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        self.repository()
            .mark_workspace_worktree_creation_uncertain(operation_id, reason, at_ms)
    }
}
