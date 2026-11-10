// Repository-scoped worktree status reads and legacy upsert/delegation share
// the existing RuntimeStore implementation without exceeding file-size limits.
macro_rules! delegate_worktree_status {
    () => {
        fn upsert_managed_worktree(
            &self,
            record: &runtime_core::ManagedWorktreeRecord,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository.upsert_managed_worktree(record)
        }

        fn list_managed_worktree_status_for_root(
            &self,
            canonical_repo_root: &str,
        ) -> Result<Vec<runtime_core::ManagedWorktreeStatusProjection>, runtime_core::RuntimeError>
        {
            self.repository
                .list_managed_worktree_status_for_root(canonical_repo_root)
        }

        fn managed_worktree_revision(
            &self,
            worktree_id: &str,
        ) -> Result<u64, runtime_core::RuntimeError> {
            self.repository.managed_worktree_revision(worktree_id)
        }

        fn upsert_managed_worktree_claim(
            &self,
            record: &runtime_core::ManagedWorktreeClaimRecord,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository.upsert_managed_worktree_claim(record)
        }
    };
}

pub(crate) use delegate_worktree_status;
