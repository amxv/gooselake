// Repository-scoped worktree status reads and legacy upsert/delegation share
// the existing RuntimeStore implementation without exceeding file-size limits.
macro_rules! delegate_worktree_status {
    () => {
        fn list_managed_worktrees(
            &self,
        ) -> Result<Vec<runtime_core::ManagedWorktreeRecord>, runtime_core::RuntimeError> {
            self.repository.list_managed_worktrees()
        }

        fn first_active_foreign_worktree_claim_for_session(
            &self,
            session_id: &str,
            worktree_id: &str,
        ) -> Result<Option<String>, runtime_core::RuntimeError> {
            self.repository
                .first_active_foreign_worktree_claim_for_session(session_id, worktree_id)
        }

        fn list_active_managed_worktree_claims_for_session(
            &self,
            session_id: &str,
        ) -> Result<Vec<runtime_core::ManagedWorktreeClaimRecord>, runtime_core::RuntimeError> {
            self.repository
                .list_active_managed_worktree_claims_for_session(session_id)
        }

        fn get_managed_worktree_claim(
            &self,
            worktree_id: &str,
            session_id: &str,
        ) -> Result<Option<runtime_core::ManagedWorktreeClaimRecord>, runtime_core::RuntimeError> {
            self.repository.get_managed_worktree_claim(worktree_id, session_id)
        }

        fn upsert_managed_worktree(
            &self,
            record: &runtime_core::ManagedWorktreeRecord,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository.upsert_managed_worktree(record)
        }

        fn repair_managed_worktree_if_unchanged(
            &self,
            original: &runtime_core::ManagedWorktreeRecord,
            normalized: &runtime_core::ManagedWorktreeRecord,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository
                .repair_managed_worktree_if_unchanged(original, normalized)
        }

        fn record_managed_worktree_event(
            &self,
            record: &runtime_core::ManagedWorktreeRecord,
            init_script_status: &str,
            recovered: bool,
            session_id: Option<&str>,
            team_id: Option<&str>,
        ) -> Result<runtime_core::ManagedWorktreeRecord, runtime_core::RuntimeError> {
            self.repository.record_managed_worktree_event(
                record,
                init_script_status,
                recovered,
                session_id,
                team_id,
            )
        }

        fn get_managed_worktree(
            &self,
            worktree_id: &str,
        ) -> Result<Option<runtime_core::ManagedWorktreeRecord>, runtime_core::RuntimeError> {
            self.repository.get_managed_worktree(worktree_id)
        }

        fn get_managed_worktree_by_identity(
            &self,
            repo_root: &str,
            worktree_cwd: &str,
            branch_name: &str,
        ) -> Result<Option<runtime_core::ManagedWorktreeRecord>, runtime_core::RuntimeError> {
            self.repository
                .get_managed_worktree_by_identity(repo_root, worktree_cwd, branch_name)
        }

        fn managed_worktree_creation_snapshot(
            &self,
            repo_root: &str,
            worktree_cwd: &str,
            branch_name: &str,
        ) -> Result<
            Option<(runtime_core::ManagedWorktreeRecord, usize)>,
            runtime_core::RuntimeError,
        > {
            self.repository.managed_worktree_creation_snapshot(
                repo_root,
                worktree_cwd,
                branch_name,
            )
        }

        fn list_managed_worktree_checkout_paths(
            &self,
        ) -> Result<Vec<(String, String)>, runtime_core::RuntimeError> {
            self.repository.list_managed_worktree_checkout_paths()
        }

        fn list_managed_worktree_status_for_root(
            &self,
            canonical_repo_root: &str,
        ) -> Result<Vec<runtime_core::ManagedWorktreeStatusProjection>, runtime_core::RuntimeError>
        {
            self.repository
                .list_managed_worktree_status_for_root(canonical_repo_root)
        }

        fn visit_managed_worktree_status_for_root(
            &self,
            canonical_repo_root: &str,
            workspace_id: &str,
            visitor: &mut dyn FnMut(
                runtime_core::ManagedWorktreeStatusProjection,
                Vec<String>,
            ) -> Result<(), runtime_core::RuntimeError>,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository.visit_managed_worktree_status_for_root(
                canonical_repo_root,
                workspace_id,
                visitor,
            )
        }

        fn list_managed_worktree_status_for_root_page(
            &self,
            canonical_repo_root: &str,
            after: Option<(&str, &str)>,
            limit: usize,
        ) -> Result<(Vec<runtime_core::ManagedWorktreeStatusProjection>, bool), runtime_core::RuntimeError> {
            self.repository
                .list_managed_worktree_status_for_root_page(canonical_repo_root, after, limit)
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

        fn transition_managed_worktree_claim(
            &self,
            record: &runtime_core::ManagedWorktreeClaimRecord,
            transition: runtime_core::ManagedWorktreeClaimTransition,
        ) -> Result<runtime_core::ManagedWorktreeClaimRecord, runtime_core::RuntimeError> {
            self.repository
                .transition_managed_worktree_claim(record, transition)
        }

        fn replay_managed_worktree_cleanup(
            &self,
            worktree: &runtime_core::ManagedWorktreeRecord,
        ) -> Result<Option<runtime_core::ManagedWorktreeCleanupAdmission>, runtime_core::RuntimeError> {
            self.repository.replay_managed_worktree_cleanup(worktree)
        }

        fn admit_managed_worktree_cleanup(
            &self,
            command: &runtime_core::ManagedWorktreeCleanupCommand,
        ) -> Result<runtime_core::ManagedWorktreeCleanupAdmission, runtime_core::RuntimeError> {
            self.repository.admit_managed_worktree_cleanup(command)
        }

        fn complete_managed_worktree_cleanup(
            &self,
            operation_id: &str,
            response: &runtime_core::WorktreeCleanupResponse,
            completed_at: i64,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository.complete_managed_worktree_cleanup(operation_id, response, completed_at)
        }

        fn native_worktree_cleanup_owner(
            &self,
            repo_root: &str,
            checkout_path: &str,
            branch_name: &str,
        ) -> Result<Option<String>, runtime_core::RuntimeError> {
            self.repository.native_worktree_cleanup_owner(repo_root, checkout_path, branch_name)
        }
    };
}

pub(crate) use delegate_worktree_status;
