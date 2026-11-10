// Legacy backends may implement only hydration. SQLite overrides this with
// repository-scoped indexed revision/claim queries under one read snapshot.
macro_rules! default_worktree_status_query {
    () => {
        fn list_managed_worktree_status_for_root(
            &self,
            canonical_repo_root: &str,
        ) -> Result<Vec<crate::ManagedWorktreeStatusProjection>, crate::RuntimeError> {
            let state = self.hydrate_runtime_state()?;
            let mut output = Vec::new();
            for worktree in state.managed_worktrees {
                let Ok(root) = std::fs::canonicalize(&worktree.repo_root) else {
                    continue;
                };
                if root != std::path::Path::new(canonical_repo_root) {
                    continue;
                }
                let revision = self.managed_worktree_revision(&worktree.id)?;
                let mut active_claim_session_ids = state
                    .managed_worktree_claims
                    .iter()
                    .filter(|claim| claim.worktree_id == worktree.id && claim.released_at.is_none())
                    .map(|claim| claim.session_id.clone())
                    .collect::<Vec<_>>();
                active_claim_session_ids.sort();
                output.push(crate::ManagedWorktreeStatusProjection {
                    worktree,
                    revision,
                    active_claim_session_ids,
                });
            }
            Ok(output)
        }
    };
}

pub(crate) use default_worktree_status_query;

// Legacy stores without a point-read projection retain the historical lookup.
// SQLite uses the managed_worktrees primary-key index instead of hydrating
// unrelated agents, turns, events, processes and worktrees.
macro_rules! default_managed_worktree_lookup {
    () => {
        fn get_managed_worktree(
            &self,
            worktree_id: &str,
        ) -> Result<Option<crate::ManagedWorktreeRecord>, crate::RuntimeError> {
            Ok(self
                .hydrate_runtime_state()?
                .managed_worktrees
                .into_iter()
                .find(|worktree| worktree.id == worktree_id))
        }
    };
}

pub(crate) use default_managed_worktree_lookup;
