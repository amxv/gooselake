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
        // Non-SQLite backends retain their existing complete inventory path.
        // SQLite overrides this visitor with one bounded, read-snapshot walk.
        fn visit_managed_worktree_status_for_root(
            &self,
            canonical_repo_root: &str,
            workspace_id: &str,
            visitor: &mut dyn FnMut(
                crate::ManagedWorktreeStatusProjection,
                Vec<String>,
            ) -> Result<(), crate::RuntimeError>,
        ) -> Result<(), crate::RuntimeError> {
            let mut attached_by_cwd: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            for route in self.list_workspace_active_agent_routes(workspace_id)? {
                attached_by_cwd
                    .entry(route.authoritative_cwd)
                    .or_default()
                    .push(route.agent_id);
            }
            for attached in attached_by_cwd.values_mut() {
                attached.sort();
            }
            for projection in self.list_managed_worktree_status_for_root(canonical_repo_root)? {
                let attached = attached_by_cwd
                    .get(&projection.worktree.worktree_cwd)
                    .cloned()
                    .unwrap_or_default();
                visitor(projection, attached)?;
            }
            Ok(())
        }
        crate::services_worktree_status_trait::default_worktree_status_page_query!();
    };
}

pub(crate) use default_worktree_status_query;

// Non-SQLite stores preserve the historical inventory contract while SQLite
// overrides this with a keyset query over only the requested page. The boolean
// says whether the supplied cursor still belongs to the visible collection.
macro_rules! default_worktree_status_page_query {
    () => {
        fn list_managed_worktree_status_for_root_page(
            &self,
            canonical_repo_root: &str,
            after: Option<(&str, &str)>,
            limit: usize,
        ) -> Result<(Vec<crate::ManagedWorktreeStatusProjection>, bool), crate::RuntimeError> {
            let mut rows = self.list_managed_worktree_status_for_root(canonical_repo_root)?;
            rows.retain(|row| !row.worktree.worktree_cwd.starts_with("__gg_tombstoned__/"));
            rows.sort_by(|left, right| {
                left.worktree
                    .worktree_name
                    .cmp(&right.worktree.worktree_name)
                    .then(left.worktree.id.cmp(&right.worktree.id))
            });
            let valid_cursor = after.is_none_or(|(name, id)| {
                rows.iter()
                    .any(|row| row.worktree.worktree_name == name && row.worktree.id == id)
            });
            if let Some((name, id)) = after {
                rows.retain(|row| {
                    (
                        row.worktree.worktree_name.as_str(),
                        row.worktree.id.as_str(),
                    ) > (name, id)
                });
            }
            rows.truncate(limit);
            Ok((rows, valid_cursor))
        }
    };
}

pub(crate) use default_worktree_status_page_query;

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
