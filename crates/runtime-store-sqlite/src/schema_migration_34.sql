-- HTTP worktree inventory pages use a stable repository/name/id keyset.
-- Keep original repo roots (including aliases) as authority; no data rewrite.
CREATE INDEX idx_managed_worktrees_repo_name_id
ON managed_worktrees(repo_root, worktree_name, id);
