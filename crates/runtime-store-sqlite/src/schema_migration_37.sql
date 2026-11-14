-- Legacy worktree diagnostics count by persisted deletion policy. Keep the
-- read index-only without deserializing large path/branch/metadata records.
CREATE INDEX idx_managed_worktrees_deletion_policy
  ON managed_worktrees(deletion_policy);
