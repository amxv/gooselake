-- Hot workspace worktree inventory joins only live claim identities for the
-- selected repository. Do not scan all released historical claims each time.
CREATE INDEX idx_managed_worktree_claims_active_worktree
  ON managed_worktree_claims(worktree_id, session_id)
  WHERE released_at IS NULL;
