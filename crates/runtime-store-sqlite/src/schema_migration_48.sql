-- The legacy worktree claim preflight needs only the first active foreign
-- checkout for one session. Preserve historical created_at/worktree_id ties
-- without scanning all unrelated active claims or changing their authority.
CREATE INDEX idx_worktree_claims_active_session_order
  ON managed_worktree_claims(session_id, created_at, worktree_id)
  WHERE released_at IS NULL;
