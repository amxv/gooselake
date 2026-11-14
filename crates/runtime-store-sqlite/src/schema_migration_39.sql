-- Read-only legacy recovery anomaly projection, in historical hydration order.
-- Partial indexes skip healthy/inactive rows and cover the small input fields.
CREATE INDEX idx_sessions_active_turn_diagnostics
  ON sessions(created_at, id, active_turn_id)
  WHERE active_turn_id IS NOT NULL;
CREATE INDEX idx_approvals_pending_diagnostics
  ON approvals(created_at, id, turn_id)
  WHERE status = 'pending';
CREATE INDEX idx_worktree_claims_active_diagnostics
  ON managed_worktree_claims(created_at, worktree_id, session_id)
  WHERE released_at IS NULL;
