-- The post-commit retirement settled-turn snapshot reads approvals for one
-- owned session/turn, including rows without native provider approval refs
-- and approvals already terminal. Earlier conditional indexes could not seek
-- that complete projection and forced a scan/sort of historical approvals.
CREATE INDEX idx_approvals_session_turn_projection
ON approvals(session_id, turn_id, created_at, id);
