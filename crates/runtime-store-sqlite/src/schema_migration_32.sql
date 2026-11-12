-- A caller-bound legacy worktree-spawn completion must verify that its
-- persisted onboarding message has exactly the single intended delivery.
-- Without this index, a fixed-key completion scans all historical deliveries
-- as the workspace grows. This is additive: older receipts are not rewritten.
CREATE INDEX IF NOT EXISTS idx_team_deliveries_by_message
ON team_deliveries(message_id);
