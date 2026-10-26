-- Stable newest-first operator pages without scanning hook commands, inputs,
-- execution receipts or mutable effect phases.
CREATE INDEX idx_workspace_hook_effects_history
ON workspace_hook_effects(
  workspace_id, created_at_ms DESC, parent_event_id DESC,
  event_name DESC, hook_name DESC
);
