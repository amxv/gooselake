-- Startup must seek unresolved native interruption for each original target,
-- without scanning unrelated historical operations or inferring from live turns.
CREATE INDEX idx_workspace_interrupt_unresolved_target
ON runtime_operation_effects(target_id, operation_id)
WHERE effect_kind='workspace_turn_interrupt' AND phase IN ('started','uncertain');
