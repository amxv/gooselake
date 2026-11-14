-- Both hook workers poll for at most 64 effects at a time. Older state-first
-- indexes interleave workspace_id before the event/hook tie breakers, forcing
-- an ORDER BY temp B-tree even when the outstanding queue is small.
-- Keep completed/quarantined effects out of these compact read-side indexes.
CREATE INDEX idx_workspace_hook_effects_dispatch_order
ON workspace_hook_effects(created_at_ms,parent_event_id,hook_name)
WHERE phase='intended' AND event_name IN
  ('post_teammate_add','post_teammate_remove',
   'post_direct_message_send','post_broadcast_message_send');

CREATE INDEX idx_workspace_hook_effects_unapplied_order
ON workspace_hook_effects(created_at_ms,parent_event_id,hook_name)
WHERE application_state='pending'
  AND phase IN ('succeeded','failed','timed_out')
  AND event_name IN
    ('post_teammate_add','post_teammate_remove',
     'post_direct_message_send','post_broadcast_message_send');
