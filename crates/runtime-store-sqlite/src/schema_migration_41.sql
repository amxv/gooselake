-- The recurring workspace notice worker reads pending and injecting notices
-- globally, in creation order with onboarding before other kinds at ties.
-- A state-first index cannot provide that ordering across both states, and
-- the prior optional-cursor OR prevented an indexed continuation seek.
-- Only outstanding rows belong in this read-only projection; private notice
-- input JSON and all canonical state/authority columns stay in the base table.
CREATE INDEX idx_workspace_system_notices_active_order
ON workspace_system_notices(
  created_at_ms,
  (CASE WHEN kind = 'onboarding' THEN 0 ELSE 1 END),
  notice_id
)
WHERE state IN ('pending','injecting');
