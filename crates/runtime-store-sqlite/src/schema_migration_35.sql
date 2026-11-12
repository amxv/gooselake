-- Read-side active agent attachment index for bounded worktree inventory pages.
-- The original recreation policy JSON, member/profile, and session rows remain
-- authority. The indexed expression is only a candidate path filter.
CREATE INDEX idx_workspace_agents_active_route_cwd
ON workspace_agents(workspace_id,
  json_extract(recreation_policy_json, '$.authoritative_cwd'),
  created_at, session_id)
WHERE lifecycle_state='active';
