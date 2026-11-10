-- Model-facing status orders active workspace members by stable friendly alias.
-- Avoid sorting/loading the entire roster when only a bounded page is needed.
CREATE INDEX idx_workspace_agents_status_alias
  ON workspace_agents(workspace_id, lifecycle_state, identity_alias, session_id);
