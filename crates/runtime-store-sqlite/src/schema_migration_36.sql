-- Operator workspace history uses global creation-order keysets.
CREATE INDEX idx_workspaces_created_page
  ON workspaces(created_at, workspace_id);

-- An unfiltered ("all" lifecycle) agent page must preserve global
-- creation order inside a workspace, not scan and re-sort the separate
-- lifecycle partitions. Active/archived pages retain the existing
-- (workspace_id, lifecycle_state, created_at, session_id) index.
CREATE INDEX idx_workspace_agents_created_page
  ON workspace_agents(workspace_id, created_at, session_id);
