-- Stable chronological message-history pages: global, workspace-scoped,
-- sender-scoped, and combined workspace+sender scopes. Do not project or
-- duplicate JSON message inputs into an additional mutable read table.
CREATE INDEX idx_agent_messages_created
  ON agent_messages(created_at, id);
CREATE INDEX idx_agent_messages_workspace_sender_created
  ON agent_messages(workspace_id, sender_agent_id, created_at, id)
  WHERE workspace_id IS NOT NULL;
