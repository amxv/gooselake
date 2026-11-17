-- Every historical spawn stage remains relevant until a typed saved
-- completion is certified. Terminal labels alone cannot filter obligations.
CREATE INDEX IF NOT EXISTS idx_team_spawn_journal_all_operations
ON team_operation_journal(operation_id)
WHERE kind='spawn_member_with_worktree';
