-- Sparse, non-destructive index for the legacy team-deletion native spawn
-- fence. Terminal historical rows must not force a global journal scan on
-- every deletion; unresolved original operations retain their inspection IDs.
CREATE INDEX IF NOT EXISTS idx_team_spawn_journal_unsettled
ON team_operation_journal(team_id, operation_id)
WHERE kind='spawn_member_with_worktree' AND stage NOT IN ('completed','rolled_back');
