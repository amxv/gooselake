-- Every managed checkout generation has at most one native cleanup attempt.
-- An unresolved intent is durable authority that blocks blindly rerunning
-- git worktree remove/branch -d after a lost process or terminal receipt.
CREATE TABLE managed_worktree_cleanup_receipts (
  operation_id TEXT PRIMARY KEY,
  worktree_id TEXT NOT NULL REFERENCES managed_worktrees(id),
  worktree_created_at INTEGER NOT NULL,
  repo_root TEXT NOT NULL,
  checkout_path TEXT NOT NULL,
  branch_name TEXT NOT NULL,
  deletion_policy TEXT NOT NULL,
  reason TEXT,
  phase TEXT NOT NULL CHECK (phase IN ('pending', 'manual_review', 'completed')),
  response_json TEXT,
  requested_at INTEGER NOT NULL,
  completed_at INTEGER,
  UNIQUE(worktree_id, worktree_created_at)
);

CREATE INDEX idx_worktree_cleanup_pending_checkout
ON managed_worktree_cleanup_receipts(checkout_path)
WHERE phase != 'completed';

CREATE INDEX idx_worktree_cleanup_pending_branch
ON managed_worktree_cleanup_receipts(repo_root, branch_name)
WHERE phase != 'completed';

-- Another writer must not bind an agent to a checkout after a native cleanup
-- was admitted (or after its generation was removed). Recreated worktrees
-- have a new created_at generation and are not fenced by old receipts.
CREATE TRIGGER block_worktree_claim_during_cleanup_insert
BEFORE INSERT ON managed_worktree_claims
WHEN NEW.released_at IS NULL AND EXISTS (
  SELECT 1 FROM managed_worktree_cleanup_receipts c
  JOIN managed_worktrees w ON w.id = c.worktree_id
  WHERE w.id = NEW.worktree_id AND w.created_at = c.worktree_created_at
)
BEGIN SELECT RAISE(ABORT, 'managed worktree native cleanup already admitted'); END;

CREATE TRIGGER block_worktree_claim_during_cleanup_update
BEFORE UPDATE ON managed_worktree_claims
WHEN NEW.released_at IS NULL AND EXISTS (
  SELECT 1 FROM managed_worktree_cleanup_receipts c
  JOIN managed_worktrees w ON w.id = c.worktree_id
  WHERE w.id = NEW.worktree_id AND w.created_at = c.worktree_created_at
)
BEGIN SELECT RAISE(ABORT, 'managed worktree native cleanup already admitted'); END;

-- Session writers (including legacy routes) cannot bypass a claim reservation
-- by binding directly to an already-cleaning checkout or a descendant cwd.
-- Closing an existing historical session is still allowed.
CREATE TRIGGER block_session_bind_during_worktree_cleanup_insert
BEFORE INSERT ON sessions
WHEN NEW.status != 'closed' AND EXISTS (
  SELECT 1 FROM managed_worktree_cleanup_receipts c
  JOIN managed_worktrees w ON w.id = c.worktree_id
  WHERE w.created_at = c.worktree_created_at
    AND (NEW.worktree_id = w.id
      OR NEW.cwd = c.checkout_path
      OR substr(NEW.cwd, 1, length(c.checkout_path) + 1) = c.checkout_path || '/')
)
BEGIN SELECT RAISE(ABORT, 'managed worktree native cleanup fences session binding'); END;

CREATE TRIGGER block_session_bind_during_worktree_cleanup_update
BEFORE UPDATE ON sessions
WHEN NEW.status != 'closed' AND EXISTS (
  SELECT 1 FROM managed_worktree_cleanup_receipts c
  JOIN managed_worktrees w ON w.id = c.worktree_id
  WHERE w.created_at = c.worktree_created_at
    AND (NEW.worktree_id = w.id
      OR NEW.cwd = c.checkout_path
      OR substr(NEW.cwd, 1, length(c.checkout_path) + 1) = c.checkout_path || '/')
)
BEGIN SELECT RAISE(ABORT, 'managed worktree native cleanup fences session binding'); END;

-- An unresolved native operation must never be detached from the exact
-- checkout generation by an unrelated logical worktree upsert/repair.
CREATE TRIGGER block_pending_worktree_cleanup_identity_change
BEFORE UPDATE ON managed_worktrees
WHEN EXISTS (
  SELECT 1 FROM managed_worktree_cleanup_receipts c
  WHERE c.worktree_id = OLD.id AND c.worktree_created_at = OLD.created_at
    AND c.phase != 'completed'
) AND (
  NEW.created_at != OLD.created_at OR NEW.repo_root != OLD.repo_root OR
  NEW.worktree_cwd != OLD.worktree_cwd OR NEW.branch_name != OLD.branch_name OR
  NEW.deletion_policy != OLD.deletion_policy
)
BEGIN SELECT RAISE(ABORT, 'pending native worktree cleanup fences identity changes'); END;
