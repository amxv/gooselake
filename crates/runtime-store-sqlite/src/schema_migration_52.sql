-- Preserve existing raw claims and historical journal payloads. Generation one
-- is the explicit SQL migration boundary, not a default for tokenless JSON.
ALTER TABLE managed_worktree_claims ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 1
    CHECK (typeof(claim_generation) = 'integer' AND claim_generation > 0);
