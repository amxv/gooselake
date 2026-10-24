-- Merge provider-observed and operator-requested compaction under a single
-- durable lifecycle without rewriting historical manual compaction records.
CREATE TABLE session_compaction_runs_v18 (
  run_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES workspace_agents(session_id),
  idempotency_key TEXT,
  phase TEXT NOT NULL CHECK(phase IN
    ('intended','accepted','started','compacting','not_performed',
     'completed','failed','cancelled','manual_review')),
  record_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
INSERT INTO session_compaction_runs_v18 SELECT * FROM session_compaction_runs;
DROP TABLE session_compaction_runs;
ALTER TABLE session_compaction_runs_v18 RENAME TO session_compaction_runs;
CREATE UNIQUE INDEX idx_session_compaction_unresolved
  ON session_compaction_runs(session_id)
  WHERE phase IN ('intended','accepted','started','compacting','manual_review');
CREATE UNIQUE INDEX idx_session_compaction_idempotency
  ON session_compaction_runs(session_id,idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_session_compaction_recent
  ON session_compaction_runs(session_id,updated_at_ms DESC);
