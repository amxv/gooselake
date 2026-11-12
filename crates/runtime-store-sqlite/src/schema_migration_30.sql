-- Legacy /v1 team spawn and gg_team_manage add share this exact caller-bound
-- admission. A persisted intent consumes its key forever, including across
-- crashes and inconclusive native provider/Git/turn results. No process-local
-- cache is authoritative for native retry. Do not backfill guessed receipts
-- for historical (unkeyed) legacy requests.
CREATE TABLE IF NOT EXISTS team_spawn_admissions (
  team_id TEXT NOT NULL,
  source_session_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL CHECK(length(idempotency_key) BETWEEN 1 AND 512),
  request_hash TEXT NOT NULL,
  operation_id TEXT NOT NULL UNIQUE,
  completed_response_json TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(team_id,source_session_id,idempotency_key)
);
