-- The same session/correlation admission proof gates edit-and-rerun recovery,
-- workspace notice/process completion delivery and retirement settlement.
-- Scope the exact JSON correlation lookup so historical admissions from other
-- agents are never scanned; preserve ascending first-match and descending
-- restart-recovery order without rebuilding any authoritative records.
CREATE INDEX idx_turn_admissions_session_correlation_order
ON turn_admissions(
  session_id,
  json_extract(correlation_json, '$.correlation_id'),
  admitted_at,
  turn_id
);
