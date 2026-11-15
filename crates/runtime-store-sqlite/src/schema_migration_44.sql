-- The native-close review refresh reads only turns owned by the acknowledged
-- session. Earlier schema versions had no turns(session_id) seek index and
-- scanned the full historical turn table even for one retired attachment.
-- The implicit rowid tail preserves the exact old ORDER BY rowid ordering.
CREATE INDEX idx_turns_session_projection ON turns(session_id);
