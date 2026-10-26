-- Scope-specific operator history advances on a single global row cursor.
-- The existing unique (scope, scope_id, seq) index remains canonical for
-- legacy per-scope sequence reads and provider replay semantics.
CREATE INDEX idx_runtime_events_scope_row_id
ON runtime_events(scope, scope_id, id);
