-- Exact historical diagnostic inspection uses bounded keyset pages.
CREATE INDEX idx_team_operation_diagnostics_operation_id
ON team_operation_diagnostics(operation_id, id);
