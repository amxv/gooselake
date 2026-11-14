-- Legacy comms diagnostic status counts must not scan/deserialise every
-- delivery payload. This additive index preserves arbitrary old status text.
CREATE INDEX idx_team_deliveries_status ON team_deliveries(status);
