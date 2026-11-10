-- One immutable journal row can be addressed by its turn, message or operation
-- identity without duplicating its global ID or replacing its primary scope.
-- The composite primary key serves indexed, ordered subject-scoped replay.
CREATE TABLE runtime_event_subjects (
  subject_scope TEXT NOT NULL CHECK (subject_scope IN ('turn','message','operation')),
  subject_id TEXT NOT NULL,
  event_row_id INTEGER NOT NULL REFERENCES runtime_events(id) ON DELETE CASCADE,
  PRIMARY KEY (subject_scope, subject_id, event_row_id)
) WITHOUT ROWID;

-- Backfill already-committed parent-linked events, retaining every source row.
INSERT OR IGNORE INTO runtime_event_subjects
SELECT scope, scope_id, id FROM runtime_events
WHERE scope IN ('turn','message','operation') AND scope_id <> '';

INSERT OR IGNORE INTO runtime_event_subjects
SELECT 'turn', turn_id, id FROM runtime_events WHERE turn_id IS NOT NULL AND turn_id <> '';

INSERT OR IGNORE INTO runtime_event_subjects
SELECT 'message', message_id, id FROM (
  SELECT id, CASE WHEN json_valid(payload_json) THEN
    COALESCE(json_extract(payload_json,'$.message_id'),
             json_extract(payload_json,'$.subject.message_id'))
  END AS message_id FROM runtime_events
) WHERE typeof(message_id) = 'text' AND length(message_id) BETWEEN 1 AND 256;

INSERT OR IGNORE INTO runtime_event_subjects
SELECT 'operation', operation_id, id FROM (
  SELECT id, CASE WHEN json_valid(payload_json) THEN
    json_extract(payload_json,'$.operation_id')
  END AS operation_id FROM runtime_events
) WHERE typeof(operation_id) = 'text' AND length(operation_id) BETWEEN 1 AND 256;

-- Every writer (including the retirement/hook transaction paths and migration
-- adapters) projects subject keys in the SAME SQLite transaction as the event.
CREATE TRIGGER runtime_event_subjects_after_insert
AFTER INSERT ON runtime_events BEGIN
  INSERT OR IGNORE INTO runtime_event_subjects
  SELECT NEW.scope, NEW.scope_id, NEW.id
  WHERE NEW.scope IN ('turn','message','operation') AND NEW.scope_id <> '';

  INSERT OR IGNORE INTO runtime_event_subjects
  SELECT 'turn', NEW.turn_id, NEW.id
  WHERE NEW.turn_id IS NOT NULL AND NEW.turn_id <> '';

  INSERT OR IGNORE INTO runtime_event_subjects
  SELECT 'message', message_id, NEW.id FROM (
    SELECT CASE WHEN json_valid(NEW.payload_json) THEN
      COALESCE(json_extract(NEW.payload_json,'$.message_id'),
               json_extract(NEW.payload_json,'$.subject.message_id'))
    END AS message_id
  ) WHERE typeof(message_id) = 'text' AND length(message_id) BETWEEN 1 AND 256;

  INSERT OR IGNORE INTO runtime_event_subjects
  SELECT 'operation', operation_id, NEW.id FROM (
    SELECT CASE WHEN json_valid(NEW.payload_json) THEN
      json_extract(NEW.payload_json,'$.operation_id')
    END AS operation_id
  ) WHERE typeof(operation_id) = 'text' AND length(operation_id) BETWEEN 1 AND 256;
END;
