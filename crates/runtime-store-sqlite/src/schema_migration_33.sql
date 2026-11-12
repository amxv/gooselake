-- A team deletion must not scan every historical caller-bound spawn to
-- discover a single unresolved native operation. This is an SQLite-managed
-- sparse integrity index, rebuilt from the original journal and admissions
-- on migration, then maintained in the *same* transaction as every write.
-- A missing/mismatched journal and a completed stage without its original
-- response must remain indexed as unresolved. A rolled-back journal clears
-- its fence; successful completion clears only after the response is stored.
CREATE TABLE IF NOT EXISTS team_spawn_deletion_fence (
  operation_id TEXT PRIMARY KEY,
  team_id TEXT NOT NULL,
  keyed INTEGER NOT NULL CHECK (keyed IN (0,1))
);
CREATE INDEX IF NOT EXISTS idx_team_spawn_deletion_fence_team_keyed
ON team_spawn_deletion_fence(team_id,keyed,operation_id);

CREATE VIEW IF NOT EXISTS team_spawn_deletion_fence_candidates AS
SELECT a.operation_id,a.team_id,1 AS keyed
FROM team_spawn_admissions a
LEFT JOIN team_operation_journal j
  ON j.operation_id=a.operation_id AND j.team_id=a.team_id
 AND j.kind='spawn_member_with_worktree'
WHERE j.operation_id IS NULL OR
      (j.stage!='rolled_back' AND
       (j.stage!='completed' OR a.completed_response_json IS NULL))
UNION ALL
SELECT j.operation_id,j.team_id,0 AS keyed
FROM team_operation_journal j
WHERE j.kind='spawn_member_with_worktree'
  AND j.stage NOT IN ('completed','rolled_back')
  AND NOT EXISTS (
    SELECT 1 FROM team_spawn_admissions a WHERE a.operation_id=j.operation_id
  );

INSERT INTO team_spawn_deletion_fence(operation_id,team_id,keyed)
SELECT operation_id,team_id,keyed FROM team_spawn_deletion_fence_candidates;

CREATE TRIGGER IF NOT EXISTS trg_spawn_deletion_fence_journal_insert
AFTER INSERT ON team_operation_journal BEGIN
  DELETE FROM team_spawn_deletion_fence WHERE operation_id=NEW.operation_id;
  INSERT INTO team_spawn_deletion_fence
    SELECT * FROM team_spawn_deletion_fence_candidates
    WHERE operation_id=NEW.operation_id;
END;
CREATE TRIGGER IF NOT EXISTS trg_spawn_deletion_fence_journal_update
AFTER UPDATE ON team_operation_journal BEGIN
  DELETE FROM team_spawn_deletion_fence
    WHERE operation_id IN (OLD.operation_id,NEW.operation_id);
  INSERT INTO team_spawn_deletion_fence
    SELECT * FROM team_spawn_deletion_fence_candidates
    WHERE operation_id IN (OLD.operation_id,NEW.operation_id);
END;
CREATE TRIGGER IF NOT EXISTS trg_spawn_deletion_fence_journal_delete
AFTER DELETE ON team_operation_journal BEGIN
  DELETE FROM team_spawn_deletion_fence WHERE operation_id=OLD.operation_id;
  INSERT INTO team_spawn_deletion_fence
    SELECT * FROM team_spawn_deletion_fence_candidates
    WHERE operation_id=OLD.operation_id;
END;
CREATE TRIGGER IF NOT EXISTS trg_spawn_deletion_fence_admission_insert
AFTER INSERT ON team_spawn_admissions BEGIN
  DELETE FROM team_spawn_deletion_fence WHERE operation_id=NEW.operation_id;
  INSERT INTO team_spawn_deletion_fence
    SELECT * FROM team_spawn_deletion_fence_candidates
    WHERE operation_id=NEW.operation_id;
END;
CREATE TRIGGER IF NOT EXISTS trg_spawn_deletion_fence_admission_update
AFTER UPDATE ON team_spawn_admissions BEGIN
  DELETE FROM team_spawn_deletion_fence
    WHERE operation_id IN (OLD.operation_id,NEW.operation_id);
  INSERT INTO team_spawn_deletion_fence
    SELECT * FROM team_spawn_deletion_fence_candidates
    WHERE operation_id IN (OLD.operation_id,NEW.operation_id);
END;
CREATE TRIGGER IF NOT EXISTS trg_spawn_deletion_fence_admission_delete
AFTER DELETE ON team_spawn_admissions BEGIN
  DELETE FROM team_spawn_deletion_fence WHERE operation_id=OLD.operation_id;
  INSERT INTO team_spawn_deletion_fence
    SELECT * FROM team_spawn_deletion_fence_candidates
    WHERE operation_id=OLD.operation_id;
END;
