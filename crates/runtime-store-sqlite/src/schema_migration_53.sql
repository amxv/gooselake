-- Initialize exactly once inside this migration transaction.
-- An authorized restore prepares a distinct inactive target with a fresh
-- incarnation; ordinary startup must never infer or perform that rotation.
CREATE TABLE runtime_event_journal_identity (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  epoch TEXT NOT NULL CHECK (
    typeof(epoch) = 'text' AND length(epoch) = 32
    AND epoch NOT GLOB '*[^0-9a-f]*'
  )
);
INSERT INTO runtime_event_journal_identity(singleton, epoch)
VALUES (1, lower(hex(randomblob(16))));
