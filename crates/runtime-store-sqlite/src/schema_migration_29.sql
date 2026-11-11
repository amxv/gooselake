-- Managed-process critical lifecycle evidence is transactionally coupled to
-- the authoritative process row. SQLite update/commit hooks wake durable SSE
-- only after both the native-state receipt and journal row commit. Historical
-- process records are preserved; do NOT fabricate prior native-effect proof.
-- Legacy process.output samples remain explicitly droppable.

CREATE TRIGGER journal_managed_process_queued
AFTER INSERT ON managed_processes
WHEN NEW.status = 'queued'
BEGIN
  INSERT INTO runtime_events (
    event_id, scope, scope_id, session_id, team_id, turn_id,
    seq, kind, critical, payload_json, provider, provider_seq, created_at
  ) VALUES (
    'evt_proc_' || NEW.process_id || '_process.queued',
    'process', NEW.process_id, NEW.owner_session_id, NULL, NULL,
    (SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events
       WHERE scope = 'process' AND scope_id = NEW.process_id),
    'process.queued', 1,
    json_object('process_id', NEW.process_id, 'status', 'queued',
      'queue_order', NEW.queue_order, 'workspace_id', NEW.workspace_id),
    NULL, NULL, NEW.admitted_at
  );
END;

CREATE TRIGGER journal_managed_process_running
AFTER UPDATE OF status ON managed_processes
WHEN NEW.status = 'running' AND OLD.status != 'running'
BEGIN
  INSERT INTO runtime_events (
    event_id, scope, scope_id, session_id, team_id, turn_id,
    seq, kind, critical, payload_json, provider, provider_seq, created_at
  ) VALUES (
    'evt_proc_' || NEW.process_id || '_process.started',
    'process', NEW.process_id, NEW.owner_session_id, NULL, NULL,
    (SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events
       WHERE scope = 'process' AND scope_id = NEW.process_id),
    'process.started', 1,
    json_object('process_id', NEW.process_id, 'pid', NEW.pid,
      'cwd', NEW.cwd, 'execution_started_at', NEW.execution_started_at),
    NULL, NULL,
    COALESCE(NEW.execution_started_at, NEW.updated_at, NEW.admitted_at)
  );
END;

CREATE TRIGGER journal_managed_process_terminal
AFTER UPDATE OF status ON managed_processes
WHEN NEW.status IN ('completed', 'failed', 'timed_out', 'killed', 'canceled', 'interrupted')
  AND OLD.status NOT IN ('completed', 'failed', 'timed_out', 'killed', 'canceled', 'interrupted')
BEGIN
  INSERT INTO runtime_events (
    event_id, scope, scope_id, session_id, team_id, turn_id,
    seq, kind, critical, payload_json, provider, provider_seq, created_at
  ) VALUES (
    'evt_proc_' || NEW.process_id || '_process.' ||
      CASE NEW.status WHEN 'completed' THEN 'completed'
        WHEN 'failed' THEN 'failed' WHEN 'timed_out' THEN 'timed_out'
        WHEN 'killed' THEN 'killed' WHEN 'canceled' THEN 'canceled'
        WHEN 'interrupted' THEN 'interrupted' END,
    'process', NEW.process_id, NEW.owner_session_id, NULL, NULL,
    (SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events
       WHERE scope = 'process' AND scope_id = NEW.process_id),
    'process.' || CASE NEW.status WHEN 'completed' THEN 'completed'
        WHEN 'failed' THEN 'failed' WHEN 'timed_out' THEN 'timed_out'
        WHEN 'killed' THEN 'killed' WHEN 'canceled' THEN 'canceled'
        WHEN 'interrupted' THEN 'interrupted' END,
    1,
    json_object('process_id', NEW.process_id, 'status', NEW.status,
      'reason', NEW.terminal_reason, 'exit_code', NEW.exit_code,
      'signal', NEW.signal, 'execution_duration_ms', NEW.execution_duration_ms,
      'stdout_captured_bytes', NEW.stdout_captured_bytes,
      'stderr_captured_bytes', NEW.stderr_captured_bytes,
      'stdout_truncated', json(CASE WHEN NEW.stdout_truncated != 0 THEN 'true' ELSE 'false' END),
      'stderr_truncated', json(CASE WHEN NEW.stderr_truncated != 0 THEN 'true' ELSE 'false' END),
      'before_launch', json(CASE WHEN OLD.status IN ('queued', 'launch_reserved')
        THEN 'true' ELSE 'false' END)),
    NULL, NULL,
    COALESCE(NEW.terminal_recorded_at, NEW.ended_at, NEW.updated_at, NEW.admitted_at)
  );
END;

CREATE TRIGGER journal_managed_process_kill_requested
AFTER UPDATE OF cancel_requested ON managed_processes
WHEN OLD.cancel_requested = 0 AND NEW.cancel_requested = 1
  AND NEW.status = 'running'
BEGIN
  INSERT INTO runtime_events (
    event_id, scope, scope_id, session_id, team_id, turn_id,
    seq, kind, critical, payload_json, provider, provider_seq, created_at
  ) VALUES (
    'evt_proc_' || NEW.process_id || '_process.kill_requested',
    'process', NEW.process_id, NEW.owner_session_id, NULL, NULL,
    (SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events
       WHERE scope = 'process' AND scope_id = NEW.process_id),
    'process.kill_requested', 1,
    json_object('process_id', NEW.process_id, 'reason', 'requested',
      'cancel_requested', json('true')),
    NULL, NULL, COALESCE(NEW.updated_at, NEW.admitted_at)
  );
END;
