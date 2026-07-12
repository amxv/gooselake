pub(crate) const SCHEMA_VERSION: i64 = 2;

pub(crate) const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS source_metadata (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  source_epoch TEXT NOT NULL,
  generation_marker TEXT NOT NULL,
  database_fingerprint TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  cwd TEXT,
  model TEXT,
  permission_mode TEXT,
  system_prompt TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  provider_session_ref TEXT,
  canonical_provider_session_ref TEXT,
  active_turn_id TEXT,
  worktree_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  closed_at INTEGER,
  failure_code TEXT,
  failure_message TEXT
);

CREATE TABLE IF NOT EXISTS turns (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  provider_turn_ref TEXT,
  status TEXT NOT NULL,
  input_json TEXT NOT NULL,
  source TEXT,
  started_at INTEGER,
  completed_at INTEGER,
  usage_json TEXT,
  error_json TEXT
);

CREATE TABLE IF NOT EXISTS approvals (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  turn_id TEXT NOT NULL REFERENCES turns(id),
  tool_call_id TEXT,
  provider_approval_ref TEXT,
  status TEXT NOT NULL,
  request_json TEXT NOT NULL,
  response_json TEXT,
  created_at INTEGER NOT NULL,
  resolved_at INTEGER
);

CREATE TABLE IF NOT EXISTS runtime_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  scope TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  session_id TEXT,
  team_id TEXT,
  turn_id TEXT,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  critical INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  provider TEXT,
  provider_seq INTEGER,
  created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_scope_seq
ON runtime_events(scope, scope_id, seq);

CREATE TABLE IF NOT EXISTS teams (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  lead_agent_id TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  deleted_at INTEGER
);

CREATE TABLE IF NOT EXISTS team_members (
  team_id TEXT NOT NULL REFERENCES teams(id),
  agent_id TEXT NOT NULL REFERENCES sessions(id),
  title TEXT,
  joined_at INTEGER NOT NULL,
  added_by TEXT NOT NULL,
  creator_agent_id TEXT,
  creator_compaction_subscription TEXT NOT NULL DEFAULT 'auto',
  worktree_id TEXT,
  PRIMARY KEY (team_id, agent_id)
);

CREATE TABLE IF NOT EXISTS team_messages (
  id TEXT PRIMARY KEY,
  team_id TEXT NOT NULL REFERENCES teams(id),
  scope TEXT NOT NULL,
  sender_agent_id TEXT NOT NULL,
  recipient_agent_ids_json TEXT NOT NULL,
  input_json TEXT NOT NULL,
  image_paths_json TEXT NOT NULL DEFAULT '[]',
  priority TEXT NOT NULL,
  policy TEXT NOT NULL,
  correlation_id TEXT,
  reply_to_message_id TEXT,
  idempotency_key TEXT,
  created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_team_message_idempotency
ON team_messages(team_id, sender_agent_id, scope, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS team_deliveries (
  id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL REFERENCES team_messages(id),
  team_id TEXT NOT NULL REFERENCES teams(id),
  recipient_agent_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  effective_policy TEXT,
  injection_strategy TEXT,
  injected_turn_id TEXT,
  last_error_code TEXT,
  last_error_message TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS managed_worktrees (
  id TEXT PRIMARY KEY,
  repo_root TEXT NOT NULL,
  worktree_root TEXT NOT NULL,
  worktree_cwd TEXT NOT NULL,
  branch_name TEXT NOT NULL,
  worktree_name TEXT NOT NULL,
  unified_workspace_path TEXT NOT NULL,
  deletion_policy TEXT NOT NULL,
  created_by_session_id TEXT,
  created_by_operation_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(repo_root, worktree_cwd, branch_name)
);

CREATE TABLE IF NOT EXISTS managed_worktree_claims (
  worktree_id TEXT NOT NULL REFERENCES managed_worktrees(id),
  session_id TEXT NOT NULL REFERENCES sessions(id),
  claim_role TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  released_at INTEGER,
  PRIMARY KEY (worktree_id, session_id)
);

CREATE TABLE IF NOT EXISTS processes (
  id TEXT PRIMARY KEY,
  session_id TEXT,
  tool_call_id TEXT,
  pid INTEGER,
  command_json TEXT NOT NULL,
  cwd TEXT,
  status TEXT NOT NULL,
  exit_code INTEGER,
  signal INTEGER,
  stdout_path TEXT,
  stderr_path TEXT,
  started_at INTEGER NOT NULL,
  ended_at INTEGER,
  timeout_ms INTEGER
);

CREATE TABLE IF NOT EXISTS credentials (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  profile TEXT NOT NULL,
  kind TEXT NOT NULL,
  encrypted_secret TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(provider, profile, kind)
);

CREATE TABLE IF NOT EXISTS team_operation_journal (
  operation_id TEXT PRIMARY KEY,
  team_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  stage TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS team_operation_diagnostics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id TEXT,
  team_id TEXT,
  code TEXT NOT NULL,
  message TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS diagnostics_journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subsystem TEXT NOT NULL,
  severity TEXT NOT NULL,
  code TEXT NOT NULL,
  message TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
"#;
