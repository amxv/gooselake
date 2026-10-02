pub(crate) const SCHEMA_VERSION: i64 = 2;

pub(crate) struct Migration {
    pub version: i64,
    pub sql: &'static str,
}

pub(crate) const MIGRATION_1_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
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

const MIGRATION_2_SQL: &str = r#"
CREATE TABLE workspaces (
  workspace_id TEXT PRIMARY KEY,
  canonical_root TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'retired')),
  revision INTEGER NOT NULL CHECK (revision >= 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK (length(trim(workspace_id)) > 0),
  CHECK (length(trim(canonical_root)) > 0),
  CHECK (length(trim(display_name)) > 0)
);

CREATE TABLE runtime_operations (
  operation_id TEXT PRIMARY KEY,
  workspace_id TEXT REFERENCES workspaces(workspace_id) DEFERRABLE INITIALLY DEFERRED,
  kind TEXT NOT NULL,
  actor_kind TEXT NOT NULL CHECK (actor_kind IN ('operator', 'agent', 'system')),
  actor_id TEXT NOT NULL,
  idempotency_key TEXT,
  normalized_request_hash TEXT NOT NULL,
  normalized_request_json TEXT NOT NULL CHECK (json_valid(normalized_request_json)),
  phase TEXT NOT NULL CHECK (phase IN ('requested', 'manual_review', 'completed', 'failed')),
  exact_terminal_result_json TEXT CHECK (
    exact_terminal_result_json IS NULL OR json_valid(exact_terminal_result_json)
  ),
  error_code TEXT,
  error_message TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK (length(trim(operation_id)) > 0),
  CHECK (length(trim(kind)) > 0),
  CHECK (length(trim(actor_id)) > 0),
  CHECK (length(trim(normalized_request_hash)) > 0),
  CHECK (
    (phase IN ('completed', 'failed') AND exact_terminal_result_json IS NOT NULL)
    OR (phase NOT IN ('completed', 'failed') AND exact_terminal_result_json IS NULL)
  )
);

CREATE UNIQUE INDEX idx_runtime_operations_actor_idempotency
ON runtime_operations(actor_kind, actor_id, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_runtime_operations_recovery
ON runtime_operations(updated_at, operation_id)
WHERE phase NOT IN ('completed', 'failed');

CREATE TABLE runtime_operation_resource_fences (
  resource_kind TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  last_generation INTEGER NOT NULL CHECK (last_generation >= 0),
  PRIMARY KEY (resource_kind, resource_id)
);

CREATE TABLE runtime_operation_resource_claims (
  resource_kind TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  claim_mode TEXT NOT NULL CHECK (claim_mode = 'exclusive'),
  owner_operation_id TEXT NOT NULL REFERENCES runtime_operations(operation_id) ON DELETE CASCADE,
  fence_generation INTEGER NOT NULL CHECK (fence_generation > 0),
  acquired_at INTEGER NOT NULL,
  PRIMARY KEY (resource_kind, resource_id, claim_mode)
);

CREATE INDEX idx_runtime_operation_claim_owner
ON runtime_operation_resource_claims(owner_operation_id, resource_kind, resource_id);

CREATE TABLE runtime_operation_transitions (
  operation_id TEXT NOT NULL REFERENCES runtime_operations(operation_id) ON DELETE CASCADE,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  from_phase TEXT CHECK (from_phase IS NULL OR from_phase IN ('requested', 'manual_review', 'completed', 'failed')),
  to_phase TEXT NOT NULL CHECK (to_phase IN ('requested', 'manual_review', 'completed', 'failed')),
  evidence_json TEXT CHECK (evidence_json IS NULL OR json_valid(evidence_json)),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (operation_id, sequence)
);

CREATE TABLE runtime_operation_effects (
  effect_id TEXT PRIMARY KEY,
  operation_id TEXT NOT NULL REFERENCES runtime_operations(operation_id) ON DELETE CASCADE,
  effect_kind TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('intended', 'started', 'observed', 'finalized', 'uncertain', 'manual_review')),
  idempotency_key TEXT NOT NULL,
  evidence_json TEXT CHECK (evidence_json IS NULL OR json_valid(evidence_json)),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (operation_id, idempotency_key)
);

CREATE TABLE runtime_operation_outbox (
  outbox_id TEXT PRIMARY KEY,
  operation_id TEXT NOT NULL REFERENCES runtime_operations(operation_id) ON DELETE CASCADE,
  delivery_kind TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  state TEXT NOT NULL CHECK (state IN ('pending', 'delivering', 'delivered', 'failed', 'manual_review')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  next_attempt_at INTEGER,
  last_error_json TEXT CHECK (last_error_json IS NULL OR json_valid(last_error_json)),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  delivered_at INTEGER,
  UNIQUE (operation_id, idempotency_key)
);

CREATE INDEX idx_runtime_operation_outbox_retry
ON runtime_operation_outbox(state, next_attempt_at, created_at, outbox_id);

CREATE TABLE runtime_operation_outbox_receipts (
  outbox_id TEXT PRIMARY KEY REFERENCES runtime_operation_outbox(outbox_id) ON DELETE CASCADE,
  idempotency_key TEXT NOT NULL UNIQUE,
  received_at INTEGER NOT NULL,
  receipt_json TEXT CHECK (receipt_json IS NULL OR json_valid(receipt_json))
);
"#;

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: MIGRATION_1_SQL,
    },
    Migration {
        version: 2,
        sql: MIGRATION_2_SQL,
    },
];
