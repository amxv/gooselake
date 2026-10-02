pub(crate) const SCHEMA_VERSION: i64 = 5;

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

const MIGRATION_3_SQL: &str = r#"
CREATE TABLE retired_workspaces (
  workspace_id TEXT PRIMARY KEY REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  canonical_root TEXT NOT NULL UNIQUE,
  retired_by_operation_id TEXT REFERENCES runtime_operations(operation_id),
  retired_at INTEGER NOT NULL,
  CHECK (length(trim(canonical_root)) > 0)
);

CREATE TABLE workspace_session_ownership (
  session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE RESTRICT,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
  source TEXT NOT NULL,
  source_operation_id TEXT REFERENCES runtime_operations(operation_id),
  created_at INTEGER NOT NULL,
  UNIQUE (workspace_id, session_id),
  CHECK (length(trim(source)) > 0)
);

CREATE TRIGGER workspace_session_ownership_workspace_immutable
BEFORE UPDATE OF workspace_id ON workspace_session_ownership
WHEN OLD.workspace_id <> NEW.workspace_id
BEGIN
  SELECT RAISE(ABORT, 'workspace session ownership is immutable');
END;

CREATE TABLE workspace_agent_profiles (
  session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE RESTRICT,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
  title TEXT,
  title_provenance TEXT NOT NULL,
  added_by TEXT NOT NULL,
  creator_session_id TEXT REFERENCES sessions(id),
  creator_compaction_subscription TEXT NOT NULL DEFAULT 'auto',
  joined_at INTEGER NOT NULL,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (workspace_id, session_id)
    REFERENCES workspace_session_ownership(workspace_id, session_id) ON DELETE RESTRICT,
  CHECK (length(trim(title_provenance)) > 0),
  CHECK (length(trim(added_by)) > 0),
  CHECK (length(trim(creator_compaction_subscription)) > 0)
);

CREATE INDEX idx_workspace_agent_profiles_workspace
ON workspace_agent_profiles(workspace_id, joined_at, session_id);

CREATE TABLE legacy_workspace_migration_subjects (
  subject_kind TEXT NOT NULL CHECK (
    subject_kind IN ('session', 'team', 'team_member', 'managed_worktree', 'worktree_claim')
  ),
  subject_id TEXT NOT NULL,
  classification TEXT NOT NULL CHECK (
    classification IN ('mapped', 'archived_history', 'unresolved')
  ),
  workspace_id TEXT REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
  canonical_root TEXT,
  git_common_dir TEXT,
  repository_fingerprint TEXT,
  reason_code TEXT NOT NULL,
  evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
  resolution_source TEXT NOT NULL CHECK (
    resolution_source IN ('deterministic', 'operator_map', 'operator_archive')
  ),
  applied_at INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (subject_kind, subject_id),
  CHECK (length(trim(subject_id)) > 0),
  CHECK (length(trim(reason_code)) > 0)
);

CREATE INDEX idx_legacy_workspace_migration_classification
ON legacy_workspace_migration_subjects(classification, subject_kind, subject_id);

CREATE INDEX idx_legacy_workspace_migration_workspace
ON legacy_workspace_migration_subjects(workspace_id, subject_kind, subject_id)
WHERE workspace_id IS NOT NULL;

CREATE TABLE legacy_workspace_migration_state (
  migration_key TEXT PRIMARY KEY,
  previewed_at INTEGER NOT NULL,
  CHECK (migration_key = 'workspace_authority')
);
"#;

const MIGRATION_4_SQL: &str = r#"
ALTER TABLE approvals ADD COLUMN origin TEXT NOT NULL DEFAULT 'legacy';

CREATE UNIQUE INDEX idx_approvals_provider_identity
ON approvals(session_id, turn_id, provider_approval_ref)
WHERE provider_approval_ref IS NOT NULL;

CREATE TABLE turn_admissions (
  turn_id TEXT PRIMARY KEY REFERENCES turns(id) ON DELETE CASCADE,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
  provider TEXT NOT NULL,
  projection_source TEXT NOT NULL CHECK (
    projection_source IN ('user_visible', 'agent_message_delivery_transport', 'automation_context')
  ),
  user_input_snapshot_json TEXT NOT NULL CHECK (json_valid(user_input_snapshot_json)),
  dispatch_policy_json TEXT NOT NULL CHECK (json_valid(dispatch_policy_json)),
  correlation_json TEXT NOT NULL CHECK (json_valid(correlation_json)),
  dispatch_state TEXT NOT NULL CHECK (
    dispatch_state IN ('pending', 'dispatching', 'dispatched', 'not_dispatched', 'unknown')
  ),
  provider_native_turn_id TEXT,
  dispatch_error_json TEXT CHECK (dispatch_error_json IS NULL OR json_valid(dispatch_error_json)),
  admitted_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK (length(trim(provider)) > 0)
);

CREATE UNIQUE INDEX idx_turn_admissions_provider_native
ON turn_admissions(session_id, provider_native_turn_id)
WHERE provider_native_turn_id IS NOT NULL;

CREATE INDEX idx_turn_admissions_session_state
ON turn_admissions(session_id, dispatch_state, admitted_at, turn_id);
"#;

const MIGRATION_5_SQL: &str = r#"
CREATE TABLE workspace_agents (
  session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE RESTRICT,
  workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
  identity_alias TEXT NOT NULL UNIQUE,
  lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'archived')),
  recreation_policy_json TEXT NOT NULL CHECK (json_valid(recreation_policy_json)),
  archived_at INTEGER,
  archive_reason TEXT,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (workspace_id, session_id)
    REFERENCES workspace_session_ownership(workspace_id, session_id) ON DELETE RESTRICT,
  CHECK (length(trim(identity_alias)) > 0),
  CHECK (
    (lifecycle_state = 'active' AND archived_at IS NULL)
    OR (lifecycle_state = 'archived' AND archived_at IS NOT NULL)
  )
);

CREATE INDEX idx_workspace_agents_workspace_lifecycle
ON workspace_agents(workspace_id, lifecycle_state, created_at, session_id);

CREATE TRIGGER workspace_agents_workspace_immutable
BEFORE UPDATE OF workspace_id ON workspace_agents
WHEN OLD.workspace_id <> NEW.workspace_id
BEGIN
  SELECT RAISE(ABORT, 'workspace agent ownership is immutable');
END;

CREATE TRIGGER workspace_agents_alias_immutable
BEFORE UPDATE OF identity_alias ON workspace_agents
WHEN OLD.identity_alias <> NEW.identity_alias
BEGIN
  SELECT RAISE(ABORT, 'workspace agent alias is immutable');
END;
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
    Migration {
        version: 3,
        sql: MIGRATION_3_SQL,
    },
    Migration {
        version: 4,
        sql: MIGRATION_4_SQL,
    },
    Migration {
        version: 5,
        sql: MIGRATION_5_SQL,
    },
];
