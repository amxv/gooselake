use runtime_core::{RuntimeError, RuntimeSourceBootstrap, RuntimeSourceBootstrapRecords};
use rusqlite::{OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

pub(crate) const MAX_SOURCE_BOOTSTRAP_ROWS_PER_TABLE: i64 = 10_000;
pub(crate) const MAX_SOURCE_BOOTSTRAP_TOTAL_ROWS: i64 = 50_000;
pub(crate) const MAX_SOURCE_BOOTSTRAP_TEXT_BYTES: i64 = 16 * 1024 * 1024;
pub(crate) const MAX_SOURCE_BOOTSTRAP_SERIALIZED_BYTES: usize = 24 * 1024 * 1024;

impl SqliteRuntimeRepository {
    pub fn source_bootstrap(&self) -> Result<RuntimeSourceBootstrap, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| {
                db_error("failed to start source bootstrap read transaction", error)
            })?;

        // Read the watermark first. In WAL mode this pins the read snapshot before any
        // current-record query, so a concurrent writer cannot leak newer records into it.
        let high_watermark = transaction
            .query_row(
                "SELECT COALESCE(MAX(id), 0) FROM runtime_events",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| db_error("failed reading source high watermark", error))?;

        #[cfg(test)]
        run_after_watermark_read_hook(&self.database_path);

        let source_epoch = transaction
            .query_row(
                "SELECT source_epoch FROM source_metadata WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading source epoch", error))?
            .ok_or_else(|| {
                RuntimeError::Bootstrap(
                    "source identity is unavailable; initialize the runtime store first"
                        .to_string(),
                )
            })?;
        ensure_bootstrap_is_bounded(&transaction)?;
        let hydrated = self.hydrate_runtime_state_from_connection(&transaction, false)?;
        let records = RuntimeSourceBootstrapRecords {
            sessions: hydrated.sessions,
            approvals: hydrated.approvals,
            teams: hydrated.teams,
            team_members: hydrated.team_members,
            team_messages: hydrated.team_messages,
            team_deliveries: hydrated.team_deliveries,
            managed_worktrees: hydrated.managed_worktrees,
            managed_worktree_claims: hydrated.managed_worktree_claims,
            processes: hydrated.processes,
        };
        let serialized_bytes = serde_json::to_vec(&records)
            .map_err(|error| {
                RuntimeError::Bootstrap(format!("failed sizing source bootstrap: {error}"))
            })?
            .len();
        if serialized_bytes > MAX_SOURCE_BOOTSTRAP_SERIALIZED_BYTES {
            return Err(RuntimeError::Bootstrap(format!(
                "source bootstrap serializes to {serialized_bytes} bytes; maximum is {MAX_SOURCE_BOOTSTRAP_SERIALIZED_BYTES}"
            )));
        }

        transaction
            .commit()
            .map_err(|error| db_error("failed committing source bootstrap read", error))?;
        Ok(RuntimeSourceBootstrap {
            source_epoch,
            high_watermark,
            records,
        })
    }
}

fn ensure_bootstrap_is_bounded(connection: &rusqlite::Connection) -> Result<(), RuntimeError> {
    const TABLES: &[&str] = &[
        "sessions",
        "approvals",
        "teams",
        "team_members",
        "team_messages",
        "team_deliveries",
        "managed_worktrees",
        "managed_worktree_claims",
        "processes",
    ];
    let mut total_rows = 0_i64;
    for table in TABLES {
        let count = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|error| db_error(format!("failed counting bootstrap table {table}"), error))?;
        if count > MAX_SOURCE_BOOTSTRAP_ROWS_PER_TABLE {
            return Err(RuntimeError::Bootstrap(format!(
                "source bootstrap table {table} has {count} rows; maximum is {MAX_SOURCE_BOOTSTRAP_ROWS_PER_TABLE}"
            )));
        }
        total_rows += count;
    }
    if total_rows > MAX_SOURCE_BOOTSTRAP_TOTAL_ROWS {
        return Err(RuntimeError::Bootstrap(format!(
            "source bootstrap has {total_rows} total rows; maximum is {MAX_SOURCE_BOOTSTRAP_TOTAL_ROWS}"
        )));
    }
    let text_bytes = connection
        .query_row(
            "SELECT
              COALESCE((SELECT SUM(
                LENGTH(CAST(COALESCE(id,'') AS BLOB))+LENGTH(CAST(COALESCE(provider,'') AS BLOB))+
                LENGTH(CAST(COALESCE(status,'') AS BLOB))+LENGTH(CAST(COALESCE(cwd,'') AS BLOB))+
                LENGTH(CAST(COALESCE(model,'') AS BLOB))+LENGTH(CAST(COALESCE(permission_mode,'') AS BLOB))+
                LENGTH(CAST(COALESCE(system_prompt,'') AS BLOB))+LENGTH(CAST(COALESCE(metadata_json,'') AS BLOB))+
                LENGTH(CAST(COALESCE(provider_session_ref,'') AS BLOB))+LENGTH(CAST(COALESCE(canonical_provider_session_ref,'') AS BLOB))+
                LENGTH(CAST(COALESCE(active_turn_id,'') AS BLOB))+LENGTH(CAST(COALESCE(worktree_id,'') AS BLOB))+
                LENGTH(CAST(COALESCE(failure_code,'') AS BLOB))+LENGTH(CAST(COALESCE(failure_message,'') AS BLOB))) FROM sessions),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(COALESCE(id,'') AS BLOB))+LENGTH(CAST(COALESCE(session_id,'') AS BLOB))+
                LENGTH(CAST(COALESCE(turn_id,'') AS BLOB))+LENGTH(CAST(COALESCE(tool_call_id,'') AS BLOB))+
                LENGTH(CAST(COALESCE(provider_approval_ref,'') AS BLOB))+LENGTH(CAST(COALESCE(status,'') AS BLOB))+
                LENGTH(CAST(COALESCE(request_json,'') AS BLOB))+LENGTH(CAST(COALESCE(response_json,'') AS BLOB))) FROM approvals),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(id AS BLOB))+LENGTH(CAST(name AS BLOB))+LENGTH(CAST(lead_agent_id AS BLOB))+LENGTH(CAST(created_by AS BLOB))) FROM teams),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(team_id AS BLOB))+LENGTH(CAST(agent_id AS BLOB))+LENGTH(CAST(COALESCE(title,'') AS BLOB))+
                LENGTH(CAST(added_by AS BLOB))+LENGTH(CAST(COALESCE(creator_agent_id,'') AS BLOB))+
                LENGTH(CAST(creator_compaction_subscription AS BLOB))+LENGTH(CAST(COALESCE(worktree_id,'') AS BLOB))) FROM team_members),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(id AS BLOB))+LENGTH(CAST(team_id AS BLOB))+LENGTH(CAST(scope AS BLOB))+
                LENGTH(CAST(sender_agent_id AS BLOB))+LENGTH(CAST(recipient_agent_ids_json AS BLOB))+LENGTH(CAST(input_json AS BLOB))+
                LENGTH(CAST(image_paths_json AS BLOB))+LENGTH(CAST(priority AS BLOB))+LENGTH(CAST(policy AS BLOB))+
                LENGTH(CAST(COALESCE(correlation_id,'') AS BLOB))+LENGTH(CAST(COALESCE(reply_to_message_id,'') AS BLOB))+
                LENGTH(CAST(COALESCE(idempotency_key,'') AS BLOB))) FROM team_messages),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(id AS BLOB))+LENGTH(CAST(message_id AS BLOB))+LENGTH(CAST(team_id AS BLOB))+
                LENGTH(CAST(recipient_agent_id AS BLOB))+LENGTH(CAST(provider AS BLOB))+LENGTH(CAST(status AS BLOB))+
                LENGTH(CAST(COALESCE(effective_policy,'') AS BLOB))+LENGTH(CAST(COALESCE(injection_strategy,'') AS BLOB))+
                LENGTH(CAST(COALESCE(injected_turn_id,'') AS BLOB))+LENGTH(CAST(COALESCE(last_error_code,'') AS BLOB))+
                LENGTH(CAST(COALESCE(last_error_message,'') AS BLOB))) FROM team_deliveries),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(id AS BLOB))+LENGTH(CAST(repo_root AS BLOB))+LENGTH(CAST(worktree_root AS BLOB))+
                LENGTH(CAST(worktree_cwd AS BLOB))+LENGTH(CAST(branch_name AS BLOB))+LENGTH(CAST(worktree_name AS BLOB))+
                LENGTH(CAST(unified_workspace_path AS BLOB))+LENGTH(CAST(deletion_policy AS BLOB))+
                LENGTH(CAST(COALESCE(created_by_session_id,'') AS BLOB))+LENGTH(CAST(COALESCE(created_by_operation_id,'') AS BLOB))) FROM managed_worktrees),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(worktree_id AS BLOB))+LENGTH(CAST(session_id AS BLOB))+LENGTH(CAST(claim_role AS BLOB))) FROM managed_worktree_claims),0) +
              COALESCE((SELECT SUM(LENGTH(CAST(id AS BLOB))+LENGTH(CAST(COALESCE(session_id,'') AS BLOB))+
                LENGTH(CAST(COALESCE(tool_call_id,'') AS BLOB))+LENGTH(CAST(command_json AS BLOB))+LENGTH(CAST(COALESCE(cwd,'') AS BLOB))+
                LENGTH(CAST(status AS BLOB))+LENGTH(CAST(COALESCE(stdout_path,'') AS BLOB))+LENGTH(CAST(COALESCE(stderr_path,'') AS BLOB))) FROM processes),0)",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| db_error("failed measuring source bootstrap text bytes", error))?;
    if text_bytes > MAX_SOURCE_BOOTSTRAP_TEXT_BYTES {
        return Err(RuntimeError::Bootstrap(format!(
            "source bootstrap has {text_bytes} text bytes; maximum is {MAX_SOURCE_BOOTSTRAP_TEXT_BYTES}"
        )));
    }
    Ok(())
}

#[cfg(test)]
type BootstrapReadHook = (std::path::PathBuf, Box<dyn FnOnce() + Send + 'static>);

#[cfg(test)]
static AFTER_WATERMARK_READ_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<BootstrapReadHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn install_after_watermark_read_hook(
    database_path: std::path::PathBuf,
    hook: Box<dyn FnOnce() + Send + 'static>,
) {
    *AFTER_WATERMARK_READ_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("bootstrap read hook lock") = Some((database_path, hook));
}

#[cfg(test)]
fn run_after_watermark_read_hook(database_path: &std::path::Path) {
    let mut slot = AFTER_WATERMARK_READ_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("bootstrap read hook lock");
    let hook = if slot
        .as_ref()
        .is_some_and(|(hook_path, _)| hook_path == database_path)
    {
        slot.take().map(|(_, hook)| hook)
    } else {
        None
    };
    drop(slot);
    if let Some(hook) = hook {
        hook();
    }
}
