use runtime_core::{RuntimeError, RuntimeSourceBootstrap, RuntimeSourceBootstrapRecords};
use rusqlite::{OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

pub(crate) const MAX_SOURCE_BOOTSTRAP_ROWS_PER_TABLE: i64 = 10_000;

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
        run_after_watermark_read_hook();

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
        let hydrated = self.hydrate_runtime_state_from_connection(&transaction)?;
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
        "turns",
        "approvals",
        "teams",
        "team_members",
        "team_messages",
        "team_deliveries",
        "managed_worktrees",
        "managed_worktree_claims",
        "team_operation_journal",
        "team_operation_diagnostics",
        "processes",
        "credentials",
    ];
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
    }
    Ok(())
}

#[cfg(test)]
type BootstrapReadHook = Box<dyn FnOnce() + Send + 'static>;

#[cfg(test)]
static AFTER_WATERMARK_READ_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<BootstrapReadHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn install_after_watermark_read_hook(hook: BootstrapReadHook) {
    *AFTER_WATERMARK_READ_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("bootstrap read hook lock") = Some(hook);
}

#[cfg(test)]
fn run_after_watermark_read_hook() {
    let hook = AFTER_WATERMARK_READ_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("bootstrap read hook lock")
        .take();
    if let Some(hook) = hook {
        hook();
    }
}
