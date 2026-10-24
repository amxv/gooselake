use std::path::PathBuf;

use serde::{Deserialize, Serialize};

mod db;
mod operation_tx;
mod repository;
mod repository_agent_comms;
mod repository_agent_tool;
mod repository_compaction;
mod repository_compaction_observations;
mod repository_context_limit;
mod repository_edit_rerun;
mod repository_hydration;
mod repository_process;
mod repository_team_hooks;
mod repository_turn_authority;
mod repository_upserts;
mod repository_workspace;
mod repository_workspace_agent;
mod repository_workspace_control;
mod repository_workspace_migration;
mod repository_workspace_notice;
mod repository_workspace_rebind;
mod repository_workspace_retirement;
mod repository_workspace_retirement_admission;
mod repository_workspace_retirement_close_receipt;
mod repository_workspace_retirement_events;
mod repository_workspace_retirement_native_close;
mod repository_workspace_retirement_review;
mod repository_workspace_retirement_review_settlement;
mod repository_workspace_retirement_review_turns;
mod repository_workspace_retirement_settlement;
mod repository_workspace_retirement_turns;
mod schema;
mod store;
mod store_retirement;

pub use repository::SqliteRuntimeRepository;
pub use store::SqliteRuntimeStore;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteStoreConfig {
    pub database_path: PathBuf,
}

#[cfg(test)]
mod tests;
