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
mod schema;
mod store;

pub use repository::SqliteRuntimeRepository;
pub use store::SqliteRuntimeStore;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteStoreConfig {
    pub database_path: PathBuf,
}

#[cfg(test)]
mod tests;
