use std::path::PathBuf;

use serde::{Deserialize, Serialize};

mod db;
mod operation_tx;
mod repository;
mod repository_agent_comms;
mod repository_hydration;
mod repository_process;
mod repository_turn_authority;
mod repository_upserts;
mod repository_workspace;
mod repository_workspace_agent;
mod repository_workspace_control;
mod repository_workspace_migration;
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
