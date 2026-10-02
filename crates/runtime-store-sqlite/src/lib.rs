use std::path::PathBuf;

use serde::{Deserialize, Serialize};

mod db;
mod operation_tx;
mod repository;
mod repository_hydration;
mod repository_upserts;
mod repository_workspace;
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
