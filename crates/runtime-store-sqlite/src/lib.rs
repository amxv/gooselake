use std::path::PathBuf;

use serde::{Deserialize, Serialize};

mod db;
mod event_notifications;
mod operation_tx;
mod repository;
mod repository_agent_comms;
mod repository_agent_delivery_cas;
mod repository_agent_delivery_guard;
mod repository_agent_delivery_operator;
mod repository_agent_delivery_retry;
mod repository_agent_message_pages;
mod repository_agent_tool;
mod repository_compaction;
mod repository_compaction_observations;
mod repository_context_limit;
mod repository_edit_rerun;
mod repository_event_history;
mod repository_hydration;
mod repository_operator_agent_archive;
mod repository_operator_agent_archive_receipt;
mod repository_operator_agent_archive_recovery;
mod repository_operator_agent_archive_review;
mod repository_operator_agent_create;
mod repository_operator_agent_create_receipt;
mod repository_operator_agent_create_recovery;
mod repository_operator_agent_create_review;
mod repository_operator_agent_restore;
mod repository_operator_agent_restore_receipt;
mod repository_operator_agent_restore_recovery;
mod repository_operator_agent_restore_review;
mod repository_operator_native_policy;
mod repository_operator_native_policy_receipt;
mod repository_operator_native_policy_recovery;
mod repository_operator_native_policy_review;
mod repository_operator_user_turn;
mod repository_process;
mod repository_team_hooks;
mod repository_turn_authority;
mod repository_upserts;
mod repository_workspace;
mod repository_workspace_agent;
mod repository_workspace_agent_lifecycle;
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
mod repository_workspace_worktree_creation;
mod schema;
mod store;
mod store_context_limit;
mod store_event_history;
mod store_message_operator;
mod store_operator_agent_archive;
mod store_operator_agent_create;
mod store_operator_agent_restore;
mod store_operator_native_policy;
mod store_operator_user_turn;
mod store_pagination;
mod store_retirement;
mod store_worktree_creation;

pub use repository::SqliteRuntimeRepository;
pub use store::SqliteRuntimeStore;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteStoreConfig {
    pub database_path: PathBuf,
}

#[cfg(test)]
mod tests;
