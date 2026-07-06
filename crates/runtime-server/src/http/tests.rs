use super::*;
use axum::body::to_bytes;
use axum::http::Request;
use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCreateSessionRequest, ProviderInterruptTurnRequest, ProviderMetadata, ProviderModel,
    ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderSession, ProviderTurnAck,
    ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeProvider, RuntimeStore,
    RuntimeTeamCommsConfig, RuntimeTeamCommsService,
};
use runtime_provider_claude::{
    standalone_claude_bridge_command_path, standalone_gg_mcp_server_command_path,
};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
use runtime_tools::{
    ProcessManagerConfig, RuntimeProcessManager, RuntimeToolGateway, RuntimeToolGatewayDeps,
    RuntimeWorktreeService, TeamMcpPolicy, TeamModelPreset, WorktreeServiceConfig,
};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use tower::ServiceExt;

use crate::bootstrap::bootstrap_runtime;
use crate::config::RuntimeServerConfig;

mod support;
use support::*;

mod claude_acp_smoke;
mod codex_runtime_smoke;
use codex_runtime_smoke::{codex_test_model, create_test_session};
mod mcp_core;
mod mcp_policy_process;
mod process_more;
mod session_basics;
mod team_routes;
mod worktree_create_cleanup;
mod worktree_existing_smoke;
