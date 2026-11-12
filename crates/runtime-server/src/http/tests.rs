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
mod support_mixed_router;
use support_mixed_router::*;
mod support_onboarding;
use support_onboarding::*;
mod support_acp_http;
use support_acp_http::*;

mod claude_acp_smoke;
mod codex_edit_rerun_live;
mod codex_runtime_smoke;
mod durable_event_stream_idle;
mod durable_event_stream_lag;
mod durable_event_stream_restart;
mod durable_event_stream_routes;
use codex_runtime_smoke::{codex_test_model, create_test_session, prepare_codex_smoke_config};
mod agent_delivery_operator_retry;
mod agent_delivery_pages;
mod agent_message_routes;
mod agent_session_controls;
mod agent_session_controls_compaction;
mod agent_session_controls_edit_rerun;
mod legacy_team_operation_recovery;
mod legacy_team_spawn_idempotency;
mod mcp_core;
mod mcp_policy_process;
mod native_approval_journal_http;
mod operator_agent_archive;
mod operator_agent_create;
mod operator_agent_create_contract;
mod operator_agent_restore;
mod operator_contract_routes;
mod operator_event_pages;
mod operator_event_stream_routes;
mod operator_hook_approval_idempotency;
mod operator_hook_effect_pages;
mod operator_lead_inputs;
mod operator_native_policy;
mod operator_native_policy_review;
mod operator_notice_policy_idempotency;
mod operator_operation_pages;
mod operator_process_pages;
mod operator_rebind_etag;
mod operator_rebind_unknown;
mod operator_success_headers;
mod operator_unjournaled_idempotency;
mod operator_unsupported_preconditions;
mod operator_user_turn_idempotency;
mod operator_worktree_idempotency;
mod pre_dispatch_approval_journal_assertions;
mod process_lifecycle_journal_http;
mod process_more;
mod provider_contract_routes;
mod provider_failure_journal_http;
mod scoped_mcp;
mod session_basics;
mod session_lifecycle_journal_http;
mod session_terminal_journal_http;
mod sse_cursor_headers;
mod team_routes;
mod workspace_agent_routes;
mod workspace_control_routes;
mod workspace_migration_routes;
mod workspace_retirement_live;
mod workspace_retirement_review;
mod workspace_retirement_routes;
mod workspace_retirement_support;
mod workspace_routes;
mod workspace_worktree_creation;
mod workspace_worktree_failures;
mod workspace_worktree_routes;
mod workspace_worktree_status_projection;
mod worktree_create_cleanup;
mod worktree_existing_smoke;
