use std::collections::BTreeMap;
use std::sync::Arc;

use crate::openapi::generated_openapi_yaml;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use runtime_core::{
    ApprovalResponseInput, CreateSessionInput, ProcessGetRequest, ProcessKillRequest,
    ProcessListRequest, ProcessLogReadRequest, ProcessRunRequest, ProviderKind, ResumeSessionInput,
    RuntimeApp, RuntimeError, RuntimeEventRecord, RuntimeEventScope, RuntimeSessionManager,
    SendTurnAccepted, SendTurnInput, StartupRecoverySummary, TeamBroadcastRequest,
    TeamCancelMessageRequest, TeamCreateRequest, TeamDeliveryRecord, TeamGetDeliveriesRequest,
    TeamInterruptAllRequest, TeamJoinRequest, TeamListMessagesRequest, TeamMemberSpawnRequest,
    TeamMemberSpawnResponse, TeamMemberSpawnWorktreeInput, TeamRemoveMemberRequest,
    TeamRetryDeliveryRequest, TeamSendDirectRequest, TeamSetLeadRequest, TeamViewSnapshotRequest,
    ToolInvokeRequest, WorktreeClaimRequest, WorktreeCleanupRequest, WorktreeCreateRequest,
    WorktreeMemberRemovedRequest, WorktreeReleaseRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tokio_stream::StreamExt;

mod auth;
mod diagnostics;
mod events;
mod mcp;
mod processes;
mod sessions;
mod shared;
mod teams;
mod worktrees;

use auth::*;
use diagnostics::*;
use events::*;
use mcp::*;
use processes::*;
use sessions::*;
use shared::*;
use teams::*;
use worktrees::*;

const MCP_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub app: Arc<RuntimeApp>,
    pub runtime: Arc<RuntimeSessionManager>,
    pub bearer_token: String,
    pub public_base_url: String,
    pub startup_recovery: Arc<StartupRecoverySummary>,
}

pub fn build_router(state: AppState) -> Router {
    let mcp = Router::new()
        .route("/capabilities", get(mcp_capabilities))
        .route("/invoke", post(mcp_invoke))
        .layer(DefaultBodyLimit::max(MCP_MAX_REQUEST_BODY_BYTES));

    let protected = Router::new()
        .route("/health", get(protected_health))
        .route("/providers", get(list_providers))
        .route("/providers/{provider}/models", get(list_provider_models))
        .route("/providers/codex/auth/status", get(codex_auth_status))
        .route("/providers/acp/auth/status", get(acp_auth_status))
        .route("/providers/claude/auth/status", get(claude_auth_status))
        .route("/providers/claude/auth/api-key", post(claude_auth_api_key))
        .route(
            "/providers/claude/auth/import-json",
            post(claude_auth_import_json),
        )
        .route(
            "/providers/claude/auth/import-file",
            post(claude_auth_import_file),
        )
        .route("/providers/claude/auth/logout", post(claude_auth_logout))
        .route("/openapi.yaml", get(openapi_yaml))
        .route("/version", get(version))
        .route("/bootstrap", get(source_bootstrap))
        .route("/events", get(replay_global_events))
        .route("/events/stream", get(stream_global_events))
        .route("/sessions", post(create_session).get(list_sessions))
        .route("/sessions/{session_id}", get(get_session))
        .route("/sessions/{session_id}/resume", post(resume_session))
        .route("/sessions/{session_id}/close", post(close_session))
        .route("/sessions/{session_id}/turns", post(send_turn))
        .route(
            "/sessions/{session_id}/turns/{turn_id}/interrupt",
            post(interrupt_turn),
        )
        .route(
            "/sessions/{session_id}/approvals/{approval_id}",
            post(respond_approval),
        )
        .route("/sessions/{session_id}/events", get(replay_session_events))
        .route(
            "/sessions/{session_id}/events/stream",
            get(stream_session_events),
        )
        .route("/processes", post(start_process).get(list_processes))
        .route("/processes/{process_id}", get(get_process))
        .route("/processes/{process_id}/logs", get(get_process_logs))
        .route("/processes/{process_id}/events", get(replay_process_events))
        .route(
            "/processes/{process_id}/events/stream",
            get(stream_process_events),
        )
        .route("/processes/{process_id}/kill", post(kill_process))
        .route("/worktrees", post(create_worktree).get(list_worktrees))
        .route("/worktrees/{worktree_id}", get(get_worktree))
        .route("/worktrees/{worktree_id}/claims", post(claim_worktree))
        .route("/worktrees/{worktree_id}/release", post(release_worktree))
        .route("/worktrees/{worktree_id}/cleanup", post(cleanup_worktree))
        .route("/diagnostics", get(runtime_diagnostics))
        .route("/diagnostics/providers", get(provider_diagnostics))
        .route("/diagnostics/comms", get(comms_diagnostics))
        .route("/diagnostics/processes", get(process_diagnostics))
        .route("/diagnostics/worktrees", get(worktree_diagnostics))
        .route("/diagnostics/recovery", get(recovery_diagnostics))
        .route(
            "/diagnostics/team-operations",
            get(list_team_operation_diagnostics),
        )
        .route("/teams", post(create_team).get(list_teams))
        .route("/teams/{team_id}", get(get_team).delete(delete_team))
        .route("/teams/{team_id}/members", post(join_team_member))
        .route("/teams/{team_id}/members/spawn", post(spawn_team_member))
        .route(
            "/teams/{team_id}/members/{agent_id}",
            delete(remove_team_member),
        )
        .route("/teams/{team_id}/lead", post(set_team_lead))
        .route(
            "/teams/{team_id}/messages",
            post(send_team_direct).get(list_team_messages),
        )
        .route("/teams/{team_id}/broadcasts", post(send_team_broadcast))
        .route("/teams/{team_id}/deliveries", get(list_team_deliveries))
        .route(
            "/teams/{team_id}/deliveries/{delivery_id}/retry",
            post(retry_team_delivery),
        )
        .route(
            "/teams/{team_id}/messages/{message_id}/cancel",
            post(cancel_team_message),
        )
        .route("/teams/{team_id}/view", get(get_team_view_snapshot))
        .route("/teams/{team_id}/events", get(replay_team_events))
        .route("/teams/{team_id}/events/stream", get(stream_team_events))
        .route(
            "/teams/{team_id}/interrupt-all",
            post(interrupt_all_team_turns),
        )
        .nest("/mcp", mcp)
        .route_layer(middleware::from_fn_with_state(
            state.bearer_token.clone(),
            bearer_auth,
        ));

    Router::new()
        .route("/health", get(health))
        .route("/openapi.yaml", get(openapi_yaml))
        .nest("/v1", protected)
        .with_state(state)
}

#[cfg(test)]
mod tests;
