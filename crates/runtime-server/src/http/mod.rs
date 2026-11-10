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
    ProcessListRequest, ProcessLogReadRequest, ProcessRunRequest, ProcessSchedulerSettings,
    ProviderKind, ResumeSessionInput, RuntimeApp, RuntimeError, RuntimeEventRecord,
    RuntimeEventScope, RuntimeSessionManager, SendTurnAccepted, SendTurnInput,
    StartupRecoverySummary, TeamBroadcastRequest, TeamCancelMessageRequest, TeamCreateRequest,
    TeamDeliveryRecord, TeamGetDeliveriesRequest, TeamInterruptAllRequest, TeamJoinRequest,
    TeamListMessagesRequest, TeamMemberSpawnRequest, TeamMemberSpawnResponse,
    TeamMemberSpawnWorktreeInput, TeamRemoveMemberRequest, TeamRetryDeliveryRequest,
    TeamSendDirectRequest, TeamSetLeadRequest, TeamViewSnapshotRequest, ToolInvokeRequest,
    WorktreeClaimRequest, WorktreeCleanupRequest, WorktreeCreateRequest,
    WorktreeMemberRemovedRequest, WorktreeReleaseRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
#[cfg(test)]
use tokio_stream::StreamExt;

mod agent_controls;
mod auth;
mod diagnostics;
mod durable_event_stream;
mod events;
mod mcp;
mod messages;
mod operator_event_streams;
mod operator_events;
mod operator_hook_effects;
mod operator_processes;
mod processes;
mod route_table;
mod sessions;
mod shared;
mod teams;
mod v2_contract;
mod workspace_migrations;
mod workspace_retirement_review;
mod workspaces;

// Expose the actual handler DTOs to the typed contract generator, not alternate
// schema-only copies that might drift from serde behavior.
pub(crate) use agent_controls::{
    EditReviewInput, ManualCompactionReviewInput, UpdateAgentPermissionInput,
    UpdateAgentPreferencesInput,
};
pub(crate) use auth::{ProviderCapabilitiesResponse, RuntimeCapabilitiesResponse};
pub(crate) use messages::AgentMessageCreateInput;
pub(crate) use sessions::PublicSendTurnInput;
pub(crate) use v2_contract::OperatorErrorResponse;
pub(crate) use workspaces::WorkspaceWorktreeCreateInput;
mod worktrees;

use agent_controls::*;
use auth::*;
use diagnostics::*;
use durable_event_stream::*;
use events::*;
use mcp::*;
use messages::*;
use operator_event_streams::*;
use operator_events::*;
use operator_hook_effects::*;
use operator_processes::*;
use processes::*;
pub(crate) use route_table::route_table;
use sessions::*;
use shared::*;
use teams::*;
use v2_contract::*;
use workspace_migrations::*;
use workspace_retirement_review::*;
use workspaces::*;
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

// Every route is built from the same declarative table used for OpenAPI.
// Handler and method types are checked by Rust rather than guessed by text
// inspection; the nested scopes retain their original auth/body-size layers.
macro_rules! router_group {
    (
        $(
            $path:literal => $method:ident($handler:ident)
            $(.$next_method:ident($next_handler:ident))*
            $(@ $max_body:expr)?;
        )*
    ) => {
        Router::new()
            $(
                .route(
                    $path,
                    $method($handler)
                        $(.$next_method($next_handler))*
                        $(.layer(DefaultBodyLimit::max($max_body)))?
                )
            )*
    };
}

macro_rules! build_router_from_table {
    (
        $mcp:ident ($mcp_prefix:literal, $mcp_auth:literal) { $($mcp_route:tt)* }
        $scoped_mcp:ident ($scoped_prefix:literal, $scoped_auth:literal) { $($scoped_route:tt)* }
        $protected:ident ($v1_prefix:literal, $v1_auth:literal) { $($v1_route:tt)* }
        $protected_v2:ident ($v2_prefix:literal, $v2_auth:literal) { $($v2_route:tt)* }
        $root:ident ($root_prefix:literal, $root_auth:literal) { $($root_route:tt)* }
    ) => {
        pub fn build_router(state: AppState) -> Router {
            let $mcp = router_group! { $($mcp_route)* }
                .layer(DefaultBodyLimit::max(MCP_MAX_REQUEST_BODY_BYTES));
            // Agent-scoped MCP uses a separate principal and never inherits
            // the operator bearer credential.
            let $scoped_mcp = router_group! { $($scoped_route)* }
                .layer(DefaultBodyLimit::max(MCP_MAX_REQUEST_BODY_BYTES));
            let $protected = router_group! { $($v1_route)* }
                .nest("/mcp", $mcp)
                .route_layer(middleware::from_fn_with_state(
                    state.bearer_token.clone(),
                    bearer_auth,
                ));
            let $protected_v2 = router_group! { $($v2_route)* }
                .route_layer(middleware::from_fn_with_state(
                    state.bearer_token.clone(),
                    bearer_auth,
                ))
                .layer(middleware::from_fn(v2_operator_response_contract));

            router_group! { $($root_route)* }
                .nest($v1_prefix, $protected)
                .nest($v2_prefix, $protected_v2)
                .nest($scoped_prefix, $scoped_mcp)
                .with_state(state)
        }
    };
}

route_table!(build_router_from_table);

#[cfg(test)]
mod tests;
