use std::sync::Arc;

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
    SendTurnAccepted, SendTurnInput, TeamBroadcastRequest, TeamCancelMessageRequest,
    TeamCreateRequest, TeamDeliveryRecord, TeamGetDeliveriesRequest, TeamInterruptAllRequest,
    TeamJoinRequest, TeamListMessagesRequest, TeamMemberSpawnRequest, TeamMemberSpawnResponse,
    TeamMemberSpawnWorktreeInput, TeamRemoveMemberRequest, TeamRetryDeliveryRequest,
    TeamSendDirectRequest, TeamSetLeadRequest, TeamViewSnapshotRequest, ToolInvokeRequest,
    WorktreeClaimRequest, WorktreeCleanupRequest, WorktreeCreateRequest,
    WorktreeMemberRemovedRequest, WorktreeReleaseRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tokio_stream::StreamExt;

const MCP_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub app: Arc<RuntimeApp>,
    pub runtime: Arc<RuntimeSessionManager>,
    pub bearer_token: String,
    pub public_base_url: String,
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
        .route("/version", get(version))
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
        .nest("/v1", protected)
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    providers: usize,
    public_base_url: String,
}

#[derive(Debug, Serialize)]
struct VersionResponse {
    version: &'static str,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        providers: state.app.provider_registry.len(),
        public_base_url: state.public_base_url,
    })
}

async fn protected_health(State(state): State<AppState>) -> Json<HealthResponse> {
    health(State(state)).await
}

async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[derive(Debug, Serialize)]
struct ProviderListResponse {
    providers: Vec<runtime_core::ProviderMetadata>,
}

async fn list_providers(State(state): State<AppState>) -> Json<ProviderListResponse> {
    Json(ProviderListResponse {
        providers: state.app.provider_registry.metadata(),
    })
}

#[derive(Debug, Serialize)]
struct ProviderModelsResponse {
    provider: String,
    models: Vec<runtime_core::ProviderModel>,
}

async fn list_provider_models(
    State(state): State<AppState>,
    Path(provider): Path<String>,
) -> Result<Json<ProviderModelsResponse>, ApiError> {
    let provider = parse_provider_kind(provider.as_str())?;
    let adapter = state
        .app
        .provider_registry
        .get(provider)
        .ok_or_else(|| ApiError::not_found(format!("provider {}", provider.as_str())))?;
    let models = adapter.list_models().await.map_err(ApiError::from)?;
    Ok(Json(ProviderModelsResponse {
        provider: provider.as_str().to_string(),
        models,
    }))
}

async fn codex_auth_status(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    let status = state
        .runtime
        .provider_auth_status(ProviderKind::Codex)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(status))
}

async fn claude_auth_status(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    let status = state
        .runtime
        .provider_auth_status(ProviderKind::Claude)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(status))
}

#[derive(Debug, Deserialize)]
struct ClaudeApiKeyRequest {
    api_key: String,
}

async fn claude_auth_api_key(
    State(state): State<AppState>,
    Json(input): Json<ClaudeApiKeyRequest>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    let status = state
        .runtime
        .provider_auth_set_api_key(ProviderKind::Claude, input.api_key)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(status))
}

#[derive(Debug, Deserialize)]
struct ClaudeAuthImportJsonRequest {
    auth_json: Option<Value>,
    auth_json_text: Option<String>,
}

async fn claude_auth_import_json(
    State(state): State<AppState>,
    Json(input): Json<ClaudeAuthImportJsonRequest>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    if let Some(auth_json) = input.auth_json {
        let status = state
            .runtime
            .provider_auth_import_json(ProviderKind::Claude, auth_json)
            .await
            .map_err(ApiError::from)?;
        return Ok(Json(status));
    }
    if let Some(auth_json_text) = input.auth_json_text {
        let status = state
            .runtime
            .provider_auth_import_json_text(ProviderKind::Claude, auth_json_text)
            .await
            .map_err(ApiError::from)?;
        return Ok(Json(status));
    }
    Err(ApiError::bad_request(
        "expected auth_json or auth_json_text".to_string(),
    ))
}

async fn claude_auth_import_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid multipart payload: {error}")))?
    {
        let name = field.name().unwrap_or_default().to_string();
        if name != "file" {
            continue;
        }
        let bytes = field.bytes().await.map_err(|error| {
            ApiError::bad_request(format!("failed reading upload field: {error}"))
        })?;
        let auth_json_text = String::from_utf8(bytes.to_vec()).map_err(|error| {
            ApiError::bad_request(format!("uploaded file is not utf-8: {error}"))
        })?;
        let status = state
            .runtime
            .provider_auth_import_json_text(ProviderKind::Claude, auth_json_text)
            .await
            .map_err(ApiError::from)?;
        return Ok(Json(status));
    }
    Err(ApiError::bad_request(
        "multipart field 'file' is required".to_string(),
    ))
}

async fn claude_auth_logout(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    let status = state
        .runtime
        .provider_auth_logout(ProviderKind::Claude)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(status))
}

async fn create_session(
    State(state): State<AppState>,
    Json(input): Json<CreateSessionInput>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let session = state
        .runtime
        .create_session(input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

async fn list_sessions(State(state): State<AppState>) -> Json<Vec<runtime_core::SessionRecord>> {
    Json(state.runtime.list_sessions().await)
}

async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let session = state
        .runtime
        .get_session(session_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

async fn resume_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    input: Option<Json<ResumeSessionInput>>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let input = input
        .map(|Json(value)| value)
        .unwrap_or(ResumeSessionInput {
            provider_session_ref: None,
            canonical_provider_session_ref: None,
        });
    let session = state
        .runtime
        .resume_session(session_id.as_str(), input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

#[derive(Debug, Deserialize)]
struct CloseSessionInput {
    reason: Option<String>,
}

async fn close_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    input: Option<Json<CloseSessionInput>>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let reason = input.and_then(|Json(value)| value.reason);
    let session = state
        .runtime
        .close_session(session_id.as_str(), reason)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

async fn send_turn(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(input): Json<SendTurnInput>,
) -> Result<Json<SendTurnAccepted>, ApiError> {
    let accepted = state
        .runtime
        .send_turn(session_id.as_str(), input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(accepted))
}

async fn interrupt_turn(
    State(state): State<AppState>,
    Path((session_id, turn_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    state
        .runtime
        .interrupt_turn(session_id.as_str(), turn_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::ACCEPTED)
}

async fn respond_approval(
    State(state): State<AppState>,
    Path((session_id, approval_id)): Path<(String, String)>,
    Json(input): Json<ApprovalResponseInput>,
) -> Result<Json<runtime_core::ApprovalRecord>, ApiError> {
    let approval = state
        .runtime
        .respond_approval(session_id.as_str(), approval_id.as_str(), input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(approval))
}

#[derive(Debug, Deserialize)]
struct EventReplayQuery {
    after_seq: Option<i64>,
    limit: Option<usize>,
}

async fn replay_session_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .runtime
        .replay_session_events(
            session_id.as_str(),
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

async fn stream_session_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let _ = state
        .runtime
        .get_session(session_id.as_str())
        .await
        .map_err(ApiError::from)?;

    // Subscribe before replay to avoid missing events appended during replay/live handoff.
    let receiver = state.runtime.subscribe_events();
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok());
    let cursor = query.after_seq.or(last_event_id);
    let replay_page_limit = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    let mut replay_events = Vec::new();
    let mut replay_cursor = cursor;

    loop {
        let page = state
            .runtime
            .replay_session_events(session_id.as_str(), replay_cursor, replay_page_limit)
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        replay_cursor = page.last().map(|event| event.seq);
        let page_len = page.len();
        replay_events.extend(page);
        if page_len < replay_page_limit {
            break;
        }
    }

    #[cfg(test)]
    if let Some(delay_ms) = headers
        .get("x-gg-test-handoff-delay-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
    {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }

    let replay_high_watermark_seq = replay_cursor.or(cursor).unwrap_or(0);

    let replay_stream = tokio_stream::iter(replay_events.into_iter().filter_map(|event| {
        let payload = serde_json::to_string(&event).ok()?;
        Some(Ok(Event::default()
            .id(event.seq.to_string())
            .event(event.kind)
            .data(payload)))
    }));

    let live_stream = BroadcastStream::new(receiver).filter_map(move |next| match next {
        Ok(event) if event.session_id.as_deref() == Some(session_id.as_str()) => {
            if event.seq <= replay_high_watermark_seq {
                return None;
            }
            let payload = match serde_json::to_string(&event) {
                Ok(payload) => payload,
                Err(_) => return None,
            };
            Some(Ok(Event::default()
                .id(event.seq.to_string())
                .event(event.kind)
                .data(payload)))
        }
        Ok(_) => None,
        Err(_) => None,
    });
    let stream = replay_stream.chain(live_stream);

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

async fn replay_global_events(
    State(state): State<AppState>,
    Query(query): Query<EventReplayQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .app
        .services
        .store
        .list_runtime_events(
            None,
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

async fn stream_global_events(
    State(state): State<AppState>,
    Query(query): Query<EventReplayQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok());
    let cursor = query.after_seq.or(last_event_id);
    let replay_page_limit = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    let mut replay_events = Vec::new();
    let mut replay_cursor = cursor;

    loop {
        let page = state
            .app
            .services
            .store
            .list_runtime_events(None, replay_cursor, replay_page_limit)
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        replay_cursor = page.last().map(|event| event.row_id);
        let page_len = page.len();
        replay_events.extend(page);
        if page_len < replay_page_limit {
            break;
        }
    }

    let replay_high_watermark_seq = replay_cursor.or(cursor).unwrap_or(0);
    let replay_stream = tokio_stream::iter(replay_events.into_iter().filter_map(|event| {
        let payload = serde_json::to_string(&event).ok()?;
        Some(Ok(Event::default()
            .id(event.row_id.to_string())
            .event(event.kind)
            .data(payload)))
    }));

    let (tx, rx) = mpsc::channel::<Result<Event, std::convert::Infallible>>(128);
    let store = state.app.services.store.clone();
    tokio::spawn(async move {
        let mut cursor = replay_high_watermark_seq;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let page = match store.list_runtime_events(None, Some(cursor), replay_page_limit) {
                Ok(page) => page,
                Err(_) => continue,
            };
            if page.is_empty() {
                continue;
            }
            for event in page {
                cursor = cursor.max(event.row_id);
                let payload = match serde_json::to_string(&event) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                let sse = Event::default()
                    .id(event.row_id.to_string())
                    .event(event.kind)
                    .data(payload);
                if tx.send(Ok(sse)).await.is_err() {
                    return;
                }
            }
        }
    });

    let live_stream = ReceiverStream::new(rx);
    let stream = replay_stream.chain(live_stream);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

#[derive(Debug, Deserialize)]
struct TeamCreateInput {
    name: String,
    lead_agent_id: String,
    member_agent_ids: Option<Vec<String>>,
    created_by: Option<String>,
}

async fn create_team(
    State(state): State<AppState>,
    Json(input): Json<TeamCreateInput>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .create_team(TeamCreateRequest {
            name: input.name,
            lead_agent_id: input.lead_agent_id,
            member_agent_ids: input.member_agent_ids.unwrap_or_default(),
            created_by: input.created_by,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

async fn list_teams(
    State(state): State<AppState>,
) -> Result<Json<Vec<runtime_core::TeamWithMembers>>, ApiError> {
    let teams = state
        .app
        .services
        .team_comms
        .list_teams()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(teams))
}

async fn get_team(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .get_team(team_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

#[derive(Debug, Deserialize)]
struct TeamJoinInput {
    agent_id: String,
    title: Option<String>,
    added_by: Option<String>,
    creator_agent_id: Option<String>,
    creator_compaction_subscription: Option<String>,
    worktree_id: Option<String>,
}

async fn join_team_member(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamJoinInput>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .join_team(TeamJoinRequest {
            team_id,
            agent_id: input.agent_id,
            title: input.title,
            added_by: input.added_by,
            creator_agent_id: input.creator_agent_id,
            creator_compaction_subscription: input.creator_compaction_subscription,
            worktree_id: input.worktree_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

#[derive(Debug, Deserialize)]
struct TeamMemberSpawnInput {
    source_session_id: String,
    provider: Option<String>,
    model: Option<String>,
    title: Option<String>,
    prompt: Option<String>,
    permission_mode: Option<String>,
    metadata: Option<Value>,
    worktree: Option<TeamMemberSpawnWorktreeInput>,
    creator_agent_id: Option<String>,
    creator_compaction_subscription: Option<String>,
}

async fn spawn_team_member(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamMemberSpawnInput>,
) -> Result<Json<TeamMemberSpawnResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .spawn_team_member(TeamMemberSpawnRequest {
            team_id,
            source_session_id: input.source_session_id,
            provider: input.provider,
            model: input.model,
            title: input.title,
            prompt: input.prompt,
            permission_mode: input.permission_mode,
            metadata: input.metadata,
            worktree: input.worktree,
            creator_agent_id: input.creator_agent_id,
            creator_compaction_subscription: input.creator_compaction_subscription,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

async fn remove_team_member(
    State(state): State<AppState>,
    Path((team_id, agent_id)): Path<(String, String)>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let removed_agent_id = agent_id.clone();
    let team_id_for_cleanup = team_id.clone();
    let team = state
        .app
        .services
        .team_comms
        .remove_team_member(TeamRemoveMemberRequest { team_id, agent_id })
        .await
        .map_err(ApiError::from)?;
    // Cleanup is best effort by policy; membership removal must stand even if cleanup fails.
    let _ = state
        .app
        .services
        .worktrees
        .on_member_removed(WorktreeMemberRemovedRequest {
            team_id: team_id_for_cleanup,
            agent_id: removed_agent_id,
            removed_by: None,
        })
        .await;
    Ok(Json(team))
}

#[derive(Debug, Deserialize)]
struct TeamSetLeadInput {
    lead_agent_id: String,
}

async fn set_team_lead(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamSetLeadInput>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .set_team_lead(TeamSetLeadRequest {
            team_id,
            lead_agent_id: input.lead_agent_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

async fn delete_team(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .app
        .services
        .team_comms
        .delete_team(team_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct TeamDirectInput {
    sender_agent_id: String,
    recipient_agent_id: String,
    input: Value,
    image_paths: Option<Vec<String>>,
    priority: Option<String>,
    policy: Option<String>,
    correlation_id: Option<String>,
    reply_to_message_id: Option<String>,
    idempotency_key: Option<String>,
}

async fn send_team_direct(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamDirectInput>,
) -> Result<Json<runtime_core::TeamMessageAck>, ApiError> {
    let ack = state
        .app
        .services
        .team_comms
        .send_direct(TeamSendDirectRequest {
            team_id,
            sender_agent_id: input.sender_agent_id,
            recipient_agent_id: input.recipient_agent_id,
            input: input.input,
            image_paths: input.image_paths.unwrap_or_default(),
            priority: input.priority.unwrap_or_else(|| "normal".to_string()),
            policy: input
                .policy
                .unwrap_or_else(|| "non_interrupting".to_string()),
            correlation_id: input.correlation_id,
            reply_to_message_id: input.reply_to_message_id,
            idempotency_key: input.idempotency_key,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ack))
}

#[derive(Debug, Deserialize)]
struct TeamBroadcastInput {
    sender_agent_id: String,
    input: Value,
    image_paths: Option<Vec<String>>,
    priority: Option<String>,
    policy: Option<String>,
    include_sender: Option<bool>,
    correlation_id: Option<String>,
    idempotency_key: Option<String>,
}

async fn send_team_broadcast(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamBroadcastInput>,
) -> Result<Json<runtime_core::TeamMessageAck>, ApiError> {
    let ack = state
        .app
        .services
        .team_comms
        .broadcast(TeamBroadcastRequest {
            team_id,
            sender_agent_id: input.sender_agent_id,
            input: input.input,
            image_paths: input.image_paths.unwrap_or_default(),
            priority: input.priority.unwrap_or_else(|| "normal".to_string()),
            policy: input
                .policy
                .unwrap_or_else(|| "non_interrupting".to_string()),
            include_sender: input.include_sender.unwrap_or(false),
            correlation_id: input.correlation_id,
            idempotency_key: input.idempotency_key,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ack))
}

#[derive(Debug, Deserialize)]
struct TeamListMessagesQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn list_team_messages(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<TeamListMessagesQuery>,
) -> Result<Json<runtime_core::TeamListMessagesResponse>, ApiError> {
    let response = state
        .app
        .services
        .team_comms
        .list_messages(TeamListMessagesRequest {
            team_id,
            cursor: query.cursor,
            limit: query.limit,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct TeamDeliveriesQuery {
    message_id: Option<String>,
    recipient_agent_id: Option<String>,
}

async fn list_team_deliveries(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<TeamDeliveriesQuery>,
) -> Result<Json<Vec<TeamDeliveryRecord>>, ApiError> {
    let deliveries = state
        .app
        .services
        .team_comms
        .get_deliveries(TeamGetDeliveriesRequest {
            team_id,
            message_id: query.message_id,
            recipient_agent_id: query.recipient_agent_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(deliveries))
}

async fn retry_team_delivery(
    State(state): State<AppState>,
    Path((team_id, delivery_id)): Path<(String, String)>,
) -> Result<Json<TeamDeliveryRecord>, ApiError> {
    let delivery = state
        .app
        .services
        .team_comms
        .retry_delivery(TeamRetryDeliveryRequest {
            team_id,
            delivery_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(delivery))
}

async fn cancel_team_message(
    State(state): State<AppState>,
    Path((team_id, message_id)): Path<(String, String)>,
) -> Result<Json<Vec<TeamDeliveryRecord>>, ApiError> {
    let rows = state
        .app
        .services
        .team_comms
        .cancel_message(TeamCancelMessageRequest {
            team_id,
            message_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize)]
struct TeamSnapshotQuery {
    message_cursor: Option<String>,
    message_limit: Option<usize>,
    include_delivery_map: Option<bool>,
    delivery_recipient_filter: Option<String>,
}

async fn get_team_view_snapshot(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<TeamSnapshotQuery>,
) -> Result<Json<runtime_core::TeamViewSnapshotResponse>, ApiError> {
    let snapshot = state
        .app
        .services
        .team_comms
        .get_view_snapshot(TeamViewSnapshotRequest {
            team_id,
            message_cursor: query.message_cursor,
            message_limit: query.message_limit,
            include_delivery_map: query.include_delivery_map,
            delivery_recipient_filter: query.delivery_recipient_filter,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

async fn replay_team_events(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .app
        .services
        .team_comms
        .replay_team_events(
            team_id.as_str(),
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

async fn stream_team_events(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let _ = state
        .app
        .services
        .team_comms
        .get_team(team_id.as_str())
        .await
        .map_err(ApiError::from)?;

    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok());
    let cursor = query.after_seq.or(last_event_id);
    let replay_page_limit = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    let mut replay_events = Vec::new();
    let mut replay_cursor = cursor;

    loop {
        let page = state
            .app
            .services
            .team_comms
            .replay_team_events(team_id.as_str(), replay_cursor, replay_page_limit)
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        replay_cursor = page.last().map(|event| event.seq);
        let page_len = page.len();
        replay_events.extend(page);
        if page_len < replay_page_limit {
            break;
        }
    }

    let replay_high_watermark_seq = replay_cursor.or(cursor).unwrap_or(0);
    let replay_stream = tokio_stream::iter(replay_events.into_iter().filter_map(|event| {
        let payload = serde_json::to_string(&event).ok()?;
        Some(Ok(Event::default()
            .id(event.seq.to_string())
            .event(event.kind)
            .data(payload)))
    }));

    let (tx, rx) = mpsc::channel::<Result<Event, std::convert::Infallible>>(128);
    let team_id_for_live = team_id.clone();
    let store = state.app.services.store.clone();
    tokio::spawn(async move {
        let mut cursor = replay_high_watermark_seq;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let page = match store.list_runtime_events(
                Some((RuntimeEventScope::Team, team_id_for_live.as_str())),
                Some(cursor),
                replay_page_limit,
            ) {
                Ok(page) => page,
                Err(_) => continue,
            };
            if page.is_empty() {
                continue;
            }
            for event in page {
                cursor = cursor.max(event.seq);
                let payload = match serde_json::to_string(&event) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                let sse = Event::default()
                    .id(event.seq.to_string())
                    .event(event.kind)
                    .data(payload);
                if tx.send(Ok(sse)).await.is_err() {
                    return;
                }
            }
        }
    });

    let live_stream = ReceiverStream::new(rx);
    let stream = replay_stream.chain(live_stream);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

async fn interrupt_all_team_turns(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
) -> Result<Json<runtime_core::TeamInterruptAllResponse>, ApiError> {
    let response = state
        .app
        .services
        .team_comms
        .interrupt_all_team_turns(TeamInterruptAllRequest { team_id })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct WorktreeCreateInput {
    source_session_id: String,
    repo_root: Option<String>,
    worktree_name: String,
    branch_prefix: Option<String>,
    base_ref: Option<String>,
    deletion_policy: Option<String>,
    run_init_script: Option<bool>,
    created_by_session_id: Option<String>,
    operation_id: Option<String>,
    team_id: Option<String>,
}

async fn create_worktree(
    State(state): State<AppState>,
    Json(input): Json<WorktreeCreateInput>,
) -> Result<Json<runtime_core::WorktreeCreateResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .create_worktree(WorktreeCreateRequest {
            team_id: input.team_id,
            source_session_id: input.source_session_id,
            repo_root: input.repo_root,
            worktree_name: input.worktree_name,
            branch_prefix: input.branch_prefix,
            base_ref: input.base_ref,
            deletion_policy: input.deletion_policy,
            run_init_script: input.run_init_script,
            created_by_session_id: input.created_by_session_id,
            operation_id: input.operation_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

async fn list_worktrees(
    State(state): State<AppState>,
) -> Result<Json<Vec<runtime_core::ManagedWorktreeRecord>>, ApiError> {
    let rows = state
        .app
        .services
        .worktrees
        .list_worktrees()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

async fn get_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
) -> Result<Json<runtime_core::ManagedWorktreeRecord>, ApiError> {
    let row = state
        .app
        .services
        .worktrees
        .get_worktree(worktree_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(row))
}

#[derive(Debug, Deserialize)]
struct WorktreeClaimInput {
    session_id: String,
    claim_role: String,
}

async fn claim_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
    Json(input): Json<WorktreeClaimInput>,
) -> Result<Json<runtime_core::WorktreeClaimResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .claim_worktree(WorktreeClaimRequest {
            worktree_id,
            session_id: input.session_id,
            claim_role: input.claim_role,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct WorktreeReleaseInput {
    session_id: String,
    cleanup_if_last_claim: Option<bool>,
}

async fn release_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
    Json(input): Json<WorktreeReleaseInput>,
) -> Result<Json<runtime_core::WorktreeReleaseResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .release_worktree(WorktreeReleaseRequest {
            worktree_id,
            session_id: input.session_id,
            cleanup_if_last_claim: input.cleanup_if_last_claim,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct WorktreeCleanupInput {
    reason: Option<String>,
}

async fn cleanup_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
    input: Option<Json<WorktreeCleanupInput>>,
) -> Result<Json<runtime_core::WorktreeCleanupResponse>, ApiError> {
    let input = input
        .map(|Json(value)| value)
        .unwrap_or(WorktreeCleanupInput { reason: None });
    let response = state
        .app
        .services
        .worktrees
        .cleanup_worktree(WorktreeCleanupRequest {
            worktree_id,
            reason: input.reason,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct TeamOperationDiagnosticsQuery {
    team_id: Option<String>,
    operation_id: Option<String>,
}

async fn list_team_operation_diagnostics(
    State(state): State<AppState>,
    Query(query): Query<TeamOperationDiagnosticsQuery>,
) -> Result<Json<Vec<runtime_core::TeamOperationDiagnosticRecord>>, ApiError> {
    let rows = state
        .app
        .services
        .store
        .list_team_operation_diagnostics(query.team_id.as_deref(), query.operation_id.as_deref())
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize)]
struct ProcessStartInput {
    command: String,
    cwd: Option<String>,
    timeout_ms: Option<u64>,
    session_id: Option<String>,
}

async fn start_process(
    State(state): State<AppState>,
    Json(input): Json<ProcessStartInput>,
) -> Result<Json<runtime_core::ProcessDetails>, ApiError> {
    let details = state
        .app
        .services
        .process_manager
        .run_process(ProcessRunRequest {
            caller_session_id: input.session_id,
            tool_call_id: None,
            command: input.command,
            cwd: input.cwd,
            timeout_ms: input.timeout_ms,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(details))
}

#[derive(Debug, Deserialize)]
struct ProcessListQuery {
    session_id: Option<String>,
    include_completed: Option<bool>,
}

async fn list_processes(
    State(state): State<AppState>,
    Query(query): Query<ProcessListQuery>,
) -> Result<Json<Vec<runtime_core::ProcessSummary>>, ApiError> {
    let rows = state
        .app
        .services
        .process_manager
        .list_processes(ProcessListRequest {
            caller_session_id: query.session_id,
            include_completed: query.include_completed.unwrap_or(true),
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize)]
struct ProcessSessionQuery {
    session_id: Option<String>,
}

async fn get_process(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessSessionQuery>,
) -> Result<Json<runtime_core::ProcessDetails>, ApiError> {
    let details = state
        .app
        .services
        .process_manager
        .get_process(ProcessGetRequest {
            process_id,
            caller_session_id: query.session_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(details))
}

#[derive(Debug, Deserialize)]
struct ProcessLogsQuery {
    session_id: Option<String>,
    stream: Option<String>,
    head_lines: Option<usize>,
    tail_lines: Option<usize>,
    max_bytes: Option<usize>,
}

async fn get_process_logs(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessLogsQuery>,
) -> Result<Json<Vec<runtime_core::ProcessLogsChunk>>, ApiError> {
    let logs = state
        .app
        .services
        .process_manager
        .read_process_logs(ProcessLogReadRequest {
            process_id,
            caller_session_id: query.session_id,
            stream: query.stream,
            head_lines: query.head_lines,
            tail_lines: query.tail_lines,
            max_bytes: query.max_bytes,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(logs))
}

#[derive(Debug, Deserialize)]
struct ProcessEventsQuery {
    session_id: Option<String>,
    after_seq: Option<i64>,
    limit: Option<usize>,
}

async fn replay_process_events(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessEventsQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .app
        .services
        .process_manager
        .replay_events(
            process_id,
            query.session_id,
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

async fn stream_process_events(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessEventsQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let receiver = state.app.services.process_manager.subscribe_events();
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok());
    let cursor = query.after_seq.or(last_event_id);
    let replay_page_limit = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    let mut replay_events = Vec::new();
    let mut replay_cursor = cursor;

    loop {
        let page = state
            .app
            .services
            .process_manager
            .replay_events(
                process_id.clone(),
                query.session_id.clone(),
                replay_cursor,
                replay_page_limit,
            )
            .await
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        replay_cursor = page.last().map(|event| event.seq);
        let page_len = page.len();
        replay_events.extend(page);
        if page_len < replay_page_limit {
            break;
        }
    }

    #[cfg(test)]
    if let Some(delay_ms) = headers
        .get("x-gg-test-handoff-delay-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
    {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }

    let replay_high_watermark_seq = replay_cursor.or(cursor).unwrap_or(0);
    let replay_stream = tokio_stream::iter(replay_events.into_iter().filter_map(|event| {
        let payload = serde_json::to_string(&event).ok()?;
        Some(Ok(Event::default()
            .id(event.seq.to_string())
            .event(event.kind)
            .data(payload)))
    }));

    let process_id_for_live = process_id.clone();
    let live_stream = BroadcastStream::new(receiver).filter_map(move |next| match next {
        Ok(event)
            if event.scope == runtime_core::RuntimeEventScope::Process
                && event.scope_id == process_id_for_live =>
        {
            if event.seq <= replay_high_watermark_seq {
                return None;
            }
            let payload = match serde_json::to_string(&event) {
                Ok(payload) => payload,
                Err(_) => return None,
            };
            Some(Ok(Event::default()
                .id(event.seq.to_string())
                .event(event.kind)
                .data(payload)))
        }
        _ => None,
    });
    let stream = replay_stream.chain(live_stream);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

#[derive(Debug, Deserialize)]
struct ProcessKillInput {
    session_id: Option<String>,
    reason: Option<String>,
}

async fn kill_process(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    input: Option<Json<ProcessKillInput>>,
) -> Result<Json<runtime_core::ProcessDetails>, ApiError> {
    let input = input.map(|Json(value)| value).unwrap_or(ProcessKillInput {
        session_id: None,
        reason: None,
    });
    let details = state
        .app
        .services
        .process_manager
        .kill_process(ProcessKillRequest {
            process_id,
            caller_session_id: input.session_id,
            reason: input.reason,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(details))
}

async fn mcp_capabilities(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let result = state
        .app
        .services
        .tool_gateway
        .capabilities()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
struct McpInvokeRequest {
    namespace: Option<String>,
    #[serde(alias = "toolName")]
    tool_name: String,
    #[serde(alias = "callerAgentId")]
    caller_agent_id: String,
    #[serde(default, alias = "invocationId")]
    invocation_id: Option<String>,
    #[serde(default)]
    args: serde_json::Value,
}

async fn mcp_invoke(
    State(state): State<AppState>,
    Json(request): Json<McpInvokeRequest>,
) -> Result<Json<Value>, ApiError> {
    let caller_session_id = request.caller_agent_id.trim();
    if caller_session_id.is_empty() {
        return Err(ApiError::bad_request(
            "caller_agent_id is required".to_string(),
        ));
    }
    if request.tool_name.trim().is_empty() {
        return Err(ApiError::bad_request("tool_name is required".to_string()));
    }
    let caller_session = state
        .runtime
        .get_session(caller_session_id)
        .await
        .map_err(ApiError::from)?;
    if matches!(caller_session.status.as_str(), "closed" | "failed") {
        return Err(ApiError::bad_request(format!(
            "caller session {} is not active (status={})",
            caller_session_id, caller_session.status
        )));
    }
    let result = state
        .app
        .services
        .tool_gateway
        .invoke_tool(ToolInvokeRequest {
            namespace: request.namespace,
            tool_name: request.tool_name,
            caller_session_id: caller_session_id.to_string(),
            invocation_id: request.invocation_id,
            args: request.args,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(result))
}

fn parse_provider_kind(value: &str) -> Result<ProviderKind, ApiError> {
    ProviderKind::from_str(value)
        .ok_or_else(|| ApiError::bad_request(format!("unknown provider {}", value)))
}

async fn bearer_auth(
    State(expected_token): State<String>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    let expected = format!("Bearer {expected_token}");
    if auth_header == Some(expected.as_str()) {
        return next.run(request).await;
    }

    (StatusCode::UNAUTHORIZED, "missing or invalid bearer token").into_response()
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    fn not_found(message: String) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message,
        }
    }
}

impl From<RuntimeError> for ApiError {
    fn from(value: RuntimeError) -> Self {
        match value {
            RuntimeError::NotFound(message) | RuntimeError::ProviderNotRegistered(message) => {
                Self::not_found(message)
            }
            RuntimeError::Configuration(message)
            | RuntimeError::InvalidState(message)
            | RuntimeError::ProtocolViolation(message)
            | RuntimeError::Unsupported(message) => Self::bad_request(message),
            RuntimeError::ProviderAlreadyRegistered(message)
            | RuntimeError::Bootstrap(message)
            | RuntimeError::Io(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({
                "error": self.message,
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use runtime_core::{
        ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
        ProviderCreateSessionRequest, ProviderInterruptTurnRequest, ProviderMetadata,
        ProviderModel, ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderSession,
        ProviderTurnAck, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest,
        RuntimeProvider, RuntimeStore, RuntimeTeamCommsConfig, RuntimeTeamCommsService,
    };
    use runtime_provider_claude::{
        standalone_claude_bridge_command_path, standalone_gg_mcp_server_command_path,
    };
    use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
    use runtime_tools::{
        ProcessManagerConfig, RuntimeProcessManager, RuntimeToolGateway, RuntimeWorktreeService,
        WorktreeServiceConfig,
    };
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{timeout, Duration};
    use tower::ServiceExt;

    use crate::bootstrap::bootstrap_runtime;
    use crate::config::RuntimeServerConfig;

    #[derive(Default)]
    struct TestProviderState {
        sessions: HashMap<String, TestProviderSession>,
    }

    #[derive(Default)]
    struct TestProviderSession {
        provider_session_ref: String,
        history: Vec<String>,
        completed: HashMap<String, ProviderTurnResult>,
        pending: HashMap<String, ProviderSendTurnRequest>,
    }

    #[derive(Default)]
    struct TestProvider {
        state: Mutex<TestProviderState>,
    }

    #[derive(Default)]
    struct TestClaudeProvider {
        state: Mutex<TestProviderState>,
    }

    impl TestProvider {
        fn extract_text(input: &[serde_json::Value]) -> String {
            for item in input {
                if let Some(text) = item.get("text").and_then(serde_json::Value::as_str) {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        return trimmed.to_string();
                    }
                }
            }
            "empty".to_string()
        }
    }

    impl TestClaudeProvider {
        fn extract_text(input: &[serde_json::Value]) -> String {
            TestProvider::extract_text(input)
        }
    }

    #[async_trait::async_trait]
    impl RuntimeProvider for TestProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Codex
        }

        fn metadata(&self) -> ProviderMetadata {
            ProviderMetadata {
                kind: ProviderKind::Codex,
                display_name: "Test Codex".to_string(),
                enabled: true,
            }
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
            Ok(vec![ProviderModel {
                id: "test-model".to_string(),
                display_name: "Test Model".to_string(),
            }])
        }

        async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
            Ok(ProviderAuthStatus {
                authenticated: true,
                mode: Some("test".to_string()),
                detail: None,
            })
        }

        async fn create_session(
            &self,
            req: ProviderCreateSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            state.sessions.insert(
                req.runtime_session_id.clone(),
                TestProviderSession {
                    provider_session_ref: format!("test-thread-{}", req.runtime_session_id),
                    ..Default::default()
                },
            );
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id.clone(),
                provider_session_ref: format!("test-thread-{}", req.runtime_session_id),
                canonical_provider_session_ref: None,
            })
        }

        async fn resume_session(
            &self,
            req: ProviderResumeSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            let session = state
                .sessions
                .entry(req.runtime_session_id.clone())
                .or_default();
            session.provider_session_ref = req.provider_session_ref.clone();
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id,
                provider_session_ref: req.provider_session_ref,
                canonical_provider_session_ref: req.canonical_provider_session_ref,
            })
        }

        async fn send_turn(
            &self,
            req: ProviderSendTurnRequest,
        ) -> Result<ProviderTurnAck, RuntimeError> {
            let mut state = self.state.lock().await;
            let session = state
                .sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
                })?;

            if let Some(approval_id) = req.approval_id.clone() {
                session.pending.insert(approval_id, req.clone());
                return Ok(ProviderTurnAck {
                    runtime_session_id: req.runtime_session_id,
                    turn_id: req.turn_id,
                });
            }

            let user_text = Self::extract_text(req.input.as_slice());
            let first_prompt = session
                .history
                .first()
                .cloned()
                .unwrap_or_else(|| "none".to_string());
            let reply = if user_text.contains("first prompt") {
                first_prompt
            } else {
                format!("ack:{user_text}")
            };
            session.history.push(user_text);
            session.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: req.runtime_session_id.clone(),
                    turn_id: req.turn_id.clone(),
                    status: ProviderTurnStatus::Completed,
                    usage: Some(serde_json::json!({ "last_message": reply })),
                    error: None,
                },
            );

            Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
            })
        }

        async fn interrupt_turn(
            &self,
            _req: ProviderInterruptTurnRequest,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn respond_approval(
            &self,
            req: ProviderApprovalResponseRequest,
        ) -> Result<(), RuntimeError> {
            let decision = ApprovalDecision::parse(req.decision.as_str())?;
            let mut state = self.state.lock().await;
            let session = state
                .sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
                })?;

            let pending = session
                .pending
                .remove(req.approval_id.as_str())
                .ok_or_else(|| RuntimeError::NotFound(format!("approval {}", req.approval_id)))?;
            if decision == ApprovalDecision::Decline {
                session.completed.insert(
                    req.turn_id.clone(),
                    ProviderTurnResult {
                        runtime_session_id: req.runtime_session_id,
                        turn_id: req.turn_id,
                        status: ProviderTurnStatus::Interrupted,
                        usage: None,
                        error: Some(serde_json::json!({ "message": "declined" })),
                    },
                );
            } else {
                let user_text = Self::extract_text(pending.input.as_slice());
                session.completed.insert(
                    req.turn_id.clone(),
                    ProviderTurnResult {
                        runtime_session_id: pending.runtime_session_id,
                        turn_id: pending.turn_id,
                        status: ProviderTurnStatus::Completed,
                        usage: Some(serde_json::json!({ "last_message": user_text })),
                        error: None,
                    },
                );
            }
            Ok(())
        }

        async fn wait_for_turn(
            &self,
            req: ProviderWaitTurnRequest,
        ) -> Result<ProviderTurnResult, RuntimeError> {
            let state = self.state.lock().await;
            let session = state
                .sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
                })?;
            session
                .completed
                .get(req.turn_id.as_str())
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("test turn {}", req.turn_id)))
        }
    }

    #[async_trait::async_trait]
    impl RuntimeProvider for TestClaudeProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Claude
        }

        fn metadata(&self) -> ProviderMetadata {
            ProviderMetadata {
                kind: ProviderKind::Claude,
                display_name: "Test Claude".to_string(),
                enabled: true,
            }
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
            Ok(vec![ProviderModel {
                id: "test-claude-model".to_string(),
                display_name: "Test Claude Model".to_string(),
            }])
        }

        async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
            Ok(ProviderAuthStatus {
                authenticated: true,
                mode: Some("test".to_string()),
                detail: None,
            })
        }

        async fn create_session(
            &self,
            req: ProviderCreateSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            state.sessions.insert(
                req.runtime_session_id.clone(),
                TestProviderSession {
                    provider_session_ref: format!("test-claude-thread-{}", req.runtime_session_id),
                    ..Default::default()
                },
            );
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id.clone(),
                provider_session_ref: format!("test-claude-thread-{}", req.runtime_session_id),
                canonical_provider_session_ref: Some(format!(
                    "claude-canonical-{}",
                    req.runtime_session_id
                )),
            })
        }

        async fn resume_session(
            &self,
            req: ProviderResumeSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            let session = state
                .sessions
                .entry(req.runtime_session_id.clone())
                .or_default();
            session.provider_session_ref = req.provider_session_ref.clone();
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id,
                provider_session_ref: req.provider_session_ref,
                canonical_provider_session_ref: req.canonical_provider_session_ref,
            })
        }

        async fn send_turn(
            &self,
            req: ProviderSendTurnRequest,
        ) -> Result<ProviderTurnAck, RuntimeError> {
            let mut state = self.state.lock().await;
            let session = state
                .sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
                })?;

            if let Some(approval_id) = req.approval_id.clone() {
                session.pending.insert(approval_id, req.clone());
                return Ok(ProviderTurnAck {
                    runtime_session_id: req.runtime_session_id,
                    turn_id: req.turn_id,
                });
            }

            let user_text = Self::extract_text(req.input.as_slice());
            session.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: req.runtime_session_id.clone(),
                    turn_id: req.turn_id.clone(),
                    status: ProviderTurnStatus::Completed,
                    usage: Some(
                        serde_json::json!({ "last_message": format!("claude:{user_text}") }),
                    ),
                    error: None,
                },
            );

            Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
            })
        }

        async fn interrupt_turn(
            &self,
            _req: ProviderInterruptTurnRequest,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn respond_approval(
            &self,
            req: ProviderApprovalResponseRequest,
        ) -> Result<(), RuntimeError> {
            let decision = ApprovalDecision::parse(req.decision.as_str())?;
            let mut state = self.state.lock().await;
            let session = state
                .sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
                })?;

            let pending = session
                .pending
                .remove(req.approval_id.as_str())
                .ok_or_else(|| RuntimeError::NotFound(format!("approval {}", req.approval_id)))?;
            if decision == ApprovalDecision::Decline {
                session.completed.insert(
                    req.turn_id.clone(),
                    ProviderTurnResult {
                        runtime_session_id: req.runtime_session_id,
                        turn_id: req.turn_id,
                        status: ProviderTurnStatus::Interrupted,
                        usage: None,
                        error: Some(serde_json::json!({ "message": "declined" })),
                    },
                );
            } else {
                let user_text = Self::extract_text(pending.input.as_slice());
                session.completed.insert(
                    req.turn_id.clone(),
                    ProviderTurnResult {
                        runtime_session_id: pending.runtime_session_id,
                        turn_id: pending.turn_id,
                        status: ProviderTurnStatus::Completed,
                        usage: Some(
                            serde_json::json!({ "last_message": format!("claude:{user_text}") }),
                        ),
                        error: None,
                    },
                );
            }
            Ok(())
        }

        async fn wait_for_turn(
            &self,
            req: ProviderWaitTurnRequest,
        ) -> Result<ProviderTurnResult, RuntimeError> {
            let state = self.state.lock().await;
            let session = state
                .sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
                })?;
            session
                .completed
                .get(req.turn_id.as_str())
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("test turn {}", req.turn_id)))
        }
    }

    async fn build_test_router() -> (Router, String, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("initialize store");

        let mut registry = runtime_core::ProviderRegistry::new();
        registry
            .register(Arc::new(TestProvider::default()))
            .expect("register test provider");
        let provider_registry = Arc::new(registry);
        let runtime = Arc::new(
            RuntimeSessionManager::new(store.clone(), provider_registry.clone(), 512)
                .expect("build runtime"),
        );
        let process_manager = RuntimeProcessManager::new(
            store.clone(),
            ProcessManagerConfig {
                enabled: true,
                max_concurrent: 1,
                default_timeout_ms: 60_000,
                max_output_bytes_per_process: 100_000,
                allow_shell: false,
                completed_retention_ms: 600_000,
                output_event_sample_bytes: 8 * 1024,
                log_dir: temp_dir.path().join("process-logs"),
            },
        )
        .await
        .expect("process manager");
        let tool_gateway = Arc::new(RuntimeToolGateway::new(process_manager.clone()));
        let team_comms = RuntimeTeamCommsService::new(
            store.clone(),
            runtime.clone(),
            RuntimeTeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1_000,
            },
        )
        .expect("team comms");

        let worktrees = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("worktree service");

        let app = runtime_core::RuntimeApp::new(
            provider_registry.clone(),
            runtime_core::RuntimeServices {
                store: store.clone(),
                tool_gateway,
                process_manager,
                team_comms,
                worktrees,
            },
            runtime_core::EventQueueLimits {
                live_queue_capacity: 512,
                critical_queue_capacity: 512,
                team_queue_capacity: 512,
            },
            runtime_core::ProcessLimits {
                max_concurrent: 1,
                default_timeout_ms: 60_000,
                max_output_bytes_per_process: 100_000,
            },
            runtime_core::WorktreeSettings {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build app");
        app.initialize().await.expect("initialize app");
        let bearer_token = "test-token".to_string();

        let router = build_router(AppState {
            app: Arc::new(app),
            runtime,
            bearer_token: bearer_token.clone(),
            public_base_url: "http://localhost:8080".to_string(),
        });

        (router, bearer_token, temp_dir)
    }

    async fn build_mixed_provider_test_router() -> (Router, String, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("initialize store");

        let mut registry = runtime_core::ProviderRegistry::new();
        registry
            .register(Arc::new(TestProvider::default()))
            .expect("register codex test provider");
        registry
            .register(Arc::new(TestClaudeProvider::default()))
            .expect("register claude test provider");
        let provider_registry = Arc::new(registry);
        let runtime = Arc::new(
            RuntimeSessionManager::new(store.clone(), provider_registry.clone(), 512)
                .expect("build runtime"),
        );
        let process_manager = RuntimeProcessManager::new(
            store.clone(),
            ProcessManagerConfig {
                enabled: true,
                max_concurrent: 1,
                default_timeout_ms: 60_000,
                max_output_bytes_per_process: 100_000,
                allow_shell: false,
                completed_retention_ms: 600_000,
                output_event_sample_bytes: 8 * 1024,
                log_dir: temp_dir.path().join("process-logs"),
            },
        )
        .await
        .expect("process manager");
        let tool_gateway = Arc::new(RuntimeToolGateway::new(process_manager.clone()));
        let team_comms = RuntimeTeamCommsService::new(
            store.clone(),
            runtime.clone(),
            RuntimeTeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1_000,
            },
        )
        .expect("team comms");
        let worktrees = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("worktree service");

        let app = runtime_core::RuntimeApp::new(
            provider_registry.clone(),
            runtime_core::RuntimeServices {
                store: store.clone(),
                tool_gateway,
                process_manager,
                team_comms,
                worktrees,
            },
            runtime_core::EventQueueLimits {
                live_queue_capacity: 512,
                critical_queue_capacity: 512,
                team_queue_capacity: 512,
            },
            runtime_core::ProcessLimits {
                max_concurrent: 1,
                default_timeout_ms: 60_000,
                max_output_bytes_per_process: 100_000,
            },
            runtime_core::WorktreeSettings {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build app");
        app.initialize().await.expect("initialize app");
        let bearer_token = "mixed-provider-token".to_string();

        let router = build_router(AppState {
            app: Arc::new(app),
            runtime,
            bearer_token: bearer_token.clone(),
            public_base_url: "http://localhost:8080".to_string(),
        });

        (router, bearer_token, temp_dir)
    }

    #[tokio::test]
    async fn version_route_is_available() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");

        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v1/version")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("version response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("version body");
        let json: serde_json::Value = serde_json::from_slice(&payload).expect("version json");
        assert_eq!(
            json.get("version").and_then(serde_json::Value::as_str),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn session_stream_replays_from_cursor_before_live_events() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create response");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_payload = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create payload");
        let created: serde_json::Value =
            serde_json::from_slice(&create_payload).expect("create json");
        let session_id = created
            .get("id")
            .and_then(serde_json::Value::as_str)
            .expect("session id")
            .to_string();

        for text in ["first prompt", "what was my first prompt"] {
            let send_body = serde_json::json!({
                "input": [{ "type": "text", "text": text }],
                "expected_turn_id": null,
                "permission_mode": null
            });
            let send_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/v1/sessions/{session_id}/turns"))
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(send_body.to_string()))
                        .unwrap(),
                )
                .await
                .expect("send response");
            assert_eq!(send_response.status(), StatusCode::OK);

            let mut idle = false;
            for _ in 0..50 {
                let session_response = router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(format!("/v1/sessions/{session_id}"))
                            .header(header::AUTHORIZATION, format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .expect("session response");
                let body = to_bytes(session_response.into_body(), usize::MAX)
                    .await
                    .expect("session body");
                let session: serde_json::Value =
                    serde_json::from_slice(&body).expect("session json");
                if session
                    .get("active_turn_id")
                    .is_some_and(serde_json::Value::is_null)
                {
                    idle = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(idle, "turn did not finish in time for replay test");
        }

        let replay_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("events response");
        let replay_payload = to_bytes(replay_response.into_body(), usize::MAX)
            .await
            .expect("events payload");
        let events: Vec<runtime_core::RuntimeEventRecord> =
            serde_json::from_slice(&replay_payload).expect("events json");
        assert!(
            events.len() >= 5,
            "expected at least session.created + 2 turn start/terminal pairs"
        );
        let cursor = events
            .iter()
            .find(|event| event.kind == "turn.completed")
            .map(|event| event.seq)
            .expect("turn.completed seq");
        let expected_ids = events
            .iter()
            .filter(|event| event.seq > cursor)
            .map(|event| event.seq.to_string())
            .collect::<Vec<_>>();
        assert!(
            !expected_ids.is_empty(),
            "expected replay window after cursor"
        );
        let recalled_message = events
            .iter()
            .filter_map(|event| event.payload.get("usage"))
            .filter_map(|usage| usage.get("last_message"))
            .filter_map(serde_json::Value::as_str)
            .find(|message| *message == "first prompt");
        assert_eq!(
            recalled_message,
            Some("first prompt"),
            "second turn should preserve context from the first turn"
        );

        let stream_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/sessions/{session_id}/events/stream?after_seq={cursor}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("stream response");
        assert_eq!(stream_response.status(), StatusCode::OK);

        let mut data_stream = stream_response.into_body().into_data_stream();
        let mut sse_payload = String::new();
        for _ in 0..8 {
            let next = timeout(Duration::from_secs(1), data_stream.next()).await;
            match next {
                Ok(Some(Ok(chunk))) => {
                    sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                    let all_present = expected_ids
                        .iter()
                        .all(|seq| sse_payload.contains(format!("id: {seq}").as_str()));
                    if all_present {
                        break;
                    }
                }
                _ => break,
            }
        }
        for seq in expected_ids {
            assert!(
                sse_payload.contains(format!("id: {seq}").as_str()),
                "missing replayed seq {seq} in SSE payload: {sse_payload}"
            );
        }
    }

    #[tokio::test]
    async fn session_stream_replays_exhaustive_backlog_across_pages() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create response");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_payload = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create payload");
        let created: serde_json::Value =
            serde_json::from_slice(&create_payload).expect("create json");
        let session_id = created
            .get("id")
            .and_then(serde_json::Value::as_str)
            .expect("session id")
            .to_string();

        for index in 0..8 {
            let send_body = serde_json::json!({
                "input": [{ "type": "text", "text": format!("replay page turn {index}") }],
                "expected_turn_id": null,
                "permission_mode": null
            });
            let send_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/v1/sessions/{session_id}/turns"))
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(send_body.to_string()))
                        .unwrap(),
                )
                .await
                .expect("send response");
            assert_eq!(send_response.status(), StatusCode::OK);

            let mut idle = false;
            for _ in 0..80 {
                let session_response = router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(format!("/v1/sessions/{session_id}"))
                            .header(header::AUTHORIZATION, format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .expect("session response");
                let body = to_bytes(session_response.into_body(), usize::MAX)
                    .await
                    .expect("session body");
                let session: serde_json::Value =
                    serde_json::from_slice(&body).expect("session json");
                if session
                    .get("active_turn_id")
                    .is_some_and(serde_json::Value::is_null)
                {
                    idle = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(idle, "turn {index} did not finish in time");
        }

        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events_payload = to_bytes(events_response.into_body(), usize::MAX)
            .await
            .expect("events payload");
        let events: Vec<runtime_core::RuntimeEventRecord> =
            serde_json::from_slice(&events_payload).expect("events json");
        assert!(
            events.len() > 10,
            "expected sizable backlog for pagination regression"
        );
        let cursor = 1_i64;
        let expected_ids = events
            .iter()
            .filter(|event| event.seq > cursor)
            .map(|event| event.seq)
            .collect::<Vec<_>>();
        assert!(
            expected_ids.len() > 8,
            "expected more than one replay page of missed events"
        );

        let stream_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/sessions/{session_id}/events/stream?after_seq={cursor}&limit=3"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("stream response");
        assert_eq!(stream_response.status(), StatusCode::OK);

        let mut data_stream = stream_response.into_body().into_data_stream();
        let mut sse_payload = String::new();
        for _ in 0..80 {
            let next = timeout(Duration::from_millis(300), data_stream.next()).await;
            match next {
                Ok(Some(Ok(chunk))) => {
                    sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                    let all_present = expected_ids
                        .iter()
                        .all(|seq| sse_payload.contains(format!("id: {seq}\n").as_str()));
                    if all_present {
                        break;
                    }
                }
                _ => break,
            }
        }

        for seq in expected_ids {
            assert!(
                sse_payload.contains(format!("id: {seq}\n").as_str()),
                "missing replay backlog seq {seq} in paged stream payload"
            );
        }
    }

    #[tokio::test]
    async fn session_stream_handoff_window_event_is_not_lost() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create response");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_payload = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create payload");
        let created: serde_json::Value =
            serde_json::from_slice(&create_payload).expect("create json");
        let session_id = created
            .get("id")
            .and_then(serde_json::Value::as_str)
            .expect("session id")
            .to_string();

        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events_payload = to_bytes(events_response.into_body(), usize::MAX)
            .await
            .expect("events payload");
        let events: Vec<runtime_core::RuntimeEventRecord> =
            serde_json::from_slice(&events_payload).expect("events json");
        let cursor = events.last().map(|event| event.seq).unwrap_or(0);

        let stream_router = router.clone();
        let stream_token = token.clone();
        let stream_session_id = session_id.clone();
        let stream_handle = tokio::spawn(async move {
            stream_router
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/sessions/{stream_session_id}/events/stream?after_seq={cursor}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {stream_token}"))
                        .header("x-gg-test-handoff-delay-ms", "300")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        let send_body = serde_json::json!({
            "input": [{ "type": "text", "text": "handoff window message" }],
            "expected_turn_id": null,
            "permission_mode": null
        });
        let send_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/turns"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(send_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("send response");
        assert_eq!(send_response.status(), StatusCode::OK);

        let stream_response = stream_handle
            .await
            .expect("stream task join")
            .expect("stream response");
        assert_eq!(stream_response.status(), StatusCode::OK);

        let mut data_stream = stream_response.into_body().into_data_stream();
        let mut sse_payload = String::new();
        for _ in 0..8 {
            let next = timeout(Duration::from_secs(1), data_stream.next()).await;
            match next {
                Ok(Some(Ok(chunk))) => {
                    sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                    if sse_payload.contains("event: turn.started")
                        || sse_payload.contains("event: turn.completed")
                    {
                        break;
                    }
                }
                _ => break,
            }
        }

        assert!(
            sse_payload.contains("event: turn.started")
                || sse_payload.contains("event: turn.completed"),
            "expected handoff-window event to be delivered in stream payload: {sse_payload}"
        );
    }

    #[tokio::test]
    async fn health_route_is_public() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");

        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: bootstrapped.auth.bearer_token,
            public_base_url: bootstrapped.public_base_url,
        });

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_route_requires_token() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");

        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });

        let unauthorized = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = router
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn claude_auth_endpoints_are_runtime_managed() {
        let prior_bridge_home_override = std::env::var_os("GG_CLAUDE_BRIDGE_HOME");
        let prior_bridge_config_override = std::env::var_os("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let isolated_bridge_home = temp_dir.path().join("isolated-bridge-home");
        let isolated_bridge_config = temp_dir.path().join("isolated-bridge-config");
        std::fs::create_dir_all(isolated_bridge_home.as_path()).expect("create isolated home dir");
        std::fs::create_dir_all(isolated_bridge_config.as_path())
            .expect("create isolated config dir");
        std::env::set_var(
            "GG_CLAUDE_BRIDGE_HOME",
            isolated_bridge_home.display().to_string(),
        );
        std::env::set_var(
            "GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR",
            isolated_bridge_config.display().to_string(),
        );

        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.codex.enabled = false;
        config.providers.claude.enabled = true;
        let claude_provider_dir = config.resolve_provider_dir("claude");
        let claude_credentials_path = claude_provider_dir
            .join("home")
            .join(".claude")
            .join(".credentials.json");
        let claude_config_path = claude_provider_dir.join("config").join(".claude.json");

        let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");
        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });

        let initial_status = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/providers/claude/auth/status")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("initial status response");
        assert_eq!(initial_status.status(), StatusCode::OK);
        let initial_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(initial_status.into_body(), usize::MAX)
                .await
                .expect("initial status body"),
        )
        .expect("initial status json");
        assert_eq!(initial_json["authenticated"], false);

        let api_key_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/providers/claude/auth/api-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({ "api_key": "sk-ant-test-123" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("api-key response");
        assert_eq!(api_key_response.status(), StatusCode::OK);
        let api_key_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(api_key_response.into_body(), usize::MAX)
                .await
                .expect("api-key body"),
        )
        .expect("api-key json");
        assert_eq!(api_key_json["authenticated"], true);
        assert_eq!(api_key_json["mode"], "api_key");

        let import_json_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/providers/claude/auth/import-json")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "auth_json": {
                                "credentials_json": {
                                    "claudeAiOauth": {
                                        "accessToken": "runtime-managed-auth",
                                        "refreshToken": "runtime-managed-auth"
                                    }
                                },
                                "config_json": {
                                    "projects": {}
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("import-json response");
        assert_eq!(import_json_response.status(), StatusCode::OK);
        let import_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(import_json_response.into_body(), usize::MAX)
                .await
                .expect("import-json body"),
        )
        .expect("import-json json");
        assert_eq!(import_json["authenticated"], true);
        assert_eq!(import_json["mode"], "claude_code_oauth");
        assert!(claude_credentials_path.exists());
        assert!(claude_config_path.exists());

        let boundary = "phase7boundary";
        let multipart_body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"auth_bundle.json\"\r\nContent-Type: application/json\r\n\r\n{{\"credentials_json\":{{\"claudeAiOauth\":{{\"accessToken\":\"multipart-import\",\"refreshToken\":\"multipart-import\"}}}},\"config_json\":{{\"projects\":{{}}}}}}\r\n--{boundary}--\r\n"
        );
        let import_file_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/providers/claude/auth/import-file")
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(multipart_body))
                    .unwrap(),
            )
            .await
            .expect("import-file response");
        assert_eq!(import_file_response.status(), StatusCode::OK);

        let logout_response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/providers/claude/auth/logout")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("logout response");
        assert_eq!(logout_response.status(), StatusCode::OK);
        let logout_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(logout_response.into_body(), usize::MAX)
                .await
                .expect("logout body"),
        )
        .expect("logout json");
        assert_eq!(logout_json["authenticated"], false);
        assert!(!claude_credentials_path.exists());
        assert!(!claude_config_path.exists());

        match prior_bridge_home_override {
            Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_HOME", value),
            None => std::env::remove_var("GG_CLAUDE_BRIDGE_HOME"),
        }
        match prior_bridge_config_override {
            Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR", value),
            None => std::env::remove_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR"),
        }
    }

    #[tokio::test]
    #[ignore = "requires local Claude auth sources at ~/.claude/.credentials.json and ~/.gg/claude/.claude.json (or ~/.claude.json fallback)"]
    async fn ignored_real_claude_http_smoke_exercises_mcp_with_gg_mcp_enabled() {
        let prior_bridge_home_override = std::env::var_os("GG_CLAUDE_BRIDGE_HOME");
        let prior_bridge_config_override = std::env::var_os("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR");
        std::env::remove_var("GG_CLAUDE_BRIDGE_HOME");
        std::env::remove_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR");

        let home_dir = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .expect("HOME must be set for Claude smoke");
        let credentials_source_path = std::env::var("GG_CLAUDE_SMOKE_CREDENTIALS_SOURCE")
            .ok()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| home_dir.join(".claude").join(".credentials.json"));
        let config_source_path = std::env::var("GG_CLAUDE_SMOKE_CONFIG_SOURCE")
            .ok()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                let gg_claude_config = home_dir.join(".gg").join("claude").join(".claude.json");
                if gg_claude_config.exists() {
                    gg_claude_config
                } else {
                    home_dir.join(".claude.json")
                }
            });
        assert!(
            credentials_source_path.exists(),
            "Claude smoke credentials source path must exist: {}",
            credentials_source_path.display()
        );
        assert!(
            config_source_path.exists(),
            "Claude smoke config source path must exist: {}",
            config_source_path.display()
        );

        let claude_bridge_command_path = standalone_claude_bridge_command_path();
        let gg_mcp_command_path = standalone_gg_mcp_server_command_path();
        assert!(
            claude_bridge_command_path.exists(),
            "branch-owned Claude bridge launcher is missing at {}",
            claude_bridge_command_path.display()
        );
        assert!(
            gg_mcp_command_path.exists(),
            "branch-owned gg-mcp-server launcher is missing at {}",
            gg_mcp_command_path.display()
        );

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.codex.enabled = false;
        config.providers.claude.enabled = true;
        config.processes.enabled = true;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind smoke listener");
        let listen_addr = listener.local_addr().expect("smoke listener addr");
        config.server.public_base_url = format!("http://{listen_addr}");

        let bootstrapped = bootstrap_runtime(config).await.expect("bootstrap");
        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });
        let smoke_server_router = router.clone();
        let smoke_server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, smoke_server_router).await;
        });

        let auth_status_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/providers/claude/auth/status")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("auth status response");
        assert_eq!(auth_status_response.status(), StatusCode::OK);
        let auth_status_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(auth_status_response.into_body(), usize::MAX)
                .await
                .expect("auth status body"),
        )
        .expect("auth status json");
        assert_eq!(
            auth_status_json["authenticated"],
            true,
            "Claude auth status should be authenticated for canonical-path smoke setup; credentials_source_path={} config_source_path={} detail={}",
            credentials_source_path.display(),
            config_source_path.display(),
            auth_status_json["detail"]
        );

        let smoke_model = std::env::var("GG_CLAUDE_SMOKE_MODEL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "claude-sonnet-4-5".to_string());
        let smoke_permission_mode = std::env::var("GG_CLAUDE_SMOKE_PERMISSION_MODE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "bypassPermissions".to_string());
        let smoke_cwd = std::env::current_dir()
            .expect("current dir")
            .display()
            .to_string();

        let create_session_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "claude",
                            "model": smoke_model,
                            "cwd": smoke_cwd,
                            "permission_mode": smoke_permission_mode,
                            "metadata": {}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create session response");
        assert_eq!(create_session_response.status(), StatusCode::OK);
        let create_session_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_session_response.into_body(), usize::MAX)
                .await
                .expect("create session body"),
        )
        .expect("create session json");
        let session_id = create_session_json["id"]
            .as_str()
            .expect("session id")
            .to_string();

        let marker_path = temp_dir.path().join("mcp-smoke-marker.txt");
        let tool_prompt = format!(
            "Use tool gg_process_run to execute this exact command and nothing else: touch {}\nAfter the tool call, reply with exactly: MCP_SMOKE_DONE",
            marker_path.display()
        );

        let send_turn_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/turns"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "input": [
                                {
                                    "type": "text",
                                    "text": tool_prompt,
                                }
                            ],
                            "expected_turn_id": null,
                            "permission_mode": null
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("send turn response");
        assert_eq!(send_turn_response.status(), StatusCode::OK);
        let send_turn_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(send_turn_response.into_body(), usize::MAX)
                .await
                .expect("send turn body"),
        )
        .expect("send turn json");
        let turn_id = send_turn_json["turn_id"]
            .as_str()
            .expect("turn id")
            .to_string();

        let max_wait_secs = std::env::var("GG_CLAUDE_SMOKE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(300);
        let deadline = std::time::Instant::now() + Duration::from_secs(max_wait_secs.max(30));
        let mut saw_terminal_event = false;
        let mut terminal_last_message: Option<String> = None;
        let mut accepted_approvals = std::collections::BTreeSet::new();
        let mut event_cursor: Option<i64> = None;
        let smoke_debug = std::env::var("GG_CLAUDE_SMOKE_DEBUG")
            .ok()
            .map(|value| value.trim() == "1")
            .unwrap_or(false);
        let mut recent_matching_events: std::collections::VecDeque<String> =
            std::collections::VecDeque::new();
        while std::time::Instant::now() < deadline {
            let events_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/sessions/{session_id}/events?after_seq={}",
                            event_cursor.unwrap_or(0)
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("session events response");
            assert_eq!(events_response.status(), StatusCode::OK);
            let events_json: serde_json::Value = serde_json::from_slice(
                &to_bytes(events_response.into_body(), usize::MAX)
                    .await
                    .expect("session events body"),
            )
            .expect("session events json");

            let events = events_json
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            if let Some(last_seq) = events
                .iter()
                .filter_map(|event| event.get("seq").and_then(serde_json::Value::as_i64))
                .max()
            {
                event_cursor = Some(last_seq);
            }

            for event in events {
                let kind = event
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let event_seq = event
                    .get("seq")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or_default();
                if smoke_debug {
                    let event_turn_id = event
                        .get("turn_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("<none>");
                    eprintln!(
                        "[claude-smoke] session_event seq={} kind={} turn_id={}",
                        event_seq, kind, event_turn_id
                    );
                }
                let is_matching_turn = event
                    .get("turn_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| value == turn_id);
                if !is_matching_turn {
                    continue;
                }
                let summary = format!("{event_seq}:{kind}");
                recent_matching_events.push_back(summary.clone());
                while recent_matching_events.len() > 24 {
                    let _ = recent_matching_events.pop_front();
                }
                if smoke_debug {
                    eprintln!("[claude-smoke] event {summary}");
                }
                if kind == "approval.requested" {
                    if let Some(approval_id) = event
                        .get("payload")
                        .and_then(|payload| {
                            payload
                                .get("approval_id")
                                .or_else(|| payload.get("approvalId"))
                        })
                        .and_then(serde_json::Value::as_str)
                    {
                        if !accepted_approvals.contains(approval_id) {
                            let approval_response = router
                                .clone()
                                .oneshot(
                                    Request::builder()
                                        .method("POST")
                                        .uri(format!(
                                            "/v1/sessions/{session_id}/approvals/{approval_id}"
                                        ))
                                        .header(header::CONTENT_TYPE, "application/json")
                                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                                        .body(Body::from(
                                            serde_json::json!({
                                                "decision": "accept",
                                                "payload": null,
                                            })
                                            .to_string(),
                                        ))
                                        .unwrap(),
                                )
                                .await
                                .expect("approval response");
                            assert_eq!(approval_response.status(), StatusCode::OK);
                            accepted_approvals.insert(approval_id.to_string());
                            if smoke_debug {
                                eprintln!("[claude-smoke] accepted approval_id={approval_id}");
                            }
                        }
                    }
                }
                if matches!(kind, "turn.completed" | "turn.failed" | "turn.interrupted") {
                    saw_terminal_event = true;
                    terminal_last_message = event
                        .get("payload")
                        .and_then(|payload| payload.get("usage"))
                        .and_then(|usage| usage.get("last_message"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    break;
                }
            }

            if saw_terminal_event {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let session_snapshot_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("session snapshot response");
        let session_snapshot_status = session_snapshot_response.status();
        let session_snapshot: serde_json::Value = serde_json::from_slice(
            &to_bytes(session_snapshot_response.into_body(), usize::MAX)
                .await
                .expect("session snapshot body"),
        )
        .unwrap_or(serde_json::json!({
            "error": "failed to parse session snapshot body",
        }));
        assert!(
            saw_terminal_event,
            "Claude turn did not reach terminal state; recent_events={:?}; approvals_accepted={:?}; session_status_code={}; session_snapshot={}",
            recent_matching_events,
            accepted_approvals,
            session_snapshot_status,
            session_snapshot
        );
        assert!(
            marker_path.exists(),
            "expected Claude MCP tool call to create marker file {}",
            marker_path.display()
        );
        if let Some(last_message) = terminal_last_message.as_deref() {
            assert!(
                last_message.contains("MCP_SMOKE_DONE"),
                "Claude terminal message did not acknowledge MCP flow: {last_message}"
            );
        }

        let close_response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/close"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("close response");
        assert_eq!(close_response.status(), StatusCode::OK);
        smoke_server_handle.abort();
        match prior_bridge_home_override {
            Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_HOME", value),
            None => std::env::remove_var("GG_CLAUDE_BRIDGE_HOME"),
        }
        match prior_bridge_config_override {
            Some(value) => std::env::set_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR", value),
            None => std::env::remove_var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR"),
        }
    }

    #[tokio::test]
    async fn mixed_provider_team_flow_uses_shared_runtime_services() {
        let (router, token, _temp_dir) = build_mixed_provider_test_router().await;

        let codex_session_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "codex",
                            "model": "test-model",
                            "cwd": null,
                            "permission_mode": null,
                            "metadata": {}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create codex session");
        assert_eq!(codex_session_response.status(), StatusCode::OK);
        let codex_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(codex_session_response.into_body(), usize::MAX)
                .await
                .expect("codex body"),
        )
        .expect("codex json");
        let codex_session_id = codex_json["id"].as_str().expect("codex session id");

        let claude_session_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "claude",
                            "model": "test-claude-model",
                            "cwd": null,
                            "permission_mode": null,
                            "metadata": {}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create claude session");
        assert_eq!(claude_session_response.status(), StatusCode::OK);
        let claude_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(claude_session_response.into_body(), usize::MAX)
                .await
                .expect("claude body"),
        )
        .expect("claude json");
        let claude_session_id = claude_json["id"].as_str().expect("claude session id");

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "mixed-provider-team",
                            "lead_agent_id": codex_session_id,
                            "member_agent_ids": []
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let create_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("create team body"),
        )
        .expect("create team json");
        let team_id = create_team_json["team"]["id"].as_str().expect("team id");

        let join_member_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/members"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "agent_id": claude_session_id,
                            "title": "Claude Teammate"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("join member");
        assert_eq!(join_member_response.status(), StatusCode::OK);

        let send_direct_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/messages"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "sender_agent_id": codex_session_id,
                            "recipient_agent_id": claude_session_id,
                            "input": [{"type":"text","text":"hello mixed provider"}],
                            "priority": "normal",
                            "policy": "non_interrupting"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("send direct");
        assert_eq!(send_direct_response.status(), StatusCode::OK);
        let direct_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(send_direct_response.into_body(), usize::MAX)
                .await
                .expect("send direct body"),
        )
        .expect("send direct json");
        assert_eq!(
            direct_json["message"]["team_id"].as_str(),
            Some(team_id),
            "team comms should accept mixed-provider sender/recipient sessions"
        );
    }

    #[tokio::test]
    async fn mcp_process_run_and_logs_share_runtime_process_service() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create session");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_bytes = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body");
        let created: serde_json::Value =
            serde_json::from_slice(&create_bytes).expect("create json");
        let session_id = created["id"].as_str().expect("session id").to_string();

        let invoke_body = serde_json::json!({
            "namespace": "gg_process",
            "tool_name": "gg_process_run",
            "caller_agent_id": session_id,
            "invocation_id": "inv_1",
            "args": {
                "command": "echo phase4_mcp_runtime_path"
            }
        });
        let invoke_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/mcp/invoke")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(invoke_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("invoke");
        assert_eq!(invoke_response.status(), StatusCode::OK);
        let invoke_bytes = to_bytes(invoke_response.into_body(), usize::MAX)
            .await
            .expect("invoke body");
        let invoke_json: serde_json::Value =
            serde_json::from_slice(&invoke_bytes).expect("invoke json");
        assert_eq!(invoke_json["ok"].as_bool(), Some(true));
        let process_id = invoke_json
            .pointer("/result/process/process_id")
            .and_then(serde_json::Value::as_str)
            .expect("process_id")
            .to_string();

        let mut done = false;
        for _ in 0..80 {
            let get_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/processes/{process_id}?session_id={session_id}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("get process");
            assert_eq!(get_response.status(), StatusCode::OK);
            let get_bytes = to_bytes(get_response.into_body(), usize::MAX)
                .await
                .expect("get process body");
            let process_json: serde_json::Value =
                serde_json::from_slice(&get_bytes).expect("get process json");
            let status = process_json
                .pointer("/process/status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(done, "process did not reach terminal state");

        let logs_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}/logs?session_id={session_id}&stream=stdout"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("logs response");
        assert_eq!(logs_response.status(), StatusCode::OK);
        let logs_bytes = to_bytes(logs_response.into_body(), usize::MAX)
            .await
            .expect("logs body");
        let logs_json: serde_json::Value = serde_json::from_slice(&logs_bytes).expect("logs json");
        let combined = logs_json
            .as_array()
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter_map(|row| row.get("content"))
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            combined.contains("phase4_mcp_runtime_path"),
            "expected process output in logs, got {combined}"
        );
    }

    #[tokio::test]
    async fn process_output_events_are_sampled_while_logs_remain_authoritative() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create session");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_response.into_body(), usize::MAX)
                .await
                .expect("create body"),
        )
        .expect("create json");
        let session_id = create_json["id"].as_str().expect("session id").to_string();

        let run_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/processes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "command": "seq 1 2500",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("run process");
        assert_eq!(run_response.status(), StatusCode::OK);
        let run_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(run_response.into_body(), usize::MAX)
                .await
                .expect("run body"),
        )
        .expect("run json");
        let process_id = run_json
            .pointer("/process/process_id")
            .and_then(serde_json::Value::as_str)
            .expect("process id")
            .to_string();

        let mut completed = false;
        for _ in 0..100 {
            let get_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/processes/{process_id}?session_id={session_id}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("get process");
            assert_eq!(get_response.status(), StatusCode::OK);
            let process_json: serde_json::Value = serde_json::from_slice(
                &to_bytes(get_response.into_body(), usize::MAX)
                    .await
                    .expect("get body"),
            )
            .expect("process json");
            let status = process_json
                .pointer("/process/status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(completed, "process did not reach terminal state");

        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/processes/{process_id}/events?limit=10000"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("events");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
            &to_bytes(events_response.into_body(), usize::MAX)
                .await
                .expect("events body"),
        )
        .expect("events json");
        let sampled_output_events = events
            .iter()
            .filter(|event| event.kind == "process.output")
            .count();
        assert!(
            sampled_output_events > 0,
            "expected sampled process.output events"
        );
        assert!(
            sampled_output_events < 2500,
            "expected output events to be sampled/coalesced"
        );

        let logs_response = router
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}/logs?session_id={session_id}&stream=stdout&tail_lines=3000"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("logs");
        assert_eq!(logs_response.status(), StatusCode::OK);
        let logs_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(logs_response.into_body(), usize::MAX)
                .await
                .expect("logs body"),
        )
        .expect("logs json");
        let output = logs_json
            .as_array()
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter_map(|row| row.get("content"))
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            output.contains("2500"),
            "expected full stdout log content to remain retrievable"
        );
    }

    #[tokio::test]
    async fn process_http_ownership_enforced_by_session_identity() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_one = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create one");
        let create_one_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_one.into_body(), usize::MAX)
                .await
                .expect("create one body"),
        )
        .expect("create one json");
        let owner_session_id = create_one_json["id"]
            .as_str()
            .expect("owner session id")
            .to_string();

        let create_two = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create two");
        let create_two_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_two.into_body(), usize::MAX)
                .await
                .expect("create two body"),
        )
        .expect("create two json");
        let other_session_id = create_two_json["id"]
            .as_str()
            .expect("other session id")
            .to_string();

        let run_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/processes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "command": "echo owned",
                            "session_id": owner_session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("run process");
        assert_eq!(run_response.status(), StatusCode::OK);
        let run_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(run_response.into_body(), usize::MAX)
                .await
                .expect("run body"),
        )
        .expect("run json");
        let process_id = run_json
            .pointer("/process/process_id")
            .and_then(serde_json::Value::as_str)
            .expect("process id")
            .to_string();

        let unauthorized_get = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}?session_id={other_session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("unauthorized get");
        assert_eq!(unauthorized_get.status(), StatusCode::BAD_REQUEST);

        let unauthorized_events = router
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}/events?session_id={other_session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("unauthorized events");
        assert_eq!(unauthorized_events.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn mcp_body_limit_is_scoped_to_mcp_routes_only() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create session");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_response.into_body(), usize::MAX)
                .await
                .expect("create body"),
        )
        .expect("create json");
        let session_id = create_json["id"].as_str().expect("session id").to_string();

        let oversized = "x".repeat(MCP_MAX_REQUEST_BODY_BYTES + 4096);
        let oversized_mcp = serde_json::json!({
            "namespace": "gg_process",
            "tool_name": "gg_process_status",
            "caller_agent_id": session_id,
            "args": {
                "blob": oversized
            }
        });
        let oversized_mcp_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/mcp/invoke")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(oversized_mcp.to_string()))
                    .unwrap(),
            )
            .await
            .expect("oversized mcp response");
        assert_eq!(
            oversized_mcp_response.status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );

        let oversized_metadata = "m".repeat(MCP_MAX_REQUEST_BODY_BYTES + 4096);
        let non_mcp_large_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {
                "oversized": oversized_metadata
            }
        });
        let non_mcp_response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(non_mcp_large_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("non mcp response");
        assert_eq!(non_mcp_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn max_concurrent_one_blocks_second_spawn_until_first_finishes() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create session");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_response.into_body(), usize::MAX)
                .await
                .expect("create body"),
        )
        .expect("create json");
        let session_id = create_json["id"].as_str().expect("session id").to_string();

        let first_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/processes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "command": "sleep 1",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("first process");
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(first_response.into_body(), usize::MAX)
                .await
                .expect("first body"),
        )
        .expect("first json");
        let first_process_id = first_json
            .pointer("/process/process_id")
            .and_then(serde_json::Value::as_str)
            .expect("first process id")
            .to_string();

        let second_router = router.clone();
        let second_token = token.clone();
        let second_session = session_id.clone();
        let mut second_handle = tokio::spawn(async move {
            second_router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/processes")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {second_token}"))
                        .body(Body::from(
                            serde_json::json!({
                                "command": "seq 1 500000",
                                "session_id": second_session,
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
        });

        let early = timeout(Duration::from_millis(150), &mut second_handle).await;
        assert!(
            early.is_err(),
            "second process started too early before slot became available"
        );

        let mut first_done = false;
        for _ in 0..80 {
            let get_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/processes/{first_process_id}?session_id={session_id}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("first get");
            let body = to_bytes(get_response.into_body(), usize::MAX)
                .await
                .expect("first get body");
            let row: serde_json::Value = serde_json::from_slice(&body).expect("first get json");
            let status = row
                .pointer("/process/status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if status == "completed" {
                first_done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        assert!(first_done, "first process did not complete in time");

        let second_response = second_handle
            .await
            .expect("second join")
            .expect("second response");
        assert_eq!(second_response.status(), StatusCode::OK);
        let second_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(second_response.into_body(), usize::MAX)
                .await
                .expect("second body"),
        )
        .expect("second json");
        let second_process_id = second_json
            .pointer("/process/process_id")
            .and_then(serde_json::Value::as_str)
            .expect("second process id")
            .to_string();

        let mut second_done = false;
        for _ in 0..240 {
            let get_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/processes/{second_process_id}?session_id={session_id}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("second get");
            assert_eq!(get_response.status(), StatusCode::OK);
            let body = to_bytes(get_response.into_body(), usize::MAX)
                .await
                .expect("second get body");
            let row: serde_json::Value = serde_json::from_slice(&body).expect("second get json");
            let status = row
                .pointer("/process/status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
                second_done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            second_done,
            "second process did not complete after first released the slot"
        );
    }

    #[tokio::test]
    async fn process_events_stream_delivers_live_sampled_output() {
        let (router, token, _temp_dir) = build_test_router().await;

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "test-model",
            "cwd": null,
            "permission_mode": null,
            "metadata": {}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create session");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_response.into_body(), usize::MAX)
                .await
                .expect("create body"),
        )
        .expect("create json");
        let session_id = create_json["id"].as_str().expect("session id").to_string();

        let run_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/processes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "command": "seq 1 200000",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("run process");
        assert_eq!(run_response.status(), StatusCode::OK);
        let run_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(run_response.into_body(), usize::MAX)
                .await
                .expect("run body"),
        )
        .expect("run json");
        let process_id = run_json
            .pointer("/process/process_id")
            .and_then(serde_json::Value::as_str)
            .expect("process id")
            .to_string();

        let replay_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}/events?session_id={session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("replay response");
        assert_eq!(replay_response.status(), StatusCode::OK);
        let replay_events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
            &to_bytes(replay_response.into_body(), usize::MAX)
                .await
                .expect("replay body"),
        )
        .expect("replay json");
        let cursor = replay_events.last().map(|event| event.seq).unwrap_or(0);

        let stream_router = router.clone();
        let stream_token = token.clone();
        let stream_process_id = process_id.clone();
        let stream_session_id = session_id.clone();
        let stream_handle = tokio::spawn(async move {
            stream_router
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/processes/{stream_process_id}/events/stream?session_id={stream_session_id}&after_seq={cursor}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {stream_token}"))
                        .header("x-gg-test-handoff-delay-ms", "200")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        });

        let stream_response = stream_handle
            .await
            .expect("stream join")
            .expect("stream response");
        assert_eq!(stream_response.status(), StatusCode::OK);

        let mut data_stream = stream_response.into_body().into_data_stream();
        let mut payload = String::new();
        for _ in 0..60 {
            let next = timeout(Duration::from_millis(300), data_stream.next()).await;
            match next {
                Ok(Some(Ok(chunk))) => {
                    payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                    if payload.contains("event: process.output")
                        || payload.contains("event: process.completed")
                    {
                        break;
                    }
                }
                _ => break,
            }
        }

        assert!(
            payload.contains("event: process.output")
                || payload.contains("event: process.completed"),
            "expected live process event in stream payload: {payload}"
        );
    }

    #[tokio::test]
    #[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json"]
    async fn smoke_real_codex_mcp_process_run_with_staged_auth_copy() {
        let home_dir = std::env::var("HOME").expect("HOME must be set");
        let source_auth = std::path::PathBuf::from(home_dir)
            .join(".gg")
            .join("codex")
            .join("auth.json");
        assert!(source_auth.exists(), "missing {}", source_auth.display());

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.claude.enabled = false;
        config.providers.codex.enabled = true;

        let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");
        let staged_auth = config
            .resolve_provider_dir("codex")
            .join("home")
            .join("auth.json");
        assert!(staged_auth.exists(), "expected staged auth copy");

        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "gpt-5.2-codex",
            "cwd": temp_dir.path().display().to_string(),
            "permission_mode": null,
            "metadata": {"smoke":"phase4_mcp_process"}
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create session");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_response.into_body(), usize::MAX)
                .await
                .expect("create body"),
        )
        .expect("create json");
        let session_id = create_json["id"].as_str().expect("session id").to_string();

        let invoke_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/mcp/invoke")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "namespace": "gg_process",
                            "tool_name": "gg_process_run",
                            "caller_agent_id": session_id,
                            "invocation_id": "smoke_phase4_mcp",
                            "args": {
                                "command": "echo phase4_mcp_smoke_token_74211"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("invoke");
        assert_eq!(invoke_response.status(), StatusCode::OK);
        let invoke_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(invoke_response.into_body(), usize::MAX)
                .await
                .expect("invoke body"),
        )
        .expect("invoke json");
        assert_eq!(invoke_json["ok"].as_bool(), Some(true));
        let process_id = invoke_json
            .pointer("/result/process/process_id")
            .and_then(serde_json::Value::as_str)
            .expect("process id")
            .to_string();

        let mut completed = false;
        for _ in 0..120 {
            let get_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/processes/{process_id}?session_id={session_id}"
                        ))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("get process");
            assert_eq!(get_response.status(), StatusCode::OK);
            let process_json: serde_json::Value = serde_json::from_slice(
                &to_bytes(get_response.into_body(), usize::MAX)
                    .await
                    .expect("get body"),
            )
            .expect("process json");
            let status = process_json
                .pointer("/process/status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
                completed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(completed, "process did not reach terminal state");

        let logs_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}/logs?session_id={session_id}&stream=stdout"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("logs");
        assert_eq!(logs_response.status(), StatusCode::OK);
        let logs_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(logs_response.into_body(), usize::MAX)
                .await
                .expect("logs body"),
        )
        .expect("logs json");
        let output = logs_json
            .as_array()
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter_map(|row| row.get("content"))
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            output.contains("phase4_mcp_smoke_token_74211"),
            "missing expected process output in logs"
        );
    }

    #[tokio::test]
    #[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json"]
    async fn smoke_real_codex_runtime_slice_with_staged_auth_copy() {
        let home_dir = std::env::var("HOME").expect("HOME must be set");
        let source_auth = std::path::PathBuf::from(home_dir)
            .join(".gg")
            .join("codex")
            .join("auth.json");
        assert!(
            source_auth.exists(),
            "expected real auth file at {}",
            source_auth.display()
        );

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.claude.enabled = false;
        config.providers.codex.enabled = true;

        let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");

        let staged_auth = config
            .resolve_provider_dir("codex")
            .join("home")
            .join("auth.json");
        assert!(staged_auth.exists(), "expected staged auth copy");

        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });

        let auth_status_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/providers/codex/auth/status")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("auth status response");
        assert_eq!(auth_status_response.status(), StatusCode::OK);
        let auth_status_bytes = to_bytes(auth_status_response.into_body(), usize::MAX)
            .await
            .expect("auth status body");
        let auth_status_json: serde_json::Value =
            serde_json::from_slice(&auth_status_bytes).expect("auth status json");
        assert_eq!(
            auth_status_json["authenticated"].as_bool(),
            Some(true),
            "expected codex auth to be authenticated"
        );

        let create_body = serde_json::json!({
            "provider": "codex",
            "model": "gpt-5.2-codex",
            "cwd": temp_dir.path().display().to_string(),
            "permission_mode": null,
            "metadata": {
                "smoke": "real_codex_phase3"
            }
        });
        let create_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("create response");
        assert_eq!(create_response.status(), StatusCode::OK);
        let create_bytes = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body");
        let created_session: serde_json::Value =
            serde_json::from_slice(&create_bytes).expect("create json");
        let session_id = created_session["id"]
            .as_str()
            .expect("session id")
            .to_string();

        let turn_body = serde_json::json!({
            "input": [
                {
                    "type": "text",
                    "text": "Reply with exactly this token and nothing else: phase3token_94731"
                }
            ],
            "expected_turn_id": null,
            "permission_mode": null
        });
        let send_turn_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/turns"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(turn_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("send turn response");
        assert_eq!(send_turn_response.status(), StatusCode::OK);
        let send_turn_bytes = to_bytes(send_turn_response.into_body(), usize::MAX)
            .await
            .expect("send turn body");
        let accepted_turn: serde_json::Value =
            serde_json::from_slice(&send_turn_bytes).expect("send turn json");
        let turn_id = accepted_turn["turn_id"]
            .as_str()
            .expect("turn id")
            .to_string();

        let mut finished = false;
        for _attempt in 0..80 {
            let get_session_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/sessions/{session_id}"))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("get session response");
            assert_eq!(get_session_response.status(), StatusCode::OK);
            let session_bytes = to_bytes(get_session_response.into_body(), usize::MAX)
                .await
                .expect("get session body");
            let session_json: serde_json::Value =
                serde_json::from_slice(&session_bytes).expect("get session json");
            if session_json["active_turn_id"].is_null() {
                finished = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        assert!(finished, "turn did not reach terminal state in time");

        let second_turn_body = serde_json::json!({
            "input": [
                {
                    "type": "text",
                    "text": "What exact token did you reply with previously? Reply with only that token."
                }
            ],
            "expected_turn_id": null,
            "permission_mode": null
        });
        let second_send_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/turns"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(second_turn_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("second send response");
        assert_eq!(second_send_response.status(), StatusCode::OK);

        let mut second_finished = false;
        for _attempt in 0..80 {
            let get_session_response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/sessions/{session_id}"))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("get session response");
            assert_eq!(get_session_response.status(), StatusCode::OK);
            let session_bytes = to_bytes(get_session_response.into_body(), usize::MAX)
                .await
                .expect("get session body");
            let session_json: serde_json::Value =
                serde_json::from_slice(&session_bytes).expect("get session json");
            if session_json["active_turn_id"].is_null() {
                second_finished = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        assert!(
            second_finished,
            "second turn did not reach terminal state in time"
        );

        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events_bytes = to_bytes(events_response.into_body(), usize::MAX)
            .await
            .expect("events body");
        let events: serde_json::Value = serde_json::from_slice(&events_bytes).expect("events json");
        let kinds = events
            .as_array()
            .expect("events array")
            .iter()
            .filter_map(|event| event.get("kind").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>();
        assert!(
            kinds.contains(&"turn.started"),
            "missing turn.started event"
        );
        assert!(
            kinds.contains(&"turn.completed")
                || kinds.contains(&"turn.failed")
                || kinds.contains(&"turn.interrupted"),
            "missing terminal turn event for {}",
            turn_id
        );
        let terminal_count_before_restart = kinds
            .iter()
            .filter(|kind| {
                **kind == "turn.completed"
                    || **kind == "turn.failed"
                    || **kind == "turn.interrupted"
            })
            .count();
        assert!(
            terminal_count_before_restart >= 2,
            "expected at least two terminal turns before restart"
        );

        // Simulate restart and verify persisted session can be resumed and used.
        let restarted = bootstrap_runtime(config.clone())
            .await
            .expect("restart bootstrap");
        let restarted_token = restarted.auth.bearer_token.clone();
        let restarted_router = build_router(AppState {
            app: restarted.app,
            runtime: restarted.runtime,
            bearer_token: restarted_token.clone(),
            public_base_url: restarted.public_base_url,
        });

        let resume_response = restarted_router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/resume"))
                    .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("resume response");
        assert_eq!(resume_response.status(), StatusCode::OK);

        let third_turn_body = serde_json::json!({
            "input": [
                {
                    "type": "text",
                    "text": "After resume, what exact token did you output earlier? Reply with only the token."
                }
            ],
            "expected_turn_id": null,
            "permission_mode": null
        });
        let third_send_response = restarted_router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/turns"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                    .body(Body::from(third_turn_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("third send response");
        assert_eq!(third_send_response.status(), StatusCode::OK);

        let mut third_finished = false;
        for _attempt in 0..80 {
            let get_session_response = restarted_router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/sessions/{session_id}"))
                        .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("get resumed session response");
            assert_eq!(get_session_response.status(), StatusCode::OK);
            let session_bytes = to_bytes(get_session_response.into_body(), usize::MAX)
                .await
                .expect("get resumed session body");
            let session_json: serde_json::Value =
                serde_json::from_slice(&session_bytes).expect("get resumed session json");
            if session_json["active_turn_id"].is_null() {
                third_finished = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        assert!(
            third_finished,
            "third turn after resume did not reach terminal state in time"
        );

        let resumed_events_response = restarted_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{session_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("resumed events response");
        assert_eq!(resumed_events_response.status(), StatusCode::OK);
        let resumed_events_bytes = to_bytes(resumed_events_response.into_body(), usize::MAX)
            .await
            .expect("resumed events body");
        let resumed_events: serde_json::Value =
            serde_json::from_slice(&resumed_events_bytes).expect("resumed events json");
        let resumed_kinds = resumed_events
            .as_array()
            .expect("resumed events array")
            .iter()
            .filter_map(|event| event.get("kind"))
            .filter_map(serde_json::Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert!(
            resumed_kinds.iter().any(|kind| kind == "session.resumed"),
            "expected session.resumed event after explicit resume"
        );
        let terminal_count_after_resume = resumed_kinds
            .iter()
            .filter(|kind| {
                kind.as_str() == "turn.completed"
                    || kind.as_str() == "turn.failed"
                    || kind.as_str() == "turn.interrupted"
            })
            .count();
        assert!(
            terminal_count_after_resume >= 3,
            "expected another terminal turn after resume"
        );

        let close_response = restarted_router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/sessions/{session_id}/close"))
                    .header(header::AUTHORIZATION, format!("Bearer {restarted_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("close response");
        assert_eq!(close_response.status(), StatusCode::OK);
    }

    async fn create_test_session(router: Router, token: &str, suite: &str) -> String {
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "codex",
                            "model": "test-model",
                            "metadata": {"suite": suite}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create session response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("create session body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("create session json");
        json["id"].as_str().expect("session id").to_string()
    }

    #[tokio::test]
    async fn team_routes_lifecycle_and_controls() {
        let (router, token, _temp_dir) = build_test_router().await;

        let leader_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
        let member_one_session_id =
            create_test_session(router.clone(), token.as_str(), "phase5").await;
        let member_two_session_id =
            create_test_session(router.clone(), token.as_str(), "phase5").await;

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "Lifecycle Team",
                            "lead_agent_id": leader_session_id,
                            "member_agent_ids": [member_one_session_id]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team response");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let create_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("create team body"),
        )
        .expect("create team json");
        let team_id = create_team_json["team"]["id"]
            .as_str()
            .expect("team id")
            .to_string();

        let list_teams_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/teams")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("list teams response");
        assert_eq!(list_teams_response.status(), StatusCode::OK);
        let list_teams_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(list_teams_response.into_body(), usize::MAX)
                .await
                .expect("list teams body"),
        )
        .expect("list teams json");
        assert_eq!(list_teams_json.as_array().map(Vec::len), Some(1));

        let get_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("get team response");
        assert_eq!(get_team_response.status(), StatusCode::OK);
        let get_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(get_team_response.into_body(), usize::MAX)
                .await
                .expect("get team body"),
        )
        .expect("get team json");
        assert_eq!(
            get_team_json["members"].as_array().map(Vec::len),
            Some(2),
            "team should include lead plus one member"
        );

        let join_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/members"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "agent_id": member_two_session_id,
                            "title": "Worker"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("join response");
        assert_eq!(join_response.status(), StatusCode::OK);

        let set_lead_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/lead"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "lead_agent_id": member_one_session_id
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("set lead response");
        assert_eq!(set_lead_response.status(), StatusCode::OK);
        let set_lead_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(set_lead_response.into_body(), usize::MAX)
                .await
                .expect("set lead body"),
        )
        .expect("set lead json");
        assert_eq!(
            set_lead_json["team"]["lead_agent_id"].as_str(),
            Some(member_one_session_id.as_str())
        );

        let remove_member_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/teams/{team_id}/members/{leader_session_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("remove member response");
        assert_eq!(remove_member_response.status(), StatusCode::OK);
        let remove_member_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(remove_member_response.into_body(), usize::MAX)
                .await
                .expect("remove member body"),
        )
        .expect("remove member json");
        assert_eq!(
            remove_member_json["members"].as_array().map(Vec::len),
            Some(2)
        );

        let direct_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/messages"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "sender_agent_id": member_one_session_id,
                            "recipient_agent_id": member_two_session_id,
                            "input": [{"type":"text","text":"phase5 lifecycle direct"}],
                            "policy": "non_interrupting",
                            "priority": "normal",
                            "idempotency_key": "phase5_lifecycle_direct"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("direct response");
        assert_eq!(direct_response.status(), StatusCode::OK);
        let direct_ack: serde_json::Value = serde_json::from_slice(
            &to_bytes(direct_response.into_body(), usize::MAX)
                .await
                .expect("direct body"),
        )
        .expect("direct ack");
        let message_id = direct_ack["message"]["id"]
            .as_str()
            .expect("message id")
            .to_string();
        let delivery_id = direct_ack["deliveries"][0]["id"]
            .as_str()
            .expect("delivery id")
            .to_string();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let list_messages_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/messages?limit=10"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("list messages response");
        assert_eq!(list_messages_response.status(), StatusCode::OK);
        let list_messages_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(list_messages_response.into_body(), usize::MAX)
                .await
                .expect("list messages body"),
        )
        .expect("list messages json");
        assert!(
            list_messages_json["messages"].as_array().map(|messages| {
                messages
                    .iter()
                    .any(|message| message["id"].as_str() == Some(message_id.as_str()))
            }) == Some(true),
            "expected direct message in list"
        );

        let list_deliveries_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/teams/{team_id}/deliveries?message_id={message_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("list deliveries response");
        assert_eq!(list_deliveries_response.status(), StatusCode::OK);
        let list_deliveries_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(list_deliveries_response.into_body(), usize::MAX)
                .await
                .expect("list deliveries body"),
        )
        .expect("list deliveries json");
        assert!(
            list_deliveries_json.as_array().map(|rows| !rows.is_empty()) == Some(true),
            "expected at least one delivery for direct message"
        );

        let retry_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/v1/teams/{team_id}/deliveries/{delivery_id}/retry"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("retry response");
        assert!(
            retry_response.status() == StatusCode::OK
                || retry_response.status() == StatusCode::BAD_REQUEST,
            "retry route should be wired and return a domain status"
        );

        let cancel_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/messages/{message_id}/cancel"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("cancel response");
        assert!(
            cancel_response.status() == StatusCode::OK
                || cancel_response.status() == StatusCode::BAD_REQUEST,
            "cancel route should be wired and return a domain status"
        );

        let snapshot_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/view"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("snapshot response");
        assert_eq!(snapshot_response.status(), StatusCode::OK);

        let interrupt_all_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/interrupt-all"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("interrupt-all response");
        assert_eq!(interrupt_all_response.status(), StatusCode::OK);
        let interrupt_all_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(interrupt_all_response.into_body(), usize::MAX)
                .await
                .expect("interrupt-all body"),
        )
        .expect("interrupt-all json");
        assert_eq!(
            interrupt_all_json["team_id"].as_str(),
            Some(team_id.as_str())
        );

        let delete_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/teams/{team_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("delete team response");
        assert_eq!(delete_team_response.status(), StatusCode::NO_CONTENT);

        let get_deleted_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("get deleted team response");
        assert_eq!(get_deleted_team_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn team_and_global_events_stream_replay_then_live() {
        let (router, token, _temp_dir) = build_test_router().await;

        let lead_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
        let member_one_session_id =
            create_test_session(router.clone(), token.as_str(), "phase5").await;
        let member_two_session_id =
            create_test_session(router.clone(), token.as_str(), "phase5").await;

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "SSE Team",
                            "lead_agent_id": lead_session_id,
                            "member_agent_ids": [member_one_session_id]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team response");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let create_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("create team body"),
        )
        .expect("create team json");
        let team_id = create_team_json["team"]["id"]
            .as_str()
            .expect("team id")
            .to_string();

        let direct_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/messages"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "sender_agent_id": lead_session_id,
                            "recipient_agent_id": member_one_session_id,
                            "input": [{"type":"text","text":"phase5 team stream seed"}],
                            "policy": "non_interrupting",
                            "priority": "normal",
                            "idempotency_key": "phase5_stream_direct_1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("direct response");
        assert_eq!(direct_response.status(), StatusCode::OK);

        let team_events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("team events response");
        assert_eq!(team_events_response.status(), StatusCode::OK);
        let team_events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
            &to_bytes(team_events_response.into_body(), usize::MAX)
                .await
                .expect("team events body"),
        )
        .expect("team events json");
        assert!(
            team_events.len() >= 2,
            "expected at least two team events for replay"
        );
        let team_cursor = team_events[0].seq;
        let replay_team_ids = team_events
            .iter()
            .filter(|event| event.seq > team_cursor)
            .map(|event| event.seq.to_string())
            .collect::<Vec<_>>();

        let team_stream_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/teams/{team_id}/events/stream?after_seq={team_cursor}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("team stream response");
        assert_eq!(team_stream_response.status(), StatusCode::OK);
        let mut team_stream = team_stream_response.into_body().into_data_stream();

        let join_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/members"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "agent_id": member_two_session_id
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("join response");
        assert_eq!(join_response.status(), StatusCode::OK);

        let mut team_sse_payload = String::new();
        let mut observed_team_live = false;
        for _ in 0..20 {
            let next = timeout(Duration::from_secs(1), team_stream.next()).await;
            if let Ok(Some(Ok(chunk))) = next {
                team_sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                if replay_team_ids
                    .iter()
                    .all(|seq| team_sse_payload.contains(format!("id: {seq}").as_str()))
                    && team_sse_payload.contains("event: team.member_joined")
                {
                    observed_team_live = true;
                    break;
                }
            }
        }
        assert!(
            observed_team_live,
            "expected team stream replay ids and team.member_joined live event in payload: {team_sse_payload}"
        );

        let global_events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("global events response");
        assert_eq!(global_events_response.status(), StatusCode::OK);
        let global_events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
            &to_bytes(global_events_response.into_body(), usize::MAX)
                .await
                .expect("global events body"),
        )
        .expect("global events json");
        assert!(
            global_events.len() >= 2,
            "expected at least two global events for replay"
        );
        let global_cursor = global_events[0].row_id;
        let replay_global_ids = global_events
            .iter()
            .filter(|event| event.row_id > global_cursor)
            .map(|event| event.row_id.to_string())
            .collect::<Vec<_>>();

        let global_stream_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/stream?after_seq={global_cursor}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("global stream response");
        assert_eq!(global_stream_response.status(), StatusCode::OK);
        let mut global_stream = global_stream_response.into_body().into_data_stream();

        let set_lead_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/lead"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "lead_agent_id": member_one_session_id
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("set lead response");
        assert_eq!(set_lead_response.status(), StatusCode::OK);

        let mut global_sse_payload = String::new();
        let mut observed_global_live = false;
        for _ in 0..20 {
            let next = timeout(Duration::from_secs(1), global_stream.next()).await;
            if let Ok(Some(Ok(chunk))) = next {
                global_sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
                if replay_global_ids
                    .iter()
                    .all(|seq| global_sse_payload.contains(format!("id: {seq}").as_str()))
                    && global_sse_payload.contains("event: team.lead_changed")
                {
                    observed_global_live = true;
                    break;
                }
            }
        }
        assert!(
            observed_global_live,
            "expected global stream replay ids and team.lead_changed live event in payload: {global_sse_payload}"
        );
    }

    #[tokio::test]
    async fn team_routes_direct_and_broadcast_create_deliveries_and_snapshot() {
        let (router, token, _temp_dir) = build_test_router().await;

        let leader_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
        let teammate_session_id =
            create_test_session(router.clone(), token.as_str(), "phase5").await;

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "Phase5 Team",
                            "lead_agent_id": leader_session_id,
                            "member_agent_ids": [teammate_session_id]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team response");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let create_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("create team body"),
        )
        .expect("create team json");
        let team_id = create_team_json
            .pointer("/team/id")
            .and_then(serde_json::Value::as_str)
            .expect("team id")
            .to_string();

        let direct_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/messages"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "sender_agent_id": leader_session_id,
                            "recipient_agent_id": teammate_session_id,
                            "input": [{"type":"text","text":"phase5 direct hello"}],
                            "policy": "non_interrupting",
                            "priority": "normal",
                            "idempotency_key": "phase5_direct_1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("direct send response");
        assert_eq!(direct_response.status(), StatusCode::OK);

        let broadcast_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/broadcasts"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "sender_agent_id": leader_session_id,
                            "input": [{"type":"text","text":"phase5 broadcast hello"}],
                            "policy": "start_new_turn_only",
                            "priority": "normal",
                            "include_sender": false,
                            "idempotency_key": "phase5_broadcast_1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("broadcast response");
        assert_eq!(broadcast_response.status(), StatusCode::OK);

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let deliveries_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/deliveries"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("deliveries response");
        assert_eq!(deliveries_response.status(), StatusCode::OK);
        let deliveries: Vec<runtime_core::TeamDeliveryRecord> = serde_json::from_slice(
            &to_bytes(deliveries_response.into_body(), usize::MAX)
                .await
                .expect("deliveries body"),
        )
        .expect("deliveries json");
        assert!(deliveries.len() >= 2, "expected at least two deliveries");

        let snapshot_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/view"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("snapshot response");
        assert_eq!(snapshot_response.status(), StatusCode::OK);
        let snapshot_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(snapshot_response.into_body(), usize::MAX)
                .await
                .expect("snapshot body"),
        )
        .expect("snapshot json");
        assert!(
            snapshot_json["messages"]
                .as_array()
                .map(|rows| !rows.is_empty())
                == Some(true),
            "expected snapshot messages"
        );
    }

    #[tokio::test]
    #[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json"]
    async fn smoke_real_codex_phase5_team_comms_slice() {
        let home_dir = std::env::var("HOME").expect("HOME must be set");
        let source_auth = std::path::PathBuf::from(home_dir)
            .join(".gg")
            .join("codex")
            .join("auth.json");
        assert!(source_auth.exists(), "missing {}", source_auth.display());

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.claude.enabled = false;
        config.providers.codex.enabled = true;

        let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");
        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });

        let create_session = |router: Router, token: String, cwd: String| async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/sessions")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(
                            serde_json::json!({
                                "provider": "codex",
                                "model": "gpt-5.2-codex",
                                "cwd": cwd,
                                "metadata": {"smoke":"phase5_team_comms"}
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .expect("create session response");
            assert_eq!(response.status(), StatusCode::OK);
            let json: serde_json::Value = serde_json::from_slice(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("create body"),
            )
            .expect("create json");
            json["id"].as_str().expect("session id").to_string()
        };

        let cwd = temp_dir.path().display().to_string();
        let lead_session_id = create_session(router.clone(), token.clone(), cwd.clone()).await;
        let member_session_id = create_session(router.clone(), token.clone(), cwd.clone()).await;

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "Real Codex Team",
                            "lead_agent_id": lead_session_id,
                            "member_agent_ids": [member_session_id]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let create_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("team body"),
        )
        .expect("team json");
        let team_id = create_team_json["team"]["id"]
            .as_str()
            .expect("team id")
            .to_string();

        let direct_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/messages"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "sender_agent_id": lead_session_id,
                            "recipient_agent_id": member_session_id,
                            "input": [{"type":"text","text":"Reply only with phase5teamtoken_89321"}],
                            "policy": "immediate_interrupt",
                            "priority": "high",
                            "idempotency_key": "phase5_smoke_direct_1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("direct response");
        assert_eq!(direct_response.status(), StatusCode::OK);

        let broadcast_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/broadcasts"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "sender_agent_id": lead_session_id,
                            "input": [{"type":"text","text":"Broadcast ack phase5teamtoken_89321"}],
                            "policy": "start_new_turn_only",
                            "priority": "normal",
                            "include_sender": true,
                            "idempotency_key": "phase5_smoke_broadcast_1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("broadcast response");
        assert_eq!(broadcast_response.status(), StatusCode::OK);

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let deliveries_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/deliveries"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("deliveries response");
        assert_eq!(deliveries_response.status(), StatusCode::OK);
        let deliveries_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(deliveries_response.into_body(), usize::MAX)
                .await
                .expect("deliveries body"),
        )
        .expect("deliveries json");
        assert!(
            deliveries_json.as_array().map(|rows| !rows.is_empty()) == Some(true),
            "expected non-empty deliveries"
        );

        let events_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("events response");
        assert_eq!(events_response.status(), StatusCode::OK);
        let events_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(events_response.into_body(), usize::MAX)
                .await
                .expect("events body"),
        )
        .expect("events json");
        let kinds = events_json
            .as_array()
            .into_iter()
            .flat_map(|events| events.iter())
            .filter_map(|event| event.get("kind"))
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>();
        assert!(
            kinds.iter().any(|kind| *kind == "team_message.created"),
            "expected team_message.created in team events"
        );
    }

    #[tokio::test]
    async fn phase6_spawn_member_with_created_worktree_and_cleanup_on_remove() {
        let (router, token, temp_dir) = build_test_router().await;
        let repo_dir = temp_dir.path().join("repo");
        std::fs::create_dir_all(&repo_dir).expect("create repo dir");
        std::fs::write(repo_dir.join("README.md"), "phase6\n").expect("write readme");
        let init_status = std::process::Command::new("git")
            .arg("init")
            .arg("-b")
            .arg("main")
            .arg(repo_dir.as_os_str())
            .status()
            .expect("git init");
        assert!(init_status.success(), "git init should succeed");
        let add_status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo_dir.as_os_str())
            .args(["add", "."])
            .status()
            .expect("git add");
        assert!(add_status.success(), "git add should succeed");
        let commit_status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo_dir.as_os_str())
            .args([
                "-c",
                "user.name=GG Runtime",
                "-c",
                "user.email=runtime@example.com",
                "commit",
                "-m",
                "init",
            ])
            .status()
            .expect("git commit");
        assert!(commit_status.success(), "git commit should succeed");

        let lead_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "codex",
                            "model": "test-model",
                            "cwd": repo_dir.display().to_string(),
                            "metadata": {"suite":"phase6"}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create lead session");
        assert_eq!(lead_response.status(), StatusCode::OK);
        let lead_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(lead_response.into_body(), usize::MAX)
                .await
                .expect("lead body"),
        )
        .expect("lead json");
        let lead_session_id = lead_json["id"].as_str().expect("lead id").to_string();

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "Phase6 Team",
                            "lead_agent_id": lead_session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let create_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("team body"),
        )
        .expect("team json");
        let team_id = create_team_json["team"]["id"]
            .as_str()
            .expect("team id")
            .to_string();

        let spawn_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/members/spawn"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "source_session_id": lead_session_id,
                            "title": "Phase 6 Implementer",
                            "prompt": "Implement phase 6.",
                            "worktree": {
                                "mode": "create",
                                "name": "phase6-worker",
                                "run_init_script": false
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("spawn response");
        assert_eq!(spawn_response.status(), StatusCode::OK);
        let spawn_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(spawn_response.into_body(), usize::MAX)
                .await
                .expect("spawn body"),
        )
        .expect("spawn json");
        let spawned_session_id = spawn_json["spawned_session"]["id"]
            .as_str()
            .expect("spawned session id")
            .to_string();
        let worktree_id = spawn_json["worktree"]["id"]
            .as_str()
            .expect("worktree id")
            .to_string();
        let worktree_cwd = spawn_json["worktree"]["worktree_cwd"]
            .as_str()
            .expect("worktree cwd")
            .to_string();
        assert!(
            Path::new(worktree_cwd.as_str()).exists(),
            "spawn-created worktree path should exist"
        );

        tokio::time::sleep(Duration::from_millis(150)).await;
        let deliveries_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/deliveries"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("deliveries response");
        assert_eq!(deliveries_response.status(), StatusCode::OK);
        let deliveries: serde_json::Value = serde_json::from_slice(
            &to_bytes(deliveries_response.into_body(), usize::MAX)
                .await
                .expect("deliveries body"),
        )
        .expect("deliveries json");
        assert!(
            deliveries.as_array().map(|rows| !rows.is_empty()) == Some(true),
            "onboarding delivery should be persisted"
        );

        let remove_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/teams/{team_id}/members/{spawned_session_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("remove response");
        assert_eq!(remove_response.status(), StatusCode::OK);

        tokio::time::sleep(Duration::from_millis(250)).await;
        let cleanup_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/worktrees/{worktree_id}/cleanup"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("cleanup response");
        assert_eq!(cleanup_response.status(), StatusCode::OK);
        let cleanup_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(cleanup_response.into_body(), usize::MAX)
                .await
                .expect("cleanup body"),
        )
        .expect("cleanup json");
        assert!(
            cleanup_json["status"].as_str() == Some("deleted")
                || cleanup_json["status"].as_str() == Some("cleanup_failed")
                || cleanup_json["status"].as_str() == Some("retained_by_policy")
                || cleanup_json["status"].as_str() == Some("skipped_live_claims"),
            "cleanup endpoint should report structured status"
        );
    }

    #[tokio::test]
    async fn phase6_spawn_member_use_existing_mode_reuses_existing_worktree() {
        let (router, token, temp_dir) = build_test_router().await;
        let repo_dir = temp_dir.path().join("repo");
        std::fs::create_dir_all(&repo_dir).expect("create repo dir");
        std::fs::write(repo_dir.join("README.md"), "phase6 use_existing\n").expect("write readme");
        let init_status = std::process::Command::new("git")
            .arg("init")
            .arg("-b")
            .arg("main")
            .arg(repo_dir.as_os_str())
            .status()
            .expect("git init");
        assert!(init_status.success(), "git init should succeed");
        let add_status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo_dir.as_os_str())
            .args(["add", "."])
            .status()
            .expect("git add");
        assert!(add_status.success(), "git add should succeed");
        let commit_status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo_dir.as_os_str())
            .args([
                "-c",
                "user.name=GG Runtime",
                "-c",
                "user.email=runtime@example.com",
                "commit",
                "-m",
                "init",
            ])
            .status()
            .expect("git commit");
        assert!(commit_status.success(), "git commit should succeed");

        let lead_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "codex",
                            "model": "test-model",
                            "cwd": repo_dir.display().to_string(),
                            "metadata": {"suite":"phase6"}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create lead session");
        assert_eq!(lead_response.status(), StatusCode::OK);
        let lead_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(lead_response.into_body(), usize::MAX)
                .await
                .expect("lead body"),
        )
        .expect("lead json");
        let lead_session_id = lead_json["id"].as_str().expect("lead id").to_string();

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "Phase6 Existing Team",
                            "lead_agent_id": lead_session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let create_team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("team body"),
        )
        .expect("team json");
        let team_id = create_team_json["team"]["id"]
            .as_str()
            .expect("team id")
            .to_string();

        let create_worktree_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/worktrees")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "team_id": team_id,
                            "source_session_id": lead_session_id,
                            "worktree_name": "phase6-existing-worker",
                            "branch_prefix": "gg",
                            "run_init_script": false,
                            "deletion_policy": "retain_on_last_claim",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create worktree response");
        assert_eq!(create_worktree_response.status(), StatusCode::OK);
        let create_worktree_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_worktree_response.into_body(), usize::MAX)
                .await
                .expect("create worktree body"),
        )
        .expect("create worktree json");
        let existing_worktree_id = create_worktree_json["worktree"]["id"]
            .as_str()
            .expect("existing worktree id")
            .to_string();

        let spawn_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/members/spawn"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "source_session_id": lead_session_id,
                            "title": "Phase 6 Existing",
                            "prompt": "Use existing worktree.",
                            "worktree": {
                                "mode": "use_existing",
                                "name": "phase6-existing-worker",
                                "branch_prefix": "gg",
                                "run_init_script": false
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("spawn response");
        assert_eq!(spawn_response.status(), StatusCode::OK);
        let spawn_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(spawn_response.into_body(), usize::MAX)
                .await
                .expect("spawn body"),
        )
        .expect("spawn json");
        assert_eq!(
            spawn_json["worktree_assignment_mode"].as_str(),
            Some("reused"),
            "documented use_existing mode must select the reuse path"
        );
        assert_eq!(
            spawn_json["worktree_created_by_operation"].as_bool(),
            Some(false),
            "reused path must not report created worktree ownership"
        );
        assert_eq!(
            spawn_json["worktree"]["id"].as_str(),
            Some(existing_worktree_id.as_str())
        );
    }

    #[tokio::test]
    #[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json and phase6 worktree tooling"]
    async fn smoke_real_codex_phase6_spawn_worktree_and_cleanup() {
        let home_dir = std::env::var("HOME").expect("HOME must be set");
        let source_auth = std::path::PathBuf::from(home_dir)
            .join(".gg")
            .join("codex")
            .join("auth.json");
        assert!(source_auth.exists(), "missing {}", source_auth.display());

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let repo_dir = temp_dir.path().join("repo");
        std::fs::create_dir_all(&repo_dir).expect("create repo dir");
        std::fs::write(repo_dir.join("README.md"), "phase6 smoke\n").expect("write readme");
        assert!(std::process::Command::new("git")
            .arg("init")
            .arg("-b")
            .arg("main")
            .arg(repo_dir.as_os_str())
            .status()
            .expect("git init")
            .success());
        assert!(std::process::Command::new("git")
            .arg("-C")
            .arg(repo_dir.as_os_str())
            .args(["add", "."])
            .status()
            .expect("git add")
            .success());
        assert!(std::process::Command::new("git")
            .arg("-C")
            .arg(repo_dir.as_os_str())
            .args([
                "-c",
                "user.name=GG Runtime",
                "-c",
                "user.email=runtime@example.com",
                "commit",
                "-m",
                "init",
            ])
            .status()
            .expect("git commit")
            .success());

        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.claude.enabled = false;
        config.providers.codex.enabled = true;
        let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");

        let token = bootstrapped.auth.bearer_token.clone();
        let router = build_router(AppState {
            app: bootstrapped.app,
            runtime: bootstrapped.runtime,
            bearer_token: token.clone(),
            public_base_url: bootstrapped.public_base_url,
        });

        let lead_session_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "codex",
                            "model": "gpt-5.2-codex",
                            "cwd": repo_dir.display().to_string(),
                            "metadata": {"smoke":"phase6"}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create lead session");
        assert_eq!(lead_session_response.status(), StatusCode::OK);
        let lead_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(lead_session_response.into_body(), usize::MAX)
                .await
                .expect("lead body"),
        )
        .expect("lead json");
        let lead_session_id = lead_json["id"].as_str().expect("lead id").to_string();

        let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "Phase6 Smoke Team",
                            "lead_agent_id": lead_session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team");
        assert_eq!(create_team_response.status(), StatusCode::OK);
        let team_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(create_team_response.into_body(), usize::MAX)
                .await
                .expect("team body"),
        )
        .expect("team json");
        let team_id = team_json["team"]["id"]
            .as_str()
            .expect("team id")
            .to_string();

        let spawn_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/teams/{team_id}/members/spawn"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "source_session_id": lead_session_id,
                            "provider": "codex",
                            "model": "gpt-5.2-codex",
                            "title": "Phase 6 Implementer",
                            "prompt": "Execute phase 6 smoke instructions.",
                            "worktree": {
                                "mode": "create",
                                "name": "phase6-smoke-worker",
                                "run_init_script": false
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("spawn member");
        assert_eq!(spawn_response.status(), StatusCode::OK);
        let spawn_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(spawn_response.into_body(), usize::MAX)
                .await
                .expect("spawn body"),
        )
        .expect("spawn json");
        let spawned_session_id = spawn_json["spawned_session"]["id"]
            .as_str()
            .expect("spawned id")
            .to_string();
        let worktree_id = spawn_json["worktree"]["id"]
            .as_str()
            .expect("worktree id")
            .to_string();

        tokio::time::sleep(Duration::from_millis(300)).await;
        let deliveries_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/teams/{team_id}/deliveries"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("deliveries");
        assert_eq!(deliveries_response.status(), StatusCode::OK);
        let deliveries_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(deliveries_response.into_body(), usize::MAX)
                .await
                .expect("deliveries body"),
        )
        .expect("deliveries json");
        assert!(
            deliveries_json.as_array().map(|rows| !rows.is_empty()) == Some(true),
            "expected onboarding delivery rows"
        );

        let remove_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/teams/{team_id}/members/{spawned_session_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("remove member");
        assert_eq!(remove_response.status(), StatusCode::OK);

        let cleanup_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/worktrees/{worktree_id}/cleanup"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({"reason":"phase6_smoke"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("cleanup");
        assert_eq!(cleanup_response.status(), StatusCode::OK);

        let diagnostics_response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/diagnostics/team-operations?team_id={team_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("diagnostics");
        assert_eq!(diagnostics_response.status(), StatusCode::OK);
    }
}
