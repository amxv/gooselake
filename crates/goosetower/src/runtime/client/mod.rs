use std::fmt;

mod error;
#[cfg(test)]
mod tests;
mod types;

pub use self::error::RuntimeClientError;
pub use self::types::*;
use std::time::Duration;

use reqwest::{Client, Method};
use runtime_core::{
    ApprovalResponseInput, ManagedWorktreeRecord, ProcessDetails, ProcessLogsChunk, ProcessSummary,
    ProviderAuthStatus, ProviderKind, ResumeSessionInput, RuntimeEventRecord, SendTurnAccepted,
    SendTurnInput, SessionRecord, TeamDeliveryRecord, TeamInterruptAllResponse,
    TeamMemberSpawnResponse, TeamMessageAck, TeamViewSnapshotResponse, TeamWithMembers,
    WorktreeClaimResponse, WorktreeCleanupResponse, WorktreeCreateResponse,
    WorktreeReleaseResponse,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct GooselakeRuntimeClientConfig {
    pub source_id: String,
    pub source_epoch: String,
    pub base_url: String,
    pub bearer_token: Option<String>,
    pub timeout: Duration,
}

impl GooselakeRuntimeClientConfig {
    pub fn new(
        source_id: impl Into<String>,
        source_epoch: impl Into<String>,
        base_url: impl Into<String>,
        bearer_token: Option<String>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            source_epoch: source_epoch.into(),
            base_url: base_url.into(),
            bearer_token,
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
pub struct GooselakeRuntimeClient {
    http: Client,
    config: GooselakeRuntimeClientConfig,
}

impl fmt::Debug for GooselakeRuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GooselakeRuntimeClient")
            .field("source_id", &self.config.source_id)
            .field("source_epoch", &self.config.source_epoch)
            .field("base_url", &self.config.base_url)
            .finish_non_exhaustive()
    }
}

impl GooselakeRuntimeClient {
    pub fn new(config: GooselakeRuntimeClientConfig) -> Result<Self, RuntimeClientError> {
        let http = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(RuntimeClientError::Transport)?;
        Ok(Self { http, config })
    }

    pub fn source_id(&self) -> &str {
        self.config.source_id.as_str()
    }

    pub fn source_epoch(&self) -> &str {
        self.config.source_epoch.as_str()
    }

    pub fn base_url(&self) -> &str {
        self.config.base_url.as_str()
    }

    pub fn bearer_token(&self) -> Option<&str> {
        self.config.bearer_token.as_deref()
    }

    pub fn http(&self) -> &Client {
        &self.http
    }

    pub fn endpoint(&self, path: &str) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        let path = path.trim_start_matches('/');
        format!("{base}/{path}")
    }

    pub async fn health(&self) -> Result<RuntimeHealthResponse, RuntimeClientError> {
        self.request_json(Method::GET, "/health", Option::<&()>::None)
            .await
    }

    pub async fn protected_health(&self) -> Result<RuntimeHealthResponse, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/health", Option::<&()>::None)
            .await
    }

    pub async fn version(&self) -> Result<RuntimeVersionResponse, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/version", Option::<&()>::None)
            .await
    }

    pub async fn providers(&self) -> Result<ProviderListResponse, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/providers", Option::<&()>::None)
            .await
    }

    pub async fn provider_models(
        &self,
        provider: ProviderKind,
    ) -> Result<ProviderModelsResponse, RuntimeClientError> {
        self.request_json(
            Method::GET,
            &format!("/v1/providers/{}/models", provider.as_str()),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn provider_auth_status(
        &self,
        provider: ProviderKind,
    ) -> Result<ProviderAuthStatus, RuntimeClientError> {
        self.request_json(
            Method::GET,
            &format!("/v1/providers/{}/auth/status", provider.as_str()),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionRecord>, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/sessions", Option::<&()>::None)
            .await
    }

    pub async fn get_session(&self, session_id: &str) -> Result<SessionRecord, RuntimeClientError> {
        self.request_json(
            Method::GET,
            &format!("/v1/sessions/{session_id}"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn create_session(
        &self,
        input: &runtime_core::CreateSessionInput,
    ) -> Result<SessionRecord, RuntimeClientError> {
        self.request_json(Method::POST, "/v1/sessions", Some(input))
            .await
    }

    pub async fn resume_session(
        &self,
        session_id: &str,
        input: &ResumeSessionInput,
    ) -> Result<SessionRecord, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/sessions/{session_id}/resume"),
            Some(input),
        )
        .await
    }

    pub async fn close_session(
        &self,
        session_id: &str,
        input: &CloseSessionRequest,
    ) -> Result<SessionRecord, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/sessions/{session_id}/close"),
            Some(input),
        )
        .await
    }

    pub async fn send_turn(
        &self,
        session_id: &str,
        input: &SendTurnInput,
    ) -> Result<SendTurnAccepted, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/sessions/{session_id}/turns"),
            Some(input),
        )
        .await
    }

    pub async fn interrupt_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), RuntimeClientError> {
        self.request_empty(
            Method::POST,
            &format!("/v1/sessions/{session_id}/turns/{turn_id}/interrupt"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn respond_approval(
        &self,
        session_id: &str,
        approval_id: &str,
        input: &ApprovalResponseInput,
    ) -> Result<runtime_core::ApprovalRecord, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/sessions/{session_id}/approvals/{approval_id}"),
            Some(input),
        )
        .await
    }

    pub async fn list_teams(&self) -> Result<Vec<TeamWithMembers>, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/teams", Option::<&()>::None)
            .await
    }

    pub async fn get_team(&self, team_id: &str) -> Result<TeamWithMembers, RuntimeClientError> {
        self.request_json(
            Method::GET,
            &format!("/v1/teams/{team_id}"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn create_team(
        &self,
        input: &TeamCreateInput,
    ) -> Result<TeamWithMembers, RuntimeClientError> {
        self.request_json(Method::POST, "/v1/teams", Some(input))
            .await
    }

    pub async fn join_team(
        &self,
        team_id: &str,
        input: &TeamJoinInput,
    ) -> Result<TeamWithMembers, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/members"),
            Some(input),
        )
        .await
    }

    pub async fn spawn_team_member(
        &self,
        team_id: &str,
        input: &TeamMemberSpawnInput,
    ) -> Result<TeamMemberSpawnResponse, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/members/spawn"),
            Some(input),
        )
        .await
    }

    pub async fn remove_team_member(
        &self,
        team_id: &str,
        agent_id: &str,
    ) -> Result<TeamWithMembers, RuntimeClientError> {
        self.request_json(
            Method::DELETE,
            &format!("/v1/teams/{team_id}/members/{agent_id}"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn set_team_lead(
        &self,
        team_id: &str,
        input: &TeamSetLeadInput,
    ) -> Result<TeamWithMembers, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/lead"),
            Some(input),
        )
        .await
    }

    pub async fn delete_team(&self, team_id: &str) -> Result<(), RuntimeClientError> {
        self.request_empty(
            Method::DELETE,
            &format!("/v1/teams/{team_id}"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn send_team_direct(
        &self,
        team_id: &str,
        input: &TeamDirectInput,
    ) -> Result<TeamMessageAck, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/messages"),
            Some(input),
        )
        .await
    }

    pub async fn send_team_broadcast(
        &self,
        team_id: &str,
        input: &TeamBroadcastInput,
    ) -> Result<TeamMessageAck, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/broadcasts"),
            Some(input),
        )
        .await
    }

    pub async fn list_team_messages(
        &self,
        team_id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<runtime_core::TeamListMessagesResponse, RuntimeClientError> {
        let path = query_path(
            &format!("/v1/teams/{team_id}/messages"),
            [
                ("cursor", cursor.map(ToOwned::to_owned)),
                ("limit", limit.map(|value| value.to_string())),
            ],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    pub async fn list_team_deliveries(
        &self,
        team_id: &str,
        message_id: Option<&str>,
        recipient_agent_id: Option<&str>,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeClientError> {
        let path = query_path(
            &format!("/v1/teams/{team_id}/deliveries"),
            [
                ("message_id", message_id.map(ToOwned::to_owned)),
                (
                    "recipient_agent_id",
                    recipient_agent_id.map(ToOwned::to_owned),
                ),
            ],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    pub async fn retry_team_delivery(
        &self,
        team_id: &str,
        delivery_id: &str,
    ) -> Result<TeamDeliveryRecord, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/deliveries/{delivery_id}/retry"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn cancel_team_message(
        &self,
        team_id: &str,
        message_id: &str,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/messages/{message_id}/cancel"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn team_view(
        &self,
        team_id: &str,
        message_cursor: Option<&str>,
        message_limit: Option<usize>,
        include_delivery_map: Option<bool>,
        delivery_recipient_filter: Option<&str>,
    ) -> Result<TeamViewSnapshotResponse, RuntimeClientError> {
        let path = query_path(
            &format!("/v1/teams/{team_id}/view"),
            [
                ("message_cursor", message_cursor.map(ToOwned::to_owned)),
                (
                    "message_limit",
                    message_limit.map(|value| value.to_string()),
                ),
                (
                    "include_delivery_map",
                    include_delivery_map.map(|value| value.to_string()),
                ),
                (
                    "delivery_recipient_filter",
                    delivery_recipient_filter.map(ToOwned::to_owned),
                ),
            ],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    pub async fn interrupt_all_team_turns(
        &self,
        team_id: &str,
    ) -> Result<TeamInterruptAllResponse, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/teams/{team_id}/interrupt-all"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn list_processes(
        &self,
        session_id: Option<&str>,
        include_completed: Option<bool>,
    ) -> Result<Vec<ProcessSummary>, RuntimeClientError> {
        let path = query_path(
            "/v1/processes",
            [
                ("session_id", session_id.map(ToOwned::to_owned)),
                (
                    "include_completed",
                    include_completed.map(|value| value.to_string()),
                ),
            ],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    pub async fn get_process(
        &self,
        process_id: &str,
        session_id: Option<&str>,
    ) -> Result<ProcessDetails, RuntimeClientError> {
        let path = query_path(
            &format!("/v1/processes/{process_id}"),
            [("session_id", session_id.map(ToOwned::to_owned))],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    pub async fn get_process_logs(
        &self,
        process_id: &str,
        query: &ProcessLogsQuery,
    ) -> Result<Vec<ProcessLogsChunk>, RuntimeClientError> {
        let path = query_path(
            &format!("/v1/processes/{process_id}/logs"),
            [
                ("session_id", query.session_id.clone()),
                ("stream", query.stream.clone()),
                (
                    "head_lines",
                    query.head_lines.map(|value| value.to_string()),
                ),
                (
                    "tail_lines",
                    query.tail_lines.map(|value| value.to_string()),
                ),
                ("max_bytes", query.max_bytes.map(|value| value.to_string())),
            ],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    pub async fn start_process(
        &self,
        input: &ProcessStartInput,
    ) -> Result<ProcessDetails, RuntimeClientError> {
        self.request_json(Method::POST, "/v1/processes", Some(input))
            .await
    }

    pub async fn kill_process(
        &self,
        process_id: &str,
        input: &ProcessKillInput,
    ) -> Result<ProcessDetails, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/processes/{process_id}/kill"),
            Some(input),
        )
        .await
    }

    pub async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/worktrees", Option::<&()>::None)
            .await
    }

    pub async fn get_worktree(
        &self,
        worktree_id: &str,
    ) -> Result<ManagedWorktreeRecord, RuntimeClientError> {
        self.request_json(
            Method::GET,
            &format!("/v1/worktrees/{worktree_id}"),
            Option::<&()>::None,
        )
        .await
    }

    pub async fn create_worktree(
        &self,
        input: &WorktreeCreateInput,
    ) -> Result<WorktreeCreateResponse, RuntimeClientError> {
        self.request_json(Method::POST, "/v1/worktrees", Some(input))
            .await
    }

    pub async fn claim_worktree(
        &self,
        worktree_id: &str,
        input: &WorktreeClaimInput,
    ) -> Result<WorktreeClaimResponse, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/worktrees/{worktree_id}/claims"),
            Some(input),
        )
        .await
    }

    pub async fn release_worktree(
        &self,
        worktree_id: &str,
        input: &WorktreeReleaseInput,
    ) -> Result<WorktreeReleaseResponse, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/worktrees/{worktree_id}/release"),
            Some(input),
        )
        .await
    }

    pub async fn cleanup_worktree(
        &self,
        worktree_id: &str,
        input: &WorktreeCleanupInput,
    ) -> Result<WorktreeCleanupResponse, RuntimeClientError> {
        self.request_json(
            Method::POST,
            &format!("/v1/worktrees/{worktree_id}/cleanup"),
            Some(input),
        )
        .await
    }

    pub async fn diagnostics(&self) -> Result<RuntimeDiagnosticsResponse, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/diagnostics", Option::<&()>::None)
            .await
    }

    pub async fn provider_diagnostics(&self) -> Result<Value, RuntimeClientError> {
        self.request_json(
            Method::GET,
            "/v1/diagnostics/providers",
            Option::<&()>::None,
        )
        .await
    }

    pub async fn comms_diagnostics(&self) -> Result<Value, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/diagnostics/comms", Option::<&()>::None)
            .await
    }

    pub async fn process_diagnostics(&self) -> Result<Value, RuntimeClientError> {
        self.request_json(
            Method::GET,
            "/v1/diagnostics/processes",
            Option::<&()>::None,
        )
        .await
    }

    pub async fn worktree_diagnostics(&self) -> Result<Value, RuntimeClientError> {
        self.request_json(
            Method::GET,
            "/v1/diagnostics/worktrees",
            Option::<&()>::None,
        )
        .await
    }

    pub async fn recovery_diagnostics(&self) -> Result<Value, RuntimeClientError> {
        self.request_json(Method::GET, "/v1/diagnostics/recovery", Option::<&()>::None)
            .await
    }

    pub async fn replay_global_events(
        &self,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeClientError> {
        self.replay_events("/v1/events", after_seq, limit).await
    }

    pub async fn replay_session_events(
        &self,
        session_id: &str,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeClientError> {
        self.replay_events(
            &format!("/v1/sessions/{session_id}/events"),
            after_seq,
            limit,
        )
        .await
    }

    pub async fn replay_team_events(
        &self,
        team_id: &str,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeClientError> {
        self.replay_events(&format!("/v1/teams/{team_id}/events"), after_seq, limit)
            .await
    }

    pub async fn replay_process_events(
        &self,
        process_id: &str,
        session_id: Option<&str>,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeClientError> {
        let path = query_path(
            &format!("/v1/processes/{process_id}/events"),
            [
                ("session_id", session_id.map(ToOwned::to_owned)),
                ("after_seq", after_seq.map(|value| value.to_string())),
                ("limit", limit.map(|value| value.to_string())),
            ],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    async fn replay_events(
        &self,
        path: &str,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeClientError> {
        let path = query_path(
            path,
            [
                ("after_seq", after_seq.map(|value| value.to_string())),
                ("limit", limit.map(|value| value.to_string())),
            ],
        );
        self.request_json(Method::GET, &path, Option::<&()>::None)
            .await
    }

    async fn request_empty<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&T>,
    ) -> Result<(), RuntimeClientError> {
        let response = self.send_request(method, path, body).await?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(self.error_from_response(response).await)
    }

    async fn request_json<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&T>,
    ) -> Result<R, RuntimeClientError> {
        let response = self.send_request(method, path, body).await?;
        if response.status().is_success() {
            return response
                .json::<R>()
                .await
                .map_err(RuntimeClientError::Decode);
        }
        Err(self.error_from_response(response).await)
    }

    async fn send_request<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&T>,
    ) -> Result<reqwest::Response, RuntimeClientError> {
        let mut request = self.http.request(method, self.endpoint(path));
        if let Some(token) = self.config.bearer_token.as_ref() {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        request.send().await.map_err(RuntimeClientError::Transport)
    }

    async fn error_from_response(&self, response: reqwest::Response) -> RuntimeClientError {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        RuntimeClientError::Http { status, body }
    }
}

fn query_path<const N: usize>(path: &str, params: [(&str, Option<String>); N]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        if let Some(value) = value {
            serializer.append_pair(key, value.as_str());
        }
    }
    let query = serializer.finish();
    if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    }
}
