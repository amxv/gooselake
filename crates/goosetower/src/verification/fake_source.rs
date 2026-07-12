use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{extract::Request, Json, Router};
use futures_util::stream;
use runtime_core::{RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

pub const SEED_VERSION: &str = "p02-fake-gooselake/v1";
pub const FIXED_CLOCK_MS: i64 = 1_700_100_000_000;
pub const SOURCE_ID: &str = "p02-source";
pub const INITIAL_EPOCH: &str = "p02-epoch-001";
const CONTROL_HEADER: &str = "x-gooseweb-verification-control";
const CONTROL_SECRET: &str = "p02-local-control";
pub const RUNTIME_BEARER: &str = "p02-runtime-token";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultControl {
    EmitNext,
    EmitBatch { count: usize },
    DelayNext { milliseconds: u64 },
    DuplicateNext,
    GapNext,
    ChangeEpoch,
    DisconnectNext,
    Offline(bool),
    DelayTerminal { milliseconds: u64 },
    RejectNextCommand { code: String },
    Reset,
}

#[derive(Debug, Clone, Serialize)]
pub struct FakeSourceObserver {
    pub seed_version: &'static str,
    pub fixed_clock_ms: i64,
    pub source_id: &'static str,
    pub source_epoch: String,
    pub next_source_seq: i64,
    pub records: Vec<Value>,
    pub events: Vec<RuntimeEventRecord>,
    pub pending_faults: Vec<FaultControl>,
    pub offline: bool,
}

#[derive(Debug, Clone)]
struct FakeState {
    epoch_number: u32,
    next_seq: i64,
    records: Vec<Value>,
    events: Vec<RuntimeEventRecord>,
    live: VecDeque<RuntimeEventRecord>,
    faults: VecDeque<FaultControl>,
    offline: bool,
}

impl Default for FakeState {
    fn default() -> Self {
        Self {
            epoch_number: 1,
            next_seq: 4,
            records: seed_records(),
            events: seed_events(),
            live: VecDeque::new(),
            faults: VecDeque::new(),
            offline: false,
        }
    }
}

impl FakeState {
    fn epoch(&self) -> String {
        format!("p02-epoch-{:03}", self.epoch_number)
    }

    fn create_event(&mut self, kind: &str, payload: Value) -> RuntimeEventRecord {
        let row_id = self.next_seq;
        self.next_seq += 1;
        RuntimeEventRecord {
            row_id,
            event_id: format!("p02-event-{row_id:04}"),
            scope: RuntimeEventScope::Session,
            scope_id: "p02-session-001".into(),
            session_id: Some("p02-session-001".into()),
            team_id: None,
            turn_id: Some("p02-turn-001".into()),
            seq: row_id,
            kind: kind.into(),
            criticality: RuntimeEventCriticality::Critical,
            payload,
            provider: Some("codex".into()),
            provider_seq: Some(row_id),
            created_at: FIXED_CLOCK_MS + row_id * 10,
        }
    }

    fn emit_one(&mut self) {
        let mut event = self.create_event(
            "turn.completed",
            json!({"text":"P02 deterministic terminal","unknown_public_extension":{"kept":true}}),
        );
        if matches!(self.faults.front(), Some(FaultControl::GapNext)) {
            self.faults.pop_front();
            self.next_seq += 1;
            event = self.create_event("turn.completed", event.payload);
        }
        self.events.push(event.clone());
        self.live.push_back(event.clone());
        if matches!(self.faults.front(), Some(FaultControl::DuplicateNext)) {
            self.faults.pop_front();
            self.live.push_back(event);
        }
    }

    fn observer(&self) -> FakeSourceObserver {
        FakeSourceObserver {
            seed_version: SEED_VERSION,
            fixed_clock_ms: FIXED_CLOCK_MS,
            source_id: SOURCE_ID,
            source_epoch: self.epoch(),
            next_source_seq: self.next_seq,
            records: self.records.iter().map(redact_json).collect(),
            events: self.events.iter().cloned().map(redact_event).collect(),
            pending_faults: self.faults.iter().cloned().collect(),
            offline: self.offline,
        }
    }
}

#[derive(Clone, Default)]
pub struct FakeGooselakeSource {
    state: Arc<Mutex<FakeState>>,
}

impl FakeGooselakeSource {
    pub fn router(&self) -> Router {
        let public_contract = Router::new()
            .route("/v1/health", get(health))
            .route("/v1/version", get(version))
            .route("/v1/providers", get(providers))
            .route("/v1/sessions", get(list_sessions).post(create_session))
            .route("/v1/sessions/{id}", get(get_session))
            .route("/v1/sessions/{id}/turns", post(send_turn))
            .route("/v1/sessions/{id}/turns/{turn}/interrupt", post(empty_ok))
            .route("/v1/teams", get(list_teams))
            .route("/v1/teams/{id}", get(get_team))
            .route("/v1/teams/{id}/view", get(team_view))
            .route("/v1/teams/{id}/broadcast", post(team_action))
            .route("/v1/teams/{id}/direct", post(team_action))
            .route("/v1/teams/{id}/deliveries", get(team_deliveries))
            .route("/v1/processes", get(list_processes))
            .route("/v1/processes/{id}", get(get_process))
            .route("/v1/processes/{id}/logs", get(process_logs))
            .route("/v1/worktrees", get(empty_array))
            .route("/v1/diagnostics", get(diagnostics))
            .route("/v1/events", get(replay_global))
            .route("/v1/events/stream", get(event_stream))
            .route("/v1/sessions/{id}/events", get(replay_scoped))
            .route("/v1/teams/{id}/events", get(replay_scoped))
            .route("/v1/processes/{id}/events", get(replay_scoped))
            .route_layer(middleware::from_fn(require_runtime_auth));
        let verification_controls = Router::new()
            .route("/__verification/v1/control", post(control))
            .route("/__verification/v1/observer", get(observer));
        Router::new()
            .route("/health", get(health))
            .merge(public_contract)
            .merge(verification_controls)
            .with_state(self.state.clone())
    }

    pub async fn observer(&self) -> FakeSourceObserver {
        self.state.lock().await.observer()
    }
}

type Shared = Arc<Mutex<FakeState>>;

async fn require_runtime_auth(request: Request, next: Next) -> Response {
    let authorized = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        == Some(format!("Bearer {RUNTIME_BEARER}").as_str());
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":{"code":"unauthorized","message":"runtime bearer required"}})),
        )
            .into_response();
    }
    next.run(request).await
}

async fn health() -> Json<Value> {
    Json(json!({"status":"ok","providers":0,"public_base_url":null}))
}

async fn version() -> Json<Value> {
    Json(json!({"version":SEED_VERSION}))
}

async fn providers() -> Json<Value> {
    Json(json!({"providers":[]}))
}

async fn list_sessions(State(state): State<Shared>) -> Json<Value> {
    Json(json!([state.lock().await.records[0].clone()]))
}

async fn get_session(Path(_): Path<String>, State(state): State<Shared>) -> Json<Value> {
    Json(state.lock().await.records[0].clone())
}

async fn create_session(State(state): State<Shared>) -> impl IntoResponse {
    command_response(state, state_session()).await
}

async fn send_turn(State(state): State<Shared>) -> impl IntoResponse {
    command_response(
        state,
        json!({"session_id":"p02-session-001","turn_id":"p02-turn-001","status":"accepted","accepted_at":FIXED_CLOCK_MS}),
    )
    .await
}

async fn list_teams(State(state): State<Shared>) -> Json<Value> {
    Json(json!([state.lock().await.records[1].clone()]))
}

async fn get_team(Path(_): Path<String>, State(state): State<Shared>) -> Json<Value> {
    Json(state.lock().await.records[1].clone())
}

async fn team_view(Path(_): Path<String>, State(state): State<Shared>) -> Json<Value> {
    let records = state.lock().await.records.clone();
    Json(json!({
        "team": records[1], "messages": [records[2]],
        "deliveries_by_message_id": {"p02-message-001":[records[3]]},
        "next_message_cursor": null, "snapshot_at": FIXED_CLOCK_MS
    }))
}

async fn team_action(State(state): State<Shared>) -> impl IntoResponse {
    command_response(
        state,
        json!({"message_id":"p02-message-001","delivery_ids":["p02-delivery-001"]}),
    )
    .await
}

async fn team_deliveries(State(state): State<Shared>) -> Json<Value> {
    Json(json!([state.lock().await.records[3].clone()]))
}

async fn list_processes(State(state): State<Shared>) -> Json<Value> {
    Json(json!([state.lock().await.records[4].clone()]))
}

async fn get_process(State(state): State<Shared>) -> Json<Value> {
    let process = state.lock().await.records[4].clone();
    Json(
        json!({"process":process,"exit_code":0,"signal":null,"timeout_ms":1000,"stdout_path":null,"stderr_path":null,"stdout_bytes":21,"stderr_bytes":0,"stdout_truncated":false,"stderr_truncated":false}),
    )
}

async fn process_logs() -> Json<Value> {
    Json(
        json!([{"process_id":"p02-process-001","stream":"stdout","content":"P02 deterministic log\n","head_lines":1,"tail_lines":1,"truncated":false,"bytes":22}]),
    )
}

async fn diagnostics(State(state): State<Shared>) -> Json<Value> {
    let state = state.lock().await;
    Json(
        json!({"providers":{},"comms":{"messages":1},"processes":{"count":1},"worktrees":{},"recovery":{"epoch":state.epoch()}}),
    )
}

async fn empty_array() -> Json<Value> {
    Json(json!([]))
}
async fn empty_ok() -> StatusCode {
    StatusCode::NO_CONTENT
}

#[derive(Deserialize)]
struct ReplayQuery {
    after_seq: Option<i64>,
    limit: Option<usize>,
}

async fn replay_global(
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Json<Value> {
    Json(replay(&state.lock().await.events, query))
}

async fn replay_scoped(
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Json<Value> {
    Json(replay(&state.lock().await.events, query))
}

fn replay(events: &[RuntimeEventRecord], query: ReplayQuery) -> Value {
    let after = query.after_seq.unwrap_or(0);
    let limit = query.limit.unwrap_or(2000).clamp(1, 2000);
    json!(events
        .iter()
        .filter(|event| event.row_id > after)
        .take(limit)
        .collect::<Vec<_>>())
}

async fn event_stream(
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> impl IntoResponse {
    let mut state = state.lock().await;
    if state.offline {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if matches!(state.faults.front(), Some(FaultControl::DisconnectNext)) {
        state.faults.pop_front();
        return Sse::new(stream::iter(
            Vec::<Result<Event, std::convert::Infallible>>::new(),
        ))
        .into_response();
    }
    let delay = match state.faults.front() {
        Some(FaultControl::DelayNext { milliseconds })
        | Some(FaultControl::DelayTerminal { milliseconds }) => {
            let delay = *milliseconds;
            state.faults.pop_front();
            delay
        }
        _ => 0,
    };
    let after = query.after_seq.unwrap_or(0);
    let mut events = state
        .events
        .iter()
        .filter(|event| event.row_id > after)
        .cloned()
        .collect::<Vec<_>>();
    events.extend(state.live.drain(..));
    events.sort_by_key(|event| event.row_id);
    let limit = query.limit.unwrap_or(2000).clamp(1, 2000);
    events.truncate(limit);
    drop(state);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay.min(5_000))).await;
    }
    let frames = events.into_iter().map(|event| {
        Ok::<_, std::convert::Infallible>(
            Event::default()
                .id(event.row_id.to_string())
                .event("runtime")
                .json_data(event)
                .expect("serializable event"),
        )
    });
    Sse::new(stream::iter(frames)).into_response()
}

async fn control(
    headers: HeaderMap,
    State(state): State<Shared>,
    Json(control): Json<FaultControl>,
) -> impl IntoResponse {
    if headers
        .get(CONTROL_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(CONTROL_SECRET)
    {
        return (StatusCode::NOT_FOUND, Json(json!({"error":"not_found"})));
    }
    let mut state = state.lock().await;
    match control.clone() {
        FaultControl::EmitNext => state.emit_one(),
        FaultControl::EmitBatch { count } => {
            for _ in 0..count.min(32) {
                state.emit_one();
            }
        }
        FaultControl::ChangeEpoch => {
            state.epoch_number += 1;
            state.next_seq = 1;
            state.events.clear();
            state.live.clear();
        }
        FaultControl::Offline(value) => state.offline = value,
        FaultControl::Reset => *state = FakeState::default(),
        fault => state.faults.push_back(fault),
    }
    (
        StatusCode::OK,
        Json(json!({"ok":true,"epoch":state.epoch(),"next_source_seq":state.next_seq})),
    )
}

async fn observer(headers: HeaderMap, State(state): State<Shared>) -> impl IntoResponse {
    if headers
        .get(CONTROL_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(CONTROL_SECRET)
    {
        return (StatusCode::NOT_FOUND, Json(json!({"error":"not_found"}))).into_response();
    }
    Json(state.lock().await.observer()).into_response()
}

async fn command_response(state: Shared, success: Value) -> (StatusCode, Json<Value>) {
    let mut state = state.lock().await;
    if let Some(index) = state
        .faults
        .iter()
        .position(|fault| matches!(fault, FaultControl::RejectNextCommand { .. }))
    {
        if let Some(FaultControl::RejectNextCommand { code }) = state.faults.remove(index) {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error":{"code":code,"message":"deterministic rejection"}})),
            );
        }
    }
    (StatusCode::OK, Json(success))
}

fn state_session() -> Value {
    json!({"id":"p02-session-001","provider":"codex","status":"ready","cwd":"/p02/workspace","model":"gpt-5","permission_mode":"default","system_prompt":null,"metadata":{"seed_version":SEED_VERSION},"provider_session_ref":"provider-session-fixed-001","canonical_provider_session_ref":"provider-session-fixed-001","active_turn_id":null,"worktree_id":null,"created_at":FIXED_CLOCK_MS,"updated_at":FIXED_CLOCK_MS,"closed_at":null,"failure_code":null,"failure_message":null})
}

fn seed_records() -> Vec<Value> {
    vec![
        state_session(),
        json!({"team":{"id":"p02-team-001","name":"P02 deterministic team","lead_agent_id":"p02-session-001","created_by":"verification","created_at":FIXED_CLOCK_MS,"updated_at":FIXED_CLOCK_MS,"deleted_at":null},"members":[{"team_id":"p02-team-001","agent_id":"p02-session-001","title":"Lead","joined_at":FIXED_CLOCK_MS,"added_by":"verification","creator_agent_id":null,"creator_compaction_subscription":"auto","worktree_id":null}]}),
        json!({"id":"p02-message-001","team_id":"p02-team-001","scope":"broadcast","sender_agent_id":"p02-session-001","recipient_agent_ids":["p02-session-001"],"input":[{"type":"text","text":"P02 deterministic team action"}],"image_paths":[],"priority":"normal","policy":"non_interrupting","correlation_id":"p02-correlation-001","reply_to_message_id":null,"idempotency_key":"p02-idempotency-001","created_at":FIXED_CLOCK_MS+20}),
        json!({"id":"p02-delivery-001","message_id":"p02-message-001","team_id":"p02-team-001","recipient_agent_id":"p02-session-001","provider":"codex","status":"injected","effective_policy":"non_interrupting","injection_strategy":"turn","injected_turn_id":"p02-turn-001","last_error_code":null,"last_error_message":null,"created_at":FIXED_CLOCK_MS+20,"updated_at":FIXED_CLOCK_MS+30}),
        json!({"process_id":"p02-process-001","session_id":"p02-session-001","pid":4242,"status":"completed","command":{"argv":["printf","P02 deterministic log"]},"cwd":"/p02/workspace","started_at":FIXED_CLOCK_MS+40,"ended_at":FIXED_CLOCK_MS+50}),
    ]
}

fn seed_events() -> Vec<RuntimeEventRecord> {
    [
        (1, "session.created", RuntimeEventScope::Session),
        (2, "team.message.created", RuntimeEventScope::Team),
        (3, "process.completed", RuntimeEventScope::Process),
    ]
    .into_iter()
    .map(|(row_id, kind, scope)| RuntimeEventRecord {
        row_id,
        event_id: format!("p02-event-{row_id:04}"),
        scope,
        scope_id: match scope {
            RuntimeEventScope::Team => "p02-team-001",
            RuntimeEventScope::Process => "p02-process-001",
            _ => "p02-session-001",
        }
        .into(),
        session_id: Some("p02-session-001".into()),
        team_id: (scope == RuntimeEventScope::Team).then(|| "p02-team-001".into()),
        turn_id: None,
        seq: row_id,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({"seed_version":SEED_VERSION,"unknown_public_extension":{"kept":true}}),
        provider: Some("codex".into()),
        provider_seq: Some(row_id),
        created_at: FIXED_CLOCK_MS + row_id * 10,
    })
    .collect()
}

fn redact_event(mut event: RuntimeEventRecord) -> RuntimeEventRecord {
    event.payload = redact_json(&event.payload);
    event
}

fn redact_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let normalized = key.to_ascii_lowercase();
                    let redacted = [
                        "authorization",
                        "bearer",
                        "token",
                        "ticket",
                        "password",
                        "credential",
                        "cookie",
                        "csrf",
                        "secret",
                        "raw_image",
                        "image_data",
                    ]
                    .iter()
                    .any(|needle| normalized.contains(needle));
                    (
                        key.clone(),
                        if redacted {
                            Value::String("[redacted]".into())
                        } else {
                            redact_json(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact_json).collect()),
        Value::String(text) if text.to_ascii_lowercase().starts_with("bearer ") => {
            Value::String("[redacted]".into())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::*;
    use crate::config::{GoosetowerConfig, RuntimeSourceConfig};
    use crate::gateway::GatewayState;
    use crate::materializer::{BootstrapOptions, SourceBootstrap};
    use crate::runtime::{
        GooselakeRuntimeClient, GooselakeRuntimeClientConfig, RuntimeSseFanIn,
        RuntimeSseFanInConfig, SourceHealthState,
    };

    async fn spawn() -> (FakeGooselakeSource, String) {
        let source = FakeGooselakeSource::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = source.router();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (source, format!("http://{address}"))
    }

    fn client(base: String) -> GooselakeRuntimeClient {
        GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
            SOURCE_ID,
            INITIAL_EPOCH,
            base,
            Some(RUNTIME_BEARER.into()),
        ))
        .unwrap()
    }

    async fn apply_control(base: &str, control: FaultControl) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{base}/__verification/v1/control"))
            .header(CONTROL_HEADER, CONTROL_SECRET)
            .json(&control)
            .send()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn fake_source_public_contract_bootstraps_real_materializer() {
        let (_source, base) = spawn().await;
        let client = client(base);
        assert_eq!(client.version().await.unwrap().version, SEED_VERSION);
        let bootstrap = SourceBootstrap::from_runtime_client(
            &client,
            BootstrapOptions {
                replay_cursor_limit: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(bootstrap.state.sessions.len(), 1);
        assert_eq!(bootstrap.state.teams.len(), 1);
        assert_eq!(bootstrap.state.messages_by_team["p02-team-001"].len(), 1);
        assert_eq!(bootstrap.state.processes.len(), 1);
        assert_eq!(bootstrap.state.source_health.last_source_seq, Some(3));
    }

    #[tokio::test]
    async fn fake_source_bootstraps_real_gateway_materialized_observer() {
        let (_source, base) = spawn().await;
        let mut config = GoosetowerConfig::default();
        config.runtimes.sources = vec![RuntimeSourceConfig {
            source_id: SOURCE_ID.into(),
            source_epoch: INITIAL_EPOCH.into(),
            base_url: base,
            bearer_token: Some(RUNTIME_BEARER.into()),
            display_name: "P02 deterministic source".into(),
            ..Default::default()
        }];
        let gateway = GatewayState::new(Arc::new(config)).unwrap();
        gateway.bootstrap_enabled_sources().await;
        let observer = gateway.debug_materializer_summary().await;
        assert_eq!(observer.len(), 1);
        assert_eq!(observer[0].source_id, SOURCE_ID);
        assert_eq!(observer[0].source_epoch, INITIAL_EPOCH);
        assert_eq!(observer[0].sessions, 1);
        assert_eq!(observer[0].teams, 1);
        assert_eq!(observer[0].processes, 1);
        assert_eq!(observer[0].source_health.last_source_seq, Some(1));
    }

    #[tokio::test]
    async fn replay_paginates_and_sse_handoff_dedupes_overlap() {
        let (_source, base) = spawn().await;
        let client = client(base.clone());
        assert_eq!(
            client
                .replay_global_events(None, Some(2))
                .await
                .unwrap()
                .iter()
                .map(|event| event.row_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            client.replay_global_events(Some(2), Some(2)).await.unwrap()[0].row_id,
            3
        );
        apply_control(&base, FaultControl::DuplicateNext).await;
        apply_control(&base, FaultControl::EmitNext).await;
        let fan_in = RuntimeSseFanIn::new(
            client,
            RuntimeSseFanInConfig {
                stale_after: Duration::from_millis(50),
                ..Default::default()
            },
        );
        let (tx, mut rx) = mpsc::channel(16);
        let mut seen = HashSet::new();
        let cursor = fan_in.consume_once(Some(3), &tx, &mut seen).await.unwrap();
        assert_eq!(cursor, Some(4));
        drop(tx);
        let mut ids = Vec::new();
        while let Some(event) = rx.recv().await {
            ids.push(event.source_seq);
        }
        assert_eq!(ids, vec![4]);
        assert_eq!(fan_in.health().state, SourceHealthState::Live);
    }

    #[tokio::test]
    async fn gap_fault_localizes_existing_live_to_gap_transition_baseline() {
        let (_source, base) = spawn().await;
        apply_control(&base, FaultControl::GapNext).await;
        apply_control(&base, FaultControl::EmitNext).await;
        let fan_in = RuntimeSseFanIn::new(
            client(base),
            RuntimeSseFanInConfig {
                stale_after: Duration::from_millis(50),
                ..Default::default()
            },
        );
        let (tx, _rx) = mpsc::channel(16);
        let task = tokio::spawn(async move {
            let mut seen = HashSet::new();
            fan_in.consume_once(Some(3), &tx, &mut seen).await
        });
        let failure = task
            .await
            .expect_err("P06 baseline must remain detected in P02");
        assert!(failure.is_panic());
        let panic = failure.into_panic();
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(message.contains("invalid source lifecycle transition Live -> GapDetected"));
    }

    #[tokio::test]
    async fn faults_epoch_offline_rejection_delay_and_redaction_are_bounded() {
        let (source, base) = spawn().await;
        assert_eq!(
            apply_control(&base, FaultControl::EmitBatch { count: 100 })
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(source.observer().await.events.len(), 35);
        apply_control(
            &base,
            FaultControl::RejectNextCommand {
                code: "p02_rejected".into(),
            },
        )
        .await;
        let error = client(base.clone())
            .create_session(&runtime_core::CreateSessionInput {
                provider: runtime_core::ProviderKind::Codex,
                cwd: None,
                model: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("409"));
        apply_control(&base, FaultControl::DelayNext { milliseconds: 2 }).await;
        let started = std::time::Instant::now();
        client(base.clone())
            .http()
            .get(format!("{base}/v1/events/stream?after_seq=35"))
            .bearer_auth(RUNTIME_BEARER)
            .send()
            .await
            .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(2));
        apply_control(&base, FaultControl::DelayTerminal { milliseconds: 2 }).await;
        let response = client(base.clone())
            .http()
            .get(format!("{base}/v1/events/stream?after_seq=35"))
            .bearer_auth(RUNTIME_BEARER)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        apply_control(&base, FaultControl::DisconnectNext).await;
        let disconnected = client(base.clone())
            .http()
            .get(format!("{base}/v1/events/stream?after_seq=35"))
            .bearer_auth(RUNTIME_BEARER)
            .send()
            .await
            .unwrap();
        assert!(disconnected.bytes().await.unwrap().is_empty());
        apply_control(&base, FaultControl::ChangeEpoch).await;
        assert_eq!(source.observer().await.source_epoch, "p02-epoch-002");
        apply_control(&base, FaultControl::Offline(true)).await;
        let response = reqwest::Client::new()
            .get(format!("{base}/v1/events/stream"))
            .bearer_auth(RUNTIME_BEARER)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        apply_control(&base, FaultControl::Reset).await;
        assert_eq!(source.observer().await.source_epoch, INITIAL_EPOCH);
        assert!(!source.observer().await.offline);
        let secret = json!({"authorization":"Bearer should-not-leak","nested":{"ticket_secret":"bad"},"safe":"kept"});
        assert_eq!(
            redact_json(&secret),
            json!({"authorization":"[redacted]","nested":{"ticket_secret":"[redacted]"},"safe":"kept"})
        );
    }

    #[tokio::test]
    async fn controls_are_not_reachable_without_verification_secret() {
        let (_source, base) = spawn().await;
        let response = reqwest::Client::new()
            .post(format!("{base}/__verification/v1/control"))
            .json(&FaultControl::EmitNext)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let unauthorized = reqwest::Client::new()
            .get(format!("{base}/v1/sessions"))
            .send()
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let public_health = reqwest::Client::new()
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(public_health.status(), StatusCode::OK);
    }
}
