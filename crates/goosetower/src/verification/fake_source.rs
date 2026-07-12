use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{extract::Request, Json, Router};
use futures_util::{stream, StreamExt};
use runtime_core::{RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex};

pub const SEED_VERSION: &str = "p02-fake-gooselake/v1";
pub const FIXED_CLOCK_MS: i64 = 1_700_100_000_000;
pub const SOURCE_ID: &str = "p02-source";
pub const INITIAL_EPOCH: &str = "p02-epoch-001";
const CONTROL_HEADER: &str = "x-gooseweb-verification-control";
const CONTROL_SECRET: &str = "p02-local-control";
pub const RUNTIME_BEARER: &str = "p02-runtime-token";
const MAX_RETAINED_EVENTS: usize = 256;

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
    team_command_number: usize,
    records: Vec<Value>,
    events: VecDeque<RuntimeEventRecord>,
    event_delays_ms: BTreeMap<i64, u64>,
    event_tx: broadcast::Sender<(RuntimeEventRecord, u64)>,
    faults: VecDeque<FaultControl>,
    offline: bool,
}

impl Default for FakeState {
    fn default() -> Self {
        let (event_tx, _) = broadcast::channel(MAX_RETAINED_EVENTS);
        Self {
            epoch_number: 1,
            next_seq: 4,
            team_command_number: 0,
            records: seed_records(),
            events: seed_events().into(),
            event_delays_ms: BTreeMap::new(),
            event_tx,
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
        event.seq = self
            .events
            .iter()
            .filter(|item| item.scope == RuntimeEventScope::Session)
            .map(|item| item.seq)
            .max()
            .unwrap_or(0)
            + 1;
        let delay = match self.faults.front() {
            Some(FaultControl::DelayNext { milliseconds })
            | Some(FaultControl::DelayTerminal { milliseconds }) => {
                let delay = *milliseconds;
                self.faults.pop_front();
                Some(delay.min(5_000))
            }
            _ => None,
        };
        if let Some(delay) = delay {
            self.event_delays_ms.insert(event.row_id, delay);
        }
        let duplicate = matches!(self.faults.front(), Some(FaultControl::DuplicateNext));
        if duplicate {
            self.faults.pop_front();
        }
        self.retain_event(event.clone());
        if duplicate {
            self.retain_event(event.clone());
        }
        let event_delay = delay.unwrap_or_default();
        let _ = self.event_tx.send((event.clone(), event_delay));
        if duplicate {
            let _ = self.event_tx.send((event, event_delay));
        }
    }

    fn retain_event(&mut self, event: RuntimeEventRecord) {
        self.events.push_back(event);
        while self.events.len() > MAX_RETAINED_EVENTS {
            if let Some(removed) = self.events.pop_front() {
                self.event_delays_ms.remove(&removed.row_id);
            }
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
            .route("/v1/providers/{provider}/models", get(provider_models))
            .route("/v1/providers/{provider}/auth/status", get(provider_auth))
            .route("/v1/sessions", get(list_sessions).post(create_session))
            .route("/v1/sessions/{id}", get(get_session))
            .route("/v1/sessions/{id}/resume", post(get_session))
            .route("/v1/sessions/{id}/close", post(get_session))
            .route("/v1/sessions/{id}/turns", post(send_turn))
            .route("/v1/sessions/{id}/turns/{turn}/interrupt", post(empty_ok))
            .route("/v1/sessions/{id}/events/stream", get(stream_session))
            .route("/v1/teams", get(list_teams).post(team_static))
            .route("/v1/teams/{id}", get(get_team).delete(empty_ok))
            .route("/v1/teams/{id}/members", post(team_static))
            .route("/v1/teams/{id}/members/spawn", post(spawn_team_member))
            .route("/v1/teams/{id}/members/{agent}", delete(team_static))
            .route("/v1/teams/{id}/lead", post(team_static))
            .route("/v1/teams/{id}/view", get(team_view))
            .route(
                "/v1/teams/{id}/messages",
                get(team_messages).post(team_direct),
            )
            .route("/v1/teams/{id}/broadcasts", post(team_broadcast))
            .route("/v1/teams/{id}/deliveries", get(team_deliveries))
            .route(
                "/v1/teams/{id}/deliveries/{delivery}/retry",
                post(retry_delivery),
            )
            .route(
                "/v1/teams/{id}/messages/{message}/cancel",
                post(cancel_message),
            )
            .route("/v1/teams/{id}/interrupt-all", post(interrupt_all))
            .route("/v1/teams/{id}/events/stream", get(stream_team))
            .route("/v1/processes", get(list_processes).post(get_process))
            .route("/v1/processes/{id}", get(get_process))
            .route("/v1/processes/{id}/logs", get(process_logs))
            .route("/v1/processes/{id}/kill", post(get_process))
            .route("/v1/processes/{id}/events/stream", get(stream_process))
            .route("/v1/worktrees", get(empty_array).post(worktree_create))
            .route("/v1/worktrees/{id}", get(worktree_get))
            .route("/v1/worktrees/{id}/claims", post(worktree_claim))
            .route("/v1/worktrees/{id}/release", post(worktree_release))
            .route("/v1/worktrees/{id}/cleanup", post(worktree_cleanup))
            .route("/v1/diagnostics", get(diagnostics))
            .route("/v1/diagnostics/providers", get(diagnostic_part))
            .route("/v1/diagnostics/comms", get(diagnostic_part))
            .route("/v1/diagnostics/processes", get(diagnostic_part))
            .route("/v1/diagnostics/worktrees", get(diagnostic_part))
            .route("/v1/diagnostics/recovery", get(diagnostic_part))
            .route("/v1/diagnostics/team-operations", get(empty_array))
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
    Json(json!({"providers":[{"kind":"codex","display_name":"Codex","enabled":true}]}))
}

async fn provider_models(Path(provider): Path<String>) -> Json<Value> {
    Json(
        json!({"provider":provider,"models":[{"id":"gpt-5","display_name":"GPT-5","reasoning_levels":["medium"]}]}),
    )
}

async fn provider_auth() -> Json<Value> {
    Json(json!({"authenticated":true,"mode":"verification","detail":null}))
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
    let mut state = state.lock().await;
    if let Some(error) = take_rejection(&mut state) {
        return error;
    }
    state.records.push(json!({"id":"p02-turn-001","session_id":"p02-session-001","provider_turn_ref":"provider-turn-fixed-001","status":"completed","input":[{"type":"text","text":"P02 deterministic turn action"}],"source":"operator","started_at":FIXED_CLOCK_MS+60,"completed_at":FIXED_CLOCK_MS+80,"usage":{"assistant_text":"P02 deterministic terminal"},"error":null}));
    state.records.truncate(64);
    state.emit_one();
    (
        StatusCode::ACCEPTED,
        Json(
            json!({"session_id":"p02-session-001","turn_id":"p02-turn-001","status":"accepted","accepted_at":FIXED_CLOCK_MS}),
        ),
    )
}

async fn list_teams(State(state): State<Shared>) -> Json<Value> {
    Json(json!([state.lock().await.records[1].clone()]))
}

async fn get_team(Path(_): Path<String>, State(state): State<Shared>) -> Json<Value> {
    Json(state.lock().await.records[1].clone())
}

async fn team_static(State(state): State<Shared>) -> Json<Value> {
    Json(state.lock().await.records[1].clone())
}

async fn team_view(Path(_): Path<String>, State(state): State<Shared>) -> Json<Value> {
    let records = state.lock().await.records.clone();
    let messages = message_records(&records);
    let delivery_map = delivery_records(&records).into_iter().fold(
        BTreeMap::<String, Vec<Value>>::new(),
        |mut map, delivery| {
            if let Some(id) = delivery.get("message_id").and_then(Value::as_str) {
                map.entry(id.into()).or_default().push(delivery);
            }
            map
        },
    );
    Json(json!({
        "team": records[1], "messages": messages,
        "deliveries_by_message_id": delivery_map,
        "next_message_cursor": null, "snapshot_at": FIXED_CLOCK_MS
    }))
}

async fn team_direct(State(state): State<Shared>) -> impl IntoResponse {
    team_action(state, "direct").await
}

async fn team_broadcast(State(state): State<Shared>) -> impl IntoResponse {
    team_action(state, "broadcast").await
}

async fn team_action(state: Shared, scope: &'static str) -> impl IntoResponse {
    let mut state = state.lock().await;
    if let Some(error) = take_rejection(&mut state) {
        return error;
    }
    state.team_command_number += 1;
    let number = state.team_command_number;
    let message_id = format!("p02-message-command-{number:03}");
    let delivery_id = format!("p02-delivery-command-{number:03}");
    let message = json!({"id":message_id,"team_id":"p02-team-001","scope":scope,"sender_agent_id":"p02-session-001","recipient_agent_ids":["p02-session-001"],"input":[{"type":"text","text":format!("P02 deterministic {scope} action")}],"image_paths":[],"priority":"normal","policy":"non_interrupting","correlation_id":format!("p02-command-correlation-{number:03}"),"reply_to_message_id":null,"idempotency_key":format!("p02-command-key-{number:03}"),"created_at":FIXED_CLOCK_MS+100+number as i64});
    let delivery = json!({"id":delivery_id,"message_id":message_id,"team_id":"p02-team-001","recipient_agent_id":"p02-session-001","provider":"codex","status":"injected","effective_policy":"non_interrupting","injection_strategy":"turn","injected_turn_id":"p02-turn-001","last_error_code":null,"last_error_message":null,"created_at":FIXED_CLOCK_MS+100+number as i64,"updated_at":FIXED_CLOCK_MS+110+number as i64});
    state.records.push(message.clone());
    state.records.push(delivery.clone());
    while state.records.len() > 64 {
        state.records.remove(5);
    }
    let event = state.create_event(
        "team_message.created",
        json!({"message":message,"deliveries":[delivery]}),
    );
    let mut event = RuntimeEventRecord {
        scope: RuntimeEventScope::Team,
        scope_id: "p02-team-001".into(),
        team_id: Some("p02-team-001".into()),
        session_id: None,
        turn_id: None,
        seq: state
            .events
            .iter()
            .filter(|event| event.scope == RuntimeEventScope::Team)
            .map(|event| event.seq)
            .max()
            .unwrap_or(0)
            + 1,
        ..event
    };
    event.event_id = format!("p02-team-event-{:04}", event.row_id);
    state.retain_event(event.clone());
    let _ = state.event_tx.send((event, 0));
    (
        StatusCode::OK,
        Json(json!({"message":message,"deliveries":[delivery],"disposition":"created"})),
    )
}

async fn team_messages(State(state): State<Shared>) -> Json<Value> {
    Json(json!({"messages":message_records(&state.lock().await.records),"next_cursor":null}))
}

async fn retry_delivery(State(state): State<Shared>) -> Json<Value> {
    Json(state.lock().await.records[3].clone())
}

async fn cancel_message(State(state): State<Shared>) -> Json<Value> {
    Json(json!([state.lock().await.records[3].clone()]))
}

async fn interrupt_all() -> Json<Value> {
    Json(
        json!({"team_id":"p02-team-001","interrupted_session_ids":[],"skipped_session_ids":["p02-session-001"]}),
    )
}

async fn spawn_team_member(State(state): State<Shared>) -> Json<Value> {
    let records = state.lock().await.records.clone();
    Json(
        json!({"operation_id":"p02-operation-001","team":records[1],"spawned_session":records[0],"spawned_member":records[1]["members"][0],"worktree":null,"worktree_assignment_mode":"none","worktree_created_by_operation":false,"onboarding":{},"journal_stage":"completed"}),
    )
}

async fn team_deliveries(State(state): State<Shared>) -> Json<Value> {
    Json(json!(delivery_records(&state.lock().await.records)))
}

fn message_records(records: &[Value]) -> Vec<Value> {
    records
        .iter()
        .filter(|record| record.get("team_id").is_some() && record.get("scope").is_some())
        .cloned()
        .collect()
}

fn delivery_records(records: &[Value]) -> Vec<Value> {
    records
        .iter()
        .filter(|record| {
            record.get("recipient_agent_id").is_some() && record.get("message_id").is_some()
        })
        .cloned()
        .collect()
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

async fn diagnostic_part(State(state): State<Shared>) -> Json<Value> {
    Json(json!({"seed_version":SEED_VERSION,"source_epoch":state.lock().await.epoch()}))
}

fn worktree_record() -> Value {
    json!({"id":"p02-worktree-001","repo_root":"/p02/repo","worktree_root":"/p02/worktrees","worktree_cwd":"/p02/worktrees/p02","branch_name":"verification/p02","worktree_name":"p02","unified_workspace_path":"/p02/worktrees/p02","deletion_policy":"retain","created_by_session_id":"p02-session-001","created_by_operation_id":"p02-operation-001","created_at":FIXED_CLOCK_MS,"updated_at":FIXED_CLOCK_MS})
}

async fn worktree_get() -> Json<Value> {
    Json(worktree_record())
}
async fn worktree_create() -> Json<Value> {
    Json(json!({"worktree":worktree_record(),"created":true,"init_script_status":"not_requested"}))
}
async fn worktree_claim() -> Json<Value> {
    Json(
        json!({"worktree":worktree_record(),"claim":{"worktree_id":"p02-worktree-001","session_id":"p02-session-001","claim_role":"team_member","created_at":FIXED_CLOCK_MS,"released_at":null}}),
    )
}
async fn worktree_release() -> Json<Value> {
    Json(
        json!({"worktree":worktree_record(),"released_claim":{"worktree_id":"p02-worktree-001","session_id":"p02-session-001","claim_role":"team_member","created_at":FIXED_CLOCK_MS,"released_at":FIXED_CLOCK_MS+90},"active_claim_count":0,"cleanup":null}),
    )
}
async fn worktree_cleanup() -> Json<Value> {
    Json(
        json!({"worktree_id":"p02-worktree-001","status":"retained","deletion_policy":"retain","active_claim_count":0,"worktree_path_deleted":false,"branch_deleted":false,"diagnostics":[]}),
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
    headers: HeaderMap,
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Json<Value> {
    Json(replay(
        &state.lock().await.events,
        cursor_after(&headers, &query),
        query.limit,
        None,
    ))
}

async fn replay_scoped(
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Json<Value> {
    let scope = scope_for_id(&id);
    Json(replay(
        &state.lock().await.events,
        cursor_after(&headers, &query),
        query.limit,
        Some((scope, id)),
    ))
}

fn replay(
    events: &VecDeque<RuntimeEventRecord>,
    after: i64,
    limit: Option<usize>,
    scope: Option<(RuntimeEventScope, String)>,
) -> Value {
    let limit = limit.unwrap_or(2000).clamp(1, 2000);
    json!(events
        .iter()
        .filter(|event| scope
            .as_ref()
            .is_none_or(|(kind, id)| event.scope == *kind && event.scope_id == *id))
        .filter(|event| if scope.is_some() {
            event.seq > after
        } else {
            event.row_id > after
        })
        .take(limit)
        .collect::<Vec<_>>())
}

fn cursor_after(headers: &HeaderMap, query: &ReplayQuery) -> i64 {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .or(query.after_seq)
        .unwrap_or(0)
}

fn scope_for_id(id: &str) -> RuntimeEventScope {
    if id.starts_with("p02-team") {
        RuntimeEventScope::Team
    } else if id.starts_with("p02-process") {
        RuntimeEventScope::Process
    } else {
        RuntimeEventScope::Session
    }
}

async fn event_stream(
    headers: HeaderMap,
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Response {
    stream_events(state, headers, query, None).await
}

async fn stream_session(
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Response {
    stream_events(
        state,
        headers,
        query,
        Some((RuntimeEventScope::Session, id)),
    )
    .await
}

async fn stream_team(
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Response {
    stream_events(state, headers, query, Some((RuntimeEventScope::Team, id))).await
}

async fn stream_process(
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ReplayQuery>,
    State(state): State<Shared>,
) -> Response {
    stream_events(
        state,
        headers,
        query,
        Some((RuntimeEventScope::Process, id)),
    )
    .await
}

async fn stream_events(
    state: Shared,
    headers: HeaderMap,
    query: ReplayQuery,
    scope: Option<(RuntimeEventScope, String)>,
) -> Response {
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
    let after = cursor_after(&headers, &query);
    let live_rx = state.event_tx.subscribe();
    let mut replay = state
        .events
        .iter()
        .filter(|event| {
            scope
                .as_ref()
                .is_none_or(|(kind, id)| event.scope == *kind && event.scope_id == *id)
        })
        .filter(|event| {
            if scope.is_some() {
                event.seq > after
            } else {
                event.row_id > after
            }
        })
        .map(|event| {
            (
                event.clone(),
                state
                    .event_delays_ms
                    .get(&event.row_id)
                    .copied()
                    .unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    let limit = query.limit.unwrap_or(2000).clamp(1, 2000);
    replay.truncate(limit);
    let high_watermark = replay
        .last()
        .map(|(event, _)| {
            if scope.is_some() {
                event.seq
            } else {
                event.row_id
            }
        })
        .unwrap_or(after);
    drop(state);
    let scoped = scope.is_some();
    let replay_stream =
        stream::iter(replay).then(move |(event, delay)| event_frame(event, delay, scoped));
    let live_stream = stream::unfold(
        (live_rx, high_watermark, scope),
        |(mut receiver, watermark, scope)| async move {
            loop {
                match receiver.recv().await {
                    Ok((event, delay))
                        if scope.as_ref().is_none_or(|(kind, id)| {
                            event.scope == *kind && event.scope_id == *id
                        }) && (if scope.is_some() {
                            event.seq
                        } else {
                            event.row_id
                        }) > watermark =>
                    {
                        let next_watermark = if scope.is_some() {
                            event.seq
                        } else {
                            event.row_id
                        };
                        return Some((
                            event_frame(event, delay, scope.is_some()).await,
                            (receiver, next_watermark, scope),
                        ));
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );
    Sse::new(replay_stream.chain(live_stream)).into_response()
}

async fn event_frame(
    event: RuntimeEventRecord,
    delay_ms: u64,
    scoped: bool,
) -> Result<Event, std::convert::Infallible> {
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms.min(5_000))).await;
    }
    Ok(Event::default()
        .id(if scoped { event.seq } else { event.row_id }.to_string())
        .event("runtime")
        .json_data(event)
        .expect("serializable event"))
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
            state.event_delays_ms.clear();
        }
        FaultControl::Offline(value) => state.offline = value,
        FaultControl::Reset => *state = FakeState::default(),
        fault => {
            state.faults.push_back(fault);
            while state.faults.len() > 64 {
                state.faults.pop_front();
            }
        }
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
    if let Some(error) = take_rejection(&mut state) {
        return error;
    }
    (StatusCode::OK, Json(success))
}

fn take_rejection(state: &mut FakeState) -> Option<(StatusCode, Json<Value>)> {
    if let Some(index) = state
        .faults
        .iter()
        .position(|fault| matches!(fault, FaultControl::RejectNextCommand { .. }))
    {
        if let Some(FaultControl::RejectNextCommand { code }) = state.faults.remove(index) {
            return Some((
                StatusCode::CONFLICT,
                Json(json!({"error":{"code":code,"message":"deterministic rejection"}})),
            ));
        }
    }
    None
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
        seq: 1,
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
#[path = "fake_source_tests.rs"]
mod tests;
