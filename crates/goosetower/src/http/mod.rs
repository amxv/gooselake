use std::sync::Arc;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, options, post};
use axum::{Json, Router};
use serde::Serialize;
use std::collections::HashMap;

use crate::auth::origin_is_allowed;
use crate::config::{GoosetowerConfig, RuntimeSourceConfig};
use crate::gateway::GatewayState;
use crate::runtime::{GooselakeRuntimeClient, GooselakeRuntimeClientConfig};

mod dev_tickets;

use dev_tickets::{dev_ticket_preflight, mint_dev_ticket};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<GoosetowerConfig>,
    pub api_bearer_token: Arc<str>,
    pub runtime_client: RuntimeHealthClient,
    pub gateway: Arc<GatewayState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Starting,
    Ready,
}

#[derive(Clone)]
pub struct RuntimeHealthClient;

impl RuntimeHealthClient {
    pub fn new() -> Self {
        Self
    }

    async fn check_source(
        &self,
        config: &GoosetowerConfig,
        source: &RuntimeSourceConfig,
    ) -> RuntimeSourceStatus {
        if !source.enabled {
            return RuntimeSourceStatus::from_source(source, None, "disabled", None, None);
        }

        let token = match config.resolve_runtime_auth(source) {
            Ok(token) => token,
            Err(error) => {
                return RuntimeSourceStatus::from_source(
                    source,
                    None,
                    "auth_error",
                    None,
                    Some(error.to_string()),
                );
            }
        };

        let runtime_client = match GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
            source.source_id.clone(),
            source.base_url.clone(),
            token,
        )) {
            Ok(client) => client,
            Err(error) => {
                return RuntimeSourceStatus::from_source(
                    source,
                    None,
                    "offline",
                    None,
                    Some(error.to_string()),
                );
            }
        };

        match runtime_client.health().await {
            Ok(_) => {}
            Err(error) => {
                return RuntimeSourceStatus::from_source(
                    source,
                    None,
                    "offline",
                    None,
                    Some(error.to_string()),
                );
            }
        }

        let bootstrap = match runtime_client.source_bootstrap().await {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                return RuntimeSourceStatus::from_source(
                    source,
                    None,
                    "incompatible",
                    None,
                    Some(format!("runtime bootstrap authority unavailable: {error}")),
                );
            }
        };

        match runtime_client.version().await {
            Ok(version_payload) => RuntimeSourceStatus::from_source(
                source,
                Some(bootstrap.source_epoch),
                "healthy",
                Some(version_payload.version),
                None,
            ),
            Err(error) => RuntimeSourceStatus::from_source(
                source,
                Some(bootstrap.source_epoch),
                "unhealthy",
                None,
                Some(error.to_string()),
            ),
        }
    }
}

impl Default for RuntimeHealthClient {
    fn default() -> Self {
        Self::new()
    }
}

pub fn build_router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/health", get(protected_health))
        .route("/sources", get(list_sources))
        .route("/metrics", get(metrics))
        .route("/debug/protocol", get(debug_protocol))
        .route("/debug/sources", get(debug_sources))
        .route("/debug/subscriptions", get(debug_subscriptions))
        .route("/debug/materializer", get(debug_materializer))
        .route("/debug/audit", get(debug_audit))
        .route_layer(middleware::from_fn_with_state(
            state.api_bearer_token.clone(),
            bearer_auth,
        ));
    let dev_tickets = Router::new()
        .route("/dev/tickets", options(dev_ticket_preflight))
        .route(
            "/dev/tickets",
            post(mint_dev_ticket).route_layer(middleware::from_fn_with_state(
                state.api_bearer_token.clone(),
                bearer_auth,
            )),
        );

    Router::new()
        .route("/health", get(health))
        .nest(
            "/v1",
            Router::new()
                .route("/realtime", get(realtime))
                .merge(dev_tickets)
                .merge(protected),
        )
        .with_state(state)
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    status: &'static str,
    service: &'static str,
    version: &'static str,
    public_base_url: String,
    configured_sources: usize,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "goosetower",
        version: env!("CARGO_PKG_VERSION"),
        public_base_url: state.config.server.public_base_url.clone(),
        configured_sources: state.config.runtimes.sources.len(),
    })
}

async fn protected_health(State(state): State<AppState>) -> Json<ProtectedHealthResponse> {
    let mut healthy_sources = 0;
    let mut source_count = 0;
    for source in &state.config.runtimes.sources {
        if !source.enabled {
            continue;
        }
        source_count += 1;
        let status = state
            .runtime_client
            .check_source(&state.config, source)
            .await;
        if status.health == "healthy" {
            healthy_sources += 1;
        }
    }

    Json(ProtectedHealthResponse {
        status: if source_count == healthy_sources {
            "ok"
        } else {
            "degraded"
        },
        service: "goosetower",
        version: env!("CARGO_PKG_VERSION"),
        public_base_url: state.config.server.public_base_url.clone(),
        configured_sources: state.config.runtimes.sources.len(),
        healthy_sources,
    })
}

#[derive(Debug, Serialize)]
pub struct ProtectedHealthResponse {
    status: &'static str,
    service: &'static str,
    version: &'static str,
    public_base_url: String,
    configured_sources: usize,
    healthy_sources: usize,
}

#[derive(Debug, Serialize)]
pub struct SourcesResponse {
    sources: Vec<RuntimeSourceStatus>,
}

async fn list_sources(State(state): State<AppState>) -> Json<SourcesResponse> {
    let mut sources = Vec::with_capacity(state.config.runtimes.sources.len());
    for source in &state.config.runtimes.sources {
        sources.push(
            state
                .runtime_client
                .check_source(&state.config, source)
                .await,
        );
    }
    Json(SourcesResponse { sources })
}

async fn metrics(State(state): State<AppState>) -> Json<crate::gateway::GatewayMetricsSnapshot> {
    Json(state.gateway.metrics_snapshot())
}

async fn debug_protocol(
    State(state): State<AppState>,
) -> Result<Json<crate::gateway::ProtocolDebugSnapshot>, StatusCode> {
    ensure_debug_enabled(&state)?;
    Ok(Json(state.gateway.debug_protocol_version().await))
}

async fn debug_sources(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::gateway::SourceDebugSnapshot>>, StatusCode> {
    ensure_debug_enabled(&state)?;
    Ok(Json(state.gateway.debug_active_sources().await))
}

async fn debug_subscriptions(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::gateway::ActiveConnectionDebug>>, StatusCode> {
    ensure_debug_enabled(&state)?;
    Ok(Json(state.gateway.debug_active_subscriptions().await))
}

async fn debug_materializer(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::gateway::MaterializerDebugSummary>>, StatusCode> {
    ensure_debug_enabled(&state)?;
    Ok(Json(state.gateway.debug_materializer_summary().await))
}

async fn debug_audit(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::gateway::GatewayAuditRecord>>, StatusCode> {
    ensure_debug_enabled(&state)?;
    Ok(Json(state.gateway.recent_gateway_audit().await))
}

fn ensure_debug_enabled(state: &AppState) -> Result<(), StatusCode> {
    state
        .config
        .debug
        .endpoints_enabled
        .then_some(())
        .ok_or(StatusCode::FORBIDDEN)
}

async fn realtime(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
    else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let allowed_origins = match state.gateway.allowed_origins() {
        Ok(origins) => origins,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if !origin_is_allowed(origin.as_str(), &allowed_origins) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(ticket) = query.get("ticket") else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let auth = match state
        .gateway
        .validate_ticket(ticket.as_str(), origin.as_str())
        .await
    {
        Ok(auth) => auth,
        Err(reject) => {
            tracing::info!(reason = %reject.code, "gateway audit auth.rejected");
            return reject.status.into_response();
        }
    };
    if !auth.has_scope("gateway:connect") {
        return StatusCode::FORBIDDEN.into_response();
    }
    let gateway = state.gateway.clone();
    ws.max_message_size(state.config.websocket.max_message_bytes)
        .on_upgrade(move |socket| gateway.handle_socket(socket, auth))
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeSourceStatus {
    source_id: String,
    source_epoch: String,
    source_kind: String,
    base_url: String,
    enabled: bool,
    display_name: String,
    workspace_id: String,
    health: &'static str,
    version: Option<String>,
    error: Option<String>,
}

impl RuntimeSourceStatus {
    fn from_source(
        source: &RuntimeSourceConfig,
        source_epoch: Option<String>,
        health: &'static str,
        version: Option<String>,
        error: Option<String>,
    ) -> Self {
        Self {
            source_id: source.source_id.clone(),
            source_epoch: source_epoch.unwrap_or_else(|| "unavailable".to_string()),
            source_kind: source.source_kind.clone(),
            base_url: source.base_url.clone(),
            enabled: source.enabled,
            display_name: source.display_name.clone(),
            workspace_id: source.workspace_id.clone(),
            health,
            version,
            error,
        }
    }
}

async fn bearer_auth(
    State(expected_token): State<Arc<str>>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(header_value) = headers.get(header::AUTHORIZATION) else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    let Ok(raw) = header_value.to_str() else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    let Some(token) = raw.strip_prefix("Bearer ") else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    if token != expected_token.as_ref() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await.into_response())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::{to_bytes, Body};
    use axum::http::{HeaderValue, Method, Request, StatusCode};
    use futures_util::{SinkExt, StreamExt};
    use prost::Message as ProstMessage;
    use runtime_core::{SendTurnAccepted, SessionRecord};
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    use tower::ServiceExt;

    use super::*;
    use crate::auth::TicketIssuer;
    use crate::config::{AuthConfig, TicketConfig};
    use crate::materializer::MaterializedState;
    use crate::protocol::generated::goosetower::v1::command::Payload as CommandPayload;
    use crate::protocol::generated::goosetower::v1::realtime_envelope::Payload;
    use crate::protocol::generated::goosetower::v1::{
        Command, CommandSendTurn, MessageKind, Ping, RealtimeEnvelope, Scope, Subscribe,
    };
    use crate::protocol::PROTOCOL_VERSION;

    #[tokio::test]
    async fn health_route_is_public_and_v1_requires_bearer_auth() {
        let state = test_state(GoosetowerConfig::default(), "tower-token");
        let router = build_router(state);

        let public = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("public health response");
        assert_eq!(public.status(), StatusCode::OK);

        let unauthorized = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("unauthorized response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = router
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .header(header::AUTHORIZATION, "Bearer tower-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("authorized response");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dev_ticket_preflight_allows_configured_origins_without_weakening_post_auth() {
        let mut config = GoosetowerConfig::default();
        config.server.allowed_gooseweb_origins = vec!["http://localhost:3000".to_string()];
        let router = build_router(test_state(config, "tower-token"));

        let preflight = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/v1/dev/tickets")
                    .header(header::ORIGIN, "http://localhost:3000")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(
                        header::ACCESS_CONTROL_REQUEST_HEADERS,
                        "authorization,content-type",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("preflight response");
        assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            preflight.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://localhost:3000"))
        );
        assert_eq!(
            preflight
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_METHODS),
            Some(&HeaderValue::from_static("POST, OPTIONS"))
        );
        assert_eq!(
            preflight
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_HEADERS),
            Some(&HeaderValue::from_static("authorization, content-type"))
        );

        let rejected_preflight = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/v1/dev/tickets")
                    .header(header::ORIGIN, "http://evil.local")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("rejected preflight response");
        assert_eq!(rejected_preflight.status(), StatusCode::FORBIDDEN);

        let unauthorized_post = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/dev/tickets")
                    .header(header::ORIGIN, "http://localhost:3000")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .expect("unauthorized post response");
        assert_eq!(unauthorized_post.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sources_route_reports_mock_runtime_health_and_version() {
        let runtime_token = "runtime-token";
        let runtime_addr = spawn_mock_runtime(runtime_token).await;
        let mut config = GoosetowerConfig::default();
        config.auth = AuthConfig {
            api_token: Some("tower-token".to_string()),
            api_token_file: None,
        };
        config.tickets = TicketConfig {
            signing_key: Some("ticket-key".to_string()),
            ..TicketConfig::default()
        };
        config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
        config.runtimes.sources[0].bearer_token = Some(runtime_token.to_string());

        let router = build_router(test_state(config, "tower-token"));
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v1/sources")
                    .header(header::AUTHORIZATION, "Bearer tower-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("sources response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("sources body");
        let json: Value = serde_json::from_slice(&body).expect("sources json");
        let source = &json["sources"][0];
        assert_eq!(source["source_id"], "local");
        assert_eq!(source["source_epoch"], "runtime-health-epoch");
        assert_eq!(source["health"], "healthy");
        assert_eq!(source["version"], "9.9.9-test");
        assert_eq!(source["workspace_id"], "default");
    }

    #[tokio::test]
    async fn realtime_gateway_rejects_invalid_origin_and_replayed_ticket() {
        let config = GoosetowerConfig::default();
        let addr = spawn_gateway(config.clone()).await.0;
        let ticket = mint_test_ticket(&config);

        let invalid = connect_gateway_with_origin(addr, &ticket, "http://evil.local").await;
        assert!(invalid.is_err(), "invalid origin should fail upgrade");

        let (mut socket, _) = connect_gateway(addr, &ticket)
            .await
            .expect("valid websocket");
        let hello = read_kind(&mut socket, MessageKind::Hello).await;
        assert!(matches!(hello.payload, Some(Payload::Hello(_))));
        socket.close(None).await.expect("close socket");

        let replay = connect_gateway(addr, &ticket).await;
        assert!(replay.is_err(), "replayed ticket should fail upgrade");
    }

    #[tokio::test]
    async fn realtime_gateway_sends_hello_heartbeat_and_enforces_binary_size() {
        let mut config = GoosetowerConfig::default();
        config.websocket.max_message_bytes = 64;
        let addr = spawn_gateway(config.clone()).await.0;
        let ticket = mint_test_ticket(&config);
        let (mut socket, _) = connect_gateway(addr, &ticket).await.expect("websocket");

        let hello = read_kind(&mut socket, MessageKind::Hello).await;
        let Some(Payload::Hello(hello)) = hello.payload else {
            panic!("expected hello");
        };
        assert_eq!(hello.protocol_version, PROTOCOL_VERSION);
        assert_eq!(hello.max_message_bytes, 64);

        socket
            .send(WsMessage::Binary(
                envelope(Payload::Ping(Ping {
                    client_time_unix_ms: 42,
                }))
                .encode_to_vec()
                .into(),
            ))
            .await
            .expect("send ping");
        let pong = read_kind(&mut socket, MessageKind::Pong).await;
        assert!(matches!(pong.payload, Some(Payload::Pong(_))));

        socket
            .send(WsMessage::Binary(vec![1_u8; 65].into()))
            .await
            .expect("send oversized");
        let next = socket
            .next()
            .await
            .expect("oversized frame closes connection");
        assert!(next.is_err() || matches!(next, Ok(WsMessage::Close(_))));
    }

    #[tokio::test]
    async fn realtime_gateway_subscribe_returns_snapshot() {
        let config = GoosetowerConfig::default();
        let (addr, gateway) = spawn_gateway(config.clone()).await;
        let mut state = MaterializedState::new("local", "static-0");
        state
            .sessions
            .insert("session_1".to_string(), session_record());
        gateway
            .replace_materialized_state("local".to_string(), state)
            .await;

        let ticket = mint_test_ticket(&config);
        let (mut socket, _) = connect_gateway(addr, &ticket).await.expect("websocket");
        let _ = read_kind(&mut socket, MessageKind::Hello).await;
        socket
            .send(WsMessage::Binary(
                envelope(Payload::Subscribe(Subscribe {
                    subscription_id: "sub_board".to_string(),
                    view_kind: "board".to_string(),
                    request_id: "request-board".to_string(),
                    filters: [("source_id".to_string(), "local".to_string())]
                        .into_iter()
                        .collect(),
                }))
                .encode_to_vec()
                .into(),
            ))
            .await
            .expect("send subscribe");

        let snapshot = read_kind(&mut socket, MessageKind::Snapshot).await;
        let Some(Payload::Snapshot(snapshot)) = snapshot.payload else {
            panic!("expected snapshot");
        };
        assert_eq!(snapshot.view_kind, "board");
        let body: Value = serde_json::from_slice(&snapshot.body).expect("snapshot json");
        assert_eq!(body["total_rows"], 1);
        assert_eq!(body["rows"][0]["session_id"], "session_1");
    }

    #[tokio::test]
    async fn realtime_gateway_routes_command_and_returns_duplicate() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let runtime_addr = spawn_command_runtime("runtime-token", call_count.clone()).await;
        let mut config = GoosetowerConfig::default();
        config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
        config.runtimes.sources[0].bearer_token = Some("runtime-token".to_string());
        let (addr, gateway) = spawn_gateway(config.clone()).await;
        let mut materialized = MaterializedState::new("local", "static-0");
        materialized.mark_live();
        materialized.upsert_session(session_record());
        gateway
            .replace_materialized_state("local".to_string(), materialized)
            .await;
        let ticket = mint_test_ticket(&config);
        let (mut socket, _) = connect_gateway(addr, &ticket).await.expect("websocket");
        let _ = read_kind(&mut socket, MessageKind::Hello).await;

        let command = command_envelope("cmd_1");
        socket
            .send(WsMessage::Binary(command.encode_to_vec().into()))
            .await
            .expect("send command");
        let accepted = read_kind(&mut socket, MessageKind::CommandAccepted).await;
        assert!(matches!(
            accepted.payload,
            Some(Payload::CommandAccepted(_))
        ));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        socket
            .send(WsMessage::Binary(
                command_envelope("cmd_1").encode_to_vec().into(),
            ))
            .await
            .expect("send duplicate");
        let duplicate = read_kind(&mut socket, MessageKind::CommandDuplicate).await;
        assert!(matches!(
            duplicate.payload,
            Some(Payload::CommandDuplicate(_))
        ));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    fn test_state(config: GoosetowerConfig, token: &str) -> AppState {
        let config = Arc::new(config);
        AppState {
            gateway: Arc::new(GatewayState::new(config.clone()).expect("gateway state")),
            config,
            api_bearer_token: Arc::from(token.to_string()),
            runtime_client: RuntimeHealthClient::new(),
        }
    }

    async fn spawn_gateway(config: GoosetowerConfig) -> (SocketAddr, Arc<GatewayState>) {
        let config = Arc::new(config);
        let gateway = Arc::new(GatewayState::new(config.clone()).expect("gateway"));
        let state = AppState {
            gateway: gateway.clone(),
            config,
            api_bearer_token: Arc::from("tower-token".to_string()),
            runtime_client: RuntimeHealthClient::new(),
        };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway");
        let addr = listener.local_addr().expect("gateway addr");
        tokio::spawn(async move {
            axum::serve(listener, build_router(state))
                .await
                .expect("gateway server");
        });
        (addr, gateway)
    }

    fn mint_test_ticket(config: &GoosetowerConfig) -> String {
        TicketIssuer::from_config(config)
            .expect("issuer")
            .mint_dev_ticket(
                "session_1",
                "default",
                vec!["gateway:connect".to_string(), "gateway:command".to_string()],
                vec!["http://localhost:3000".to_string()],
            )
            .expect("ticket")
    }

    async fn connect_gateway(
        addr: SocketAddr,
        ticket: &str,
    ) -> tokio_tungstenite::tungstenite::Result<(
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    )> {
        connect_gateway_with_origin(addr, ticket, "http://localhost:3000").await
    }

    async fn connect_gateway_with_origin(
        addr: SocketAddr,
        ticket: &str,
        origin: &str,
    ) -> tokio_tungstenite::tungstenite::Result<(
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    )> {
        let mut request = format!("ws://{addr}/v1/realtime?ticket={ticket}")
            .into_client_request()
            .expect("websocket request");
        request.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_str(origin).expect("origin header"),
        );
        connect_async(request).await
    }

    async fn read_kind(
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        kind: MessageKind,
    ) -> RealtimeEnvelope {
        for _ in 0..8 {
            let message = socket
                .next()
                .await
                .expect("websocket message")
                .expect("websocket ok");
            let WsMessage::Binary(bytes) = message else {
                continue;
            };
            let envelope = RealtimeEnvelope::decode(bytes.as_ref()).expect("decode envelope");
            if MessageKind::try_from(envelope.message_kind).ok() == Some(kind) {
                return envelope;
            }
        }
        panic!("did not receive {:?}", kind);
    }

    fn envelope(payload: Payload) -> RealtimeEnvelope {
        let kind = match &payload {
            Payload::Ping(_) => MessageKind::Ping,
            Payload::Subscribe(_) => MessageKind::Subscribe,
            Payload::Command(_) => MessageKind::Command,
            _ => MessageKind::Unspecified,
        };
        RealtimeEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_id: "test_msg".to_string(),
            message_kind: kind as i32,
            lane: crate::protocol::generated::goosetower::v1::Lane::Critical as i32,
            payload: Some(payload),
            ..RealtimeEnvelope::default()
        }
    }

    fn command_envelope(command_id: &str) -> RealtimeEnvelope {
        envelope(Payload::Command(Command {
            command_id: command_id.to_string(),
            target: Some(crate::protocol::generated::goosetower::v1::EntityRef {
                scope: Scope::Session as i32,
                scope_id: "session_1".to_string(),
                entity_id: "session_1".to_string(),
                entity_version: 1,
            }),
            base_entity_version: 1,
            created_at_client_unix_ms: 1,
            payload: Some(CommandPayload::SendTurn(CommandSendTurn {
                session_id: "session_1".to_string(),
                text: "hello".to_string(),
                input: Vec::new(),
            })),
            ..Command::default()
        }))
    }

    fn session_record() -> SessionRecord {
        SessionRecord {
            id: "session_1".to_string(),
            provider: "codex".to_string(),
            status: "ready".to_string(),
            cwd: Some("/repo".to_string()),
            model: Some("gpt-5".to_string()),
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: None,
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: 1,
            updated_at: 1,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        }
    }

    async fn spawn_mock_runtime(expected_token: &'static str) -> SocketAddr {
        let app = Router::new()
            .route(
                "/health",
                get(|| async {
                    Json(serde_json::json!({
                        "status": "ok",
                        "providers": 1,
                        "public_base_url": "http://runtime.test"
                    }))
                }),
            )
            .route(
                "/v1/version",
                get(move |headers: HeaderMap| async move {
                    let authorized = headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        == Some(format!("Bearer {expected_token}").as_str());
                    if !authorized {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    Json(serde_json::json!({ "version": "9.9.9-test" })).into_response()
                }),
            )
            .route(
                "/v1/bootstrap",
                get(move |headers: HeaderMap| async move {
                    let authorized = headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        == Some(format!("Bearer {expected_token}").as_str());
                    if !authorized {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    Json(serde_json::json!({
                        "source_epoch": "runtime-health-epoch",
                        "high_watermark": 0,
                        "records": {
                            "sessions": [], "approvals": [], "teams": [],
                            "team_members": [], "team_messages": [], "team_deliveries": [],
                            "managed_worktrees": [], "managed_worktree_claims": [], "processes": []
                        }
                    }))
                    .into_response()
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock runtime");
        let addr = listener.local_addr().expect("mock runtime addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock runtime server");
        });
        addr
    }

    async fn spawn_command_runtime(
        expected_token: &'static str,
        call_count: Arc<AtomicUsize>,
    ) -> SocketAddr {
        let route = move |headers: HeaderMap| {
            let call_count = call_count.clone();
            async move {
                let authorized = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    == Some(format!("Bearer {expected_token}").as_str());
                if !authorized {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                call_count.fetch_add(1, Ordering::SeqCst);
                Json(SendTurnAccepted {
                    session_id: "session_1".to_string(),
                    turn_id: "turn_1".to_string(),
                    status: "in_progress".to_string(),
                })
                .into_response()
            }
        };
        let app = Router::new().route("/v1/sessions/session_1/turns", post(route));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind command runtime");
        let addr = listener.local_addr().expect("command runtime addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("command runtime server");
        });
        addr
    }
}
