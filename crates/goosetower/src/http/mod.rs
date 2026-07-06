use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use reqwest::Client;
use serde::Serialize;

use crate::config::{GoosetowerConfig, RuntimeSourceConfig};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<GoosetowerConfig>,
    pub api_bearer_token: Arc<str>,
    pub runtime_client: RuntimeHealthClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Starting,
    Ready,
}

#[derive(Clone)]
pub struct RuntimeHealthClient {
    client: Client,
}

impl RuntimeHealthClient {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("valid reqwest client"),
        }
    }

    async fn check_source(
        &self,
        config: &GoosetowerConfig,
        source: &RuntimeSourceConfig,
    ) -> RuntimeSourceStatus {
        if !source.enabled {
            return RuntimeSourceStatus::from_source(source, "disabled", None, None);
        }

        let base_url = source.base_url.trim_end_matches('/');
        let token = match config.resolve_runtime_auth(source) {
            Ok(token) => token,
            Err(error) => {
                return RuntimeSourceStatus::from_source(
                    source,
                    "auth_error",
                    None,
                    Some(error.to_string()),
                );
            }
        };

        let health_url = format!("{base_url}/health");
        let version_url = format!("{base_url}/v1/version");

        let health_result = self.client.get(&health_url).send().await;
        let health_response = match health_result {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                return RuntimeSourceStatus::from_source(
                    source,
                    "unhealthy",
                    None,
                    Some(format!("health check returned {}", response.status())),
                );
            }
            Err(error) => {
                return RuntimeSourceStatus::from_source(
                    source,
                    "offline",
                    None,
                    Some(error.to_string()),
                );
            }
        };
        drop(health_response);

        let mut version_request = self.client.get(&version_url);
        if let Some(token) = token {
            version_request = version_request.bearer_auth(token);
        }
        match version_request.send().await {
            Ok(response) if response.status().is_success() => {
                let version_payload = response.json::<RuntimeVersionResponse>().await.ok();
                RuntimeSourceStatus::from_source(
                    source,
                    "healthy",
                    version_payload.map(|payload| payload.version),
                    None,
                )
            }
            Ok(response) => RuntimeSourceStatus::from_source(
                source,
                "unhealthy",
                None,
                Some(format!("version check returned {}", response.status())),
            ),
            Err(error) => {
                RuntimeSourceStatus::from_source(source, "offline", None, Some(error.to_string()))
            }
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
        .route_layer(middleware::from_fn_with_state(
            state.api_bearer_token.clone(),
            bearer_auth,
        ));

    Router::new()
        .route("/health", get(health))
        .nest("/v1", protected)
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

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeSourceStatus {
    source_id: String,
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
        health: &'static str,
        version: Option<String>,
        error: Option<String>,
    ) -> Self {
        Self {
            source_id: source.source_id.clone(),
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

#[derive(Debug, serde::Deserialize)]
struct RuntimeVersionResponse {
    version: String,
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

    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use super::*;
    use crate::config::{AuthConfig, TicketConfig};

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
        assert_eq!(source["health"], "healthy");
        assert_eq!(source["version"], "9.9.9-test");
        assert_eq!(source["workspace_id"], "default");
    }

    fn test_state(config: GoosetowerConfig, token: &str) -> AppState {
        AppState {
            config: Arc::new(config),
            api_bearer_token: Arc::from(token.to_string()),
            runtime_client: RuntimeHealthClient::new(),
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
}
