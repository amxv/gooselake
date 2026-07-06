use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::Json;
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use serde_json::{Value, json};
use tokio::process::Command;

#[derive(Clone)]
pub struct StubGatewayState {
    expected_auth_header: String,
    model_presets: Vec<String>,
    pub team_tools_enabled: bool,
    pub capabilities_calls: Arc<AtomicUsize>,
}

pub fn mcp_server_command() -> Command {
    Command::new(mcp_server_binary_path())
}

pub fn stub_gateway_state(auth_token: &str, model_presets: Vec<String>) -> StubGatewayState {
    StubGatewayState {
        expected_auth_header: format!("Bearer {auth_token}"),
        model_presets,
        team_tools_enabled: true,
        capabilities_calls: Arc::new(AtomicUsize::new(0)),
    }
}

pub async fn invoke_non_json_stub(
    State(state): State<StubGatewayState>,
    headers: HeaderMap,
) -> (StatusCode, &'static str) {
    let provided_auth = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided_auth != state.expected_auth_header {
        return (StatusCode::UNAUTHORIZED, "unauthorized");
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "gateway returned plain text",
    )
}

pub async fn capabilities_stub(
    State(state): State<StubGatewayState>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    state.capabilities_calls.fetch_add(1, Ordering::SeqCst);
    let provided_auth = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided_auth != state.expected_auth_header {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "ok": false,
                "error": {
                    "code": "unauthorized",
                    "message": "invalid auth header",
                }
            })),
        );
    }

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "result": {
                "serverVersion": "integration-test",
                "ggProcessEnabled": true,
                "ggTeamModelPresetsRevision": 1,
                "ggTeamModelPresets": state.model_presets,
                "supportedNamespaces": if state.team_tools_enabled {
                    json!(["gg_process", "gg_team"])
                } else {
                    json!(["gg_process"])
                },
            }
        })),
    )
}

pub async fn invoke_stub(
    State(state): State<StubGatewayState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let provided_auth = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided_auth != state.expected_auth_header {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "ok": false,
                "error": {
                    "code": "unauthorized",
                    "message": "invalid auth header",
                }
            })),
        );
    }

    let team_id = body
        .get("args")
        .and_then(|args| args.get("team_id"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let caller_agent_id = body
        .get("caller_agent_id")
        .and_then(Value::as_str)
        .unwrap_or_default();

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "result": {
                "team_id": team_id,
                "members": [
                    {
                        "agent_id": caller_agent_id,
                        "role": "lead",
                    }
                ],
            }
        })),
    )
}

pub fn extract_json_payload(content: &[rmcp::model::Content]) -> Result<Value, String> {
    let first = content.first().ok_or("tool result content is empty")?;
    let text = first
        .raw
        .as_text()
        .map(|entry| entry.text.as_str())
        .ok_or("tool result content is not text")?;
    serde_json::from_str::<Value>(text).map_err(|error| format!("invalid json payload: {error}"))
}

fn mcp_server_binary_path() -> PathBuf {
    let cargo_bin_path = PathBuf::from(env!("CARGO_BIN_EXE_gg-mcp-server"));
    if is_executable_file(&cargo_bin_path) {
        return cargo_bin_path;
    }

    let mut candidates = vec![cargo_bin_path.clone()];
    candidates.push(sidecar_target_binary_path());

    candidates
        .iter()
        .find(|path| is_executable_file(path))
        .cloned()
        .unwrap_or_else(|| {
            panic!(
                "gg-mcp-server test binary was not found. Tried: {}",
                candidates
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
}

fn sidecar_target_binary_path() -> PathBuf {
    let executable_name = if cfg!(windows) {
        "gg-mcp-server.exe"
    } else {
        "gg-mcp-server"
    };

    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join(executable_name)
}

fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}
