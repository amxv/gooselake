use serde_json::{Value, json};

use crate::constants::GG_MCP_CALLER_AGENT_ID_ARG_KEY;

#[derive(Clone)]
pub(crate) struct GatewayClient {
    base_url: String,
    auth_token: String,
    http_client: reqwest::Client,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TeamModelPresetCapabilitySnapshot {
    pub(crate) revision: u64,
    pub(crate) presets: Vec<String>,
    pub(crate) team_tools_enabled: bool,
}

#[derive(Clone)]
pub(crate) struct GatewayClientConfig {
    pub(crate) base_url: String,
    pub(crate) auth_token: String,
    pub(crate) default_caller_agent_id: Option<String>,
    pub(crate) require_tool_caller_agent_id: bool,
}

impl GatewayClientConfig {
    pub(crate) fn from_env() -> Option<Self> {
        let base_url = std::env::var("GG_MCP_GATEWAY_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let auth_token = std::env::var("GG_MCP_GATEWAY_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let default_caller_agent_id = std::env::var("GG_MCP_CALLER_AGENT_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let require_tool_caller_agent_id = env_bool_flag("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID");

        let (Some(base_url), Some(auth_token)) = (base_url, auth_token) else {
            return None;
        };

        Some(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            auth_token,
            default_caller_agent_id,
            require_tool_caller_agent_id,
        })
    }
}

impl GatewayClient {
    pub(crate) fn from_config(config: GatewayClientConfig) -> Self {
        Self {
            base_url: config.base_url,
            auth_token: config.auth_token,
            http_client: reqwest::Client::new(),
        }
    }

    pub(crate) async fn fetch_team_model_presets(
        &self,
    ) -> Result<TeamModelPresetCapabilitySnapshot, String> {
        let response = self
            .http_client
            .get(format!("{}/capabilities", self.base_url))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.auth_token),
            )
            .send()
            .await
            .map_err(|error| format!("Failed to reach gateway /capabilities endpoint: {error}"))?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            format!("Failed to read gateway /capabilities response body: {error}")
        })?;
        let parsed: Value = serde_json::from_str(&body).map_err(|error| {
            format!("Failed to parse gateway /capabilities response as JSON: {error}")
        })?;

        if !status.is_success() {
            return Err(format!(
                "Gateway /capabilities returned status {} with payload {parsed}",
                status.as_u16()
            ));
        }

        let presets = parsed
            .pointer("/result/ggTeamModelPresets")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect::<Vec<_>>();
        let revision = parsed
            .pointer("/result/ggTeamModelPresetsRevision")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let team_tools_enabled = parsed
            .pointer("/result/supportedNamespaces")
            .and_then(Value::as_array)
            .map(|namespaces| {
                namespaces
                    .iter()
                    .any(|namespace| namespace.as_str() == Some("gg_team"))
            })
            .unwrap_or(false);

        Ok(TeamModelPresetCapabilitySnapshot {
            revision,
            presets,
            team_tools_enabled,
        })
    }

    pub(crate) async fn invoke_tool(
        &self,
        namespace: &str,
        tool_name: &str,
        args: Value,
        caller_agent_id: &str,
        invocation_id: Option<&str>,
    ) -> Result<Value, String> {
        let request_payload = json!({
            "namespace": namespace,
            "tool_name": tool_name,
            "caller_agent_id": caller_agent_id,
            "invocation_id": invocation_id,
            "args": args,
        });
        let response = self
            .http_client
            .post(format!("{}/invoke", self.base_url))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.auth_token),
            )
            .json(&request_payload)
            .send()
            .await
            .map_err(|error| format!("Failed to reach gateway /invoke endpoint: {error}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| format!("Failed to read gateway /invoke response body: {error}"))?;
        let parsed: Value = serde_json::from_str(&body).map_err(|error| {
            format!("Failed to parse gateway /invoke response as JSON: {error}")
        })?;

        if status.is_success() {
            return Ok(parsed);
        }

        if parsed.is_object() {
            return Ok(parsed);
        }

        Err(format!(
            "Gateway /invoke returned status {} with non-JSON-object body",
            status.as_u16()
        ))
    }
}

pub(crate) fn process_tools_enabled_from_env() -> bool {
    match std::env::var("GG_MCP_ENABLE_PROCESS_TOOLS") {
        Ok(raw_value) => {
            let normalized = raw_value.trim().to_ascii_lowercase();
            !matches!(normalized.as_str(), "0" | "false" | "off" | "")
        }
        Err(_) => true,
    }
}

fn env_bool_flag(key: &str) -> bool {
    match std::env::var(key) {
        Ok(raw_value) => {
            let normalized = raw_value.trim().to_ascii_lowercase();
            !matches!(normalized.as_str(), "0" | "false" | "off" | "")
        }
        Err(_) => false,
    }
}

pub(crate) fn normalize_non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn gateway_not_configured_envelope() -> Value {
    json!({
        "ok": false,
        "error": {
            "code": "backend_unavailable",
            "message": "GG MCP tool gateway is not configured",
            "details": {
                "required_env": [
                    "GG_MCP_GATEWAY_URL",
                    "GG_MCP_GATEWAY_TOKEN"
                ],
                "caller_sources": [
                    "GG_MCP_CALLER_AGENT_ID",
                    GG_MCP_CALLER_AGENT_ID_ARG_KEY
                ]
            }
        }
    })
}
