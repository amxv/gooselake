use std::process;

use rmcp::model::{CallToolResult, Content};
use serde_json::{Value, json};

use crate::constants::GG_TEAM_MANAGE_BASE_DESCRIPTION;

pub(crate) fn build_ping_payload() -> Value {
    json!({
        "ok": true,
        "result": {
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "pid": process::id(),
            "build_mode": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rust_log": std::env::var("RUST_LOG").ok(),
        }
    })
}

pub(crate) fn build_team_manage_description(model_presets: &[String]) -> String {
    if model_presets.is_empty() {
        GG_TEAM_MANAGE_BASE_DESCRIPTION.to_string()
    } else {
        format!(
            "{GG_TEAM_MANAGE_BASE_DESCRIPTION} Available model_preset values: {}.",
            model_presets.join(", ")
        )
    }
}

pub(crate) fn envelope_to_call_tool_result(envelope: Value) -> CallToolResult {
    let is_ok = envelope
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let serialized = serde_json::to_string(&envelope).unwrap_or_else(|error| {
        format!(
            "{{\"ok\":false,\"error\":{{\"code\":\"internal_error\",\"message\":\"Failed to serialize MCP tool envelope: {error}\"}}}}"
        )
    });

    if is_ok {
        CallToolResult::success(vec![Content::text(serialized)])
    } else {
        CallToolResult::error(vec![Content::text(serialized)])
    }
}

pub(crate) fn annotate_envelope_with_caller_agent_id(
    mut envelope: Value,
    caller_agent_id: Option<&str>,
) -> Value {
    let Some(caller_agent_id) = caller_agent_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
    else {
        return envelope;
    };

    let Some(envelope_object) = envelope.as_object_mut() else {
        return envelope;
    };
    envelope_object
        .entry("caller_agent_id".to_string())
        .or_insert_with(|| Value::String(caller_agent_id.clone()));
    if let Some(result_object) = envelope_object
        .get_mut("result")
        .and_then(serde_json::Value::as_object_mut)
    {
        result_object
            .entry("caller_agent_id".to_string())
            .or_insert_with(|| Value::String(caller_agent_id));
    }

    envelope
}
