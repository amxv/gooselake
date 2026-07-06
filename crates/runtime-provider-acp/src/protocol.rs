use std::path::{Path, PathBuf};

use runtime_core::RuntimeError;
use serde_json::Value;

use crate::config::DEFAULT_PROTOCOL_VERSION;
use crate::state::AcpAgentCapabilities;

pub(super) fn message_id_key(message: &Value) -> Option<String> {
    match message.get("id") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Number(value)) => Some(value.to_string()),
        _ => None,
    }
}

pub(super) fn parse_initialize_capabilities(
    result: &Value,
) -> Result<AcpAgentCapabilities, RuntimeError> {
    let protocol_version = result
        .get("protocolVersion")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "acp initialize response missing protocolVersion".to_string(),
            )
        })?;
    if protocol_version != DEFAULT_PROTOCOL_VERSION {
        return Err(RuntimeError::ProtocolViolation(format!(
            "acp protocol version mismatch (expected={}, actual={})",
            DEFAULT_PROTOCOL_VERSION, protocol_version
        )));
    }

    let agent_capabilities = result
        .get("agentCapabilities")
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));

    Ok(AcpAgentCapabilities {
        load_session: agent_capabilities
            .get("loadSession")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        resume_session: agent_capabilities
            .get("sessionCapabilities")
            .and_then(|value| value.get("resume"))
            .is_some(),
        close_session: agent_capabilities
            .get("sessionCapabilities")
            .and_then(|value| value.get("close"))
            .is_some(),
    })
}

pub(super) fn jsonrpc_error_message(error: &Value) -> String {
    match error.get("message").and_then(Value::as_str) {
        Some(message) => message.to_string(),
        None => error.to_string(),
    }
}

pub(super) fn absolutize_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}
