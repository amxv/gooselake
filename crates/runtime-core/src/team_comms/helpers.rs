use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use serde_json::Value;

use crate::{
    AgentMessageContextKind, AgentMessageRecord, ProviderKind, RuntimeError, TeamMemberRecord,
    TeamMessageRecord,
};

use super::{
    DELIVERY_POLICY_IMMEDIATE_INTERRUPT, DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY,
    DELIVERY_POLICY_NON_INTERRUPTING, DELIVERY_POLICY_START_NEW_TURN_ONLY,
    DELIVERY_STATUS_CANCELLED, DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_FAILED,
    DELIVERY_STATUS_INJECTED, DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_PENDING,
};

const MAX_MESSAGE_IMAGE_COUNT: usize = 8;

pub(super) fn ensure_member(
    maybe_members: Option<&HashMap<String, TeamMemberRecord>>,
    agent_id: &str,
    team_id: &str,
) -> Result<(), RuntimeError> {
    if maybe_members
        .map(|members| members.contains_key(agent_id))
        .unwrap_or(false)
    {
        return Ok(());
    }
    Err(RuntimeError::InvalidState(format!(
        "agent {} is not a member of team {}",
        agent_id, team_id
    )))
}

pub(super) fn remove_delivery_from_recipient_index(
    recipient_delivery_ids: &mut HashMap<String, Vec<String>>,
    recipient_agent_id: &str,
    delivery_id: &str,
) {
    let mut should_remove_key = false;
    if let Some(ids) = recipient_delivery_ids.get_mut(recipient_agent_id) {
        ids.retain(|candidate| candidate != delivery_id);
        should_remove_key = ids.is_empty();
    }
    if should_remove_key {
        recipient_delivery_ids.remove(recipient_agent_id);
    }
}

pub(super) fn normalize_non_empty(value: &str, field: &str) -> Result<String, RuntimeError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(RuntimeError::InvalidState(format!(
            "{} cannot be empty",
            field
        )));
    }
    Ok(trimmed.to_string())
}

pub(super) fn normalized_non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn normalize_non_empty_input(input: Value) -> Result<Value, RuntimeError> {
    let Value::Array(items) = input else {
        return Err(RuntimeError::InvalidState(
            "message input must be an array".to_string(),
        ));
    };
    if items.is_empty() {
        return Err(RuntimeError::InvalidState(
            "message input cannot be empty".to_string(),
        ));
    }
    Ok(Value::Array(items))
}

pub(super) fn normalize_scope(scope: &str) -> Result<String, RuntimeError> {
    match scope.trim().to_ascii_lowercase().as_str() {
        "direct" => Ok("direct".to_string()),
        "broadcast" => Ok("broadcast".to_string()),
        value => Err(RuntimeError::InvalidState(format!(
            "unsupported message scope {}",
            value
        ))),
    }
}

pub(super) fn normalize_priority(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return "normal".to_string();
    }
    normalized
}

pub(super) fn normalize_policy(value: &str) -> Result<String, RuntimeError> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        DELIVERY_POLICY_NON_INTERRUPTING
        | DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY
        | DELIVERY_POLICY_IMMEDIATE_INTERRUPT
        | DELIVERY_POLICY_START_NEW_TURN_ONLY => Ok(normalized),
        _ => Err(RuntimeError::InvalidState(format!(
            "unsupported delivery policy {}",
            value
        ))),
    }
}

pub(super) fn idempotency_index_key(team_id: &str, sender: &str, scope: &str, key: &str) -> String {
    format!("{}|{}|{}|{}", team_id, sender, scope, key)
}

pub(super) fn agent_idempotency_index_key(message: &AgentMessageRecord, key: &str) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        message.sender_agent_id,
        message.scope,
        message.context_kind.as_str(),
        message.workspace_id.as_deref().unwrap_or(""),
        message.legacy_team_id.as_deref().unwrap_or(""),
        key
    )
}

pub(super) fn prospective_agent_idempotency_key(
    sender_agent_id: &str,
    scope: &str,
    context_kind: AgentMessageContextKind,
    workspace_id: Option<&str>,
    key: &str,
) -> String {
    format!(
        "{}|{}|{}|{}||{}",
        sender_agent_id,
        scope,
        context_kind.as_str(),
        workspace_id.unwrap_or(""),
        key
    )
}

pub(super) fn validate_message_image_paths(
    image_paths: Vec<String>,
) -> Result<Vec<String>, RuntimeError> {
    if image_paths.len() > MAX_MESSAGE_IMAGE_COUNT {
        return Err(RuntimeError::InvalidState(format!(
            "image_paths exceeds maximum count {MAX_MESSAGE_IMAGE_COUNT}"
        )));
    }
    let mut validated = Vec::with_capacity(image_paths.len());
    for (index, raw_path) in image_paths.into_iter().enumerate() {
        let source_path = raw_path.trim();
        if source_path.is_empty() {
            return Err(image_path_error(index, "blank"));
        }
        let path = Path::new(source_path);
        let metadata = std::fs::metadata(path).map_err(|error| {
            let reason = match error.kind() {
                std::io::ErrorKind::NotFound => "not_found",
                std::io::ErrorKind::PermissionDenied => "not_readable",
                _ => "metadata_failed",
            };
            image_path_error(index, reason)
        })?;
        if !metadata.is_file() {
            return Err(image_path_error(index, "not_file"));
        }
        let mut file = std::fs::File::open(path).map_err(|error| {
            let reason = match error.kind() {
                std::io::ErrorKind::PermissionDenied => "not_readable",
                _ => "read_failed",
            };
            image_path_error(index, reason)
        })?;
        let mut prefix = [0_u8; 12];
        let read = file
            .read(&mut prefix)
            .map_err(|_| image_path_error(index, "read_failed"))?;
        if infer_image_media_type(&prefix[..read]).is_none() {
            return Err(image_path_error(index, "unsupported_media_type"));
        }
        validated.push(source_path.to_string());
    }
    Ok(validated)
}

pub(super) fn ensure_message_images_supported(
    provider: &str,
    has_images: bool,
) -> Result<(), RuntimeError> {
    if !has_images {
        return Ok(());
    }
    match ProviderKind::from_str(provider) {
        Some(ProviderKind::Codex | ProviderKind::Claude) => Ok(()),
        Some(ProviderKind::Acp) => Err(RuntimeError::Unsupported(format!(
            "message image delivery is not supported by the active {provider} transport"
        ))),
        None => Err(RuntimeError::ProtocolViolation(format!(
            "unknown recipient provider {provider}"
        ))),
    }
}

fn image_path_error(index: usize, reason: &str) -> RuntimeError {
    RuntimeError::InvalidState(format!("image_paths[{index}] is invalid ({reason})"))
}

fn infer_image_media_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some("image/png");
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("image/jpeg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    None
}

pub(super) fn parse_counter(value: &str) -> Option<u64> {
    value
        .rsplit('_')
        .next()
        .and_then(|suffix| suffix.parse::<u64>().ok())
}

pub(super) fn is_terminal_status(status: &str) -> bool {
    matches!(
        status,
        DELIVERY_STATUS_INJECTED | DELIVERY_STATUS_FAILED | DELIVERY_STATUS_CANCELLED
    )
}

pub(super) fn is_valid_transition(current: &str, next: &str) -> bool {
    matches!(
        (current, next),
        (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_PENDING)
            | (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_DEFERRED)
            | (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_INJECTING)
            | (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_CANCELLED)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_PENDING)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_DEFERRED)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_INJECTING)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_CANCELLED)
            | (DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_INJECTED)
            | (DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_FAILED)
            | (DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_DEFERRED)
    )
}

pub(super) fn build_injected_input(
    message: &TeamMessageRecord,
    recipient_agent_id: &str,
) -> Vec<Value> {
    let scope = if message.scope == "broadcast" {
        "broadcast"
    } else {
        "dm"
    };
    let prefix = Value::String(format!(
        "<team_msg kind=\"{}\" sender=\"{}\" team_id=\"{}\">",
        scope, message.sender_agent_id, message.team_id
    ));
    let suffix = Value::String("</team_msg>".to_string());

    let mut input = Vec::new();
    input.push(serde_json::json!({ "type": "text", "text": prefix }));
    if let Value::Array(items) = message.input.clone() {
        input.extend(items);
    }
    if let Value::Array(paths) = &message.image_paths {
        input.extend(paths.iter().filter_map(|path| {
            path.as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|path| {
                    serde_json::json!({
                        "type": "image",
                        "path": path,
                    })
                })
        }));
    }
    input.push(serde_json::json!({
        "type": "text",
        "text": suffix,
        "recipient": recipient_agent_id,
    }));
    input
}

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or(0)
}
