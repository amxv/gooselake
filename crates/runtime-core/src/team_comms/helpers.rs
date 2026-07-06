use std::collections::HashMap;

use serde_json::Value;

use crate::{RuntimeError, TeamMemberRecord, TeamMessageRecord};

use super::{
    DELIVERY_POLICY_IMMEDIATE_INTERRUPT, DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY,
    DELIVERY_POLICY_NON_INTERRUPTING, DELIVERY_POLICY_START_NEW_TURN_ONLY,
    DELIVERY_STATUS_CANCELLED, DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_FAILED,
    DELIVERY_STATUS_INJECTED, DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_PENDING,
};

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
