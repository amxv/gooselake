use runtime_core::{ProviderKind, RuntimeError, SessionRecord, TeamMessageRecord, TeamWithMembers};
use serde_json::{json, Value};

#[derive(Debug)]
pub(super) struct TeamToolFailure {
    pub(super) code: &'static str,
    pub(super) message: String,
}

impl TeamToolFailure {
    pub(super) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(super) fn from_runtime(error: RuntimeError) -> Self {
        let code = team_tool_error_code_for_runtime(&error);
        Self::new(code, error.to_string())
    }
}

pub(super) fn team_tool_error_code_for_runtime(error: &RuntimeError) -> &'static str {
    match error {
        RuntimeError::InvalidState(_) => "unauthorized",
        RuntimeError::NotFound(_) => "not_found",
        RuntimeError::Unsupported(_) => "feature_disabled",
        _ => "tool_failed",
    }
}

pub(super) fn team_tool_error(code: impl AsRef<str>, message: impl AsRef<str>) -> Value {
    json!({
        "ok": false,
        "error": {
            "code": code.as_ref(),
            "message": message.as_ref(),
        }
    })
}

pub(super) fn ensure_team_member(
    team: &TeamWithMembers,
    caller_session_id: &str,
) -> Result<(), TeamToolFailure> {
    if team
        .members
        .iter()
        .any(|member| member.agent_id == caller_session_id)
    {
        return Ok(());
    }
    Err(TeamToolFailure::new(
        "unauthorized",
        format!(
            "agent {} is not a member of team {}",
            caller_session_id, team.team.id
        ),
    ))
}

pub(super) fn reject_image_paths_for_provider(
    provider: &str,
    tool_name: &str,
) -> Result<(), TeamToolFailure> {
    match ProviderKind::from_str(provider) {
        Some(ProviderKind::Acp) => Err(TeamToolFailure::new(
            "unsupported_provider_images",
            format!(
                "{tool_name} image_paths are not supported for ACP provider sessions because ACP image attachment delivery is not modeled by this runtime yet"
            ),
        )),
        _ => Ok(()),
    }
}

pub(super) fn status_state_for_session(session: Option<&SessionRecord>) -> &'static str {
    match session {
        Some(session) if session.status == "failed" => "errored",
        Some(session) if session.active_turn_id.is_some() => "working",
        Some(session) if session.status == "closed" => "closed",
        Some(_) => "idle",
        None => "unknown",
    }
}

pub(super) fn latest_message_for_member<'a>(
    agent_id: &str,
    messages: &'a [TeamMessageRecord],
) -> Option<&'a TeamMessageRecord> {
    messages
        .iter()
        .filter(|message| {
            message.sender_agent_id == agent_id
                || message
                    .recipient_agent_ids
                    .as_array()
                    .map(|recipients| {
                        recipients
                            .iter()
                            .any(|value| value.as_str() == Some(agent_id))
                    })
                    .unwrap_or(false)
        })
        .max_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        })
}

pub(super) fn member_last_message_output(message: &TeamMessageRecord) -> Value {
    json!({
        "message_id": message.id,
        "scope": message.scope,
        "sender_agent_id": message.sender_agent_id,
        "created_at_ms": message.created_at,
        "text": message_text(&message.input),
    })
}

fn message_text(input: &Value) -> Option<String> {
    if let Some(text) = input.as_str() {
        return Some(text.to_string());
    }
    if let Some(items) = input.as_array() {
        let text = items
            .iter()
            .filter_map(|item| {
                if let Some(text) = item.as_str() {
                    return Some(text);
                }
                item.get("text").and_then(Value::as_str)
            })
            .collect::<Vec<_>>()
            .join("\n");
        return (!text.is_empty()).then_some(text);
    }
    input
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_string)
}
