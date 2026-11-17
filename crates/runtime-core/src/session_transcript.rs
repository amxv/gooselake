use serde_json::Value;

pub(crate) fn extract_turn_user_text(input: Option<&Vec<Value>>) -> Option<String> {
    let input = input?;
    let mut lines = Vec::new();
    for item in input {
        if let Some(text) = item
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            lines.push(text.to_string());
            continue;
        }
        if let Some(raw) = item
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            lines.push(raw.to_string());
        }
    }
    if lines.is_empty() {
        return None;
    }
    Some(lines.join("\n\n"))
}

pub(crate) fn extract_assistant_text_from_usage(usage: &Value) -> Option<String> {
    usage
        .get("last_message")
        .or_else(|| usage.get("lastMessage"))
        .or_else(|| usage.get("assistant_text"))
        .or_else(|| usage.get("assistantText"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn append_session_transcript(metadata: &mut Value, role: &str, text: &str) {
    if !metadata.is_object() {
        *metadata = Value::Object(serde_json::Map::new());
    }
    let metadata_object = match metadata {
        Value::Object(object) => object,
        _ => return,
    };
    if !metadata_object.contains_key("session_transcript") {
        if let Some(existing) = metadata_object.remove("codex_transcript") {
            metadata_object.insert("session_transcript".to_string(), existing);
        }
    }
    let entry = metadata_object
        .entry("session_transcript")
        .or_insert_with(|| Value::Array(Vec::new()));
    if !entry.is_array() {
        *entry = Value::Array(Vec::new());
    }
    if let Some(rows) = entry.as_array_mut() {
        rows.push(serde_json::json!({
            "role": role,
            "text": text,
        }));
        if rows.len() > 80 {
            let to_trim = rows.len() - 80;
            rows.drain(0..to_trim);
        }
    }
}

pub fn terminal_session_metadata(
    previous: &serde_json::Value,
    turn: &crate::TurnRecord,
) -> serde_json::Value {
    let mut metadata = previous.clone();
    if matches!(turn.status.as_str(), "completed" | "interrupted") {
        if let Some(text) = extract_turn_user_text(turn.input.as_array()) {
            append_session_transcript(&mut metadata, "user", &text);
        }
        if let Some(text) = turn
            .usage
            .as_ref()
            .and_then(extract_assistant_text_from_usage)
        {
            append_session_transcript(&mut metadata, "assistant", &text);
        }
    }
    metadata
}
pub fn terminal_turn_event_payload(turn: &crate::TurnRecord) -> serde_json::Value {
    let assistant_text = turn
        .usage
        .as_ref()
        .and_then(extract_assistant_text_from_usage);
    serde_json::json!({"status":turn.status,"usage":turn.usage,"error":turn.error,"assistant_text":assistant_text})
}
pub struct StartupTerminalMetadataEvidence<'a> {
    pub turn: &'a crate::TurnRecord,
    pub admission: Option<&'a crate::TurnAdmissionRecord>,
    pub event: &'a crate::RuntimeEventRecord,
}
pub fn startup_completion_metadata_event(
    grant: &crate::StartupReattachGrant,
    current: &crate::StartupAttachmentAuthority,
    session: &crate::SessionRecord,
    terminal: Option<StartupTerminalMetadataEvidence<'_>>,
) -> Result<Option<String>, crate::RuntimeError> {
    use crate::{RuntimeError, RuntimeEventCriticality, RuntimeEventScope};
    let fail = || {
        RuntimeError::Conflict(
            "startup attachment changed without its exact supported terminal metadata receipt"
                .into(),
        )
    };
    let frozen = &grant.intent.authority;
    if session.id != grant.intent.command.session_id
        || current.request.runtime_session_id != session.id
        || current.request.metadata.as_ref() != Some(&session.metadata)
    {
        return Err(fail());
    }
    let mut comparable = current.clone();
    comparable.request.metadata = frozen.request.metadata.clone();
    if comparable != *frozen {
        return Err(fail());
    }
    if current.request.metadata == frozen.request.metadata {
        return Ok(None);
    }
    let before_metadata = frozen.request.metadata.as_ref().ok_or_else(fail)?;
    let witness = grant
        .intent
        .terminal_metadata_witness
        .as_ref()
        .ok_or_else(fail)?;
    let evidence = terminal.ok_or_else(fail)?;
    let before = &witness.turn;
    let after = evidence.turn;
    let event = evidence.event;
    if before.session_id != session.id
        || before.completed_at.is_some()
        || session.active_turn_id.is_some()
        || witness.admission.as_ref() != evidence.admission
        || !matches!(after.status.as_str(), "completed" | "interrupted")
        || after.completed_at.is_none()
    {
        return Err(fail());
    }
    let mut expected_turn = before.clone();
    expected_turn.status = after.status.clone();
    expected_turn.completed_at = after.completed_at;
    expected_turn.usage = after.usage.clone();
    expected_turn.error = after.error.clone();
    if expected_turn != *after {
        return Err(fail());
    }
    let event_id = format!("evt_turn_{}_terminal", before.id);
    let expected_kind = if after.status == "completed" {
        "turn.completed"
    } else {
        "turn.interrupted"
    };
    if event.event_id != event_id
        || event.scope != RuntimeEventScope::Session
        || event.scope_id != session.id
        || event.session_id.as_deref() != Some(session.id.as_str())
        || event.turn_id.as_deref() != Some(before.id.as_str())
        || event.team_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || event.kind != expected_kind
        || event.payload != crate::terminal_turn_event_payload(after)
    {
        return Err(fail());
    }
    let expected_metadata = crate::terminal_session_metadata(before_metadata, after);
    if current.request.metadata.as_ref() != Some(&expected_metadata) {
        return Err(fail());
    }
    Ok(Some(event_id))
}
