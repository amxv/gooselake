use serde_json::Value;

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or(0)
}

pub(super) fn is_terminal_turn_status(status: &str) -> bool {
    matches!(status, "completed" | "interrupted" | "failed")
}

pub(super) fn extract_turn_user_text(input: Option<&Vec<Value>>) -> Option<String> {
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

pub(super) fn extract_assistant_text_from_usage(usage: &Value) -> Option<String> {
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

pub(super) fn append_session_transcript(metadata: &mut Value, role: &str, text: &str) {
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
