use std::path::{Path, PathBuf};

use runtime_core::{ProviderTurnStatus, RuntimeError};
use serde_json::{json, Map, Value};

pub(crate) fn build_native_input(input: &[Value]) -> Result<Vec<Value>, RuntimeError> {
    let mut output = Vec::with_capacity(input.len());
    for item in input {
        if let Some(text) = item.as_str() {
            output.push(json!({"type": "text", "text": text}));
            continue;
        }
        let Some(object) = item.as_object() else {
            return Err(RuntimeError::ProtocolViolation(
                "Codex input item must be an object or text string".to_string(),
            ));
        };
        let item_type = object.get("type").and_then(Value::as_str).ok_or_else(|| {
            RuntimeError::ProtocolViolation("Codex input item missing type".to_string())
        })?;
        match item_type {
            "text" => {
                let text = object.get("text").and_then(Value::as_str).ok_or_else(|| {
                    RuntimeError::ProtocolViolation("Codex text input missing text".to_string())
                })?;
                output.push(json!({"type": "text", "text": text}));
            }
            "localImage" | "local_image" => {
                let path = object.get("path").and_then(Value::as_str).ok_or_else(|| {
                    RuntimeError::ProtocolViolation(
                        "Codex local-image input missing path".to_string(),
                    )
                })?;
                output.push(json!({"type": "localImage", "path": path}));
            }
            "image" => {
                if let Some(path) = object.get("path").and_then(Value::as_str) {
                    output.push(json!({"type": "localImage", "path": path}));
                } else {
                    let mut image = Map::new();
                    image.insert("type".to_string(), json!("image"));
                    if let Some(url) = object.get("url").and_then(Value::as_str) {
                        image.insert("url".to_string(), json!(url));
                    }
                    if let Some(file_id) = object.get("fileId").and_then(Value::as_str) {
                        image.insert("fileId".to_string(), json!(file_id));
                    }
                    if image.len() == 1 {
                        return Err(RuntimeError::ProtocolViolation(
                            "Codex image input requires url, fileId, or path".to_string(),
                        ));
                    }
                    output.push(Value::Object(image));
                }
            }
            "skill" => {
                let name = object.get("name").and_then(Value::as_str).ok_or_else(|| {
                    RuntimeError::ProtocolViolation("Codex skill input missing name".to_string())
                })?;
                let path = object.get("path").and_then(Value::as_str).ok_or_else(|| {
                    RuntimeError::ProtocolViolation("Codex skill input missing path".to_string())
                })?;
                output.push(json!({"type": "skill", "name": name, "path": path}));
            }
            other => {
                return Err(RuntimeError::ProtocolViolation(format!(
                "unsupported Codex structured input type {other}; refusing to flatten it into text"
            )))
            }
        }
    }
    if output.is_empty() {
        return Err(RuntimeError::ProtocolViolation(
            "Codex turn input must not be empty".to_string(),
        ));
    }
    Ok(output)
}

fn normalized_permission_mode(permission_mode: Option<&str>) -> Option<String> {
    let Some(permission_mode) = permission_mode
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return None;
    };
    Some(permission_mode.to_ascii_lowercase().replace('-', "_"))
}

pub(super) fn apply_thread_permission_mode(params: &mut Value, permission_mode: Option<&str>) {
    let Some(permission_mode) = normalized_permission_mode(permission_mode) else {
        return;
    };
    match permission_mode.as_str() {
        "default" => {}
        "require_approval" => {
            params["approvalPolicy"] = json!("on-request");
        }
        "workspace_write" => {
            params["sandbox"] = json!("workspace-write");
        }
        "read_only" => {
            params["sandbox"] = json!("read-only");
        }
        "full_auto" => {
            params["approvalPolicy"] = json!("on-request");
            params["sandbox"] = json!("workspace-write");
        }
        "danger_full_access" => {
            params["approvalPolicy"] = json!("never");
            params["sandbox"] = json!("danger-full-access");
        }
        _ => {}
    }
}

pub(super) fn apply_turn_permission_mode(params: &mut Value, permission_mode: Option<&str>) {
    let Some(permission_mode) = normalized_permission_mode(permission_mode) else {
        return;
    };
    match permission_mode.as_str() {
        "default" => {}
        "require_approval" => {
            params["approvalPolicy"] = json!("on-request");
        }
        "workspace_write" => {
            params["sandboxPolicy"] = json!({"type": "workspaceWrite"});
        }
        "read_only" => {
            params["sandboxPolicy"] = json!({"type": "readOnly"});
        }
        "full_auto" => {
            params["approvalPolicy"] = json!("on-request");
            params["sandboxPolicy"] = json!({"type": "workspaceWrite"});
        }
        "danger_full_access" => {
            params["approvalPolicy"] = json!("never");
            params["sandboxPolicy"] = json!({"type": "dangerFullAccess"});
        }
        _ => {}
    }
}

pub(super) fn extract_thread_id(result: &Value) -> Option<String> {
    result
        .get("thread")
        .and_then(|thread| thread.get("id"))
        .and_then(Value::as_str)
        .or_else(|| result.get("threadId").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn extract_turn_id(result: &Value) -> Option<String> {
    result
        .get("turn")
        .and_then(|turn| turn.get("id"))
        .and_then(Value::as_str)
        .or_else(|| result.get("turnId").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn parse_turn_status(status: Option<&str>) -> ProviderTurnStatus {
    match status.unwrap_or("failed") {
        "completed" => ProviderTurnStatus::Completed,
        "interrupted" => ProviderTurnStatus::Interrupted,
        "inProgress" | "in_progress" => ProviderTurnStatus::InProgress,
        _ => ProviderTurnStatus::Failed,
    }
}

pub(super) fn terminal_turn_from_thread_read<'a>(
    response: &'a Value,
    native_turn_id: &str,
) -> Option<(ProviderTurnStatus, Option<Value>, Option<Value>)> {
    let thread = response.get("thread").unwrap_or(response);
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .or_else(|| response.get("turns").and_then(Value::as_array))?;
    let turn = turns
        .iter()
        .find(|turn| turn.get("id").and_then(Value::as_str) == Some(native_turn_id))?;
    let raw_status = turn.get("status")?;
    let status = match raw_status {
        Value::String(value) => value.as_str(),
        Value::Object(object) => object.get("type")?.as_str()?,
        _ => return None,
    };
    let status = match status {
        "completed" => ProviderTurnStatus::Completed,
        "interrupted" | "cancelled" | "canceled" => ProviderTurnStatus::Interrupted,
        "failed" => ProviderTurnStatus::Failed,
        _ => return None,
    };
    let usage = turn
        .get("usage")
        .or_else(|| turn.get("tokenUsage"))
        .cloned();
    let error = turn.get("error").filter(|value| !value.is_null()).cloned();
    Some((status, usage, error))
}

pub(super) fn turn_ids_from_thread_read(response: &Value) -> Result<Vec<String>, RuntimeError> {
    let thread = response.get("thread").unwrap_or(response);
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .or_else(|| response.get("turns").and_then(Value::as_array))
        .ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "Codex thread/read(includeTurns=true) response missing turns".to_string(),
            )
        })?;
    turns
        .iter()
        .map(|turn| {
            turn.get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation(
                        "Codex thread/read contained a turn without an id".to_string(),
                    )
                })
        })
        .collect()
}

pub(super) fn path_component(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            output.push(byte as char);
        } else {
            output.push('_');
            output.push_str(format!("{byte:02x}").as_str());
        }
    }
    if output.is_empty() {
        "session".to_string()
    } else {
        output
    }
}

pub(super) fn absolutize_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path.to_path_buf(),
    }
}
