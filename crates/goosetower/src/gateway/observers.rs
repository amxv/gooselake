use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::protocol::generated::goosetower::v1::realtime_envelope::Payload;
use crate::protocol::generated::goosetower::v1::{CursorVector, RealtimeEnvelope};

const MAX_OBSERVER_COLLECTION: usize = 128;
const MAX_OBSERVER_STRING: usize = 2_048;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServedFrameDebug {
    pub capture_index: u64,
    pub connection_id: String,
    pub gateway_seq: u64,
    pub message_kind: i32,
    pub payload_kind: String,
    pub view_kind: Option<String>,
    pub entity_id: Option<String>,
    pub cursor: Option<CursorVectorDebug>,
    pub body: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorVectorDebug {
    pub gateway_seq: u64,
    pub sources: Vec<SourceCursorDebug>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCursorDebug {
    pub source_id: String,
    pub source_epoch: String,
    pub source_seq: u64,
}

impl ServedFrameDebug {
    pub fn from_envelope(
        capture_index: u64,
        connection_id: &str,
        envelope: &RealtimeEnvelope,
    ) -> Self {
        let (payload_kind, view_kind, entity_id, cursor, body) = match envelope.payload.as_ref() {
            Some(Payload::Snapshot(snapshot)) => (
                "snapshot",
                Some(snapshot.view_kind.clone()),
                None,
                snapshot.cursor.as_ref(),
                decode_body(&snapshot.body),
            ),
            Some(Payload::Patch(patch)) => (
                "patch",
                Some(patch.view_kind.clone()),
                patch.entity.as_ref().map(|entity| entity.entity_id.clone()),
                patch.cursor.as_ref(),
                decode_body(&patch.body),
            ),
            Some(Payload::Hello(_)) => ("hello", None, None, None, None),
            Some(Payload::Ping(_)) => ("ping", None, None, None, None),
            Some(Payload::Pong(_)) => ("pong", None, None, None, None),
            Some(Payload::Resume(_)) => ("resume", None, None, None, None),
            Some(Payload::Subscribe(_)) => ("subscribe", None, None, None, None),
            Some(Payload::Unsubscribe(_)) => ("unsubscribe", None, None, None, None),
            Some(Payload::Ack(_)) => ("ack", None, None, None, None),
            Some(Payload::Event(_)) => ("event", None, None, None, None),
            Some(Payload::Command(_)) => ("command", None, None, None, None),
            Some(Payload::CommandAccepted(_)) => ("command_accepted", None, None, None, None),
            Some(Payload::CommandRejected(_)) => ("command_rejected", None, None, None, None),
            Some(Payload::CommandDuplicate(_)) => ("command_duplicate", None, None, None, None),
            Some(Payload::AuthRefresh(_)) => ("auth_refresh", None, None, None, None),
            Some(Payload::AuthExpiring(_)) => ("auth_expiring", None, None, None, None),
            Some(Payload::AuthRefreshed(_)) => ("auth_refreshed", None, None, None, None),
            Some(Payload::ConnectionDegraded(_)) => ("connection_degraded", None, None, None, None),
            Some(Payload::SourceGapDetected(_)) => ("source_gap_detected", None, None, None, None),
            Some(Payload::SourceGapFilled(_)) => ("source_gap_filled", None, None, None, None),
            Some(Payload::SourceSnapshotResync(_)) => {
                ("source_snapshot_resync", None, None, None, None)
            }
            Some(Payload::Error(_)) => ("error", None, None, None, None),
            None => ("none", None, None, None, None),
        };
        Self {
            capture_index,
            connection_id: connection_id.to_string(),
            gateway_seq: envelope.gateway_seq,
            message_kind: envelope.message_kind,
            payload_kind: payload_kind.to_string(),
            view_kind,
            entity_id,
            cursor: cursor.map(cursor_debug),
            body,
        }
    }
}

fn cursor_debug(cursor: &CursorVector) -> CursorVectorDebug {
    CursorVectorDebug {
        gateway_seq: cursor.gateway_seq,
        sources: cursor
            .sources
            .iter()
            .take(MAX_OBSERVER_COLLECTION)
            .map(|source| SourceCursorDebug {
                source_id: source.source_id.clone(),
                source_epoch: source.source_epoch.clone(),
                source_seq: source.source_seq,
            })
            .collect(),
    }
}

fn decode_body(body: &[u8]) -> Option<Value> {
    if body.is_empty() {
        return None;
    }
    let parsed = serde_json::from_slice(body).unwrap_or_else(|_| json!("[binary omitted]"));
    Some(redact_debug_value(&parsed))
}

pub fn redact_debug_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .take(MAX_OBSERVER_COLLECTION)
                .map(|(key, value)| {
                    let normalized = key.to_ascii_lowercase();
                    let secret = [
                        "authorization",
                        "bearer",
                        "token",
                        "ticket",
                        "password",
                        "credential",
                        "cookie",
                        "csrf",
                        "secret",
                        "raw_image",
                        "image_data",
                    ]
                    .iter()
                    .any(|needle| normalized.contains(needle));
                    (
                        key.clone(),
                        if secret {
                            Value::String("[redacted]".to_string())
                        } else {
                            redact_debug_value(value)
                        },
                    )
                })
                .collect::<Map<_, _>>(),
        ),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .take(MAX_OBSERVER_COLLECTION)
                .map(redact_debug_value)
                .collect(),
        ),
        Value::String(value) if contains_secret_value(value) => {
            Value::String("[redacted]".to_string())
        }
        Value::String(value) => Value::String(value.chars().take(MAX_OBSERVER_STRING).collect()),
        other => other.clone(),
    }
}

fn contains_secret_value(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    normalized.contains("bearer ")
        || normalized.contains("data:image/")
        || ["token=", "ticket=", "password=", "cookie=", "csrf="]
            .iter()
            .any(|needle| normalized.contains(needle))
}
