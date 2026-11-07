use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::workspace::{
    normalize_idempotency_key, normalized_json_hash, opaque_id, unix_time_ms, OperationActor,
};
use crate::RuntimeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentMessageContextKind {
    WorkspaceTeam,
    GlobalDirect,
    LegacyTeam,
}

impl AgentMessageContextKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceTeam => "workspace_team",
            Self::GlobalDirect => "global_direct",
            Self::LegacyTeam => "legacy_team",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "workspace_team" => Some(Self::WorkspaceTeam),
            "global_direct" => Some(Self::GlobalDirect),
            "legacy_team" => Some(Self::LegacyTeam),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMessageRecord {
    pub id: String,
    pub scope: String,
    pub context_kind: AgentMessageContextKind,
    pub workspace_id: Option<String>,
    pub legacy_team_id: Option<String>,
    pub sender_agent_id: String,
    pub recipient_agent_ids: Vec<String>,
    pub input: Value,
    #[serde(default)]
    pub image_paths: Vec<String>,
    pub priority: String,
    pub policy: String,
    pub correlation_id: Option<String>,
    pub reply_to_message_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDeliveryRecord {
    pub id: String,
    pub message_id: String,
    pub recipient_agent_id: String,
    pub provider: String,
    pub status: String,
    pub effective_policy: Option<String>,
    pub injection_strategy: Option<String>,
    pub injected_turn_id: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMessageAck {
    pub message: AgentMessageRecord,
    pub deliveries: Vec<AgentDeliveryRecord>,
    pub disposition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDirectMessageRequest {
    pub sender_agent_id: String,
    pub recipient_agent_id: String,
    pub input: Value,
    #[serde(default)]
    pub image_paths: Vec<String>,
    pub priority: String,
    pub policy: String,
    pub correlation_id: Option<String>,
    pub reply_to_message_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentBroadcastMessageRequest {
    pub sender_agent_id: String,
    pub input: Value,
    #[serde(default)]
    pub image_paths: Vec<String>,
    pub priority: String,
    pub policy: String,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentMessageListRequest {
    pub workspace_id: Option<String>,
    pub sender_agent_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentMessageListResponse {
    pub messages: Vec<AgentMessageRecord>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentDeliveryListRequest {
    pub message_id: Option<String>,
    pub recipient_agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRetryDeliveryRequest {
    pub delivery_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCancelMessageRequest {
    pub message_id: String,
}

/// Trusted operator intent. The receipt and all delivery transitions are
/// committed in the same transaction, before a response can be returned.
#[derive(Debug, Clone)]
pub struct AgentCancelMessageMutationCommand {
    pub operation_id: String,
    pub message_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCancelMessageMutationResponse {
    pub operation_id: String,
    pub deliveries: Vec<AgentDeliveryRecord>,
}

/// Exact durable receipt for re-admitting a failed/deferred delivery. The
/// receipt describes queue admission, not a native provider-delivery result.
#[derive(Debug, Clone)]
pub struct AgentRetryDeliveryMutationCommand {
    pub operation_id: String,
    pub delivery_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRetryDeliveryMutationResponse {
    pub operation_id: String,
    pub delivery: AgentDeliveryRecord,
}

pub fn prepare_agent_delivery_retry(
    delivery_id: &str,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<AgentRetryDeliveryMutationCommand, RuntimeError> {
    let delivery_id = delivery_id.trim();
    if delivery_id.is_empty() || actor.identifier.trim().is_empty() {
        return Err(RuntimeError::InvalidState(
            "delivery and operator identity are required".into(),
        ));
    }
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = serde_json::json!({"delivery_id": delivery_id});
    Ok(AgentRetryDeliveryMutationCommand {
        operation_id: opaque_id("op"),
        delivery_id: delivery_id.to_string(),
        actor,
        idempotency_key,
        normalized_request_hash: normalized_json_hash(&normalized_request)?,
        normalized_request,
        requested_at: unix_time_ms()?,
    })
}

pub fn prepare_agent_message_cancellation(
    message_id: &str,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<AgentCancelMessageMutationCommand, RuntimeError> {
    let message_id = message_id.trim();
    if message_id.is_empty() || actor.identifier.trim().is_empty() {
        return Err(RuntimeError::InvalidState(
            "message and operator identity are required".into(),
        ));
    }
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = serde_json::json!({"message_id": message_id});
    Ok(AgentCancelMessageMutationCommand {
        operation_id: opaque_id("op"),
        message_id: message_id.to_string(),
        actor,
        idempotency_key,
        normalized_request_hash: normalized_json_hash(&normalized_request)?,
        normalized_request,
        requested_at: unix_time_ms()?,
    })
}
