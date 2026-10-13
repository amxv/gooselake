use crate::{TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord, TeamRecord};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvokeRequest {
    pub namespace: Option<String>,
    pub tool_name: String,
    pub caller_session_id: String,
    pub invocation_id: Option<String>,
    pub args: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamWithMembers {
    pub team: TeamRecord,
    pub members: Vec<TeamMemberRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamCreateRequest {
    pub name: String,
    pub lead_agent_id: String,
    #[serde(default)]
    pub member_agent_ids: Vec<String>,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamJoinRequest {
    pub team_id: String,
    pub agent_id: String,
    pub title: Option<String>,
    pub added_by: Option<String>,
    pub creator_agent_id: Option<String>,
    pub creator_compaction_subscription: Option<String>,
    pub worktree_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamSetLeadRequest {
    pub team_id: String,
    pub lead_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamRemoveMemberRequest {
    pub team_id: String,
    pub agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamInterruptAllRequest {
    pub team_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamInterruptAllResponse {
    pub team_id: String,
    pub interrupted_session_ids: Vec<String>,
    pub skipped_session_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamSendDirectRequest {
    pub team_id: String,
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
pub struct TeamBroadcastRequest {
    pub team_id: String,
    pub sender_agent_id: String,
    pub input: Value,
    #[serde(default)]
    pub image_paths: Vec<String>,
    pub priority: String,
    pub policy: String,
    pub include_sender: bool,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMessageAck {
    pub message: TeamMessageRecord,
    pub deliveries: Vec<TeamDeliveryRecord>,
    pub disposition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamListMessagesRequest {
    pub team_id: String,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamListMessagesResponse {
    pub messages: Vec<TeamMessageRecord>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamGetDeliveriesRequest {
    pub team_id: String,
    pub message_id: Option<String>,
    pub recipient_agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamRetryDeliveryRequest {
    pub team_id: String,
    pub delivery_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamCancelMessageRequest {
    pub team_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamViewSnapshotRequest {
    pub team_id: String,
    pub message_cursor: Option<String>,
    pub message_limit: Option<usize>,
    pub include_delivery_map: Option<bool>,
    pub delivery_recipient_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamViewSnapshotResponse {
    pub team: TeamWithMembers,
    pub messages: Vec<TeamMessageRecord>,
    pub deliveries_by_message_id: BTreeMap<String, Vec<TeamDeliveryRecord>>,
    pub next_message_cursor: Option<String>,
    pub snapshot_at: i64,
}
