use super::*;
use runtime_core::{
    AgentBroadcastMessageRequest, AgentCancelMessageRequest, AgentDeliveryListRequest,
    AgentDirectMessageRequest, AgentMessageListRequest, AgentRetryDeliveryRequest,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AgentMessageCreateInput {
    mode: String,
    sender_agent_id: String,
    recipient_agent_id: Option<String>,
    input: Value,
    image_paths: Option<Vec<String>>,
    priority: Option<String>,
    policy: Option<String>,
    correlation_id: Option<String>,
    reply_to_message_id: Option<String>,
}

pub(super) async fn create_agent_message(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<AgentMessageCreateInput>,
) -> Result<Json<runtime_core::AgentMessageAck>, ApiError> {
    let idempotency_key = parse_idempotency_key(&headers)?;
    let mode = input.mode.trim().to_ascii_lowercase();
    let priority = input.priority.unwrap_or_else(|| "normal".to_string());
    let policy = input
        .policy
        .unwrap_or_else(|| "non_interrupting".to_string());
    let image_paths = input.image_paths.unwrap_or_default();
    let ack = match mode.as_str() {
        "direct" => {
            let recipient_agent_id = input.recipient_agent_id.ok_or_else(|| {
                ApiError::bad_request(
                    "recipient_agent_id is required for direct messages".to_string(),
                )
            })?;
            state
                .app
                .services
                .team_comms
                .send_agent_direct(AgentDirectMessageRequest {
                    sender_agent_id: input.sender_agent_id,
                    recipient_agent_id,
                    input: input.input,
                    image_paths,
                    priority,
                    policy,
                    correlation_id: input.correlation_id,
                    reply_to_message_id: input.reply_to_message_id,
                    idempotency_key,
                })
                .await?
        }
        "broadcast" => {
            if input.recipient_agent_id.is_some() {
                return Err(ApiError::bad_request(
                    "recipient_agent_id is not allowed for broadcast messages".to_string(),
                ));
            }
            if input.reply_to_message_id.is_some() {
                return Err(ApiError::bad_request(
                    "reply_to_message_id is not allowed for broadcast messages".to_string(),
                ));
            }
            state
                .app
                .services
                .team_comms
                .broadcast_workspace(AgentBroadcastMessageRequest {
                    sender_agent_id: input.sender_agent_id,
                    input: input.input,
                    image_paths,
                    priority,
                    policy,
                    correlation_id: input.correlation_id,
                    idempotency_key,
                })
                .await?
        }
        _ => {
            return Err(ApiError::bad_request(
                "message mode must be direct or broadcast".to_string(),
            ))
        }
    };
    Ok(Json(ack))
}

#[derive(Debug, Deserialize)]
pub(super) struct AgentMessageListQuery {
    workspace_id: Option<String>,
    sender_agent_id: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

pub(super) async fn list_agent_messages(
    State(state): State<AppState>,
    Query(query): Query<AgentMessageListQuery>,
) -> Result<Json<runtime_core::AgentMessageListResponse>, ApiError> {
    Ok(Json(
        state
            .app
            .services
            .team_comms
            .list_agent_messages(AgentMessageListRequest {
                workspace_id: query.workspace_id,
                sender_agent_id: query.sender_agent_id,
                cursor: query.cursor,
                limit: query.limit,
            })
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
pub(super) struct AgentDeliveryListQuery {
    recipient_agent_id: Option<String>,
}

pub(super) async fn list_agent_deliveries(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
    Query(query): Query<AgentDeliveryListQuery>,
) -> Result<Json<Vec<runtime_core::AgentDeliveryRecord>>, ApiError> {
    Ok(Json(
        state
            .app
            .services
            .team_comms
            .get_agent_deliveries(AgentDeliveryListRequest {
                message_id: Some(message_id),
                recipient_agent_id: query.recipient_agent_id,
            })
            .await?,
    ))
}

pub(super) async fn retry_agent_delivery(
    State(state): State<AppState>,
    Path(delivery_id): Path<String>,
) -> Result<Json<runtime_core::AgentDeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .app
            .services
            .team_comms
            .retry_agent_delivery(AgentRetryDeliveryRequest { delivery_id })
            .await?,
    ))
}

pub(super) async fn cancel_agent_message(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
) -> Result<Json<Vec<runtime_core::AgentDeliveryRecord>>, ApiError> {
    Ok(Json(
        state
            .app
            .services
            .team_comms
            .cancel_agent_message(AgentCancelMessageRequest { message_id })
            .await?,
    ))
}
