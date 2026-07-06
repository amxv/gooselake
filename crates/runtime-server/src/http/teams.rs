use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct TeamCreateInput {
    name: String,
    lead_agent_id: String,
    member_agent_ids: Option<Vec<String>>,
    created_by: Option<String>,
}

pub(super) async fn create_team(
    State(state): State<AppState>,
    Json(input): Json<TeamCreateInput>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .create_team(TeamCreateRequest {
            name: input.name,
            lead_agent_id: input.lead_agent_id,
            member_agent_ids: input.member_agent_ids.unwrap_or_default(),
            created_by: input.created_by,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

pub(super) async fn list_teams(
    State(state): State<AppState>,
) -> Result<Json<Vec<runtime_core::TeamWithMembers>>, ApiError> {
    let teams = state
        .app
        .services
        .team_comms
        .list_teams()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(teams))
}

pub(super) async fn get_team(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .get_team(team_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamJoinInput {
    agent_id: String,
    title: Option<String>,
    added_by: Option<String>,
    creator_agent_id: Option<String>,
    creator_compaction_subscription: Option<String>,
    worktree_id: Option<String>,
}

pub(super) async fn join_team_member(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamJoinInput>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .join_team(TeamJoinRequest {
            team_id,
            agent_id: input.agent_id,
            title: input.title,
            added_by: input.added_by,
            creator_agent_id: input.creator_agent_id,
            creator_compaction_subscription: input.creator_compaction_subscription,
            worktree_id: input.worktree_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamMemberSpawnInput {
    source_session_id: String,
    provider: Option<String>,
    model: Option<String>,
    title: Option<String>,
    prompt: Option<String>,
    permission_mode: Option<String>,
    metadata: Option<Value>,
    worktree: Option<TeamMemberSpawnWorktreeInput>,
    creator_agent_id: Option<String>,
    creator_compaction_subscription: Option<String>,
}

pub(super) async fn spawn_team_member(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamMemberSpawnInput>,
) -> Result<Json<TeamMemberSpawnResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .spawn_team_member(TeamMemberSpawnRequest {
            team_id,
            source_session_id: input.source_session_id,
            provider: input.provider,
            model: input.model,
            title: input.title,
            prompt: input.prompt,
            permission_mode: input.permission_mode,
            metadata: input.metadata,
            worktree: input.worktree,
            creator_agent_id: input.creator_agent_id,
            creator_compaction_subscription: input.creator_compaction_subscription,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub(super) async fn remove_team_member(
    State(state): State<AppState>,
    Path((team_id, agent_id)): Path<(String, String)>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let removed_agent_id = agent_id.clone();
    let team_id_for_cleanup = team_id.clone();
    let team = state
        .app
        .services
        .team_comms
        .remove_team_member(TeamRemoveMemberRequest { team_id, agent_id })
        .await
        .map_err(ApiError::from)?;
    // Cleanup is best effort by policy; membership removal must stand even if cleanup fails.
    let _ = state
        .app
        .services
        .worktrees
        .on_member_removed(WorktreeMemberRemovedRequest {
            team_id: team_id_for_cleanup,
            agent_id: removed_agent_id,
            removed_by: None,
        })
        .await;
    Ok(Json(team))
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamSetLeadInput {
    lead_agent_id: String,
}

pub(super) async fn set_team_lead(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamSetLeadInput>,
) -> Result<Json<runtime_core::TeamWithMembers>, ApiError> {
    let team = state
        .app
        .services
        .team_comms
        .set_team_lead(TeamSetLeadRequest {
            team_id,
            lead_agent_id: input.lead_agent_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(team))
}

pub(super) async fn delete_team(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .app
        .services
        .team_comms
        .delete_team(team_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamDirectInput {
    sender_agent_id: String,
    recipient_agent_id: String,
    input: Value,
    image_paths: Option<Vec<String>>,
    priority: Option<String>,
    policy: Option<String>,
    correlation_id: Option<String>,
    reply_to_message_id: Option<String>,
    idempotency_key: Option<String>,
}

pub(super) async fn send_team_direct(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamDirectInput>,
) -> Result<Json<runtime_core::TeamMessageAck>, ApiError> {
    let ack = state
        .app
        .services
        .team_comms
        .send_direct(TeamSendDirectRequest {
            team_id,
            sender_agent_id: input.sender_agent_id,
            recipient_agent_id: input.recipient_agent_id,
            input: input.input,
            image_paths: input.image_paths.unwrap_or_default(),
            priority: input.priority.unwrap_or_else(|| "normal".to_string()),
            policy: input
                .policy
                .unwrap_or_else(|| "non_interrupting".to_string()),
            correlation_id: input.correlation_id,
            reply_to_message_id: input.reply_to_message_id,
            idempotency_key: input.idempotency_key,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ack))
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamBroadcastInput {
    sender_agent_id: String,
    input: Value,
    image_paths: Option<Vec<String>>,
    priority: Option<String>,
    policy: Option<String>,
    include_sender: Option<bool>,
    correlation_id: Option<String>,
    idempotency_key: Option<String>,
}

pub(super) async fn send_team_broadcast(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Json(input): Json<TeamBroadcastInput>,
) -> Result<Json<runtime_core::TeamMessageAck>, ApiError> {
    let ack = state
        .app
        .services
        .team_comms
        .broadcast(TeamBroadcastRequest {
            team_id,
            sender_agent_id: input.sender_agent_id,
            input: input.input,
            image_paths: input.image_paths.unwrap_or_default(),
            priority: input.priority.unwrap_or_else(|| "normal".to_string()),
            policy: input
                .policy
                .unwrap_or_else(|| "non_interrupting".to_string()),
            include_sender: input.include_sender.unwrap_or(false),
            correlation_id: input.correlation_id,
            idempotency_key: input.idempotency_key,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ack))
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamListMessagesQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

pub(super) async fn list_team_messages(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<TeamListMessagesQuery>,
) -> Result<Json<runtime_core::TeamListMessagesResponse>, ApiError> {
    let response = state
        .app
        .services
        .team_comms
        .list_messages(TeamListMessagesRequest {
            team_id,
            cursor: query.cursor,
            limit: query.limit,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamDeliveriesQuery {
    message_id: Option<String>,
    recipient_agent_id: Option<String>,
}

pub(super) async fn list_team_deliveries(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<TeamDeliveriesQuery>,
) -> Result<Json<Vec<TeamDeliveryRecord>>, ApiError> {
    let deliveries = state
        .app
        .services
        .team_comms
        .get_deliveries(TeamGetDeliveriesRequest {
            team_id,
            message_id: query.message_id,
            recipient_agent_id: query.recipient_agent_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(deliveries))
}

pub(super) async fn retry_team_delivery(
    State(state): State<AppState>,
    Path((team_id, delivery_id)): Path<(String, String)>,
) -> Result<Json<TeamDeliveryRecord>, ApiError> {
    let delivery = state
        .app
        .services
        .team_comms
        .retry_delivery(TeamRetryDeliveryRequest {
            team_id,
            delivery_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(delivery))
}

pub(super) async fn cancel_team_message(
    State(state): State<AppState>,
    Path((team_id, message_id)): Path<(String, String)>,
) -> Result<Json<Vec<TeamDeliveryRecord>>, ApiError> {
    let rows = state
        .app
        .services
        .team_comms
        .cancel_message(TeamCancelMessageRequest {
            team_id,
            message_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamSnapshotQuery {
    message_cursor: Option<String>,
    message_limit: Option<usize>,
    include_delivery_map: Option<bool>,
    delivery_recipient_filter: Option<String>,
}

pub(super) async fn get_team_view_snapshot(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<TeamSnapshotQuery>,
) -> Result<Json<runtime_core::TeamViewSnapshotResponse>, ApiError> {
    let snapshot = state
        .app
        .services
        .team_comms
        .get_view_snapshot(TeamViewSnapshotRequest {
            team_id,
            message_cursor: query.message_cursor,
            message_limit: query.message_limit,
            include_delivery_map: query.include_delivery_map,
            delivery_recipient_filter: query.delivery_recipient_filter,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

pub(super) async fn replay_team_events(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .app
        .services
        .team_comms
        .replay_team_events(
            team_id.as_str(),
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

pub(super) async fn stream_team_events(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let _ = state
        .app
        .services
        .team_comms
        .get_team(team_id.as_str())
        .await
        .map_err(ApiError::from)?;

    let last_event_id = parse_last_event_id_header(&headers)?;
    let cursor = query.after_seq.or(last_event_id);
    let replay_page_limit = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    let mut replay_events = Vec::new();
    let mut replay_cursor = cursor;

    loop {
        let page = state
            .app
            .services
            .team_comms
            .replay_team_events(team_id.as_str(), replay_cursor, replay_page_limit)
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        replay_cursor = page.last().map(|event| event.seq);
        let page_len = page.len();
        replay_events.extend(page);
        if page_len < replay_page_limit {
            break;
        }
    }

    let replay_high_watermark_seq = replay_cursor.or(cursor).unwrap_or(0);
    let replay_stream = tokio_stream::iter(replay_events.into_iter().filter_map(|event| {
        let payload = serde_json::to_string(&event).ok()?;
        Some(Ok(Event::default()
            .id(event.seq.to_string())
            .event(event.kind)
            .data(payload)))
    }));

    let (tx, rx) = mpsc::channel::<Result<Event, std::convert::Infallible>>(128);
    let team_id_for_live = team_id.clone();
    let store = state.app.services.store.clone();
    tokio::spawn(async move {
        let mut cursor = replay_high_watermark_seq;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let page = match store.list_runtime_events(
                Some((RuntimeEventScope::Team, team_id_for_live.as_str())),
                Some(cursor),
                replay_page_limit,
            ) {
                Ok(page) => page,
                Err(_) => continue,
            };
            if page.is_empty() {
                continue;
            }
            for event in page {
                cursor = cursor.max(event.seq);
                let payload = match serde_json::to_string(&event) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                let sse = Event::default()
                    .id(event.seq.to_string())
                    .event(event.kind)
                    .data(payload);
                if tx.send(Ok(sse)).await.is_err() {
                    return;
                }
            }
        }
    });

    let live_stream = ReceiverStream::new(rx);
    let stream = replay_stream.chain(live_stream);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

pub(super) async fn interrupt_all_team_turns(
    State(state): State<AppState>,
    Path(team_id): Path<String>,
) -> Result<Json<runtime_core::TeamInterruptAllResponse>, ApiError> {
    let response = state
        .app
        .services
        .team_comms
        .interrupt_all_team_turns(TeamInterruptAllRequest { team_id })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}
