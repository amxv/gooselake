use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::{
    AgentBroadcastMessageRequest, AgentCancelMessageRequest, AgentDeliveryListRequest,
    AgentDeliveryRecord, AgentDirectMessageRequest, AgentMessageAck, AgentMessageContextKind,
    AgentMessageListRequest, AgentMessageListResponse, AgentMessageRecord,
    AgentRetryDeliveryRequest, RuntimeError, WorkspaceAgentLifecycleState, WorkspaceLifecycleState,
};

use super::{
    ensure_message_images_supported, normalize_non_empty, normalize_non_empty_input,
    normalize_policy, normalize_priority, normalized_non_empty, now_ms,
    prospective_agent_idempotency_key, validate_message_image_paths, DeliveryAttemptTrigger,
    RuntimeTeamCommsService, DELIVERY_STATUS_CANCELLED, DELIVERY_STATUS_DEFERRED,
    DELIVERY_STATUS_FAILED, DELIVERY_STATUS_PENDING,
};

impl RuntimeTeamCommsService {
    pub(super) async fn send_agent_direct_impl(
        &self,
        request: AgentDirectMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        self.ensure_enabled()?;
        let sender = normalize_non_empty(&request.sender_agent_id, "sender_agent_id")?;
        let recipient = normalize_non_empty(&request.recipient_agent_id, "recipient_agent_id")?;
        let sender_session = self.require_session_active(&sender).await?;
        let recipient_session = self.require_session_active(&recipient).await?;
        let sender_profile = self.store.get_workspace_agent_by_id(&sender)?;
        let recipient_profile = self.store.get_workspace_agent_by_id(&recipient)?;

        for profile in [sender_profile.as_ref(), recipient_profile.as_ref()]
            .into_iter()
            .flatten()
        {
            if profile.lifecycle_state != WorkspaceAgentLifecycleState::Active {
                return Err(RuntimeError::InvalidState(format!(
                    "agent {} is archived and cannot participate in messaging",
                    profile.agent_id
                )));
            }
        }

        let (context_kind, workspace_id) = match (&sender_profile, &recipient_profile) {
            (Some(sender_profile), Some(recipient_profile))
                if sender_profile.workspace_id == recipient_profile.workspace_id =>
            {
                (
                    AgentMessageContextKind::WorkspaceTeam,
                    Some(sender_profile.workspace_id.clone()),
                )
            }
            _ => (AgentMessageContextKind::GlobalDirect, None),
        };

        let input = normalize_non_empty_input(request.input)?;
        let image_paths = validate_message_image_paths(request.image_paths)?;
        ensure_message_images_supported(&recipient_session.provider, !image_paths.is_empty())?;

        let ack = self
            .create_agent_message_and_deliveries(
                "direct",
                context_kind,
                workspace_id,
                sender,
                vec![recipient],
                input,
                image_paths,
                request.priority,
                request.policy,
                request.correlation_id,
                request.reply_to_message_id,
                request.idempotency_key,
                HashMap::from([(recipient_session.id, recipient_session.provider)]),
            )
            .await?;
        if ack.disposition == "existing" {
            return Ok(ack);
        }
        self.queue_and_attempt_agent_delivery(&ack).await;
        let _ = sender_session;
        self.refresh_agent_ack(ack.message.id.as_str()).await
    }

    pub(super) async fn broadcast_workspace_impl(
        &self,
        request: AgentBroadcastMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        self.ensure_enabled()?;
        let sender = normalize_non_empty(&request.sender_agent_id, "sender_agent_id")?;
        self.require_session_active(&sender).await?;
        let sender_profile = self
            .store
            .get_workspace_agent_by_id(&sender)?
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "agent {} has no canonical workspace membership for broadcast",
                    sender
                ))
            })?;
        if sender_profile.lifecycle_state != WorkspaceAgentLifecycleState::Active {
            return Err(RuntimeError::InvalidState(format!(
                "agent {} is archived and cannot broadcast",
                sender
            )));
        }
        let workspace = self
            .store
            .get_workspace(&sender_profile.workspace_id)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("workspace {}", sender_profile.workspace_id))
            })?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::InvalidState(format!(
                "workspace {} is not active",
                workspace.workspace_id
            )));
        }

        let input = normalize_non_empty_input(request.input)?;
        let image_paths = validate_message_image_paths(request.image_paths)?;
        let mut roster = self.store.list_workspace_agents(
            &sender_profile.workspace_id,
            Some(WorkspaceAgentLifecycleState::Active),
        )?;
        roster.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));

        let mut recipients = Vec::new();
        let mut provider_map = HashMap::new();
        let mut seen = HashSet::new();
        for profile in roster {
            if profile.agent_id == sender || !seen.insert(profile.agent_id.clone()) {
                continue;
            }
            let session = self.require_session_active(&profile.agent_id).await?;
            ensure_message_images_supported(&session.provider, !image_paths.is_empty())?;
            provider_map.insert(profile.agent_id.clone(), session.provider);
            recipients.push(profile.agent_id);
        }

        let ack = self
            .create_agent_message_and_deliveries(
                "broadcast",
                AgentMessageContextKind::WorkspaceTeam,
                Some(workspace.workspace_id),
                sender,
                recipients,
                input,
                image_paths,
                request.priority,
                request.policy,
                request.correlation_id,
                None,
                request.idempotency_key,
                provider_map,
            )
            .await?;
        if ack.disposition == "existing" {
            return Ok(ack);
        }
        self.queue_and_attempt_agent_delivery(&ack).await;
        self.refresh_agent_ack(ack.message.id.as_str()).await
    }

    pub(super) async fn list_agent_messages_impl(
        &self,
        request: AgentMessageListRequest,
    ) -> Result<AgentMessageListResponse, RuntimeError> {
        self.ensure_enabled()?;
        let limit = request.limit.unwrap_or(100).clamp(1, 500);
        let state = self.state.read().await;
        let filtered = state
            .agent_message_ids
            .iter()
            .filter_map(|message_id| state.agent_messages.get(message_id))
            .filter(|message| {
                request
                    .workspace_id
                    .as_deref()
                    .map(|workspace_id| message.workspace_id.as_deref() == Some(workspace_id))
                    .unwrap_or(true)
            })
            .filter(|message| {
                request
                    .sender_agent_id
                    .as_deref()
                    .map(|sender| message.sender_agent_id == sender)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        let start = match request.cursor.as_deref() {
            Some(cursor) => filtered
                .iter()
                .position(|message| message.id == cursor)
                .map(|index| index + 1)
                .ok_or_else(|| {
                    RuntimeError::InvalidState(format!("cursor message {} not found", cursor))
                })?,
            None => 0,
        };
        let messages = filtered
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = filtered.len().saturating_sub(start) > messages.len();
        let next_cursor = has_more
            .then(|| messages.last().map(|message| message.id.clone()))
            .flatten();
        Ok(AgentMessageListResponse {
            messages,
            next_cursor,
        })
    }

    pub(super) async fn get_agent_deliveries_impl(
        &self,
        request: AgentDeliveryListRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        self.ensure_enabled()?;
        let state = self.state.read().await;
        let mut rows = if let Some(message_id) = request.message_id.as_deref() {
            if !state.agent_messages.contains_key(message_id) {
                return Err(RuntimeError::NotFound(format!("message {}", message_id)));
            }
            state
                .agent_message_delivery_ids
                .get(message_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|delivery_id| state.agent_deliveries.get(&delivery_id).cloned())
                .collect::<Vec<_>>()
        } else {
            state.agent_deliveries.values().cloned().collect::<Vec<_>>()
        };
        if let Some(recipient) = request.recipient_agent_id.as_deref() {
            rows.retain(|delivery| delivery.recipient_agent_id == recipient);
        }
        rows.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(rows)
    }

    pub(super) async fn retry_agent_delivery_impl(
        &self,
        request: AgentRetryDeliveryRequest,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        self.ensure_enabled()?;
        let delivery_id = normalize_non_empty(&request.delivery_id, "delivery_id")?;
        let updated = {
            let mut state = self.state.write().await;
            let delivery = state
                .agent_deliveries
                .get_mut(&delivery_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?;
            if !matches!(
                delivery.status.as_str(),
                DELIVERY_STATUS_FAILED | DELIVERY_STATUS_DEFERRED
            ) {
                return Err(RuntimeError::InvalidState(format!(
                    "delivery {} can only be retried from failed/deferred state",
                    delivery_id
                )));
            }
            delivery.status = DELIVERY_STATUS_PENDING.to_string();
            delivery.injection_strategy = None;
            delivery.injected_turn_id = None;
            delivery.last_error_code = None;
            delivery.last_error_message = None;
            delivery.updated_at = now_ms();
            delivery.clone()
        };
        self.persist_agent_delivery_with_legacy_mirror(&updated)
            .await?;
        self.inject_agent_delivery(&delivery_id, DeliveryAttemptTrigger::Retry)
            .await
    }

    pub(super) async fn cancel_agent_message_impl(
        &self,
        request: AgentCancelMessageRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        self.ensure_enabled()?;
        let message_id = normalize_non_empty(&request.message_id, "message_id")?;
        let cancelled = {
            let mut state = self.state.write().await;
            if !state.agent_messages.contains_key(&message_id) {
                return Err(RuntimeError::NotFound(format!("message {}", message_id)));
            }
            let delivery_ids = state
                .agent_message_delivery_ids
                .get(&message_id)
                .cloned()
                .unwrap_or_default();
            for delivery_id in &delivery_ids {
                if let Some(delivery) = state.agent_deliveries.get(delivery_id) {
                    if !matches!(
                        delivery.status.as_str(),
                        DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
                    ) {
                        return Err(RuntimeError::InvalidState(format!(
                            "message {} cannot be cancelled because delivery {} is in {}",
                            message_id, delivery.id, delivery.status
                        )));
                    }
                }
            }
            let mut updated = Vec::new();
            for delivery_id in delivery_ids {
                if let Some(delivery) = state.agent_deliveries.get_mut(&delivery_id) {
                    delivery.status = DELIVERY_STATUS_CANCELLED.to_string();
                    delivery.updated_at = now_ms();
                    updated.push(delivery.clone());
                }
            }
            updated
        };
        for delivery in &cancelled {
            self.persist_agent_delivery_with_legacy_mirror(delivery)
                .await?;
        }
        for recipient in cancelled
            .iter()
            .map(|delivery| delivery.recipient_agent_id.clone())
            .collect::<HashSet<_>>()
        {
            let _ = self
                .resume_deferred_agent_for_recipient(
                    &recipient,
                    DeliveryAttemptTrigger::TurnCompletedBoundary,
                )
                .await;
        }
        Ok(cancelled)
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_agent_message_and_deliveries(
        &self,
        scope: &str,
        context_kind: AgentMessageContextKind,
        workspace_id: Option<String>,
        sender_agent_id: String,
        recipient_agent_ids: Vec<String>,
        input: Value,
        image_paths: Vec<String>,
        priority: String,
        policy: String,
        correlation_id: Option<String>,
        reply_to_message_id: Option<String>,
        idempotency_key: Option<String>,
        recipient_provider_map: HashMap<String, String>,
    ) -> Result<AgentMessageAck, RuntimeError> {
        if recipient_agent_ids.len() > self.config.max_pending_deliveries {
            return Err(RuntimeError::InvalidState(format!(
                "recipient count exceeds max_pending_deliveries ({})",
                self.config.max_pending_deliveries
            )));
        }
        let normalized_policy = normalize_policy(&policy)?;
        let normalized_priority = normalize_priority(&priority);
        let normalized_idempotency_key = normalized_non_empty(idempotency_key.as_deref());

        let mut state = self.state.write().await;
        if let Some(key) = normalized_idempotency_key.as_deref() {
            let index_key = prospective_agent_idempotency_key(
                &sender_agent_id,
                scope,
                context_kind,
                workspace_id.as_deref(),
                key,
            );
            if let Some(existing_id) = state.agent_idempotency_index.get(&index_key).cloned() {
                if let Some(existing) = state.agent_messages.get(&existing_id).cloned() {
                    if existing.recipient_agent_ids != recipient_agent_ids
                        || existing.input != input
                        || existing.image_paths != image_paths
                        || existing.priority != normalized_priority
                        || existing.policy != normalized_policy
                        || existing.correlation_id != correlation_id
                        || existing.reply_to_message_id != reply_to_message_id
                    {
                        return Err(RuntimeError::Conflict(format!(
                            "idempotency key {} was already used with different normalized message input",
                            key
                        )));
                    }
                    let deliveries = state
                        .agent_message_delivery_ids
                        .get(&existing.id)
                        .into_iter()
                        .flat_map(|ids| ids.iter())
                        .filter_map(|delivery_id| state.agent_deliveries.get(delivery_id).cloned())
                        .collect::<Vec<_>>();
                    return Ok(AgentMessageAck {
                        message: existing,
                        deliveries,
                        disposition: "existing".to_string(),
                    });
                }
            }
        }

        let now = now_ms();
        let message = AgentMessageRecord {
            id: self.allocate_message_id(&state),
            scope: scope.to_string(),
            context_kind,
            workspace_id: workspace_id.clone(),
            legacy_team_id: None,
            sender_agent_id: sender_agent_id.clone(),
            recipient_agent_ids: recipient_agent_ids.clone(),
            input,
            image_paths,
            priority: normalized_priority,
            policy: normalized_policy,
            correlation_id,
            reply_to_message_id,
            idempotency_key: normalized_idempotency_key.clone(),
            created_at: now,
        };
        let mut deliveries = Vec::with_capacity(recipient_agent_ids.len());
        for recipient in recipient_agent_ids {
            let provider = recipient_provider_map
                .get(&recipient)
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::InvalidState(format!(
                        "missing provider mapping for recipient {}",
                        recipient
                    ))
                })?;
            deliveries.push(AgentDeliveryRecord {
                id: self.allocate_delivery_id(&state),
                message_id: message.id.clone(),
                recipient_agent_id: recipient,
                provider,
                status: DELIVERY_STATUS_PENDING.to_string(),
                effective_policy: Some(message.policy.clone()),
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: None,
                last_error_message: None,
                created_at: now,
                updated_at: now,
            });
        }

        self.store
            .insert_agent_message_with_deliveries(&message, &deliveries)?;
        state.agent_message_ids.push(message.id.clone());
        state
            .agent_messages
            .insert(message.id.clone(), message.clone());
        if let Some(key) = normalized_idempotency_key.as_deref() {
            state.agent_idempotency_index.insert(
                prospective_agent_idempotency_key(
                    &sender_agent_id,
                    scope,
                    context_kind,
                    workspace_id.as_deref(),
                    key,
                ),
                message.id.clone(),
            );
        }
        for delivery in &deliveries {
            state
                .agent_deliveries
                .insert(delivery.id.clone(), delivery.clone());
            state
                .agent_message_delivery_ids
                .entry(message.id.clone())
                .or_default()
                .push(delivery.id.clone());
            state
                .agent_recipient_delivery_ids
                .entry(delivery.recipient_agent_id.clone())
                .or_default()
                .push(delivery.id.clone());
        }
        Ok(AgentMessageAck {
            message,
            deliveries,
            disposition: "created".to_string(),
        })
    }

    async fn queue_and_attempt_agent_delivery(&self, ack: &AgentMessageAck) {
        if ack.disposition == "existing" {
            return;
        }
        for delivery in &ack.deliveries {
            let _ = self
                .inject_agent_delivery(&delivery.id, DeliveryAttemptTrigger::Queue)
                .await;
        }
    }

    async fn refresh_agent_ack(&self, message_id: &str) -> Result<AgentMessageAck, RuntimeError> {
        let state = self.state.read().await;
        let message = state
            .agent_messages
            .get(message_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("message {}", message_id)))?;
        let deliveries = state
            .agent_message_delivery_ids
            .get(message_id)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|delivery_id| state.agent_deliveries.get(delivery_id).cloned())
            .collect::<Vec<_>>();
        Ok(AgentMessageAck {
            message,
            deliveries,
            disposition: "created".to_string(),
        })
    }
}
