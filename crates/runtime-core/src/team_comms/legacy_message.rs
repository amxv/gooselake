//! Legacy /v1 message/pending-delivery admission is pre-native authority.
//! Do not publish cache or inject recipients before the whole critical SQLite
//! receipt is durable. An ambiguous result is not permission for a resend.

use super::*;

impl RuntimeTeamCommsService {
    pub(super) async fn create_message_and_deliveries(
        &self,
        team_id: &str,
        scope: &str,
        sender_agent_id: &str,
        recipient_agent_ids: Vec<String>,
        input: Value,
        image_paths: Vec<String>,
        priority: String,
        policy: String,
        correlation_id: Option<String>,
        reply_to_message_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<TeamMessageAck, RuntimeError> {
        let normalized_scope = normalize_scope(scope)?;
        let normalized_priority = normalize_priority(priority.as_str());
        let normalized_policy = normalize_policy(policy.as_str())?;
        let normalized_sender = normalize_non_empty(sender_agent_id, "sender_agent_id")?;

        if recipient_agent_ids.len() > self.config.max_pending_deliveries {
            return Err(RuntimeError::InvalidState(format!(
                "recipient count exceeds max_pending_deliveries ({})",
                self.config.max_pending_deliveries
            )));
        }

        let normalized_idempotency_key = normalized_non_empty(idempotency_key.as_deref());
        let mut recipient_provider_map = HashMap::new();
        let mut deduped_recipients = Vec::new();
        let mut seen = HashSet::new();
        for recipient in recipient_agent_ids {
            let recipient = normalize_non_empty(recipient.as_str(), "recipient_agent_id")?;
            if !seen.insert(recipient.clone()) {
                continue;
            }
            let recipient_session = self.require_session_active(recipient.as_str()).await?;
            recipient_provider_map.insert(recipient.clone(), recipient_session.provider);
            deduped_recipients.push(recipient);
        }

        // Hold the service cache guard across preflight, the authoritative
        // transaction, and cache publication. Independent service instances
        // are serialized by SQLite BEGIN IMMEDIATE, never this local lock.
        let mut state = self.state.write().await;
        let team = state
            .teams
            .get(team_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
        let team_state_id = team.id.clone();
        ensure_member(
            state.members_by_team.get(team_id),
            normalized_sender.as_str(),
            team_id,
        )?;
        for recipient in &deduped_recipients {
            ensure_member(state.members_by_team.get(team_id), recipient, team_id)?;
        }

        if let Some(key) = normalized_idempotency_key.as_ref() {
            let idx_key =
                idempotency_index_key(team_id, normalized_sender.as_str(), &normalized_scope, key);
            if let Some(existing_id) = state.idempotency_index.get(&idx_key) {
                if let Some(existing) = state.messages.get(existing_id) {
                    let requested_recipients = serde_json::json!(&deduped_recipients);
                    let requested_images = serde_json::json!(&image_paths);
                    if existing.recipient_agent_ids != requested_recipients
                        || existing.input != input
                        || existing.image_paths != requested_images
                        || existing.priority != normalized_priority
                        || existing.policy != normalized_policy
                        || existing.correlation_id != correlation_id
                        || existing.reply_to_message_id != reply_to_message_id
                    {
                        return Err(RuntimeError::Conflict(
                            "legacy message idempotency key was already used for different input"
                                .into(),
                        ));
                    }
                    let cached_deliveries = state
                        .message_delivery_ids
                        .get(existing_id)
                        .into_iter()
                        .flat_map(|ids| ids.iter())
                        .filter_map(|delivery_id| state.deliveries.get(delivery_id).cloned())
                        .collect::<Vec<_>>();
                    return Ok(TeamMessageAck {
                        message: existing.clone(),
                        deliveries: cached_deliveries,
                        disposition: "existing".into(),
                    });
                }
            }
        }

        let now = now_ms();
        let message = TeamMessageRecord {
            // All service processes can observe the same numeric max on
            // startup; the random service nonce prevents cross-owner ID
            // collision without ever falling back to mutating an old row.
            id: format!(
                "msg_{}_{}",
                self.event_id_nonce,
                self.next_message_id.fetch_add(1, Ordering::Relaxed)
            ),
            team_id: team_state_id.clone(),
            scope: normalized_scope,
            sender_agent_id: normalized_sender.clone(),
            recipient_agent_ids: serde_json::json!(&deduped_recipients),
            input,
            image_paths: serde_json::json!(image_paths),
            priority: normalized_priority,
            policy: normalized_policy,
            correlation_id,
            reply_to_message_id,
            idempotency_key: normalized_idempotency_key,
            created_at: now,
        };

        let mut deliveries = Vec::with_capacity(deduped_recipients.len());
        for recipient in deduped_recipients {
            let provider = recipient_provider_map.remove(&recipient).ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "missing provider mapping for recipient {recipient}"
                ))
            })?;
            deliveries.push(TeamDeliveryRecord {
                id: format!(
                    "dlv_{}_{}",
                    self.event_id_nonce,
                    self.next_delivery_id.fetch_add(1, Ordering::Relaxed)
                ),
                message_id: message.id.clone(),
                team_id: team_state_id.clone(),
                recipient_agent_id: recipient,
                provider,
                status: DELIVERY_STATUS_PENDING.into(),
                effective_policy: Some(message.policy.clone()),
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: None,
                last_error_message: None,
                created_at: now,
                updated_at: now,
            });
        }

        let mut message_event = self.new_team_event(
            team_id,
            "team_message.created",
            serde_json::json!({"message":message,"deliveries":deliveries}),
            Some(normalized_sender),
        );
        message_event.event_id = format!("evt_legacy_message_{}_created", message.id);
        message_event.created_at = now;
        let mut events = Vec::with_capacity(deliveries.len() + 1);
        events.push(message_event);
        for delivery in &deliveries {
            let mut event = self.new_team_event(
                team_id,
                "team_delivery.pending",
                serde_json::json!({"delivery":delivery}),
                Some(delivery.recipient_agent_id.clone()),
            );
            event.event_id = format!("evt_legacy_delivery_{}_pending", delivery.id);
            event.created_at = now;
            events.push(event);
        }

        let ack =
            self.store
                .commit_legacy_team_message_admission(&message, &deliveries, &events)?;

        // Only a fully committed admission can reach the in-process indexes.
        // A competing independently committed keyed message can return its
        // historical receipt with different IDs; never dispatch it again.
        if !state.messages.contains_key(&ack.message.id) {
            state
                .team_message_ids
                .entry(ack.message.team_id.clone())
                .or_default()
                .push(ack.message.id.clone());
            state
                .messages
                .insert(ack.message.id.clone(), ack.message.clone());
        }
        if let Some(key) = ack.message.idempotency_key.as_ref() {
            state.idempotency_index.insert(
                idempotency_index_key(
                    &ack.message.team_id,
                    &ack.message.sender_agent_id,
                    &ack.message.scope,
                    key,
                ),
                ack.message.id.clone(),
            );
        }
        for delivery in &ack.deliveries {
            if state.deliveries.contains_key(&delivery.id) {
                continue;
            }
            state
                .team_delivery_ids
                .entry(ack.message.team_id.clone())
                .or_default()
                .push(delivery.id.clone());
            state
                .message_delivery_ids
                .entry(ack.message.id.clone())
                .or_default()
                .push(delivery.id.clone());
            state
                .recipient_delivery_ids
                .entry(delivery.recipient_agent_id.clone())
                .or_default()
                .push(delivery.id.clone());
            state
                .deliveries
                .insert(delivery.id.clone(), delivery.clone());
        }

        Ok(ack)
    }
}
