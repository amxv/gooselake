use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::{
    AgentBroadcastMessageRequest, AgentCancelMessageMutationCommand,
    AgentCancelMessageMutationResponse, AgentCancelMessageRequest, AgentDeliveryListRequest,
    AgentDeliveryRecord, AgentDirectMessageRequest, AgentMessageAck, AgentMessageListRequest,
    AgentMessageListResponse, AgentRetryDeliveryMutationCommand,
    AgentRetryDeliveryMutationResponse, AgentRetryDeliveryRequest, RuntimeError,
    RuntimeEventRecord, RuntimeEventScope, TeamBroadcastRequest, TeamCancelMessageRequest,
    TeamCommsService, TeamCreateRequest, TeamDeliveryRecord, TeamGetDeliveriesRequest,
    TeamInterruptAllRequest, TeamInterruptAllResponse, TeamJoinRequest, TeamListMessagesRequest,
    TeamListMessagesResponse, TeamMessageAck, TeamRemoveMemberRequest, TeamRetryDeliveryRequest,
    TeamSendDirectRequest, TeamSetLeadRequest, TeamViewSnapshotRequest, TeamViewSnapshotResponse,
    TeamWithMembers,
};

use super::{
    normalize_non_empty, normalize_non_empty_input, now_ms, remove_delivery_from_recipient_index,
    DeliveryAttemptTrigger, RuntimeTeamCommsService, DELIVERY_STATUS_CANCELLED,
    DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_FAILED, DELIVERY_STATUS_PENDING,
};

#[async_trait]
impl TeamCommsService for RuntimeTeamCommsService {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        self.ensure_enabled()
    }

    async fn create_team(
        &self,
        request: TeamCreateRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.legacy_roster_create(request).await
    }

    async fn list_teams(&self) -> Result<Vec<TeamWithMembers>, RuntimeError> {
        self.ensure_enabled()?;
        let team_ids = {
            let state = self.state.read().await;
            let mut teams = state.teams.values().cloned().collect::<Vec<_>>();
            teams.sort_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
            teams.into_iter().map(|team| team.id).collect::<Vec<_>>()
        };

        let mut rows = Vec::with_capacity(team_ids.len());
        for team_id in team_ids {
            rows.push(self.team_with_members(team_id.as_str()).await?);
        }
        Ok(rows)
    }

    async fn get_team(&self, team_id: &str) -> Result<TeamWithMembers, RuntimeError> {
        self.ensure_enabled()?;
        self.team_with_members(normalize_non_empty(team_id, "team_id")?.as_str())
            .await
    }

    async fn join_team(&self, request: TeamJoinRequest) -> Result<TeamWithMembers, RuntimeError> {
        self.legacy_roster_join(request).await
    }

    async fn remove_team_member(
        &self,
        request: TeamRemoveMemberRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.legacy_roster_remove(request).await
    }

    async fn set_team_lead(
        &self,
        request: TeamSetLeadRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.legacy_roster_set_lead(request).await
    }

    async fn delete_team(&self, team_id: &str) -> Result<(), RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(team_id, "team_id")?;
        // Hold the local admission mutex until the durable tombstone and
        // recipient Critical receipts have committed. Another process still
        // competes through SQLite's immediate team/delivery generation CAS.
        let mut state = self.state.write().await;
        let before = state
            .teams
            .get(&team_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
        let mut all = state
            .deliveries
            .values()
            .filter(|row| row.team_id == team_id)
            .cloned()
            .collect::<Vec<_>>();
        all.sort_by(|a, b| (&a.message_id, &a.id).cmp(&(&b.message_id, &b.id)));
        if all.iter().any(|row| row.status == "injecting") {
            return Err(RuntimeError::Conflict(
                "cannot delete a team with unresolved native injecting deliveries".into(),
            ));
        }
        // A canonical /v2 delivery owns its own cancellation authority. A
        // legacy team tombstone must not rewrite that unrelated owner.
        if all.iter().any(|row| {
            matches!(
                row.status.as_str(),
                DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
            ) && state.agent_deliveries.contains_key(&row.id)
        }) {
            return Err(RuntimeError::Conflict(
                "team deletion cannot cancel a canonical-owned recipient".into(),
            ));
        }
        let revision = before.updated_at.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidState("legacy team deletion revision exhausted".into())
        })?;
        let max_delivery_revision = all
            .iter()
            .map(|row| row.updated_at.saturating_add(1))
            .max()
            .unwrap_or(revision);
        let timestamp = now_ms().max(revision).max(max_delivery_revision);
        let mut deleted = before.clone();
        deleted.updated_at = timestamp;
        deleted.deleted_at = Some(timestamp);
        let mut cancelled = Vec::new();
        let mut events = Vec::new();
        for previous in all.iter().filter(|row| {
            matches!(
                row.status.as_str(),
                DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
            )
        }) {
            let mut row = previous.clone();
            row.status = DELIVERY_STATUS_CANCELLED.into();
            row.injection_strategy = None;
            row.injected_turn_id = None;
            row.last_error_code = None;
            row.last_error_message = None;
            row.updated_at = timestamp;
            let mut event = self.new_team_event(
                &team_id,
                "team_delivery.cancelled",
                serde_json::json!({
                    "delivery":row,
                    "before_status":previous.status,
                    "before_updated_at":previous.updated_at,
                }),
                Some(previous.recipient_agent_id.clone()),
            );
            event.event_id = format!(
                "evt_legacy_team_delete_{}_delivery_{}_cancelled_{}",
                team_id, previous.id, previous.updated_at
            );
            event.created_at = timestamp;
            cancelled.push(row);
            events.push(event);
        }
        let mut tombstone_event = self.new_team_event(
            &team_id,
            "team.deleted",
            serde_json::json!({"team":deleted}),
            Some(deleted.created_by.clone()),
        );
        tombstone_event.event_id = format!("evt_team_{}_deleted", team_id);
        tombstone_event.created_at = timestamp;
        events.push(tombstone_event);
        // An event trigger fault or a competing native-claim/admission leaves
        // ALL in-memory team, delivery and recipient-queue state untouched.
        let committed = self
            .store
            .commit_legacy_team_deletion(&before, &deleted, &all, &cancelled, &events)?;
        // A lost SQLite commit reply can be reconciled against a previously
        // committed exact tombstone with a different clock sample. The store
        // has verified all Critical receipts and recipient generations.
        let mut expected_committed = before.clone();
        expected_committed.updated_at = committed.updated_at;
        expected_committed.deleted_at = Some(committed.updated_at);
        if committed != expected_committed || committed.updated_at <= before.updated_at {
            return Err(RuntimeError::Conflict(
                "legacy team deletion Critical receipt disagrees with tombstone".into(),
            ));
        }

        for row in &all {
            state.deliveries.remove(&row.id);
            remove_delivery_from_recipient_index(
                &mut state.recipient_delivery_ids,
                &row.recipient_agent_id,
                &row.id,
            );
        }
        let message_ids = state
            .messages
            .values()
            .filter(|message| message.team_id == team_id)
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        for id in message_ids {
            state.messages.remove(&id);
            state.message_delivery_ids.remove(&id);
        }
        state.team_delivery_ids.remove(&team_id);
        state.team_message_ids.remove(&team_id);
        state.members_by_team.remove(&team_id);
        state.teams.remove(&team_id);
        state
            .idempotency_index
            .retain(|key, _| !key.starts_with(&format!("{}|", team_id)));
        Ok(())
    }

    async fn interrupt_all_team_turns(
        &self,
        request: TeamInterruptAllRequest,
    ) -> Result<TeamInterruptAllResponse, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let members = self.team_with_members(team_id.as_str()).await?.members;

        let mut interrupted = Vec::new();
        let mut skipped = Vec::new();
        for member in members {
            let session = match self.runtime.get_session(member.agent_id.as_str()).await {
                Ok(session) => session,
                Err(_) => {
                    skipped.push(member.agent_id);
                    continue;
                }
            };
            let Some(turn_id) = session.active_turn_id.clone() else {
                skipped.push(session.id);
                continue;
            };
            match self
                .runtime
                .interrupt_turn(session.id.as_str(), turn_id.as_str())
                .await
            {
                Ok(_) => interrupted.push(session.id),
                Err(_) => skipped.push(session.id),
            }
        }

        let response = TeamInterruptAllResponse {
            team_id: team_id.clone(),
            interrupted_session_ids: interrupted,
            skipped_session_ids: skipped,
        };

        let _ = self
            .append_team_event(
                team_id.as_str(),
                "team.interrupt_all",
                serde_json::json!({ "result": response }),
                None,
            )
            .await;

        Ok(response)
    }

    async fn send_direct(
        &self,
        request: TeamSendDirectRequest,
    ) -> Result<TeamMessageAck, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let sender = normalize_non_empty(request.sender_agent_id.as_str(), "sender_agent_id")?;
        let recipient =
            normalize_non_empty(request.recipient_agent_id.as_str(), "recipient_agent_id")?;
        let input = normalize_non_empty_input(request.input)?;

        let ack = self
            .create_message_and_deliveries(
                team_id.as_str(),
                "direct",
                sender.as_str(),
                vec![recipient],
                input,
                request.image_paths,
                request.priority,
                request.policy,
                request.correlation_id,
                request.reply_to_message_id,
                request.idempotency_key,
            )
            .await?;

        if ack.disposition == "created" {
            self.queue_and_attempt_delivery(&ack).await;
        }

        Ok(ack)
    }

    async fn broadcast(
        &self,
        request: TeamBroadcastRequest,
    ) -> Result<TeamMessageAck, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let sender = normalize_non_empty(request.sender_agent_id.as_str(), "sender_agent_id")?;
        let input = normalize_non_empty_input(request.input)?;

        let recipients = {
            let state = self.state.read().await;
            let members = state
                .members_by_team
                .get(&team_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
            let mut recipients = members.keys().cloned().collect::<Vec<_>>();
            recipients.sort();
            if !request.include_sender {
                recipients.retain(|member| member != &sender);
            }
            recipients
        };

        let ack = self
            .create_message_and_deliveries(
                team_id.as_str(),
                "broadcast",
                sender.as_str(),
                recipients,
                input,
                request.image_paths,
                request.priority,
                request.policy,
                request.correlation_id,
                None,
                request.idempotency_key,
            )
            .await?;

        if ack.disposition == "created" {
            self.queue_and_attempt_delivery(&ack).await;
        }

        Ok(ack)
    }

    async fn send_agent_direct(
        &self,
        request: AgentDirectMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        self.send_agent_direct_impl(request).await
    }

    async fn broadcast_workspace(
        &self,
        request: AgentBroadcastMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        self.broadcast_workspace_impl(request).await
    }

    async fn list_agent_messages(
        &self,
        request: AgentMessageListRequest,
    ) -> Result<AgentMessageListResponse, RuntimeError> {
        self.list_agent_messages_impl(request).await
    }

    async fn get_agent_deliveries(
        &self,
        request: AgentDeliveryListRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        self.get_agent_deliveries_impl(request).await
    }

    async fn get_agent_deliveries_page(
        &self,
        request: AgentDeliveryListRequest,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        self.get_agent_deliveries_page_impl(request, cursor, limit)
            .await
    }

    async fn retry_agent_delivery(
        &self,
        request: AgentRetryDeliveryRequest,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        self.retry_agent_delivery_impl(request).await
    }

    async fn retry_agent_delivery_operator(
        &self,
        command: AgentRetryDeliveryMutationCommand,
    ) -> Result<AgentRetryDeliveryMutationResponse, RuntimeError> {
        self.retry_agent_delivery_operator_impl(command).await
    }

    async fn cancel_agent_message(
        &self,
        request: AgentCancelMessageRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        self.cancel_agent_message_impl(request).await
    }

    async fn cancel_agent_message_operator(
        &self,
        command: AgentCancelMessageMutationCommand,
    ) -> Result<AgentCancelMessageMutationResponse, RuntimeError> {
        self.cancel_agent_message_operator_impl(command).await
    }

    async fn list_messages(
        &self,
        request: TeamListMessagesRequest,
    ) -> Result<TeamListMessagesResponse, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let limit = request.limit.unwrap_or(100).clamp(1, 500);

        let (messages, next_cursor) = {
            let state = self.state.read().await;
            let ids = state
                .team_message_ids
                .get(&team_id)
                .cloned()
                .unwrap_or_default();

            let start = match request
                .cursor
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                Some(cursor_id) => ids
                    .iter()
                    .position(|message_id| message_id == cursor_id)
                    .map(|idx| idx + 1)
                    .ok_or_else(|| {
                        RuntimeError::InvalidState(format!(
                            "cursor message {} not found for team {}",
                            cursor_id, team_id
                        ))
                    })?,
                None => 0,
            };

            let mut page = Vec::new();
            for message_id in ids.iter().skip(start).take(limit) {
                if let Some(message) = state.messages.get(message_id) {
                    page.push(message.clone());
                }
            }

            let has_more = ids.len().saturating_sub(start) > page.len();
            let next_cursor = if has_more {
                page.last().map(|message| message.id.clone())
            } else {
                None
            };
            (page, next_cursor)
        };

        Ok(TeamListMessagesResponse {
            messages,
            next_cursor,
        })
    }

    async fn get_deliveries(
        &self,
        request: TeamGetDeliveriesRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;

        let deliveries = {
            let state = self.state.read().await;
            let source_ids = if let Some(message_id) = request.message_id.as_ref() {
                let message = state
                    .messages
                    .get(message_id)
                    .ok_or_else(|| RuntimeError::NotFound(format!("message {}", message_id)))?;
                if message.team_id != team_id {
                    return Ok(Vec::new());
                }
                state
                    .message_delivery_ids
                    .get(message_id)
                    .cloned()
                    .unwrap_or_default()
            } else {
                state
                    .team_delivery_ids
                    .get(&team_id)
                    .cloned()
                    .unwrap_or_default()
            };

            let mut rows = source_ids
                .into_iter()
                .filter_map(|delivery_id| state.deliveries.get(&delivery_id).cloned())
                .collect::<Vec<_>>();

            if let Some(recipient) = request.recipient_agent_id.as_deref() {
                rows.retain(|delivery| delivery.recipient_agent_id == recipient);
            }
            rows
        };

        Ok(deliveries)
    }

    async fn retry_delivery(
        &self,
        request: TeamRetryDeliveryRequest,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let delivery_id = normalize_non_empty(request.delivery_id.as_str(), "delivery_id")?;

        let migrated_to_agent_authority = {
            let state = self.state.read().await;
            let delivery = state
                .deliveries
                .get(&delivery_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?;
            if delivery.team_id != team_id {
                return Err(RuntimeError::InvalidState(format!(
                    "delivery {} does not belong to team {}",
                    delivery_id, team_id
                )));
            }
            state.agent_deliveries.contains_key(&delivery_id)
        };
        if migrated_to_agent_authority {
            self.retry_agent_delivery_impl(AgentRetryDeliveryRequest {
                delivery_id: delivery_id.clone(),
            })
            .await?;
            return self
                .state
                .read()
                .await
                .deliveries
                .get(&delivery_id)
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)));
        }

        let previous = {
            let state = self.state.read().await;
            let delivery = state
                .deliveries
                .get(&delivery_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?;
            if delivery.team_id != team_id {
                return Err(RuntimeError::InvalidState(format!(
                    "delivery {} does not belong to team {}",
                    delivery_id, team_id
                )));
            }
            if !matches!(
                delivery.status.as_str(),
                DELIVERY_STATUS_FAILED | DELIVERY_STATUS_DEFERRED
            ) {
                return Err(RuntimeError::InvalidState(format!(
                    "delivery {} can only be retried from failed/deferred state",
                    delivery_id
                )));
            }
            delivery.clone()
        };
        self.transition_delivery_status(
            &previous,
            DELIVERY_STATUS_PENDING,
            None,
            None,
            None,
            DeliveryAttemptTrigger::Retry,
        )
        .await?;
        self.inject_delivery(delivery_id.as_str(), DeliveryAttemptTrigger::Retry)
            .await
    }

    async fn cancel_message(
        &self,
        request: TeamCancelMessageRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let message_id = normalize_non_empty(request.message_id.as_str(), "message_id")?;

        let migrated_to_agent_authority = {
            let state = self.state.read().await;
            let message = state
                .messages
                .get(&message_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("message {}", message_id)))?;
            if message.team_id != team_id {
                return Err(RuntimeError::InvalidState(format!(
                    "message {} does not belong to team {}",
                    message_id, team_id
                )));
            }
            state.agent_messages.contains_key(&message_id)
        };
        if migrated_to_agent_authority {
            self.cancel_agent_message_impl(AgentCancelMessageRequest {
                message_id: message_id.clone(),
            })
            .await?;
            let state = self.state.read().await;
            let delivery_ids = state
                .message_delivery_ids
                .get(&message_id)
                .cloned()
                .unwrap_or_default();
            return Ok(delivery_ids
                .into_iter()
                .filter_map(|delivery_id| state.deliveries.get(&delivery_id).cloned())
                .collect());
        }

        self.legacy_cancel_message(&team_id, &message_id).await
    }

    async fn get_view_snapshot(
        &self,
        request: TeamViewSnapshotRequest,
    ) -> Result<TeamViewSnapshotResponse, RuntimeError> {
        self.ensure_enabled()?;

        let team = self.team_with_members(request.team_id.as_str()).await?;
        let messages_page = self
            .list_messages(TeamListMessagesRequest {
                team_id: request.team_id,
                cursor: request.message_cursor,
                limit: request.message_limit,
            })
            .await?;

        let include_delivery_map = request.include_delivery_map.unwrap_or(true);
        let recipient_filter = request.delivery_recipient_filter;

        let mut deliveries_by_message_id = BTreeMap::new();
        if include_delivery_map {
            let state = self.state.read().await;
            for message in &messages_page.messages {
                let delivery_ids = state
                    .message_delivery_ids
                    .get(&message.id)
                    .cloned()
                    .unwrap_or_default();
                let mut rows = Vec::new();
                for delivery_id in delivery_ids {
                    let Some(delivery) = state.deliveries.get(&delivery_id) else {
                        continue;
                    };
                    if recipient_filter
                        .as_deref()
                        .map(|recipient| delivery.recipient_agent_id == recipient)
                        .unwrap_or(true)
                    {
                        rows.push(delivery.clone());
                    }
                }
                deliveries_by_message_id.insert(message.id.clone(), rows);
            }
        }

        Ok(TeamViewSnapshotResponse {
            team,
            messages: messages_page.messages,
            deliveries_by_message_id,
            next_message_cursor: messages_page.next_cursor,
            snapshot_at: now_ms(),
        })
    }

    fn replay_team_events(
        &self,
        team_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.store.list_runtime_events(
            Some((RuntimeEventScope::Team, team_id)),
            after_seq,
            limit.max(1),
        )
    }
}
