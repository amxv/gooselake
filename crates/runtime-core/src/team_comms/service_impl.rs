use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;

use crate::{
    RuntimeError, RuntimeEventRecord, RuntimeEventScope, TeamBroadcastRequest,
    TeamCancelMessageRequest, TeamCommsService, TeamCreateRequest, TeamDeliveryRecord,
    TeamGetDeliveriesRequest, TeamInterruptAllRequest, TeamInterruptAllResponse, TeamJoinRequest,
    TeamListMessagesRequest, TeamListMessagesResponse, TeamMemberRecord, TeamMessageAck,
    TeamRecord, TeamRemoveMemberRequest, TeamRetryDeliveryRequest, TeamSendDirectRequest,
    TeamSetLeadRequest, TeamViewSnapshotRequest, TeamViewSnapshotResponse, TeamWithMembers,
};

use super::{
    ensure_member, idempotency_index_key, normalize_non_empty, normalize_non_empty_input,
    normalized_non_empty, now_ms, remove_delivery_from_recipient_index, DeliveryAttemptTrigger,
    RuntimeTeamCommsService, DELIVERY_STATUS_CANCELLED, DELIVERY_STATUS_DEFERRED,
    DELIVERY_STATUS_FAILED, DELIVERY_STATUS_PENDING,
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
        self.ensure_enabled()?;
        let name = normalize_non_empty(request.name.as_str(), "name")?;
        let lead_agent_id = normalize_non_empty(request.lead_agent_id.as_str(), "lead_agent_id")?;
        let created_by = request.created_by.unwrap_or_else(|| "user".to_string());
        let now = now_ms();

        self.require_session_active(&lead_agent_id).await?;
        for member in &request.member_agent_ids {
            self.require_session_active(member).await?;
        }

        let team = {
            let mut state = self.state.write().await;
            let team_id = self.allocate_team_id(&state);
            let mut members = HashMap::new();

            let lead_member = TeamMemberRecord {
                team_id: team_id.clone(),
                agent_id: lead_agent_id.clone(),
                title: None,
                joined_at: now,
                added_by: created_by.clone(),
                creator_agent_id: None,
                creator_compaction_subscription: "auto".to_string(),
                worktree_id: None,
            };
            members.insert(lead_member.agent_id.clone(), lead_member.clone());

            for member_agent_id in &request.member_agent_ids {
                if member_agent_id == &lead_agent_id {
                    continue;
                }
                let member = TeamMemberRecord {
                    team_id: team_id.clone(),
                    agent_id: member_agent_id.clone(),
                    title: None,
                    joined_at: now,
                    added_by: created_by.clone(),
                    creator_agent_id: None,
                    creator_compaction_subscription: "auto".to_string(),
                    worktree_id: None,
                };
                members.insert(member.agent_id.clone(), member);
            }

            let team = TeamRecord {
                id: team_id.clone(),
                name,
                lead_agent_id,
                created_by,
                created_at: now,
                updated_at: now,
                deleted_at: None,
            };

            state.teams.insert(team_id.clone(), team.clone());
            state.members_by_team.insert(team_id, members);
            team
        };

        self.store.upsert_team(&team)?;
        let members = {
            let state = self.state.read().await;
            state
                .members_by_team
                .get(&team.id)
                .map(|members| members.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        for member in &members {
            self.store.upsert_team_member(member)?;
        }

        let _ = self
            .append_team_event(
                team.id.as_str(),
                "team.created",
                serde_json::json!({ "team": team, "members": members }),
                Some(team.lead_agent_id.clone()),
            )
            .await;

        self.team_with_members(team.id.as_str()).await
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
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let agent_id = normalize_non_empty(request.agent_id.as_str(), "agent_id")?;
        self.require_session_active(agent_id.as_str()).await?;

        let now = now_ms();
        let updated_team = {
            let mut state = self.state.write().await;
            let lead_agent_id = state
                .teams
                .get(&team_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
            let lead_for_added_by = lead_agent_id.lead_agent_id.clone();
            let members = state.members_by_team.entry(team_id.clone()).or_default();
            if members.contains_key(&agent_id) {
                return Err(RuntimeError::InvalidState(format!(
                    "agent {} is already a member of team {}",
                    agent_id, team_id
                )));
            }
            let member = TeamMemberRecord {
                team_id: team_id.clone(),
                agent_id: agent_id.clone(),
                title: normalized_non_empty(request.title.as_deref()),
                joined_at: now,
                added_by: request.added_by.unwrap_or(lead_for_added_by),
                creator_agent_id: request.creator_agent_id,
                creator_compaction_subscription: request
                    .creator_compaction_subscription
                    .unwrap_or_else(|| "auto".to_string()),
                worktree_id: request.worktree_id,
            };
            members.insert(agent_id.clone(), member.clone());
            let team = state
                .teams
                .get_mut(&team_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
            team.updated_at = now;
            (team.clone(), member)
        };

        self.store.upsert_team(&updated_team.0)?;
        self.store.upsert_team_member(&updated_team.1)?;

        let _ = self
            .append_team_event(
                team_id.as_str(),
                "team.member_joined",
                serde_json::json!({ "team": updated_team.0, "member": updated_team.1 }),
                Some(agent_id),
            )
            .await;

        self.team_with_members(team_id.as_str()).await
    }

    async fn remove_team_member(
        &self,
        request: TeamRemoveMemberRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let agent_id = normalize_non_empty(request.agent_id.as_str(), "agent_id")?;

        let (updated_team, removed_agent_id) = {
            let mut state = self.state.write().await;
            let team_snapshot = state
                .teams
                .get(&team_id)
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
            let team = state
                .members_by_team
                .get_mut(&team_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("team {} members", team_id)))?;
            if !team.contains_key(&agent_id) {
                return Err(RuntimeError::NotFound(format!(
                    "agent {} not in team {}",
                    agent_id, team_id
                )));
            }
            if team.len() <= 1 {
                return Err(RuntimeError::InvalidState(format!(
                    "cannot remove last member from team {}; delete team instead",
                    team_id
                )));
            }
            team.remove(&agent_id);
            let mut new_lead = team_snapshot.lead_agent_id.clone();
            if team_snapshot.lead_agent_id == agent_id {
                let next_lead = team
                    .values()
                    .min_by(|left, right| {
                        left.joined_at
                            .cmp(&right.joined_at)
                            .then_with(|| left.agent_id.cmp(&right.agent_id))
                    })
                    .map(|member| member.agent_id.clone())
                    .ok_or_else(|| {
                        RuntimeError::InvalidState(format!(
                            "team {} lost lead during removal",
                            team_id
                        ))
                    })?;
                new_lead = next_lead;
            }
            let team = state
                .teams
                .get_mut(&team_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
            team.lead_agent_id = new_lead;
            team.updated_at = now_ms();
            (team.clone(), agent_id)
        };

        self.store.upsert_team(&updated_team)?;
        self.store
            .delete_team_member(team_id.as_str(), removed_agent_id.as_str())?;

        let _ = self
            .append_team_event(
                team_id.as_str(),
                "team.member_left",
                serde_json::json!({
                    "team": updated_team,
                    "agent_id": removed_agent_id,
                }),
                Some(removed_agent_id),
            )
            .await;

        self.team_with_members(team_id.as_str()).await
    }

    async fn set_team_lead(
        &self,
        request: TeamSetLeadRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(request.team_id.as_str(), "team_id")?;
        let lead_agent_id = normalize_non_empty(request.lead_agent_id.as_str(), "lead_agent_id")?;

        let updated_team = {
            let mut state = self.state.write().await;
            ensure_member(
                state.members_by_team.get(&team_id),
                lead_agent_id.as_str(),
                &team_id,
            )?;
            let team = state
                .teams
                .get_mut(&team_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
            if team.lead_agent_id == lead_agent_id {
                None
            } else {
                team.lead_agent_id = lead_agent_id.clone();
                team.updated_at = now_ms();
                Some(team.clone())
            }
        };

        let Some(updated_team) = updated_team else {
            return self.team_with_members(team_id.as_str()).await;
        };

        self.store.upsert_team(&updated_team)?;

        let _ = self
            .append_team_event(
                team_id.as_str(),
                "team.lead_changed",
                serde_json::json!({ "team": updated_team, "lead_agent_id": lead_agent_id }),
                Some(lead_agent_id),
            )
            .await;

        self.team_with_members(team_id.as_str()).await
    }

    async fn delete_team(&self, team_id: &str) -> Result<(), RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(team_id, "team_id")?;

        let (deleted, cancelled_deliveries) = {
            let mut state = self.state.write().await;
            let mut team = state
                .teams
                .remove(&team_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
            state.members_by_team.remove(&team_id);

            let mut cancelled_deliveries = Vec::new();
            let mut message_ids = state.team_message_ids.remove(&team_id).unwrap_or_default();
            let team_delivery_ids = state.team_delivery_ids.remove(&team_id).unwrap_or_default();
            for message_id in message_ids.clone() {
                if let Some(message) = state.messages.remove(&message_id) {
                    if let Some(idempotency_key) =
                        normalized_non_empty(message.idempotency_key.as_deref())
                    {
                        state.idempotency_index.remove(&idempotency_index_key(
                            message.team_id.as_str(),
                            message.sender_agent_id.as_str(),
                            message.scope.as_str(),
                            idempotency_key.as_str(),
                        ));
                    }
                }
            }

            let mut removed_delivery_ids = HashSet::new();
            for delivery_id in team_delivery_ids {
                removed_delivery_ids.insert(delivery_id);
            }
            for message_id in message_ids.drain(..) {
                for delivery_id in state
                    .message_delivery_ids
                    .remove(&message_id)
                    .unwrap_or_default()
                {
                    removed_delivery_ids.insert(delivery_id);
                }
            }

            for delivery_id in removed_delivery_ids {
                let Some(mut delivery) = state.deliveries.remove(&delivery_id) else {
                    continue;
                };
                remove_delivery_from_recipient_index(
                    &mut state.recipient_delivery_ids,
                    delivery.recipient_agent_id.as_str(),
                    delivery.id.as_str(),
                );
                if matches!(
                    delivery.status.as_str(),
                    DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
                ) {
                    delivery.status = DELIVERY_STATUS_CANCELLED.to_string();
                    delivery.updated_at = now_ms();
                    cancelled_deliveries.push(delivery.clone());
                }
            }

            state
                .idempotency_index
                .retain(|key, _| !key.starts_with(&format!("{}|", team_id)));
            team.deleted_at = Some(now_ms());
            team.updated_at = now_ms();
            (team, cancelled_deliveries)
        };

        self.store.upsert_team(&deleted)?;
        for delivery in &cancelled_deliveries {
            self.store.upsert_team_delivery(delivery)?;
            let _ = self
                .append_team_event(
                    team_id.as_str(),
                    "team_delivery.cancelled",
                    serde_json::json!({ "delivery": delivery }),
                    Some(delivery.recipient_agent_id.clone()),
                )
                .await;
        }

        let _ = self
            .append_team_event(
                team_id.as_str(),
                "team.deleted",
                serde_json::json!({ "team": deleted }),
                Some(deleted.created_by.clone()),
            )
            .await;

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

        let updated = {
            let mut state = self.state.write().await;
            let delivery = state
                .deliveries
                .get_mut(&delivery_id)
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
            delivery.status = DELIVERY_STATUS_PENDING.to_string();
            delivery.injection_strategy = None;
            delivery.injected_turn_id = None;
            delivery.last_error_code = None;
            delivery.last_error_message = None;
            delivery.updated_at = now_ms();
            delivery.clone()
        };

        self.store.upsert_team_delivery(&updated)?;
        let _ = self
            .append_team_event(
                team_id.as_str(),
                "team_delivery.pending",
                serde_json::json!({ "delivery": updated }),
                Some(updated.recipient_agent_id.clone()),
            )
            .await;

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

        let cancelled = {
            let mut state = self.state.write().await;
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

            let delivery_ids = state
                .message_delivery_ids
                .get(&message_id)
                .cloned()
                .unwrap_or_default();

            for delivery_id in &delivery_ids {
                let Some(delivery) = state.deliveries.get(delivery_id) else {
                    continue;
                };
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

            let mut updated = Vec::new();
            for delivery_id in delivery_ids {
                if let Some(delivery) = state.deliveries.get_mut(&delivery_id) {
                    delivery.status = DELIVERY_STATUS_CANCELLED.to_string();
                    delivery.updated_at = now_ms();
                    updated.push(delivery.clone());
                }
            }
            updated
        };

        for delivery in &cancelled {
            self.store.upsert_team_delivery(delivery)?;
            let _ = self
                .append_team_event(
                    team_id.as_str(),
                    "team_delivery.cancelled",
                    serde_json::json!({ "delivery": delivery }),
                    Some(delivery.recipient_agent_id.clone()),
                )
                .await;
        }

        let _ = self
            .append_team_event(
                team_id.as_str(),
                "team_message.completed",
                serde_json::json!({ "message_id": message_id }),
                None,
            )
            .await;

        Ok(cancelled)
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
