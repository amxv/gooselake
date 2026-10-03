use serde_json::Value;

use crate::{
    AgentDeliveryRecord, AgentMessageContextKind, AgentMessageRecord, RuntimeError, SendTurnInput,
};

use super::{
    is_terminal_status, is_valid_transition, normalize_policy, now_ms, DeliveryAttemptTrigger,
    RuntimeTeamCommsService, DELIVERY_POLICY_IMMEDIATE_INTERRUPT,
    DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY, DELIVERY_POLICY_NON_INTERRUPTING,
    DELIVERY_POLICY_START_NEW_TURN_ONLY, DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_FAILED,
    DELIVERY_STATUS_INJECTED, DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_PENDING,
};

impl RuntimeTeamCommsService {
    pub(super) async fn recover_startup_agent_deliveries(&self) -> Result<usize, RuntimeError> {
        let injecting = {
            let state = self.state.read().await;
            state
                .agent_deliveries
                .values()
                .filter(|delivery| delivery.status == DELIVERY_STATUS_INJECTING)
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut recovered = 0usize;
        for delivery in injecting {
            if let Some(turn_id) = self
                .runtime
                .admitted_turn_for_correlation(&delivery.recipient_agent_id, &delivery.message_id)
                .await
            {
                let injected = self
                    .transition_agent_delivery_status(
                        &delivery,
                        DELIVERY_STATUS_INJECTED,
                        Some("startup_recovered_admitted_turn".to_string()),
                        None,
                        None,
                    )
                    .await?;
                self.transition_agent_delivery_with_turn_id(&injected.id, turn_id)
                    .await?;
            } else {
                self.transition_agent_delivery_status(
                    &delivery,
                    DELIVERY_STATUS_DEFERRED,
                    None,
                    Some("startup_requeue_before_turn_admission".to_string()),
                    Some("no durable correlated turn admission exists".to_string()),
                )
                .await?;
            }
            recovered += 1;
        }
        Ok(recovered)
    }

    pub(super) async fn inject_agent_delivery(
        &self,
        delivery_id: &str,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        let recipient = {
            let state = self.state.read().await;
            state
                .agent_deliveries
                .get(delivery_id)
                .map(|delivery| delivery.recipient_agent_id.clone())
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?
        };
        let guard = self.get_or_create_recipient_guard(&recipient).await;
        let _recipient_lock = guard.lock().await;

        let (delivery, message) = {
            let state = self.state.read().await;
            let delivery = state
                .agent_deliveries
                .get(delivery_id)
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?;
            let message = state
                .agent_messages
                .get(&delivery.message_id)
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::InvalidState(format!(
                        "delivery {} references missing message {}",
                        delivery.id, delivery.message_id
                    ))
                })?;
            (delivery, message)
        };
        if !matches!(
            delivery.status.as_str(),
            DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
        ) {
            return Ok(delivery);
        }
        if self
            .find_agent_recipient_queue_blocker(&delivery)
            .await
            .is_some()
        {
            if delivery.status == DELIVERY_STATUS_PENDING {
                return self
                    .transition_agent_delivery_status(
                        &delivery,
                        DELIVERY_STATUS_DEFERRED,
                        None,
                        None,
                        None,
                    )
                    .await;
            }
            return Ok(delivery);
        }

        let policy = normalize_policy(
            delivery
                .effective_policy
                .as_deref()
                .unwrap_or(message.policy.as_str()),
        )?;
        let recipient_session = match self.runtime.get_session(&delivery.recipient_agent_id).await {
            Ok(session) => session,
            Err(error) => {
                return self
                    .transition_agent_delivery_status(
                        &delivery,
                        DELIVERY_STATUS_FAILED,
                        None,
                        Some("recipient_session_not_found".to_string()),
                        Some(error.to_string()),
                    )
                    .await;
            }
        };
        if matches!(recipient_session.status.as_str(), "closed" | "failed") {
            return self
                .transition_agent_delivery_status(
                    &delivery,
                    DELIVERY_STATUS_FAILED,
                    None,
                    Some("recipient_session_closed".to_string()),
                    Some(format!(
                        "recipient session {} unavailable in status {}",
                        recipient_session.id, recipient_session.status
                    )),
                )
                .await;
        }

        if let Some(active_turn_id) = recipient_session.active_turn_id.as_deref() {
            match policy.as_str() {
                DELIVERY_POLICY_NON_INTERRUPTING | DELIVERY_POLICY_START_NEW_TURN_ONLY => {
                    return self
                        .transition_agent_delivery_status(
                            &delivery,
                            DELIVERY_STATUS_DEFERRED,
                            None,
                            None,
                            None,
                        )
                        .await;
                }
                DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY => {
                    if trigger != DeliveryAttemptTrigger::TurnCompletedBoundary {
                        return self
                            .transition_agent_delivery_status(
                                &delivery,
                                DELIVERY_STATUS_DEFERRED,
                                None,
                                None,
                                None,
                            )
                            .await;
                    }
                    self.runtime
                        .interrupt_turn(&recipient_session.id, active_turn_id)
                        .await
                        .map_err(|error| {
                            RuntimeError::InvalidState(format!(
                                "interrupt_after_tool_boundary failed for delivery {}: {}",
                                delivery.id, error
                            ))
                        })?;
                }
                DELIVERY_POLICY_IMMEDIATE_INTERRUPT => {
                    self.runtime
                        .interrupt_turn(&recipient_session.id, active_turn_id)
                        .await
                        .map_err(|error| {
                            RuntimeError::InvalidState(format!(
                                "immediate_interrupt failed for delivery {}: {}",
                                delivery.id, error
                            ))
                        })?;
                }
                _ => {
                    return self
                        .transition_agent_delivery_status(
                            &delivery,
                            DELIVERY_STATUS_DEFERRED,
                            None,
                            None,
                            None,
                        )
                        .await;
                }
            }
        }

        let injecting = self
            .transition_agent_delivery_status(
                &delivery,
                DELIVERY_STATUS_INJECTING,
                None,
                None,
                None,
            )
            .await?;
        let sender_alias = self
            .store
            .get_workspace_agent_by_id(&message.sender_agent_id)?
            .map(|profile| profile.alias)
            .unwrap_or_else(|| message.sender_agent_id.clone());
        let injected_input = build_agent_injected_input(&message, &sender_alias);
        let result = self
            .runtime
            .send_turn(
                &recipient_session.id,
                SendTurnInput {
                    input: injected_input,
                    expected_turn_id: None,
                    permission_mode: None,
                    projection_source: Some(
                        crate::TurnInputProjectionSource::AgentMessageDeliveryTransport,
                    ),
                    user_input_snapshot: None,
                    correlation_id: Some(message.id.clone()),
                },
            )
            .await;

        match result {
            Ok(ack) => {
                let injected = self
                    .transition_agent_delivery_status(
                        &injecting,
                        DELIVERY_STATUS_INJECTED,
                        Some("runtime_send_turn".to_string()),
                        None,
                        None,
                    )
                    .await?;
                self.transition_agent_delivery_with_turn_id(&injected.id, ack.turn_id)
                    .await
            }
            Err(error) if matches!(error, RuntimeError::InvalidState(_)) => {
                self.transition_agent_delivery_status(
                    &injecting,
                    DELIVERY_STATUS_DEFERRED,
                    None,
                    Some("turn_ownership_rejected".to_string()),
                    Some(error.to_string()),
                )
                .await
            }
            Err(error) => {
                self.transition_agent_delivery_status(
                    &injecting,
                    DELIVERY_STATUS_FAILED,
                    None,
                    Some("provider_rejected".to_string()),
                    Some(error.to_string()),
                )
                .await
            }
        }
    }

    async fn transition_agent_delivery_with_turn_id(
        &self,
        delivery_id: &str,
        turn_id: String,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        let updated = {
            let mut state = self.state.write().await;
            let delivery = state
                .agent_deliveries
                .get_mut(delivery_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?;
            delivery.injected_turn_id = Some(turn_id);
            delivery.updated_at = now_ms();
            delivery.clone()
        };
        self.persist_agent_delivery_with_legacy_mirror(&updated)
            .await?;
        Ok(updated)
    }

    pub(super) async fn persist_agent_delivery_with_legacy_mirror(
        &self,
        updated: &AgentDeliveryRecord,
    ) -> Result<(), RuntimeError> {
        self.store.upsert_agent_delivery(updated)?;
        let legacy = {
            let mut state = self.state.write().await;
            state.deliveries.get_mut(&updated.id).map(|delivery| {
                delivery.provider = updated.provider.clone();
                delivery.status = updated.status.clone();
                delivery.effective_policy = updated.effective_policy.clone();
                delivery.injection_strategy = updated.injection_strategy.clone();
                delivery.injected_turn_id = updated.injected_turn_id.clone();
                delivery.last_error_code = updated.last_error_code.clone();
                delivery.last_error_message = updated.last_error_message.clone();
                delivery.updated_at = updated.updated_at;
                delivery.clone()
            })
        };
        if let Some(legacy) = legacy {
            self.store.upsert_team_delivery(&legacy)?;
        }
        Ok(())
    }

    async fn transition_agent_delivery_status(
        &self,
        current: &AgentDeliveryRecord,
        next_status: &str,
        injection_strategy: Option<String>,
        last_error_code: Option<String>,
        last_error_message: Option<String>,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        if !is_valid_transition(&current.status, next_status) {
            if current.status == next_status {
                return Ok(current.clone());
            }
            return Err(RuntimeError::InvalidState(format!(
                "invalid agent delivery transition {} -> {}",
                current.status, next_status
            )));
        }
        let updated = {
            let mut state = self.state.write().await;
            let delivery = state
                .agent_deliveries
                .get_mut(&current.id)
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", current.id)))?;
            if delivery.status != current.status {
                return Ok(delivery.clone());
            }
            delivery.status = next_status.to_string();
            delivery.updated_at = now_ms();
            if let Some(strategy) = injection_strategy {
                delivery.injection_strategy = Some(strategy);
            }
            if next_status == DELIVERY_STATUS_FAILED {
                delivery.last_error_code = last_error_code;
                delivery.last_error_message = last_error_message;
            } else {
                delivery.last_error_code = None;
                delivery.last_error_message = None;
            }
            if next_status != DELIVERY_STATUS_INJECTED {
                delivery.injected_turn_id = None;
            }
            delivery.clone()
        };
        self.persist_agent_delivery_with_legacy_mirror(&updated)
            .await?;
        Ok(updated)
    }

    async fn find_agent_recipient_queue_blocker(
        &self,
        delivery: &AgentDeliveryRecord,
    ) -> Option<String> {
        let state = self.state.read().await;
        let current_key = (delivery.created_at, delivery.id.as_str());
        let mut candidates = state
            .agent_recipient_delivery_ids
            .get(&delivery.recipient_agent_id)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| state.agent_deliveries.get(id))
            .filter(|candidate| candidate.id != delivery.id)
            .filter(|candidate| !is_terminal_status(&candidate.status))
            .filter(|candidate| (candidate.created_at, candidate.id.as_str()) < current_key)
            .map(|candidate| (candidate.created_at, candidate.id.clone()))
            .collect::<Vec<_>>();
        candidates.extend(
            state
                .recipient_delivery_ids
                .get(&delivery.recipient_agent_id)
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter_map(|id| state.deliveries.get(id))
                .filter(|candidate| candidate.id != delivery.id)
                .filter(|candidate| !is_terminal_status(&candidate.status))
                .filter(|candidate| (candidate.created_at, candidate.id.as_str()) < current_key)
                .map(|candidate| (candidate.created_at, candidate.id.clone())),
        );
        candidates.sort();
        candidates.first().map(|(_, id)| id.clone())
    }

    pub(super) async fn resume_deferred_agent_for_recipient(
        &self,
        recipient_agent_id: &str,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<(), RuntimeError> {
        let deferred_ids = {
            let state = self.state.read().await;
            state
                .agent_recipient_delivery_ids
                .get(recipient_agent_id)
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter_map(|delivery_id| state.agent_deliveries.get(delivery_id))
                .filter(|delivery| delivery.status == DELIVERY_STATUS_DEFERRED)
                .map(|delivery| delivery.id.clone())
                .collect::<Vec<_>>()
        };
        for delivery_id in deferred_ids {
            let _ = self.inject_agent_delivery(&delivery_id, trigger).await;
        }
        Ok(())
    }
}

fn build_agent_injected_input(message: &AgentMessageRecord, sender_alias: &str) -> Vec<Value> {
    let kind = if message.scope == "broadcast" {
        "broadcast"
    } else {
        "direct"
    };
    let context = match message.context_kind {
        AgentMessageContextKind::WorkspaceTeam => "workspace",
        AgentMessageContextKind::GlobalDirect => "global",
        AgentMessageContextKind::LegacyTeam => "legacy",
    };
    let prefix = format!(
        "<agent_msg kind=\"{}\" sender=\"{}\" context=\"{}\" attachments=\"{}\">",
        kind,
        escape_xml_attr(sender_alias),
        context,
        message.image_paths.len()
    );
    let mut input = vec![serde_json::json!({ "type": "text", "text": prefix })];
    if let Value::Array(items) = message.input.clone() {
        input.extend(items);
    }
    input.extend(message.image_paths.iter().map(|path| {
        serde_json::json!({
            "type": "image",
            "path": path,
        })
    }));
    input.push(serde_json::json!({
        "type": "text",
        "text": "</agent_msg>",
    }));
    input
}

fn escape_xml_attr(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
