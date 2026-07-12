use crate::{RuntimeError, RuntimeRecordMutation, SendTurnInput, TeamDeliveryRecord};

use super::{
    build_injected_input, is_terminal_status, is_valid_transition, normalize_policy, now_ms,
    DeliveryAttemptTrigger, RuntimeTeamCommsService, DELIVERY_POLICY_IMMEDIATE_INTERRUPT,
    DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY, DELIVERY_POLICY_NON_INTERRUPTING,
    DELIVERY_POLICY_START_NEW_TURN_ONLY, DELIVERY_STATUS_CANCELLED, DELIVERY_STATUS_DEFERRED,
    DELIVERY_STATUS_FAILED, DELIVERY_STATUS_INJECTED, DELIVERY_STATUS_INJECTING,
    DELIVERY_STATUS_PENDING,
};

impl RuntimeTeamCommsService {
    pub(super) async fn inject_delivery(
        &self,
        delivery_id: &str,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        let recipient = {
            let state = self.state.read().await;
            state
                .deliveries
                .get(delivery_id)
                .map(|delivery| delivery.recipient_agent_id.clone())
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?
        };

        let guard = self.get_or_create_recipient_guard(recipient.as_str()).await;
        let _recipient_lock = guard.lock().await;

        let (delivery, message) = {
            let state = self.state.read().await;
            let delivery = state
                .deliveries
                .get(delivery_id)
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", delivery_id)))?;
            let message = state
                .messages
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

        if let Some(_blocker_id) = self.find_recipient_queue_blocker(&delivery).await {
            if delivery.status == DELIVERY_STATUS_PENDING {
                return self
                    .transition_delivery_status(
                        &delivery,
                        DELIVERY_STATUS_DEFERRED,
                        None,
                        None,
                        None,
                        trigger,
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
                    .transition_delivery_status(
                        &delivery,
                        DELIVERY_STATUS_FAILED,
                        None,
                        Some("recipient_session_not_found".to_string()),
                        Some(error.to_string()),
                        trigger,
                    )
                    .await;
            }
        };

        if matches!(recipient_session.status.as_str(), "closed" | "failed") {
            return self
                .transition_delivery_status(
                    &delivery,
                    DELIVERY_STATUS_FAILED,
                    None,
                    Some("recipient_session_closed".to_string()),
                    Some(format!(
                        "recipient session {} unavailable in status {}",
                        recipient_session.id, recipient_session.status
                    )),
                    trigger,
                )
                .await;
        }

        if let Some(active_turn_id) = recipient_session.active_turn_id.as_deref() {
            match policy.as_str() {
                DELIVERY_POLICY_NON_INTERRUPTING | DELIVERY_POLICY_START_NEW_TURN_ONLY => {
                    return self
                        .transition_delivery_status(
                            &delivery,
                            DELIVERY_STATUS_DEFERRED,
                            None,
                            None,
                            None,
                            trigger,
                        )
                        .await;
                }
                DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY => {
                    if trigger != DeliveryAttemptTrigger::TurnCompletedBoundary {
                        return self
                            .transition_delivery_status(
                                &delivery,
                                DELIVERY_STATUS_DEFERRED,
                                None,
                                None,
                                None,
                                trigger,
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
                        .transition_delivery_status(
                            &delivery,
                            DELIVERY_STATUS_DEFERRED,
                            None,
                            None,
                            None,
                            trigger,
                        )
                        .await;
                }
            }
        }

        let injecting = self
            .transition_delivery_status(
                &delivery,
                DELIVERY_STATUS_INJECTING,
                None,
                None,
                None,
                trigger,
            )
            .await?;

        let injected_input = build_injected_input(&message, &recipient_session.id);
        let result = self
            .runtime
            .send_turn(
                &recipient_session.id,
                SendTurnInput {
                    input: injected_input,
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await;

        match result {
            Ok(ack) => {
                let injected = self
                    .transition_delivery_status(
                        &injecting,
                        DELIVERY_STATUS_INJECTED,
                        Some("runtime_send_turn".to_string()),
                        None,
                        None,
                        trigger,
                    )
                    .await?;
                let _ = self
                    .transition_delivery_with_turn_id(injected.id.as_str(), ack.turn_id)
                    .await;
                Ok(injected)
            }
            Err(error) => {
                if matches!(error, RuntimeError::InvalidState(_)) {
                    return self
                        .transition_delivery_status(
                            &injecting,
                            DELIVERY_STATUS_DEFERRED,
                            None,
                            Some("turn_ownership_rejected".to_string()),
                            Some(error.to_string()),
                            trigger,
                        )
                        .await;
                }
                self.transition_delivery_status(
                    &injecting,
                    DELIVERY_STATUS_FAILED,
                    None,
                    Some("provider_rejected".to_string()),
                    Some(error.to_string()),
                    trigger,
                )
                .await
            }
        }
    }

    async fn transition_delivery_with_turn_id(
        &self,
        delivery_id: &str,
        turn_id: String,
    ) -> Result<(), RuntimeError> {
        let maybe_delivery = {
            let mut state = self.state.write().await;
            let Some(delivery) = state.deliveries.get_mut(delivery_id) else {
                return Ok(());
            };
            delivery.injected_turn_id = Some(turn_id.clone());
            delivery.updated_at = now_ms();
            Some(delivery.clone())
        };

        if let Some(delivery) = maybe_delivery {
            let _ = self
                .append_team_event_with_mutations(
                    delivery.team_id.as_str(),
                    "team_delivery.injected",
                    serde_json::json!({ "delivery": delivery }),
                    Some(delivery.recipient_agent_id.clone()),
                    &[RuntimeRecordMutation::TeamDelivery(delivery.clone())],
                )
                .await;
        }

        Ok(())
    }
    async fn transition_delivery_status(
        &self,
        current: &TeamDeliveryRecord,
        next_status: &str,
        injection_strategy: Option<String>,
        last_error_code: Option<String>,
        last_error_message: Option<String>,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        if !is_valid_transition(current.status.as_str(), next_status) {
            if current.status == next_status {
                return Ok(current.clone());
            }
            return Err(RuntimeError::InvalidState(format!(
                "invalid delivery transition {} -> {}",
                current.status, next_status
            )));
        }

        let updated = {
            let mut state = self.state.write().await;
            let delivery = state
                .deliveries
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

        let kind = match next_status {
            DELIVERY_STATUS_PENDING => "team_delivery.pending",
            DELIVERY_STATUS_DEFERRED => "team_delivery.deferred",
            DELIVERY_STATUS_INJECTING => "team_delivery.injecting",
            DELIVERY_STATUS_INJECTED => "team_delivery.injected",
            DELIVERY_STATUS_FAILED => "team_delivery.failed",
            DELIVERY_STATUS_CANCELLED => "team_delivery.cancelled",
            _ => "team_delivery.updated",
        };
        let _ = self
            .append_team_event_with_mutations(
                updated.team_id.as_str(),
                kind,
                serde_json::json!({
                    "delivery": updated,
                    "trigger": format!("{:?}", trigger).to_ascii_lowercase(),
                }),
                Some(current.recipient_agent_id.clone()),
                &[RuntimeRecordMutation::TeamDelivery(updated.clone())],
            )
            .await;

        let should_complete = self.message_terminal_state(&current.message_id).await;
        if should_complete {
            let _ = self
                .append_team_event(
                    current.team_id.as_str(),
                    "team_message.completed",
                    serde_json::json!({
                        "message_id": current.message_id,
                        "team_id": current.team_id,
                    }),
                    Some(current.recipient_agent_id.clone()),
                )
                .await;
        }

        Ok(updated)
    }

    async fn message_terminal_state(&self, message_id: &str) -> bool {
        let state = self.state.read().await;
        let Some(delivery_ids) = state.message_delivery_ids.get(message_id) else {
            return true;
        };
        if delivery_ids.is_empty() {
            return true;
        }
        delivery_ids.iter().all(|delivery_id| {
            state
                .deliveries
                .get(delivery_id)
                .map(|delivery| is_terminal_status(delivery.status.as_str()))
                .unwrap_or(false)
        })
    }

    async fn find_recipient_queue_blocker(&self, delivery: &TeamDeliveryRecord) -> Option<String> {
        let state = self.state.read().await;
        let recipient_ids = state
            .recipient_delivery_ids
            .get(&delivery.recipient_agent_id)
            .cloned()
            .unwrap_or_default();

        let mut blocker = None;
        for candidate_id in recipient_ids {
            if candidate_id == delivery.id {
                break;
            }
            let Some(candidate) = state.deliveries.get(&candidate_id) else {
                continue;
            };
            if !is_terminal_status(candidate.status.as_str()) {
                blocker = Some(candidate.id.clone());
                break;
            }
        }
        blocker
    }

    pub(super) async fn resume_deferred_for_recipient(
        &self,
        recipient_agent_id: &str,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<(), RuntimeError> {
        let deferred_ids = {
            let state = self.state.read().await;
            state
                .recipient_delivery_ids
                .get(recipient_agent_id)
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter_map(|delivery_id| state.deliveries.get(delivery_id))
                .filter(|delivery| delivery.status == DELIVERY_STATUS_DEFERRED)
                .map(|delivery| delivery.id.clone())
                .collect::<Vec<_>>()
        };

        for delivery_id in deferred_ids {
            let _ = self.inject_delivery(delivery_id.as_str(), trigger).await;
        }

        Ok(())
    }
}
