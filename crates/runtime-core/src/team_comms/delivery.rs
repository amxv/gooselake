use crate::{RuntimeError, SendTurnInput, TeamDeliveryRecord};

use super::{
    build_injected_input, is_terminal_status, is_valid_transition, normalize_policy, now_ms,
    DeliveryAttemptTrigger, RuntimeTeamCommsService, DELIVERY_POLICY_IMMEDIATE_INTERRUPT,
    DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY, DELIVERY_POLICY_NON_INTERRUPTING,
    DELIVERY_POLICY_START_NEW_TURN_ONLY, DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_FAILED,
    DELIVERY_STATUS_INJECTED, DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_PENDING,
};

impl RuntimeTeamCommsService {
    pub(super) async fn inject_delivery(
        &self,
        delivery_id: &str,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        if let Some(legacy_delivery) = {
            let state = self.state.read().await;
            if state.agent_deliveries.contains_key(delivery_id) {
                state.deliveries.get(delivery_id).cloned()
            } else {
                None
            }
        } {
            return Ok(legacy_delivery);
        }

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

        let interrupt_active_turn =
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
                        Some(active_turn_id.to_string())
                    }
                    DELIVERY_POLICY_IMMEDIATE_INTERRUPT => Some(active_turn_id.to_string()),
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
            } else {
                None
            };

        // The recipient mutex is local to one process. Only the successful
        // durable generation-specific Critical event/CAS transaction may
        // release a native provider send *or* an interrupt side effect.
        let injecting = self
            .claim_legacy_delivery_injection(&delivery, trigger)
            .await?;
        if let Some(active_turn_id) = interrupt_active_turn {
            self.runtime
                .interrupt_turn(&recipient_session.id, &active_turn_id)
                .await
                .map_err(|error| {
                    RuntimeError::InvalidState(format!(
                        "legacy delivery {} reserved native injection; interrupt outcome requires review: {}",
                        delivery.id, error
                    ))
                })?;
        }

        let injected_input = build_injected_input(&message, &recipient_session.id);
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
                    turn_settings: Default::default(),
                },
            )
            .await;

        match result {
            Ok(ack) => self
                .commit_legacy_delivery_injected(&injecting, ack.turn_id, trigger)
                .await,
            Err(error) => Err(RuntimeError::InvalidState(format!(
                "legacy delivery {} native send outcome is unproven ({}); retained durable injecting claim for explicit reconciliation, not automatic retry",
                injecting.id, error
            ))),
        }
    }

    async fn claim_legacy_delivery_injection(
        &self,
        previous: &TeamDeliveryRecord,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        let mut state = self.state.write().await;
        if state.deliveries.get(&previous.id) != Some(previous)
            || state.agent_deliveries.contains_key(&previous.id)
        {
            return Err(RuntimeError::Conflict(
                "legacy injection lost in-process delivery ownership; no native dispatch".into(),
            ));
        }
        let revision = previous.updated_at.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidState("legacy injection delivery revision exhausted".into())
        })?;
        let mut injecting = previous.clone();
        injecting.status = DELIVERY_STATUS_INJECTING.into();
        injecting.updated_at = now_ms().max(revision);
        let mut event = self.new_team_event(
            &previous.team_id,
            "team_delivery.injecting",
            serde_json::json!({
                "delivery":injecting,
                "trigger":format!("{trigger:?}").to_ascii_lowercase(),
            }),
            Some(previous.recipient_agent_id.clone()),
        );
        // Generation-derived ID is also the lookup key for a later
        // acknowledged native outcome. Do not depend on process-local event
        // nonce or inspect a growing history for this authority.
        event.event_id = format!(
            "evt_legacy_delivery_{}_injecting_{}",
            previous.id, injecting.updated_at
        );
        event.created_at = injecting.updated_at;
        // `false` and an ambiguous SQLite error are *never* retryable native
        // dispatch. The old cache remains unchanged until true is confirmed.
        if !self
            .store
            .claim_legacy_team_delivery_injection(previous, &injecting, &event)?
        {
            return Err(RuntimeError::Conflict(
                "another legacy delivery owner advanced the durable generation; no native dispatch"
                    .into(),
            ));
        }
        state
            .deliveries
            .insert(injecting.id.clone(), injecting.clone());
        Ok(injecting)
    }

    pub(super) async fn commit_legacy_delivery_injected(
        &self,
        before: &TeamDeliveryRecord,
        turn_id: String,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        let mut state = self.state.write().await;
        if state.deliveries.get(&before.id) != Some(before)
            || state.agent_deliveries.contains_key(&before.id)
        {
            return Err(RuntimeError::Conflict(
                "legacy acknowledged native turn lost injecting cache authority; no redispatch"
                    .into(),
            ));
        }
        let revision = before.updated_at.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidState("legacy injected delivery revision exhausted".into())
        })?;
        let mut injected = before.clone();
        injected.status = DELIVERY_STATUS_INJECTED.into();
        injected.injection_strategy = Some("runtime_send_turn".into());
        injected.injected_turn_id = Some(turn_id.clone());
        injected.last_error_code = None;
        injected.last_error_message = None;
        injected.updated_at = now_ms().max(revision);
        let mut event = self.new_team_event(
            &before.team_id,
            "team_delivery.injected",
            serde_json::json!({
                "delivery":injected,
                "before_updated_at":before.updated_at,
                "trigger":format!("{trigger:?}").to_ascii_lowercase(),
            }),
            Some(before.recipient_agent_id.clone()),
        );
        event.event_id = format!(
            "evt_legacy_delivery_{}_injected_{}",
            before.id, before.updated_at
        );
        event.created_at = injected.updated_at;
        event.turn_id = Some(turn_id);
        // Native acknowledgement has already happened. A failed/ambiguous
        // journal result leaves injecting in memory and SQLite for explicit
        // reconciliation, never a second native call or retryable failure.
        let committed = self
            .store
            .commit_legacy_team_delivery_injected(before, &injected, &event)?;
        if committed != injected {
            return Err(RuntimeError::Conflict(
                "legacy injected Critical receipt does not match native acknowledgement".into(),
            ));
        }
        state
            .deliveries
            .insert(committed.id.clone(), committed.clone());
        Ok(committed)
    }
    pub(super) async fn transition_delivery_status(
        &self,
        current: &TeamDeliveryRecord,
        next_status: &str,
        injection_strategy: Option<String>,
        last_error_code: Option<String>,
        last_error_message: Option<String>,
        trigger: DeliveryAttemptTrigger,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        if current.status == next_status {
            // No generation changed, so do not emit a duplicate Critical event.
            return Ok(current.clone());
        }
        if !is_valid_transition(current.status.as_str(), next_status) {
            return Err(RuntimeError::InvalidState(format!(
                "invalid delivery transition {} -> {}",
                current.status, next_status
            )));
        }
        if injection_strategy.is_some()
            || !matches!(
                next_status,
                DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED | DELIVERY_STATUS_FAILED
            )
        {
            return Err(RuntimeError::InvalidState(
                "legacy pre-native status update cannot claim a native injection strategy".into(),
            ));
        }
        let mut state = self.state.write().await;
        if state.deliveries.get(&current.id) != Some(current)
            || state.agent_deliveries.contains_key(&current.id)
        {
            return Err(RuntimeError::Conflict(
                "legacy pre-native transition lost in-process generation authority".into(),
            ));
        }
        let revision = current.updated_at.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidState("legacy pre-native delivery revision exhausted".into())
        })?;
        let mut updated = current.clone();
        updated.status = next_status.into();
        updated.injection_strategy = None;
        updated.injected_turn_id = None;
        updated.updated_at = now_ms().max(revision);
        if next_status == DELIVERY_STATUS_FAILED {
            updated.last_error_code = last_error_code;
            updated.last_error_message = last_error_message;
        } else {
            updated.last_error_code = None;
            updated.last_error_message = None;
        }
        let trigger = format!("{trigger:?}").to_ascii_lowercase();
        let mut event = self.new_team_event(
            &current.team_id,
            &format!("team_delivery.{next_status}"),
            serde_json::json!({
                "delivery":updated,
                "before_status":current.status,
                "before_updated_at":current.updated_at,
                "trigger":trigger,
            }),
            Some(current.recipient_agent_id.clone()),
        );
        event.event_id = format!(
            "evt_legacy_delivery_{}_{}_{}",
            current.id, next_status, updated.updated_at
        );
        event.created_at = updated.updated_at;
        // A failed Critical event or lost SQLite response never changes the
        // cache. In particular a failed/deferred delivery does not become
        // eligible for native dispatch without a committed generation.
        let committed = self
            .store
            .commit_legacy_team_delivery_pre_native_transition(current, &updated, &event)?;
        if committed != updated {
            return Err(RuntimeError::Conflict(
                "legacy pre-native Critical transition disagreed with proposed generation".into(),
            ));
        }
        state
            .deliveries
            .insert(committed.id.clone(), committed.clone());
        Ok(committed)
    }

    async fn find_recipient_queue_blocker(&self, delivery: &TeamDeliveryRecord) -> Option<String> {
        let state = self.state.read().await;
        let current_key = (delivery.created_at, delivery.id.as_str());
        let recipient_ids = state
            .recipient_delivery_ids
            .get(&delivery.recipient_agent_id)
            .cloned()
            .unwrap_or_default();

        let mut candidates = Vec::new();
        for candidate_id in recipient_ids {
            let Some(candidate) = state.deliveries.get(&candidate_id) else {
                continue;
            };
            if candidate.id != delivery.id
                && !is_terminal_status(candidate.status.as_str())
                && (candidate.created_at, candidate.id.as_str()) < current_key
            {
                candidates.push((candidate.created_at, candidate.id.clone()));
            }
        }
        candidates.extend(
            state
                .agent_recipient_delivery_ids
                .get(&delivery.recipient_agent_id)
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter_map(|id| state.agent_deliveries.get(id))
                .filter(|candidate| !is_terminal_status(&candidate.status))
                .filter(|candidate| (candidate.created_at, candidate.id.as_str()) < current_key)
                .map(|candidate| (candidate.created_at, candidate.id.clone())),
        );
        candidates.sort();
        candidates.first().map(|(_, id)| id.clone())
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
                .filter(|delivery| !state.agent_deliveries.contains_key(&delivery.id))
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
