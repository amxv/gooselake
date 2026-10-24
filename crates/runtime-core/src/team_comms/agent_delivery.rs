use serde_json::Value;

use crate::{AgentDeliveryRecord, AgentMessageRecord, RuntimeError, SendTurnInput};

use super::{
    is_terminal_status, is_valid_transition, normalize_policy, now_ms, DeliveryAttemptTrigger,
    RuntimeTeamCommsService, DELIVERY_POLICY_IMMEDIATE_INTERRUPT,
    DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY, DELIVERY_POLICY_NON_INTERRUPTING,
    DELIVERY_POLICY_START_NEW_TURN_ONLY, DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_FAILED,
    DELIVERY_STATUS_INJECTED, DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_PENDING,
};

impl RuntimeTeamCommsService {
    pub(super) async fn cache_agent_delivery_records(&self, rows: &[AgentDeliveryRecord]) {
        let mut state = self.state.write().await;
        for row in rows {
            if state.agent_deliveries.get(&row.id).is_some_and(|cached| {
                cached.status == "cancelled"
                    && cached.last_error_code.as_deref() == Some("workspace_retirement")
                    && (row.status != "cancelled"
                        || row.last_error_code.as_deref() != Some("workspace_retirement"))
            }) {
                continue;
            }
            state.agent_deliveries.insert(row.id.clone(), row.clone());
            if let Some(legacy) = state.deliveries.get_mut(&row.id) {
                legacy.status = row.status.clone();
                legacy.effective_policy = row.effective_policy.clone();
                legacy.injection_strategy = row.injection_strategy.clone();
                legacy.injected_turn_id = row.injected_turn_id.clone();
                legacy.last_error_code = row.last_error_code.clone();
                legacy.last_error_message = row.last_error_message.clone();
                legacy.updated_at = row.updated_at;
            }
        }
    }

    /// Retirement can settle delivery authority in a separate transaction.
    /// Cached pending work must not overwrite or dispatch that cancelled row.
    pub(super) async fn refresh_current_agent_delivery(
        &self,
        id: &str,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        match self.store.get_agent_delivery(id) {
            Ok(Some(row)) => {
                self.cache_agent_delivery_records(std::slice::from_ref(&row))
                    .await;
                Ok(row)
            }
            Ok(None) => Err(RuntimeError::NotFound(format!("delivery {id}"))),
            Err(RuntimeError::Unsupported(_)) => self
                .state
                .read()
                .await
                .agent_deliveries
                .get(id)
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("delivery {id}"))),
            Err(error) => Err(error),
        }
    }

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

        self.refresh_current_agent_delivery(delivery_id).await?;

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
        if self
            .store
            .retiring_operation_for_session(&message.sender_agent_id)?
            .is_some()
            || self
                .store
                .retiring_operation_for_session(&delivery.recipient_agent_id)?
                .is_some()
        {
            return self.refresh_current_agent_delivery(delivery_id).await;
        }
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

        let interrupt_active_turn =
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
                        Some(active_turn_id.to_string())
                    }
                    DELIVERY_POLICY_IMMEDIATE_INTERRUPT => Some(active_turn_id.to_string()),
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
            } else {
                None
            };

        // Claim before the first native effect. The retirement fence rejects
        // late claims, while an earlier claim remains visible as in-flight work.
        let (injecting, claimed) = self
            .try_transition_agent_delivery_status(
                &delivery,
                DELIVERY_STATUS_INJECTING,
                None,
                None,
                None,
            )
            .await?;
        if !claimed {
            return Ok(injecting);
        }
        if let Some(active_turn_id) = interrupt_active_turn {
            self.runtime
                .interrupt_turn(&recipient_session.id, &active_turn_id)
                .await
                .map_err(|error| {
                    RuntimeError::InvalidState(format!(
                        "{policy} failed for delivery {}: {error}",
                        delivery.id
                    ))
                })?;
        }
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
                    turn_settings: Default::default(),
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
            Err(error)
                if matches!(
                    error,
                    RuntimeError::InvalidState(_) | RuntimeError::OnboardingPending(_)
                ) =>
            {
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
        if let Err(error) = self.store.upsert_agent_delivery(updated) {
            // A concurrent retirement may have won after the cache read.
            // Restore its authoritative status even when this mutation fails.
            let _ = self.refresh_current_agent_delivery(&updated.id).await;
            return Err(error);
        }
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
            if let Err(error) = self.store.upsert_team_delivery(&legacy) {
                let _ = self.refresh_current_agent_delivery(&updated.id).await;
                return Err(error);
            }
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
        self.try_transition_agent_delivery_status(
            current,
            next_status,
            injection_strategy,
            last_error_code,
            last_error_message,
        )
        .await
        .map(|(delivery, _)| delivery)
    }

    async fn try_transition_agent_delivery_status(
        &self,
        current: &AgentDeliveryRecord,
        next_status: &str,
        injection_strategy: Option<String>,
        last_error_code: Option<String>,
        last_error_message: Option<String>,
    ) -> Result<(AgentDeliveryRecord, bool), RuntimeError> {
        let authoritative = self.refresh_current_agent_delivery(&current.id).await?;
        if authoritative.status != current.status {
            return Ok((authoritative, false));
        }
        if !is_valid_transition(&current.status, next_status) {
            if current.status == next_status {
                return Ok((current.clone(), false));
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
                return Ok((delivery.clone(), false));
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
        Ok((updated, true))
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
        self.get_agent_deliveries_impl(crate::AgentDeliveryListRequest {
            message_id: None,
            recipient_agent_id: Some(recipient_agent_id.to_string()),
        })
        .await?;
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
        "dm"
    };
    let context = message.context_kind.as_str();
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

#[cfg(test)]
mod compact_envelope_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn message_context_uses_friendly_identity_and_preserves_image_order() {
        let message = AgentMessageRecord {
            id: "msg_1".into(),
            scope: "direct".into(),
            context_kind: crate::AgentMessageContextKind::WorkspaceTeam,
            workspace_id: Some("workspace_internal".into()),
            legacy_team_id: None,
            sender_agent_id: "sess_hidden".into(),
            recipient_agent_ids: vec!["sess_recipient".into()],
            input: json!([{"type":"text","text":"Review this"}]),
            image_paths: vec!["/tmp/first.png".into(), "/tmp/second.jpg".into()],
            priority: "normal".into(),
            policy: "non_interrupting".into(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
            created_at: 10,
        };
        let items = build_agent_injected_input(&message, "Alice & \"Bob\" <Lead>");
        let prefix = items[0]["text"].as_str().unwrap();
        assert!(prefix.contains("kind=\"dm\""));
        assert!(prefix.contains("context=\"workspace_team\""));
        assert!(prefix.contains("sender=\"Alice &amp; &quot;Bob&quot; &lt;Lead&gt;\""));
        assert!(prefix.contains("attachments=\"2\""));
        assert!(!prefix.contains("workspace_internal"));
        assert!(!prefix.contains("sess_hidden"));
        assert_eq!(items[1]["text"], "Review this");
        assert_eq!(items[2]["path"], "/tmp/first.png");
        assert_eq!(items[3]["path"], "/tmp/second.jpg");
        assert_eq!(items[4]["text"], "</agent_msg>");
    }
}
