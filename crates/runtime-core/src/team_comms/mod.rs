mod delivery;
mod helpers;
mod service_impl;
mod state;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

use helpers::*;
use state::{DeliveryAttemptTrigger, TeamCommsState};

use crate::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    RuntimeSessionManager, RuntimeStore, SessionRecord, TeamDeliveryRecord, TeamMessageAck,
    TeamMessageRecord, TeamWithMembers,
};

const DELIVERY_POLICY_NON_INTERRUPTING: &str = "non_interrupting";
const DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY: &str = "interrupt_after_tool_boundary";
const DELIVERY_POLICY_IMMEDIATE_INTERRUPT: &str = "immediate_interrupt";
const DELIVERY_POLICY_START_NEW_TURN_ONLY: &str = "start_new_turn_only";

const DELIVERY_STATUS_PENDING: &str = "pending";
const DELIVERY_STATUS_DEFERRED: &str = "deferred";
const DELIVERY_STATUS_INJECTING: &str = "injecting";
const DELIVERY_STATUS_INJECTED: &str = "injected";
const DELIVERY_STATUS_FAILED: &str = "failed";
const DELIVERY_STATUS_CANCELLED: &str = "cancelled";

#[derive(Debug, Clone)]
pub struct RuntimeTeamCommsConfig {
    pub enabled: bool,
    pub max_pending_deliveries: usize,
}

pub struct RuntimeTeamCommsService {
    store: Arc<dyn RuntimeStore>,
    runtime: Arc<RuntimeSessionManager>,
    config: RuntimeTeamCommsConfig,
    state: RwLock<TeamCommsState>,
    next_team_id: AtomicU64,
    next_message_id: AtomicU64,
    next_delivery_id: AtomicU64,
    next_event_id: AtomicU64,
    event_id_nonce: String,
    recipient_injection_guards: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl RuntimeTeamCommsService {
    pub fn new(
        store: Arc<dyn RuntimeStore>,
        runtime: Arc<RuntimeSessionManager>,
        config: RuntimeTeamCommsConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        let hydrated = store.hydrate_runtime_state()?;
        let mut state = TeamCommsState::default();
        let mut max_team_id = 0_u64;
        let mut max_message_id = 0_u64;
        let mut max_delivery_id = 0_u64;

        for team in hydrated.teams {
            if team.deleted_at.is_none() {
                max_team_id = max_team_id.max(parse_counter(&team.id).unwrap_or(0));
                state.teams.insert(team.id.clone(), team);
            }
        }

        for member in hydrated.team_members {
            if state.teams.contains_key(&member.team_id) {
                state
                    .members_by_team
                    .entry(member.team_id.clone())
                    .or_default()
                    .insert(member.agent_id.clone(), member);
            }
        }

        for message in hydrated.team_messages {
            if !state.teams.contains_key(&message.team_id) {
                continue;
            }
            max_message_id = max_message_id.max(parse_counter(&message.id).unwrap_or(0));
            if let Some(idempotency_key) = normalized_non_empty(message.idempotency_key.as_deref())
            {
                state.idempotency_index.insert(
                    idempotency_index_key(
                        &message.team_id,
                        &message.sender_agent_id,
                        &message.scope,
                        &idempotency_key,
                    ),
                    message.id.clone(),
                );
            }
            state
                .team_message_ids
                .entry(message.team_id.clone())
                .or_default()
                .push(message.id.clone());
            state.messages.insert(message.id.clone(), message);
        }

        for delivery in hydrated.team_deliveries {
            if !state.teams.contains_key(&delivery.team_id) {
                continue;
            }
            if !state.messages.contains_key(&delivery.message_id) {
                continue;
            }
            max_delivery_id = max_delivery_id.max(parse_counter(&delivery.id).unwrap_or(0));
            state
                .team_delivery_ids
                .entry(delivery.team_id.clone())
                .or_default()
                .push(delivery.id.clone());
            state
                .message_delivery_ids
                .entry(delivery.message_id.clone())
                .or_default()
                .push(delivery.id.clone());
            state
                .recipient_delivery_ids
                .entry(delivery.recipient_agent_id.clone())
                .or_default()
                .push(delivery.id.clone());
            state.deliveries.insert(delivery.id.clone(), delivery);
        }

        let service = Arc::new(Self {
            store,
            runtime,
            config,
            state: RwLock::new(state),
            next_team_id: AtomicU64::new(max_team_id + 1),
            next_message_id: AtomicU64::new(max_message_id + 1),
            next_delivery_id: AtomicU64::new(max_delivery_id + 1),
            next_event_id: AtomicU64::new(1),
            event_id_nonce: format!("{:032x}", rand::random::<u128>()),
            recipient_injection_guards: Mutex::new(HashMap::new()),
        });

        let replay_service = Arc::clone(&service);
        tokio::spawn(async move {
            let mut receiver = replay_service.runtime.subscribe_events();
            while let Ok(event) = receiver.recv().await {
                if !matches!(
                    event.kind.as_str(),
                    "turn.completed" | "turn.interrupted" | "turn.failed"
                ) {
                    continue;
                }
                if let Some(session_id) = event.session_id {
                    let _ = replay_service
                        .resume_deferred_for_recipient(
                            session_id.as_str(),
                            DeliveryAttemptTrigger::TurnCompletedBoundary,
                        )
                        .await;
                }
            }
        });

        Ok(service)
    }

    pub async fn recover_startup_deferred_deliveries(&self) -> Result<usize, RuntimeError> {
        self.ensure_enabled()?;
        let recipients = {
            let state = self.state.read().await;
            state
                .recipient_delivery_ids
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };

        let mut retried = 0usize;
        for recipient_id in recipients {
            let recipient_session = match self.runtime.get_session(recipient_id.as_str()).await {
                Ok(session) => session,
                Err(_) => continue,
            };
            if matches!(recipient_session.status.as_str(), "closed" | "failed") {
                continue;
            }
            if recipient_session.active_turn_id.is_some() {
                continue;
            }
            let deferred_ids = {
                let state = self.state.read().await;
                state
                    .recipient_delivery_ids
                    .get(recipient_id.as_str())
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|delivery_id| {
                        state
                            .deliveries
                            .get(delivery_id)
                            .map(|delivery| delivery.status == DELIVERY_STATUS_DEFERRED)
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>()
            };
            for delivery_id in deferred_ids {
                if let Ok(updated) = self
                    .inject_delivery(
                        delivery_id.as_str(),
                        DeliveryAttemptTrigger::StartupRecovery,
                    )
                    .await
                {
                    if updated.status != DELIVERY_STATUS_DEFERRED {
                        retried += 1;
                    }
                }
            }
        }

        Ok(retried)
    }

    fn ensure_enabled(&self) -> Result<(), RuntimeError> {
        if self.config.enabled {
            return Ok(());
        }
        Err(RuntimeError::Unsupported(
            "team comms service is disabled".to_string(),
        ))
    }

    async fn append_team_event(
        &self,
        team_id: &str,
        kind: &str,
        payload: Value,
        session_id: Option<String>,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.store.append_runtime_event(&NewRuntimeEvent {
            event_id: format!(
                "evt_team_{}_{}_{}",
                team_id,
                self.event_id_nonce,
                self.next_event_id.fetch_add(1, Ordering::Relaxed)
            ),
            scope: RuntimeEventScope::Team,
            scope_id: team_id.to_string(),
            session_id,
            team_id: Some(team_id.to_string()),
            turn_id: None,
            kind: kind.to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload,
            provider: None,
            provider_seq: None,
            created_at: now_ms(),
        })
    }

    fn allocate_team_id(&self, state: &TeamCommsState) -> String {
        loop {
            let id = format!("team_{}", self.next_team_id.fetch_add(1, Ordering::Relaxed));
            if !state.teams.contains_key(&id) {
                return id;
            }
        }
    }

    fn allocate_message_id(&self, state: &TeamCommsState) -> String {
        loop {
            let id = format!(
                "msg_{}",
                self.next_message_id.fetch_add(1, Ordering::Relaxed)
            );
            if !state.messages.contains_key(&id) {
                return id;
            }
        }
    }

    fn allocate_delivery_id(&self, state: &TeamCommsState) -> String {
        loop {
            let id = format!(
                "dlv_{}",
                self.next_delivery_id.fetch_add(1, Ordering::Relaxed)
            );
            if !state.deliveries.contains_key(&id) {
                return id;
            }
        }
    }

    async fn require_session_active(
        &self,
        session_id: &str,
    ) -> Result<SessionRecord, RuntimeError> {
        let session = self.runtime.get_session(session_id).await?;
        if matches!(session.status.as_str(), "closed" | "failed") {
            return Err(RuntimeError::InvalidState(format!(
                "session {} is not available in status {}",
                session_id, session.status
            )));
        }
        Ok(session)
    }

    async fn create_message_and_deliveries(
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

        let (message, deliveries, disposition) = {
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
                let idx_key = idempotency_index_key(
                    team_id,
                    normalized_sender.as_str(),
                    &normalized_scope,
                    key,
                );
                if let Some(existing_id) = state.idempotency_index.get(&idx_key).cloned() {
                    if let Some(existing_message) = state.messages.get(&existing_id).cloned() {
                        let existing_deliveries = state
                            .message_delivery_ids
                            .get(&existing_id)
                            .into_iter()
                            .flat_map(|ids| ids.iter())
                            .filter_map(|delivery_id| state.deliveries.get(delivery_id).cloned())
                            .collect::<Vec<_>>();
                        return Ok(TeamMessageAck {
                            message: existing_message,
                            deliveries: existing_deliveries,
                            disposition: "existing".to_string(),
                        });
                    }
                }
            }

            let now = now_ms();
            let message_id = self.allocate_message_id(&state);
            let message = TeamMessageRecord {
                id: message_id.clone(),
                team_id: team_state_id.clone(),
                scope: normalized_scope.clone(),
                sender_agent_id: normalized_sender.clone(),
                recipient_agent_ids: Value::Array(
                    deduped_recipients
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect::<Vec<_>>(),
                ),
                input,
                image_paths: Value::Array(image_paths.into_iter().map(Value::String).collect()),
                priority: normalized_priority,
                policy: normalized_policy,
                correlation_id,
                reply_to_message_id,
                idempotency_key: normalized_idempotency_key.clone(),
                created_at: now,
            };

            let mut deliveries = Vec::new();
            for recipient in deduped_recipients {
                let provider =
                    recipient_provider_map
                        .get(&recipient)
                        .cloned()
                        .ok_or_else(|| {
                            RuntimeError::InvalidState(format!(
                                "missing provider mapping for recipient {}",
                                recipient
                            ))
                        })?;
                let delivery = TeamDeliveryRecord {
                    id: self.allocate_delivery_id(&state),
                    message_id: message.id.clone(),
                    team_id: team_state_id.clone(),
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
                };
                deliveries.push(delivery);
            }

            state.messages.insert(message.id.clone(), message.clone());
            state
                .team_message_ids
                .entry(team_state_id.clone())
                .or_default()
                .push(message.id.clone());

            if let Some(key) = normalized_idempotency_key.as_ref() {
                state.idempotency_index.insert(
                    idempotency_index_key(
                        team_id,
                        normalized_sender.as_str(),
                        &normalized_scope,
                        key,
                    ),
                    message.id.clone(),
                );
            }

            for delivery in &deliveries {
                state
                    .deliveries
                    .insert(delivery.id.clone(), delivery.clone());
                state
                    .team_delivery_ids
                    .entry(team_state_id.clone())
                    .or_default()
                    .push(delivery.id.clone());
                state
                    .message_delivery_ids
                    .entry(message.id.clone())
                    .or_default()
                    .push(delivery.id.clone());
                state
                    .recipient_delivery_ids
                    .entry(delivery.recipient_agent_id.clone())
                    .or_default()
                    .push(delivery.id.clone());
            }

            (message, deliveries, "created".to_string())
        };

        self.store.upsert_team_message(&message)?;
        for delivery in &deliveries {
            self.store.upsert_team_delivery(delivery)?;
        }

        let _ = self
            .append_team_event(
                team_id,
                "team_message.created",
                serde_json::json!({ "message": message, "deliveries": deliveries }),
                Some(normalized_sender),
            )
            .await;

        for delivery in &deliveries {
            let _ = self
                .append_team_event(
                    team_id,
                    "team_delivery.pending",
                    serde_json::json!({ "delivery": delivery }),
                    Some(delivery.recipient_agent_id.clone()),
                )
                .await;
        }

        Ok(TeamMessageAck {
            message,
            deliveries,
            disposition,
        })
    }

    async fn get_or_create_recipient_guard(&self, recipient: &str) -> Arc<Mutex<()>> {
        let mut guards = self.recipient_injection_guards.lock().await;
        guards
            .entry(recipient.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn queue_and_attempt_delivery(&self, ack: &TeamMessageAck) {
        for delivery in &ack.deliveries {
            let _ = self
                .inject_delivery(delivery.id.as_str(), DeliveryAttemptTrigger::Queue)
                .await;
        }
    }

    async fn team_with_members(&self, team_id: &str) -> Result<TeamWithMembers, RuntimeError> {
        let state = self.state.read().await;
        let team = state
            .teams
            .get(team_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("team {}", team_id)))?;
        let mut members = state
            .members_by_team
            .get(team_id)
            .map(|members| members.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        members.sort_by(|left, right| {
            left.joined_at
                .cmp(&right.joined_at)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
        });
        Ok(TeamWithMembers { team, members })
    }
}
