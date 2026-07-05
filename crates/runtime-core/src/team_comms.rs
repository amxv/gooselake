use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

use crate::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    RuntimeSessionManager, RuntimeStore, SendTurnInput, SessionRecord, TeamBroadcastRequest,
    TeamCancelMessageRequest, TeamCommsService, TeamCreateRequest, TeamDeliveryRecord,
    TeamGetDeliveriesRequest, TeamInterruptAllRequest, TeamInterruptAllResponse, TeamJoinRequest,
    TeamListMessagesRequest, TeamListMessagesResponse, TeamMemberRecord, TeamMessageAck,
    TeamMessageRecord, TeamRecord, TeamRemoveMemberRequest, TeamRetryDeliveryRequest,
    TeamSendDirectRequest, TeamSetLeadRequest, TeamViewSnapshotRequest, TeamViewSnapshotResponse,
    TeamWithMembers,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryAttemptTrigger {
    Queue,
    Retry,
    TurnCompletedBoundary,
    StartupRecovery,
}

#[derive(Default)]
struct TeamCommsState {
    teams: HashMap<String, TeamRecord>,
    members_by_team: HashMap<String, HashMap<String, TeamMemberRecord>>,
    messages: HashMap<String, TeamMessageRecord>,
    deliveries: HashMap<String, TeamDeliveryRecord>,
    team_message_ids: HashMap<String, Vec<String>>,
    team_delivery_ids: HashMap<String, Vec<String>>,
    message_delivery_ids: HashMap<String, Vec<String>>,
    recipient_delivery_ids: HashMap<String, Vec<String>>,
    idempotency_index: HashMap<String, String>,
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

    async fn inject_delivery(
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
            self.store.upsert_team_delivery(&delivery)?;
            let _ = self
                .append_team_event(
                    delivery.team_id.as_str(),
                    "team_delivery.injected",
                    serde_json::json!({ "delivery": delivery }),
                    Some(delivery.recipient_agent_id.clone()),
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

        self.store.upsert_team_delivery(&updated)?;

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
            .append_team_event(
                updated.team_id.as_str(),
                kind,
                serde_json::json!({
                    "delivery": updated,
                    "trigger": format!("{:?}", trigger).to_ascii_lowercase(),
                }),
                Some(current.recipient_agent_id.clone()),
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

    async fn resume_deferred_for_recipient(
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

fn ensure_member(
    maybe_members: Option<&HashMap<String, TeamMemberRecord>>,
    agent_id: &str,
    team_id: &str,
) -> Result<(), RuntimeError> {
    if maybe_members
        .map(|members| members.contains_key(agent_id))
        .unwrap_or(false)
    {
        return Ok(());
    }
    Err(RuntimeError::InvalidState(format!(
        "agent {} is not a member of team {}",
        agent_id, team_id
    )))
}

fn remove_delivery_from_recipient_index(
    recipient_delivery_ids: &mut HashMap<String, Vec<String>>,
    recipient_agent_id: &str,
    delivery_id: &str,
) {
    let mut should_remove_key = false;
    if let Some(ids) = recipient_delivery_ids.get_mut(recipient_agent_id) {
        ids.retain(|candidate| candidate != delivery_id);
        should_remove_key = ids.is_empty();
    }
    if should_remove_key {
        recipient_delivery_ids.remove(recipient_agent_id);
    }
}

fn normalize_non_empty(value: &str, field: &str) -> Result<String, RuntimeError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(RuntimeError::InvalidState(format!(
            "{} cannot be empty",
            field
        )));
    }
    Ok(trimmed.to_string())
}

fn normalized_non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn normalize_non_empty_input(input: Value) -> Result<Value, RuntimeError> {
    let Value::Array(items) = input else {
        return Err(RuntimeError::InvalidState(
            "message input must be an array".to_string(),
        ));
    };
    if items.is_empty() {
        return Err(RuntimeError::InvalidState(
            "message input cannot be empty".to_string(),
        ));
    }
    Ok(Value::Array(items))
}

fn normalize_scope(scope: &str) -> Result<String, RuntimeError> {
    match scope.trim().to_ascii_lowercase().as_str() {
        "direct" => Ok("direct".to_string()),
        "broadcast" => Ok("broadcast".to_string()),
        value => Err(RuntimeError::InvalidState(format!(
            "unsupported message scope {}",
            value
        ))),
    }
}

fn normalize_priority(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return "normal".to_string();
    }
    normalized
}

fn normalize_policy(value: &str) -> Result<String, RuntimeError> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        DELIVERY_POLICY_NON_INTERRUPTING
        | DELIVERY_POLICY_INTERRUPT_AFTER_TOOL_BOUNDARY
        | DELIVERY_POLICY_IMMEDIATE_INTERRUPT
        | DELIVERY_POLICY_START_NEW_TURN_ONLY => Ok(normalized),
        _ => Err(RuntimeError::InvalidState(format!(
            "unsupported delivery policy {}",
            value
        ))),
    }
}

fn idempotency_index_key(team_id: &str, sender: &str, scope: &str, key: &str) -> String {
    format!("{}|{}|{}|{}", team_id, sender, scope, key)
}

fn parse_counter(value: &str) -> Option<u64> {
    value
        .rsplit('_')
        .next()
        .and_then(|suffix| suffix.parse::<u64>().ok())
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status,
        DELIVERY_STATUS_INJECTED | DELIVERY_STATUS_FAILED | DELIVERY_STATUS_CANCELLED
    )
}

fn is_valid_transition(current: &str, next: &str) -> bool {
    matches!(
        (current, next),
        (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_PENDING)
            | (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_DEFERRED)
            | (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_INJECTING)
            | (DELIVERY_STATUS_PENDING, DELIVERY_STATUS_CANCELLED)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_PENDING)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_DEFERRED)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_INJECTING)
            | (DELIVERY_STATUS_DEFERRED, DELIVERY_STATUS_CANCELLED)
            | (DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_INJECTED)
            | (DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_FAILED)
            | (DELIVERY_STATUS_INJECTING, DELIVERY_STATUS_DEFERRED)
    )
}

fn build_injected_input(message: &TeamMessageRecord, recipient_agent_id: &str) -> Vec<Value> {
    let scope = if message.scope == "broadcast" {
        "broadcast"
    } else {
        "dm"
    };
    let prefix = Value::String(format!(
        "<team_msg kind=\"{}\" sender=\"{}\" team_id=\"{}\">",
        scope, message.sender_agent_id, message.team_id
    ));
    let suffix = Value::String("</team_msg>".to_string());

    let mut input = Vec::new();
    input.push(serde_json::json!({ "type": "text", "text": prefix }));
    if let Value::Array(items) = message.input.clone() {
        input.extend(items);
    }
    if let Value::Array(paths) = &message.image_paths {
        input.extend(paths.iter().filter_map(|path| {
            path.as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|path| {
                    serde_json::json!({
                        "type": "image",
                        "path": path,
                    })
                })
        }));
    }
    input.push(serde_json::json!({
        "type": "text",
        "text": suffix,
        "recipient": recipient_agent_id,
    }));
    input
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};

    use crate::{
        ApprovalRecord, CreateSessionInput, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
        ProcessRecord, ProviderAuthStatus, ProviderCreateSessionRequest,
        ProviderInterruptTurnRequest, ProviderKind, ProviderMetadata, ProviderModel,
        ProviderRegistry, ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderSession,
        ProviderTurnAck, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest,
        RuntimeProvider, RuntimeStore, SessionRecord, TeamOperationDiagnosticRecord,
        TeamOperationJournalRecord, TurnRecord,
    };

    #[derive(Default)]
    struct TestStore {
        hydrated: std::sync::Mutex<crate::RuntimeHydratedState>,
        events: std::sync::Mutex<Vec<RuntimeEventRecord>>,
    }

    impl TestStore {
        fn upsert_with_key<T, F>(rows: &mut Vec<T>, value: T, key: F)
        where
            T: Clone,
            F: Fn(&T) -> String,
        {
            let value_key = key(&value);
            if let Some(existing) = rows.iter_mut().find(|row| key(row) == value_key) {
                *existing = value;
                return;
            }
            rows.push(value);
        }
    }

    #[async_trait]
    impl RuntimeStore for TestStore {
        async fn initialize(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn append_runtime_event(
            &self,
            event: &NewRuntimeEvent,
        ) -> Result<RuntimeEventRecord, RuntimeError> {
            let mut events = self.events.lock().expect("events lock");
            if let Some(existing) = events.iter().find(|row| row.event_id == event.event_id) {
                return Ok(existing.clone());
            }
            let row_id = i64::try_from(events.len()).unwrap_or(0) + 1;
            let seq = events
                .iter()
                .filter(|row| row.scope == event.scope && row.scope_id == event.scope_id)
                .map(|row| row.seq)
                .max()
                .unwrap_or(0)
                + 1;
            let record = RuntimeEventRecord {
                row_id,
                event_id: event.event_id.clone(),
                scope: event.scope,
                scope_id: event.scope_id.clone(),
                session_id: event.session_id.clone(),
                team_id: event.team_id.clone(),
                turn_id: event.turn_id.clone(),
                seq,
                kind: event.kind.clone(),
                criticality: event.criticality,
                payload: event.payload.clone(),
                provider: event.provider.clone(),
                provider_seq: event.provider_seq,
                created_at: event.created_at,
            };
            events.push(record.clone());
            Ok(record)
        }

        fn list_runtime_events(
            &self,
            scope: Option<(RuntimeEventScope, &str)>,
            after_seq: Option<i64>,
            limit: usize,
        ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
            let events = self.events.lock().expect("events lock");
            let mut rows = events.clone();
            if let Some((scope_value, scope_id)) = scope {
                rows.retain(|row| row.scope == scope_value && row.scope_id == scope_id);
                if let Some(after) = after_seq {
                    rows.retain(|row| row.seq > after);
                }
            } else if let Some(after) = after_seq {
                rows.retain(|row| row.row_id > after);
            }
            rows.sort_by_key(|row| row.row_id);
            rows.truncate(limit);
            Ok(rows)
        }

        fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.sessions, record.clone(), |row| row.id.clone());
            Ok(())
        }

        fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.turns, record.clone(), |row| row.id.clone());
            Ok(())
        }

        fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.approvals, record.clone(), |row| {
                row.id.clone()
            });
            Ok(())
        }

        fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.teams, record.clone(), |row| row.id.clone());
            Ok(())
        }

        fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.team_members, record.clone(), |row| {
                format!("{}|{}", row.team_id, row.agent_id)
            });
            Ok(())
        }

        fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            hydrated
                .team_members
                .retain(|row| !(row.team_id == team_id && row.agent_id == agent_id));
            Ok(())
        }

        fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.team_messages, record.clone(), |row| {
                row.id.clone()
            });
            Ok(())
        }

        fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.team_deliveries, record.clone(), |row| {
                row.id.clone()
            });
            Ok(())
        }

        fn upsert_managed_worktree(
            &self,
            record: &ManagedWorktreeRecord,
        ) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.managed_worktrees, record.clone(), |row| {
                row.id.clone()
            });
            Ok(())
        }

        fn upsert_managed_worktree_claim(
            &self,
            record: &ManagedWorktreeClaimRecord,
        ) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(
                &mut hydrated.managed_worktree_claims,
                record.clone(),
                |row| format!("{}|{}", row.worktree_id, row.session_id),
            );
            Ok(())
        }

        fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(&mut hydrated.processes, record.clone(), |row| {
                row.id.clone()
            });
            Ok(())
        }

        fn upsert_team_operation_journal(
            &self,
            record: &TeamOperationJournalRecord,
        ) -> Result<(), RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            Self::upsert_with_key(
                &mut hydrated.team_operation_journal,
                record.clone(),
                |row| row.operation_id.clone(),
            );
            Ok(())
        }

        fn append_team_operation_diagnostic(
            &self,
            operation_id: Option<&str>,
            team_id: Option<&str>,
            code: &str,
            message: &str,
            payload: &Value,
            created_at: i64,
        ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated lock");
            let id = i64::try_from(hydrated.team_operation_diagnostics.len()).unwrap_or(0) + 1;
            let record = TeamOperationDiagnosticRecord {
                id,
                operation_id: operation_id.map(str::to_string),
                team_id: team_id.map(str::to_string),
                code: code.to_string(),
                message: message.to_string(),
                payload: payload.clone(),
                created_at,
            };
            hydrated.team_operation_diagnostics.push(record.clone());
            Ok(record)
        }

        fn list_team_operation_journal(
            &self,
            team_id: Option<&str>,
        ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
            let hydrated = self.hydrated.lock().expect("hydrated lock");
            let mut rows = hydrated.team_operation_journal.clone();
            if let Some(team_id) = team_id {
                rows.retain(|row| row.team_id == team_id);
            }
            Ok(rows)
        }

        fn list_team_operation_diagnostics(
            &self,
            team_id: Option<&str>,
            operation_id: Option<&str>,
        ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
            let hydrated = self.hydrated.lock().expect("hydrated lock");
            let mut rows = hydrated.team_operation_diagnostics.clone();
            if let Some(team_id) = team_id {
                rows.retain(|row| row.team_id.as_deref() == Some(team_id));
            }
            if let Some(operation_id) = operation_id {
                rows.retain(|row| row.operation_id.as_deref() == Some(operation_id));
            }
            Ok(rows)
        }

        fn hydrate_runtime_state(&self) -> Result<crate::RuntimeHydratedState, RuntimeError> {
            Ok(self.hydrated.lock().expect("hydrated lock").clone())
        }
    }

    #[derive(Default)]
    struct TestProviderState {
        sessions: HashMap<String, String>,
        completed: HashMap<String, ProviderTurnResult>,
    }

    struct TestProvider {
        wait_ms: u64,
        state: Mutex<TestProviderState>,
    }

    impl TestProvider {
        fn new(wait_ms: u64) -> Self {
            Self {
                wait_ms,
                state: Mutex::new(TestProviderState::default()),
            }
        }
    }

    #[async_trait]
    impl RuntimeProvider for TestProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Codex
        }

        fn metadata(&self) -> ProviderMetadata {
            ProviderMetadata {
                kind: ProviderKind::Codex,
                display_name: "Test Codex".to_string(),
                enabled: true,
            }
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
            Ok(vec![ProviderModel {
                id: "test-model".to_string(),
                display_name: "Test Model".to_string(),
            }])
        }

        async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
            Ok(ProviderAuthStatus {
                authenticated: true,
                mode: Some("test".to_string()),
                detail: None,
            })
        }

        async fn create_session(
            &self,
            req: ProviderCreateSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            state.sessions.insert(
                req.runtime_session_id.clone(),
                format!("test-thread-{}", req.runtime_session_id),
            );
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id.clone(),
                provider_session_ref: format!("test-thread-{}", req.runtime_session_id),
                canonical_provider_session_ref: None,
            })
        }

        async fn resume_session(
            &self,
            req: ProviderResumeSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            state.sessions.insert(
                req.runtime_session_id.clone(),
                req.provider_session_ref.clone(),
            );
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id,
                provider_session_ref: req.provider_session_ref,
                canonical_provider_session_ref: req.canonical_provider_session_ref,
            })
        }

        async fn send_turn(
            &self,
            req: ProviderSendTurnRequest,
        ) -> Result<ProviderTurnAck, RuntimeError> {
            let mut state = self.state.lock().await;
            state.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: req.runtime_session_id.clone(),
                    turn_id: req.turn_id.clone(),
                    status: ProviderTurnStatus::Completed,
                    usage: Some(serde_json::json!({ "last_message": "ok" })),
                    error: None,
                },
            );
            Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
            })
        }

        async fn wait_for_turn(
            &self,
            req: ProviderWaitTurnRequest,
        ) -> Result<ProviderTurnResult, RuntimeError> {
            if self.wait_ms > 0 {
                sleep(Duration::from_millis(self.wait_ms)).await;
            }
            let state = self.state.lock().await;
            state
                .completed
                .get(req.turn_id.as_str())
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("test turn {}", req.turn_id)))
        }

        async fn interrupt_turn(
            &self,
            _req: ProviderInterruptTurnRequest,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    fn build_runtime_and_service(
        store: Arc<TestStore>,
        wait_ms: u64,
    ) -> (Arc<RuntimeSessionManager>, Arc<RuntimeTeamCommsService>) {
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(TestProvider::new(wait_ms)))
            .expect("register provider");
        let runtime = Arc::new(
            RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512)
                .expect("build runtime"),
        );
        let team_comms = RuntimeTeamCommsService::new(
            store,
            runtime.clone(),
            RuntimeTeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1_000,
            },
        )
        .expect("build team comms");
        (runtime, team_comms)
    }

    async fn create_test_session(runtime: &RuntimeSessionManager) -> String {
        runtime
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: Some("test-model".to_string()),
                cwd: None,
                permission_mode: None,
                metadata: Some(serde_json::json!({ "suite": "team_comms" })),
            })
            .await
            .expect("create session")
            .id
    }

    #[tokio::test]
    async fn direct_message_image_paths_are_injected_as_image_items() {
        let store = Arc::new(TestStore::default());
        let (runtime, service) = build_runtime_and_service(store, 0);
        let lead = create_test_session(&runtime).await;
        let member = create_test_session(&runtime).await;

        let team = service
            .create_team(TeamCreateRequest {
                name: "Image Team".to_string(),
                lead_agent_id: lead.clone(),
                member_agent_ids: vec![member.clone()],
                created_by: Some("test".to_string()),
            })
            .await
            .expect("create team");

        service
            .send_direct(TeamSendDirectRequest {
                team_id: team.team.id,
                sender_agent_id: lead,
                recipient_agent_id: member.clone(),
                input: serde_json::json!([{ "type": "text", "text": "please inspect" }]),
                image_paths: vec!["/tmp/reference.png".to_string()],
                priority: "normal".to_string(),
                policy: "non_interrupting".to_string(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: None,
            })
            .await
            .expect("send direct with image");

        let turns = runtime
            .list_session_turns(member.as_str())
            .await
            .expect("member turns");
        assert!(turns
            .iter()
            .flat_map(|turn| turn.input.as_array().into_iter().flatten())
            .any(|item| {
                item.get("type").and_then(Value::as_str) == Some("image")
                    && item.get("path").and_then(Value::as_str) == Some("/tmp/reference.png")
            }));
    }

    #[tokio::test]
    async fn restart_appends_new_team_event_rows_without_event_id_collision() {
        let store = Arc::new(TestStore::default());
        let (runtime, service) = build_runtime_and_service(store.clone(), 1);
        let lead = create_test_session(&runtime).await;
        let member = create_test_session(&runtime).await;

        let created = service
            .create_team(TeamCreateRequest {
                name: "Restart Team".to_string(),
                lead_agent_id: lead.clone(),
                member_agent_ids: vec![member.clone()],
                created_by: Some("test".to_string()),
            })
            .await
            .expect("create team");
        let team_id = created.team.id.clone();

        let before = service
            .replay_team_events(team_id.as_str(), None, 128)
            .expect("replay before");
        assert!(
            before.iter().any(|event| event.kind == "team.created"),
            "expected team.created before restart"
        );

        drop(service);
        drop(runtime);

        let (_runtime_after_restart, service_after_restart) =
            build_runtime_and_service(store.clone(), 1);
        service_after_restart
            .set_team_lead(TeamSetLeadRequest {
                team_id: team_id.clone(),
                lead_agent_id: member.clone(),
            })
            .await
            .expect("set team lead after restart");

        let after = service_after_restart
            .replay_team_events(team_id.as_str(), None, 256)
            .expect("replay after");
        assert!(
            after.len() > before.len(),
            "expected event stream to append after restart mutation"
        );
        assert!(
            after.iter().any(|event| event.kind == "team.lead_changed"),
            "expected team.lead_changed event to append after restart"
        );
    }

    #[tokio::test]
    async fn startup_recovery_retries_deferred_delivery_for_ready_recipient() {
        let store = Arc::new(TestStore::default());
        let now = now_ms();

        store
            .upsert_session(&SessionRecord {
                id: "sess_lead_seed".to_string(),
                provider: "codex".to_string(),
                status: "ready".to_string(),
                cwd: None,
                model: Some("test-model".to_string()),
                permission_mode: None,
                system_prompt: None,
                metadata: serde_json::json!({}),
                provider_session_ref: Some("provider-lead-seed".to_string()),
                canonical_provider_session_ref: None,
                active_turn_id: None,
                worktree_id: None,
                created_at: now,
                updated_at: now,
                closed_at: None,
                failure_code: None,
                failure_message: None,
            })
            .expect("seed lead session");
        store
            .upsert_session(&SessionRecord {
                id: "sess_ready_seed".to_string(),
                provider: "codex".to_string(),
                status: "ready".to_string(),
                cwd: None,
                model: Some("test-model".to_string()),
                permission_mode: None,
                system_prompt: None,
                metadata: serde_json::json!({}),
                provider_session_ref: Some("provider-ready-seed".to_string()),
                canonical_provider_session_ref: None,
                active_turn_id: None,
                worktree_id: None,
                created_at: now,
                updated_at: now,
                closed_at: None,
                failure_code: None,
                failure_message: None,
            })
            .expect("seed recipient session");
        store
            .upsert_team(&TeamRecord {
                id: "team_seed".to_string(),
                name: "Seed Team".to_string(),
                lead_agent_id: "sess_lead_seed".to_string(),
                created_by: "test".to_string(),
                created_at: now,
                updated_at: now,
                deleted_at: None,
            })
            .expect("seed team");
        store
            .upsert_team_member(&TeamMemberRecord {
                team_id: "team_seed".to_string(),
                agent_id: "sess_ready_seed".to_string(),
                title: None,
                joined_at: now,
                added_by: "test".to_string(),
                creator_agent_id: None,
                creator_compaction_subscription: "auto".to_string(),
                worktree_id: None,
            })
            .expect("seed member");
        store
            .upsert_team_message(&TeamMessageRecord {
                id: "msg_seed".to_string(),
                team_id: "team_seed".to_string(),
                scope: "direct".to_string(),
                sender_agent_id: "sess_lead_seed".to_string(),
                recipient_agent_ids: serde_json::json!(["sess_ready_seed"]),
                input: serde_json::json!([{ "type": "text", "text": "seed deferred" }]),
                image_paths: serde_json::json!([]),
                priority: "normal".to_string(),
                policy: "non_interrupting".to_string(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("seed-idempotency".to_string()),
                created_at: now,
            })
            .expect("seed message");
        store
            .upsert_team_delivery(&TeamDeliveryRecord {
                id: "dlv_seed".to_string(),
                message_id: "msg_seed".to_string(),
                team_id: "team_seed".to_string(),
                recipient_agent_id: "sess_ready_seed".to_string(),
                provider: "codex".to_string(),
                status: DELIVERY_STATUS_DEFERRED.to_string(),
                effective_policy: Some("non_interrupting".to_string()),
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: Some("seed_restart_gap".to_string()),
                last_error_message: Some("seed deferred before restart".to_string()),
                created_at: now,
                updated_at: now,
            })
            .expect("seed delivery");

        let (_runtime, service) = build_runtime_and_service(store.clone(), 0);
        let before = service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: "team_seed".to_string(),
                message_id: Some("msg_seed".to_string()),
                recipient_agent_id: Some("sess_ready_seed".to_string()),
            })
            .await
            .expect("delivery before startup replay");
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].status, DELIVERY_STATUS_DEFERRED);

        let retried = service
            .recover_startup_deferred_deliveries()
            .await
            .expect("startup deferred recovery");
        assert!(
            retried >= 1,
            "expected startup replay to retry at least one deferred delivery"
        );

        let mut recovered_status = None;
        for _ in 0..30 {
            let rows = service
                .get_deliveries(TeamGetDeliveriesRequest {
                    team_id: "team_seed".to_string(),
                    message_id: Some("msg_seed".to_string()),
                    recipient_agent_id: Some("sess_ready_seed".to_string()),
                })
                .await
                .expect("delivery rows");
            if let Some(row) = rows.first() {
                recovered_status = Some(row.status.clone());
                if row.status != DELIVERY_STATUS_DEFERRED {
                    break;
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert_ne!(
            recovered_status.as_deref(),
            Some(DELIVERY_STATUS_DEFERRED),
            "deferred delivery should not remain permanently deferred after startup recovery"
        );
    }

    #[tokio::test]
    async fn delete_team_cancels_outstanding_delivery_and_clears_recipient_queue_blockers() {
        let store = Arc::new(TestStore::default());
        let (runtime, service) = build_runtime_and_service(store.clone(), 300);
        let lead = create_test_session(&runtime).await;
        let recipient = create_test_session(&runtime).await;

        let created = service
            .create_team(TeamCreateRequest {
                name: "Delete Queue Team".to_string(),
                lead_agent_id: lead.clone(),
                member_agent_ids: vec![recipient.clone()],
                created_by: Some("test".to_string()),
            })
            .await
            .expect("create team");
        let deleted_team_id = created.team.id.clone();

        let first_ack = service
            .send_direct(TeamSendDirectRequest {
                team_id: deleted_team_id.clone(),
                sender_agent_id: lead.clone(),
                recipient_agent_id: recipient.clone(),
                input: serde_json::json!([{ "type": "text", "text": "first" }]),
                image_paths: Vec::new(),
                priority: "normal".to_string(),
                policy: "non_interrupting".to_string(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("delete-q-1".to_string()),
            })
            .await
            .expect("first direct");
        assert_eq!(first_ack.deliveries.len(), 1);

        let second_ack = service
            .send_direct(TeamSendDirectRequest {
                team_id: deleted_team_id.clone(),
                sender_agent_id: lead.clone(),
                recipient_agent_id: recipient.clone(),
                input: serde_json::json!([{ "type": "text", "text": "second" }]),
                image_paths: Vec::new(),
                priority: "normal".to_string(),
                policy: "non_interrupting".to_string(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("delete-q-2".to_string()),
            })
            .await
            .expect("second direct");
        assert_eq!(second_ack.deliveries.len(), 1);
        let second_delivery_id = second_ack.deliveries[0].id.clone();

        let mut second_is_outstanding = false;
        for _ in 0..40 {
            let rows = service
                .get_deliveries(TeamGetDeliveriesRequest {
                    team_id: deleted_team_id.clone(),
                    message_id: Some(second_ack.message.id.clone()),
                    recipient_agent_id: Some(recipient.clone()),
                })
                .await
                .expect("list deliveries");
            if let Some(delivery) = rows.first() {
                if matches!(
                    delivery.status.as_str(),
                    DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
                ) {
                    second_is_outstanding = true;
                    break;
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            second_is_outstanding,
            "expected second delivery to be pending/deferred before team deletion"
        );

        service
            .delete_team(deleted_team_id.as_str())
            .await
            .expect("delete team");
        sleep(Duration::from_millis(450)).await;

        let hydrated_after_delete = store.hydrate_runtime_state().expect("hydrate");
        let deleted_delivery = hydrated_after_delete
            .team_deliveries
            .iter()
            .find(|delivery| delivery.id == second_delivery_id)
            .cloned()
            .expect("deleted team delivery row");
        assert_eq!(
            deleted_delivery.status, DELIVERY_STATUS_CANCELLED,
            "deleted team's outstanding delivery must be cancelled and must not resume/inject"
        );

        let created_second_team = service
            .create_team(TeamCreateRequest {
                name: "Live Team".to_string(),
                lead_agent_id: lead.clone(),
                member_agent_ids: vec![recipient.clone()],
                created_by: Some("test".to_string()),
            })
            .await
            .expect("create second team");
        let live_team_id = created_second_team.team.id;

        let third_ack = service
            .send_direct(TeamSendDirectRequest {
                team_id: live_team_id.clone(),
                sender_agent_id: lead.clone(),
                recipient_agent_id: recipient.clone(),
                input: serde_json::json!([{ "type": "text", "text": "third" }]),
                image_paths: Vec::new(),
                priority: "normal".to_string(),
                policy: "non_interrupting".to_string(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("delete-q-3".to_string()),
            })
            .await
            .expect("third direct");
        let third_delivery_id = third_ack.deliveries[0].id.clone();

        let mut third_terminal_status = None;
        for _ in 0..80 {
            let rows = service
                .get_deliveries(TeamGetDeliveriesRequest {
                    team_id: live_team_id.clone(),
                    message_id: Some(third_ack.message.id.clone()),
                    recipient_agent_id: Some(recipient.clone()),
                })
                .await
                .expect("list third delivery");
            if let Some(delivery) = rows
                .iter()
                .find(|delivery| delivery.id == third_delivery_id)
            {
                if is_terminal_status(delivery.status.as_str()) {
                    third_terminal_status = Some(delivery.status.clone());
                    break;
                }
            }
            sleep(Duration::from_millis(15)).await;
        }

        assert_eq!(
            third_terminal_status.as_deref(),
            Some(DELIVERY_STATUS_INJECTED),
            "later delivery must not be blocked/deferred by stale deleted-team queue state"
        );
    }
}
