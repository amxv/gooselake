use std::sync::Arc;

use serde_json::Value;
use tokio::sync::broadcast;

use crate::{
    ApprovalRecord, PersistedUserInputSnapshot, ProviderDispatchOutcome,
    ProviderInterruptTurnRequest, ProviderKind, ProviderRuntimeEvent, ProviderSendTurnRequest,
    ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError,
    RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, TurnAdmissionRecord,
    TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
    TurnRecord,
};

use super::helpers::{
    append_session_transcript, extract_assistant_text_from_usage, extract_turn_user_text,
    is_terminal_turn_status, now_ms,
};
use super::{RuntimeSessionManager, SendTurnAccepted, SendTurnInput};

fn is_valid_dispatch_transition(from: TurnDispatchState, to: TurnDispatchState) -> bool {
    use TurnDispatchState::{Dispatched, Dispatching, NotDispatched, Pending, Unknown};

    matches!(
        (from, to),
        (Pending, Pending | Dispatching | NotDispatched)
            | (
                Dispatching,
                Dispatching | Dispatched | NotDispatched | Unknown
            )
            | (Dispatched, Dispatched)
            | (NotDispatched, NotDispatched)
            | (Unknown, Unknown | Dispatched | NotDispatched)
    )
}

impl RuntimeSessionManager {
    pub async fn admitted_turn_for_correlation(
        &self,
        session_id: &str,
        correlation_id: &str,
    ) -> Option<String> {
        self.turn_admissions
            .read()
            .await
            .values()
            .find(|admission| {
                admission.session_id == session_id
                    && admission.correlation.correlation_id.as_deref() == Some(correlation_id)
            })
            .map(|admission| admission.turn_id.clone())
    }

    pub async fn latest_turn_admission_for_correlation(
        &self,
        session_id: &str,
        correlation_id: &str,
    ) -> Option<TurnAdmissionRecord> {
        self.turn_admissions
            .read()
            .await
            .values()
            .filter(|admission| {
                admission.session_id == session_id
                    && admission.correlation.correlation_id.as_deref() == Some(correlation_id)
            })
            .max_by(|left, right| {
                left.admitted_at
                    .cmp(&right.admitted_at)
                    .then(left.updated_at.cmp(&right.updated_at))
                    .then(left.turn_id.cmp(&right.turn_id))
            })
            .cloned()
    }

    pub async fn send_turn(
        self: &Arc<Self>,
        session_id: &str,
        input: SendTurnInput,
    ) -> Result<SendTurnAccepted, RuntimeError> {
        let session = self.get_session(session_id).await?;
        if session.status == "closed" || session.status == "failed" {
            return Err(RuntimeError::InvalidState(format!(
                "session {session_id} is not writable in status {}",
                session.status
            )));
        }
        if session.active_turn_id.is_some() {
            return Err(RuntimeError::InvalidState(format!(
                "session {session_id} already has an active turn"
            )));
        }
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let turn_id = self.allocate_id("turn", provider_kind.as_str());
        let now = now_ms();
        let effective_permission_mode = input
            .permission_mode
            .clone()
            .or_else(|| session.permission_mode.clone());
        let requires_approval = effective_permission_mode.as_deref() == Some("require_approval");
        let approval_id = if requires_approval {
            Some(self.allocate_id("apr", provider_kind.as_str()))
        } else {
            None
        };
        let projection_source = input.projection_source.unwrap_or_default();
        let user_input_snapshot = input
            .user_input_snapshot
            .clone()
            .unwrap_or_else(|| PersistedUserInputSnapshot::from_input(&input.input));
        let source = match projection_source {
            TurnInputProjectionSource::UserVisible => "user",
            TurnInputProjectionSource::AgentMessageDeliveryTransport => {
                "agent_message_delivery_transport"
            }
            TurnInputProjectionSource::AutomationContext => "automation_context",
        };
        let turn = TurnRecord {
            id: turn_id.clone(),
            session_id: session_id.to_string(),
            provider_turn_ref: None,
            status: "admitted".to_string(),
            input: Value::Array(input.input.clone()),
            source: Some(source.to_string()),
            started_at: Some(now),
            completed_at: None,
            usage: None,
            error: None,
        };
        let mut updated_session = session.clone();
        updated_session.status = "turn_admitted".to_string();
        updated_session.active_turn_id = Some(turn_id.clone());
        updated_session.updated_at = now;
        let approval = approval_id.as_ref().map(|approval_id| ApprovalRecord {
            id: approval_id.clone(),
            session_id: session_id.to_string(),
            turn_id: turn_id.clone(),
            origin: "runtime_pre_dispatch_policy".to_string(),
            tool_call_id: None,
            provider_approval_ref: None,
            status: "pending".to_string(),
            request: serde_json::json!({
                "reason": "manual approval required before provider execution",
            }),
            response: None,
            created_at: now,
            resolved_at: None,
        });
        let admission = TurnAdmissionRecord {
            turn_id: turn_id.clone(),
            session_id: session_id.to_string(),
            provider: provider_kind.as_str().to_string(),
            projection_source,
            user_input_snapshot,
            dispatch_policy: TurnDispatchPolicySnapshot {
                permission_mode: effective_permission_mode.clone(),
                pre_dispatch_approval_required: requires_approval,
            },
            correlation: TurnCorrelationState {
                expected_turn_id: input.expected_turn_id.clone(),
                correlation_id: input.correlation_id.clone(),
            },
            dispatch_state: TurnDispatchState::Pending,
            provider_native_turn_id: None,
            dispatch_error: None,
            admitted_at: now,
            updated_at: now,
        };

        self.store
            .admit_turn(&admission, &turn, &updated_session, approval.as_ref())?;
        {
            let mut turns = self.turns.write().await;
            turns.insert(turn_id.clone(), turn.clone());
        }
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated_session.clone());
        }
        if let Some(approval) = approval.clone() {
            {
                let mut approvals = self.approvals.write().await;
                approvals.insert(approval.id.clone(), approval);
            }
        }
        self.turn_admissions
            .write()
            .await
            .insert(turn_id.clone(), admission);
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id.as_str()),
            "turn.admitted",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "projection_source": projection_source.as_str(),
                "requires_approval": requires_approval,
            }),
        )
        .await?;

        if requires_approval {
            let approval_id =
                approval_id.expect("approval id must exist when approval is required");
            {
                let mut turns = self.turns.write().await;
                let turn = turns
                    .get_mut(&turn_id)
                    .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
                turn.status = "waiting_for_approval".to_string();
                self.store.upsert_turn(turn)?;
            }
            {
                let mut sessions = self.sessions.write().await;
                let session = sessions
                    .get_mut(session_id)
                    .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
                session.status = "waiting_for_approval".to_string();
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            self.append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id.as_str()),
                "approval.requested",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "approval_id": approval_id,
                    "origin": "runtime_pre_dispatch_policy",
                }),
            )
            .await?;
            return Ok(SendTurnAccepted {
                session_id: session_id.to_string(),
                turn_id,
                status: "waiting_for_approval".to_string(),
            });
        }

        self.update_turn_dispatch_authority(
            turn_id.as_str(),
            TurnDispatchState::Dispatching,
            None,
            None,
        )
        .await?;

        let provider_send_input = ProviderSendTurnRequest {
            runtime_session_id: session_id.to_string(),
            turn_id: turn_id.clone(),
            input: input.input,
            expected_turn_id: input.expected_turn_id,
            permission_mode: effective_permission_mode,
            approval_id: approval_id.clone(),
        };
        let (ack, provider_events) = match self
            .dispatch_send_turn_with_resume_fallback(provider_kind, provider_send_input, &session)
            .await
        {
            Ok((ack, provider_events))
                if ack.runtime_session_id == session_id && ack.turn_id == turn_id =>
            {
                (ack, provider_events)
            }
            Ok((ack, _)) => {
                let error = RuntimeError::provider_dispatch_unknown(
                    "ack_identity_mismatch",
                    format!(
                        "provider send_turn acknowledgement mismatch (expected_session={session_id}, expected_turn={turn_id}, actual_session={}, actual_turn={})",
                        ack.runtime_session_id, ack.turn_id
                    ),
                );
                self.mark_dispatch_unknown(session_id, turn_id.as_str(), &error)
                    .await?;
                return Err(error);
            }
            Err(error)
                if error.provider_dispatch_outcome() == ProviderDispatchOutcome::NotDispatched =>
            {
                self.mark_not_dispatched(session_id, turn_id.as_str(), &error)
                    .await?;
                return Err(error);
            }
            Err(error) => {
                let unknown = RuntimeError::provider_dispatch_unknown(
                    error.provider_dispatch_code().unwrap_or("provider_error"),
                    error.to_string(),
                );
                self.mark_dispatch_unknown(session_id, turn_id.as_str(), &unknown)
                    .await?;
                return Err(unknown);
            }
        };

        self.update_turn_dispatch_authority(
            turn_id.as_str(),
            TurnDispatchState::Dispatched,
            ack.provider_native_turn_id.clone(),
            None,
        )
        .await?;

        {
            let mut turns = self.turns.write().await;
            let turn = turns
                .get_mut(&turn_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
            turn.status = "in_progress".to_string();
            self.store.upsert_turn(turn)?;
        }
        {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
            session.status = "turn_running".to_string();
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }

        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id.as_str()),
            "turn.started",
            RuntimeEventCriticality::Critical,
            serde_json::json!({}),
        )
        .await?;
        self.spawn_wait_for_turn(
            provider_kind,
            session_id.to_string(),
            turn_id.clone(),
            provider_events,
        );

        Ok(SendTurnAccepted {
            session_id: session_id.to_string(),
            turn_id,
            status: "in_progress".to_string(),
        })
    }

    pub async fn interrupt_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let session = self.get_session(session_id).await?;
        if session.active_turn_id.as_deref() != Some(turn_id) {
            return Err(RuntimeError::InvalidState(format!(
                "turn {turn_id} is not active for session {session_id}"
            )));
        }
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;
        provider
            .interrupt_turn(ProviderInterruptTurnRequest {
                runtime_session_id: session_id.to_string(),
                turn_id: turn_id.to_string(),
            })
            .await?;
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id),
                "turn.interrupt_requested",
                RuntimeEventCriticality::Critical,
                serde_json::json!({}),
            )
            .await?;
        Ok(())
    }

    pub fn replay_session_events(
        &self,
        session_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.store.list_runtime_events(
            Some((RuntimeEventScope::Session, session_id)),
            after_seq,
            limit.max(1),
        )
    }

    pub(super) async fn update_turn_dispatch_authority(
        &self,
        turn_id: &str,
        state: TurnDispatchState,
        provider_native_turn_id: Option<String>,
        dispatch_error: Option<Value>,
    ) -> Result<TurnAdmissionRecord, RuntimeError> {
        let current = self
            .turn_admissions
            .read()
            .await
            .get(turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn admission {turn_id}")))?;
        if !is_valid_dispatch_transition(current.dispatch_state, state) {
            return Err(RuntimeError::ProtocolViolation(format!(
                "invalid provider dispatch transition for turn {turn_id}: {} -> {}",
                current.dispatch_state.as_str(),
                state.as_str()
            )));
        }
        if let Some(provider_native_turn_id) = provider_native_turn_id.as_deref() {
            if provider_native_turn_id.trim().is_empty()
                || provider_native_turn_id.trim() != provider_native_turn_id
            {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "provider-native turn id for {turn_id} must be non-empty and trimmed"
                )));
            }
            if !matches!(
                state,
                TurnDispatchState::Dispatched | TurnDispatchState::Unknown
            ) {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "provider-native turn id for {turn_id} cannot be attached in dispatch state {}",
                    state.as_str()
                )));
            }
            if let Some(existing) = current.provider_native_turn_id.as_deref() {
                if existing != provider_native_turn_id {
                    return Err(RuntimeError::ProtocolViolation(format!(
                        "logical turn {turn_id} cannot be remapped from provider-native turn {existing} to {provider_native_turn_id}"
                    )));
                }
            }
        }
        let mut updated = current;
        updated.dispatch_state = state;
        if provider_native_turn_id.is_some() {
            updated.provider_native_turn_id = provider_native_turn_id;
        }
        updated.dispatch_error = dispatch_error;
        updated.updated_at = now_ms();
        self.store.upsert_turn_admission(&updated)?;
        self.turn_admissions
            .write()
            .await
            .insert(turn_id.to_string(), updated.clone());
        Ok(updated)
    }

    pub(super) async fn mark_not_dispatched(
        &self,
        session_id: &str,
        turn_id: &str,
        error: &RuntimeError,
    ) -> Result<(), RuntimeError> {
        self.update_turn_dispatch_authority(
            turn_id,
            TurnDispatchState::NotDispatched,
            None,
            Some(serde_json::json!({
                "code": error.provider_dispatch_code(),
                "message": error.to_string(),
            })),
        )
        .await?;
        {
            let mut turns = self.turns.write().await;
            let turn = turns
                .get_mut(turn_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
            turn.status = "failed".to_string();
            turn.completed_at = Some(now_ms());
            turn.error = Some(serde_json::json!({
                "message": error.to_string(),
                "provider_dispatch_outcome": "not_dispatched",
            }));
            self.store.upsert_turn(turn)?;
        }
        {
            let mut approvals = self.approvals.write().await;
            for approval in approvals.values_mut().filter(|approval| {
                approval.session_id == session_id
                    && approval.turn_id == turn_id
                    && approval.status == "pending"
            }) {
                approval.status = "cancelled".to_string();
                approval.response = Some(serde_json::json!({
                    "reason": "provider dispatch was proven not to have occurred",
                }));
                approval.resolved_at = Some(now_ms());
                self.store.upsert_approval(approval)?;
            }
        }
        {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
            if session.active_turn_id.as_deref() == Some(turn_id) {
                session.active_turn_id = None;
            }
            if !matches!(session.status.as_str(), "closed" | "failed") {
                session.status = "ready".to_string();
            }
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "turn.failed",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "error": error.to_string(),
                "provider_dispatch_outcome": "not_dispatched",
            }),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn mark_dispatch_unknown(
        &self,
        session_id: &str,
        turn_id: &str,
        error: &RuntimeError,
    ) -> Result<(), RuntimeError> {
        self.update_turn_dispatch_authority(
            turn_id,
            TurnDispatchState::Unknown,
            None,
            Some(serde_json::json!({
                "code": error.provider_dispatch_code(),
                "message": error.to_string(),
            })),
        )
        .await?;
        {
            let mut turns = self.turns.write().await;
            let turn = turns
                .get_mut(turn_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
            turn.status = "dispatch_unknown".to_string();
            turn.error = Some(serde_json::json!({
                "message": error.to_string(),
                "provider_dispatch_outcome": "unknown",
            }));
            self.store.upsert_turn(turn)?;
        }
        {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
            session.status = "turn_recovery_required".to_string();
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "turn.dispatch_unknown",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "error": error.to_string(),
                "provider_dispatch_outcome": "unknown",
            }),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn dispatch_send_turn_with_resume_fallback(
        &self,
        provider_kind: ProviderKind,
        request: ProviderSendTurnRequest,
        session: &crate::SessionRecord,
    ) -> Result<
        (
            crate::ProviderTurnAck,
            Option<broadcast::Receiver<ProviderRuntimeEvent>>,
        ),
        RuntimeError,
    > {
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::provider_not_dispatched(
                "provider_not_registered",
                format!("provider '{}' is not registered", provider_kind.as_str()),
            )
        })?;
        let provider_events = provider.subscribe_events();
        match provider.send_turn(request.clone()).await {
            Ok(ack) => Ok((ack, provider_events)),
            Err(error)
                if error.provider_dispatch_outcome() == ProviderDispatchOutcome::NotDispatched
                    && error.provider_dispatch_code() == Some("session_not_found") =>
            {
                let provider_session_ref =
                    session.provider_session_ref.clone().ok_or_else(|| {
                        RuntimeError::provider_not_dispatched(
                            "provider_session_unavailable",
                            format!(
                                "provider session {} was not found and cannot be resumed",
                                request.runtime_session_id
                            ),
                        )
                    })?;
                let resume_request = self
                    .provider_resume_request_for_session(
                        session,
                        provider_session_ref,
                        session.canonical_provider_session_ref.clone(),
                    )
                    .map_err(|resume_error| {
                        RuntimeError::provider_not_dispatched(
                            "provider_resume_failed",
                            resume_error.to_string(),
                        )
                    })?;
                provider
                    .resume_session_with_policy(resume_request)
                    .await
                    .map_err(|resume_error| {
                        RuntimeError::provider_not_dispatched(
                            "provider_resume_failed",
                            resume_error.to_string(),
                        )
                    })?;
                provider
                    .send_turn(request)
                    .await
                    .map(|ack| (ack, provider_events))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn spawn_wait_for_turn(
        self: &Arc<Self>,
        provider: ProviderKind,
        session_id: String,
        turn_id: String,
        provider_events: Option<broadcast::Receiver<ProviderRuntimeEvent>>,
    ) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let provider_adapter = match manager.providers.get(provider) {
                Some(provider_adapter) => provider_adapter,
                None => return,
            };
            let wait = provider_adapter.wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                timeout_ms: None,
            });
            tokio::pin!(wait);
            let mut provider_events =
                provider_events.or_else(|| provider_adapter.subscribe_events());
            let result = loop {
                let Some(events) = provider_events.as_mut() else {
                    break wait.await;
                };
                tokio::select! {
                    result = &mut wait => break result,
                    event = events.recv() => {
                        match event {
                            Ok(ProviderRuntimeEvent::ApprovalRequested {
                                runtime_session_id,
                                turn_id: event_turn_id,
                                provider_approval_ref,
                                tool_call_id,
                                request,
                            }) if runtime_session_id == session_id && event_turn_id == turn_id => {
                                if let Err(error) = manager
                                    .record_provider_approval(
                                        session_id.as_str(),
                                        turn_id.as_str(),
                                        provider_approval_ref.as_str(),
                                        tool_call_id,
                                        request,
                                    )
                                    .await
                                {
                                    let _ = manager
                                        .mark_provider_event_stream_unknown(
                                            session_id.as_str(),
                                            turn_id.as_str(),
                                            "provider_approval_persistence_failed",
                                            error.to_string(),
                                        )
                                        .await;
                                    return;
                                }
                            }
                            Ok(_) => {}
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                let _ = manager
                                    .mark_provider_event_stream_unknown(
                                        session_id.as_str(),
                                        turn_id.as_str(),
                                        "provider_event_stream_lagged",
                                        format!("provider event stream skipped {skipped} events"),
                                    )
                                    .await;
                                return;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                let _ = manager
                                    .mark_provider_event_stream_unknown(
                                        session_id.as_str(),
                                        turn_id.as_str(),
                                        "provider_event_stream_closed",
                                        "provider event stream closed before turn completion".to_string(),
                                    )
                                    .await;
                                return;
                            }
                        }
                    }
                }
            };
            match result {
                Ok(turn_result) => {
                    if let Err(error) = manager.apply_terminal_result(turn_result).await {
                        if std::env::var("GG_CLAUDE_SMOKE_DEBUG")
                            .ok()
                            .map(|value| value.trim() == "1")
                            .unwrap_or(false)
                        {
                            eprintln!(
                                "[runtime-core] failed to apply terminal turn result for session_id={} turn_id={}: {}",
                                session_id, turn_id, error
                            );
                        }
                    }
                }
                Err(error) => {
                    let _ = manager
                        .apply_terminal_failure(session_id.as_str(), turn_id.as_str(), error)
                        .await;
                }
            }
        });
    }

    async fn mark_provider_event_stream_unknown(
        &self,
        session_id: &str,
        turn_id: &str,
        code: &str,
        message: String,
    ) -> Result<(), RuntimeError> {
        {
            let mut turns = self.turns.write().await;
            if let Some(turn) = turns.get_mut(turn_id) {
                turn.status = "provider_event_recovery_required".to_string();
                turn.error = Some(serde_json::json!({
                    "code": code,
                    "message": message,
                }));
                self.store.upsert_turn(turn)?;
            }
        }
        {
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(session_id) {
                session.status = "turn_recovery_required".to_string();
                session.failure_code = Some(code.to_string());
                session.failure_message = Some(message.clone());
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "provider.event_stream_unknown",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "code": code,
                "message": message,
            }),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn apply_terminal_result(
        &self,
        result: ProviderTurnResult,
    ) -> Result<(), RuntimeError> {
        let mut turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        let Some(turn) = turns.get_mut(&result.turn_id) else {
            return Err(RuntimeError::NotFound(format!("turn {}", result.turn_id)));
        };
        if turn.session_id != result.runtime_session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "provider turn ownership mismatch for turn {}",
                result.turn_id
            )));
        }

        if is_terminal_turn_status(turn.status.as_str()) {
            let incoming_status = result.status.as_str();
            if turn.status == incoming_status {
                return Ok(());
            }
            let session_id = turn.session_id.clone();
            let conflict = format!(
                "conflicting terminal state for turn {} (stored={}, incoming={})",
                result.turn_id, turn.status, incoming_status
            );
            if let Some(session) = sessions.get_mut(&session_id) {
                session.status = "failed".to_string();
                session.failure_code = Some("terminal_conflict".to_string());
                session.failure_message = Some(conflict.clone());
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            return Err(RuntimeError::ProtocolViolation(conflict));
        }

        turn.status = result.status.as_str().to_string();
        turn.completed_at = Some(now_ms());
        turn.usage = result.usage.clone();
        turn.error = result.error.clone();
        self.store.upsert_turn(turn)?;

        let Some(session) = sessions.get_mut(&result.runtime_session_id) else {
            return Err(RuntimeError::NotFound(format!(
                "session {}",
                result.runtime_session_id
            )));
        };
        if session.active_turn_id.as_deref() == Some(result.turn_id.as_str()) {
            session.active_turn_id = None;
        }
        if session.status != "closed" && session.status != "failed" {
            session.status = "ready".to_string();
        }
        if result.status == ProviderTurnStatus::Completed
            || result.status == ProviderTurnStatus::Interrupted
        {
            let user_text = extract_turn_user_text(turn.input.as_array());
            let assistant_text = result
                .usage
                .as_ref()
                .and_then(extract_assistant_text_from_usage);
            if let Some(user_text) = user_text {
                append_session_transcript(&mut session.metadata, "user", user_text.as_str());
            }
            if let Some(assistant_text) = assistant_text {
                append_session_transcript(
                    &mut session.metadata,
                    "assistant",
                    assistant_text.as_str(),
                );
            }
        }
        session.updated_at = now_ms();
        self.store.upsert_session(session)?;

        let event_kind = match result.status {
            ProviderTurnStatus::Completed => "turn.completed",
            ProviderTurnStatus::Interrupted => "turn.interrupted",
            ProviderTurnStatus::Failed => "turn.failed",
            ProviderTurnStatus::InProgress => "turn.in_progress",
        };
        let assistant_text = result
            .usage
            .as_ref()
            .and_then(extract_assistant_text_from_usage);
        drop(sessions);
        drop(turns);

        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                result.runtime_session_id.as_str(),
                Some(result.runtime_session_id.as_str()),
                Some(result.turn_id.as_str()),
                event_kind,
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "status": result.status.as_str(),
                    "usage": result.usage,
                    "error": result.error,
                    "assistant_text": assistant_text,
                }),
            )
            .await?;
        Ok(())
    }

    async fn apply_terminal_failure(
        &self,
        session_id: &str,
        turn_id: &str,
        error: RuntimeError,
    ) -> Result<(), RuntimeError> {
        let mut turns = self.turns.write().await;
        if let Some(turn) = turns.get_mut(turn_id) {
            if !is_terminal_turn_status(turn.status.as_str()) {
                turn.status = "failed".to_string();
                turn.completed_at = Some(now_ms());
                turn.error = Some(serde_json::json!({ "message": error.to_string() }));
                self.store.upsert_turn(turn)?;
            }
        }
        drop(turns);

        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            if session.active_turn_id.as_deref() == Some(turn_id) {
                session.active_turn_id = None;
            }
            session.status = "failed".to_string();
            session.failure_code = Some("provider_wait_failure".to_string());
            session.failure_message = Some(error.to_string());
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }
        drop(sessions);

        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id),
                "provider.error",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "error": error.to_string() }),
            )
            .await?;
        Ok(())
    }
}
