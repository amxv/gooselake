use std::sync::Arc;

use serde_json::Value;

use crate::{
    ApprovalDecision, ApprovalRecord, ProviderApprovalResponseRequest,
    ProviderInterruptTurnRequest, ProviderKind, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest,
    RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, TurnRecord,
};

use super::helpers::{
    append_session_transcript, extract_assistant_text_from_usage, extract_turn_user_text,
    is_terminal_turn_status, now_ms,
};
use super::{ApprovalResponseInput, RuntimeSessionManager, SendTurnAccepted, SendTurnInput};

impl RuntimeSessionManager {
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

        let session_metadata_for_provider = session.metadata.clone();
        let provider_send_input = ProviderSendTurnRequest {
            runtime_session_id: session_id.to_string(),
            turn_id: turn_id.clone(),
            input: input.input.clone(),
            expected_turn_id: input.expected_turn_id.clone(),
            permission_mode: effective_permission_mode.clone(),
            approval_id: approval_id.clone(),
        };
        let ack_result = self
            .dispatch_send_turn_with_resume_fallback(
                provider_kind,
                provider_send_input,
                session.cwd.clone(),
                session.provider_session_ref.clone(),
                session.canonical_provider_session_ref.clone(),
                session_metadata_for_provider,
            )
            .await;

        let ack = match ack_result {
            Ok(ack) => {
                if ack.runtime_session_id != session_id || ack.turn_id != turn_id {
                    return Err(RuntimeError::ProtocolViolation(format!(
                        "provider send_turn acknowledgement mismatch (expected_session={}, expected_turn={}, actual_session={}, actual_turn={})",
                        session_id, turn_id, ack.runtime_session_id, ack.turn_id
                    )));
                }
                ack
            }
            Err(error) => {
                // Fail closed: persist a terminal failed turn and keep session writable.
                let failed_turn = TurnRecord {
                    id: turn_id.clone(),
                    session_id: session_id.to_string(),
                    provider_turn_ref: None,
                    status: "failed".to_string(),
                    input: Value::Array(input.input),
                    source: Some("user".to_string()),
                    started_at: Some(now),
                    completed_at: Some(now_ms()),
                    usage: None,
                    error: Some(serde_json::json!({ "message": error.to_string() })),
                };
                let mut failed_session = session.clone();
                failed_session.active_turn_id = None;
                if failed_session.status != "closed" && failed_session.status != "failed" {
                    failed_session.status = "ready".to_string();
                }
                failed_session.updated_at = now_ms();
                self.store.upsert_turn(&failed_turn)?;
                self.store.upsert_session(&failed_session)?;
                {
                    let mut turns = self.turns.write().await;
                    turns.insert(turn_id.clone(), failed_turn);
                }
                {
                    let mut sessions = self.sessions.write().await;
                    sessions.insert(session_id.to_string(), failed_session);
                }
                let _ = self
                    .append_event(
                        RuntimeEventScope::Session,
                        session_id,
                        Some(session_id),
                        Some(turn_id.as_str()),
                        "turn.failed",
                        RuntimeEventCriticality::Critical,
                        serde_json::json!({ "error": error.to_string() }),
                    )
                    .await?;
                return Err(error);
            }
        };

        let turn = TurnRecord {
            id: turn_id.clone(),
            session_id: session_id.to_string(),
            provider_turn_ref: None,
            status: if requires_approval {
                "waiting_for_approval".to_string()
            } else {
                "in_progress".to_string()
            },
            input: Value::Array(input.input),
            source: Some("user".to_string()),
            started_at: Some(now),
            completed_at: None,
            usage: None,
            error: None,
        };
        let mut updated_session = session.clone();
        updated_session.status = if requires_approval {
            "waiting_for_approval".to_string()
        } else {
            "turn_running".to_string()
        };
        updated_session.active_turn_id = Some(turn_id.clone());
        updated_session.updated_at = now;

        self.store.upsert_turn(&turn)?;
        self.store.upsert_session(&updated_session)?;
        {
            let mut turns = self.turns.write().await;
            turns.insert(turn_id.clone(), turn.clone());
        }
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated_session);
        }
        if requires_approval {
            let approval_id =
                approval_id.expect("approval id must exist when approval is required");
            let approval = ApprovalRecord {
                id: approval_id.clone(),
                session_id: session_id.to_string(),
                turn_id: turn_id.clone(),
                tool_call_id: None,
                provider_approval_ref: Some(approval_id.clone()),
                status: "pending".to_string(),
                request: serde_json::json!({
                    "reason": "manual approval required before provider execution",
                }),
                response: None,
                created_at: now,
                resolved_at: None,
            };
            self.store.upsert_approval(&approval)?;
            {
                let mut approvals = self.approvals.write().await;
                approvals.insert(approval_id.clone(), approval);
            }
            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(turn_id.as_str()),
                    "approval.requested",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({
                        "approval_id": approval_id,
                    }),
                )
                .await?;
        } else {
            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(turn_id.as_str()),
                    "turn.started",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({}),
                )
                .await?;
            self.spawn_wait_for_turn(provider_kind, session_id.to_string(), turn_id.clone());
        }

        Ok(SendTurnAccepted {
            session_id: session_id.to_string(),
            turn_id,
            status: if requires_approval {
                "waiting_for_approval".to_string()
            } else {
                let _ = ack;
                "in_progress".to_string()
            },
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

    pub async fn respond_approval(
        self: &Arc<Self>,
        session_id: &str,
        approval_id: &str,
        input: ApprovalResponseInput,
    ) -> Result<ApprovalRecord, RuntimeError> {
        let session = self.get_session(session_id).await?;
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;

        let mut approvals = self.approvals.write().await;
        let existing = approvals
            .get(approval_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("approval {approval_id}")))?;
        if existing.session_id != session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "approval {} does not belong to session {}",
                approval_id, session_id
            )));
        }
        if existing.status != "pending" {
            return Err(RuntimeError::InvalidState(format!(
                "approval {} is not pending",
                approval_id
            )));
        }
        let normalized_decision = ApprovalDecision::parse(input.decision.as_str())?;

        provider
            .respond_approval(ProviderApprovalResponseRequest {
                runtime_session_id: session_id.to_string(),
                turn_id: existing.turn_id.clone(),
                approval_id: approval_id.to_string(),
                decision: normalized_decision.as_str().to_string(),
                payload: input.payload.clone(),
            })
            .await?;

        let mut resolved = existing.clone();
        resolved.status = normalized_decision.as_str().to_string();
        resolved.response = input.payload.clone();
        resolved.resolved_at = Some(now_ms());
        self.store.upsert_approval(&resolved)?;
        approvals.insert(approval_id.to_string(), resolved.clone());
        drop(approvals);

        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(resolved.turn_id.as_str()),
                "approval.resolved",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "approval_id": approval_id }),
            )
            .await?;

        if normalized_decision == ApprovalDecision::Accept {
            let mut turns = self.turns.write().await;
            let mut sessions = self.sessions.write().await;
            if let Some(turn) = turns.get_mut(&resolved.turn_id) {
                turn.status = "in_progress".to_string();
                turn.error = None;
                self.store.upsert_turn(turn)?;
            }
            if let Some(session) = sessions.get_mut(session_id) {
                session.status = "turn_running".to_string();
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            drop(sessions);
            drop(turns);

            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(resolved.turn_id.as_str()),
                    "turn.started",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({
                        "source": "approval.accepted",
                    }),
                )
                .await?;
            self.spawn_wait_for_turn(
                provider_kind,
                session_id.to_string(),
                resolved.turn_id.clone(),
            );
        } else {
            let mut turns = self.turns.write().await;
            let mut sessions = self.sessions.write().await;
            if let Some(turn) = turns.get_mut(&resolved.turn_id) {
                turn.status = "interrupted".to_string();
                turn.completed_at = Some(now_ms());
                turn.error = Some(serde_json::json!({
                    "message": "approval declined",
                }));
                self.store.upsert_turn(turn)?;
            }
            if let Some(session) = sessions.get_mut(session_id) {
                if session.active_turn_id.as_deref() == Some(resolved.turn_id.as_str()) {
                    session.active_turn_id = None;
                }
                if session.status != "closed" && session.status != "failed" {
                    session.status = "ready".to_string();
                }
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            drop(sessions);
            drop(turns);
            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(resolved.turn_id.as_str()),
                    "turn.interrupted",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({
                        "source": "approval.declined",
                    }),
                )
                .await?;
        }

        Ok(resolved)
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

    async fn dispatch_send_turn_with_resume_fallback(
        &self,
        provider_kind: ProviderKind,
        request: ProviderSendTurnRequest,
        cwd: Option<String>,
        provider_session_ref: Option<String>,
        canonical_provider_session_ref: Option<String>,
        metadata: Value,
    ) -> Result<crate::ProviderTurnAck, RuntimeError> {
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;
        match provider.send_turn(request.clone()).await {
            Ok(ack) => Ok(ack),
            Err(RuntimeError::NotFound(_)) => {
                let provider_session_ref = provider_session_ref.ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "provider session {} was not found and cannot be resumed",
                        request.runtime_session_id
                    ))
                })?;
                provider
                    .resume_session(ProviderResumeSessionRequest {
                        runtime_session_id: request.runtime_session_id.clone(),
                        provider_session_ref,
                        canonical_provider_session_ref,
                        cwd,
                        metadata: Some(metadata),
                    })
                    .await?;
                provider.send_turn(request).await
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn spawn_wait_for_turn(
        self: &Arc<Self>,
        provider: ProviderKind,
        session_id: String,
        turn_id: String,
    ) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let provider_adapter = match manager.providers.get(provider) {
                Some(provider_adapter) => provider_adapter,
                None => return,
            };
            let result = provider_adapter
                .wait_for_turn(ProviderWaitTurnRequest {
                    runtime_session_id: session_id.clone(),
                    turn_id: turn_id.clone(),
                    timeout_ms: None,
                })
                .await;
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
