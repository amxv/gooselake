use std::sync::Arc;

use serde_json::Value;

use crate::{
    ApprovalDecision, ApprovalRecord, ProviderApprovalResponseRequest, ProviderDispatchOutcome,
    ProviderKind, ProviderSendTurnRequest, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, TurnDispatchState,
};

use super::helpers::{is_terminal_turn_status, now_ms};
use super::{ApprovalResponseInput, RuntimeSessionManager};

impl RuntimeSessionManager {
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

        let existing = self
            .approvals
            .read()
            .await
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

        if existing.origin == "runtime_pre_dispatch_policy" {
            let mut resolved = existing.clone();
            resolved.status = normalized_decision.as_str().to_string();
            resolved.response = input.payload.clone();
            resolved.resolved_at = Some(now_ms());
            self.store.upsert_approval(&resolved)?;
            self.approvals
                .write()
                .await
                .insert(approval_id.to_string(), resolved.clone());

            self.append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(resolved.turn_id.as_str()),
                "approval.resolved",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "approval_id": approval_id,
                    "origin": "runtime_pre_dispatch_policy",
                    "decision": normalized_decision.as_str(),
                }),
            )
            .await?;

            if normalized_decision == ApprovalDecision::Decline {
                self.update_turn_dispatch_authority(
                    resolved.turn_id.as_str(),
                    TurnDispatchState::NotDispatched,
                    None,
                    Some(serde_json::json!({
                        "code": "pre_dispatch_approval_declined",
                        "message": "runtime pre-dispatch approval declined",
                    })),
                )
                .await?;
                {
                    let mut turns = self.turns.write().await;
                    if let Some(turn) = turns.get_mut(&resolved.turn_id) {
                        turn.status = "interrupted".to_string();
                        turn.completed_at = Some(now_ms());
                        turn.error = Some(serde_json::json!({
                            "message": "approval declined before provider dispatch",
                        }));
                        self.store.upsert_turn(turn)?;
                    }
                }
                {
                    let mut sessions = self.sessions.write().await;
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
                }
                self.append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(resolved.turn_id.as_str()),
                    "turn.interrupted",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({
                        "source": "pre_dispatch_approval.declined",
                    }),
                )
                .await?;
                return Ok(resolved);
            }

            let turn = self
                .turns
                .read()
                .await
                .get(&resolved.turn_id)
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("turn {}", resolved.turn_id)))?;
            let admission = self
                .turn_admissions
                .read()
                .await
                .get(&resolved.turn_id)
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("turn admission {}", resolved.turn_id))
                })?;
            self.update_turn_dispatch_authority(
                resolved.turn_id.as_str(),
                TurnDispatchState::Dispatching,
                None,
                None,
            )
            .await?;
            let request = ProviderSendTurnRequest {
                runtime_session_id: session_id.to_string(),
                turn_id: resolved.turn_id.clone(),
                input: turn.input.as_array().cloned().unwrap_or_default(),
                expected_turn_id: admission.correlation.expected_turn_id.clone(),
                permission_mode: admission.dispatch_policy.permission_mode.clone(),
                approval_id: None,
            };
            let (ack, provider_events) = match self
                .dispatch_send_turn_with_resume_fallback(provider_kind, request, &session)
                .await
            {
                Ok((ack, provider_events))
                    if ack.runtime_session_id == session_id && ack.turn_id == resolved.turn_id =>
                {
                    (ack, provider_events)
                }
                Ok((ack, _)) => {
                    let error = RuntimeError::provider_dispatch_unknown(
                        "ack_identity_mismatch",
                        format!(
                            "provider send_turn acknowledgement mismatch after approval (expected_session={session_id}, expected_turn={}, actual_session={}, actual_turn={})",
                            resolved.turn_id, ack.runtime_session_id, ack.turn_id
                        ),
                    );
                    self.mark_dispatch_unknown(session_id, resolved.turn_id.as_str(), &error)
                        .await?;
                    return Err(error);
                }
                Err(error)
                    if error.provider_dispatch_outcome()
                        == ProviderDispatchOutcome::NotDispatched =>
                {
                    self.mark_not_dispatched(session_id, resolved.turn_id.as_str(), &error)
                        .await?;
                    return Err(error);
                }
                Err(error) => {
                    let unknown = RuntimeError::provider_dispatch_unknown(
                        error.provider_dispatch_code().unwrap_or("provider_error"),
                        error.to_string(),
                    );
                    self.mark_dispatch_unknown(session_id, resolved.turn_id.as_str(), &unknown)
                        .await?;
                    return Err(unknown);
                }
            };
            self.update_turn_dispatch_authority(
                resolved.turn_id.as_str(),
                TurnDispatchState::Dispatched,
                ack.provider_native_turn_id,
                None,
            )
            .await?;
            {
                let mut turns = self.turns.write().await;
                if let Some(turn) = turns.get_mut(&resolved.turn_id) {
                    turn.status = "in_progress".to_string();
                    turn.error = None;
                    self.store.upsert_turn(turn)?;
                }
            }
            {
                let mut sessions = self.sessions.write().await;
                if let Some(session) = sessions.get_mut(session_id) {
                    session.status = "turn_running".to_string();
                    session.failure_code = None;
                    session.failure_message = None;
                    session.updated_at = now_ms();
                    self.store.upsert_session(session)?;
                }
            }
            self.append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(resolved.turn_id.as_str()),
                "turn.started",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "source": "pre_dispatch_approval.accepted",
                }),
            )
            .await?;
            self.spawn_wait_for_turn(
                provider_kind,
                session_id.to_string(),
                resolved.turn_id.clone(),
                provider_events,
            );
            return Ok(resolved);
        }

        let mut decision_pending = existing.clone();
        decision_pending.status = "decision_pending".to_string();
        decision_pending.response = Some(serde_json::json!({
            "decision": normalized_decision.as_str(),
            "payload": input.payload.clone(),
        }));
        self.store.upsert_approval(&decision_pending)?;
        self.approvals
            .write()
            .await
            .insert(approval_id.to_string(), decision_pending.clone());

        let provider_approval_id = existing
            .provider_approval_ref
            .clone()
            .unwrap_or_else(|| approval_id.to_string());
        if let Err(error) = provider
            .respond_approval(ProviderApprovalResponseRequest {
                runtime_session_id: session_id.to_string(),
                turn_id: existing.turn_id.clone(),
                approval_id: provider_approval_id,
                decision: normalized_decision.as_str().to_string(),
                payload: input.payload.clone(),
            })
            .await
        {
            {
                let mut turns = self.turns.write().await;
                if let Some(turn) = turns.get_mut(&existing.turn_id) {
                    turn.status = "approval_decision_unknown".to_string();
                    turn.error = Some(serde_json::json!({
                        "message": error.to_string(),
                        "approval_id": approval_id,
                    }));
                    self.store.upsert_turn(turn)?;
                }
            }
            {
                let mut sessions = self.sessions.write().await;
                if let Some(session) = sessions.get_mut(session_id) {
                    session.status = "turn_recovery_required".to_string();
                    session.updated_at = now_ms();
                    self.store.upsert_session(session)?;
                }
            }
            self.append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(existing.turn_id.as_str()),
                "approval.decision_unknown",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "approval_id": approval_id,
                    "error": error.to_string(),
                }),
            )
            .await?;
            return Err(RuntimeError::provider_dispatch_unknown(
                "approval_decision_unknown",
                error.to_string(),
            ));
        }

        let mut resolved = existing.clone();
        resolved.status = normalized_decision.as_str().to_string();
        resolved.response = input.payload.clone();
        resolved.resolved_at = Some(now_ms());
        self.store.upsert_approval(&resolved)?;
        self.approvals
            .write()
            .await
            .insert(approval_id.to_string(), resolved.clone());

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

        if existing.origin == "provider" {
            let mut turns = self.turns.write().await;
            let mut sessions = self.sessions.write().await;
            if let Some(turn) = turns.get_mut(&resolved.turn_id) {
                if !is_terminal_turn_status(turn.status.as_str()) {
                    turn.status = "in_progress".to_string();
                    turn.error = None;
                    self.store.upsert_turn(turn)?;
                }
            }
            if let Some(session) = sessions.get_mut(session_id) {
                if session.active_turn_id.as_deref() == Some(resolved.turn_id.as_str())
                    && session.status != "closed"
                    && session.status != "failed"
                {
                    session.status = "turn_running".to_string();
                    session.updated_at = now_ms();
                    self.store.upsert_session(session)?;
                }
            }
            drop(sessions);
            drop(turns);
        } else if normalized_decision == ApprovalDecision::Accept {
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
            self.spawn_wait_for_turn(
                provider_kind,
                session_id.to_string(),
                resolved.turn_id.clone(),
                None,
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

    pub async fn record_provider_approval(
        &self,
        session_id: &str,
        turn_id: &str,
        provider_approval_ref: &str,
        tool_call_id: Option<String>,
        request: Value,
    ) -> Result<ApprovalRecord, RuntimeError> {
        let provider_approval_ref = provider_approval_ref.trim();
        if provider_approval_ref.is_empty() {
            return Err(RuntimeError::InvalidState(
                "provider approval reference is required".to_string(),
            ));
        }
        let session = self.get_session(session_id).await?;
        if session.active_turn_id.as_deref() != Some(turn_id) {
            return Err(RuntimeError::InvalidState(format!(
                "turn {turn_id} is not active for session {session_id}"
            )));
        }
        let turn = self
            .turns
            .read()
            .await
            .get(turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
        if turn.session_id != session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "turn {turn_id} does not belong to session {session_id}"
            )));
        }
        let admission = self
            .turn_admissions
            .read()
            .await
            .get(turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn admission {turn_id}")))?;
        if admission.dispatch_state != TurnDispatchState::Dispatched {
            return Err(RuntimeError::InvalidState(format!(
                "provider approval cannot be recorded while turn {turn_id} dispatch state is {}",
                admission.dispatch_state.as_str()
            )));
        }

        let mut approvals = self.approvals.write().await;
        if let Some(existing) = approvals.values().find(|approval| {
            approval.session_id == session_id
                && approval.turn_id == turn_id
                && approval.provider_approval_ref.as_deref() == Some(provider_approval_ref)
        }) {
            return Ok(existing.clone());
        }
        let now = now_ms();
        let approval = ApprovalRecord {
            id: self.allocate_id("apr", admission.provider.as_str()),
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            origin: "provider".to_string(),
            tool_call_id,
            provider_approval_ref: Some(provider_approval_ref.to_string()),
            status: "pending".to_string(),
            request,
            response: None,
            created_at: now,
            resolved_at: None,
        };
        self.store.upsert_approval(&approval)?;
        approvals.insert(approval.id.clone(), approval.clone());
        drop(approvals);

        {
            let mut turns = self.turns.write().await;
            if let Some(turn) = turns.get_mut(turn_id) {
                turn.status = "waiting_for_approval".to_string();
                self.store.upsert_turn(turn)?;
            }
        }
        {
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(session_id) {
                session.status = "waiting_for_approval".to_string();
                session.updated_at = now;
                self.store.upsert_session(session)?;
            }
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "approval.requested",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "approval_id": approval.id,
                "provider_approval_ref": provider_approval_ref,
                "origin": "provider",
            }),
        )
        .await?;
        Ok(approval)
    }
}
