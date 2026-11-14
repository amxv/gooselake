use crate::{
    NewRuntimeEvent, ProviderTurnResult, ProviderTurnStatus, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, SessionRecord, TurnRecord,
};

use super::helpers::{
    append_session_transcript, extract_assistant_text_from_usage, extract_turn_user_text,
    is_terminal_turn_status, now_ms,
};
use super::RuntimeSessionManager;

fn terminal_turn(turn: &TurnRecord) -> bool {
    is_terminal_turn_status(&turn.status)
        || matches!(turn.status.as_str(), "cancelled" | "superseded")
}

fn operator_closed_turn(turn: &TurnRecord) -> bool {
    terminal_turn(turn)
        && turn.completed_at.is_some()
        && turn.error.as_ref().is_some_and(|error| {
            error.get("code").and_then(serde_json::Value::as_str)
                == Some("operator_verified_native_attachment_closed")
                && error.get("proof").is_some_and(|proof| {
                    proof
                        .get("operation_id")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
                        && proof
                            .get("provider_close_effect_id")
                            .and_then(serde_json::Value::as_str)
                            .is_some()
                })
        })
}

fn stale_callback_reason(
    turn: Option<&TurnRecord>,
    session: Option<&SessionRecord>,
    turn_id: &str,
) -> Option<&'static str> {
    let Some(turn) = turn else {
        return Some("turn_not_attached");
    };
    if operator_closed_turn(turn) {
        return Some("operator_verified_native_attachment_closed");
    }
    if terminal_turn(turn) {
        return Some("turn_already_terminal");
    }
    let Some(session) = session else {
        return Some("session_not_attached");
    };
    if session.status == "closed" {
        return Some("session_already_closed");
    }
    if session.active_turn_id.as_deref() != Some(turn_id) {
        return Some("turn_no_longer_active");
    }
    // A provider error is not independent evidence that a quarantined native
    // effect stopped. Never turn a previously ambiguous provider stream or
    // approval decision into a retryable terminal failure by observation.
    if session.status == "turn_recovery_required"
        || matches!(
            turn.status.as_str(),
            "provider_event_recovery_required" | "approval_decision_unknown"
        )
    {
        return Some("turn_recovery_quarantined");
    }
    None
}

impl RuntimeSessionManager {
    pub(super) async fn mark_provider_event_stream_unknown(
        &self,
        session_id: &str,
        turn_id: &str,
        code: &str,
        message: String,
    ) -> Result<(), RuntimeError> {
        // Review commits and hydrates with the same lock order. Keep the proof
        // check and both projection decisions on one side of that boundary.
        let mut turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        let turn = turns.get(turn_id).cloned();
        if turn
            .as_ref()
            .is_some_and(|turn| turn.session_id != session_id)
        {
            return Err(RuntimeError::ProtocolViolation(
                "provider turn ownership mismatch".into(),
            ));
        }
        let session = sessions.get(session_id).cloned();
        let preserved = stale_callback_reason(turn.as_ref(), session.as_ref(), turn_id);
        if preserved.is_none() {
            let before_turn = turn.expect("current callback turn");
            let before_session = session.expect("current callback session");
            let mut updated_turn = before_turn.clone();
            updated_turn.status = "provider_event_recovery_required".into();
            updated_turn.error = Some(serde_json::json!({"code":code,"message":message}));
            let mut updated_session = before_session.clone();
            updated_session.status = "turn_recovery_required".into();
            updated_session.failure_code = Some(code.into());
            updated_session.failure_message = Some(message.clone());
            updated_session.updated_at = now_ms();
            let event = NewRuntimeEvent {
                event_id: format!("evt_turn_{turn_id}_event_stream_unknown"),
                scope: RuntimeEventScope::Session,
                scope_id: session_id.to_string(),
                session_id: Some(session_id.to_string()),
                team_id: None,
                turn_id: Some(turn_id.to_string()),
                kind: "provider.event_stream_unknown".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"code":code,"message":message}),
                provider: None,
                provider_seq: None,
                created_at: now_ms(),
            };
            let committed = self.store.commit_provider_failure_result(
                &before_turn,
                &updated_turn,
                &before_session,
                &updated_session,
                &event,
            )?;
            turns.insert(committed.turn.id.clone(), committed.turn);
            if let Some(session) = committed.session {
                sessions.insert(session.id.clone(), session);
            }
            drop(sessions);
            drop(turns);
            let _ = self.event_tx.send(committed.event);
            return Ok(());
        }
        drop(sessions);
        drop(turns);
        if preserved == Some("turn_recovery_quarantined") {
            // First durable ambiguous-outcome receipt remains authoritative.
            return Ok(());
        }
        let mut payload = serde_json::json!({"code":code,"message":message});
        if let Some(reason) = preserved {
            payload["state_preserved"] = true.into();
            payload["reason"] = reason.into();
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "provider.event_stream_unknown",
            RuntimeEventCriticality::Critical,
            payload,
        )
        .await?;
        Ok(())
    }

    /// Returns whether the result applied to the current attachment. Ignored
    /// late evidence must not refresh context observations for a restored one.
    pub(super) async fn apply_terminal_result(
        &self,
        result: ProviderTurnResult,
    ) -> Result<bool, RuntimeError> {
        let mut turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        let mut turn = turns
            .get(&result.turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn {}", result.turn_id)))?;
        if turn.session_id != result.runtime_session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "provider turn ownership mismatch for turn {}",
                result.turn_id
            )));
        }
        if operator_closed_turn(&turn) {
            let payload = serde_json::json!({
                "status":result.status.as_str(),"usage":result.usage,"error":result.error,
                "stored_status":turn.status,"state_preserved":true,
                "reason":"operator_verified_native_attachment_closed",
                "closure_proof":turn.error.as_ref().and_then(|error|error.get("proof")),
            });
            drop(sessions);
            drop(turns);
            self.append_event(
                RuntimeEventScope::Session,
                &result.runtime_session_id,
                Some(&result.runtime_session_id),
                Some(&result.turn_id),
                "provider.late_turn_outcome",
                RuntimeEventCriticality::Critical,
                payload,
            )
            .await?;
            return Ok(false);
        }
        if terminal_turn(&turn) {
            if turn.status == result.status.as_str() {
                return Ok(false);
            }
            let conflict = format!(
                "conflicting terminal state for turn {} (stored={}, incoming={})",
                result.turn_id,
                turn.status,
                result.status.as_str()
            );
            // This is a *late observation* of an already-terminal turn, not
            // authority over whichever turn currently owns the session. An
            // old provider callback must not fail a newer active attachment
            // (or reopen a session which has since been closed).
            let preserved_reason = sessions.get(&turn.session_id).and_then(|session| {
                if session.status == "closed" {
                    Some("session_already_closed")
                } else if session
                    .active_turn_id
                    .as_deref()
                    .is_some_and(|active| active != turn.id)
                {
                    Some("newer_turn_active")
                } else {
                    None
                }
            });
            if let Some(reason) = preserved_reason {
                let payload = serde_json::json!({
                    "status": result.status.as_str(),
                    "stored_status": turn.status,
                    "state_preserved": true,
                    "reason": reason,
                });
                drop(sessions);
                drop(turns);
                self.append_event(
                    RuntimeEventScope::Session,
                    &result.runtime_session_id,
                    Some(&result.runtime_session_id),
                    Some(&result.turn_id),
                    "provider.late_turn_outcome",
                    RuntimeEventCriticality::Critical,
                    payload,
                )
                .await?;
                return Err(RuntimeError::ProtocolViolation(conflict));
            }
            if let Some(mut session) = sessions.get(&turn.session_id).cloned() {
                session.status = "failed".into();
                session.failure_code = Some("terminal_conflict".into());
                session.failure_message = Some(conflict.clone());
                session.updated_at = now_ms();
                self.store.upsert_session(&session)?;
                sessions.insert(session.id.clone(), session);
            }
            return Err(RuntimeError::ProtocolViolation(conflict));
        }
        let mut session = sessions
            .get(&result.runtime_session_id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("session {}", result.runtime_session_id))
            })?;
        let current_attachment = session.status != "closed"
            && session.active_turn_id.as_deref() == Some(result.turn_id.as_str());
        let previous_turn = turn.clone();
        let previous_session = current_attachment.then(|| session.clone());
        turn.status = result.status.as_str().into();
        turn.completed_at = Some(now_ms());
        turn.usage = result.usage.clone();
        turn.error = result.error.clone();

        if current_attachment {
            session.active_turn_id = None;
            if session.status != "failed" {
                session.status = "ready".into();
            }
            if matches!(
                result.status,
                ProviderTurnStatus::Completed | ProviderTurnStatus::Interrupted
            ) {
                if let Some(text) = extract_turn_user_text(turn.input.as_array()) {
                    append_session_transcript(&mut session.metadata, "user", &text);
                }
                if let Some(text) = result
                    .usage
                    .as_ref()
                    .and_then(extract_assistant_text_from_usage)
                {
                    append_session_transcript(&mut session.metadata, "assistant", &text);
                }
            }
            session.updated_at = now_ms();
        }
        let event_kind = match result.status {
            ProviderTurnStatus::Completed => "turn.completed",
            ProviderTurnStatus::Interrupted => "turn.interrupted",
            ProviderTurnStatus::Failed => "turn.failed",
            ProviderTurnStatus::InProgress => {
                return Err(RuntimeError::ProtocolViolation(
                    "provider wait returned a nonterminal result".into(),
                ))
            }
        };
        let assistant_text = result
            .usage
            .as_ref()
            .and_then(extract_assistant_text_from_usage);
        let event = NewRuntimeEvent {
            event_id: format!("evt_turn_{}_terminal", result.turn_id),
            scope: RuntimeEventScope::Session,
            scope_id: result.runtime_session_id.clone(),
            session_id: Some(result.runtime_session_id.clone()),
            team_id: None,
            turn_id: Some(result.turn_id.clone()),
            kind: event_kind.into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({
                "status":result.status.as_str(),"usage":result.usage,
                "error":result.error,"assistant_text":assistant_text,
            }),
            provider: None,
            provider_seq: None,
            created_at: now_ms(),
        };
        // The provider has already finished. Commit its turn, attached
        // session and critical event in one store transaction before touching
        // the caches. A failed journal write must not leave partial terminal
        // state or imply permission to resend native input.
        let committed = self.store.commit_provider_turn_result(
            &previous_turn,
            &turn,
            previous_session.as_ref(),
            current_attachment.then_some(&session),
            &event,
        )?;
        turns.insert(committed.turn.id.clone(), committed.turn);
        if let Some(persisted_session) = committed.session {
            sessions.insert(persisted_session.id.clone(), persisted_session);
        }
        drop(sessions);
        drop(turns);
        let _ = self.event_tx.send(committed.event);
        Ok(current_attachment)
    }

    pub(super) async fn apply_terminal_failure(
        &self,
        session_id: &str,
        turn_id: &str,
        error: RuntimeError,
    ) -> Result<(), RuntimeError> {
        let mut turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        let turn = turns.get(turn_id).cloned();
        if turn
            .as_ref()
            .is_some_and(|turn| turn.session_id != session_id)
        {
            return Err(RuntimeError::ProtocolViolation(
                "provider turn ownership mismatch".into(),
            ));
        }
        let session = sessions.get(session_id).cloned();
        let preserved = stale_callback_reason(turn.as_ref(), session.as_ref(), turn_id);
        if preserved.is_none() {
            let before_turn = turn.expect("current callback turn");
            let before_session = session.expect("current callback session");
            let mut updated_turn = before_turn.clone();
            updated_turn.status = "failed".into();
            updated_turn.completed_at = Some(now_ms());
            updated_turn.error = Some(serde_json::json!({"message":error.to_string()}));
            let mut updated_session = before_session.clone();
            updated_session.active_turn_id = None;
            updated_session.status = "failed".into();
            updated_session.failure_code = Some("provider_wait_failure".into());
            updated_session.failure_message = Some(error.to_string());
            updated_session.updated_at = now_ms();
            let event = NewRuntimeEvent {
                event_id: format!("evt_turn_{turn_id}_wait_failure"),
                scope: RuntimeEventScope::Session,
                scope_id: session_id.to_string(),
                session_id: Some(session_id.to_string()),
                team_id: None,
                turn_id: Some(turn_id.to_string()),
                kind: "provider.error".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"error":error.to_string()}),
                provider: None,
                provider_seq: None,
                created_at: now_ms(),
            };
            let committed = self.store.commit_provider_failure_result(
                &before_turn,
                &updated_turn,
                &before_session,
                &updated_session,
                &event,
            )?;
            turns.insert(committed.turn.id.clone(), committed.turn);
            if let Some(session) = committed.session {
                sessions.insert(session.id.clone(), session);
            }
            drop(sessions);
            drop(turns);
            let _ = self.event_tx.send(committed.event);
            return Ok(());
        }
        drop(sessions);
        drop(turns);
        let mut payload = serde_json::json!({"error":error.to_string()});
        if let Some(reason) = preserved {
            payload["state_preserved"] = true.into();
            payload["reason"] = reason.into();
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "provider.error",
            RuntimeEventCriticality::Critical,
            payload,
        )
        .await?;
        Ok(())
    }
}
