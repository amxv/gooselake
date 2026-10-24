use crate::{
    ProviderTurnResult, ProviderTurnStatus, RuntimeError, RuntimeEventCriticality,
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
            let mut turn = turn.expect("current callback turn");
            turn.status = "provider_event_recovery_required".into();
            turn.error = Some(serde_json::json!({"code":code,"message":message}));
            self.store.upsert_turn(&turn)?;
            turns.insert(turn.id.clone(), turn);
            let mut session = session.expect("current callback session");
            session.status = "turn_recovery_required".into();
            session.failure_code = Some(code.into());
            session.failure_message = Some(message.clone());
            session.updated_at = now_ms();
            self.store.upsert_session(&session)?;
            sessions.insert(session.id.clone(), session);
        }
        drop(sessions);
        drop(turns);
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
        turn.status = result.status.as_str().into();
        turn.completed_at = Some(now_ms());
        turn.usage = result.usage.clone();
        turn.error = result.error.clone();
        self.store.upsert_turn(&turn)?;
        turns.insert(turn.id.clone(), turn.clone());

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
            self.store.upsert_session(&session)?;
            sessions.insert(session.id.clone(), session);
        }
        drop(sessions);
        drop(turns);
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
        self.append_event(
            RuntimeEventScope::Session,
            &result.runtime_session_id,
            Some(&result.runtime_session_id),
            Some(&result.turn_id),
            event_kind,
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "status":result.status.as_str(),"usage":result.usage,
                "error":result.error,"assistant_text":assistant_text,
            }),
        )
        .await?;
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
            let mut turn = turn.expect("current callback turn");
            turn.status = "failed".into();
            turn.completed_at = Some(now_ms());
            turn.error = Some(serde_json::json!({"message":error.to_string()}));
            self.store.upsert_turn(&turn)?;
            turns.insert(turn.id.clone(), turn);
            let mut session = session.expect("current callback session");
            session.active_turn_id = None;
            session.status = "failed".into();
            session.failure_code = Some("provider_wait_failure".into());
            session.failure_message = Some(error.to_string());
            session.updated_at = now_ms();
            self.store.upsert_session(&session)?;
            sessions.insert(session.id.clone(), session);
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
