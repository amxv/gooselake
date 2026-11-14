use crate::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, TurnDispatchState,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    pub(super) async fn commit_provider_dispatch_outcome(
        &self,
        session_id: &str,
        turn_id: &str,
        error: &RuntimeError,
        known_not_dispatched: bool,
    ) -> Result<(), RuntimeError> {
        let before_admission = self
            .turn_admissions
            .read()
            .await
            .get(turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn admission {turn_id}")))?;
        let before_turn = self
            .turns
            .read()
            .await
            .get(turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
        let before_session = self
            .sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
        if before_admission.session_id != session_id
            || before_turn.session_id != session_id
            || before_session.active_turn_id.as_deref() != Some(turn_id)
        {
            return Err(RuntimeError::ProtocolViolation(
                "provider dispatch outcome does not own the active turn".into(),
            ));
        }
        let now = now_ms();
        let message = error.to_string();
        let outcome = if known_not_dispatched {
            "not_dispatched"
        } else {
            "unknown"
        };
        let mut after_admission = before_admission.clone();
        after_admission.dispatch_state = if known_not_dispatched {
            TurnDispatchState::NotDispatched
        } else {
            TurnDispatchState::Unknown
        };
        after_admission.dispatch_error = Some(serde_json::json!({
            "code":error.provider_dispatch_code(), "message":message,
        }));
        after_admission.updated_at = now.max(before_admission.updated_at);
        let mut after_turn = before_turn.clone();
        after_turn.status = if known_not_dispatched {
            "failed"
        } else {
            "dispatch_unknown"
        }
        .into();
        if known_not_dispatched {
            after_turn.completed_at = Some(now.max(before_turn.started_at.unwrap_or(now)));
        }
        after_turn.error = Some(serde_json::json!({
            "message":message,"provider_dispatch_outcome":outcome,
        }));
        let mut after_session = before_session.clone();
        after_session.status = if known_not_dispatched {
            "ready"
        } else {
            "turn_recovery_required"
        }
        .into();
        if known_not_dispatched {
            after_session.active_turn_id = None;
        }
        after_session.updated_at = now.max(before_session.updated_at);

        let mut approvals = Vec::new();
        if known_not_dispatched {
            for approval in self.approvals.read().await.values().filter(|approval| {
                approval.session_id == session_id
                    && approval.turn_id == turn_id
                    && approval.status == "pending"
            }) {
                let mut cancelled = approval.clone();
                cancelled.status = "cancelled".into();
                cancelled.response = Some(serde_json::json!({
                    "reason":"provider dispatch was proven not to have occurred",
                }));
                cancelled.resolved_at = Some(now.max(approval.created_at));
                approvals.push((approval.clone(), cancelled));
            }
            approvals.sort_by(|(first, _), (second, _)| first.id.cmp(&second.id));
        }
        let event = NewRuntimeEvent {
            event_id: format!(
                "evt_turn_{turn_id}_{}",
                if known_not_dispatched {
                    "not_dispatched"
                } else {
                    "dispatch_unknown"
                },
            ),
            scope: RuntimeEventScope::Session,
            scope_id: session_id.into(),
            session_id: Some(session_id.into()),
            team_id: None,
            turn_id: Some(turn_id.into()),
            kind: if known_not_dispatched {
                "turn.failed"
            } else {
                "turn.dispatch_unknown"
            }
            .into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"error":message,"provider_dispatch_outcome":outcome}),
            provider: None,
            provider_seq: None,
            created_at: now,
        };
        let committed = self.store.commit_provider_dispatch_outcome(
            &before_admission,
            &after_admission,
            &before_turn,
            &after_turn,
            &before_session,
            &after_session,
            &approvals,
            &event,
        )?;
        // Only the SQLite authority can unlock a *proven* no-effect turn.
        // Failed Critical insertion leaves DISPATCHING fenced, including after
        // a lost provider response. It cannot cause a duplicate native send.
        self.turn_admissions
            .write()
            .await
            .insert(turn_id.into(), committed.admission);
        self.turns
            .write()
            .await
            .insert(turn_id.into(), committed.turn);
        self.sessions
            .write()
            .await
            .insert(session_id.into(), committed.session);
        if !committed.approvals.is_empty() {
            let mut cache = self.approvals.write().await;
            for approval in committed.approvals {
                cache.insert(approval.id.clone(), approval);
            }
        }
        let _ = self.event_tx.send(committed.event);
        Ok(())
    }
}
