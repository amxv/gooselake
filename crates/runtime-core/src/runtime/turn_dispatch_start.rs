use crate::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, TurnDispatchState,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// Positive native dispatch is already a potentially irreversible effect.
    /// Persist the mapping/running projection/critical receipt in one store
    /// transaction, and only then publish them to the in-process caches.
    /// Any failure retains the pre-native `dispatching` fence and may require
    /// operator inspection after restart. It must never cause a native resend.
    pub(super) async fn commit_provider_dispatch_started(
        &self,
        session_id: &str,
        turn_id: &str,
        native_turn_id: Option<String>,
        source: Option<&str>,
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

        if before_admission.dispatch_state != TurnDispatchState::Dispatching
            || before_admission.session_id != session_id
            || before_turn.session_id != session_id
            || before_session.active_turn_id.as_deref() != Some(turn_id)
            || native_turn_id
                .as_deref()
                .is_some_and(|id| id.is_empty() || id.trim() != id)
        {
            return Err(RuntimeError::ProtocolViolation(
                "positive native acknowledgement is not owned by one dispatching turn".into(),
            ));
        }
        let now = now_ms();
        let mut after_admission = before_admission.clone();
        after_admission.dispatch_state = TurnDispatchState::Dispatched;
        after_admission.provider_native_turn_id = native_turn_id;
        after_admission.dispatch_error = None;
        after_admission.updated_at = now.max(before_admission.updated_at);
        let mut after_turn = before_turn.clone();
        after_turn.status = "in_progress".into();
        after_turn.error = None;
        let mut after_session = before_session.clone();
        after_session.status = "turn_running".into();
        after_session.failure_code = None;
        after_session.failure_message = None;
        after_session.updated_at = now.max(before_session.updated_at);

        let event = NewRuntimeEvent {
            event_id: format!("evt_turn_{turn_id}_started"),
            scope: RuntimeEventScope::Session,
            scope_id: session_id.into(),
            session_id: Some(session_id.into()),
            team_id: None,
            turn_id: Some(turn_id.into()),
            kind: "turn.started".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: source.map_or_else(
                || serde_json::json!({}),
                |source| serde_json::json!({"source":source}),
            ),
            provider: None,
            provider_seq: None,
            created_at: now,
        };
        let committed = self.store.commit_provider_turn_started(
            &before_admission,
            &after_admission,
            &before_turn,
            &after_turn,
            &before_session,
            &after_session,
            &event,
        )?;
        // Atomic SQLite is the authority. Live notifications are strictly
        // ancillary; an event listener failure cannot trigger native retry.
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
        let _ = self.event_tx.send(committed.event);
        Ok(())
    }
}
