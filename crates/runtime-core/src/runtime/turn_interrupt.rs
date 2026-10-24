use crate::{
    ProviderInterruptTurnRequest, ProviderKind, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope,
};

use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    pub async fn interrupt_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        if let Some(operation_id) = self.store.retiring_operation_for_session(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "workspace retirement {operation_id} owns native turn interruption"
            )));
        }
        self.interrupt_turn_under_policy_guard(session_id, turn_id)
            .await
    }

    pub(super) async fn interrupt_turn_under_policy_guard(
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
}
