use runtime_core::{
    ProviderSessionPreferencesMutationRequest, ProviderSessionPreferencesMutationResult,
    RuntimeError,
};

use crate::CodexProvider;

impl CodexProvider {
    pub(super) async fn mutate_preferences(
        &self,
        req: ProviderSessionPreferencesMutationRequest,
    ) -> Result<ProviderSessionPreferencesMutationResult, RuntimeError> {
        if let Some(effort) = req.current_preferences.thinking_effort {
            if !runtime_core::codex_model_policy()
                .thinking_efforts
                .iter()
                .any(|supported| supported == effort.as_str())
            {
                return Err(RuntimeError::Unsupported(format!(
                    "Codex does not support current thinking effort {}",
                    effort.as_str()
                )));
            }
        }
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(req.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("Codex session {}", req.runtime_session_id))
            })?;
        if session.active_turn_id.is_some() || !session.pending_approvals.is_empty() {
            return Err(RuntimeError::Conflict(format!(
                "cannot change thinking preference during active Codex work for {}",
                req.runtime_session_id
            )));
        }
        if req
            .expected_revision
            .is_some_and(|expected| expected != session.preferences_revision)
        {
            return Err(RuntimeError::Conflict(format!(
                "Codex preference revision conflict for {}",
                req.runtime_session_id
            )));
        }
        if session.current_preferences != req.current_preferences {
            session.current_preferences = req.current_preferences.clone();
            session.preferences_revision = session.preferences_revision.saturating_add(1);
        }
        Ok(ProviderSessionPreferencesMutationResult {
            revision: session.preferences_revision,
            current_preferences: session.current_preferences.clone(),
        })
    }
}
