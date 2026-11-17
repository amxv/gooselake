use crate::{
    ProviderCapabilitySupport, ProviderKind, ProviderThinkingEffort, ProviderTurnSettings,
    RuntimeError, SessionRecord,
};

use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// A turn cannot implicitly perform a separate native policy mutation.
    /// Explicit selection must match current policy, which admission rechecks
    /// transactionally; omitting it leaves the persisted policy unchanged.
    pub(super) fn validate_selected_turn_thinking(
        &self,
        session_id: &str,
        selected: Option<ProviderThinkingEffort>,
        session: SessionRecord,
    ) -> Result<SessionRecord, RuntimeError> {
        let Some(effort) = selected else {
            return Ok(session);
        };
        let agent = self
            .store
            .get_workspace_agent_by_id(session_id)?
            .ok_or_else(|| {
                RuntimeError::Unsupported(
                    "turn-selected thinking requires durable workspace agent policy".into(),
                )
            })?;
        if agent.recreation_policy.current_preferences.thinking_effort == Some(effort) {
            return Ok(session);
        }
        Err(RuntimeError::Unsupported(
            "turn-selected thinking must match the durable agent preference; update the revisioned preferences endpoint before submitting a turn".into(),
        ))
    }

    pub(super) fn validate_turn_settings(
        &self,
        provider_kind: ProviderKind,
        settings: &ProviderTurnSettings,
    ) -> Result<(), RuntimeError> {
        if settings.codex_collaboration_mode.is_some() && provider_kind != ProviderKind::Codex {
            return Err(RuntimeError::Unsupported(
                "Codex collaboration mode is only available on Codex turns".into(),
            ));
        }
        if settings.thinking_effort.is_some()
            && self.providers.get(provider_kind).is_none_or(|adapter| {
                adapter.capabilities().session_preferences != ProviderCapabilitySupport::Supported
            })
        {
            return Err(RuntimeError::Unsupported(format!(
                "provider {} does not support turn-scoped thinking effort",
                provider_kind.as_str()
            )));
        }
        Ok(())
    }
}
