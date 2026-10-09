use crate::{
    ProviderCapabilitySupport, ProviderKind, RuntimeError, SessionContextLimitSnapshot,
    WorkspaceAgentLifecycleState,
};

use super::{helpers::now_ms, RuntimeSessionManager};

impl RuntimeSessionManager {
    pub(super) async fn record_terminal_context_limit(&self, agent_id: &str, turn_id: &str) {
        // A missing observation never fabricates an estimated percentage;
        // late attachment evidence is rejected by the store's atomic CAS.
        let _ = self
            .refresh_session_context_limit(agent_id, Some(turn_id))
            .await;
    }

    /// Read only the provider's latest persisted observation for the current
    /// attachment. A stale row is deliberately invisible after a route change.
    pub fn session_context_limit(
        &self,
        agent_id: &str,
    ) -> Result<Option<SessionContextLimitSnapshot>, RuntimeError> {
        self.store
            .get_workspace_agent_by_id(agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        self.store.get_session_context_limit(agent_id)
    }

    /// The provider is interrogated without deriving context size from text or
    /// usage totals. The database compares the *pre-observation* session and
    /// revision identities in its INSERT/UPDATE transaction, so an observation
    /// from a detached or rebound provider cannot become authoritative.
    pub async fn refresh_session_context_limit(
        &self,
        agent_id: &str,
        observed_turn_id: Option<&str>,
    ) -> Result<SessionContextLimitSnapshot, RuntimeError> {
        let _guard = self.session_policy_mutation_lock.lock().await;
        let agent = self
            .store
            .get_workspace_agent_by_id(agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active {
            return Err(RuntimeError::InvalidState(format!(
                "workspace agent {agent_id} is archived"
            )));
        }
        let session = self.get_session(agent_id).await?;
        if session.active_turn_id.is_some() || session.status != "ready" {
            return Err(RuntimeError::InvalidState(format!(
                "workspace agent {agent_id} is not idle for a context-limit observation"
            )));
        }
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!(
                "unknown provider {} for workspace agent {agent_id}",
                session.provider
            ))
        })?;
        if provider_kind != agent.recreation_policy.provider {
            return Err(RuntimeError::ProtocolViolation(format!(
                "provider authority mismatch for workspace agent {agent_id}"
            )));
        }
        let provider_ref = session.provider_session_ref.clone().ok_or_else(|| {
            RuntimeError::InvalidState(format!(
                "workspace agent {agent_id} has no verified provider attachment"
            ))
        })?;
        let provider = self
            .providers
            .get(provider_kind)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider_kind.as_str().into()))?;
        if provider.capabilities().context_limit_observation != ProviderCapabilitySupport::Supported
        {
            return Err(RuntimeError::Unsupported(format!(
                "provider {} does not expose context-limit observations",
                provider_kind.as_str()
            )));
        }
        let observed_at_ms = now_ms();
        let observation = provider.observe_context_limit(agent_id).await?;
        let snapshot = SessionContextLimitSnapshot {
            agent_id: agent_id.into(),
            provider: provider_kind,
            provider_session_ref: provider_ref,
            canonical_provider_session_ref: session.canonical_provider_session_ref,
            agent_revision: agent.revision,
            observation,
            observed_at_ms,
            observed_turn_id: observed_turn_id.map(str::to_string),
        };
        snapshot.validate()?;
        if !self
            .store
            .record_session_context_limit(&snapshot, session.updated_at)?
        {
            return Err(RuntimeError::Conflict(format!(
                "context-limit observation for workspace agent {agent_id} is stale; provider binding or newer observation changed"
            )));
        }
        Ok(snapshot)
    }
}
