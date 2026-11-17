use super::helpers::now_ms;
use super::{CreateSessionInput, ResumeSessionInput, RuntimeSessionManager};
use crate::{ProviderAuthStatus, ProviderKind, RuntimeError, SessionRecord, TurnRecord};
use serde_json::Value;
use std::collections::HashMap;

impl RuntimeSessionManager {
    /// Deterministic /v1 provider-launch checks before an unrelated Git
    /// checkout or init script. This is not native provider acceptance: the
    /// actual create path still applies its locks and checks again.
    pub fn preflight_legacy_session_create(
        &self,
        provider: ProviderKind,
        permission_mode: Option<&str>,
    ) -> Result<(), RuntimeError> {
        self.providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().to_string()))?;
        if let Some(mode) = permission_mode {
            crate::ProviderPermissionIntent::explicit(mode.to_string())?;
        }
        Ok(())
    }

    pub async fn list_sessions(&self) -> Vec<SessionRecord> {
        let sessions = self.sessions.read().await;
        let mut rows = sessions.values().cloned().collect::<Vec<_>>();
        rows.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        rows
    }

    pub async fn get_session(&self, session_id: &str) -> Result<SessionRecord, RuntimeError> {
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))
    }

    /// Capture a status page under one session read lock. The caller supplies
    /// bounded agent IDs, so this never snapshots or sorts all runtime sessions.
    pub async fn session_snapshots_for_agents(
        &self,
        agent_ids: &[String],
    ) -> HashMap<String, SessionRecord> {
        let sessions = self.sessions.read().await;
        agent_ids
            .iter()
            .filter_map(|id| {
                sessions
                    .get(id)
                    .map(|session| (id.clone(), session.clone()))
            })
            .collect()
    }

    pub async fn list_session_turns(
        &self,
        session_id: &str,
    ) -> Result<Vec<TurnRecord>, RuntimeError> {
        self.get_session(session_id).await?;
        let turns = self.turns.read().await;
        let mut rows = turns
            .values()
            .filter(|turn| turn.session_id == session_id)
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.started_at
                .cmp(&right.started_at)
                .then_with(|| left.completed_at.cmp(&right.completed_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(rows)
    }

    pub async fn set_session_worktree_id(
        &self,
        session_id: &str,
        worktree_id: Option<String>,
    ) -> Result<SessionRecord, RuntimeError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
        session.worktree_id = worktree_id;
        session.updated_at = now_ms();
        let updated = session.clone();
        drop(sessions);
        self.store.upsert_session(&updated)?;
        Ok(updated)
    }

    pub async fn provider_auth_status(
        &self,
        provider: ProviderKind,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().to_string()))?;
        adapter.auth_status().await
    }

    pub async fn provider_auth_set_api_key(
        &self,
        provider: ProviderKind,
        api_key: String,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().to_string()))?;
        adapter.auth_set_api_key(api_key).await
    }

    pub async fn provider_auth_import_json(
        &self,
        provider: ProviderKind,
        auth_json: Value,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().to_string()))?;
        adapter.auth_import_json(auth_json).await
    }

    pub async fn provider_auth_import_json_text(
        &self,
        provider: ProviderKind,
        auth_json_text: String,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().to_string()))?;
        adapter.auth_import_json_text(auth_json_text).await
    }

    pub async fn provider_auth_logout(
        &self,
        provider: ProviderKind,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().to_string()))?;
        adapter.auth_logout().await
    }

    pub async fn create_session(
        &self,
        _input: CreateSessionInput,
    ) -> Result<SessionRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "legacy direct session creation is unsupported; use durable workspace-agent creation"
                .into(),
        ))
    }
    pub async fn close_session(
        &self,
        _session_id: &str,
        _reason: Option<String>,
    ) -> Result<SessionRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "legacy direct session close is unsupported; use journaled workspace-agent archive or retirement".into()))
    }
    pub async fn force_close_session(
        &self,
        _session_id: &str,
        _reason: Option<String>,
    ) -> Result<SessionRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "legacy forced session close is unsupported; inspect the original operation because this method cannot establish native closure".into()))
    }
    pub async fn resume_session(
        &self,
        _session_id: &str,
        _input: ResumeSessionInput,
    ) -> Result<SessionRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "legacy direct session resume is unsupported; use runtime startup recovery for active stored sessions or the workspace-agent restore operation for archived agents".into()))
    }
}
