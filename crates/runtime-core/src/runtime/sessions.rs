use serde_json::Value;

use crate::{
    ProviderAuthStatus, ProviderCloseSessionRequest, ProviderCreateSessionRequest, ProviderKind,
    ProviderResumeSessionRequest, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
    RuntimeRecordMutation, SessionRecord, TurnRecord,
};

use super::helpers::now_ms;
use super::{CreateSessionInput, ResumeSessionInput, RuntimeSessionManager};

impl RuntimeSessionManager {
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
        self.append_event_with_mutations(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            None,
            "session.worktree_changed",
            RuntimeEventCriticality::Critical,
            serde_json::json!({ "worktree_id": updated.worktree_id }),
            &[RuntimeRecordMutation::Session(updated.clone())],
        )
        .await?;
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
        input: CreateSessionInput,
    ) -> Result<SessionRecord, RuntimeError> {
        let provider = self.providers.get(input.provider).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(input.provider.as_str().to_string())
        })?;
        let now = now_ms();
        let session_id = self.allocate_id("sess", input.provider.as_str());
        let created = provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: session_id.clone(),
                model: input.model.clone(),
                cwd: input.cwd.clone(),
                permission_mode: input.permission_mode.clone(),
                metadata: input.metadata.clone(),
            })
            .await?;

        if created.runtime_session_id != session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "provider returned mismatched runtime session id (expected={session_id}, actual={})",
                created.runtime_session_id
            )));
        }

        let record = SessionRecord {
            id: session_id.clone(),
            provider: input.provider.as_str().to_string(),
            status: "ready".to_string(),
            cwd: input.cwd,
            model: input.model,
            permission_mode: input.permission_mode,
            system_prompt: None,
            metadata: input.metadata.unwrap_or(Value::Object(Default::default())),
            provider_session_ref: Some(created.provider_session_ref),
            canonical_provider_session_ref: created.canonical_provider_session_ref,
            active_turn_id: None,
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        };

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), record.clone());
        }
        let _ = self
            .append_event_with_mutations(
                RuntimeEventScope::Session,
                session_id.as_str(),
                Some(session_id.as_str()),
                None,
                "session.created",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "provider": record.provider }),
                &[RuntimeRecordMutation::Session(record.clone())],
            )
            .await?;
        Ok(record)
    }

    pub async fn close_session(
        &self,
        session_id: &str,
        reason: Option<String>,
    ) -> Result<SessionRecord, RuntimeError> {
        let session = self.get_session(session_id).await?;
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;

        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: session_id.to_string(),
                reason: reason.clone(),
            })
            .await?;

        self.finalize_session_close(
            session_id,
            reason.unwrap_or_else(|| "closed_by_request".to_string()),
        )
        .await
    }

    pub async fn force_close_session(
        &self,
        session_id: &str,
        reason: Option<String>,
    ) -> Result<SessionRecord, RuntimeError> {
        self.finalize_session_close(
            session_id,
            reason.unwrap_or_else(|| "closed_by_runtime_rollback".to_string()),
        )
        .await
    }

    pub async fn resume_session(
        &self,
        session_id: &str,
        input: ResumeSessionInput,
    ) -> Result<SessionRecord, RuntimeError> {
        let session = self.get_session(session_id).await?;
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;

        let provider_session_ref = input
            .provider_session_ref
            .or_else(|| session.provider_session_ref.clone())
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "session {} has no provider_session_ref to resume",
                    session_id
                ))
            })?;
        let canonical_provider_session_ref = input
            .canonical_provider_session_ref
            .or_else(|| session.canonical_provider_session_ref.clone());

        let resumed = provider
            .resume_session(ProviderResumeSessionRequest {
                runtime_session_id: session_id.to_string(),
                provider_session_ref: provider_session_ref.clone(),
                canonical_provider_session_ref: canonical_provider_session_ref.clone(),
                cwd: session.cwd.clone(),
                metadata: Some(session.metadata.clone()),
            })
            .await?;
        if resumed.runtime_session_id != session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "provider resume returned mismatched session id (expected={}, actual={})",
                session_id, resumed.runtime_session_id
            )));
        }

        let mut updated = session.clone();
        updated.provider_session_ref = Some(resumed.provider_session_ref);
        updated.canonical_provider_session_ref = resumed.canonical_provider_session_ref;
        if updated.status != "closed" {
            updated.status = "ready".to_string();
        }
        updated.updated_at = now_ms();
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated.clone());
        }
        let _ = self
            .append_event_with_mutations(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                None,
                "session.resumed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({}),
                &[RuntimeRecordMutation::Session(updated.clone())],
            )
            .await?;
        Ok(updated)
    }

    async fn finalize_session_close(
        &self,
        session_id: &str,
        reason: String,
    ) -> Result<SessionRecord, RuntimeError> {
        let session = self.get_session(session_id).await?;
        let mut updated = session.clone();
        updated.status = "closed".to_string();
        updated.closed_at = Some(now_ms());
        updated.updated_at = now_ms();
        updated.active_turn_id = None;

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated.clone());
        }
        let _ = self
            .append_event_with_mutations(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                None,
                "session.closed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "reason": reason }),
                &[RuntimeRecordMutation::Session(updated.clone())],
            )
            .await?;
        Ok(updated)
    }
}
