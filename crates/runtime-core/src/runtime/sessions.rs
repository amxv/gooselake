use serde_json::Value;

use crate::{
    ProviderAuthStatus, ProviderCloseSessionRequest, ProviderCreateSessionPolicyRequest,
    ProviderKind, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, SessionRecord,
    TurnRecord,
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
        input: CreateSessionInput,
    ) -> Result<SessionRecord, RuntimeError> {
        // Legacy /v1 still creates provider-native sessions before SQLite
        // admission. Hold the same root lock as workspace retirement across
        // that entire window; a later DB rejection must not orphan native
        // activity outside the retired workspace's obligation inventory.
        let matched_workspace = if let Some(cwd) = input.cwd.as_deref() {
            let candidate =
                std::fs::canonicalize(cwd).unwrap_or_else(|_| std::path::PathBuf::from(cwd));
            let workspaces = match self.store.list_workspaces() {
                Ok(workspaces) => workspaces,
                Err(RuntimeError::Unsupported(_)) => Vec::new(),
                Err(error) => return Err(error),
            };
            let repository_root = crate::resolve_repository_identity(&candidate)
                .ok()
                .map(|identity| identity.canonical_root);
            let managed_worktrees = if workspaces.is_empty() {
                Vec::new()
            } else {
                self.store.hydrate_runtime_state()?.managed_worktrees
            };
            workspaces
                .into_iter()
                .filter(|workspace| {
                    candidate.starts_with(&workspace.canonical_root)
                        || repository_root.as_deref() == Some(workspace.canonical_root.as_str())
                        || managed_worktrees.iter().any(|worktree| {
                            worktree.repo_root == workspace.canonical_root
                                && std::path::Path::new(&worktree.worktree_cwd).is_absolute()
                                && candidate.starts_with(&worktree.worktree_cwd)
                        })
                })
                .max_by_key(|workspace| workspace.canonical_root.len())
        } else {
            None
        };
        let _legacy_workspace_guard = if let Some(workspace) = matched_workspace {
            let lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
            let guard = lock.read_owned().await;
            let current = self
                .store
                .get_workspace(&workspace.workspace_id)?
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("workspace {}", workspace.workspace_id))
                })?;
            if current.lifecycle_state != crate::WorkspaceLifecycleState::Active {
                return Err(RuntimeError::Conflict(format!(
                    "legacy provider create rejected for inactive workspace {}",
                    workspace.workspace_id
                )));
            }
            let preflight = self
                .store
                .workspace_retirement_preflight(&workspace.workspace_id)?;
            if preflight
                .blockers
                .iter()
                .any(|blocker| blocker.code == "retirement_in_progress")
            {
                return Err(RuntimeError::Conflict(format!(
                    "legacy provider create rejected for retiring workspace {}",
                    workspace.workspace_id
                )));
            }
            Some(guard)
        } else {
            None
        };
        let provider = self.providers.get(input.provider).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(input.provider.as_str().to_string())
        })?;
        let now = now_ms();
        let session_id = self.allocate_id("sess", input.provider.as_str());
        let mut create_request = ProviderCreateSessionPolicyRequest::legacy_compatible(
            session_id.clone(),
            input.model.clone(),
            input.cwd.clone(),
            input.permission_mode.clone(),
            input.metadata.clone(),
        )?;
        if input.provider == ProviderKind::Claude && input.permission_mode.is_none() {
            create_request.launch_policy.permission_intent =
                crate::ProviderPermissionIntent::InheritProviderConfiguration;
        }
        let created = provider.create_session_with_policy(create_request).await?;

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

        if let Err(error) = self.store.upsert_session(&record) {
            if let Err(native_error) = provider
                .close_session(ProviderCloseSessionRequest {
                    runtime_session_id: session_id.clone(),
                    reason: Some("legacy_session_admission_failed".into()),
                })
                .await
            {
                tracing::warn!(session_id,error=%native_error,
                    "legacy session native cleanup could not be verified after failed durable admission");
            }
            return Err(error);
        }
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), record.clone());
        }
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id.as_str(),
                Some(session_id.as_str()),
                None,
                "session.created",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "provider": record.provider }),
            )
            .await?;
        Ok(record)
    }

    pub async fn close_session(
        &self,
        session_id: &str,
        reason: Option<String>,
    ) -> Result<SessionRecord, RuntimeError> {
        let _guard = self.session_policy_mutation_lock.lock().await;
        if let Some(operation_id) = self.store.retiring_operation_for_session(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "native close is owned by workspace retirement {operation_id}"
            )));
        }
        if self.store.unresolved_session_edit_rerun(session_id)? {
            return Err(RuntimeError::Conflict(
                "cannot close a session with unresolved native edit-and-rerun".into(),
            ));
        }
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
        let _guard = self.session_policy_mutation_lock.lock().await;
        if let Some(operation_id) = self.store.retiring_operation_for_session(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "native resume is frozen by workspace retirement {operation_id}"
            )));
        }
        if self.store.unresolved_session_edit_rerun(session_id)? {
            return Err(RuntimeError::Conflict(
                "cannot resume a session with unresolved native edit-and-rerun".into(),
            ));
        }
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

        let resume_request = self.provider_resume_request_for_session(
            &session,
            provider_session_ref.clone(),
            canonical_provider_session_ref.clone(),
        )?;
        let resumed = provider.resume_session_with_policy(resume_request).await?;
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
        self.store.upsert_session(&updated)?;
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated.clone());
        }
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                None,
                "session.resumed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({}),
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
        self.store.upsert_session(&updated)?;

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated.clone());
        }
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                None,
                "session.closed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "reason": reason }),
            )
            .await?;
        Ok(updated)
    }
}
