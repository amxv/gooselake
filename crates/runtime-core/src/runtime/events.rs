use serde_json::Value;

use crate::{
    NewRuntimeEvent, ProviderRuntimeEvent, RuntimeError, RuntimeEventCriticality,
    RuntimeEventRecord, RuntimeEventScope,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    pub(super) async fn verify_provider_session_identity(
        &self,
        provider: &dyn crate::RuntimeProvider,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let Some(identity) = provider.observe_session_identity(session_id).await? else {
            return Ok(());
        };
        if identity.runtime_session_id != session_id {
            return Err(RuntimeError::ProtocolViolation(
                "provider returned another session's native identity".into(),
            ));
        }
        let canonical = identity.canonical_provider_session_ref.ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "provider did not establish a canonical native session identity".into(),
            )
        })?;
        self.record_provider_session_identity(
            session_id,
            turn_id,
            identity.provider_session_ref,
            canonical,
        )
        .await
    }

    pub(super) async fn record_provider_side_event(
        &self,
        session_id: &str,
        turn_id: &str,
        event: ProviderRuntimeEvent,
    ) -> Result<Option<ProviderRuntimeEvent>, RuntimeError> {
        match event {
            ProviderRuntimeEvent::SessionIdentityObserved {
                runtime_session_id,
                turn_id: event_turn_id,
                provider_session_ref,
                canonical_provider_session_ref,
            } if runtime_session_id == session_id && event_turn_id == turn_id => {
                if let Err(error) = self
                    .record_provider_session_identity(
                        session_id,
                        turn_id,
                        provider_session_ref,
                        canonical_provider_session_ref,
                    )
                    .await
                {
                    let _ = self
                        .mark_provider_event_stream_unknown(
                            session_id,
                            turn_id,
                            "provider_session_identity_persistence_failed",
                            error.to_string(),
                        )
                        .await;
                    return Err(error);
                }
                Ok(None)
            }
            ProviderRuntimeEvent::ApprovalRequested {
                runtime_session_id,
                turn_id: event_turn_id,
                provider_approval_ref,
                tool_call_id,
                request,
            } if runtime_session_id == session_id && event_turn_id == turn_id => {
                if let Err(error) = self
                    .record_provider_approval(
                        session_id,
                        turn_id,
                        provider_approval_ref.as_str(),
                        tool_call_id,
                        request,
                    )
                    .await
                {
                    let _ = self
                        .mark_provider_event_stream_unknown(
                            session_id,
                            turn_id,
                            "provider_approval_persistence_failed",
                            error.to_string(),
                        )
                        .await;
                    return Err(error);
                }
                Ok(None)
            }
            ProviderRuntimeEvent::PermissionObserved {
                runtime_session_id,
                turn_id: event_turn_id,
                permission_mode,
                resolved_turn_selection,
            } if runtime_session_id == session_id && event_turn_id == turn_id => {
                self.record_provider_permission_observed(
                    session_id,
                    turn_id,
                    permission_mode,
                    resolved_turn_selection,
                )
                .await?;
                Ok(None)
            }
            ProviderRuntimeEvent::ContextCompactionObserved {
                runtime_session_id,
                turn_id: Some(event_turn_id),
                phase,
                trigger,
                pre_tokens,
                post_tokens,
                context_window_size,
            } if runtime_session_id == session_id && event_turn_id == turn_id => {
                self.record_provider_context_compaction_observed(
                    session_id,
                    turn_id,
                    phase,
                    trigger,
                    pre_tokens,
                    post_tokens,
                    context_window_size,
                )
                .await?;
                Ok(None)
            }
            event => Ok(Some(event)),
        }
    }

    pub(super) async fn append_event(
        &self,
        scope: RuntimeEventScope,
        scope_id: &str,
        session_id: Option<&str>,
        turn_id: Option<&str>,
        kind: &str,
        criticality: RuntimeEventCriticality,
        payload: Value,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        let event = NewRuntimeEvent {
            event_id: self.allocate_id("evt", scope.as_str()),
            scope,
            scope_id: scope_id.to_string(),
            session_id: session_id.map(str::to_string),
            team_id: None,
            turn_id: turn_id.map(str::to_string),
            kind: kind.to_string(),
            criticality,
            payload,
            provider: None,
            provider_seq: None,
            created_at: now_ms(),
        };
        let record = self.store.append_runtime_event(&event)?;
        let _ = self.event_tx.send(record.clone());
        Ok(record)
    }

    pub(super) async fn record_provider_permission_observed(
        &self,
        session_id: &str,
        turn_id: &str,
        permission_mode: String,
        resolved_turn_selection: Option<String>,
    ) -> Result<(), RuntimeError> {
        if let Err(error) = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id),
                "provider.permission_observed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "permission_mode": permission_mode,
                    "resolved_turn_selection": resolved_turn_selection,
                }),
            )
            .await
        {
            let _ = self
                .mark_provider_event_stream_unknown(
                    session_id,
                    turn_id,
                    "provider_permission_observation_persistence_failed",
                    error.to_string(),
                )
                .await;
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn record_provider_session_identity(
        &self,
        session_id: &str,
        turn_id: &str,
        provider_session_ref: String,
        canonical_provider_session_ref: String,
    ) -> Result<(), RuntimeError> {
        if provider_session_ref.trim().is_empty()
            || canonical_provider_session_ref.trim().is_empty()
        {
            return Err(RuntimeError::ProtocolViolation(
                "provider observed an empty native session identity".to_string(),
            ));
        }
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
        // Late notifications from an earlier turn must not overwrite the
        // identity of an already-resumed or rebound provider attachment.
        if session.active_turn_id.as_deref() != Some(turn_id) {
            return Ok(());
        }
        if session.provider_session_ref.as_deref() == Some(provider_session_ref.as_str())
            && session.canonical_provider_session_ref.as_deref()
                == Some(canonical_provider_session_ref.as_str())
        {
            return Ok(());
        }
        let mut updated = session.clone();
        updated.provider_session_ref = Some(provider_session_ref);
        updated.canonical_provider_session_ref = Some(canonical_provider_session_ref);
        updated.updated_at = now_ms();
        self.store.upsert_session(&updated)?;
        *session = updated;
        Ok(())
    }

    pub(super) async fn record_provider_context_compaction_observed(
        &self,
        session_id: &str,
        turn_id: &str,
        phase: String,
        trigger: Option<String>,
        pre_tokens: Option<u64>,
        post_tokens: Option<u64>,
        context_window_size: Option<u64>,
    ) -> Result<(), RuntimeError> {
        if let Err(error) = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id),
                "provider.context_compaction_observed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "phase": phase,
                    "trigger": trigger,
                    "pre_tokens": pre_tokens,
                    "post_tokens": post_tokens,
                    "context_window_size": context_window_size,
                }),
            )
            .await
        {
            let _ = self
                .mark_provider_event_stream_unknown(
                    session_id,
                    turn_id,
                    "provider_context_compaction_observation_persistence_failed",
                    error.to_string(),
                )
                .await;
            return Err(error);
        }
        Ok(())
    }
}
