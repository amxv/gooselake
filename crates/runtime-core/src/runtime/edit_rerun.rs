use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::{
    normalized_json_hash, PersistedUserInputSnapshot, ProviderCapabilitySupport,
    ProviderHardForkEditRerunRequest, ProviderKind, ProviderSession, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionEditRerunOperation,
    TurnInputProjectionSource,
};

use super::helpers::now_ms;
use super::{RuntimeSessionManager, SendTurnInput};

/// Rebuild native structured input from the durable editable snapshot. Never
/// silently flatten images or invocation metadata into user-visible text.
pub(super) fn edited_input_from_snapshot(
    provider: ProviderKind,
    snapshot: &PersistedUserInputSnapshot,
) -> Result<Vec<Value>, RuntimeError> {
    let mut items = Vec::new();
    if !snapshot.prompt_text.is_empty() {
        items.push(json!({"type":"text","text":snapshot.prompt_text}));
    }
    for image in &snapshot.image_refs {
        let path = image.persisted_absolute_path.as_deref().ok_or_else(|| {
            RuntimeError::InvalidState("edited image has no durable absolute path".into())
        })?;
        if !Path::new(path).is_absolute() || !Path::new(path).is_file() {
            return Err(RuntimeError::NotFound(format!(
                "persisted edit image attachment is missing or not absolute: {path}"
            )));
        }
        if !image.media_type.starts_with("image/") {
            return Err(RuntimeError::InvalidState(
                "edited image has invalid media type".into(),
            ));
        }
        items.push(json!({"type":"local_image","path":path,"media_type":image.media_type}));
    }
    if let Some(skill) = snapshot.invocation.as_ref() {
        if provider != ProviderKind::Codex || skill.provider != "codex" {
            return Err(RuntimeError::Unsupported(
                "edited invocation has no matching provider-native skill capability".into(),
            ));
        }
        let path = skill.path.as_deref().ok_or_else(|| {
            RuntimeError::InvalidState("native edited skill has no persisted path".into())
        })?;
        if skill.name.trim().is_empty() || !Path::new(path).is_file() {
            return Err(RuntimeError::NotFound(format!(
                "persisted native skill is missing: {path}"
            )));
        }
        items.push(json!({"type":"skill","name":skill.name,"path":path}));
    }
    if items.is_empty() {
        return Err(RuntimeError::InvalidState(
            "edited turn must contain text, images or a native skill".into(),
        ));
    }
    Ok(items)
}

impl RuntimeSessionManager {
    pub fn session_edit_rerun_operation(
        &self,
        agent_id: &str,
        operation_id: &str,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        self.store
            .get_workspace_agent_by_id(agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        let op = self
            .store
            .get_session_edit_rerun(operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("edit-and-rerun {operation_id}")))?;
        if op.agent_id != agent_id {
            return Err(RuntimeError::NotFound(format!(
                "edit-and-rerun {operation_id}"
            )));
        }
        Ok(op)
    }

    pub fn resolve_edit_rerun_review(
        &self,
        agent_id: &str,
        operation_id: &str,
        evidence: &str,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        self.session_edit_rerun_operation(agent_id, operation_id)?;
        self.store
            .resolve_session_edit_rerun_review(operation_id, evidence, now_ms())
    }

    /// A verified provider fork is a distinct durable effect. A replacement
    /// logical turn is admitted ONLY after the child identity is committed.
    /// If any native outcome is uncertain, the edit is permanently fenced
    /// until explicit operator reconciliation; it is never blindly replayed.
    pub async fn edit_workspace_agent_turn(
        self: &Arc<Self>,
        agent_id: &str,
        target_turn_id: &str,
        edited_snapshot: PersistedUserInputSnapshot,
        idempotency_key: Option<String>,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        let guard = self.session_policy_mutation_lock.lock().await;
        let target_turn_id = target_turn_id.trim();
        if target_turn_id.is_empty() {
            return Err(RuntimeError::InvalidState(
                "missing historical turn id".into(),
            ));
        }
        let request_hash = normalized_json_hash(&json!({
            "agent_id":agent_id,"target_turn_id":target_turn_id,
            "edited_snapshot":edited_snapshot
        }))?;
        if let Some(key) = idempotency_key.as_deref() {
            if key.trim().is_empty() || key.trim() != key || key.len() > 128 {
                return Err(RuntimeError::InvalidState(
                    "Idempotency-Key must be 1-128 trimmed characters".into(),
                ));
            }
            if let Some(previous) = self.store.get_session_edit_rerun_by_key(agent_id, key)? {
                if previous.request_hash != request_hash {
                    return Err(RuntimeError::Conflict(
                        "Idempotency-Key reused with different edit input".into(),
                    ));
                }
                return Ok(previous);
            }
        }
        let agent = self.get_active_workspace_agent_by_id(agent_id)?;
        let session = self.get_session(agent_id).await?;
        if session.status != "ready" || session.active_turn_id.is_some() {
            return Err(RuntimeError::Conflict(
                "edit-and-rerun requires a ready idle agent".into(),
            ));
        }
        if self.store.unresolved_workspace_agent_rebind(agent_id)?
            || self.store.unresolved_session_compaction(agent_id)?
            || self.store.unresolved_session_edit_rerun(agent_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has an unresolved provider operation".into(),
            ));
        }
        let provider = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation("invalid stored edit provider".into())
        })?;
        if provider != agent.recreation_policy.provider {
            return Err(RuntimeError::ProtocolViolation(
                "edit provider authority differs from recreation policy".into(),
            ));
        }
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(session.provider.clone()))?;
        if adapter.capabilities().hard_fork_edit_rerun != ProviderCapabilitySupport::Supported {
            return Err(RuntimeError::Unsupported(format!(
                "provider {} does not support hard-fork edit-and-rerun",
                session.provider
            )));
        }
        if !edited_snapshot.image_refs.is_empty()
            && adapter.capabilities().images != ProviderCapabilitySupport::Supported
        {
            return Err(RuntimeError::Unsupported(
                "provider cannot rerun native images".into(),
            ));
        }
        let edited_input = edited_input_from_snapshot(provider, &edited_snapshot)?;
        let historical = self
            .turn_admissions
            .read()
            .await
            .get(target_turn_id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("persisted historical turn {target_turn_id}"))
            })?;
        let old_turn = self
            .turns
            .read()
            .await
            .get(target_turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("historical turn {target_turn_id}")))?;
        if historical.session_id != agent_id
            || historical.projection_source != TurnInputProjectionSource::UserVisible
            || historical.provider_native_turn_id.is_none()
            || old_turn.status != "completed"
        {
            return Err(RuntimeError::Conflict(
                "edit target must be a completed user-visible turn with persistent native identity"
                    .into(),
            ));
        }
        if historical.user_input_snapshot.prompt_text.is_empty()
            && historical.user_input_snapshot.image_refs.is_empty()
            && historical.user_input_snapshot.invocation.is_none()
        {
            return Err(RuntimeError::Conflict(
                "historical edit target is missing its durable user-input snapshot".into(),
            ));
        }
        let source_ref = session.provider_session_ref.clone().ok_or_else(|| {
            RuntimeError::InvalidState("hard fork requires current native provider session".into())
        })?;
        let at = now_ms();
        let operation = SessionEditRerunOperation {
            operation_id: self.allocate_id("edit", provider.as_str()),
            agent_id: agent_id.into(),
            target_turn_id: target_turn_id.into(),
            provider,
            source_provider_session_ref: source_ref.clone(),
            source_canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            agent_revision: agent.revision,
            expected_session_updated_at: session.updated_at,
            request_hash,
            idempotency_key,
            edited_snapshot: edited_snapshot.clone(),
            phase: "intended".into(),
            child_provider_session_ref: None,
            child_canonical_provider_session_ref: None,
            replacement_turn_id: None,
            error_code: None,
            review_evidence: None,
            created_at_ms: at,
            updated_at_ms: at,
        };
        let admitted = self.store.begin_session_edit_rerun(&operation)?;
        if admitted.operation_id != operation.operation_id {
            // Another manager may have raced the same idempotent request.
            return Ok(admitted);
        }

        // Ensure the real adapter has the exact persisted native attachment
        // and historical mapping before the irreversible fork intent.
        let prepare = async {
            if let Some(current) = adapter.observe_session_identity(agent_id).await? {
                if current.runtime_session_id != agent_id
                    || current.provider_session_ref != source_ref
                    || current.canonical_provider_session_ref
                        != session.canonical_provider_session_ref
                {
                    return Err(RuntimeError::ProtocolViolation(
                        "edit provider attachment differs from durable session".into(),
                    ));
                }
            }
            let native = historical
                .provider_native_turn_id
                .as_deref()
                .expect("validated");
            match adapter
                .restore_turn_identity_mapping(agent_id, target_turn_id, native)
                .await
            {
                Ok(()) => Ok(()),
                Err(RuntimeError::NotFound(_)) => {
                    // Resume only an unattached provider. A blind resume of
                    // an already active Codex adapter would destroy its live
                    // turn mapping and transport.
                    let resume = self.provider_resume_request_for_session(
                        &session,
                        source_ref.clone(),
                        session.canonical_provider_session_ref.clone(),
                    )?;
                    let observed = adapter.resume_session_with_policy(resume).await?;
                    if observed.runtime_session_id != agent_id
                        || observed.provider_session_ref != source_ref
                        || observed.canonical_provider_session_ref
                            != session.canonical_provider_session_ref
                    {
                        return Err(RuntimeError::ProtocolViolation(
                            "edit resume changed provider identity".into(),
                        ));
                    }
                    adapter
                        .restore_turn_identity_mapping(agent_id, target_turn_id, native)
                        .await
                }
                Err(other) => Err(other),
            }
        }
        .await;
        if let Err(error) = prepare {
            // No hard fork was requested, but provider resume itself can
            // have changed external state. Keep it fenced for reconciliation.
            let _ = self.store.advance_session_edit_rerun(
                &operation.operation_id,
                "manual_review",
                None,
                None,
                Some("edit_preparation_identity_unverified"),
                now_ms(),
            );
            return Err(error);
        }
        self.store.advance_session_edit_rerun(
            &operation.operation_id,
            "native_started",
            None,
            None,
            None,
            now_ms(),
        )?;
        let child: ProviderSession = match adapter
            .hard_fork_edit_rerun(ProviderHardForkEditRerunRequest {
                runtime_session_id: agent_id.into(),
                target_turn_id: target_turn_id.into(),
                edited_input: edited_input.clone(),
            })
            .await
        {
            Ok(child) => child,
            Err(error) => {
                let _ = self.store.advance_session_edit_rerun(
                    &operation.operation_id,
                    "manual_review",
                    None,
                    None,
                    Some("native_fork_outcome_unknown"),
                    now_ms(),
                );
                return Err(RuntimeError::provider_dispatch_unknown(
                    "native_fork_outcome_unknown",
                    format!("edit {} may have forked the provider; inspect operation instead of retrying: {error}",operation.operation_id)
                ));
            }
        };
        // Atomic SQLite session child identity + supersession ledger + edit
        // transition. Failure keeps the native effect quarantined.
        let persisted = self.store.advance_session_edit_rerun(
            &operation.operation_id,
            "fork_observed",
            Some(&child),
            None,
            None,
            now_ms(),
        )?;
        let committed_agent = self
            .store
            .get_workspace_agent_by_id(agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        {
            let mut sessions = self.sessions.write().await;
            let current = sessions
                .get_mut(agent_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("session {agent_id}")))?;
            current.provider_session_ref = Some(child.provider_session_ref);
            current.canonical_provider_session_ref = child.canonical_provider_session_ref;
            // The durable fork rebuilt the active transcript against the
            // retained provider prefix. Keep the in-memory projection equal
            // before the replacement turn can append its new messages.
            current.metadata = committed_agent.metadata;
            current.updated_at = persisted
                .updated_at_ms
                .max(session.updated_at.saturating_add(1));
        }
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                agent_id,
                Some(agent_id),
                Some(target_turn_id),
                "turn.edit_fork_observed",
                RuntimeEventCriticality::Critical,
                json!({"operation_id":operation.operation_id}),
            )
            .await;
        let replacement = self
            .send_turn_under_policy_guard(
                agent_id,
                SendTurnInput {
                    input: edited_input,
                    expected_turn_id: None,
                    permission_mode: None,
                    projection_source: Some(TurnInputProjectionSource::UserVisible),
                    user_input_snapshot: Some(edited_snapshot),
                    correlation_id: Some(operation.operation_id.clone()),
                    // Current persisted thinking preference remains authoritative.
                    // Only Codex collaboration mode is historically turn scoped.
                    turn_settings: crate::ProviderTurnSettings {
                        thinking_effort: None,
                        codex_collaboration_mode: historical
                            .dispatch_policy
                            .turn_settings
                            .codex_collaboration_mode,
                    },
                },
                Some(&operation.operation_id),
                None,
                None,
                guard,
            )
            .await;

        // A dispatch error may follow successful durable admission; detect
        // that exact operation correlation before declaring an unknown loss.
        let correlated = self
            .latest_turn_admission_for_correlation(agent_id, &operation.operation_id)
            .await;
        let correlated = match correlated {
            Some(admission) => Some(admission),
            None => self
                .store
                .list_turn_admissions()?
                .into_iter()
                .find(|admission| {
                    admission.session_id == agent_id
                        && admission.correlation.correlation_id.as_deref()
                            == Some(operation.operation_id.as_str())
                }),
        };
        if let Some(admission) = correlated {
            let dispatch_error_code = replacement
                .as_ref()
                .err()
                .map(|_| "replacement_turn_dispatch_outcome_requires_inspection");
            return self.store.advance_session_edit_rerun(
                &operation.operation_id,
                "completed",
                None,
                Some(&admission.turn_id),
                dispatch_error_code,
                now_ms(),
            );
        }
        let _ = self.store.advance_session_edit_rerun(
            &operation.operation_id,
            "manual_review",
            None,
            None,
            Some("replacement_admission_unverified"),
            now_ms(),
        );
        Err(RuntimeError::provider_dispatch_unknown(
            "replacement_admission_unverified",
            format!("verified edit fork {} has no durable replacement admission: {}; inspect operation before resending",
                operation.operation_id,
                replacement.err().map_or_else(||"unknown admission outcome".into(),|e|e.to_string())
            )
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PersistedUserInputSnapshotImageRef, PersistedUserInputSnapshotInvocation};

    #[test]
    fn edited_snapshot_reconstructs_native_ordered_images_and_skill() {
        let temp = tempfile::tempdir().expect("tempdir");
        let first = temp.path().join("a.png");
        let second = temp.path().join("b.png");
        let skill = temp.path().join("SKILL.md");
        std::fs::write(&first, b"image-a").unwrap();
        std::fs::write(&second, b"image-b").unwrap();
        std::fs::write(&skill, b"skill-b").unwrap();
        let snapshot = PersistedUserInputSnapshot {
            prompt_text: "edited text".into(),
            image_refs: vec![first.clone(), second.clone()]
                .into_iter()
                .map(|path| PersistedUserInputSnapshotImageRef {
                    media_type: "image/png".into(),
                    persisted_absolute_path: Some(path.to_string_lossy().to_string()),
                    persisted_display_path: Some("image.png".into()),
                })
                .collect(),
            invocation: Some(PersistedUserInputSnapshotInvocation {
                provider: "codex".into(),
                name: "diagnose".into(),
                display_name: Some("Diagnose".into()),
                path: Some(skill.to_string_lossy().to_string()),
            }),
        };
        let items = edited_input_from_snapshot(ProviderKind::Codex, &snapshot).unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0]["text"], "edited text");
        assert_eq!(items[1]["path"], first.to_string_lossy().as_ref());
        assert_eq!(items[2]["path"], second.to_string_lossy().as_ref());
        assert_eq!(items[3]["type"], "skill");
        assert_eq!(items[3]["name"], "diagnose");
        assert!(matches!(
            edited_input_from_snapshot(ProviderKind::Claude, &snapshot),
            Err(RuntimeError::Unsupported(_))
        ));
        std::fs::remove_file(second).unwrap();
        assert!(matches!(
            edited_input_from_snapshot(ProviderKind::Codex, &snapshot),
            Err(RuntimeError::NotFound(_))
        ));
    }
}
