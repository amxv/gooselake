use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::{
    normalized_json_hash, PersistedUserInputSnapshot, ProviderCapabilitySupport,
    ProviderHardForkEditRerunRequest, ProviderKind, ProviderSession, RuntimeError,
    SessionEditRerunOperation, TurnInputProjectionSource,
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
    pub(super) fn publish_edit_transition(
        &self,
        receipt: crate::EditRerunTransitionReceipt,
    ) -> SessionEditRerunOperation {
        if let Some(event) = receipt.journal_event {
            let _ = self.event_tx.send(event);
        }
        receipt.operation
    }

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
            .map(|receipt| self.publish_edit_transition(receipt))
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
        self.fence_unresolved_operator_turn_native_effect(agent_id)?;
        let agent = self.get_active_workspace_agent_by_id(agent_id)?;
        // A verified native fork must advance this SQLite signed revision.
        // An exhausted roster revision cannot commit its child attachment;
        // fail before admitting the intent or dispatching the hard fork.
        if agent.revision >= i64::MAX as u64 {
            return Err(RuntimeError::InvalidState(
                "agent revision cannot advance beyond SQLite signed integer range".into(),
            ));
        }
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
            legacy_native_recovery_evidence: None,
            created_at_ms: at,
            updated_at_ms: at,
        };
        let receipt = self.store.begin_session_edit_rerun(&operation)?;
        let execute = receipt.changed && receipt.operation.operation_id == operation.operation_id;
        let admitted = self.publish_edit_transition(receipt);
        if !execute {
            return Ok(admitted); // Historical inspection is never preparation/fork authority.
        }
        let operation = admitted;
        let owner = crate::SessionControlCoordinate::EditRerun {
            agent_id: operation.agent_id.clone(),
            operation_id: operation.operation_id.clone(),
        };

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
            let code = error
                .provider_dispatch_code()
                .unwrap_or("edit_preparation_identity_unverified")
                .to_owned();
            let error = self.session_control_error(&owner, &code, error, None);
            let error = match self.store.advance_session_edit_rerun(
                &operation.operation_id,
                "manual_review",
                None,
                None,
                Some("edit_preparation_identity_unverified"),
                now_ms(),
            ) {
                Ok(receipt) => {
                    self.publish_edit_transition(receipt);
                    error
                }
                Err(failure) => self.session_control_quarantine_failed(error, failure),
            };
            return Err(error);
        }
        let receipt = self
            .store
            .advance_session_edit_rerun(
                &operation.operation_id,
                "native_started",
                None,
                None,
                None,
                now_ms(),
            )
            .map_err(|error| {
                self.session_control_error(&owner, "edit_receipt_unavailable", error, None)
            })?;
        if !receipt.changed {
            return Ok(self.publish_edit_transition(receipt));
        }
        self.publish_edit_transition(receipt);
        // Only this changed committed receipt reaches the existing hard_fork_edit_rerun call.
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
                let recovery = error.legacy_native_recovery();
                let error = self.session_control_error_with_recovery(
                    &owner, "native_fork_outcome_unknown",
                    RuntimeError::provider_dispatch_unknown(
                        "native_fork_outcome_unknown",
                        format!("edit {} may have forked the provider; inspect the original edit instead of retrying: {error}", operation.operation_id),
                    ), recovery, None,
                );
                let error = match self.store.advance_session_edit_rerun(
                    &operation.operation_id,
                    "manual_review",
                    None,
                    None,
                    Some("native_fork_outcome_unknown"),
                    now_ms(),
                ) {
                    Ok(receipt) => {
                        self.publish_edit_transition(receipt);
                        error
                    }
                    Err(failure) => self.session_control_quarantine_failed(error, failure),
                };
                return Err(error);
            }
        };
        let receipt = {
            let _turns = self.turns.write().await;
            let mut sessions = self.sessions.write().await;
            let receipt = self
                .store
                .advance_session_edit_rerun(
                    &operation.operation_id,
                    "fork_observed",
                    Some(&child),
                    None,
                    None,
                    now_ms(),
                )
                .map_err(|error| {
                    self.session_control_error(&owner, "edit_receipt_unavailable", error, None)
                })?;
            if !receipt.changed {
                return Ok(self.publish_edit_transition(receipt));
            }
            let committed = receipt.committed_session.as_ref().ok_or_else(|| {
                self.session_control_error(
                    &owner,
                    "edit_receipt_unavailable",
                    RuntimeError::ProtocolViolation(
                        "fork receipt lacks committed session projection".into(),
                    ),
                    None,
                )
            })?;
            if committed.id != agent_id
                || committed.provider != operation.provider.as_str()
                || committed.provider_session_ref != receipt.operation.child_provider_session_ref
                || committed.canonical_provider_session_ref
                    != receipt.operation.child_canonical_provider_session_ref
                || !sessions.contains_key(agent_id)
            {
                return Err(self.session_control_error(
                    &owner,
                    "edit_receipt_unavailable",
                    RuntimeError::ProtocolViolation(
                        "fork receipt has inconsistent child projection".into(),
                    ),
                    None,
                ));
            }
            sessions.insert(agent_id.to_string(), committed.clone());
            receipt
        };
        let persisted = self.publish_edit_transition(receipt);
        // Continue the existing replacement send; remove the postcommit append_event block.
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

        // A dispatch error may follow successful durable admission. Preserve
        // the original cause and correlate only this edit's actual admission.
        let recovery = replacement
            .as_ref()
            .err()
            .and_then(RuntimeError::legacy_native_recovery);
        let replacement_failure = replacement.as_ref().err().map(ToString::to_string);
        let correlated = self
            .latest_turn_admission_for_correlation(agent_id, &operation.operation_id)
            .await;
        let correlated = match correlated {
            Some(admission) => Some(admission),
            None => self.store.get_persisted_turn_admission_for_correlation(agent_id, &operation.operation_id)
                .map_err(|error| self.session_control_error_with_recovery(
                    &owner, "edit_receipt_unavailable",
                    RuntimeError::Bootstrap(format!("verified fork replacement lookup failed ({error}); dispatch context: {}", replacement_failure.as_deref().unwrap_or("no dispatch error"))),
                    recovery.clone(), None,
                ))?,
        };
        if let Some(admission) = correlated {
            if admission.session_id != operation.agent_id
                || admission.provider != operation.provider.as_str()
                || admission.correlation.correlation_id.as_deref()
                    != Some(operation.operation_id.as_str())
                || admission.projection_source != TurnInputProjectionSource::UserVisible
                || admission.user_input_snapshot != operation.edited_snapshot
            {
                return Err(self.session_control_error_with_recovery(
                    &owner,
                    "replacement_admission_unverified",
                    RuntimeError::ProtocolViolation(
                        "replacement admission does not belong to the original edit".into(),
                    ),
                    recovery,
                    None,
                ));
            }
            if let Some(cause) = recovery.as_ref() {
                self.persist_session_control_legacy_recovery(
                    &owner,
                    cause,
                    Some(admission.turn_id.clone()),
                )?;
            }
            let dispatch_error_code = replacement_failure
                .as_ref()
                .map(|_| "replacement_turn_dispatch_outcome_requires_inspection");
            let completion = self.store.advance_session_edit_rerun(
                &operation.operation_id,
                "completed",
                None,
                Some(&admission.turn_id),
                dispatch_error_code,
                now_ms(),
            );
            match completion {
                Ok(receipt) => return Ok(self.publish_edit_transition(receipt)),
                Err(error) => {
                    // A lost acknowledgement can follow the committed exact
                    // completion. Inspect once; never replay a native command.
                    if let Ok(Some(saved)) =
                        self.store.get_session_edit_rerun(&operation.operation_id)
                    {
                        let matching = saved.operation_id == operation.operation_id
                            && saved.agent_id == operation.agent_id
                            && saved.target_turn_id == operation.target_turn_id
                            && saved.provider == operation.provider
                            && saved.source_provider_session_ref
                                == operation.source_provider_session_ref
                            && saved.source_canonical_provider_session_ref
                                == operation.source_canonical_provider_session_ref
                            && saved.agent_revision == operation.agent_revision
                            && saved.expected_session_updated_at
                                == operation.expected_session_updated_at
                            && saved.request_hash == operation.request_hash
                            && saved.idempotency_key == operation.idempotency_key
                            && saved.edited_snapshot == operation.edited_snapshot
                            && saved.created_at_ms == operation.created_at_ms
                            && saved.child_provider_session_ref
                                == persisted.child_provider_session_ref
                            && saved.child_canonical_provider_session_ref
                                == persisted.child_canonical_provider_session_ref
                            && saved.phase == "completed"
                            && saved.replacement_turn_id.as_deref()
                                == Some(admission.turn_id.as_str())
                            && saved.error_code.as_deref() == dispatch_error_code;
                        let evidence_matches = recovery.as_ref().is_none_or(|cause| {
                            matches!(self.store.read_session_control_legacy_recovery(&owner), Ok(Some(saved)) if saved == *cause)
                        });
                        if matching && evidence_matches {
                            return Ok(saved);
                        }
                    }
                    return Err(self.session_control_error_with_recovery(
                        &owner,
                        "edit_receipt_unavailable",
                        error,
                        recovery,
                        Some(admission.turn_id),
                    ));
                }
            }
        }
        let error = self.session_control_error_with_recovery(
            &owner, "replacement_admission_unverified",
            RuntimeError::provider_dispatch_unknown(
                "replacement_admission_unverified",
                format!("verified edit fork {} has no verified durable replacement admission: {}; inspect the original edit before resending",
                    operation.operation_id, replacement_failure.as_deref().unwrap_or("unknown admission outcome")),
            ), recovery, None,
        );
        let error = match self.store.advance_session_edit_rerun(
            &operation.operation_id,
            "manual_review",
            None,
            None,
            Some("replacement_admission_unverified"),
            now_ms(),
        ) {
            Ok(receipt) => {
                self.publish_edit_transition(receipt);
                error
            }
            Err(failure) => self.session_control_quarantine_failed(error, failure),
        };
        Err(error)
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
