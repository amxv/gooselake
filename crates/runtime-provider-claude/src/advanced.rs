use crate::{bridge::send_bridge_request, provider::ClaudeSessionHandle, ClaudeProvider};
use runtime_core::*;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub(crate) async fn ensure_idle(session: &Arc<ClaudeSessionHandle>) -> Result<(), RuntimeError> {
    if session.quarantined.load(Ordering::SeqCst) {
        return Err(RuntimeError::InvalidState(
            "Claude session is quarantined pending provider recovery".into(),
        ));
    }
    if session.active_turn_id.read().await.is_some()
        || session.pending_runtime_turn_id.read().await.is_some()
    {
        return Err(RuntimeError::InvalidState("Claude session is busy".into()));
    }
    Ok(())
}

pub(crate) fn context_from_usage(usage: &Value) -> Option<ProviderContextLimitObservation> {
    fn read_u64(usage: &Value, keys: &[&str]) -> Option<u64> {
        keys.iter().find_map(|key| {
            usage.get(*key).and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
            })
        })
    }

    let window = read_u64(
        usage,
        &[
            "contextWindowSize",
            "context_window_size",
            "contextWindow",
            "context_window",
        ],
    )
    .filter(|window| *window > 0)?;
    let total = read_u64(usage, &["inputTokens", "input_tokens"])?
        .saturating_add(read_u64(usage, &["outputTokens", "output_tokens"]).unwrap_or(0))
        .saturating_add(
            read_u64(
                usage,
                &["cacheCreationInputTokens", "cache_creation_input_tokens"],
            )
            .unwrap_or(0),
        )
        .saturating_add(
            read_u64(usage, &["cacheReadInputTokens", "cache_read_input_tokens"]).unwrap_or(0),
        );
    Some(ProviderContextLimitObservation {
        model_context_window: window,
        last_total_tokens: total,
        remaining_percentage: ((u128::from(window.saturating_sub(total)) * 100)
            / u128::from(window)) as u8,
    })
}

pub(crate) async fn prune_hard_fork_turns(
    session: &Arc<ClaudeSessionHandle>,
    rolled_back_bridge_turn_ids: &[String],
) {
    let removed = {
        let mut runtime_turn_by_bridge_turn = session.runtime_turn_by_bridge_turn.lock().await;
        rolled_back_bridge_turn_ids
            .iter()
            .filter_map(|bridge_turn_id| {
                runtime_turn_by_bridge_turn
                    .remove(bridge_turn_id)
                    .map(|runtime_turn_id| (runtime_turn_id, bridge_turn_id.clone()))
            })
            .collect::<Vec<_>>()
    };

    if !removed.is_empty() {
        let mut bridge_turn_by_runtime_turn = session.bridge_turn_by_runtime_turn.lock().await;
        for (runtime_turn_id, _) in &removed {
            bridge_turn_by_runtime_turn.remove(runtime_turn_id);
        }
        drop(bridge_turn_by_runtime_turn);

        let mut completed_turns = session.completed_turns.lock().await;
        for (runtime_turn_id, _) in &removed {
            completed_turns.remove(runtime_turn_id);
        }
    }
    *session.context_observation.write().await = None;
}

impl ClaudeProvider {
    pub async fn hard_fork_at_boundary(
        &self,
        runtime_session_id: &str,
        rollback_boundary_id: &str,
    ) -> Result<ProviderSession, RuntimeError> {
        let session = self.get_session(runtime_session_id).await?;
        let _operation = session.operation_lock.try_lock().map_err(|_| {
            RuntimeError::InvalidState("Claude hard fork cannot start while session is busy".into())
        })?;
        ensure_idle(&session).await?;
        if rollback_boundary_id.trim().is_empty()
            || session
                .canonical_provider_session_ref
                .read()
                .await
                .is_none()
        {
            return Err(RuntimeError::InvalidState(
                "hard fork requires a boundary and canonical native identity".into(),
            ));
        }
        let result: Result<ProviderSession, RuntimeError> = async {
            let response = send_bridge_request(
                &self.inner,
                &session.bridge,
                "session.hard_fork",
                json!({"sessionId": session.bridge_session_id, "rollbackBoundaryId": rollback_boundary_id}),
                self.inner.config.request_timeout_ms,
            ).await?;
            let provider = required_string(&response, "childProviderSessionRef")?;
            let canonical = required_string(&response, "childClaudeCanonicalSessionRef")?;
            let rolled_back_bridge_turn_ids = response
                .get("rolledBackTurnIds")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation(
                        "Claude hard-fork response missing rolledBackTurnIds".into(),
                    )
                })?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                        .ok_or_else(|| {
                            RuntimeError::ProtocolViolation(
                                "Claude hard-fork response contains an invalid rolled-back turn id"
                                    .into(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?;
            if rolled_back_bridge_turn_ids.is_empty() {
                return Err(RuntimeError::ProtocolViolation(
                    "Claude hard-fork response did not identify any rolled-back turns".into(),
                ));
            }
            *session.provider_session_ref.write().await = provider.into();
            *session.canonical_provider_session_ref.write().await = Some(canonical.into());
            prune_hard_fork_turns(&session, rolled_back_bridge_turn_ids.as_slice()).await;
            Ok(ProviderSession {
                runtime_session_id: runtime_session_id.into(),
                provider_session_ref: provider.into(),
                canonical_provider_session_ref: Some(canonical.into()),
            })
        }.await;
        match result {
            Ok(evidence) => Ok(evidence),
            Err(error) => {
                // A failed RPC or malformed response may follow an SDK-side
                // fork. The old provider/native identity is then untrustworthy.
                session.quarantined.store(true, Ordering::SeqCst);
                self.remove_session(runtime_session_id).await;
                let _ = send_bridge_request(
                    &self.inner,
                    &session.bridge,
                    "session.close",
                    json!({"sessionId": session.bridge_session_id, "reason": "unverified_hard_fork"}),
                    self.inner.config.request_timeout_ms,
                )
                .await;
                self.shutdown_bridges_if_idle().await;
                Err(RuntimeError::InvalidState(format!(
                    "Claude hard-fork outcome requires recovery before another turn: {error}"
                )))
            }
        }
    }

    pub(crate) async fn claude_mutate_permission(
        &self,
        req: ProviderPermissionMutationRequest,
    ) -> Result<ProviderPermissionMutationResult, RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let _operation = session.operation_lock.try_lock().map_err(|_| {
            RuntimeError::InvalidState(format!(
                "Claude permission selection cannot change while session {} is busy",
                req.runtime_session_id
            ))
        })?;
        ensure_idle(&session).await?;

        let current_revision = *session.permission_revision.read().await;
        if let Some(expected_revision) = req.expected_revision {
            if expected_revision != current_revision {
                return Err(RuntimeError::InvalidState(format!(
                    "Claude permission revision conflict: expected {expected_revision}, current {current_revision}"
                )));
            }
        }

        match &req.permission_intent {
            ProviderPermissionIntent::InheritProviderConfiguration => {}
            ProviderPermissionIntent::Explicit { mode } if mode == "dontAsk" => {
                return Err(RuntimeError::InvalidState(
                    "Claude dontAsk is not exposed as a mutable permission selection".to_string(),
                ));
            }
            ProviderPermissionIntent::Explicit { .. } => {}
            ProviderPermissionIntent::ProviderDefault => {
                return Err(RuntimeError::InvalidState(
                    "Claude permission mutation requires inherit_provider_configuration or an explicit mode".to_string(),
                ));
            }
        }

        let mut updated_policy = session.launch_policy.read().await.clone();
        updated_policy.permission_intent = req.permission_intent.clone();
        let current_preferences = session.current_preferences.read().await.clone();
        crate::policy::validate_policy(
            &updated_policy,
            session.effective_cwd.read().await.as_deref(),
            &current_preferences,
            session.model.as_deref(),
        )?;

        let next_revision = current_revision.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidState("Claude permission revision overflow".to_string())
        })?;
        *session.launch_policy.write().await = updated_policy;
        *session.permission_revision.write().await = next_revision;

        Ok(ProviderPermissionMutationResult {
            revision: next_revision,
            permission_intent: req.permission_intent,
        })
    }

    pub(crate) async fn claude_mutate_preferences(
        &self,
        req: runtime_core::ProviderSessionPreferencesMutationRequest,
    ) -> Result<runtime_core::ProviderSessionPreferencesMutationResult, RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let _operation = session.operation_lock.try_lock().map_err(|_| {
            RuntimeError::InvalidState(format!(
                "Claude session preferences cannot change while session {} is busy",
                req.runtime_session_id
            ))
        })?;
        ensure_idle(&session).await?;
        let current_revision = *session.preferences_revision.read().await;
        if let Some(expected_revision) = req.expected_revision {
            if expected_revision != current_revision {
                return Err(RuntimeError::InvalidState(format!(
                    "Claude session preference revision conflict: expected {expected_revision}, current {current_revision}"
                )));
            }
        }
        let launch_policy = session.launch_policy.read().await.clone();
        crate::policy::validate_policy(
            &launch_policy,
            session.effective_cwd.read().await.as_deref(),
            &req.current_preferences,
            session.model.as_deref(),
        )?;
        let next_revision = current_revision.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidState("Claude session preference revision overflow".to_string())
        })?;
        *session.current_preferences.write().await = req.current_preferences.clone();
        *session.preferences_revision.write().await = next_revision;
        Ok(runtime_core::ProviderSessionPreferencesMutationResult {
            revision: next_revision,
            current_preferences: req.current_preferences,
        })
    }

    pub(crate) async fn claude_context(
        &self,
        id: &str,
    ) -> Result<ProviderContextLimitObservation, RuntimeError> {
        self.get_session(id)
            .await?
            .context_observation
            .read()
            .await
            .clone()
            .ok_or_else(|| {
                RuntimeError::InvalidState("Claude context usage has not been observed".into())
            })
    }

    pub(crate) async fn claude_skills(
        &self,
        req: ProviderSkillDiscoveryRequest,
    ) -> Result<Vec<ProviderSkillDescriptor>, RuntimeError> {
        req.setting_sources_intent
            .resolved_wire_values(req.cwd.as_deref())?;
        self.ensure_provider_enabled().await?;
        let bridge = self.acquire_bridge_for_new_session().await?;
        let discovery_id = format!(
            "claude-skills-discovery-{}",
            self.inner
                .next_request_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        );
        let mut params = json!({"cwd": req.cwd, "settingSourcesIntent": req.setting_sources_intent, "forceRefresh": req.force_refresh});
        if self.inner.config.gg_mcp.enabled {
            params["ggMcpServer"] = self.build_gg_mcp_server_session_config(&discovery_id);
        }
        let response = match send_bridge_request(
            &self.inner,
            &bridge,
            "session.supported_commands",
            params.clone(),
            self.inner.config.request_timeout_ms,
        )
        .await
        {
            Err(error)
                if !self.inner.config.gg_mcp.enabled
                    && crate::auth::is_missing_gg_mcp_server_bad_request(&error) =>
            {
                params["ggMcpServer"] = self.build_gg_mcp_server_session_config(&discovery_id);
                send_bridge_request(
                    &self.inner,
                    &bridge,
                    "session.supported_commands",
                    params,
                    self.inner.config.request_timeout_ms,
                )
                .await?
            }
            result => result?,
        };
        let commands = response
            .get("commands")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation("supported_commands missing commands".into())
            })?;
        let mut seen = std::collections::BTreeSet::new();
        Ok(commands
            .iter()
            .filter_map(|c| {
                let name = c.get("name")?.as_str()?.trim();
                if name.is_empty() || !seen.insert(name.to_string()) {
                    return None;
                }
                Some(ProviderSkillDescriptor {
                    provider: ProviderKind::Claude,
                    name: name.into(),
                    description: c
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or("No description provided.")
                        .into(),
                    argument_hint: c
                        .get("argumentHint")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string),
                    display_name: None,
                    path: None,
                })
            })
            .collect())
    }

    pub(crate) async fn claude_rebind(
        &self,
        req: ProviderWorkspaceRebindRequest,
    ) -> Result<ProviderWorkspaceRebindEvidence, RuntimeError> {
        let session = self.get_session(&req.runtime_session_id).await?;
        let _operation = session.operation_lock.try_lock().map_err(|_| {
            RuntimeError::InvalidState(
                "Claude workspace rebind cannot start while session is busy".into(),
            )
        })?;
        ensure_idle(&session).await?;
        let result: Result<ProviderWorkspaceRebindEvidence, RuntimeError> = async {
            let response = send_bridge_request(
                &self.inner,
                &session.bridge,
                "session.rebind",
                json!({"sessionId": session.bridge_session_id, "destinationCwd": req.cwd}),
                self.inner.config.request_timeout_ms,
            )
            .await?;
            let cwd = required_string(&response, "effectiveCwd")?;
            let canonical =
                |s: &str| std::fs::canonicalize(s).unwrap_or_else(|_| std::path::PathBuf::from(s));
            if canonical(cwd) != canonical(&req.cwd) {
                return Err(RuntimeError::ProtocolViolation(
                    "rebind cwd mismatch".into(),
                ));
            }
            let generation = response
                .get("bindingGeneration")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation("rebind missing bindingGeneration".into())
                })?;
            if generation <= *session.binding_generation.read().await {
                return Err(RuntimeError::ProtocolViolation(
                    "rebind binding generation did not advance".into(),
                ));
            }
            let provider = required_string(&response, "providerSessionRef")?;
            let native = response
                .get("claudeCanonicalSessionRef")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string);
            if session
                .canonical_provider_session_ref
                .read()
                .await
                .is_some()
                && native.is_none()
            {
                return Err(RuntimeError::ProtocolViolation(
                    "rebind lost canonical identity".into(),
                ));
            }
            *session.effective_cwd.write().await = Some(cwd.into());
            *session.binding_generation.write().await = generation;
            *session.provider_session_ref.write().await = provider.into();
            *session.canonical_provider_session_ref.write().await = native.clone();
            Ok(ProviderWorkspaceRebindEvidence {
                runtime_session_id: req.runtime_session_id.clone(),
                cwd: cwd.into(),
                provider_session_ref: Some(provider.into()),
                canonical_provider_session_ref: native,
                binding_generation: Some(generation),
            })
        }
        .await;
        match result {
            Ok(evidence) => Ok(evidence),
            Err(error) => {
                // The bridge could already have rebound its SDK session. Never
                // dispatch another turn against unverified effective cwd,
                // including sends already queued on the detached handle.
                session.quarantined.store(true, Ordering::SeqCst);
                self.remove_session(&req.runtime_session_id).await;
                let _ = send_bridge_request(
                    &self.inner,
                    &session.bridge,
                    "session.close",
                    json!({"sessionId": session.bridge_session_id, "reason": "unverified_rebind"}),
                    self.inner.config.request_timeout_ms,
                )
                .await;
                self.shutdown_bridges_if_idle().await;
                Err(RuntimeError::InvalidState(format!(
                    "Claude rebind outcome requires recovery before another turn: {error}"
                )))
            }
        }
    }

    pub(crate) async fn claude_compact(
        &self,
        req: ProviderCompactSessionRequest,
    ) -> Result<ProviderCompactSessionOutcome, RuntimeError> {
        let session = self.get_session(&req.runtime_session_id).await?;
        let _operation = session.operation_lock.try_lock().map_err(|_| {
            RuntimeError::InvalidState(
                "Claude compaction cannot start while session is busy".into(),
            )
        })?;
        ensure_idle(&session).await?;
        if session
            .canonical_provider_session_ref
            .read()
            .await
            .is_none()
        {
            return Err(RuntimeError::InvalidState(
                "compact requires native identity".into(),
            ));
        }
        let response = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.compact",
            json!({"sessionId": session.bridge_session_id}),
            self.inner.config.request_timeout_ms,
        )
        .await?;
        match response.get("outcome").and_then(Value::as_str) {
            Some("accepted") => Ok(ProviderCompactSessionOutcome::Accepted),
            Some("not_performed") => Ok(ProviderCompactSessionOutcome::NotPerformed),
            _ => Err(RuntimeError::ProtocolViolation(
                "invalid compact outcome".into(),
            )),
        }
    }
}

fn required_string<'a>(response: &'a Value, field: &str) -> Result<&'a str, RuntimeError> {
    response
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| RuntimeError::ProtocolViolation(format!("response missing {field}")))
}
