use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use crate::{
    ProviderCapabilitySupport, ProviderCompactSessionOutcome, ProviderCompactSessionRequest,
    ProviderCompactionObservation, ProviderKind, ProviderRuntimeEvent, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionCompactionRun, WorkspaceAgentLifecycleState,
};

use super::{helpers::now_ms, RuntimeSessionManager};

impl RuntimeSessionManager {
    fn compaction_observer_flag(provider: ProviderKind) -> u8 {
        match provider {
            ProviderKind::Codex => 1,
            ProviderKind::Claude => 2,
            ProviderKind::Acp => 4,
        }
    }

    /// The production listener is the single owner of a provider's native
    /// compaction stream. Turn-local consumers only need the fallback when
    /// an embedded/standalone manager never started that listener.
    pub(super) async fn global_compaction_observer_owns(&self, session_id: &str) -> bool {
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .and_then(|session| ProviderKind::from_str(&session.provider))
            .is_some_and(|provider| {
                self.provider_compaction_observer_flags
                    .load(Ordering::Acquire)
                    & Self::compaction_observer_flag(provider)
                    != 0
            })
    }

    /// All provider events, including manual replies and background automatic
    /// compaction, enter this one attachment-fenced durable state machine.
    pub(super) async fn observe_provider_compaction_event(
        &self,
        event: &ProviderRuntimeEvent,
    ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
        let ProviderRuntimeEvent::ContextCompactionObserved {
            runtime_session_id,
            provider_session_ref,
            canonical_provider_session_ref,
            turn_id,
            phase,
            trigger,
            pre_tokens,
            post_tokens,
            context_window_size,
        } = event
        else {
            return Ok(None);
        };
        let provider_name = {
            let sessions = self.sessions.read().await;
            let Some(session) = sessions.get(runtime_session_id) else {
                return Ok(None);
            };
            session.provider.clone()
        };
        let provider = ProviderKind::from_str(&provider_name).ok_or_else(|| {
            RuntimeError::ProtocolViolation("unknown provider emitting compaction".into())
        })?;
        let observation = ProviderCompactionObservation {
            proposed_run_id: self.allocate_id("cmp", provider.as_str()),
            agent_id: runtime_session_id.clone(),
            provider,
            provider_session_ref: provider_session_ref.clone(),
            canonical_provider_session_ref: canonical_provider_session_ref.clone(),
            turn_id: turn_id.clone(),
            phase: phase.clone(),
            trigger: trigger.clone(),
            pre_tokens: *pre_tokens,
            post_tokens: *post_tokens,
            context_window_size: *context_window_size,
            observed_at_ms: now_ms(),
        };
        let receipt = match self.store.observe_provider_compaction(&observation) {
            Ok(receipt) => receipt,
            Err(RuntimeError::Conflict(message)) => {
                // A delayed event from a superseded provider generation
                // cannot mutate the new session or poison its active turn.
                tracing::debug!("ignored uncorrelated compaction observation: {message}");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if receipt.changed {
            self.append_event(
                RuntimeEventScope::Session,
                runtime_session_id,
                Some(runtime_session_id),
                turn_id.as_deref(),
                "compaction.lifecycle_changed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "run_id": receipt.run.run_id,
                    "phase": receipt.run.phase,
                    "source": receipt.run.source,
                    "trigger": receipt.run.trigger,
                    "pre_tokens": receipt.run.pre_tokens,
                    "post_tokens": receipt.run.post_tokens,
                    "context_window_size": receipt.run.context_window_size,
                }),
            )
            .await?;
        }
        if receipt.changed && receipt.run.phase == "completed" {
            let _ = self
                .refresh_session_context_limit(runtime_session_id, turn_id.as_deref())
                .await;
        }
        Ok(Some(receipt.run))
    }

    /// Subscribe at runtime bootstrap, before any client can admit work.
    /// This also observes Codex automatic compactions with no active turn ID.
    pub fn start_provider_compaction_observers(self: &Arc<Self>) {
        for metadata in self.providers.metadata() {
            let Some(mut events) = self
                .providers
                .get(metadata.kind)
                .and_then(|adapter| adapter.subscribe_events())
            else {
                continue;
            };
            let manager = Arc::downgrade(self);
            self.provider_compaction_observer_flags.fetch_or(
                Self::compaction_observer_flag(metadata.kind),
                Ordering::Release,
            );
            tokio::spawn(async move {
                loop {
                    match events.recv().await {
                        Ok(event @ ProviderRuntimeEvent::ContextCompactionObserved { .. }) => {
                            let Some(runtime) = manager.upgrade() else {
                                break;
                            };
                            if let Err(error) =
                                runtime.observe_provider_compaction_event(&event).await
                            {
                                tracing::warn!(
                                    "could not persist provider compaction observation: {error}"
                                );
                            }
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Never synthesize a missing terminal event.
                            tracing::warn!("compaction event receiver lagged; ambiguous manual runs require review");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        }
    }

    pub fn session_compaction_run(
        &self,
        agent_id: &str,
        run_id: &str,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        self.store
            .get_workspace_agent_by_id(agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        let run = self
            .store
            .get_session_compaction(run_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("compaction run {run_id}")))?;
        if run.agent_id != agent_id {
            return Err(RuntimeError::NotFound(format!("compaction run {run_id}")));
        }
        Ok(run)
    }

    /// A trusted operator can settle a quarantined native outcome only after
    /// verifying it externally. This never redispatches the provider request.
    pub fn resolve_compaction_review(
        &self,
        agent_id: &str,
        run_id: &str,
        phase: &str,
        evidence: &str,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        self.session_compaction_run(agent_id, run_id)?;
        self.store
            .resolve_session_compaction_review(run_id, phase, evidence, now_ms())
    }

    /// A single-flight, crash-safe manager operation. Provider accepted means
    /// only that native compaction started; follow-up observations settle it.
    pub async fn compact_workspace_agent(
        self: &Arc<Self>,
        agent_id: &str,
        idempotency_key: Option<String>,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        let _guard = self.session_policy_mutation_lock.lock().await;
        if self.store.unresolved_session_edit_rerun(agent_id)? {
            return Err(RuntimeError::Conflict(
                "cannot compact while native edit-and-rerun remains unresolved".into(),
            ));
        }
        if let Some(key) = idempotency_key.as_deref() {
            if key.trim().is_empty() || key.len() > 128 {
                return Err(RuntimeError::InvalidState(
                    "Idempotency-Key must be 1-128 nonblank characters".into(),
                ));
            }
            if let Some(existing) = self.store.get_session_compaction_by_key(agent_id, key)? {
                return Ok(existing);
            }
        }
        let agent = self.get_active_workspace_agent_by_id(agent_id)?;
        if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active {
            return Err(RuntimeError::Conflict("agent is archived".into()));
        }
        let session = self.get_session(agent_id).await?;
        if session.status != "ready" || session.active_turn_id.is_some() {
            return Err(RuntimeError::Conflict(
                "manual compaction requires a ready idle agent".into(),
            ));
        }
        if self.store.unresolved_workspace_agent_rebind(agent_id)? {
            return Err(RuntimeError::Conflict(
                "cannot compact during unresolved workspace rebinding".into(),
            ));
        }
        let provider = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation("unknown provider for session compaction".into())
        })?;
        if provider != agent.recreation_policy.provider {
            return Err(RuntimeError::ProtocolViolation(
                "agent/provider authority mismatch during compaction".into(),
            ));
        }
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().into()))?;
        if adapter.capabilities().manual_compact != ProviderCapabilitySupport::Supported {
            return Err(RuntimeError::Unsupported(format!(
                "provider {} does not support manual compaction",
                provider.as_str()
            )));
        }
        let provider_session_ref = session.provider_session_ref.clone().ok_or_else(|| {
            RuntimeError::InvalidState("compaction requires a native provider attachment".into())
        })?;
        let now = now_ms();
        let run = SessionCompactionRun {
            run_id: self.allocate_id("cmp", provider.as_str()),
            agent_id: agent_id.to_owned(),
            provider,
            provider_session_ref,
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            agent_revision: agent.revision,
            expected_session_updated_at: session.updated_at,
            idempotency_key,
            phase: "intended".into(),
            outcome: None,
            error_code: None,
            review_evidence: None,
            source: "manual".into(),
            confidence: None,
            observed_turn_id: None,
            trigger: Some("manual".into()),
            pre_tokens: None,
            post_tokens: None,
            context_window_size: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.store.begin_session_compaction(&run)?;
        // Subscribe before native dispatch to preserve an observation that
        // arrives ahead of the provider's synchronous acceptance response.
        let events = adapter.subscribe_events();
        let native_request = ProviderCompactSessionRequest {
            runtime_session_id: agent_id.into(),
        };
        let native_result = match adapter.compact_session(native_request.clone()).await {
            Err(RuntimeError::NotFound(_)) => {
                // The local provider adapter may have been recreated. Resuming
                // its exact durable policy is safe; repeating an ambiguous
                // native compact request is not.
                let resume = self.provider_resume_request_for_session(
                    &session,
                    run.provider_session_ref.clone(),
                    run.canonical_provider_session_ref.clone(),
                );
                match resume {
                    Ok(resume) => match adapter.resume_session_with_policy(resume).await {
                        Ok(observed)
                            if observed.runtime_session_id == agent_id
                                && observed.provider_session_ref == run.provider_session_ref
                                && observed.canonical_provider_session_ref
                                    == run.canonical_provider_session_ref =>
                        {
                            adapter.compact_session(native_request).await
                        }
                        Ok(_) => Err(RuntimeError::ProtocolViolation(
                            "compaction resume changed native provider session identity".into(),
                        )),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            }
            result => result,
        };
        let (phase, outcome, error_code) = match &native_result {
            Ok(ProviderCompactSessionOutcome::Accepted) => (
                "accepted",
                Some(ProviderCompactSessionOutcome::Accepted),
                None,
            ),
            Ok(ProviderCompactSessionOutcome::NotPerformed) => (
                "not_performed",
                Some(ProviderCompactSessionOutcome::NotPerformed),
                None,
            ),
            Err(_) => (
                "manual_review",
                None,
                Some("native_compaction_outcome_unknown"),
            ),
        };
        if let Some(observed) = self.store.get_session_compaction(&run.run_id)? {
            if observed.phase == "completed" {
                // Native terminal evidence is stronger than a late or
                // contradictory synchronous response.
                return Ok(observed);
            }
        }
        let persisted = self.store.transition_session_compaction(
            &run.run_id,
            phase,
            outcome,
            error_code,
            now_ms(),
        )?;
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                agent_id,
                Some(agent_id),
                None,
                "compaction.request_state_changed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({"run_id": run.run_id, "phase": persisted.phase}),
            )
            .await;
        match native_result {
            Ok(ProviderCompactSessionOutcome::Accepted) => {
                if persisted.phase == "completed" {
                    return Ok(persisted);
                }
                if self.provider_compaction_observer_flags.load(Ordering::Acquire)
                    & Self::compaction_observer_flag(provider) != 0
                {
                    // The global receiver owns the event stream; a second
                    // subscriber would risk duplicate weak native signals.
                    self.spawn_compaction_timeout(persisted.run_id.clone());
                    Ok(persisted)
                } else if let Some(events) = events {
                    // Standalone manager instances (including tests) that
                    // have no global worker still observe native completion.
                    self.spawn_compaction_observer(persisted.run_id.clone(), agent_id.into(), events);
                    Ok(persisted)
                } else {
                    // An adapter that accepts compaction but cannot surface
                    // terminal events must not leave an unresolvable forever-
                    // accepted request in the durable manager state.
                    self.store.transition_session_compaction(
                        &persisted.run_id,
                        "manual_review",
                        None,
                        Some("provider_compaction_observer_unavailable"),
                        now_ms(),
                    )
                }
            }
            Ok(ProviderCompactSessionOutcome::NotPerformed) => Ok(persisted),
            Err(error) => Err(RuntimeError::provider_dispatch_unknown(
                "native_compaction_outcome_unknown",
                format!(
                    "manual compaction {} may have reached the provider; inspect its durable status instead of resubmitting: {error}",
                    persisted.run_id
                ),
            )),
        }
    }

    fn spawn_compaction_observer(
        self: &Arc<Self>,
        run_id: String,
        agent_id: String,
        mut events: broadcast::Receiver<ProviderRuntimeEvent>,
    ) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let terminal = tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    if let Ok(Some(current)) = manager.store.get_session_compaction(&run_id) {
                        if matches!(current.phase.as_str(), "completed" | "failed" | "cancelled") {
                            return true;
                        }
                    }
                    match events.recv().await {
                        Ok(
                            ref event @ ProviderRuntimeEvent::ContextCompactionObserved {
                                ref runtime_session_id,
                                ..
                            },
                        ) if runtime_session_id == &agent_id => {
                            if let Ok(Some(run)) =
                                manager.observe_provider_compaction_event(&event).await
                            {
                                if run.run_id == run_id
                                    && matches!(
                                        run.phase.as_str(),
                                        "completed" | "failed" | "cancelled"
                                    )
                                {
                                    return true;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => return false,
                        Err(broadcast::error::RecvError::Closed) => return false,
                        _ => {}
                    }
                }
            })
            .await
            .unwrap_or(false);
            if terminal {
                return;
            }
            // Lost native evidence must not be interpreted as a failure or
            // successful compaction. Preserve the manual-review fence.
            let _ = manager.store.transition_session_compaction(
                &run_id,
                "manual_review",
                None,
                Some("compaction_evidence_missing"),
                now_ms(),
            );
        });
    }

    fn spawn_compaction_timeout(self: &Arc<Self>, run_id: String) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            if let Ok(Some(run)) = manager.store.get_session_compaction(&run_id) {
                if matches!(
                    run.phase.as_str(),
                    "intended" | "accepted" | "started" | "compacting"
                ) {
                    let _ = manager.store.transition_session_compaction(
                        &run_id,
                        "manual_review",
                        None,
                        Some("compaction_evidence_missing"),
                        now_ms(),
                    );
                }
            }
        });
    }
}
