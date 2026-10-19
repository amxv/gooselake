use std::sync::Arc;
use std::time::Duration;

use crate::{
    PersistedUserInputSnapshot, RuntimeError, SendTurnInput, TurnDispatchState,
    TurnInputProjectionSource, WorkspaceAgentLifecycleState, WorkspaceSystemNoticeRecord,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// Workspace creation commits its onboarding intent before anyone may
    /// address the new agent, but native delivery runs asynchronously. This
    /// turn-admission fence orders onboarding ahead of user, message and
    /// process inputs. Its one exception is the exact worker-owned claim.
    pub(super) fn ensure_workspace_onboarding_before_turn(
        &self,
        session_id: &str,
        input: &SendTurnInput,
        authorized_notice_id: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let Some(onboarding) = self
            .store
            .get_workspace_system_notice(&format!("workspace-onboarding:{session_id}"))?
        else {
            // Explicit legacy/unowned sessions have no workspace onboarding.
            return Ok(());
        };
        // The correlation and input are visible to the operator. Matching
        // their bytes is not authority to impersonate a system notice: only
        // this module's worker path can pass the trusted notice identity.
        let exact_claim = authorized_notice_id == Some(onboarding.notice_id.as_str())
            && onboarding.state == "injecting"
            && input.projection_source == Some(TurnInputProjectionSource::AutomationContext)
            && input.correlation_id.as_deref() == Some(onboarding.correlation_id.as_str())
            && input.input == onboarding.input
            && input.expected_turn_id.is_none()
            && input.permission_mode.is_none()
            && input.user_input_snapshot.is_none()
            && input.turn_settings == Default::default();
        if onboarding.state == "delivered" || exact_claim {
            return Ok(());
        }
        Err(RuntimeError::OnboardingPending(format!(
            "workspace agent {session_id} onboarding is {}; inspect its onboarding notice before submitting another turn",
            onboarding.state
        )))
    }

    pub fn get_workspace_onboarding_notice(
        &self,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<Option<WorkspaceSystemNoticeRecord>, RuntimeError> {
        let agent = self.get_workspace_agent(workspace_id, agent_id)?;
        self.store
            .get_workspace_system_notice(&format!("workspace-onboarding:{}", agent.agent_id,))
    }

    /// Repair only a claim that never reached canonical turn admission. The
    /// provider cannot have dispatched such work under the admission invariant.
    pub async fn recover_workspace_system_notices(&self) -> Result<usize, RuntimeError> {
        let mut repaired = 0;
        let mut cursor = None;
        loop {
            let batch = self
                .store
                .list_workspace_system_notices_after(cursor.as_ref(), 256)?;
            if batch.is_empty() {
                break;
            }
            cursor = batch.last().cloned();
            for notice in batch {
                if notice.state == "injecting"
                    && self
                        .latest_turn_admission_for_correlation(
                            &notice.recipient_agent_id,
                            &notice.correlation_id,
                        )
                        .await
                        .is_none()
                    && self.store.transition_workspace_system_notice(
                        &notice.notice_id,
                        "injecting",
                        "pending",
                        None,
                        Some("recovered_pre_admission_claim"),
                        now_ms(),
                    )?
                {
                    repaired += 1;
                }
            }
        }
        Ok(repaired)
    }

    pub fn start_workspace_system_notice_worker(self: &Arc<Self>) {
        let runtime = Arc::downgrade(self);
        tokio::spawn(async move {
            let Some(manager) = runtime.upgrade() else {
                return;
            };
            let mut events = manager.subscribe_events();
            drop(manager);
            let mut ticker = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {},
                    event = events.recv() => {
                        if event.is_err() { continue; }
                    },
                }
                let Some(manager) = runtime.upgrade() else {
                    break;
                };
                let _ = manager.dispatch_workspace_system_notices().await;
            }
        });
    }

    /// A single recipient-scoped system event has one immutable input and one
    /// stable correlation. No ambiguous provider outcome is ever resubmitted.
    pub async fn dispatch_workspace_system_notices(
        self: &Arc<Self>,
    ) -> Result<usize, RuntimeError> {
        let mut cursor = self.system_notice_dispatch_lock.lock().await;
        let mut batch = self
            .store
            .list_workspace_system_notices_after(cursor.as_ref(), 256)?;
        if batch.is_empty() && cursor.is_some() {
            // An old notice can become pending again (for example, when an
            // archived agent is restored). Its immutable ordering key may be
            // behind the previous pass's cursor. Wrap immediately rather than
            // requiring an extra worker tick before considering it again.
            batch = self.store.list_workspace_system_notices_after(None, 256)?;
        }
        *cursor = batch.last().cloned();
        let mut settled = 0;
        for notice in batch {
            if self.reconcile_workspace_notice(&notice).await? {
                settled += 1;
                continue;
            }
            if notice.state != "pending" {
                continue;
            }
            if !self.store.is_workspace_system_notice_head(&notice)? {
                continue;
            }
            let Some(agent) = self
                .store
                .get_workspace_agent_by_id(&notice.recipient_agent_id)?
            else {
                continue;
            };
            if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active {
                self.transition_notice(
                    &notice,
                    "pending",
                    "cancelled",
                    None,
                    Some("recipient_archived"),
                )?;
                settled += 1;
                continue;
            }
            let session = match self.get_session(&notice.recipient_agent_id).await {
                Ok(session) => session,
                Err(_) => continue,
            };
            if session.active_turn_id.is_some()
                || matches!(session.status.as_str(), "failed" | "closed")
            {
                continue;
            }
            let snapshot = PersistedUserInputSnapshot::from_input(&notice.input);
            if let Some(path) = snapshot.first_missing_image_path() {
                self.transition_notice(
                    &notice,
                    "pending",
                    "manual_review",
                    None,
                    Some(&format!("missing_onboarding_image:{path}")),
                )?;
                settled += 1;
                continue;
            }
            if !self.transition_notice(&notice, "pending", "injecting", None, None)? {
                continue;
            }
            let result = {
                let guard = self.session_policy_mutation_lock.lock().await;
                self.send_turn_under_policy_guard(
                    &notice.recipient_agent_id,
                    SendTurnInput {
                        input: notice.input.clone(),
                        expected_turn_id: None,
                        permission_mode: None,
                        projection_source: Some(TurnInputProjectionSource::AutomationContext),
                        user_input_snapshot: None,
                        correlation_id: Some(notice.correlation_id.clone()),
                        turn_settings: Default::default(),
                    },
                    None,
                    Some(&notice.notice_id),
                    guard,
                )
                .await
            };
            let mut claimed = notice.clone();
            claimed.state = "injecting".into();
            if let Err(error) = result {
                if self
                    .latest_turn_admission_for_correlation(
                        &notice.recipient_agent_id,
                        &notice.correlation_id,
                    )
                    .await
                    .is_none()
                {
                    self.transition_notice(
                        &claimed,
                        "injecting",
                        "pending",
                        None,
                        Some(&format!("turn_not_admitted:{error}")),
                    )?;
                    continue;
                }
            }
            if self.reconcile_workspace_notice(&claimed).await? {
                settled += 1;
            }
        }
        Ok(settled)
    }

    fn transition_notice(
        &self,
        notice: &WorkspaceSystemNoticeRecord,
        from: &str,
        to: &str,
        turn_id: Option<&str>,
        error: Option<&str>,
    ) -> Result<bool, RuntimeError> {
        self.store.transition_workspace_system_notice(
            &notice.notice_id,
            from,
            to,
            turn_id,
            error,
            now_ms(),
        )
    }

    async fn reconcile_workspace_notice(
        &self,
        notice: &WorkspaceSystemNoticeRecord,
    ) -> Result<bool, RuntimeError> {
        let Some(admission) = self
            .latest_turn_admission_for_correlation(
                &notice.recipient_agent_id,
                &notice.correlation_id,
            )
            .await
        else {
            return Ok(false);
        };
        let current = notice.state.as_str();
        match admission.dispatch_state {
            TurnDispatchState::Dispatched => {
                self.transition_notice(notice, current, "delivered", Some(&admission.turn_id), None)
            }
            TurnDispatchState::Unknown => self.transition_notice(
                notice,
                current,
                "manual_review",
                Some(&admission.turn_id),
                Some("provider_dispatch_outcome_unknown_do_not_retry"),
            ),
            TurnDispatchState::NotDispatched => {
                if current == "pending" {
                    return Ok(false);
                }
                self.transition_notice(
                    notice,
                    current,
                    "pending",
                    None,
                    Some("provider_proved_not_dispatched"),
                )
            }
            TurnDispatchState::Pending | TurnDispatchState::Dispatching => Ok(false),
        }
    }
}
