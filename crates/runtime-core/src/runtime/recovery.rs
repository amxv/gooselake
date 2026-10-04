use std::sync::Arc;

use crate::{
    ProviderDispatchOutcome, ProviderKind, ProviderSendTurnRequest, RuntimeError,
    RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, TurnDispatchState,
};

use super::helpers::{is_terminal_turn_status, now_ms};
use super::{RuntimeSessionManager, StartupRecoveryProviderStatus, StartupRecoverySummary};

impl RuntimeSessionManager {
    pub async fn recover_startup(self: &Arc<Self>) -> Result<StartupRecoverySummary, RuntimeError> {
        let started_at = now_ms();
        let mut summary = StartupRecoverySummary {
            started_at,
            ..Default::default()
        };
        let turns_snapshot = self.turns.read().await.clone();
        let approvals_snapshot = self.approvals.read().await.clone();
        let admissions_snapshot = self.turn_admissions.read().await.clone();
        let session_ids = self
            .sessions
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        summary.turns_scanned = turns_snapshot.len();
        summary.approvals_scanned = approvals_snapshot.len();
        summary.sessions_scanned = session_ids.len();

        for provider in self.providers.metadata() {
            let status = match self.providers.get(provider.kind) {
                Some(adapter) => match adapter.healthcheck().await {
                    Ok(()) => StartupRecoveryProviderStatus {
                        provider: provider.kind.as_str().to_string(),
                        healthy: true,
                        detail: None,
                    },
                    Err(error) => StartupRecoveryProviderStatus {
                        provider: provider.kind.as_str().to_string(),
                        healthy: false,
                        detail: Some(error.to_string()),
                    },
                },
                None => StartupRecoveryProviderStatus {
                    provider: provider.kind.as_str().to_string(),
                    healthy: false,
                    detail: Some("provider not registered".to_string()),
                },
            };
            summary.provider_status.push(status);
        }

        for session_id in session_ids {
            let session = match self.get_session(session_id.as_str()).await {
                Ok(session) => session,
                Err(_) => continue,
            };

            let mut updated_session = session.clone();
            let mut session_changed = false;
            let active_admission = session
                .active_turn_id
                .as_ref()
                .and_then(|turn_id| admissions_snapshot.get(turn_id))
                .cloned();
            let mut provider_resume_ready = true;

            if !matches!(session.status.as_str(), "closed" | "failed") {
                if let Some(provider_session_ref) = session.provider_session_ref.clone() {
                    let provider_kind = ProviderKind::from_str(session.provider.as_str())
                        .ok_or_else(|| {
                            RuntimeError::ProtocolViolation(format!(
                                "unknown provider {}",
                                session.provider
                            ))
                        })?;
                    if let Some(provider) = self.providers.get(provider_kind) {
                        let resume_request = self.provider_resume_request_for_session(
                            &session,
                            provider_session_ref,
                            session.canonical_provider_session_ref.clone(),
                        )?;
                        match provider.resume_session_with_policy(resume_request).await {
                            Ok(resumed) => {
                                summary.resumed_sessions += 1;
                                if updated_session.provider_session_ref
                                    != Some(resumed.provider_session_ref.clone())
                                {
                                    updated_session.provider_session_ref =
                                        Some(resumed.provider_session_ref);
                                    session_changed = true;
                                }
                                if updated_session.canonical_provider_session_ref
                                    != resumed.canonical_provider_session_ref
                                {
                                    updated_session.canonical_provider_session_ref =
                                        resumed.canonical_provider_session_ref;
                                    session_changed = true;
                                }
                                if let Some(admission) = active_admission.as_ref() {
                                    if admission.dispatch_state == TurnDispatchState::Dispatched {
                                        if let Some(provider_native_turn_id) =
                                            admission.provider_native_turn_id.as_deref()
                                        {
                                            if let Err(error) = provider
                                                .restore_turn_identity_mapping(
                                                    session.id.as_str(),
                                                    admission.turn_id.as_str(),
                                                    provider_native_turn_id,
                                                )
                                                .await
                                            {
                                                provider_resume_ready = false;
                                                updated_session.status =
                                                    "turn_recovery_required".to_string();
                                                updated_session.failure_code = Some(
                                                    "startup_turn_identity_restore_failed"
                                                        .to_string(),
                                                );
                                                updated_session.failure_message =
                                                    Some(error.to_string());
                                                session_changed = true;
                                                summary.notes.push(format!(
                                                    "session {} could not restore native identity for turn {}: {}",
                                                    session.id, admission.turn_id, error
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                provider_resume_ready = false;
                                if active_admission.is_some() {
                                    updated_session.status = "turn_recovery_required".to_string();
                                    updated_session.failure_code =
                                        Some("startup_provider_resume_failed".to_string());
                                    updated_session.failure_message = Some(error.to_string());
                                } else {
                                    updated_session.status = "failed".to_string();
                                    updated_session.failure_code =
                                        Some("startup_provider_resume_failed".to_string());
                                    updated_session.failure_message = Some(error.to_string());
                                    updated_session.active_turn_id = None;
                                }
                                session_changed = true;
                                summary.notes.push(format!(
                                    "session {} marked failed: {}",
                                    session.id, error
                                ));
                            }
                        }
                    }
                } else {
                    provider_resume_ready = false;
                    if active_admission.is_some() {
                        updated_session.status = "turn_recovery_required".to_string();
                        updated_session.failure_code =
                            Some("startup_missing_provider_ref".to_string());
                        updated_session.failure_message =
                            Some("missing provider_session_ref".to_string());
                    } else {
                        updated_session.status = "failed".to_string();
                        updated_session.failure_code =
                            Some("startup_missing_provider_ref".to_string());
                        updated_session.failure_message =
                            Some("missing provider_session_ref".to_string());
                        updated_session.active_turn_id = None;
                    }
                    session_changed = true;
                }
            }

            let pending_approval_for_turn = |turn_id: &str| -> bool {
                approvals_snapshot.values().any(|approval| {
                    approval.turn_id == turn_id
                        && approval.session_id == session.id
                        && approval.status == "pending"
                })
            };
            let approval_decision_pending_for_turn = |turn_id: &str| -> bool {
                approvals_snapshot.values().any(|approval| {
                    approval.turn_id == turn_id
                        && approval.session_id == session.id
                        && approval.status == "decision_pending"
                })
            };
            let pre_dispatch_approval_status_for_turn = |turn_id: &str| -> Option<&str> {
                approvals_snapshot.values().find_map(|approval| {
                    (approval.turn_id == turn_id
                        && approval.session_id == session.id
                        && approval.origin == "runtime_pre_dispatch_policy")
                        .then_some(approval.status.as_str())
                })
            };

            if let Some(active_turn_id) = updated_session.active_turn_id.clone() {
                if let (Some(turn), Some(admission)) = (
                    turns_snapshot.get(active_turn_id.as_str()),
                    admissions_snapshot.get(active_turn_id.as_str()),
                ) {
                    if approval_decision_pending_for_turn(turn.id.as_str()) {
                        let mut repaired = turn.clone();
                        repaired.status = "approval_decision_unknown".to_string();
                        repaired.error = Some(serde_json::json!({
                            "message": "approval decision outcome is unknown after restart",
                        }));
                        self.store.upsert_turn(&repaired)?;
                        self.turns
                            .write()
                            .await
                            .insert(repaired.id.clone(), repaired);
                        updated_session.status = "turn_recovery_required".to_string();
                        updated_session.updated_at = now_ms();
                        self.store.upsert_session(&updated_session)?;
                        self.sessions
                            .write()
                            .await
                            .insert(updated_session.id.clone(), updated_session.clone());
                        summary.turns_reconciled += 1;
                        summary.sessions_reconciled += 1;
                        summary.notes.push(format!(
                            "session {} retained turn {} with unknown approval decision outcome",
                            session.id, turn.id
                        ));
                        continue;
                    }
                    let provider_kind = ProviderKind::from_str(session.provider.as_str())
                        .ok_or_else(|| {
                            RuntimeError::ProtocolViolation(format!(
                                "unknown provider {}",
                                session.provider
                            ))
                        })?;
                    match admission.dispatch_state {
                        TurnDispatchState::Dispatching | TurnDispatchState::Unknown => {
                            if admission.dispatch_state == TurnDispatchState::Dispatching {
                                self.update_turn_dispatch_authority(
                                    turn.id.as_str(),
                                    TurnDispatchState::Unknown,
                                    admission.provider_native_turn_id.clone(),
                                    Some(serde_json::json!({
                                        "message": "startup recovery found provider dispatch in-flight",
                                    })),
                                )
                                .await?;
                            }
                            let mut repaired = turn.clone();
                            repaired.status = "dispatch_unknown".to_string();
                            repaired.error = Some(serde_json::json!({
                                "message": "provider dispatch outcome is unknown after restart",
                            }));
                            self.store.upsert_turn(&repaired)?;
                            self.turns
                                .write()
                                .await
                                .insert(repaired.id.clone(), repaired);
                            updated_session.status = "turn_recovery_required".to_string();
                            session_changed = true;
                            summary.turns_reconciled += 1;
                            summary.notes.push(format!(
                                "session {} retained turn {} with unknown provider dispatch outcome",
                                session.id, turn.id
                            ));
                            if session_changed {
                                updated_session.updated_at = now_ms();
                                self.store.upsert_session(&updated_session)?;
                                self.sessions
                                    .write()
                                    .await
                                    .insert(updated_session.id.clone(), updated_session.clone());
                                summary.sessions_reconciled += 1;
                            }
                            continue;
                        }
                        TurnDispatchState::NotDispatched => {
                            let error = RuntimeError::provider_not_dispatched(
                                "startup_not_dispatched",
                                "startup recovery confirmed the prior provider turn was not dispatched",
                            );
                            self.mark_not_dispatched(session.id.as_str(), turn.id.as_str(), &error)
                                .await?;
                            summary.turns_reconciled += 1;
                            summary.sessions_reconciled += 1;
                            continue;
                        }
                        TurnDispatchState::Pending
                            if pre_dispatch_approval_status_for_turn(turn.id.as_str())
                                == Some("pending") =>
                        {
                            let mut repaired = turn.clone();
                            repaired.status = "waiting_for_approval".to_string();
                            repaired.error = None;
                            self.store.upsert_turn(&repaired)?;
                            self.turns
                                .write()
                                .await
                                .insert(repaired.id.clone(), repaired);
                            updated_session.status = "waiting_for_approval".to_string();
                            updated_session.failure_code = None;
                            updated_session.failure_message = None;
                            updated_session.updated_at = now_ms();
                            self.store.upsert_session(&updated_session)?;
                            self.sessions
                                .write()
                                .await
                                .insert(updated_session.id.clone(), updated_session.clone());
                            summary.turns_reconciled += 1;
                            summary.sessions_reconciled += 1;
                            continue;
                        }
                        TurnDispatchState::Pending
                            if pre_dispatch_approval_status_for_turn(turn.id.as_str())
                                == Some("decline") =>
                        {
                            let error = RuntimeError::provider_not_dispatched(
                                "pre_dispatch_approval_declined",
                                "runtime pre-dispatch approval was declined before provider dispatch",
                            );
                            self.mark_not_dispatched(session.id.as_str(), turn.id.as_str(), &error)
                                .await?;
                            summary.turns_reconciled += 1;
                            summary.sessions_reconciled += 1;
                            continue;
                        }
                        TurnDispatchState::Pending if provider_resume_ready => {
                            if let Some(missing_path) =
                                admission.user_input_snapshot.first_missing_image_path()
                            {
                                let error = RuntimeError::provider_not_dispatched(
                                    "missing_turn_input_attachment",
                                    format!(
                                        "durable turn input attachment is unavailable during startup recovery: {missing_path}"
                                    ),
                                );
                                self.mark_not_dispatched(
                                    session.id.as_str(),
                                    turn.id.as_str(),
                                    &error,
                                )
                                .await?;
                                summary.turns_reconciled += 1;
                                summary.sessions_reconciled += 1;
                                summary.notes.push(format!(
                                    "turn {} was not replayed because input attachment {} is unavailable",
                                    turn.id, missing_path
                                ));
                                continue;
                            }
                            self.update_turn_dispatch_authority(
                                turn.id.as_str(),
                                TurnDispatchState::Dispatching,
                                None,
                                None,
                            )
                            .await?;
                            let request = ProviderSendTurnRequest {
                                runtime_session_id: session.id.clone(),
                                turn_id: turn.id.clone(),
                                input: turn.input.as_array().cloned().unwrap_or_default(),
                                expected_turn_id: admission.correlation.expected_turn_id.clone(),
                                permission_mode: admission.dispatch_policy.permission_mode.clone(),
                                approval_id: None,
                            };
                            match self
                                .dispatch_send_turn_with_resume_fallback(
                                    provider_kind,
                                    request,
                                    &updated_session,
                                )
                                .await
                            {
                                Ok((ack, provider_events))
                                    if ack.runtime_session_id == session.id
                                        && ack.turn_id == turn.id =>
                                {
                                    self.update_turn_dispatch_authority(
                                        turn.id.as_str(),
                                        TurnDispatchState::Dispatched,
                                        ack.provider_native_turn_id,
                                        None,
                                    )
                                    .await?;
                                    let mut repaired = turn.clone();
                                    repaired.status = "in_progress".to_string();
                                    updated_session.status = "turn_running".to_string();
                                    summary.resumed_waits += 1;
                                    self.spawn_wait_for_turn(
                                        provider_kind,
                                        session.id.clone(),
                                        turn.id.clone(),
                                        provider_events,
                                    );
                                    repaired.error = None;
                                    self.store.upsert_turn(&repaired)?;
                                    self.turns
                                        .write()
                                        .await
                                        .insert(repaired.id.clone(), repaired);
                                    session_changed = true;
                                    summary.turns_reconciled += 1;
                                }
                                Ok((ack, _)) => {
                                    let error = RuntimeError::provider_dispatch_unknown(
                                        "ack_identity_mismatch",
                                        format!(
                                            "startup retry acknowledgement mismatch: session={}, turn={}",
                                            ack.runtime_session_id, ack.turn_id
                                        ),
                                    );
                                    self.mark_dispatch_unknown(
                                        session.id.as_str(),
                                        turn.id.as_str(),
                                        &error,
                                    )
                                    .await?;
                                    summary.turns_reconciled += 1;
                                    summary.sessions_reconciled += 1;
                                    continue;
                                }
                                Err(error)
                                    if error.provider_dispatch_outcome()
                                        == ProviderDispatchOutcome::NotDispatched =>
                                {
                                    self.mark_not_dispatched(
                                        session.id.as_str(),
                                        turn.id.as_str(),
                                        &error,
                                    )
                                    .await?;
                                    summary.turns_reconciled += 1;
                                    summary.sessions_reconciled += 1;
                                    continue;
                                }
                                Err(error) => {
                                    let unknown = RuntimeError::provider_dispatch_unknown(
                                        error
                                            .provider_dispatch_code()
                                            .unwrap_or("startup_provider_error"),
                                        error.to_string(),
                                    );
                                    self.mark_dispatch_unknown(
                                        session.id.as_str(),
                                        turn.id.as_str(),
                                        &unknown,
                                    )
                                    .await?;
                                    summary.turns_reconciled += 1;
                                    summary.sessions_reconciled += 1;
                                    continue;
                                }
                            }
                        }
                        TurnDispatchState::Pending => {
                            updated_session.status = "turn_recovery_required".to_string();
                            session_changed = true;
                            if session_changed {
                                updated_session.updated_at = now_ms();
                                self.store.upsert_session(&updated_session)?;
                                self.sessions
                                    .write()
                                    .await
                                    .insert(updated_session.id.clone(), updated_session.clone());
                                summary.sessions_reconciled += 1;
                            }
                            continue;
                        }
                        TurnDispatchState::Dispatched if !provider_resume_ready => {
                            let mut repaired = turn.clone();
                            repaired.status = "provider_session_recovery_required".to_string();
                            repaired.error = Some(serde_json::json!({
                                "code": updated_session
                                    .failure_code
                                    .clone()
                                    .unwrap_or_else(|| "startup_provider_unavailable".to_string()),
                                "message": updated_session
                                    .failure_message
                                    .clone()
                                    .unwrap_or_else(|| "provider session could not be resumed during startup recovery".to_string()),
                            }));
                            self.store.upsert_turn(&repaired)?;
                            self.turns
                                .write()
                                .await
                                .insert(repaired.id.clone(), repaired);
                            updated_session.status = "turn_recovery_required".to_string();
                            updated_session.updated_at = now_ms();
                            self.store.upsert_session(&updated_session)?;
                            self.sessions
                                .write()
                                .await
                                .insert(updated_session.id.clone(), updated_session.clone());
                            summary.turns_reconciled += 1;
                            summary.sessions_reconciled += 1;
                            summary.notes.push(format!(
                                "session {} retained dispatched turn {} because provider resume evidence is unavailable",
                                session.id, turn.id
                            ));
                            continue;
                        }
                        TurnDispatchState::Dispatched => {
                            let mut repaired = turn.clone();
                            if pending_approval_for_turn(turn.id.as_str()) {
                                repaired.status = "waiting_for_approval".to_string();
                                updated_session.status = "waiting_for_approval".to_string();
                                summary.resumed_waits += 1;
                                self.spawn_wait_for_turn(
                                    provider_kind,
                                    session.id.clone(),
                                    turn.id.clone(),
                                    None,
                                );
                            } else {
                                repaired.status = "in_progress".to_string();
                                updated_session.status = "turn_running".to_string();
                                summary.resumed_waits += 1;
                                self.spawn_wait_for_turn(
                                    provider_kind,
                                    session.id.clone(),
                                    turn.id.clone(),
                                    None,
                                );
                            }
                            repaired.error = None;
                            if repaired != *turn {
                                self.store.upsert_turn(&repaired)?;
                                self.turns
                                    .write()
                                    .await
                                    .insert(repaired.id.clone(), repaired);
                                summary.turns_reconciled += 1;
                            }
                            session_changed = true;
                            if session_changed {
                                updated_session.updated_at = now_ms();
                                self.store.upsert_session(&updated_session)?;
                                self.sessions
                                    .write()
                                    .await
                                    .insert(updated_session.id.clone(), updated_session.clone());
                                summary.sessions_reconciled += 1;
                            }
                            continue;
                        }
                    }
                }
                match turns_snapshot.get(active_turn_id.as_str()) {
                    None => {
                        updated_session.active_turn_id = None;
                        if !matches!(updated_session.status.as_str(), "closed" | "failed") {
                            updated_session.status = "ready".to_string();
                        }
                        session_changed = true;
                        summary.notes.push(format!(
                            "session {} cleared stale active turn {}",
                            session.id, active_turn_id
                        ));
                    }
                    Some(turn) if turn.session_id != session.id => {
                        updated_session.active_turn_id = None;
                        updated_session.status = "failed".to_string();
                        updated_session.failure_code =
                            Some("startup_turn_ownership_mismatch".to_string());
                        updated_session.failure_message =
                            Some(format!("turn {} belongs to {}", turn.id, turn.session_id));
                        session_changed = true;
                    }
                    Some(turn) if is_terminal_turn_status(turn.status.as_str()) => {
                        updated_session.active_turn_id = None;
                        if !matches!(updated_session.status.as_str(), "closed" | "failed") {
                            updated_session.status = "ready".to_string();
                        }
                        session_changed = true;
                    }
                    Some(turn) if turn.status == "waiting_for_approval" => {
                        if pending_approval_for_turn(turn.id.as_str()) {
                            if updated_session.status != "waiting_for_approval" {
                                updated_session.status = "waiting_for_approval".to_string();
                                session_changed = true;
                            }
                        } else {
                            let mut repaired = turn.clone();
                            repaired.status = "failed".to_string();
                            repaired.completed_at = Some(now_ms());
                            repaired.error = Some(serde_json::json!({
                                "message": "startup recovery: missing pending approval",
                            }));
                            self.store.upsert_turn(&repaired)?;
                            {
                                let mut turns = self.turns.write().await;
                                turns.insert(repaired.id.clone(), repaired);
                            }
                            summary.turns_reconciled += 1;
                            updated_session.active_turn_id = None;
                            if !matches!(updated_session.status.as_str(), "closed" | "failed") {
                                updated_session.status = "ready".to_string();
                            }
                            session_changed = true;
                        }
                    }
                    Some(turn) => {
                        if !matches!(updated_session.status.as_str(), "closed" | "failed") {
                            updated_session.status = "turn_running".to_string();
                        }
                        let provider_kind = ProviderKind::from_str(session.provider.as_str())
                            .ok_or_else(|| {
                                RuntimeError::ProtocolViolation(format!(
                                    "unknown provider {}",
                                    session.provider
                                ))
                            })?;
                        summary.resumed_waits += 1;
                        self.spawn_wait_for_turn(
                            provider_kind,
                            session.id.clone(),
                            turn.id.clone(),
                            None,
                        );
                    }
                }
            } else if matches!(
                updated_session.status.as_str(),
                "turn_running" | "waiting_for_approval"
            ) {
                updated_session.status = "ready".to_string();
                session_changed = true;
            }

            if session_changed {
                updated_session.updated_at = now_ms();
                self.store.upsert_session(&updated_session)?;
                {
                    let mut sessions = self.sessions.write().await;
                    sessions.insert(updated_session.id.clone(), updated_session.clone());
                }
                summary.sessions_reconciled += 1;
            }
        }

        let approval_ids = approvals_snapshot.keys().cloned().collect::<Vec<_>>();
        for approval_id in approval_ids {
            let Some(approval) = approvals_snapshot.get(approval_id.as_str()) else {
                continue;
            };
            if approval.status != "pending" {
                continue;
            }
            let turn = turns_snapshot.get(approval.turn_id.as_str());
            if turn.is_none()
                || turn.is_some_and(|turn| is_terminal_turn_status(turn.status.as_str()))
            {
                let mut resolved = approval.clone();
                resolved.status = "decline".to_string();
                resolved.resolved_at = Some(now_ms());
                resolved.response = Some(serde_json::json!({
                    "reason": "startup_recovery_orphaned_approval",
                }));
                self.store.upsert_approval(&resolved)?;
                {
                    let mut approvals = self.approvals.write().await;
                    approvals.insert(resolved.id.clone(), resolved);
                }
                summary.approvals_reconciled += 1;
            }
        }

        summary.completed_at = now_ms();
        Ok(summary)
    }

    pub async fn emit_startup_recovered_event(
        &self,
        summary: &StartupRecoverySummary,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.append_event(
            RuntimeEventScope::System,
            "startup_recovery",
            None,
            None,
            "runtime.startup_recovered",
            RuntimeEventCriticality::Critical,
            serde_json::json!({ "summary": summary }),
        )
        .await
    }
}
