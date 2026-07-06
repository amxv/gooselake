use std::sync::Arc;

use crate::{
    ProviderKind, ProviderResumeSessionRequest, RuntimeError, RuntimeEventCriticality,
    RuntimeEventRecord, RuntimeEventScope,
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
                        match provider
                            .resume_session(ProviderResumeSessionRequest {
                                runtime_session_id: session.id.clone(),
                                provider_session_ref,
                                canonical_provider_session_ref: session
                                    .canonical_provider_session_ref
                                    .clone(),
                                cwd: session.cwd.clone(),
                                metadata: Some(session.metadata.clone()),
                            })
                            .await
                        {
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
                            }
                            Err(error) => {
                                updated_session.status = "failed".to_string();
                                updated_session.failure_code =
                                    Some("startup_provider_resume_failed".to_string());
                                updated_session.failure_message = Some(error.to_string());
                                updated_session.active_turn_id = None;
                                session_changed = true;
                                summary.notes.push(format!(
                                    "session {} marked failed: {}",
                                    session.id, error
                                ));
                            }
                        }
                    }
                } else {
                    updated_session.status = "failed".to_string();
                    updated_session.failure_code = Some("startup_missing_provider_ref".to_string());
                    updated_session.failure_message =
                        Some("missing provider_session_ref".to_string());
                    updated_session.active_turn_id = None;
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

            if let Some(active_turn_id) = updated_session.active_turn_id.clone() {
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
