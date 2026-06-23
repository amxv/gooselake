use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, RwLock};

use crate::{
    ApprovalDecision, ApprovalRecord, NewRuntimeEvent, ProviderApprovalResponseRequest,
    ProviderAuthStatus, ProviderCloseSessionRequest, ProviderCreateSessionRequest,
    ProviderInterruptTurnRequest, ProviderKind, ProviderRegistry, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest,
    RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, RuntimeStore,
    SessionRecord, TurnRecord,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionInput {
    pub provider: ProviderKind,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub permission_mode: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendTurnInput {
    pub input: Vec<Value>,
    pub expected_turn_id: Option<String>,
    pub permission_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeSessionInput {
    pub provider_session_ref: Option<String>,
    pub canonical_provider_session_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalResponseInput {
    pub decision: String,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendTurnAccepted {
    pub session_id: String,
    pub turn_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StartupRecoveryProviderStatus {
    pub provider: String,
    pub healthy: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StartupRecoverySummary {
    pub started_at: i64,
    pub completed_at: i64,
    pub sessions_scanned: usize,
    pub turns_scanned: usize,
    pub approvals_scanned: usize,
    pub sessions_reconciled: usize,
    pub turns_reconciled: usize,
    pub approvals_reconciled: usize,
    pub resumed_sessions: usize,
    pub resumed_waits: usize,
    pub deferred_deliveries_retried: usize,
    pub provider_status: Vec<StartupRecoveryProviderStatus>,
    pub notes: Vec<String>,
}

pub struct RuntimeSessionManager {
    store: Arc<dyn RuntimeStore>,
    providers: Arc<ProviderRegistry>,
    sessions: RwLock<HashMap<String, SessionRecord>>,
    turns: RwLock<HashMap<String, TurnRecord>>,
    approvals: RwLock<HashMap<String, ApprovalRecord>>,
    next_id: AtomicU64,
    event_tx: broadcast::Sender<RuntimeEventRecord>,
}

impl RuntimeSessionManager {
    pub fn new(
        store: Arc<dyn RuntimeStore>,
        providers: Arc<ProviderRegistry>,
        live_event_capacity: usize,
    ) -> Result<Self, RuntimeError> {
        let hydrated = store.hydrate_runtime_state()?;
        let sessions = hydrated
            .sessions
            .into_iter()
            .map(|session| (session.id.clone(), session))
            .collect::<HashMap<_, _>>();
        let turns = hydrated
            .turns
            .into_iter()
            .map(|turn| (turn.id.clone(), turn))
            .collect::<HashMap<_, _>>();
        let approvals = hydrated
            .approvals
            .into_iter()
            .map(|approval| (approval.id.clone(), approval))
            .collect::<HashMap<_, _>>();
        let (event_tx, _) = broadcast::channel(live_event_capacity.max(128));

        Ok(Self {
            store,
            providers,
            sessions: RwLock::new(sessions),
            turns: RwLock::new(turns),
            approvals: RwLock::new(approvals),
            next_id: AtomicU64::new(1),
            event_tx,
        })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEventRecord> {
        self.event_tx.subscribe()
    }

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

        self.store.upsert_session(&record)?;
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

    pub async fn send_turn(
        self: &Arc<Self>,
        session_id: &str,
        input: SendTurnInput,
    ) -> Result<SendTurnAccepted, RuntimeError> {
        let session = self.get_session(session_id).await?;
        if session.status == "closed" || session.status == "failed" {
            return Err(RuntimeError::InvalidState(format!(
                "session {session_id} is not writable in status {}",
                session.status
            )));
        }
        if session.active_turn_id.is_some() {
            return Err(RuntimeError::InvalidState(format!(
                "session {session_id} already has an active turn"
            )));
        }
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let turn_id = self.allocate_id("turn", provider_kind.as_str());
        let now = now_ms();
        let effective_permission_mode = input
            .permission_mode
            .clone()
            .or_else(|| session.permission_mode.clone());
        let requires_approval = effective_permission_mode.as_deref() == Some("require_approval");
        let approval_id = if requires_approval {
            Some(self.allocate_id("apr", provider_kind.as_str()))
        } else {
            None
        };

        let session_metadata_for_provider = session.metadata.clone();
        let provider_send_input = ProviderSendTurnRequest {
            runtime_session_id: session_id.to_string(),
            turn_id: turn_id.clone(),
            input: input.input.clone(),
            expected_turn_id: input.expected_turn_id.clone(),
            permission_mode: effective_permission_mode.clone(),
            approval_id: approval_id.clone(),
        };
        let ack_result = self
            .dispatch_send_turn_with_resume_fallback(
                provider_kind,
                provider_send_input,
                session.cwd.clone(),
                session.provider_session_ref.clone(),
                session.canonical_provider_session_ref.clone(),
                session_metadata_for_provider,
            )
            .await;

        let ack = match ack_result {
            Ok(ack) => {
                if ack.runtime_session_id != session_id || ack.turn_id != turn_id {
                    return Err(RuntimeError::ProtocolViolation(format!(
                        "provider send_turn acknowledgement mismatch (expected_session={}, expected_turn={}, actual_session={}, actual_turn={})",
                        session_id, turn_id, ack.runtime_session_id, ack.turn_id
                    )));
                }
                ack
            }
            Err(error) => {
                // Fail closed: persist a terminal failed turn and keep session writable.
                let failed_turn = TurnRecord {
                    id: turn_id.clone(),
                    session_id: session_id.to_string(),
                    provider_turn_ref: None,
                    status: "failed".to_string(),
                    input: Value::Array(input.input),
                    source: Some("user".to_string()),
                    started_at: Some(now),
                    completed_at: Some(now_ms()),
                    usage: None,
                    error: Some(serde_json::json!({ "message": error.to_string() })),
                };
                let mut failed_session = session.clone();
                failed_session.active_turn_id = None;
                if failed_session.status != "closed" && failed_session.status != "failed" {
                    failed_session.status = "ready".to_string();
                }
                failed_session.updated_at = now_ms();
                self.store.upsert_turn(&failed_turn)?;
                self.store.upsert_session(&failed_session)?;
                {
                    let mut turns = self.turns.write().await;
                    turns.insert(turn_id.clone(), failed_turn);
                }
                {
                    let mut sessions = self.sessions.write().await;
                    sessions.insert(session_id.to_string(), failed_session);
                }
                let _ = self
                    .append_event(
                        RuntimeEventScope::Session,
                        session_id,
                        Some(session_id),
                        Some(turn_id.as_str()),
                        "turn.failed",
                        RuntimeEventCriticality::Critical,
                        serde_json::json!({ "error": error.to_string() }),
                    )
                    .await?;
                return Err(error);
            }
        };

        let turn = TurnRecord {
            id: turn_id.clone(),
            session_id: session_id.to_string(),
            provider_turn_ref: None,
            status: if requires_approval {
                "waiting_for_approval".to_string()
            } else {
                "in_progress".to_string()
            },
            input: Value::Array(input.input),
            source: Some("user".to_string()),
            started_at: Some(now),
            completed_at: None,
            usage: None,
            error: None,
        };
        let mut updated_session = session.clone();
        updated_session.status = if requires_approval {
            "waiting_for_approval".to_string()
        } else {
            "turn_running".to_string()
        };
        updated_session.active_turn_id = Some(turn_id.clone());
        updated_session.updated_at = now;

        self.store.upsert_turn(&turn)?;
        self.store.upsert_session(&updated_session)?;
        {
            let mut turns = self.turns.write().await;
            turns.insert(turn_id.clone(), turn.clone());
        }
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated_session);
        }
        if requires_approval {
            let approval_id =
                approval_id.expect("approval id must exist when approval is required");
            let approval = ApprovalRecord {
                id: approval_id.clone(),
                session_id: session_id.to_string(),
                turn_id: turn_id.clone(),
                tool_call_id: None,
                provider_approval_ref: Some(approval_id.clone()),
                status: "pending".to_string(),
                request: serde_json::json!({
                    "reason": "manual approval required before provider execution",
                }),
                response: None,
                created_at: now,
                resolved_at: None,
            };
            self.store.upsert_approval(&approval)?;
            {
                let mut approvals = self.approvals.write().await;
                approvals.insert(approval_id.clone(), approval);
            }
            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(turn_id.as_str()),
                    "approval.requested",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({
                        "approval_id": approval_id,
                    }),
                )
                .await?;
        } else {
            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(turn_id.as_str()),
                    "turn.started",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({}),
                )
                .await?;
            self.spawn_wait_for_turn(provider_kind, session_id.to_string(), turn_id.clone());
        }

        Ok(SendTurnAccepted {
            session_id: session_id.to_string(),
            turn_id,
            status: if requires_approval {
                "waiting_for_approval".to_string()
            } else {
                let _ = ack;
                "in_progress".to_string()
            },
        })
    }

    pub async fn interrupt_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let session = self.get_session(session_id).await?;
        if session.active_turn_id.as_deref() != Some(turn_id) {
            return Err(RuntimeError::InvalidState(format!(
                "turn {turn_id} is not active for session {session_id}"
            )));
        }
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;
        provider
            .interrupt_turn(ProviderInterruptTurnRequest {
                runtime_session_id: session_id.to_string(),
                turn_id: turn_id.to_string(),
            })
            .await?;
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id),
                "turn.interrupt_requested",
                RuntimeEventCriticality::Critical,
                serde_json::json!({}),
            )
            .await?;
        Ok(())
    }

    pub async fn respond_approval(
        self: &Arc<Self>,
        session_id: &str,
        approval_id: &str,
        input: ApprovalResponseInput,
    ) -> Result<ApprovalRecord, RuntimeError> {
        let session = self.get_session(session_id).await?;
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;

        let mut approvals = self.approvals.write().await;
        let existing = approvals
            .get(approval_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("approval {approval_id}")))?;
        if existing.session_id != session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "approval {} does not belong to session {}",
                approval_id, session_id
            )));
        }
        if existing.status != "pending" {
            return Err(RuntimeError::InvalidState(format!(
                "approval {} is not pending",
                approval_id
            )));
        }
        let normalized_decision = ApprovalDecision::parse(input.decision.as_str())?;

        provider
            .respond_approval(ProviderApprovalResponseRequest {
                runtime_session_id: session_id.to_string(),
                turn_id: existing.turn_id.clone(),
                approval_id: approval_id.to_string(),
                decision: normalized_decision.as_str().to_string(),
                payload: input.payload.clone(),
            })
            .await?;

        let mut resolved = existing.clone();
        resolved.status = normalized_decision.as_str().to_string();
        resolved.response = input.payload.clone();
        resolved.resolved_at = Some(now_ms());
        self.store.upsert_approval(&resolved)?;
        approvals.insert(approval_id.to_string(), resolved.clone());
        drop(approvals);

        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(resolved.turn_id.as_str()),
                "approval.resolved",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "approval_id": approval_id }),
            )
            .await?;

        if normalized_decision == ApprovalDecision::Accept {
            let mut turns = self.turns.write().await;
            let mut sessions = self.sessions.write().await;
            if let Some(turn) = turns.get_mut(&resolved.turn_id) {
                turn.status = "in_progress".to_string();
                turn.error = None;
                self.store.upsert_turn(turn)?;
            }
            if let Some(session) = sessions.get_mut(session_id) {
                session.status = "turn_running".to_string();
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            drop(sessions);
            drop(turns);

            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(resolved.turn_id.as_str()),
                    "turn.started",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({
                        "source": "approval.accepted",
                    }),
                )
                .await?;
            self.spawn_wait_for_turn(
                provider_kind,
                session_id.to_string(),
                resolved.turn_id.clone(),
            );
        } else {
            let mut turns = self.turns.write().await;
            let mut sessions = self.sessions.write().await;
            if let Some(turn) = turns.get_mut(&resolved.turn_id) {
                turn.status = "interrupted".to_string();
                turn.completed_at = Some(now_ms());
                turn.error = Some(serde_json::json!({
                    "message": "approval declined",
                }));
                self.store.upsert_turn(turn)?;
            }
            if let Some(session) = sessions.get_mut(session_id) {
                if session.active_turn_id.as_deref() == Some(resolved.turn_id.as_str()) {
                    session.active_turn_id = None;
                }
                if session.status != "closed" && session.status != "failed" {
                    session.status = "ready".to_string();
                }
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            drop(sessions);
            drop(turns);
            let _ = self
                .append_event(
                    RuntimeEventScope::Session,
                    session_id,
                    Some(session_id),
                    Some(resolved.turn_id.as_str()),
                    "turn.interrupted",
                    RuntimeEventCriticality::Critical,
                    serde_json::json!({
                        "source": "approval.declined",
                    }),
                )
                .await?;
        }

        Ok(resolved)
    }

    pub fn replay_session_events(
        &self,
        session_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.store.list_runtime_events(
            Some((RuntimeEventScope::Session, session_id)),
            after_seq,
            limit.max(1),
        )
    }

    async fn dispatch_send_turn_with_resume_fallback(
        &self,
        provider_kind: ProviderKind,
        request: ProviderSendTurnRequest,
        cwd: Option<String>,
        provider_session_ref: Option<String>,
        canonical_provider_session_ref: Option<String>,
        metadata: Value,
    ) -> Result<crate::ProviderTurnAck, RuntimeError> {
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;
        match provider.send_turn(request.clone()).await {
            Ok(ack) => Ok(ack),
            Err(RuntimeError::NotFound(_)) => {
                let provider_session_ref = provider_session_ref.ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "provider session {} was not found and cannot be resumed",
                        request.runtime_session_id
                    ))
                })?;
                provider
                    .resume_session(ProviderResumeSessionRequest {
                        runtime_session_id: request.runtime_session_id.clone(),
                        provider_session_ref,
                        canonical_provider_session_ref,
                        cwd,
                        metadata: Some(metadata),
                    })
                    .await?;
                provider.send_turn(request).await
            }
            Err(error) => Err(error),
        }
    }

    fn spawn_wait_for_turn(
        self: &Arc<Self>,
        provider: ProviderKind,
        session_id: String,
        turn_id: String,
    ) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let provider_adapter = match manager.providers.get(provider) {
                Some(provider_adapter) => provider_adapter,
                None => return,
            };
            let result = provider_adapter
                .wait_for_turn(ProviderWaitTurnRequest {
                    runtime_session_id: session_id.clone(),
                    turn_id: turn_id.clone(),
                    timeout_ms: None,
                })
                .await;
            match result {
                Ok(turn_result) => {
                    if let Err(error) = manager.apply_terminal_result(turn_result).await {
                        if std::env::var("GG_CLAUDE_SMOKE_DEBUG")
                            .ok()
                            .map(|value| value.trim() == "1")
                            .unwrap_or(false)
                        {
                            eprintln!(
                                "[runtime-core] failed to apply terminal turn result for session_id={} turn_id={}: {}",
                                session_id, turn_id, error
                            );
                        }
                    }
                }
                Err(error) => {
                    let _ = manager
                        .apply_terminal_failure(session_id.as_str(), turn_id.as_str(), error)
                        .await;
                }
            }
        });
    }

    async fn apply_terminal_result(&self, result: ProviderTurnResult) -> Result<(), RuntimeError> {
        let mut turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        let Some(turn) = turns.get_mut(&result.turn_id) else {
            return Err(RuntimeError::NotFound(format!("turn {}", result.turn_id)));
        };
        if turn.session_id != result.runtime_session_id {
            return Err(RuntimeError::ProtocolViolation(format!(
                "provider turn ownership mismatch for turn {}",
                result.turn_id
            )));
        }

        if is_terminal_turn_status(turn.status.as_str()) {
            let incoming_status = result.status.as_str();
            if turn.status == incoming_status {
                return Ok(());
            }
            let session_id = turn.session_id.clone();
            let conflict = format!(
                "conflicting terminal state for turn {} (stored={}, incoming={})",
                result.turn_id, turn.status, incoming_status
            );
            if let Some(session) = sessions.get_mut(&session_id) {
                session.status = "failed".to_string();
                session.failure_code = Some("terminal_conflict".to_string());
                session.failure_message = Some(conflict.clone());
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            return Err(RuntimeError::ProtocolViolation(conflict));
        }

        turn.status = result.status.as_str().to_string();
        turn.completed_at = Some(now_ms());
        turn.usage = result.usage.clone();
        turn.error = result.error.clone();
        self.store.upsert_turn(turn)?;

        let Some(session) = sessions.get_mut(&result.runtime_session_id) else {
            return Err(RuntimeError::NotFound(format!(
                "session {}",
                result.runtime_session_id
            )));
        };
        if session.active_turn_id.as_deref() == Some(result.turn_id.as_str()) {
            session.active_turn_id = None;
        }
        if session.status != "closed" && session.status != "failed" {
            session.status = "ready".to_string();
        }
        if result.status == ProviderTurnStatus::Completed
            || result.status == ProviderTurnStatus::Interrupted
        {
            let user_text = extract_turn_user_text(turn.input.as_array());
            let assistant_text = result
                .usage
                .as_ref()
                .and_then(extract_assistant_text_from_usage);
            if let Some(user_text) = user_text {
                append_session_transcript(&mut session.metadata, "user", user_text.as_str());
            }
            if let Some(assistant_text) = assistant_text {
                append_session_transcript(
                    &mut session.metadata,
                    "assistant",
                    assistant_text.as_str(),
                );
            }
        }
        session.updated_at = now_ms();
        self.store.upsert_session(session)?;

        let event_kind = match result.status {
            ProviderTurnStatus::Completed => "turn.completed",
            ProviderTurnStatus::Interrupted => "turn.interrupted",
            ProviderTurnStatus::Failed => "turn.failed",
            ProviderTurnStatus::InProgress => "turn.in_progress",
        };
        let assistant_text = result
            .usage
            .as_ref()
            .and_then(extract_assistant_text_from_usage);
        drop(sessions);
        drop(turns);

        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                result.runtime_session_id.as_str(),
                Some(result.runtime_session_id.as_str()),
                Some(result.turn_id.as_str()),
                event_kind,
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "status": result.status.as_str(),
                    "usage": result.usage,
                    "error": result.error,
                    "assistant_text": assistant_text,
                }),
            )
            .await?;
        Ok(())
    }

    async fn apply_terminal_failure(
        &self,
        session_id: &str,
        turn_id: &str,
        error: RuntimeError,
    ) -> Result<(), RuntimeError> {
        let mut turns = self.turns.write().await;
        if let Some(turn) = turns.get_mut(turn_id) {
            if !is_terminal_turn_status(turn.status.as_str()) {
                turn.status = "failed".to_string();
                turn.completed_at = Some(now_ms());
                turn.error = Some(serde_json::json!({ "message": error.to_string() }));
                self.store.upsert_turn(turn)?;
            }
        }
        drop(turns);

        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            if session.active_turn_id.as_deref() == Some(turn_id) {
                session.active_turn_id = None;
            }
            session.status = "failed".to_string();
            session.failure_code = Some("provider_wait_failure".to_string());
            session.failure_message = Some(error.to_string());
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }
        drop(sessions);

        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id),
                "provider.error",
                RuntimeEventCriticality::Critical,
                serde_json::json!({ "error": error.to_string() }),
            )
            .await?;
        Ok(())
    }

    async fn append_event(
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

    fn allocate_id(&self, prefix: &str, suffix: &str) -> String {
        let seq = self.next_id.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{suffix}_{}_{}", now_ms(), seq)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or(0)
}

fn is_terminal_turn_status(status: &str) -> bool {
    matches!(status, "completed" | "interrupted" | "failed")
}

fn extract_turn_user_text(input: Option<&Vec<Value>>) -> Option<String> {
    let input = input?;
    let mut lines = Vec::new();
    for item in input {
        if let Some(text) = item
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            lines.push(text.to_string());
            continue;
        }
        if let Some(raw) = item
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            lines.push(raw.to_string());
        }
    }
    if lines.is_empty() {
        return None;
    }
    Some(lines.join("\n\n"))
}

fn extract_assistant_text_from_usage(usage: &Value) -> Option<String> {
    usage
        .get("last_message")
        .or_else(|| usage.get("lastMessage"))
        .or_else(|| usage.get("assistant_text"))
        .or_else(|| usage.get("assistantText"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn append_session_transcript(metadata: &mut Value, role: &str, text: &str) {
    if !metadata.is_object() {
        *metadata = Value::Object(serde_json::Map::new());
    }
    let metadata_object = match metadata {
        Value::Object(object) => object,
        _ => return,
    };
    if !metadata_object.contains_key("session_transcript") {
        if let Some(existing) = metadata_object.remove("codex_transcript") {
            metadata_object.insert("session_transcript".to_string(), existing);
        }
    }
    let entry = metadata_object
        .entry("session_transcript")
        .or_insert_with(|| Value::Array(Vec::new()));
    if !entry.is_array() {
        *entry = Value::Array(Vec::new());
    }
    if let Some(rows) = entry.as_array_mut() {
        rows.push(serde_json::json!({
            "role": role,
            "text": text,
        }));
        if rows.len() > 80 {
            let to_trim = rows.len() - 80;
            rows.drain(0..to_trim);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use tokio::time::{sleep, Duration};

    use crate::{
        ManagedWorktreeClaimRecord, ManagedWorktreeRecord, ProcessRecord, ProviderMetadata,
        ProviderModel, ProviderSession, ProviderTurnAck, RuntimeProvider, TeamDeliveryRecord,
        TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord,
        TeamOperationJournalRecord, TeamRecord,
    };

    #[derive(Default)]
    struct MockStore {
        sessions: Mutex<HashMap<String, SessionRecord>>,
        turns: Mutex<HashMap<String, TurnRecord>>,
        approvals: Mutex<HashMap<String, ApprovalRecord>>,
        events: Mutex<Vec<RuntimeEventRecord>>,
    }

    #[async_trait]
    impl RuntimeStore for MockStore {
        async fn initialize(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn append_runtime_event(
            &self,
            event: &NewRuntimeEvent,
        ) -> Result<RuntimeEventRecord, RuntimeError> {
            let mut events = self.events.lock().expect("events mutex");
            let next_seq = events
                .iter()
                .filter(|row| row.scope == event.scope && row.scope_id == event.scope_id)
                .map(|row| row.seq)
                .max()
                .unwrap_or(0)
                + 1;
            let row_id = i64::try_from(events.len()).unwrap_or(0) + 1;
            let record = RuntimeEventRecord {
                row_id,
                event_id: event.event_id.clone(),
                scope: event.scope,
                scope_id: event.scope_id.clone(),
                session_id: event.session_id.clone(),
                team_id: event.team_id.clone(),
                turn_id: event.turn_id.clone(),
                seq: next_seq,
                kind: event.kind.clone(),
                criticality: event.criticality,
                payload: event.payload.clone(),
                provider: event.provider.clone(),
                provider_seq: event.provider_seq,
                created_at: event.created_at,
            };
            events.push(record.clone());
            Ok(record)
        }

        fn list_runtime_events(
            &self,
            scope: Option<(RuntimeEventScope, &str)>,
            after_seq: Option<i64>,
            limit: usize,
        ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
            let events = self.events.lock().expect("events mutex");
            let mut rows = events.clone();
            if let Some((scope, scope_id)) = scope {
                rows.retain(|row| row.scope == scope && row.scope_id == scope_id);
                if let Some(after_seq) = after_seq {
                    rows.retain(|row| row.seq > after_seq);
                }
            }
            rows.truncate(limit);
            Ok(rows)
        }

        fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
            self.sessions
                .lock()
                .expect("sessions mutex")
                .insert(record.id.clone(), record.clone());
            Ok(())
        }

        fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError> {
            self.turns
                .lock()
                .expect("turns mutex")
                .insert(record.id.clone(), record.clone());
            Ok(())
        }

        fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError> {
            self.approvals
                .lock()
                .expect("approvals mutex")
                .insert(record.id.clone(), record.clone());
            Ok(())
        }

        fn upsert_team(&self, _record: &TeamRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team_member(&self, _record: &TeamMemberRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn delete_team_member(&self, _team_id: &str, _agent_id: &str) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team_message(&self, _record: &TeamMessageRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team_delivery(&self, _record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_managed_worktree(
            &self,
            _record: &ManagedWorktreeRecord,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_managed_worktree_claim(
            &self,
            _record: &ManagedWorktreeClaimRecord,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_process(&self, _record: &ProcessRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team_operation_journal(
            &self,
            _record: &TeamOperationJournalRecord,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn append_team_operation_diagnostic(
            &self,
            _operation_id: Option<&str>,
            _team_id: Option<&str>,
            _code: &str,
            _message: &str,
            _payload: &Value,
            _created_at: i64,
        ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
            Ok(TeamOperationDiagnosticRecord {
                id: 1,
                operation_id: None,
                team_id: None,
                code: "stub".to_string(),
                message: "stub".to_string(),
                payload: serde_json::json!({}),
                created_at: 0,
            })
        }

        fn list_team_operation_journal(
            &self,
            _team_id: Option<&str>,
        ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
            Ok(Vec::new())
        }

        fn list_team_operation_diagnostics(
            &self,
            _team_id: Option<&str>,
            _operation_id: Option<&str>,
        ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
            Ok(Vec::new())
        }

        fn hydrate_runtime_state(&self) -> Result<crate::RuntimeHydratedState, RuntimeError> {
            let sessions = self
                .sessions
                .lock()
                .expect("sessions mutex")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            let turns = self
                .turns
                .lock()
                .expect("turns mutex")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            let approvals = self
                .approvals
                .lock()
                .expect("approvals mutex")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            Ok(crate::RuntimeHydratedState {
                sessions,
                turns,
                approvals,
                ..Default::default()
            })
        }
    }

    struct MockProvider {
        wait_delay_ms: u64,
        fail_send: bool,
    }

    #[async_trait]
    impl RuntimeProvider for MockProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Codex
        }

        fn metadata(&self) -> ProviderMetadata {
            ProviderMetadata {
                kind: ProviderKind::Codex,
                display_name: "Mock Codex".to_string(),
                enabled: true,
            }
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
            Ok(vec![ProviderModel {
                id: "mock".to_string(),
                display_name: "Mock".to_string(),
            }])
        }

        async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
            Ok(ProviderAuthStatus {
                authenticated: true,
                mode: Some("mock".to_string()),
                detail: None,
            })
        }

        async fn create_session(
            &self,
            req: ProviderCreateSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id.clone(),
                provider_session_ref: format!("mock:{}", req.runtime_session_id),
                canonical_provider_session_ref: None,
            })
        }

        async fn send_turn(
            &self,
            req: ProviderSendTurnRequest,
        ) -> Result<ProviderTurnAck, RuntimeError> {
            if self.fail_send {
                return Err(RuntimeError::Io("mock send failure".to_string()));
            }
            Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
            })
        }

        async fn resume_session(
            &self,
            req: ProviderResumeSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id,
                provider_session_ref: req.provider_session_ref,
                canonical_provider_session_ref: req.canonical_provider_session_ref,
            })
        }

        async fn respond_approval(
            &self,
            _req: ProviderApprovalResponseRequest,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn wait_for_turn(
            &self,
            req: ProviderWaitTurnRequest,
        ) -> Result<ProviderTurnResult, RuntimeError> {
            if self.wait_delay_ms > 0 {
                sleep(Duration::from_millis(self.wait_delay_ms)).await;
            }
            Ok(ProviderTurnResult {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
                status: ProviderTurnStatus::Completed,
                usage: None,
                error: None,
            })
        }
    }

    fn manager_with_provider(wait_delay_ms: u64) -> Arc<RuntimeSessionManager> {
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(MockProvider {
                wait_delay_ms,
                fail_send: false,
            }))
            .expect("register provider");
        let store = Arc::new(MockStore::default());
        Arc::new(
            RuntimeSessionManager::new(store, Arc::new(registry), 256)
                .expect("build runtime manager"),
        )
    }

    fn manager_with_provider_and_store(
        wait_delay_ms: u64,
    ) -> (Arc<RuntimeSessionManager>, Arc<MockStore>) {
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(MockProvider {
                wait_delay_ms,
                fail_send: false,
            }))
            .expect("register provider");
        let store = Arc::new(MockStore::default());
        let manager = Arc::new(
            RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256)
                .expect("build runtime manager"),
        );
        (manager, store)
    }

    fn manager_with_failing_send_provider() -> Arc<RuntimeSessionManager> {
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(MockProvider {
                wait_delay_ms: 0,
                fail_send: true,
            }))
            .expect("register provider");
        let store = Arc::new(MockStore::default());
        Arc::new(
            RuntimeSessionManager::new(store, Arc::new(registry), 256)
                .expect("build runtime manager"),
        )
    }

    #[derive(Default)]
    struct PermissionCaptureProvider {
        sent_permission_modes: Arc<Mutex<Vec<Option<String>>>>,
    }

    #[async_trait]
    impl RuntimeProvider for PermissionCaptureProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Codex
        }

        fn metadata(&self) -> ProviderMetadata {
            ProviderMetadata {
                kind: ProviderKind::Codex,
                display_name: "Permission Capture".to_string(),
                enabled: true,
            }
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn create_session(
            &self,
            req: ProviderCreateSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id.clone(),
                provider_session_ref: format!("capture:{}", req.runtime_session_id),
                canonical_provider_session_ref: None,
            })
        }

        async fn send_turn(
            &self,
            req: ProviderSendTurnRequest,
        ) -> Result<ProviderTurnAck, RuntimeError> {
            self.sent_permission_modes
                .lock()
                .expect("permission capture mutex")
                .push(req.permission_mode.clone());
            Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
            })
        }

        async fn wait_for_turn(
            &self,
            req: ProviderWaitTurnRequest,
        ) -> Result<ProviderTurnResult, RuntimeError> {
            Ok(ProviderTurnResult {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
                status: ProviderTurnStatus::Completed,
                usage: None,
                error: None,
            })
        }
    }

    fn manager_with_permission_capture_provider(
    ) -> (Arc<RuntimeSessionManager>, Arc<Mutex<Vec<Option<String>>>>) {
        let provider = PermissionCaptureProvider::default();
        let captured = Arc::clone(&provider.sent_permission_modes);
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(provider))
            .expect("register provider");
        let store = Arc::new(MockStore::default());
        let manager = Arc::new(
            RuntimeSessionManager::new(store, Arc::new(registry), 256)
                .expect("build runtime manager"),
        );
        (manager, captured)
    }

    #[test]
    fn assistant_text_extraction_supports_snake_and_camel_fields() {
        let usage_snake = serde_json::json!({ "last_message": "snake" });
        let usage_camel = serde_json::json!({ "lastMessage": "camel" });
        let usage_assistant_text = serde_json::json!({ "assistant_text": "provider-neutral" });
        assert_eq!(
            extract_assistant_text_from_usage(&usage_snake).as_deref(),
            Some("snake")
        );
        assert_eq!(
            extract_assistant_text_from_usage(&usage_camel).as_deref(),
            Some("camel")
        );
        assert_eq!(
            extract_assistant_text_from_usage(&usage_assistant_text).as_deref(),
            Some("provider-neutral")
        );
    }

    #[test]
    fn append_session_transcript_migrates_legacy_key() {
        let mut metadata = serde_json::json!({
            "codex_transcript": [{"role":"assistant","text":"old"}]
        });
        append_session_transcript(&mut metadata, "assistant", "new");
        let rows = metadata
            .get("session_transcript")
            .and_then(Value::as_array)
            .expect("session transcript rows");
        assert_eq!(rows.len(), 2);
        assert!(metadata.get("codex_transcript").is_none());
    }

    #[tokio::test]
    async fn one_active_turn_per_session_is_enforced() {
        let manager = manager_with_provider(200);
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");

        let _ = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"first"})],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await
            .expect("first turn");

        let second = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"second"})],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await;
        assert!(matches!(second, Err(RuntimeError::InvalidState(_))));
    }

    #[tokio::test]
    async fn duplicate_terminal_event_is_idempotent_and_conflict_fails_closed() {
        let manager = manager_with_provider(0);
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");
        let accepted = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"hello"})],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await
            .expect("send turn");

        // Let spawned wait complete first.
        sleep(Duration::from_millis(20)).await;

        manager
            .apply_terminal_result(ProviderTurnResult {
                runtime_session_id: session.id.clone(),
                turn_id: accepted.turn_id.clone(),
                status: ProviderTurnStatus::Completed,
                usage: None,
                error: None,
            })
            .await
            .expect("idempotent duplicate terminal");

        let conflict = manager
            .apply_terminal_result(ProviderTurnResult {
                runtime_session_id: session.id.clone(),
                turn_id: accepted.turn_id.clone(),
                status: ProviderTurnStatus::Failed,
                usage: None,
                error: None,
            })
            .await;
        assert!(matches!(conflict, Err(RuntimeError::ProtocolViolation(_))));

        let updated = manager
            .get_session(session.id.as_str())
            .await
            .expect("session");
        assert_eq!(updated.status, "failed");
        assert_eq!(updated.failure_code.as_deref(), Some("terminal_conflict"));
    }

    #[tokio::test]
    async fn provider_turn_ownership_mismatch_is_rejected() {
        let manager = manager_with_provider(200);
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");
        let accepted = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"hello"})],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await
            .expect("send turn");

        let mismatched = manager
            .apply_terminal_result(ProviderTurnResult {
                runtime_session_id: "sess_other".to_string(),
                turn_id: accepted.turn_id,
                status: ProviderTurnStatus::Completed,
                usage: None,
                error: None,
            })
            .await;
        assert!(matches!(
            mismatched,
            Err(RuntimeError::ProtocolViolation(_))
        ));
    }

    #[tokio::test]
    async fn send_turn_failure_does_not_leave_session_bricked() {
        let manager = manager_with_failing_send_provider();
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");

        let send = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"hello"})],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await;
        assert!(matches!(send, Err(RuntimeError::Io(_))));

        let updated = manager
            .get_session(session.id.as_str())
            .await
            .expect("session");
        assert_eq!(updated.active_turn_id, None);
        assert_eq!(updated.status, "ready");

        // A follow-up send is still allowed to proceed to provider dispatch path.
        let second = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"again"})],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await;
        assert!(matches!(second, Err(RuntimeError::Io(_))));
    }

    #[tokio::test]
    async fn send_turn_inherits_session_permission_mode_when_turn_omits_it() {
        let (manager, captured_permission_modes) = manager_with_permission_capture_provider();
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: Some("full_auto".to_string()),
                metadata: None,
            })
            .await
            .expect("create session");

        let _ = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"inherit mode"})],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await
            .expect("send turn");

        let captured = captured_permission_modes
            .lock()
            .expect("captured permission modes")
            .clone();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].as_deref(), Some("full_auto"));
    }

    #[tokio::test]
    async fn approval_requested_and_resolution_transitions_turn() {
        let manager = manager_with_provider(0);
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");

        let accepted = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
                    expected_turn_id: None,
                    permission_mode: Some("require_approval".to_string()),
                },
            )
            .await
            .expect("accepted waiting approval");
        assert_eq!(accepted.status, "waiting_for_approval");

        let events = manager
            .replay_session_events(session.id.as_str(), None, 50)
            .expect("events");
        let approval_event = events
            .iter()
            .find(|event| event.kind == "approval.requested")
            .expect("approval requested event");
        let approval_id = approval_event
            .payload
            .get("approval_id")
            .and_then(Value::as_str)
            .expect("approval id payload")
            .to_string();
        {
            let approvals = manager.approvals.read().await;
            let persisted = approvals
                .get(approval_id.as_str())
                .expect("pending approval");
            assert_eq!(persisted.status, "pending");
        }

        let resolved = manager
            .respond_approval(
                session.id.as_str(),
                approval_id.as_str(),
                ApprovalResponseInput {
                    decision: "decline".to_string(),
                    payload: Some(serde_json::json!({"reason":"not now"})),
                },
            )
            .await
            .expect("resolve approval");
        assert_eq!(resolved.status, "decline");
        {
            let approvals = manager.approvals.read().await;
            let persisted = approvals
                .get(approval_id.as_str())
                .expect("resolved approval");
            assert_eq!(persisted.status, "decline");
            assert!(persisted.resolved_at.is_some());
        }

        let updated = manager
            .get_session(session.id.as_str())
            .await
            .expect("session");
        assert_eq!(updated.active_turn_id, None);
        assert_eq!(updated.status, "ready");
    }

    #[tokio::test]
    async fn approval_accept_is_case_insensitive_and_advances_turn() {
        let manager = manager_with_provider(0);
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");

        let accepted = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
                    expected_turn_id: None,
                    permission_mode: Some("require_approval".to_string()),
                },
            )
            .await
            .expect("accepted waiting approval");
        assert_eq!(accepted.status, "waiting_for_approval");

        let events = manager
            .replay_session_events(session.id.as_str(), None, 50)
            .expect("events");
        let approval_id = events
            .iter()
            .find(|event| event.kind == "approval.requested")
            .and_then(|event| event.payload.get("approval_id"))
            .and_then(Value::as_str)
            .expect("approval id")
            .to_string();

        let resolved = manager
            .respond_approval(
                session.id.as_str(),
                approval_id.as_str(),
                ApprovalResponseInput {
                    decision: "Accept".to_string(),
                    payload: None,
                },
            )
            .await
            .expect("resolve approval");
        assert_eq!(resolved.status, "accept");

        sleep(Duration::from_millis(20)).await;
        let updated = manager
            .get_session(session.id.as_str())
            .await
            .expect("session");
        assert_eq!(updated.active_turn_id, None);
        assert_eq!(updated.status, "ready");
    }

    #[tokio::test]
    async fn approval_invalid_decision_is_rejected() {
        let manager = manager_with_provider(0);
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");

        let _ = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
                    expected_turn_id: None,
                    permission_mode: Some("require_approval".to_string()),
                },
            )
            .await
            .expect("accepted waiting approval");

        let events = manager
            .replay_session_events(session.id.as_str(), None, 50)
            .expect("events");
        let approval_id = events
            .iter()
            .find(|event| event.kind == "approval.requested")
            .and_then(|event| event.payload.get("approval_id"))
            .and_then(Value::as_str)
            .expect("approval id")
            .to_string();

        let result = manager
            .respond_approval(
                session.id.as_str(),
                approval_id.as_str(),
                ApprovalResponseInput {
                    decision: "maybe".to_string(),
                    payload: None,
                },
            )
            .await;
        assert!(matches!(result, Err(RuntimeError::InvalidState(_))));

        let approvals = manager.approvals.read().await;
        let persisted = approvals
            .get(approval_id.as_str())
            .expect("pending approval still stored");
        assert_eq!(persisted.status, "pending");
        assert!(persisted.resolved_at.is_none());
    }

    #[tokio::test]
    async fn explicit_resume_path_updates_session_and_emits_event() {
        let manager = manager_with_provider(0);
        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: None,
                cwd: Some("/tmp".to_string()),
                permission_mode: None,
                metadata: Some(serde_json::json!({"a":1})),
            })
            .await
            .expect("create session");

        let resumed = manager
            .resume_session(
                session.id.as_str(),
                ResumeSessionInput {
                    provider_session_ref: None,
                    canonical_provider_session_ref: None,
                },
            )
            .await
            .expect("resume session");
        assert_eq!(resumed.status, "ready");

        let events = manager
            .replay_session_events(session.id.as_str(), None, 20)
            .expect("events");
        assert!(
            events.iter().any(|event| event.kind == "session.resumed"),
            "session.resumed event missing"
        );
    }

    #[tokio::test]
    async fn startup_recovery_clears_stale_active_turn_and_orphaned_pending_approval() {
        let (manager, store) = manager_with_provider_and_store(0);
        let now = now_ms();
        store
            .upsert_session(&SessionRecord {
                id: "sess_recover".to_string(),
                provider: "codex".to_string(),
                status: "turn_running".to_string(),
                cwd: None,
                model: None,
                permission_mode: None,
                system_prompt: None,
                metadata: serde_json::json!({}),
                provider_session_ref: Some("provider_ref_1".to_string()),
                canonical_provider_session_ref: None,
                active_turn_id: Some("turn_missing".to_string()),
                worktree_id: None,
                created_at: now,
                updated_at: now,
                closed_at: None,
                failure_code: None,
                failure_message: None,
            })
            .expect("seed session");
        store
            .upsert_approval(&ApprovalRecord {
                id: "apr_orphan".to_string(),
                session_id: "sess_recover".to_string(),
                turn_id: "turn_missing".to_string(),
                tool_call_id: None,
                provider_approval_ref: None,
                status: "pending".to_string(),
                request: serde_json::json!({"reason":"manual"}),
                response: None,
                created_at: now,
                resolved_at: None,
            })
            .expect("seed approval");
        {
            let mut sessions = manager.sessions.write().await;
            sessions.insert(
                "sess_recover".to_string(),
                store
                    .sessions
                    .lock()
                    .expect("sessions")
                    .get("sess_recover")
                    .cloned()
                    .expect("session seeded"),
            );
        }
        {
            let mut approvals = manager.approvals.write().await;
            approvals.insert(
                "apr_orphan".to_string(),
                store
                    .approvals
                    .lock()
                    .expect("approvals")
                    .get("apr_orphan")
                    .cloned()
                    .expect("approval seeded"),
            );
        }

        let summary = manager.recover_startup().await.expect("startup recovery");
        assert!(summary.sessions_reconciled >= 1);
        assert!(summary.approvals_reconciled >= 1);
        let repaired = manager
            .get_session("sess_recover")
            .await
            .expect("repaired session");
        assert_eq!(repaired.active_turn_id, None);
        assert_eq!(repaired.status, "ready");
        let approvals = manager.approvals.read().await;
        let approval = approvals.get("apr_orphan").expect("approval present");
        assert_eq!(approval.status, "decline");
        assert!(approval.resolved_at.is_some());
    }

    #[tokio::test]
    async fn startup_recovery_preserves_waiting_for_approval_turn_without_spawning_wait() {
        let (manager, store) = manager_with_provider_and_store(0);
        let now = now_ms();
        store
            .upsert_session(&SessionRecord {
                id: "sess_pending".to_string(),
                provider: "codex".to_string(),
                status: "waiting_for_approval".to_string(),
                cwd: None,
                model: None,
                permission_mode: Some("require_approval".to_string()),
                system_prompt: None,
                metadata: serde_json::json!({}),
                provider_session_ref: Some("provider_ref_pending".to_string()),
                canonical_provider_session_ref: None,
                active_turn_id: Some("turn_pending".to_string()),
                worktree_id: None,
                created_at: now,
                updated_at: now,
                closed_at: None,
                failure_code: None,
                failure_message: None,
            })
            .expect("seed waiting session");
        store
            .upsert_turn(&TurnRecord {
                id: "turn_pending".to_string(),
                session_id: "sess_pending".to_string(),
                provider_turn_ref: None,
                status: "waiting_for_approval".to_string(),
                input: serde_json::json!([{ "type": "text", "text": "needs approval" }]),
                source: Some("user".to_string()),
                started_at: Some(now),
                completed_at: None,
                usage: None,
                error: None,
            })
            .expect("seed waiting turn");
        store
            .upsert_approval(&ApprovalRecord {
                id: "apr_pending".to_string(),
                session_id: "sess_pending".to_string(),
                turn_id: "turn_pending".to_string(),
                tool_call_id: None,
                provider_approval_ref: None,
                status: "pending".to_string(),
                request: serde_json::json!({"reason":"manual"}),
                response: None,
                created_at: now,
                resolved_at: None,
            })
            .expect("seed pending approval");
        {
            let mut sessions = manager.sessions.write().await;
            sessions.insert(
                "sess_pending".to_string(),
                store
                    .sessions
                    .lock()
                    .expect("sessions")
                    .get("sess_pending")
                    .cloned()
                    .expect("session seeded"),
            );
        }
        {
            let mut turns = manager.turns.write().await;
            turns.insert(
                "turn_pending".to_string(),
                store
                    .turns
                    .lock()
                    .expect("turns")
                    .get("turn_pending")
                    .cloned()
                    .expect("turn seeded"),
            );
        }
        {
            let mut approvals = manager.approvals.write().await;
            approvals.insert(
                "apr_pending".to_string(),
                store
                    .approvals
                    .lock()
                    .expect("approvals")
                    .get("apr_pending")
                    .cloned()
                    .expect("approval seeded"),
            );
        }

        let summary = manager.recover_startup().await.expect("startup recovery");
        assert_eq!(summary.resumed_waits, 0);
        let repaired = manager
            .get_session("sess_pending")
            .await
            .expect("repaired session");
        assert_eq!(repaired.status, "waiting_for_approval");
        assert_eq!(repaired.active_turn_id.as_deref(), Some("turn_pending"));

        let approvals = manager.approvals.read().await;
        let approval = approvals.get("apr_pending").expect("approval present");
        assert_eq!(approval.status, "pending");
        assert!(approval.resolved_at.is_none());

        let events = store.events.lock().expect("events lock").clone();
        assert!(
            !events.iter().any(|event| {
                event.session_id.as_deref() == Some("sess_pending")
                    && matches!(
                        event.kind.as_str(),
                        "turn.completed" | "turn.interrupted" | "turn.failed" | "provider.error"
                    )
            }),
            "startup recovery must not terminally reconcile waiting-for-approval turns"
        );
    }
}
