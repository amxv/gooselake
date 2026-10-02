use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};

use crate::{
    ApprovalRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent,
    ProcessRecord, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCreateSessionRequest, ProviderKind, ProviderMetadata, ProviderModel, ProviderRegistry,
    ProviderResumeSessionRequest, ProviderRuntimeEvent, ProviderSendTurnRequest, ProviderSession,
    ProviderTurnAck, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError,
    RuntimeEventRecord, RuntimeEventScope, RuntimeProvider, RuntimeStore, SessionRecord,
    TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord,
    TeamOperationJournalRecord, TeamRecord, TurnAdmissionRecord, TurnRecord,
};

use super::RuntimeSessionManager;

#[derive(Default)]
pub(super) struct MockStore {
    pub(super) sessions: Mutex<HashMap<String, SessionRecord>>,
    pub(super) turns: Mutex<HashMap<String, TurnRecord>>,
    pub(super) approvals: Mutex<HashMap<String, ApprovalRecord>>,
    pub(super) turn_admissions: Mutex<HashMap<String, TurnAdmissionRecord>>,
    pub(super) events: Mutex<Vec<RuntimeEventRecord>>,
    pub(super) fail_admission: Mutex<bool>,
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

    fn admit_turn(
        &self,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        if *self.fail_admission.lock().expect("fail admission mutex") {
            return Err(RuntimeError::Io("forced admission failure".to_string()));
        }
        let mut sessions = self.sessions.lock().expect("sessions mutex");
        let current = sessions
            .get(&session.id)
            .ok_or_else(|| RuntimeError::NotFound(format!("session {}", session.id)))?;
        if current.active_turn_id.is_some() {
            return Err(RuntimeError::Conflict(format!(
                "session {} already has an active turn",
                session.id
            )));
        }
        self.turns
            .lock()
            .expect("turns mutex")
            .insert(turn.id.clone(), turn.clone());
        if let Some(approval) = approval {
            self.approvals
                .lock()
                .expect("approvals mutex")
                .insert(approval.id.clone(), approval.clone());
        }
        self.turn_admissions
            .lock()
            .expect("turn admissions mutex")
            .insert(admission.turn_id.clone(), admission.clone());
        sessions.insert(session.id.clone(), session.clone());
        Ok(())
    }

    fn upsert_turn_admission(&self, record: &TurnAdmissionRecord) -> Result<(), RuntimeError> {
        self.turn_admissions
            .lock()
            .expect("turn admissions mutex")
            .insert(record.turn_id.clone(), record.clone());
        Ok(())
    }

    fn list_turn_admissions(&self) -> Result<Vec<TurnAdmissionRecord>, RuntimeError> {
        Ok(self
            .turn_admissions
            .lock()
            .expect("turn admissions mutex")
            .values()
            .cloned()
            .collect())
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

    fn upsert_managed_worktree(&self, _record: &ManagedWorktreeRecord) -> Result<(), RuntimeError> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MockSendBehavior {
    Success,
    NotDispatched,
    Unknown,
}

pub(super) struct MockProvider {
    wait_delay_ms: u64,
    send_behavior: MockSendBehavior,
    send_count: Arc<AtomicUsize>,
    admission_probe: Option<Arc<MockStore>>,
    provider_native_turn_id: Option<String>,
    restored_turn_mappings: Arc<Mutex<Vec<(String, String, String)>>>,
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
            reasoning_levels: Vec::new(),
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
        self.send_count.fetch_add(1, Ordering::SeqCst);
        if let Some(store) = self.admission_probe.as_ref() {
            let has_admission = store
                .turn_admissions
                .lock()
                .expect("turn admissions mutex")
                .contains_key(req.turn_id.as_str());
            if !has_admission {
                return Err(RuntimeError::ProtocolViolation(
                    "provider observed turn before durable admission".to_string(),
                ));
            }
        }
        if self.send_behavior == MockSendBehavior::NotDispatched {
            return Err(RuntimeError::provider_not_dispatched(
                "mock_send_rejected",
                "mock send failure",
            ));
        }
        if self.send_behavior == MockSendBehavior::Unknown {
            return Err(RuntimeError::Io("mock ambiguous send failure".to_string()));
        }
        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            provider_native_turn_id: self.provider_native_turn_id.clone(),
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

    async fn restore_turn_identity_mapping(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        provider_native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        self.restored_turn_mappings
            .lock()
            .expect("restored turn mappings mutex")
            .push((
                runtime_session_id.to_string(),
                turn_id.to_string(),
                provider_native_turn_id.to_string(),
            ));
        Ok(())
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

pub(super) fn manager_with_provider(wait_delay_ms: u64) -> Arc<RuntimeSessionManager> {
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(MockProvider {
            wait_delay_ms,
            send_behavior: MockSendBehavior::Success,
            send_count: Arc::new(AtomicUsize::new(0)),
            admission_probe: None,
            provider_native_turn_id: None,
            restored_turn_mappings: Arc::new(Mutex::new(Vec::new())),
        }))
        .expect("register provider");
    let store = Arc::new(MockStore::default());
    Arc::new(
        RuntimeSessionManager::new(store, Arc::new(registry), 256).expect("build runtime manager"),
    )
}

pub(super) fn manager_with_provider_and_store(
    wait_delay_ms: u64,
) -> (Arc<RuntimeSessionManager>, Arc<MockStore>) {
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(MockProvider {
            wait_delay_ms,
            send_behavior: MockSendBehavior::Success,
            send_count: Arc::new(AtomicUsize::new(0)),
            admission_probe: None,
            provider_native_turn_id: None,
            restored_turn_mappings: Arc::new(Mutex::new(Vec::new())),
        }))
        .expect("register provider");
    let store = Arc::new(MockStore::default());
    let manager = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256)
            .expect("build runtime manager"),
    );
    (manager, store)
}

pub(super) fn manager_with_failing_send_provider() -> Arc<RuntimeSessionManager> {
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(MockProvider {
            wait_delay_ms: 0,
            send_behavior: MockSendBehavior::NotDispatched,
            send_count: Arc::new(AtomicUsize::new(0)),
            admission_probe: None,
            provider_native_turn_id: None,
            restored_turn_mappings: Arc::new(Mutex::new(Vec::new())),
        }))
        .expect("register provider");
    let store = Arc::new(MockStore::default());
    Arc::new(
        RuntimeSessionManager::new(store, Arc::new(registry), 256).expect("build runtime manager"),
    )
}

pub(super) fn manager_with_observable_send_provider(
    behavior: MockSendBehavior,
    provider_native_turn_id: Option<String>,
) -> (Arc<RuntimeSessionManager>, Arc<MockStore>, Arc<AtomicUsize>) {
    let store = Arc::new(MockStore::default());
    let (manager, send_count) =
        observable_manager_for_store(store.clone(), behavior, provider_native_turn_id);
    (manager, store, send_count)
}

pub(super) fn observable_manager_for_store(
    store: Arc<MockStore>,
    behavior: MockSendBehavior,
    provider_native_turn_id: Option<String>,
) -> (Arc<RuntimeSessionManager>, Arc<AtomicUsize>) {
    let (manager, send_count, _) =
        observable_manager_for_store_with_restore_probe(store, behavior, provider_native_turn_id);
    (manager, send_count)
}

pub(super) fn observable_manager_for_store_with_restore_probe(
    store: Arc<MockStore>,
    behavior: MockSendBehavior,
    provider_native_turn_id: Option<String>,
) -> (
    Arc<RuntimeSessionManager>,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<(String, String, String)>>>,
) {
    let send_count = Arc::new(AtomicUsize::new(0));
    let restored_turn_mappings = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(MockProvider {
            wait_delay_ms: 200,
            send_behavior: behavior,
            send_count: send_count.clone(),
            admission_probe: Some(store.clone()),
            provider_native_turn_id,
            restored_turn_mappings: restored_turn_mappings.clone(),
        }))
        .expect("register provider");
    let manager = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256)
            .expect("build runtime manager"),
    );
    (manager, send_count, restored_turn_mappings)
}

pub(super) struct ProviderApprovalEventProvider {
    events: broadcast::Sender<ProviderRuntimeEvent>,
    wait_count: Arc<AtomicUsize>,
}

#[async_trait]
impl RuntimeProvider for ProviderApprovalEventProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "Approval Event Provider".to_string(),
            enabled: true,
        }
    }

    fn subscribe_events(&self) -> Option<broadcast::Receiver<ProviderRuntimeEvent>> {
        Some(self.events.subscribe())
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
            provider_session_ref: format!("approval-events:{}", req.runtime_session_id),
            canonical_provider_session_ref: None,
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let event = ProviderRuntimeEvent::ApprovalRequested {
            runtime_session_id: req.runtime_session_id.clone(),
            turn_id: req.turn_id.clone(),
            provider_approval_ref: "provider-approval-before-ack".to_string(),
            tool_call_id: Some("tool-call-1".to_string()),
            request: serde_json::json!({
                "requestType": "tool",
                "content": {"toolName": "write_file"},
            }),
        };
        let _ = self.events.send(event.clone());
        let _ = self.events.send(event);
        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            provider_native_turn_id: Some("native-approval-turn".to_string()),
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
        self.wait_count.fetch_add(1, Ordering::SeqCst);
        sleep(Duration::from_millis(150)).await;
        Ok(ProviderTurnResult {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            status: ProviderTurnStatus::Completed,
            usage: None,
            error: None,
        })
    }
}

pub(super) fn manager_with_provider_approval_events(
) -> (Arc<RuntimeSessionManager>, Arc<MockStore>, Arc<AtomicUsize>) {
    let (events, _) = broadcast::channel(16);
    let wait_count = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(ProviderApprovalEventProvider {
            events,
            wait_count: wait_count.clone(),
        }))
        .expect("register provider");
    let store = Arc::new(MockStore::default());
    let manager = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256)
            .expect("build runtime manager"),
    );
    (manager, store, wait_count)
}

#[derive(Default)]
pub(super) struct PermissionCaptureProvider {
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
            provider_native_turn_id: None,
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

pub(super) fn manager_with_permission_capture_provider(
) -> (Arc<RuntimeSessionManager>, Arc<Mutex<Vec<Option<String>>>>) {
    let provider = PermissionCaptureProvider::default();
    let captured = Arc::clone(&provider.sent_permission_modes);
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(provider))
        .expect("register provider");
    let store = Arc::new(MockStore::default());
    let manager = Arc::new(
        RuntimeSessionManager::new(store, Arc::new(registry), 256).expect("build runtime manager"),
    );
    (manager, captured)
}
