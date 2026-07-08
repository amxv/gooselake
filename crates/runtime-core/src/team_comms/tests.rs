use super::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::{
    ApprovalRecord, CreateSessionInput, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
    NewRuntimeEvent, ProcessRecord, ProviderAuthStatus, ProviderCreateSessionRequest,
    ProviderInterruptTurnRequest, ProviderKind, ProviderMetadata, ProviderModel, ProviderRegistry,
    ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderSession, ProviderTurnAck,
    ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError,
    RuntimeEventRecord, RuntimeEventScope, RuntimeProvider, RuntimeStore, SessionRecord,
    TeamCommsService, TeamCreateRequest, TeamDeliveryRecord, TeamGetDeliveriesRequest,
    TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord, TeamOperationJournalRecord,
    TeamRecord, TeamSendDirectRequest, TeamSetLeadRequest, TurnRecord,
};

#[derive(Default)]
struct TestStore {
    hydrated: std::sync::Mutex<crate::RuntimeHydratedState>,
    events: std::sync::Mutex<Vec<RuntimeEventRecord>>,
}

impl TestStore {
    fn upsert_with_key<T, F>(rows: &mut Vec<T>, value: T, key: F)
    where
        T: Clone,
        F: Fn(&T) -> String,
    {
        let value_key = key(&value);
        if let Some(existing) = rows.iter_mut().find(|row| key(row) == value_key) {
            *existing = value;
            return;
        }
        rows.push(value);
    }
}

#[async_trait]
impl RuntimeStore for TestStore {
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
        let mut events = self.events.lock().expect("events lock");
        if let Some(existing) = events.iter().find(|row| row.event_id == event.event_id) {
            return Ok(existing.clone());
        }
        let row_id = i64::try_from(events.len()).unwrap_or(0) + 1;
        let seq = events
            .iter()
            .filter(|row| row.scope == event.scope && row.scope_id == event.scope_id)
            .map(|row| row.seq)
            .max()
            .unwrap_or(0)
            + 1;
        let record = RuntimeEventRecord {
            row_id,
            event_id: event.event_id.clone(),
            scope: event.scope,
            scope_id: event.scope_id.clone(),
            session_id: event.session_id.clone(),
            team_id: event.team_id.clone(),
            turn_id: event.turn_id.clone(),
            seq,
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
        let events = self.events.lock().expect("events lock");
        let mut rows = events.clone();
        if let Some((scope_value, scope_id)) = scope {
            rows.retain(|row| row.scope == scope_value && row.scope_id == scope_id);
            if let Some(after) = after_seq {
                rows.retain(|row| row.seq > after);
            }
        } else if let Some(after) = after_seq {
            rows.retain(|row| row.row_id > after);
        }
        rows.sort_by_key(|row| row.row_id);
        rows.truncate(limit);
        Ok(rows)
    }

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.sessions, record.clone(), |row| row.id.clone());
        Ok(())
    }

    fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.turns, record.clone(), |row| row.id.clone());
        Ok(())
    }

    fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.approvals, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.teams, record.clone(), |row| row.id.clone());
        Ok(())
    }

    fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.team_members, record.clone(), |row| {
            format!("{}|{}", row.team_id, row.agent_id)
        });
        Ok(())
    }

    fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        hydrated
            .team_members
            .retain(|row| !(row.team_id == team_id && row.agent_id == agent_id));
        Ok(())
    }

    fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.team_messages, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.team_deliveries, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.managed_worktrees, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(
            &mut hydrated.managed_worktree_claims,
            record.clone(),
            |row| format!("{}|{}", row.worktree_id, row.session_id),
        );
        Ok(())
    }

    fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.processes, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_team_operation_journal(
        &self,
        record: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(
            &mut hydrated.team_operation_journal,
            record.clone(),
            |row| row.operation_id.clone(),
        );
        Ok(())
    }

    fn append_team_operation_diagnostic(
        &self,
        operation_id: Option<&str>,
        team_id: Option<&str>,
        code: &str,
        message: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        let id = i64::try_from(hydrated.team_operation_diagnostics.len()).unwrap_or(0) + 1;
        let record = TeamOperationDiagnosticRecord {
            id,
            operation_id: operation_id.map(str::to_string),
            team_id: team_id.map(str::to_string),
            code: code.to_string(),
            message: message.to_string(),
            payload: payload.clone(),
            created_at,
        };
        hydrated.team_operation_diagnostics.push(record.clone());
        Ok(record)
    }

    fn list_team_operation_journal(
        &self,
        team_id: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
        let hydrated = self.hydrated.lock().expect("hydrated lock");
        let mut rows = hydrated.team_operation_journal.clone();
        if let Some(team_id) = team_id {
            rows.retain(|row| row.team_id == team_id);
        }
        Ok(rows)
    }

    fn list_team_operation_diagnostics(
        &self,
        team_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
        let hydrated = self.hydrated.lock().expect("hydrated lock");
        let mut rows = hydrated.team_operation_diagnostics.clone();
        if let Some(team_id) = team_id {
            rows.retain(|row| row.team_id.as_deref() == Some(team_id));
        }
        if let Some(operation_id) = operation_id {
            rows.retain(|row| row.operation_id.as_deref() == Some(operation_id));
        }
        Ok(rows)
    }

    fn hydrate_runtime_state(&self) -> Result<crate::RuntimeHydratedState, RuntimeError> {
        Ok(self.hydrated.lock().expect("hydrated lock").clone())
    }
}

#[derive(Default)]
struct TestProviderState {
    sessions: HashMap<String, String>,
    completed: HashMap<String, ProviderTurnResult>,
}

struct TestProvider {
    wait_ms: u64,
    state: Mutex<TestProviderState>,
}

impl TestProvider {
    fn new(wait_ms: u64) -> Self {
        Self {
            wait_ms,
            state: Mutex::new(TestProviderState::default()),
        }
    }
}

#[async_trait]
impl RuntimeProvider for TestProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "Test Codex".to_string(),
            enabled: true,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(vec![ProviderModel {
            id: "test-model".to_string(),
            display_name: "Test Model".to_string(),
            reasoning_levels: vec!["test".to_string()],
        }])
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        Ok(ProviderAuthStatus {
            authenticated: true,
            mode: Some("test".to_string()),
            detail: None,
        })
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let mut state = self.state.lock().await;
        state.sessions.insert(
            req.runtime_session_id.clone(),
            format!("test-thread-{}", req.runtime_session_id),
        );
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id.clone(),
            provider_session_ref: format!("test-thread-{}", req.runtime_session_id),
            canonical_provider_session_ref: None,
        })
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let mut state = self.state.lock().await;
        state.sessions.insert(
            req.runtime_session_id.clone(),
            req.provider_session_ref.clone(),
        );
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: req.provider_session_ref,
            canonical_provider_session_ref: req.canonical_provider_session_ref,
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let mut state = self.state.lock().await;
        state.completed.insert(
            req.turn_id.clone(),
            ProviderTurnResult {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: req.turn_id.clone(),
                status: ProviderTurnStatus::Completed,
                usage: Some(serde_json::json!({ "last_message": "ok" })),
                error: None,
            },
        );
        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
        })
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        if self.wait_ms > 0 {
            sleep(Duration::from_millis(self.wait_ms)).await;
        }
        let state = self.state.lock().await;
        state
            .completed
            .get(req.turn_id.as_str())
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("test turn {}", req.turn_id)))
    }

    async fn interrupt_turn(&self, _req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        Ok(())
    }
}

fn build_runtime_and_service(
    store: Arc<TestStore>,
    wait_ms: u64,
) -> (Arc<RuntimeSessionManager>, Arc<RuntimeTeamCommsService>) {
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(TestProvider::new(wait_ms)))
        .expect("register provider");
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512).expect("build runtime"),
    );
    let team_comms = RuntimeTeamCommsService::new(
        store,
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("build team comms");
    (runtime, team_comms)
}

async fn create_test_session(runtime: &RuntimeSessionManager) -> String {
    runtime
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: Some("test-model".to_string()),
            cwd: None,
            permission_mode: None,
            metadata: Some(serde_json::json!({ "suite": "team_comms" })),
        })
        .await
        .expect("create session")
        .id
}

#[tokio::test]
async fn direct_message_image_paths_are_injected_as_image_items() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store, 0);
    let lead = create_test_session(&runtime).await;
    let member = create_test_session(&runtime).await;

    let team = service
        .create_team(TeamCreateRequest {
            name: "Image Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![member.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team");

    service
        .send_direct(TeamSendDirectRequest {
            team_id: team.team.id,
            sender_agent_id: lead,
            recipient_agent_id: member.clone(),
            input: serde_json::json!([{ "type": "text", "text": "please inspect" }]),
            image_paths: vec!["/tmp/reference.png".to_string()],
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
        })
        .await
        .expect("send direct with image");

    let turns = runtime
        .list_session_turns(member.as_str())
        .await
        .expect("member turns");
    assert!(turns
        .iter()
        .flat_map(|turn| turn.input.as_array().into_iter().flatten())
        .any(|item| {
            item.get("type").and_then(Value::as_str) == Some("image")
                && item.get("path").and_then(Value::as_str) == Some("/tmp/reference.png")
        }));
}

#[tokio::test]
async fn restart_appends_new_team_event_rows_without_event_id_collision() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 1);
    let lead = create_test_session(&runtime).await;
    let member = create_test_session(&runtime).await;

    let created = service
        .create_team(TeamCreateRequest {
            name: "Restart Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![member.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team");
    let team_id = created.team.id.clone();

    let before = service
        .replay_team_events(team_id.as_str(), None, 128)
        .expect("replay before");
    assert!(
        before.iter().any(|event| event.kind == "team.created"),
        "expected team.created before restart"
    );

    drop(service);
    drop(runtime);

    let (_runtime_after_restart, service_after_restart) =
        build_runtime_and_service(store.clone(), 1);
    service_after_restart
        .set_team_lead(TeamSetLeadRequest {
            team_id: team_id.clone(),
            lead_agent_id: member.clone(),
        })
        .await
        .expect("set team lead after restart");

    let after = service_after_restart
        .replay_team_events(team_id.as_str(), None, 256)
        .expect("replay after");
    assert!(
        after.len() > before.len(),
        "expected event stream to append after restart mutation"
    );
    assert!(
        after.iter().any(|event| event.kind == "team.lead_changed"),
        "expected team.lead_changed event to append after restart"
    );
}

#[tokio::test]
async fn startup_recovery_retries_deferred_delivery_for_ready_recipient() {
    let store = Arc::new(TestStore::default());
    let now = now_ms();

    store
        .upsert_session(&SessionRecord {
            id: "sess_lead_seed".to_string(),
            provider: "codex".to_string(),
            status: "ready".to_string(),
            cwd: None,
            model: Some("test-model".to_string()),
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("provider-lead-seed".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        })
        .expect("seed lead session");
    store
        .upsert_session(&SessionRecord {
            id: "sess_ready_seed".to_string(),
            provider: "codex".to_string(),
            status: "ready".to_string(),
            cwd: None,
            model: Some("test-model".to_string()),
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("provider-ready-seed".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        })
        .expect("seed recipient session");
    store
        .upsert_team(&TeamRecord {
            id: "team_seed".to_string(),
            name: "Seed Team".to_string(),
            lead_agent_id: "sess_lead_seed".to_string(),
            created_by: "test".to_string(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
        })
        .expect("seed team");
    store
        .upsert_team_member(&TeamMemberRecord {
            team_id: "team_seed".to_string(),
            agent_id: "sess_ready_seed".to_string(),
            title: None,
            joined_at: now,
            added_by: "test".to_string(),
            creator_agent_id: None,
            creator_compaction_subscription: "auto".to_string(),
            worktree_id: None,
        })
        .expect("seed member");
    store
        .upsert_team_message(&TeamMessageRecord {
            id: "msg_seed".to_string(),
            team_id: "team_seed".to_string(),
            scope: "direct".to_string(),
            sender_agent_id: "sess_lead_seed".to_string(),
            recipient_agent_ids: serde_json::json!(["sess_ready_seed"]),
            input: serde_json::json!([{ "type": "text", "text": "seed deferred" }]),
            image_paths: serde_json::json!([]),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("seed-idempotency".to_string()),
            created_at: now,
        })
        .expect("seed message");
    store
        .upsert_team_delivery(&TeamDeliveryRecord {
            id: "dlv_seed".to_string(),
            message_id: "msg_seed".to_string(),
            team_id: "team_seed".to_string(),
            recipient_agent_id: "sess_ready_seed".to_string(),
            provider: "codex".to_string(),
            status: DELIVERY_STATUS_DEFERRED.to_string(),
            effective_policy: Some("non_interrupting".to_string()),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: Some("seed_restart_gap".to_string()),
            last_error_message: Some("seed deferred before restart".to_string()),
            created_at: now,
            updated_at: now,
        })
        .expect("seed delivery");

    let (_runtime, service) = build_runtime_and_service(store.clone(), 0);
    let before = service
        .get_deliveries(TeamGetDeliveriesRequest {
            team_id: "team_seed".to_string(),
            message_id: Some("msg_seed".to_string()),
            recipient_agent_id: Some("sess_ready_seed".to_string()),
        })
        .await
        .expect("delivery before startup replay");
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].status, DELIVERY_STATUS_DEFERRED);

    let retried = service
        .recover_startup_deferred_deliveries()
        .await
        .expect("startup deferred recovery");
    assert!(
        retried >= 1,
        "expected startup replay to retry at least one deferred delivery"
    );

    let mut recovered_status = None;
    for _ in 0..30 {
        let rows = service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: "team_seed".to_string(),
                message_id: Some("msg_seed".to_string()),
                recipient_agent_id: Some("sess_ready_seed".to_string()),
            })
            .await
            .expect("delivery rows");
        if let Some(row) = rows.first() {
            recovered_status = Some(row.status.clone());
            if row.status != DELIVERY_STATUS_DEFERRED {
                break;
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert_ne!(
        recovered_status.as_deref(),
        Some(DELIVERY_STATUS_DEFERRED),
        "deferred delivery should not remain permanently deferred after startup recovery"
    );
}

#[tokio::test]
async fn delete_team_cancels_outstanding_delivery_and_clears_recipient_queue_blockers() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 300);
    let lead = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;

    let created = service
        .create_team(TeamCreateRequest {
            name: "Delete Queue Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team");
    let deleted_team_id = created.team.id.clone();

    let first_ack = service
        .send_direct(TeamSendDirectRequest {
            team_id: deleted_team_id.clone(),
            sender_agent_id: lead.clone(),
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "first" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("delete-q-1".to_string()),
        })
        .await
        .expect("first direct");
    assert_eq!(first_ack.deliveries.len(), 1);

    let second_ack = service
        .send_direct(TeamSendDirectRequest {
            team_id: deleted_team_id.clone(),
            sender_agent_id: lead.clone(),
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "second" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("delete-q-2".to_string()),
        })
        .await
        .expect("second direct");
    assert_eq!(second_ack.deliveries.len(), 1);
    let second_delivery_id = second_ack.deliveries[0].id.clone();

    let mut second_is_outstanding = false;
    for _ in 0..40 {
        let rows = service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: deleted_team_id.clone(),
                message_id: Some(second_ack.message.id.clone()),
                recipient_agent_id: Some(recipient.clone()),
            })
            .await
            .expect("list deliveries");
        if let Some(delivery) = rows.first() {
            if matches!(
                delivery.status.as_str(),
                DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
            ) {
                second_is_outstanding = true;
                break;
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        second_is_outstanding,
        "expected second delivery to be pending/deferred before team deletion"
    );

    service
        .delete_team(deleted_team_id.as_str())
        .await
        .expect("delete team");
    sleep(Duration::from_millis(450)).await;

    let hydrated_after_delete = store.hydrate_runtime_state().expect("hydrate");
    let deleted_delivery = hydrated_after_delete
        .team_deliveries
        .iter()
        .find(|delivery| delivery.id == second_delivery_id)
        .cloned()
        .expect("deleted team delivery row");
    assert_eq!(
        deleted_delivery.status, DELIVERY_STATUS_CANCELLED,
        "deleted team's outstanding delivery must be cancelled and must not resume/inject"
    );

    let created_second_team = service
        .create_team(TeamCreateRequest {
            name: "Live Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create second team");
    let live_team_id = created_second_team.team.id;

    let third_ack = service
        .send_direct(TeamSendDirectRequest {
            team_id: live_team_id.clone(),
            sender_agent_id: lead.clone(),
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "third" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("delete-q-3".to_string()),
        })
        .await
        .expect("third direct");
    let third_delivery_id = third_ack.deliveries[0].id.clone();

    let mut third_terminal_status = None;
    for _ in 0..80 {
        let rows = service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: live_team_id.clone(),
                message_id: Some(third_ack.message.id.clone()),
                recipient_agent_id: Some(recipient.clone()),
            })
            .await
            .expect("list third delivery");
        if let Some(delivery) = rows
            .iter()
            .find(|delivery| delivery.id == third_delivery_id)
        {
            if is_terminal_status(delivery.status.as_str()) {
                third_terminal_status = Some(delivery.status.clone());
                break;
            }
        }
        sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(
        third_terminal_status.as_deref(),
        Some(DELIVERY_STATUS_INJECTED),
        "later delivery must not be blocked/deferred by stale deleted-team queue state"
    );
}
