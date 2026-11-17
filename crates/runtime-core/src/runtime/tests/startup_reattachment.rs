use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

use super::super::test_support::MockStore;
use crate::*;

struct PausedResumeProvider {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
}

impl PausedResumeProvider {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: Notify::new(),
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl RuntimeProvider for PausedResumeProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "startup-test".into(),
            enabled: true,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn resume_session_with_policy(
        &self,
        request: ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        let canonical = request
            .canonical_provider_session_ref
            .or_else(|| Some(request.provider_session_ref.clone()));
        Ok(ProviderSession {
            runtime_session_id: request.runtime_session_id,
            provider_session_ref: request.provider_session_ref,
            canonical_provider_session_ref: canonical,
        })
    }

    async fn restore_turn_identity_mapping(
        &self,
        _runtime_session_id: &str,
        _turn_id: &str,
        _provider_native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

fn seed(state: Option<TurnDispatchState>) -> Arc<MockStore> {
    let store = Arc::new(MockStore::default());
    let mut session = SessionRecord {
        id: "startup-session".into(),
        provider: "codex".into(),
        status: "ready".into(),
        cwd: None,
        model: Some("mock".into()),
        permission_mode: None,
        system_prompt: Some("original policy".into()),
        metadata: json!({
            "policy_metadata":{"allowed_tools":["Read"]},
            "session_transcript":[{"role":"assistant","text":"earlier"}]
        }),
        provider_session_ref: Some("native-startup-session".into()),
        canonical_provider_session_ref: Some("native-startup-session".into()),
        active_turn_id: None,
        worktree_id: None,
        created_at: 10,
        updated_at: 10,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    };
    if let Some(state) = state {
        let input = vec![json!({"type":"text","text":"original user text"})];
        let turn = TurnRecord {
            id: "startup-turn".into(),
            session_id: session.id.clone(),
            provider_turn_ref: None,
            status: if state == TurnDispatchState::Dispatched {
                "in_progress"
            } else {
                "admitted"
            }
            .into(),
            input: json!(input),
            source: Some("user".into()),
            started_at: Some(11),
            completed_at: None,
            usage: None,
            error: None,
        };
        let admission = TurnAdmissionRecord {
            turn_id: turn.id.clone(),
            session_id: session.id.clone(),
            provider: session.provider.clone(),
            projection_source: TurnInputProjectionSource::UserVisible,
            user_input_snapshot: PersistedUserInputSnapshot::from_input(&input),
            dispatch_policy: TurnDispatchPolicySnapshot {
                permission_mode: None,
                pre_dispatch_approval_required: false,
                turn_settings: Default::default(),
            },
            correlation: TurnCorrelationState::default(),
            dispatch_state: state,
            provider_native_turn_id: (state == TurnDispatchState::Dispatched)
                .then(|| "native-turn".into()),
            dispatch_error: None,
            admitted_at: 11,
            updated_at: 12,
        };
        session.active_turn_id = Some(turn.id.clone());
        session.status = if state == TurnDispatchState::Dispatched {
            "turn_running"
        } else {
            "turn_admitted"
        }
        .into();
        session.updated_at = 12;
        // Single-threaded setup represents state loaded from an earlier runtime.
        store.turns.lock().unwrap().insert(turn.id.clone(), turn);
        store
            .turn_admissions
            .lock()
            .unwrap()
            .insert(admission.turn_id.clone(), admission);
    }
    store.upsert_session(&session).unwrap();
    store
}

fn command(store: &MockStore, instance: &str) -> StartupReattachCommand {
    let session = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap()
        .session;
    let (provider, request) = startup_resume_request_from_authority(&session, None).unwrap();
    StartupReattachCommand {
        operation_id: format!("startup-op-{instance}"),
        startup_instance_id: instance.into(),
        session_id: session.id,
        provider,
        request,
    }
}

fn grant(store: &MockStore, instance: &str) -> StartupReattachGrant {
    match store
        .admit_startup_reattach(&command(store, instance), 20)
        .unwrap()
    {
        StartupReattachAdmission::Execute(grant) => grant,
        other => panic!("expected first startup grant, got {other:?}"),
    }
}

fn ack(grant: &StartupReattachGrant) -> ProviderSession {
    ProviderSession {
        runtime_session_id: grant.intent.command.session_id.clone(),
        provider_session_ref: grant.intent.authority.request.provider_session_ref.clone(),
        canonical_provider_session_ref: grant
            .intent
            .authority
            .request
            .canonical_provider_session_ref
            .clone(),
    }
}

fn build_manager(
    store: Arc<MockStore>,
    provider: Arc<PausedResumeProvider>,
) -> Arc<RuntimeSessionManager> {
    let mut registry = ProviderRegistry::new();
    registry.register(provider).unwrap();
    Arc::new(RuntimeSessionManager::new(store, Arc::new(registry), 128).unwrap())
}

async fn entered(provider: &PausedResumeProvider) {
    timeout(Duration::from_secs(3), provider.entered.notified())
        .await
        .unwrap();
}

#[test]
fn startup_mock_consumes_one_attempt_and_replays_only_its_exact_command() {
    let store = seed(None);
    let first_command = command(&store, "one");
    let first = grant(&store, "one");
    assert!(matches!(
        store.admit_startup_reattach(&first_command, 21).unwrap(),
        StartupReattachAdmission::Unresolved { .. }
    ));
    let mut changed = first_command.clone();
    changed.request.model = Some("changed".into());
    assert!(store.admit_startup_reattach(&changed, 22).is_err());
    let completed = store
        .complete_startup_reattach(&first, &ack(&first), 23)
        .unwrap();
    assert_eq!(completed.receipt.operation_id, first_command.operation_id);
    assert!(matches!(
        store.admit_startup_reattach(&first_command, 24).unwrap(),
        StartupReattachAdmission::Replay(_)
    ));
    let ledger = store.startup.lock().unwrap();
    let operation = &ledger.operations[&first_command.operation_id];
    assert_eq!(operation.phase, OperationPhase::Completed);
    assert_eq!(operation.attempt_count, 1);
    assert_eq!(operation.effect_phase, "finalized");
    assert!(ledger.claims.is_empty());
}

#[test]
fn startup_mock_receipt_fault_preserves_known_ack_and_exclusive_claim() {
    let store = seed(None);
    let first = grant(&store, "fault");
    store.startup.lock().unwrap().fail_next_receipt = true;
    assert!(store
        .complete_startup_reattach(&first, &ack(&first), 21)
        .is_err());
    assert!(matches!(
        store
            .hold_startup_reattach(
                &first,
                Some(&ack(&first)),
                "positive ACK; receipt failed",
                22
            )
            .unwrap(),
        StartupReattachHold::Held { .. }
    ));
    assert!(matches!(
        store
            .admit_startup_reattach(&command(&store, "new-process"), 23)
            .unwrap(),
        StartupReattachAdmission::Unresolved { .. }
    ));
    let ledger = store.startup.lock().unwrap();
    let operation = &ledger.operations[&first.intent.command.operation_id];
    assert_eq!(operation.phase, OperationPhase::ManualReview);
    assert_eq!(operation.effect_phase, "observed");
    assert_eq!(operation.attempt_count, 1);
    assert!(operation.held_ack.is_some());
    assert_eq!(ledger.claims["startup-session"], first.claim);
}

#[test]
fn startup_mock_pending_dispatch_loser_does_not_consume_the_original_turn() {
    let store = seed(Some(TurnDispatchState::Pending));
    let original = store.list_turn_admissions().unwrap().remove(0);
    let first = grant(&store, "pending");
    let mut requested = original.clone();
    requested.dispatch_state = TurnDispatchState::Dispatching;
    requested.updated_at = 30;
    assert!(matches!(store.upsert_turn_admission(&requested),
        Err(RuntimeError::TurnDispatchNotClaimed { blocking_operation_id: Some(id), .. })
            if id == first.intent.command.operation_id));
    assert_eq!(store.list_turn_admissions().unwrap(), vec![original]);
    store
        .complete_startup_reattach(&first, &ack(&first), 31)
        .unwrap();
    store.upsert_turn_admission(&requested).unwrap();
    assert!(matches!(
        store.upsert_turn_admission(&requested),
        Err(RuntimeError::TurnDispatchNotClaimed {
            blocking_operation_id: None,
            ..
        })
    ));
}

#[tokio::test]
async fn startup_allows_exact_terminal_metadata_and_canonical_fill_during_resume() {
    let store = seed(Some(TurnDispatchState::Dispatched));
    let mut original = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap()
        .session;
    original.canonical_provider_session_ref = None;
    store.upsert_session(&original).unwrap();
    let provider = Arc::new(PausedResumeProvider::new());
    let manager = build_manager(store.clone(), provider.clone());
    let running = manager.clone();
    let task = tokio::spawn(async move { running.recover_startup().await });
    entered(&provider).await;
    assert!(manager
        .apply_terminal_result(ProviderTurnResult {
            runtime_session_id: "startup-session".into(),
            turn_id: "startup-turn".into(),
            status: ProviderTurnStatus::Completed,
            usage: Some(json!({"last_message":"new assistant text"})),
            error: None,
        })
        .await
        .unwrap());
    let terminal = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal.session.metadata["policy_metadata"],
        json!({"allowed_tools":["Read"]})
    );
    provider.release.notify_one();
    let summary = timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.resumed_sessions, 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let current = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    assert_eq!(current.session.metadata, terminal.session.metadata);
    assert_eq!(
        current.session.canonical_provider_session_ref.as_deref(),
        Some("native-startup-session")
    );
    assert_eq!(
        manager.get_session("startup-session").await.unwrap(),
        current.session
    );
    let entry = store
        .startup
        .lock()
        .unwrap()
        .operations
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(entry.phase, OperationPhase::Completed);
    assert_eq!(
        entry.receipt.unwrap().metadata_terminal_event_id.as_deref(),
        Some("evt_turn_startup-turn_terminal")
    );
    manager.recover_startup().await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn startup_retains_ack_if_event_stream_is_quarantined_during_resume() {
    let store = seed(Some(TurnDispatchState::Dispatched));
    let provider = Arc::new(PausedResumeProvider::new());
    let manager = build_manager(store.clone(), provider.clone());
    let running = manager.clone();
    let task = tokio::spawn(async move { running.recover_startup().await });
    entered(&provider).await;
    manager
        .mark_provider_event_stream_unknown(
            "startup-session",
            "startup-turn",
            "stream_lost",
            "native event bridge lost".into(),
        )
        .await
        .unwrap();
    let quarantined = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    provider.release.notify_one();
    let summary = timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.resumed_sessions, 0);
    assert_eq!(
        store
            .get_persisted_retirement_review_session("startup-session")
            .unwrap()
            .unwrap(),
        quarantined
    );
    assert_eq!(
        manager.get_session("startup-session").await.unwrap(),
        quarantined.session
    );
    let entry = store
        .startup
        .lock()
        .unwrap()
        .operations
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(entry.phase, OperationPhase::ManualReview);
    assert_eq!(entry.effect_phase, "observed");
    assert_eq!(entry.attempt_count, 1);
    let restarted = build_manager(store.clone(), provider.clone());
    restarted.recover_startup().await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn startup_refuses_policy_metadata_drift_instead_of_stripping_metadata() {
    let store = seed(None);
    let provider = Arc::new(PausedResumeProvider::new());
    let manager = build_manager(store.clone(), provider.clone());
    let running = manager.clone();
    let task = tokio::spawn(async move { running.recover_startup().await });
    entered(&provider).await;
    let mut changed = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap()
        .session;
    changed.metadata["policy_metadata"]["allowed_tools"] = json!(["Bash"]);
    store.upsert_session(&changed).unwrap();
    provider.release.notify_one();
    let summary = timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.resumed_sessions, 0);
    assert_eq!(
        store
            .get_persisted_retirement_review_session("startup-session")
            .unwrap()
            .unwrap()
            .session,
        changed
    );
    let entry = store
        .startup
        .lock()
        .unwrap()
        .operations
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(entry.phase, OperationPhase::ManualReview);
    assert_eq!(entry.effect_phase, "observed");
}

#[tokio::test]
async fn frozen_startup_outcomes_cannot_overwrite_a_newer_local_quarantine() {
    let store = seed(Some(TurnDispatchState::Dispatching));
    let provider = Arc::new(PausedResumeProvider::new());
    let manager = build_manager(store.clone(), provider);
    let frozen = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    manager
        .mark_provider_event_stream_unknown(
            "startup-session",
            "startup-turn",
            "stream_lost",
            "newer quarantine".into(),
        )
        .await
        .unwrap();
    let quarantined = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    let error = RuntimeError::provider_not_dispatched(
        "local_input_unavailable",
        "no native send attempted",
    );
    assert!(manager
        .commit_provider_dispatch_outcome_from_snapshot(
            "startup-session",
            "startup-turn",
            &error,
            true,
            Some(&frozen),
        )
        .await
        .is_err());
    assert!(manager
        .commit_provider_dispatch_started_from_snapshot(
            "startup-session",
            "startup-turn",
            Some("native-turn".into()),
            None,
            Some(&frozen),
        )
        .await
        .is_err());
    assert_eq!(
        store
            .get_persisted_retirement_review_session("startup-session")
            .unwrap()
            .unwrap(),
        quarantined
    );
    assert_eq!(
        manager.get_session("startup-session").await.unwrap(),
        quarantined.session
    );
}

#[tokio::test]
async fn direct_legacy_resume_refuses_before_any_native_or_store_effect() {
    let store = seed(None);
    let provider = Arc::new(PausedResumeProvider::new());
    let manager = build_manager(store.clone(), provider.clone());
    let before = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    for session_id in ["startup-session", "missing-session"] {
        assert!(matches!(
            manager
                .resume_session(
                    session_id,
                    ResumeSessionInput {
                        provider_session_ref: Some("different-native".into()),
                        canonical_provider_session_ref: Some("different-native".into()),
                    }
                )
                .await,
            Err(RuntimeError::Unsupported(_))
        ));
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .get_persisted_retirement_review_session("startup-session")
            .unwrap()
            .unwrap(),
        before
    );
    assert!(store.startup.lock().unwrap().operations.is_empty());
    assert!(store.events.lock().unwrap().is_empty());
}

#[derive(Default)]
struct StartupDecisionInFlightProvider {
    decision_entered: Notify,
    decision_release: Notify,
    decisions: AtomicUsize,
    resumes: AtomicUsize,
}

#[async_trait]
impl RuntimeProvider for StartupDecisionInFlightProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "startup-decision-in-flight".into(),
            enabled: true,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn respond_approval(
        &self,
        _request: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        self.decisions.fetch_add(1, Ordering::SeqCst);
        self.decision_entered.notify_one();
        self.decision_release.notified().await;
        Ok(())
    }

    async fn resume_session_with_policy(
        &self,
        request: ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.resumes.fetch_add(1, Ordering::SeqCst);
        Ok(ProviderSession {
            runtime_session_id: request.runtime_session_id,
            provider_session_ref: request.provider_session_ref,
            canonical_provider_session_ref: request.canonical_provider_session_ref,
        })
    }
}

#[tokio::test]
async fn terminal_before_approval_ack_does_not_clear_the_session_resume_fence() {
    let store = seed(Some(TurnDispatchState::Dispatched));
    let provider = Arc::new(StartupDecisionInFlightProvider::default());
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let manager =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128).unwrap());
    let approval = manager
        .record_provider_approval(
            "startup-session",
            "startup-turn",
            "native-decision-in-flight",
            Some("pending-tool".into()),
            json!({"command":"true"}),
        )
        .await
        .unwrap();

    let responder = manager.clone();
    let approval_id = approval.id.clone();
    let decision = tokio::spawn(async move {
        responder
            .respond_approval(
                "startup-session",
                &approval_id,
                ApprovalResponseInput {
                    decision: "accept".into(),
                    payload: None,
                },
            )
            .await
    });
    timeout(Duration::from_secs(3), provider.decision_entered.notified())
        .await
        .unwrap();
    assert_eq!(
        store.approvals.lock().unwrap()[&approval.id].status,
        "decision_pending"
    );
    assert_eq!(provider.decisions.load(Ordering::SeqCst), 1);

    assert!(manager
        .apply_terminal_result(ProviderTurnResult {
            runtime_session_id: "startup-session".into(),
            turn_id: "startup-turn".into(),
            status: ProviderTurnStatus::Completed,
            usage: Some(json!({"last_message":"completed before decision ACK"})),
            error: None,
        })
        .await
        .unwrap());
    let terminal = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    assert!(terminal.session.active_turn_id.is_none());
    assert_eq!(terminal.turns[0].status, "completed");
    assert_eq!(terminal.approvals[0].status, "decision_pending");
    let terminal_event = store
        .events
        .lock()
        .unwrap()
        .iter()
        .find(|row| row.event_id == "evt_turn_startup-turn_terminal")
        .cloned()
        .unwrap();

    let blocked = manager.recover_startup().await.unwrap();
    assert_eq!(blocked.resumed_sessions, 0, "{:?}", blocked.notes);
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 0);
    assert!(store.startup.lock().unwrap().operations.is_empty());
    assert!(store
        .admit_startup_reattach(&command(&store, "decision-pending"), 40)
        .is_err());
    assert!(store.startup.lock().unwrap().operations.is_empty());
    assert_eq!(
        store
            .get_persisted_retirement_review_session("startup-session")
            .unwrap()
            .unwrap(),
        terminal
    );

    provider.decision_release.notify_one();
    let resolved = timeout(Duration::from_secs(3), decision)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(resolved.status, "accept");
    let known = store
        .get_persisted_retirement_review_session("startup-session")
        .unwrap()
        .unwrap();
    assert_eq!(known.session, terminal.session);
    assert_eq!(known.turns, terminal.turns);
    assert_eq!(known.admissions, terminal.admissions);
    assert_eq!(known.approvals[0], resolved);
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .find(|row| row.event_id == terminal_event.event_id)
            .unwrap(),
        &terminal_event
    );
    assert_eq!(provider.decisions.load(Ordering::SeqCst), 1);

    let attached = manager.recover_startup().await.unwrap();
    assert_eq!(attached.resumed_sessions, 1, "{:?}", attached.notes);
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 1);
    let ledger = store.startup.lock().unwrap();
    assert_eq!(ledger.operations.len(), 1);
    assert_eq!(
        ledger.operations.values().next().unwrap().phase,
        OperationPhase::Completed
    );
    assert_eq!(ledger.operations.values().next().unwrap().attempt_count, 1);
    assert!(ledger.claims.is_empty());
}
