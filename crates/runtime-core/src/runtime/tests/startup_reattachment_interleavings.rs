use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

use super::super::test_support::{AuthorityPause, MockStore};
use crate::*;

struct ReleaseOnDrop(Arc<AuthorityPause>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Provider {
    resume_entered: Notify,
    allow_resume: Notify,
    resumes: AtomicUsize,
    sends: AtomicUsize,
}

impl Provider {
    fn new() -> Self {
        Self {
            resume_entered: Notify::new(),
            allow_resume: Notify::new(),
            resumes: AtomicUsize::new(0),
            sends: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl RuntimeProvider for Provider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "interleaving-test".into(),
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
        self.resumes.fetch_add(1, Ordering::SeqCst);
        self.resume_entered.notify_one();
        self.allow_resume.notified().await;
        Ok(ProviderSession {
            runtime_session_id: request.runtime_session_id,
            canonical_provider_session_ref: Some(request.provider_session_ref.clone()),
            provider_session_ref: request.provider_session_ref,
        })
    }

    async fn send_turn(
        &self,
        request: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(ProviderTurnAck {
            runtime_session_id: request.runtime_session_id,
            provider_native_turn_id: Some(format!("native-{}", request.turn_id)),
            turn_id: request.turn_id,
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

    async fn wait_for_turn(
        &self,
        _request: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        std::future::pending().await
    }
}

fn seed(dispatched: bool) -> Arc<MockStore> {
    let store = Arc::new(MockStore::default());
    let mut session = SessionRecord {
        id: "interleaving-session".into(),
        provider: "codex".into(),
        status: "ready".into(),
        cwd: None,
        model: None,
        permission_mode: None,
        system_prompt: Some("unchanged launch policy".into()),
        metadata: json!({"policy_metadata":{"allowed_tools":["Read"]}}),
        provider_session_ref: Some("native-interleaving-session".into()),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 10,
        updated_at: 10,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    };
    if dispatched {
        let input = vec![json!({"type":"text","text":"in-flight original input"})];
        let turn = TurnRecord {
            id: "interleaving-turn".into(),
            session_id: session.id.clone(),
            provider_turn_ref: None,
            status: "in_progress".into(),
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
            dispatch_state: TurnDispatchState::Dispatched,
            provider_native_turn_id: Some("native-interleaving-turn".into()),
            dispatch_error: None,
            admitted_at: 11,
            updated_at: 12,
        };
        session.status = "turn_running".into();
        session.active_turn_id = Some(turn.id.clone());
        session.updated_at = 12;
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

fn manager(store: Arc<MockStore>, provider: Arc<Provider>) -> Arc<RuntimeSessionManager> {
    let mut registry = ProviderRegistry::new();
    registry.register(provider).unwrap();
    Arc::new(RuntimeSessionManager::new(store, Arc::new(registry), 128).unwrap())
}

async fn notified(notify: &Notify, context: &str) {
    timeout(Duration::from_secs(3), notify.notified())
        .await
        .unwrap_or_else(|_| panic!("notification not reached: {context}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn terminal_callback_observes_canonical_fill_even_when_completion_return_is_lost() {
    for lost_return in [false, true] {
        let store = seed(true);
        let provider = Arc::new(Provider::new());
        let manager = manager(store.clone(), provider.clone());
        let pause = Arc::new(AuthorityPause::new());
        let _release_on_failure = ReleaseOnDrop(pause.clone());
        let terminal_at_store = Arc::new(Notify::new());
        *store.startup_after_complete.lock().unwrap() = Some(pause.clone());
        *store.terminal_commit_entered.lock().unwrap() = Some(terminal_at_store.clone());
        *store.fail_next_startup_return_after_commit.lock().unwrap() = lost_return;

        let recovering = manager.clone();
        let startup = tokio::spawn(async move { recovering.recover_startup().await });
        notified(&provider.resume_entered, "native resume entered").await;
        provider.allow_resume.notify_one();
        pause.wait_until_entered().await;

        // Test-only direct observation: the store has installed the canonical
        // fill, but its synchronous completion method has not returned yet.
        assert_eq!(
            store.sessions.lock().unwrap()["interleaving-session"]
                .canonical_provider_session_ref
                .as_deref(),
            Some("native-interleaving-session"),
        );
        // Select a deterministic schedule in either implementation. If the
        // runtime did not reserve the cache interval, force the callback to
        // capture that stale cache before allowing the store call to return.
        let cache_interval_unreserved = manager.turns.try_read().is_ok();
        let callback_started = Arc::new(Notify::new());
        let started = callback_started.clone();
        let callback_manager = manager.clone();
        let callback = tokio::spawn(async move {
            started.notify_one();
            callback_manager
                .apply_terminal_result(ProviderTurnResult {
                    runtime_session_id: "interleaving-session".into(),
                    turn_id: "interleaving-turn".into(),
                    status: ProviderTurnStatus::Completed,
                    usage: Some(json!({"last_message":"terminal answer"})),
                    error: None,
                })
                .await
        });
        notified(&callback_started, "terminal callback started").await;
        if cache_interval_unreserved {
            // The mock signals before its authority mutex. It therefore
            // proves that the callback already captured its before bundle.
            notified(&terminal_at_store, "callback captured stale cache").await;
        }
        pause.release();
        let summary = timeout(Duration::from_secs(3), startup)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(summary.resumed_sessions, 1);
        assert!(timeout(Duration::from_secs(3), callback)
            .await
            .unwrap()
            .unwrap()
            .unwrap());
        assert_eq!(provider.resumes.load(Ordering::SeqCst), 1);
        assert_eq!(provider.sends.load(Ordering::SeqCst), 0);

        let persisted = store
            .get_persisted_retirement_review_session("interleaving-session")
            .unwrap()
            .unwrap();
        assert_eq!(persisted.session.status, "ready");
        assert!(persisted.session.active_turn_id.is_none());
        assert_eq!(
            persisted.session.canonical_provider_session_ref.as_deref(),
            Some("native-interleaving-session")
        );
        assert_eq!(
            persisted.session.metadata["policy_metadata"],
            json!({"allowed_tools":["Read"]})
        );
        assert_eq!(persisted.turns[0].status, "completed");
        assert_eq!(
            manager.get_session("interleaving-session").await.unwrap(),
            persisted.session
        );
        let ledger = store.startup.lock().unwrap();
        let operation = ledger.operations.values().next().unwrap();
        assert_eq!(operation.phase, OperationPhase::Completed);
        assert_eq!(operation.attempt_count, 1);
        assert!(ledger.claims.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn original_sender_deferred_by_resume_claim_preserves_pending_then_startup_sends_once() {
    let store = seed(false);
    let provider = Arc::new(Provider::new());
    let original = manager(store.clone(), provider.clone());
    let admission_pause = Arc::new(AuthorityPause::new());
    let _release_on_failure = ReleaseOnDrop(admission_pause.clone());
    *store.after_turn_admission.lock().unwrap() = Some(admission_pause.clone());
    let sender = tokio::spawn(async move {
        original
            .send_turn(
                "interleaving-session",
                SendTurnInput {
                    input: vec![json!({"type":"text","text":"one original send"})],
                    expected_turn_id: None,
                    permission_mode: None,
                    projection_source: Some(TurnInputProjectionSource::UserVisible),
                    user_input_snapshot: None,
                    correlation_id: None,
                    turn_settings: Default::default(),
                },
            )
            .await
    });
    admission_pause.wait_until_entered().await;
    let before = store
        .get_persisted_retirement_review_session("interleaving-session")
        .unwrap()
        .unwrap();
    assert_eq!(before.admissions.len(), 1);
    assert_eq!(
        before.admissions[0].dispatch_state,
        TurnDispatchState::Pending
    );
    let restarted = manager(store.clone(), provider.clone());
    let startup = tokio::spawn(async move { restarted.recover_startup().await });
    notified(
        &provider.resume_entered,
        "startup holds native resume claim",
    )
    .await;
    let owner = store.startup.lock().unwrap().claims["interleaving-session"]
        .owner_operation_id
        .clone();

    admission_pause.release();
    let result = timeout(Duration::from_secs(3), sender)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result,
        Err(RuntimeError::TurnDispatchNotClaimed { blocking_operation_id: Some(id), .. })
            if id == owner));
    assert_eq!(provider.sends.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .get_persisted_retirement_review_session("interleaving-session")
            .unwrap()
            .unwrap(),
        before
    );

    provider.allow_resume.notify_one();
    let summary = timeout(Duration::from_secs(3), startup)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.resumed_sessions, 1);
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 1);
    assert_eq!(provider.sends.load(Ordering::SeqCst), 1);
    let after = store
        .get_persisted_retirement_review_session("interleaving-session")
        .unwrap()
        .unwrap();
    assert_eq!(after.turns.len(), 1);
    assert_eq!(after.turns[0].id, before.turns[0].id);
    assert_eq!(
        after.admissions[0].dispatch_state,
        TurnDispatchState::Dispatched
    );
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.kind == "turn.started")
            .count(),
        1
    );
}

#[tokio::test]
async fn cached_dispatch_winner_returns_typed_deferral_before_transition_validation() {
    let store = seed(true);
    let provider = Arc::new(Provider::new());
    let manager = manager(store.clone(), provider.clone());
    let before = store
        .get_persisted_retirement_review_session("interleaving-session")
        .unwrap()
        .unwrap();
    assert!(matches!(
        manager
            .update_turn_dispatch_authority(
                "interleaving-turn",
                TurnDispatchState::Dispatching,
                None,
                None,
            )
            .await,
        Err(RuntimeError::TurnDispatchNotClaimed {
            blocking_operation_id: None,
            ..
        })
    ));
    assert_eq!(
        store
            .get_persisted_retirement_review_session("interleaving-session")
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(provider.sends.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn same_manager_startup_refresh_waits_for_original_admission_publication() {
    let store = seed(false);
    let provider = Arc::new(Provider::new());
    let manager = manager(store.clone(), provider.clone());
    let pause = Arc::new(AuthorityPause::new());
    let _release_on_failure = ReleaseOnDrop(pause.clone());
    *store.after_turn_admission.lock().unwrap() = Some(pause.clone());
    let original = manager.clone();
    let sender = tokio::spawn(async move {
        original
            .send_turn(
                "interleaving-session",
                SendTurnInput {
                    input: vec![json!({"type":"text","text":"publish one original admission"})],
                    expected_turn_id: None,
                    permission_mode: None,
                    projection_source: Some(TurnInputProjectionSource::UserVisible),
                    user_input_snapshot: None,
                    correlation_id: None,
                    turn_settings: Default::default(),
                },
            )
            .await
    });
    pause.wait_until_entered().await;
    let durable = store
        .get_persisted_retirement_review_session("interleaving-session")
        .unwrap()
        .unwrap();
    assert_eq!(durable.admissions.len(), 1);
    assert_eq!(
        durable.admissions[0].dispatch_state,
        TurnDispatchState::Pending
    );
    assert_eq!(provider.sends.load(Ordering::SeqCst), 0);
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 0);

    let mut refresh = Box::pin(manager.refresh_startup_session("interleaving-session"));
    {
        // This first poll is a deterministic counterexample, with no scheduling
        // timeout: the original publication owns the turns write lock now.
        // On the former four-interval publisher every cache lock is free here,
        // so refresh reads the committed Pending bundle and returns Ready.
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            std::future::Future::poll(refresh.as_mut(), &mut context),
            std::task::Poll::Pending
        ));
    }
    pause.release();
    // Poll both continuations. The refresh was first queued with a noop waker;
    // awaiting only sender could strand that granted write permit.
    let (sent, refreshed) = timeout(Duration::from_secs(3), async {
        tokio::join!(sender, refresh)
    })
    .await
    .unwrap();
    let accepted = sent.unwrap().unwrap();
    assert_eq!(refreshed.unwrap().session.id, "interleaving-session");

    let persisted = store
        .get_persisted_retirement_review_session("interleaving-session")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.turns.len(), 1);
    assert_eq!(persisted.admissions.len(), 1);
    assert_eq!(persisted.turns[0].id, accepted.turn_id);
    assert_eq!(
        persisted.admissions[0].dispatch_state,
        TurnDispatchState::Dispatched
    );
    assert_eq!(
        manager.get_session("interleaving-session").await.unwrap(),
        persisted.session
    );
    assert_eq!(
        manager.turns.read().await[&accepted.turn_id],
        persisted.turns[0]
    );
    assert_eq!(
        manager.turn_admissions.read().await[&accepted.turn_id],
        persisted.admissions[0]
    );
    assert!(manager.approvals.read().await.is_empty());
    assert_eq!(provider.sends.load(Ordering::SeqCst), 1);
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 0);
    assert!(store.startup.lock().unwrap().operations.is_empty());
}
