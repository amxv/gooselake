//! Runtime execution grants and the changed-fork cache publication interval.
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

use super::super::test_support::{AuthorityPause, EditReceiptRace, MockEditFixture, MockStore};
use crate::*;

const SESSION: &str = "n04-edit-session";
const TARGET: &str = "n04-original-target";

#[derive(Default)]
struct Provider {
    observes: AtomicUsize,
    mappings: AtomicUsize,
    resumes: AtomicUsize,
    forks: AtomicUsize,
    sends: AtomicUsize,
}

#[async_trait]
impl RuntimeProvider for Provider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "N04 fixture".into(),
            enabled: true,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            hard_fork_edit_rerun: ProviderCapabilitySupport::Supported,
            ..Default::default()
        }
    }

    async fn observe_session_identity(
        &self,
        id: &str,
    ) -> Result<Option<ProviderSession>, RuntimeError> {
        self.observes.fetch_add(1, Ordering::SeqCst);
        assert_eq!(id, SESSION);
        Ok(Some(ProviderSession {
            runtime_session_id: id.into(),
            provider_session_ref: "source-native".into(),
            canonical_provider_session_ref: None,
        }))
    }

    async fn restore_turn_identity_mapping(
        &self,
        session: &str,
        turn: &str,
        native: &str,
    ) -> Result<(), RuntimeError> {
        self.mappings.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            (session, turn, native),
            (SESSION, TARGET, "native-original-target")
        );
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

    async fn hard_fork_edit_rerun(
        &self,
        request: ProviderHardForkEditRerunRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.forks.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.runtime_session_id, SESSION);
        assert_eq!(request.target_turn_id, TARGET);
        assert_eq!(
            request.edited_input,
            vec![json!({"type":"text","text":"after"})]
        );
        Ok(ProviderSession {
            runtime_session_id: SESSION.into(),
            provider_session_ref: "verified-child".into(),
            canonical_provider_session_ref: Some("verified-child-canonical".into()),
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

    async fn wait_for_turn(
        &self,
        _: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        std::future::pending().await
    }
}

fn fixture(race: EditReceiptRace) -> (Arc<RuntimeSessionManager>, Arc<MockStore>, Arc<Provider>) {
    let store = Arc::new(MockStore::default());
    let session = SessionRecord {
        id: SESSION.into(),
        provider: "codex".into(),
        status: "ready".into(),
        cwd: None,
        model: None,
        permission_mode: None,
        system_prompt: None,
        metadata: json!({
            "policy_marker":{"preserved":true},
            "session_transcript":[{"role":"user","text":"old"}]
        }),
        provider_session_ref: Some("source-native".into()),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 100,
        updated_at: 100,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    };
    let turn = TurnRecord {
        id: TARGET.into(),
        session_id: SESSION.into(),
        provider_turn_ref: Some("native-original-target".into()),
        status: "completed".into(),
        input: json!([{"type":"text","text":"before"}]),
        source: Some("user".into()),
        started_at: Some(90),
        completed_at: Some(100),
        usage: None,
        error: None,
    };
    let admission = TurnAdmissionRecord {
        turn_id: TARGET.into(),
        session_id: SESSION.into(),
        provider: "codex".into(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot {
            prompt_text: "before".into(),
            ..Default::default()
        },
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: Default::default(),
        dispatch_state: TurnDispatchState::Dispatched,
        provider_native_turn_id: Some("native-original-target".into()),
        dispatch_error: None,
        admitted_at: 90,
        updated_at: 100,
    };
    let agent = WorkspaceAgentRecord {
        agent_id: SESSION.into(),
        workspace_id: "n04-fixture-workspace".into(),
        alias: "n04-edit-agent".into(),
        lifecycle_state: WorkspaceAgentLifecycleState::Active,
        profile: WorkspaceAgentProfile {
            title: None,
            title_provenance: "fixture".into(),
            added_by: "fixture".into(),
            creator_session_id: None,
            creator_compaction_subscription: "auto".into(),
            joined_at: 100,
        },
        recreation_policy: WorkspaceAgentRecreationPolicy {
            provider: ProviderKind::Codex,
            model: None,
            permission_intent: Default::default(),
            setting_sources_intent: Default::default(),
            current_preferences: Default::default(),
            system_prompt: None,
            allowed_tools: vec![],
            disallowed_tools: vec![],
            authoritative_cwd: ".".into(),
            harness_version_slot: None,
        },
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: None,
        metadata: session.metadata.clone(),
        archived_at: None,
        archive_reason: None,
        revision: 0,
        created_at: 100,
        updated_at: 100,
    };
    store.upsert_session(&session).unwrap();
    store.upsert_turn(&turn).unwrap();
    store.upsert_turn_admission(&admission).unwrap();
    *store.edit_fixture.lock().unwrap() = Some(MockEditFixture {
        agent,
        operation: None,
        race,
        peer_committed: false,
    });
    let provider = Arc::new(Provider::default());
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let manager =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128).unwrap());
    (manager, store, provider)
}

async fn edit(
    manager: &Arc<RuntimeSessionManager>,
) -> Result<SessionEditRerunOperation, RuntimeError> {
    manager
        .edit_workspace_agent_turn(
            SESSION,
            TARGET,
            PersistedUserInputSnapshot {
                prompt_text: "after".into(),
                ..Default::default()
            },
            Some("n04-same-request".into()),
        )
        .await
}

fn counters(provider: &Provider) -> [usize; 5] {
    [
        provider.observes.load(Ordering::SeqCst),
        provider.mappings.load(Ordering::SeqCst),
        provider.resumes.load(Ordering::SeqCst),
        provider.forks.load(Ordering::SeqCst),
        provider.sends.load(Ordering::SeqCst),
    ]
}

#[tokio::test]
async fn replayed_edit_admission_and_native_start_never_grant_provider_execution() {
    for (race, phase, expected) in [
        (
            EditReceiptRace::AdmissionReplay,
            "intended",
            [0, 0, 0, 0, 0],
        ),
        (
            EditReceiptRace::NativeStartReplay,
            "native_started",
            [1, 1, 0, 0, 0],
        ),
        (EditReceiptRace::None, "completed", [1, 1, 0, 1, 1]),
    ] {
        let (manager, store, provider) = fixture(race);
        let result = edit(&manager).await.unwrap();
        assert_eq!(result.phase, phase);
        assert_eq!(counters(&provider), expected);
        assert_eq!(
            store.get_session_edit_rerun(&result.operation_id).unwrap(),
            Some(result.clone())
        );
        let saved = store.events.lock().unwrap().clone();
        assert_eq!(edit(&manager).await.unwrap(), result);
        assert_eq!(counters(&provider), expected);
        assert_eq!(*store.events.lock().unwrap(), saved);
        if race == EditReceiptRace::AdmissionReplay {
            assert_eq!(
                saved
                    .iter()
                    .filter(|e| e.kind == "turn.edit_admitted")
                    .count(),
                1
            );
        }
        if race == EditReceiptRace::NativeStartReplay {
            assert_eq!(
                saved
                    .iter()
                    .filter(|e| e.kind == "turn.edit_native_started")
                    .count(),
                1
            );
        }
    }
}

struct ReleaseOnDrop(Arc<AuthorityPause>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn contradictory() -> ProviderTurnResult {
    ProviderTurnResult {
        runtime_session_id: SESSION.into(),
        turn_id: TARGET.into(),
        status: ProviderTurnStatus::Failed,
        usage: None,
        error: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn changed_fork_receipt_publishes_child_before_idle_terminal_conflict_callback() {
    let (manager, store, provider) = fixture(EditReceiptRace::None);
    let pause = Arc::new(AuthorityPause::new());
    let _release_on_failure = ReleaseOnDrop(pause.clone());
    *store.edit_after_fork.lock().unwrap() = Some(pause.clone());
    let entered_terminal_store = Arc::new(Notify::new());
    *store.terminal_commit_entered.lock().unwrap() = Some(entered_terminal_store.clone());
    let editing = manager.clone();
    let task = tokio::spawn(async move { edit(&editing).await });
    pause.wait_until_entered().await;
    assert_eq!(
        store.sessions.lock().unwrap()[SESSION]
            .provider_session_ref
            .as_deref(),
        Some("verified-child")
    );

    let callback = if manager.turns.try_read().is_ok() {
        // Counterexample schedule for the old publication gap: force the
        // callback to capture old cache before allowing the store return.
        let callback_manager = manager.clone();
        let callback = tokio::spawn(async move {
            callback_manager
                .apply_terminal_result(contradictory())
                .await
        });
        timeout(Duration::from_secs(3), entered_terminal_store.notified())
            .await
            .unwrap();
        pause.release();
        timeout(Duration::from_secs(3), callback)
            .await
            .unwrap()
            .unwrap()
    } else {
        // One explicit poll queues the callback on the reserved turns lock.
        // No timer or task scheduling assumption establishes the ordering.
        let mut callback = Box::pin(manager.apply_terminal_result(contradictory()));
        {
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                callback.as_mut().poll(&mut context),
                Poll::Pending
            ));
        }
        pause.release();
        timeout(Duration::from_secs(3), callback).await.unwrap()
    };
    assert!(
        matches!(callback, Err(RuntimeError::ProtocolViolation(_))),
        "{callback:?}"
    );
    let result = timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(RuntimeError::SessionControlUnresolved {
        coordinate: SessionControlCoordinate::EditRerun { ref agent_id, .. },
        replacement_turn_id: None, ..
    }) if agent_id == SESSION),
        "{result:?}"
    );
    assert_eq!(counters(&provider), [1, 1, 0, 1, 0]);
    let persisted = store.sessions.lock().unwrap()[SESSION].clone();
    assert_eq!(persisted.status, "failed");
    assert_eq!(persisted.failure_code.as_deref(), Some("terminal_conflict"));
    assert_eq!(
        persisted.canonical_provider_session_ref.as_deref(),
        Some("verified-child-canonical")
    );
    assert_eq!(
        persisted.metadata["policy_marker"],
        json!({"preserved":true})
    );
    assert_eq!(manager.get_session(SESSION).await.unwrap(), persisted);
    let events = store.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "turn.edit_fork_observed")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "provider.terminal_conflict")
            .count(),
        1
    );
    assert_eq!(
        events.iter().filter(|e| e.kind == "turn.admitted").count(),
        0
    );
    drop(events);
    assert_eq!(store.turn_admissions.lock().unwrap().len(), 1);
}
