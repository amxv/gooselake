use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::super::test_support::MockStore;
use super::super::test_support_session_fixture::stored_session_record_for_test;
use crate::*;

struct LifecycleProbe {
    calls: Arc<AtomicUsize>,
}

impl LifecycleProbe {
    fn unexpected(&self) -> RuntimeError {
        self.calls.fetch_add(1, Ordering::SeqCst);
        RuntimeError::Io("legacy manager entered a native lifecycle adapter".into())
    }
}

#[async_trait::async_trait]
impl RuntimeProvider for LifecycleProbe {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }
    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "lifecycle-refusal-probe".into(),
            enabled: true,
        }
    }
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn create_session(
        &self,
        _request: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Err(self.unexpected())
    }
    async fn create_session_with_policy(
        &self,
        _request: ProviderCreateSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Err(self.unexpected())
    }
    async fn resume_session(
        &self,
        _request: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Err(self.unexpected())
    }
    async fn resume_session_with_policy(
        &self,
        _request: ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Err(self.unexpected())
    }
    async fn close_session(
        &self,
        _request: ProviderCloseSessionRequest,
    ) -> Result<(), RuntimeError> {
        Err(self.unexpected())
    }
}

fn fixture() -> (Arc<RuntimeSessionManager>, Arc<MockStore>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(LifecycleProbe {
            calls: calls.clone(),
        }))
        .unwrap();
    let store = Arc::new(MockStore::default());
    let manager =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128).unwrap());
    (manager, store, calls)
}

fn input() -> CreateSessionInput {
    CreateSessionInput {
        provider: ProviderKind::Codex,
        model: Some("fixture-model".into()),
        cwd: Some("/fixture/exact-cwd".into()),
        permission_mode: Some("workspace_write".into()),
        metadata: Some(serde_json::json!({"fixture":"lifecycle","policy":{"allowed":["Read"]}})),
    }
}

fn event(
    session: &SessionRecord,
    kind: &str,
    event_id: String,
    payload: serde_json::Value,
) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id,
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: None,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: session.updated_at,
    }
}

#[tokio::test]

async fn all_four_legacy_lifecycle_methods_refuse_without_native_or_state_effects() {
    let (manager, store, calls) = fixture();
    let seeded = manager
        .seed_stored_session_for_test(input(), "fixture:")
        .await
        .unwrap();
    assert_eq!(seeded.model.as_deref(), Some("fixture-model"));
    assert_eq!(seeded.cwd.as_deref(), Some("/fixture/exact-cwd"));
    assert_eq!(seeded.permission_mode.as_deref(), Some("workspace_write"));
    assert_eq!(
        seeded.provider_session_ref,
        Some(format!("fixture:{}", seeded.id))
    );
    assert_eq!(
        seeded.metadata,
        serde_json::json!({"fixture":"lifecycle","policy":{"allowed":["Read"]}})
    );
    let mut closed = manager
        .seed_stored_session_for_test(input(), "fixture:")
        .await
        .unwrap();
    closed.status = "closed".into();
    closed.closed_at = Some(closed.updated_at);
    store.upsert_session(&closed).unwrap();
    manager
        .sessions
        .write()
        .await
        .insert(closed.id.clone(), closed.clone());
    let before_sessions = store.sessions.lock().unwrap().clone();
    let before_turns = store.turns.lock().unwrap().clone();
    let before_admissions = store.turn_admissions.lock().unwrap().clone();
    let before_approvals = store.approvals.lock().unwrap().clone();
    let cached_sessions = manager.sessions.read().await.clone();
    let cached_turns = manager.turns.read().await.clone();
    let cached_admissions = manager.turn_admissions.read().await.clone();
    let cached_approvals = manager.approvals.read().await.clone();
    assert!(store.events.lock().unwrap().is_empty());
    *store.fail_session_write.lock().unwrap() = true;
    *store.fail_turn_write.lock().unwrap() = true;
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("session.created".into());

    for _ in 0..2 {
        assert!(matches!(
            manager.create_session(input()).await,
            Err(RuntimeError::Unsupported(_))
        ));
        let mut unregistered = input();
        unregistered.provider = ProviderKind::Claude;
        unregistered.permission_mode = Some(String::new());
        assert!(matches!(
            manager.create_session(unregistered).await,
            Err(RuntimeError::Unsupported(_))
        ));
        for id in [seeded.id.as_str(), closed.id.as_str(), "missing-session"] {
            assert!(matches!(
                manager
                    .resume_session(
                        id,
                        ResumeSessionInput {
                            provider_session_ref: Some("different-native".into()),
                            canonical_provider_session_ref: Some("different-canonical".into()),
                        }
                    )
                    .await,
                Err(RuntimeError::Unsupported(_))
            ));
            assert!(matches!(
                manager
                    .close_session(id, Some("operator-request".into()))
                    .await,
                Err(RuntimeError::Unsupported(_))
            ));
            assert!(matches!(
                manager
                    .force_close_session(id, Some("unverified-prose".into()))
                    .await,
                Err(RuntimeError::Unsupported(_))
            ));
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(*store.sessions.lock().unwrap(), before_sessions);
    assert_eq!(*store.turns.lock().unwrap(), before_turns);
    assert_eq!(*store.turn_admissions.lock().unwrap(), before_admissions);
    assert_eq!(*store.approvals.lock().unwrap(), before_approvals);
    assert_eq!(*manager.sessions.read().await, cached_sessions);
    assert_eq!(*manager.turns.read().await, cached_turns);
    assert_eq!(*manager.turn_admissions.read().await, cached_admissions);
    assert_eq!(*manager.approvals.read().await, cached_approvals);
    assert!(store.events.lock().unwrap().is_empty());
    assert_eq!(
        store
            .fail_next_event_append_for_kind
            .lock()
            .unwrap()
            .as_deref(),
        Some("session.created")
    );
}

#[tokio::test]
async fn store_creation_receipt_fault_rolls_back_known_projection_without_native_work() {
    let (manager, store, calls) = fixture();
    let record =
        stored_session_record_for_test(input(), "known-created-fixture".into(), "fixture:", 10)
            .unwrap();
    let created = event(
        &record,
        "session.created",
        format!("evt_session_{}_created", record.id),
        serde_json::json!({"provider":record.provider}),
    );
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("session.created".into());
    assert!(store
        .commit_session_lifecycle(None, &record, &created)
        .is_err());
    assert!(store.sessions.lock().unwrap().is_empty());
    assert!(store.events.lock().unwrap().is_empty());
    assert!(manager.sessions.read().await.is_empty());
    let (committed, receipt) = store
        .commit_session_lifecycle(None, &record, &created)
        .unwrap();
    assert_eq!(committed, record);
    let receipt = receipt.unwrap();
    assert_eq!(receipt.event_id, created.event_id);
    assert_eq!(receipt.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(store.sessions.lock().unwrap()[&record.id], record);
    assert_eq!(store.events.lock().unwrap().len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // A store receipt does not silently publish a manager cache or authorize a
    // provider create. This test supplies a known-result fixture explicitly.
    assert!(manager.sessions.read().await.is_empty());
}

#[tokio::test]
async fn store_close_receipt_fault_preserves_the_original_projection_and_critical_row() {
    let (manager, store, calls) = fixture();
    let before = manager
        .seed_stored_session_for_test(input(), "fixture:")
        .await
        .unwrap();
    let mut closed = before.clone();
    closed.status = "closed".into();
    closed.active_turn_id = None;
    closed.updated_at += 1;
    closed.closed_at = Some(closed.updated_at);
    let closed_event = event(
        &closed,
        "session.closed",
        format!("evt_session_{}_closed", closed.id),
        serde_json::json!({"reason":"known-provider-close-test"}),
    );
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("session.closed".into());
    assert!(store
        .commit_session_lifecycle(Some(&before), &closed, &closed_event)
        .is_err());
    assert_eq!(store.sessions.lock().unwrap()[&before.id], before);
    assert_eq!(manager.get_session(&before.id).await.unwrap(), before);
    assert!(store.events.lock().unwrap().is_empty());
    let (committed, receipt) = store
        .commit_session_lifecycle(Some(&before), &closed, &closed_event)
        .unwrap();
    assert_eq!(committed, closed);
    let receipt = receipt.unwrap();
    assert_eq!(receipt.event_id, closed_event.event_id);
    assert_eq!(receipt.payload["reason"], "known-provider-close-test");
    assert_eq!(receipt.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(store.sessions.lock().unwrap()[&before.id], closed);
    assert_eq!(store.events.lock().unwrap().len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn store_resume_receipt_fault_preserves_original_native_identity_without_native_work() {
    let (manager, store, calls) = fixture();
    let before = manager
        .seed_stored_session_for_test(input(), "fixture:")
        .await
        .unwrap();
    let mut resumed = before.clone();
    resumed.canonical_provider_session_ref = before.provider_session_ref.clone();
    resumed.updated_at += 1;
    let resumed_event = event(
        &resumed,
        "session.resumed",
        format!("evt_session_{}_resumed_known_fixture", resumed.id),
        serde_json::json!({}),
    );
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("session.resumed".into());
    assert!(store
        .commit_session_lifecycle(Some(&before), &resumed, &resumed_event)
        .is_err());
    assert_eq!(store.sessions.lock().unwrap()[&before.id], before);
    assert_eq!(manager.get_session(&before.id).await.unwrap(), before);
    assert!(store.events.lock().unwrap().is_empty());
    let (committed, receipt) = store
        .commit_session_lifecycle(Some(&before), &resumed, &resumed_event)
        .unwrap();
    assert_eq!(committed, resumed);
    let receipt = receipt.unwrap();
    assert_eq!(receipt.event_id, resumed_event.event_id);
    assert_eq!(receipt.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(store.sessions.lock().unwrap()[&before.id], resumed);
    assert_eq!(store.events.lock().unwrap().len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
