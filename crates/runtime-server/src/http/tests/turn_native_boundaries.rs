use super::operator_native_policy::operator_policy_fixture;
use super::workspace_worktree_routes::call;
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn unkeyed_turn_selection_never_mutates_native_preferences_before_admission() {
    for version in ["v1", "v2"] {
        let (router, token, temp, provider, runtime, agent_id, revision) =
            operator_policy_fixture().await;
        let uri = if version == "v1" {
            format!("/v1/sessions/{agent_id}/turns")
        } else {
            format!("/v2/agents/{agent_id}/turns")
        };
        let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
        let count = || {
            db.query_row(
                "SELECT COUNT(*) FROM turn_admissions WHERE session_id=?1",
                [&agent_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        };
        let before_admissions = count();
        let before_native = provider.preferences_mutation_calls();
        let before_send = provider.send_invocations.load(Ordering::SeqCst);
        let (status, result) = call(
            &router,
            &token,
            "POST",
            &uri,
            Some(
                json!({"input":[{"type":"text","text":"do not change provider policy implicitly"}],
                "turn_settings":{"thinking_effort":"high"}}),
            ),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{version} {result}");
        assert_eq!(provider.preferences_mutation_calls(), before_native);
        assert_eq!(
            provider.send_invocations.load(Ordering::SeqCst),
            before_send
        );
        assert_eq!(count(), before_admissions);
        assert_eq!(
            runtime
                .get_active_workspace_agent_by_id(&agent_id)
                .unwrap()
                .revision,
            revision
        );
        assert!(runtime
            .get_session(&agent_id)
            .await
            .unwrap()
            .active_turn_id
            .is_none());
    }
}

async fn ambiguous_fallback(mode: u8) {
    for keyed in [false, true] {
        let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
        let uri = format!("/v2/agents/{agent_id}/turns");
        let input =
            json!({"input":[{"type":"text","text":"send only on verified original attachment"}]});
        provider.send_failure_mode.store(3, Ordering::SeqCst);
        provider.set_restore_failure_mode(mode);
        let before_send = provider.send_invocations.load(Ordering::SeqCst);
        let before_resume = provider.restore_call_count().await;
        let key = keyed.then_some("original-fallback-turn");
        let (status, result) = call(&router, &token, "POST", &uri, Some(input.clone()), key).await;
        assert_eq!(status, StatusCode::CONFLICT, "{keyed} {mode}: {result}");
        assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
        assert_eq!(
            provider.send_invocations.load(Ordering::SeqCst),
            before_send + 1
        );
        assert_eq!(provider.restore_call_count().await, before_resume + 1);
        let session = runtime.get_session(&agent_id).await.unwrap();
        assert_eq!(session.status, "turn_recovery_required");
        let turn_id = session.active_turn_id.as_deref().unwrap();
        let store = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp.path().join("runtime.sqlite3"),
        });
        let admission = store
            .repository()
            .list_turn_admissions()
            .unwrap()
            .into_iter()
            .find(|row| row.turn_id == turn_id)
            .unwrap();
        assert_eq!(
            admission.dispatch_state,
            runtime_core::TurnDispatchState::Unknown
        );
        if keyed {
            let operation_id = result["error"]["details"]["operation_id"].as_str().unwrap();
            let operation = store.get_operation(operation_id).unwrap().unwrap();
            assert_eq!(operation.claims.len(), 1);
            let (_, replay) = call(&router, &token, "POST", &uri, Some(input.clone()), key).await;
            assert_eq!(replay["error"]["details"]["operation_id"], operation_id);
        }
        // A healthy adapter after restart is not permission to repeat an
        // ambiguously completed native resume from the original turn.
        provider.set_restore_failure_mode(0);
        let mut providers = runtime_core::ProviderRegistry::new();
        providers.register(provider.clone()).unwrap();
        let reopened = Arc::new(
            RuntimeSessionManager::new(Arc::new(store), Arc::new(providers), 128).unwrap(),
        );
        let recovery = reopened.recover_startup().await.unwrap();
        assert_eq!(
            provider.restore_call_count().await,
            before_resume + 1,
            "{recovery:?}"
        );
        assert_eq!(
            provider.send_invocations.load(Ordering::SeqCst),
            before_send + 1
        );
        assert_eq!(
            reopened
                .get_session(&agent_id)
                .await
                .unwrap()
                .active_turn_id
                .as_deref(),
            Some(turn_id)
        );
        let (status, blocked) = call(&router, &token, "POST", &uri, Some(input), None).await;
        assert!(!status.is_success(), "{blocked}");
        assert_eq!(
            provider.send_invocations.load(Ordering::SeqCst),
            before_send + 1
        );
    }
}

#[tokio::test]
async fn fallback_resume_lost_ack_stays_unknown_across_keyed_and_unkeyed_restart() {
    ambiguous_fallback(2).await;
}

#[tokio::test]
async fn fallback_resume_changed_attachment_never_sends_or_retries_after_restart() {
    ambiguous_fallback(3).await;
}

#[tokio::test]
async fn fallback_resume_exact_attachment_preserves_one_definite_send_retry() {
    let (router, token, _temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let before_send = provider.send_invocations.load(Ordering::SeqCst);
    let before_resume = provider.restore_call_count().await;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.send_failure_mode.store(3, Ordering::SeqCst);
    let (status, result) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(json!({"input":[{"type":"text","text":"verified reattachment then send"}]})),
        Some("verified-fallback"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(
        provider.send_invocations.load(Ordering::SeqCst),
        before_send + 2
    );
    assert_eq!(provider.restore_call_count().await, before_resume + 1);
    let resumed = provider.resumed_requests().await;
    assert_eq!(
        resumed.last().unwrap().provider_session_ref,
        original.provider_session_ref.unwrap()
    );
}

#[tokio::test]
async fn fallback_lost_outcome_receipt_quarantines_dispatching_without_native_restart() {
    let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_fallback_outcome BEFORE INSERT ON runtime_events
         WHEN NEW.kind='turn.dispatch_unknown'
         BEGIN SELECT RAISE(ABORT,'lost fallback Critical receipt'); END;",
    )
    .unwrap();
    let before_send = provider.send_invocations.load(Ordering::SeqCst);
    let before_resume = provider.restore_call_count().await;
    provider.send_failure_mode.store(3, Ordering::SeqCst);
    provider.set_restore_failure_mode(2);
    let uri = format!("/v2/agents/{agent_id}/turns");
    let input = json!({"input":[{"type":"text","text":"lost resume and receipt"}]});
    let (status, result) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(input.clone()),
        Some("lost-fallback-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{result}");
    assert_eq!(result["error"]["code"], "outcome_unresolved");
    let operation_id = result["error"]["details"]["operation_id"].as_str().unwrap();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    let session = runtime.get_session(&agent_id).await.unwrap();
    let turn_id = session.active_turn_id.as_deref().unwrap();
    let admission = store
        .repository()
        .list_turn_admissions()
        .unwrap()
        .into_iter()
        .find(|row| row.turn_id == turn_id)
        .unwrap();
    assert_eq!(
        admission.dispatch_state,
        runtime_core::TurnDispatchState::Dispatching
    );
    assert!(admission.dispatch_error.is_none());
    let snapshot = store.repository().hydrate_runtime_state().unwrap();
    assert_eq!(
        snapshot
            .turns
            .iter()
            .find(|row| row.id == turn_id)
            .unwrap()
            .status,
        "admitted"
    );
    assert_eq!(
        snapshot
            .sessions
            .iter()
            .find(|row| row.id == agent_id)
            .unwrap()
            .status,
        "turn_admitted"
    );
    assert!(store
        .repository()
        .list_runtime_events(Some((RuntimeEventScope::Session, &agent_id)), None, 100,)
        .unwrap()
        .iter()
        .all(|event| event.kind != "turn.dispatch_unknown"));
    assert_eq!(
        store
            .get_operation(operation_id)
            .unwrap()
            .unwrap()
            .claims
            .len(),
        1
    );
    assert_eq!(
        provider.send_invocations.load(Ordering::SeqCst),
        before_send + 1
    );
    assert_eq!(provider.restore_call_count().await, before_resume + 1);
    db.execute_batch("DROP TRIGGER fail_fallback_outcome")
        .unwrap();
    provider.set_restore_failure_mode(0);
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(provider.clone()).unwrap();
    let reopened =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(providers), 128).unwrap());
    for _ in 0..2 {
        reopened.recover_startup().await.unwrap();
        assert_eq!(provider.restore_call_count().await, before_resume + 1);
        assert_eq!(
            provider.send_invocations.load(Ordering::SeqCst),
            before_send + 1
        );
    }
    let admission = store
        .repository()
        .list_turn_admissions()
        .unwrap()
        .into_iter()
        .find(|row| row.turn_id == turn_id)
        .unwrap();
    assert_eq!(
        admission.dispatch_state,
        runtime_core::TurnDispatchState::Unknown
    );
    assert_eq!(
        store
            .get_operation(operation_id)
            .unwrap()
            .unwrap()
            .claims
            .len(),
        1
    );
    let recovered = reopened.get_session(&agent_id).await.unwrap();
    assert_eq!(recovered.status, "turn_recovery_required");
    assert_eq!(recovered.active_turn_id.as_deref(), Some(turn_id));
    assert_eq!(recovered.provider_session_ref, session.provider_session_ref);
    assert_eq!(
        recovered.canonical_provider_session_ref,
        session.canonical_provider_session_ref
    );
    assert_eq!(
        admission.dispatch_error.as_ref().unwrap()["code"],
        "startup_dispatch_inflight"
    );
    let events = store
        .repository()
        .list_runtime_events(Some((RuntimeEventScope::Session, &agent_id)), None, 100)
        .unwrap();
    let unknown = events
        .iter()
        .filter(|event| event.kind == "turn.dispatch_unknown")
        .collect::<Vec<_>>();
    assert_eq!(unknown.len(), 1);
    assert_eq!(
        unknown[0].event_id,
        format!("evt_turn_{turn_id}_dispatch_unknown")
    );
    assert_eq!(
        unknown[0].criticality,
        runtime_core::RuntimeEventCriticality::Critical
    );
    let (_, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(input),
        Some("lost-fallback-receipt"),
    )
    .await;
    assert_eq!(replay["error"]["details"]["operation_id"], operation_id);
    assert_eq!(provider.restore_call_count().await, before_resume + 1);
    assert_eq!(
        provider.send_invocations.load(Ordering::SeqCst),
        before_send + 1
    );
}

#[tokio::test]
async fn native_approval_lost_receipt_keeps_original_decision_without_startup_resume() {
    use super::workspace_retirement_support::{retirement_fixture, RetirementProvider};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("approval-unknown-startup");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, started) = fixture
        .call(
            "POST",
            &format!("/v2/agents/{}/turns", agent.agent_id),
            Some(json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let turn_id = started["turn_id"].as_str().unwrap();
    let pending = fixture
        .runtime
        .record_provider_approval(
            &agent.agent_id,
            turn_id,
            "native-unknown-decision",
            None,
            json!({"inert":true}),
        )
        .await
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_native_decision_receipt BEFORE INSERT ON runtime_events
         WHEN NEW.kind='approval.resolved'
         BEGIN SELECT RAISE(ABORT,'lost native approval receipt'); END;",
    )
    .unwrap();
    let before_resume = provider.resumes.load(Ordering::SeqCst);
    let uri = format!("/v1/sessions/{}/approvals/{}", agent.agent_id, pending.id);
    let (status, result) = fixture
        .call("POST", &uri, Some(json!({"decision":"accept"})), None)
        .await;
    assert!(!status.is_success(), "{result}");
    assert_eq!(provider.approval_responses.load(Ordering::SeqCst), 1);
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    let snapshot = store
        .repository()
        .get_persisted_retirement_review_session(&agent.agent_id)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.session.status, "waiting_for_approval");
    assert_eq!(snapshot.session.active_turn_id.as_deref(), Some(turn_id));
    assert_eq!(
        snapshot
            .turns
            .iter()
            .find(|row| row.id == turn_id)
            .unwrap()
            .status,
        "waiting_for_approval"
    );
    let reserved = snapshot
        .approvals
        .iter()
        .find(|row| row.id == pending.id)
        .unwrap();
    assert_eq!(reserved.status, "decision_pending");
    let attempt_id = reserved.response.as_ref().unwrap()["decision_attempt_id"]
        .as_str()
        .unwrap();
    assert!(!attempt_id.is_empty());
    let events = store
        .repository()
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &agent.agent_id)),
            None,
            100,
        )
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "approval.decision_reserved")
            .count(),
        1
    );
    assert!(events.iter().all(|event| event.kind != "approval.resolved"));
    assert_eq!(
        snapshot
            .admissions
            .iter()
            .find(|row| row.turn_id == turn_id)
            .unwrap()
            .dispatch_state,
        runtime_core::TurnDispatchState::Dispatched
    );
    assert!(
        events.len() < 100,
        "the complete scoped fixture journal must fit this read"
    );
    db.execute_batch("DROP TRIGGER fail_native_decision_receipt")
        .unwrap();
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(provider.clone()).unwrap();
    let reopened =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(providers), 128).unwrap());
    let recovery = reopened.recover_startup().await.unwrap();
    assert_eq!(recovery.resumed_sessions, 0, "{recovery:?}");
    assert!(
        recovery
            .notes
            .iter()
            .any(|note| note.contains(&agent.agent_id)
                && note.contains("startup continuation withheld")
                && note.contains("provider approval decision is unresolved")),
        "{recovery:?}"
    );
    assert_eq!(provider.resumes.load(Ordering::SeqCst), before_resume);
    assert_eq!(provider.approval_responses.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider
            .turns_containing("retirement_wait_for_interrupt")
            .await,
        1
    );
    let after = store
        .repository()
        .get_persisted_retirement_review_session(&agent.agent_id)
        .unwrap()
        .unwrap();
    let approval = after
        .approvals
        .iter()
        .find(|row| row.id == pending.id)
        .unwrap();
    assert_eq!(approval.status, "decision_pending");
    assert_eq!(
        approval.response.as_ref().unwrap()["decision_attempt_id"],
        attempt_id
    );
    assert_eq!(
        after, snapshot,
        "startup must retain the exact session, turns, approvals and original dispatch admission"
    );
    assert_eq!(
        store
            .repository()
            .list_runtime_events(
                Some((RuntimeEventScope::Session, &agent.agent_id)),
                None,
                100,
            )
            .unwrap(),
        events,
        "withholding startup must not manufacture a native-decision or quarantine receipt"
    );
    let session = reopened.get_session(&agent.agent_id).await.unwrap();
    assert_eq!(session, snapshot.session);
    let (status, replay) = fixture
        .call("POST", &uri, Some(json!({"decision":"accept"})), None)
        .await;
    assert!(!status.is_success(), "{replay}");
    assert_eq!(provider.approval_responses.load(Ordering::SeqCst), 1);
}
