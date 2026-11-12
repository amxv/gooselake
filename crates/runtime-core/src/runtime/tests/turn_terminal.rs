use std::sync::Arc;

use super::super::test_support::MockStore;
use super::*;
use crate::{RuntimeEventCriticality, RuntimeSessionManager};

#[tokio::test]
async fn failed_native_event_stream_journal_never_publishes_partial_recovery_state() {
    let (manager, store, session, turn) = fixture(false).await;
    *store.fail_next_event_append_for_kind.lock().unwrap() =
        Some("provider.event_stream_unknown".into());
    assert!(manager
        .mark_provider_event_stream_unknown(
            &session.id,
            &turn.id,
            "stream_lost",
            "native stream disconnected".into(),
        )
        .await
        .is_err());
    assert_eq!(manager.sessions.read().await[&session.id], session);
    assert_eq!(manager.turns.read().await[&turn.id], turn);
    assert_eq!(store.sessions.lock().unwrap()[&session.id], session);
    assert_eq!(store.turns.lock().unwrap()[&turn.id], turn);
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|row| row.kind != "provider.event_stream_unknown"));

    manager
        .mark_provider_event_stream_unknown(
            &session.id,
            &turn.id,
            "stream_lost",
            "native stream disconnected".into(),
        )
        .await
        .unwrap();
    assert_eq!(
        manager.sessions.read().await[&session.id].status,
        "turn_recovery_required"
    );
    assert_eq!(
        manager.turns.read().await[&turn.id].status,
        "provider_event_recovery_required"
    );
    let events = store.events.lock().unwrap();
    let result = events
        .iter()
        .find(|row| row.kind == "provider.event_stream_unknown")
        .unwrap();
    assert_eq!(
        result.event_id,
        format!("evt_turn_{}_event_stream_unknown", turn.id)
    );
    assert_eq!(result.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(result.payload["code"], "stream_lost");
    drop(events);
    manager
        .mark_provider_event_stream_unknown(
            &session.id,
            &turn.id,
            "another_error",
            "different response".into(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.kind == "provider.event_stream_unknown")
            .count(),
        1,
        "the first durable quarantine remains canonical"
    );
}

#[tokio::test]
async fn failed_native_wait_journal_never_partially_terminalizes_turn_or_session() {
    let (manager, store, session, turn) = fixture(false).await;
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("provider.error".into());
    let err = RuntimeError::Io("native provider wait failed".into());
    assert!(manager
        .apply_terminal_failure(&session.id, &turn.id, err)
        .await
        .is_err());
    assert_eq!(manager.sessions.read().await[&session.id], session);
    assert_eq!(manager.turns.read().await[&turn.id], turn);
    assert_eq!(store.sessions.lock().unwrap()[&session.id], session);
    assert_eq!(store.turns.lock().unwrap()[&turn.id], turn);
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|row| row.kind != "provider.error"));

    manager
        .apply_terminal_failure(
            &session.id,
            &turn.id,
            RuntimeError::Io("native provider wait failed".into()),
        )
        .await
        .unwrap();
    assert_eq!(manager.sessions.read().await[&session.id].status, "failed");
    assert_eq!(
        manager.sessions.read().await[&session.id].active_turn_id,
        None
    );
    assert_eq!(manager.turns.read().await[&turn.id].status, "failed");
    assert!(manager.turns.read().await[&turn.id].completed_at.is_some());
    let events = store.events.lock().unwrap();
    let result = events
        .iter()
        .find(|row| row.kind == "provider.error")
        .unwrap();
    assert_eq!(
        result.event_id,
        format!("evt_turn_{}_wait_failure", turn.id)
    );
    assert_eq!(result.criticality, RuntimeEventCriticality::Critical);
    drop(events);
}

#[tokio::test]
async fn late_wait_error_cannot_clear_previously_unknown_native_effect_quarantine() {
    let (manager, store, session, turn) = fixture(false).await;
    manager
        .mark_provider_event_stream_unknown(
            &session.id,
            &turn.id,
            "native_stream_ambiguous",
            "read side disappeared".into(),
        )
        .await
        .unwrap();
    let before_session = manager.sessions.read().await[&session.id].clone();
    let before_turn = manager.turns.read().await[&turn.id].clone();
    manager
        .apply_terminal_failure(
            &session.id,
            &turn.id,
            RuntimeError::Io("follow-on wait error".into()),
        )
        .await
        .unwrap();
    assert_eq!(manager.sessions.read().await[&session.id], before_session);
    assert_eq!(manager.turns.read().await[&turn.id], before_turn);
    assert_eq!(store.sessions.lock().unwrap()[&session.id], before_session);
    assert_eq!(store.turns.lock().unwrap()[&turn.id], before_turn);
    let errors = store
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|row| row.kind == "provider.error")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].payload["state_preserved"], true);
    assert_eq!(errors[0].payload["reason"], "turn_recovery_quarantined");
}

async fn fixture(
    operator_closed: bool,
) -> (
    Arc<RuntimeSessionManager>,
    Arc<MockStore>,
    SessionRecord,
    TurnRecord,
) {
    let (manager, store) = manager_with_provider_and_store(0);
    let mut session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .unwrap();
    let turn = TurnRecord {
        id: "historical-native-turn".into(), session_id: session.id.clone(),
        provider_turn_ref: Some("native-turn".into()),
        status: if operator_closed { "interrupted" } else { "in_progress" }.into(),
        input: serde_json::json!([{"type":"text","text":"historical input"}]),
        source: Some("user".into()), started_at: Some(10),
        completed_at: operator_closed.then_some(20), usage: None,
        error: operator_closed.then(|| serde_json::json!({
            "code":"operator_verified_native_attachment_closed","native_result":"unknown",
            "proof":{"operation_id":"retirement-operation","provider_close_effect_id":"close-effect",
                "native_identity":{"provider":"codex","provider_session_ref":"closed-native"},
                "operator_review":{"actor_id":"operator","evidence":"exit observed"}},
            "prior_status":"provider_event_recovery_required",
        })),
    };
    session.status = if operator_closed {
        "closed"
    } else {
        "turn_running"
    }
    .into();
    session.active_turn_id = (!operator_closed).then(|| turn.id.clone());
    session.closed_at = operator_closed.then_some(20);
    store.upsert_turn(&turn).unwrap();
    store.upsert_session(&session).unwrap();
    manager
        .turns
        .write()
        .await
        .insert(turn.id.clone(), turn.clone());
    manager
        .sessions
        .write()
        .await
        .insert(session.id.clone(), session.clone());
    (manager, store, session, turn)
}

async fn callback(
    manager: &RuntimeSessionManager,
    session: &SessionRecord,
    turn: &TurnRecord,
    kind: usize,
) -> Result<bool, RuntimeError> {
    match kind {
        0 => {
            manager
                .apply_terminal_result(ProviderTurnResult {
                    runtime_session_id: session.id.clone(),
                    turn_id: turn.id.clone(),
                    status: ProviderTurnStatus::Completed,
                    usage: Some(
                        serde_json::json!({"last_message":"late native answer","input_tokens":100}),
                    ),
                    error: None,
                })
                .await
        }
        1 => manager
            .mark_provider_event_stream_unknown(
                &session.id,
                &turn.id,
                "late_native_stream",
                "native connection ended".into(),
            )
            .await
            .map(|_| false),
        _ => manager
            .apply_terminal_failure(
                &session.id,
                &turn.id,
                RuntimeError::Io("late native wait error".into()),
            )
            .await
            .map(|_| false),
    }
}

#[tokio::test]
async fn late_native_callbacks_preserve_operator_closure_after_restore_and_new_work() {
    let (manager, store, mut session, turn) = fixture(true).await;
    for lifecycle in ["closed", "ready", "turn_running"] {
        session.status = lifecycle.into();
        if lifecycle != "closed" {
            session.closed_at = None;
            session.provider_session_ref = Some("explicitly-restored-native".into());
        }
        let mut newer_turn = None;
        if lifecycle == "turn_running" {
            let mut newer = turn.clone();
            newer.id = "newer-native-turn".into();
            newer.status = "in_progress".into();
            newer.completed_at = None;
            newer.error = None;
            session.active_turn_id = Some(newer.id.clone());
            store.upsert_turn(&newer).unwrap();
            manager
                .turns
                .write()
                .await
                .insert(newer.id.clone(), newer.clone());
            newer_turn = Some(newer);
        }
        store.upsert_session(&session).unwrap();
        manager
            .sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());
        for kind in 0..3 {
            assert!(!callback(&manager, &session, &turn, kind).await.unwrap());
            assert_eq!(manager.get_session(&session.id).await.unwrap(), session);
            assert_eq!(manager.turns.read().await.get(&turn.id), Some(&turn));
            assert_eq!(
                store.sessions.lock().unwrap().get(&session.id),
                Some(&session)
            );
            assert_eq!(store.turns.lock().unwrap().get(&turn.id), Some(&turn));
            if let Some(newer) = &newer_turn {
                assert_eq!(manager.turns.read().await.get(&newer.id), Some(newer));
            }
        }
    }
    let events = store.events.lock().unwrap();
    let late = events
        .iter()
        .filter(|event| event.kind == "provider.late_turn_outcome")
        .collect::<Vec<_>>();
    assert_eq!(late.len(), 3);
    assert!(late
        .iter()
        .all(|event| event.payload["status"] == "completed"
            && event.payload["state_preserved"] == true
            && event.payload["reason"] == "operator_verified_native_attachment_closed"));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.kind.as_str(),
                "provider.event_stream_unknown" | "provider.error"
            ) && event.payload["state_preserved"] == true)
            .count(),
        6
    );
}

#[tokio::test]
async fn late_unknown_and_failure_cannot_reopen_terminal_turns_or_fail_a_new_active_turn() {
    let (manager, store, mut session, mut turn) = fixture(false).await;
    session.active_turn_id = Some("newer-active-turn".into());
    store.upsert_session(&session).unwrap();
    manager
        .sessions
        .write()
        .await
        .insert(session.id.clone(), session.clone());
    for status in ["completed", "cancelled", "in_progress"] {
        turn.status = status.into();
        turn.completed_at = (status != "in_progress").then_some(20);
        store.upsert_turn(&turn).unwrap();
        manager
            .turns
            .write()
            .await
            .insert(turn.id.clone(), turn.clone());
        for kind in [1, 2] {
            callback(&manager, &session, &turn, kind).await.unwrap();
            assert_eq!(manager.get_session(&session.id).await.unwrap(), session);
            assert_eq!(manager.turns.read().await.get(&turn.id), Some(&turn));
            assert_eq!(store.turns.lock().unwrap().get(&turn.id), Some(&turn));
        }
    }
    assert!(!callback(&manager, &session, &turn, 0).await.unwrap());
    assert_eq!(manager.get_session(&session.id).await.unwrap(), session);
    assert_eq!(
        store.sessions.lock().unwrap().get(&session.id),
        Some(&session)
    );
    assert_eq!(manager.turns.read().await[&turn.id].status, "completed");
}

#[tokio::test]
async fn rejected_callback_writes_never_publish_uncommitted_cache_projections() {
    for kind in 0..3 {
        let (manager, store, session, turn) = fixture(false).await;
        *store.fail_turn_write.lock().unwrap() = true;
        assert!(callback(&manager, &session, &turn, kind).await.is_err());
        assert_eq!(manager.get_session(&session.id).await.unwrap(), session);
        assert_eq!(manager.turns.read().await.get(&turn.id), Some(&turn));
    }
    for kind in 0..3 {
        let (manager, store, session, turn) = fixture(false).await;
        *store.fail_session_write.lock().unwrap() = true;
        assert!(callback(&manager, &session, &turn, kind).await.is_err());
        assert_eq!(manager.get_session(&session.id).await.unwrap(), session);
        assert_eq!(
            store.sessions.lock().unwrap().get(&session.id),
            Some(&session)
        );
        assert_eq!(
            manager.turns.read().await.get(&turn.id),
            store.turns.lock().unwrap().get(&turn.id)
        );
    }
}

#[tokio::test]
async fn terminal_journal_failure_preserves_turn_and_session_caches_until_atomic_receipt() {
    let (manager, store, session, turn) = fixture(false).await;
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("turn.completed".into());
    let result = callback(&manager, &session, &turn, 0).await;
    assert!(result.is_err());
    assert_eq!(manager.get_session(&session.id).await.unwrap(), session);
    assert_eq!(manager.turns.read().await.get(&turn.id), Some(&turn));
    assert_eq!(
        store.sessions.lock().unwrap().get(&session.id),
        Some(&session)
    );
    assert_eq!(store.turns.lock().unwrap().get(&turn.id), Some(&turn));
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|event| event.kind != "turn.completed"));

    assert!(callback(&manager, &session, &turn, 0).await.unwrap());
    assert_eq!(manager.turns.read().await[&turn.id].status, "completed");
    assert_eq!(
        manager.get_session(&session.id).await.unwrap().status,
        "ready"
    );
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.kind == "turn.completed")
            .count(),
        1,
    );
}
