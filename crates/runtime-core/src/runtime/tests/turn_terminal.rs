use std::sync::Arc;

use super::super::test_support::MockStore;
use super::*;
use crate::RuntimeSessionManager;

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
