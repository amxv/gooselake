use super::*;
use crate::RuntimeEventCriticality;

fn input() -> CreateSessionInput {
    CreateSessionInput {
        provider: ProviderKind::Codex,
        model: None,
        cwd: None,
        permission_mode: None,
        metadata: None,
    }
}

#[tokio::test]
async fn native_session_create_journal_fault_rolls_back_all_cached_and_durable_authority() {
    let (manager, store) = manager_with_provider_and_store(0);
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("session.created".into());
    let result = manager.create_session(input()).await;
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native_session_creation_unjournaled")
            || error
                .to_string()
                .contains("critical durable admission failed")
    );
    assert!(manager.sessions.read().await.is_empty());
    assert!(store.sessions.lock().unwrap().is_empty());
    assert!(store.events.lock().unwrap().is_empty());
    let created = manager.create_session(input()).await.unwrap();
    assert_eq!(manager.get_session(&created.id).await.unwrap(), created);
    assert_eq!(store.sessions.lock().unwrap()[&created.id], created);
    let rows = store.events.lock().unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| row.kind == "session.created")
            .count(),
        1
    );
    assert_eq!(
        rows[0].event_id,
        format!("evt_session_{}_created", created.id)
    );
    assert_eq!(rows[0].criticality, RuntimeEventCriticality::Critical);
}

#[tokio::test]
async fn native_session_close_receipt_fault_does_not_publish_uncommitted_closure() {
    let (manager, store) = manager_with_provider_and_store(0);
    let created = manager.create_session(input()).await.unwrap();
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("session.closed".into());
    let error = manager
        .close_session(&created.id, Some("operator_requested".into()))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native_session_close_unjournaled")
            || error.to_string().contains("native close acknowledged")
    );
    assert_eq!(manager.get_session(&created.id).await.unwrap(), created);
    assert_eq!(store.sessions.lock().unwrap()[&created.id], created);
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|row| row.kind != "session.closed"));

    // Explicit local reconciliation is allowed; it never resends native close.
    let reconciled = manager
        .force_close_session(&created.id, Some("operator_reconciled".into()))
        .await
        .unwrap();
    assert_eq!(reconciled.status, "closed");
    assert!(reconciled.closed_at.is_some());
    let rows = store.events.lock().unwrap();
    let closed = rows
        .iter()
        .find(|row| row.kind == "session.closed")
        .unwrap();
    assert_eq!(
        closed.event_id,
        format!("evt_session_{}_closed", created.id)
    );
    assert_eq!(closed.payload["reason"], "operator_reconciled");
    assert_eq!(closed.criticality, RuntimeEventCriticality::Critical);
    drop(rows);
    assert_eq!(
        manager.close_session(&created.id, None).await.unwrap(),
        reconciled
    );
    assert_eq!(
        store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.kind == "session.closed")
            .count(),
        1
    );
}

#[tokio::test]
async fn native_session_resume_receipt_fault_preserves_original_session_projection() {
    let (manager, store) = manager_with_provider_and_store(0);
    let created = manager.create_session(input()).await.unwrap();
    *store.fail_next_event_append_for_kind.lock().unwrap() = Some("session.resumed".into());
    let error = manager
        .resume_session(
            &created.id,
            ResumeSessionInput {
                provider_session_ref: None,
                canonical_provider_session_ref: None,
            },
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native_session_resume_unjournaled")
            || error.to_string().contains("native resume was acknowledged")
    );
    assert_eq!(manager.get_session(&created.id).await.unwrap(), created);
    assert_eq!(store.sessions.lock().unwrap()[&created.id], created);
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|row| row.kind != "session.resumed"));
}
