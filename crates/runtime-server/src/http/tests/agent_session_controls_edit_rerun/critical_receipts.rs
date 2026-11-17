//! Authenticated fake-provider proofs for canonical edit receipt failures.
use super::*;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;

fn body() -> Value {
    json!({"edited_snapshot":{"prompt_text":"receipt-owned replacement","image_refs":[],"invocation":null}})
}

fn original_edit(db: &Connection, agent: &str) -> Option<Value> {
    let raw: Option<String> = db
        .query_row(
            "SELECT record_json FROM session_edit_reruns WHERE session_id=?1",
            [agent],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    raw.map(|raw| serde_json::from_str(&raw).unwrap())
}

fn attachment(db: &Connection, agent: &str) -> (Option<String>, Option<String>, i64) {
    db.query_row(
        "SELECT s.provider_session_ref,s.canonical_provider_session_ref,a.revision
         FROM sessions s JOIN workspace_agents a ON a.session_id=s.id WHERE s.id=?1",
        [agent],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn edit_event_count(db: &Connection, agent: &str, kind: &str) -> i64 {
    db.query_row(
        "SELECT COUNT(*) FROM runtime_events WHERE session_id=?1 AND kind=?2",
        rusqlite::params![agent, kind],
        |row| row.get(0),
    )
    .unwrap()
}

fn require_owner(error: &Value, agent: &str, owner: &str) {
    let details = &error["error"]["details"];
    assert_eq!(details["family"], "edit_rerun", "{error}");
    assert_eq!(details["agent_id"], agent);
    assert_eq!(details["operation_id"], owner);
    assert_eq!(
        details["inspection_path"],
        format!("/v2/agents/{agent}/edits/{owner}")
    );
    assert_eq!(details["native_retry_authorized"], false);
}

#[tokio::test]
async fn edit_admission_and_native_start_event_failures_never_grant_a_fork() {
    for (kind, saved_phase) in [
        ("turn.edit_admitted", None),
        ("turn.edit_native_started", Some("intended")),
    ] {
        let (router, token, temp, provider, agent, target) = ready_edit_agent(1).await;
        let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
        let before = attachment(&db, &agent);
        let sends = provider.send_invocations.load(Ordering::SeqCst);
        let uri = format!("/v2/agents/{agent}/turns/{target}/edit-rerun");
        db.execute_batch(&format!(
            "CREATE TRIGGER n04_http_receipt_fault BEFORE INSERT ON runtime_events
             WHEN NEW.kind='{kind}'
             BEGIN SELECT RAISE(ABORT,'injected exact edit Critical failure'); END;"
        ))
        .unwrap();
        let (status, response) = call(
            &router,
            &token,
            "POST",
            &uri,
            Some(body()),
            Some("n04-before-fork"),
        )
        .await;
        assert!(!status.is_success(), "{response}");
        assert_eq!(provider.edit_calls(), 0);
        assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
        assert_eq!(attachment(&db, &agent), before);
        assert_eq!(edit_event_count(&db, &agent, kind), 0);
        let saved = original_edit(&db, &agent);
        assert_eq!(
            saved.as_ref().and_then(|op| op["phase"].as_str()),
            saved_phase
        );
        if let Some(saved) = saved {
            require_owner(&response, &agent, saved["operation_id"].as_str().unwrap());
            // Preparation may already have performed native work. Do not
            // reinterpret a failed native-start receipt as NotDispatched.
            assert!(response["error"]["details"]
                .get("dispatch_outcome")
                .is_none());
            let (status, replay) = call(
                &router,
                &token,
                "POST",
                &uri,
                Some(body()),
                Some("n04-before-fork"),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{replay}");
            assert_eq!(replay, saved);
            assert_eq!(provider.edit_calls(), 0);
            assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
        } else {
            db.execute_batch("DROP TRIGGER n04_http_receipt_fault")
                .unwrap();
            let (status, accepted) = call(
                &router,
                &token,
                "POST",
                &uri,
                Some(body()),
                Some("n04-before-fork"),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{accepted}");
            assert_eq!(accepted["phase"], "completed");
            assert_eq!(provider.edit_calls(), 1);
            assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends + 1);
            for phase in ["admitted", "native_started", "fork_observed", "completed"] {
                assert_eq!(
                    edit_event_count(&db, &agent, &format!("turn.edit_{phase}")),
                    1
                );
            }
        }
    }
}

#[tokio::test]
async fn failed_fork_receipt_retains_unknown_native_child_without_repeating_fork_or_send() {
    let (router, token, temp, provider, agent, target) = ready_edit_agent(1).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let before = attachment(&db, &agent);
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    db.execute_batch(
        "CREATE TRIGGER n04_http_receipt_fault BEFORE INSERT ON runtime_events
         WHEN NEW.kind='turn.edit_fork_observed'
         BEGIN SELECT RAISE(ABORT,'injected fork receipt failure'); END;",
    )
    .unwrap();
    let uri = format!("/v2/agents/{agent}/turns/{target}/edit-rerun");
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body()),
        Some("n04-fork-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    let saved = original_edit(&db, &agent).unwrap();
    let owner = saved["operation_id"].as_str().unwrap();
    require_owner(&error, &agent, owner);
    assert_eq!(saved["phase"], "native_started");
    assert!(saved["child_provider_session_ref"].is_null());
    assert!(saved["replacement_turn_id"].is_null());
    assert_eq!(attachment(&db, &agent), before);
    assert_eq!(
        provider.edit_calls(),
        1,
        "one real fake-provider hard fork already occurred"
    );
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
    assert_eq!(edit_event_count(&db, &agent, "turn.edit_fork_observed"), 0);
    let correlated: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM turn_admissions WHERE session_id=?1
         AND json_extract(correlation_json,'$.correlation_id')=?2",
            rusqlite::params![agent, owner],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(correlated, 0);

    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body()),
        Some("n04-fork-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, saved);
    db.execute_batch("DROP TRIGGER n04_http_receipt_fault")
        .unwrap();
    let repository =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let recovery_at = saved["updated_at_ms"].as_i64().unwrap() + 1;
    let receipts = repository
        .recover_session_edit_reruns_with_receipts(recovery_at)
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].operation.phase, "manual_review");
    assert!(receipts[0].operation.child_provider_session_ref.is_none());
    let held = original_edit(&db, &agent).unwrap();
    let event_count = edit_event_count(&db, &agent, "turn.edit_manual_review");
    let (status, review) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent}/edits/{owner}/resolve"),
        Some(json!({"evidence":"A prose statement cannot prove the unknown child"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{review}");
    assert_eq!(original_edit(&db, &agent).unwrap(), held);
    assert_eq!(
        edit_event_count(&db, &agent, "turn.edit_manual_review"),
        event_count
    );
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
}

#[tokio::test]
async fn failed_completion_event_keeps_real_replacement_and_recovery_settles_without_native_replay()
{
    let (router, token, temp, provider, agent, target) = ready_edit_agent(1).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    db.execute_batch(
        "CREATE TRIGGER n04_http_receipt_fault BEFORE INSERT ON runtime_events
         WHEN NEW.kind='turn.edit_completed'
         BEGIN SELECT RAISE(ABORT,'injected completion receipt failure'); END;",
    )
    .unwrap();
    let uri = format!("/v2/agents/{agent}/turns/{target}/edit-rerun");
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body()),
        Some("n04-completion-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    let saved = original_edit(&db, &agent).unwrap();
    let owner = saved["operation_id"].as_str().unwrap();
    require_owner(&error, &agent, owner);
    assert_eq!(saved["phase"], "fork_observed");
    assert!(saved["child_provider_session_ref"].is_string());
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends + 1);
    let replacement: (String, String, Option<String>) = db
        .query_row(
            "SELECT turn_id,dispatch_state,provider_native_turn_id FROM turn_admissions
         WHERE session_id=?1 AND json_extract(correlation_json,'$.correlation_id')=?2",
            rusqlite::params![agent, owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(replacement.1, "dispatched");
    assert!(replacement.2.is_some());
    assert_eq!(
        error["error"]["details"]["replacement_turn_id"],
        replacement.0
    );
    assert!(error["error"]["details"].get("dispatch_outcome").is_none());
    assert_eq!(edit_event_count(&db, &agent, "turn.edit_completed"), 0);
    assert_eq!(db.query_row(
        "SELECT COUNT(*) FROM runtime_events WHERE event_id=?1 AND kind='turn.admitted' AND critical=1",
        [format!("evt_turn_{}_admitted", replacement.0)], |r| r.get::<_, i64>(0),
    ).unwrap(), 1);

    db.execute_batch("DROP TRIGGER n04_http_receipt_fault")
        .unwrap();
    let repository =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let recovery_at = saved["updated_at_ms"].as_i64().unwrap() + 1;
    let receipts = repository
        .recover_session_edit_reruns_with_receipts(recovery_at)
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].operation.phase, "completed");
    assert_eq!(
        receipts[0].operation.replacement_turn_id.as_deref(),
        Some(replacement.0.as_str())
    );
    assert_eq!(
        receipts[0].journal_event.as_ref().unwrap().payload["origin"],
        "startup"
    );
    assert_eq!(edit_event_count(&db, &agent, "turn.edit_completed"), 1);
    assert!(repository
        .recover_session_edit_reruns_with_receipts(recovery_at + 1)
        .unwrap()
        .is_empty());
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body()),
        Some("n04-completion-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["operation_id"], owner);
    assert_eq!(replay["phase"], "completed");
    assert_eq!(replay["replacement_turn_id"], replacement.0);
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends + 1);
    let state: (String, Option<String>) = db
        .query_row(
            "SELECT dispatch_state,provider_native_turn_id FROM turn_admissions WHERE turn_id=?1",
            [&replacement.0],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (replacement.1, replacement.2));
}
