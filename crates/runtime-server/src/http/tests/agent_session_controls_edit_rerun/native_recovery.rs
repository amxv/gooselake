//! Existing HTTP entry points must expose their real family-owned receipt.
use super::*;
use rusqlite::{types::Value as SqlValue, Connection};
use serde_json::{json, Value};
use std::collections::BTreeMap;
fn edit_input() -> Value {
    json!({"edited_snapshot":{"prompt_text":"retain this original edit owner","image_refs":[],"invocation":null}})
}
fn assert_family(
    error: &Value,
    family: &str,
    agent: &str,
    field: &str,
    owner: &str,
    collection: &str,
) {
    let details = &error["error"]["details"];
    assert_eq!(details["family"], family, "{error}");
    assert_eq!(details["agent_id"], agent);
    assert_eq!(details[field], owner);
    assert_eq!(
        details["inspection_path"],
        format!("/v2/agents/{agent}/{collection}/{owner}")
    );
    assert_eq!(details["native_retry_authorized"], false);
    if family == "compaction" {
        assert!(details.get("operation_id").is_none());
    }
}
#[tokio::test]
async fn unkeyed_unknown_edit_returns_its_existing_family_owner() {
    let (router, token, temp, provider, agent, turn) = ready_edit_agent(2).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let canonical_before: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_operations", [], |r| r.get(0))
        .unwrap();
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent}/turns/{turn}/edit-rerun"),
        Some(edit_input()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "native_fork_outcome_unknown");
    let owner: String = db
        .query_row(
            "SELECT operation_id FROM session_edit_reruns WHERE session_id=?1",
            [&agent],
            |r| r.get(0),
        )
        .unwrap();
    assert_family(
        &error,
        "edit_rerun",
        &agent,
        "operation_id",
        &owner,
        "edits",
    );
    let (status, saved) = call(
        &router,
        &token,
        "GET",
        error["error"]["details"]["inspection_path"]
            .as_str()
            .unwrap(),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["operation_id"], owner);
    assert_eq!(saved["phase"], "manual_review");
    assert_eq!(provider.edit_calls(), 1);
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent}/turns/{turn}/edit-rerun"),
        Some(edit_input()),
        Some("not-authority-to-repeat"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.edit_calls(), 1);
    let canonical_after: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_operations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        canonical_after, canonical_before,
        "edit must not manufacture runtime operation ownership"
    );
}
#[tokio::test]
async fn unkeyed_unknown_compaction_returns_its_existing_family_owner() {
    let (router, token, temp, provider, agent, _) = ready_edit_agent(1).await;
    provider.set_compaction_mode(3);
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let canonical_before: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_operations", [], |r| r.get(0))
        .unwrap();
    let uri = format!("/v2/agents/{agent}/compact");
    let (status, error) = call(&router, &token, "POST", &uri, None, None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "native_compaction_outcome_unknown");
    let owner: String = db
        .query_row(
            "SELECT run_id FROM session_compaction_runs WHERE session_id=?1",
            [&agent],
            |r| r.get(0),
        )
        .unwrap();
    assert_family(
        &error,
        "compaction",
        &agent,
        "run_id",
        &owner,
        "compactions",
    );
    let (status, saved) = call(
        &router,
        &token,
        "GET",
        error["error"]["details"]["inspection_path"]
            .as_str()
            .unwrap(),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["run_id"], owner);
    assert_eq!(saved["phase"], "manual_review");
    assert_eq!(provider.compaction_calls(), 1);
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &uri,
        None,
        Some("not-authority-to-repeat"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.compaction_calls(), 1);
    let canonical_after: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_operations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        canonical_after, canonical_before,
        "compaction must not manufacture runtime operation ownership"
    );
}
fn protected_compaction_rows(db: &Connection) -> BTreeMap<String, Vec<Vec<SqlValue>>> {
    [
        ("run", "SELECT * FROM session_compaction_runs"),
        ("admissions", "SELECT * FROM turn_admissions"),
        (
            "events",
            "SELECT * FROM runtime_events WHERE kind LIKE 'compaction.%'",
        ),
        (
            "notices",
            "SELECT * FROM workspace_system_notices WHERE kind LIKE '%compaction%'",
        ),
        ("hooks", "SELECT * FROM workspace_hook_effects"),
    ]
    .into_iter()
    .map(|(name, sql)| {
        let mut statement = db.prepare(sql).unwrap();
        let n = statement.column_count();
        let mut rows = statement
            .query_map([], |r| {
                (0..n)
                    .map(|i| r.get::<_, SqlValue>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        rows.sort_by_cached_key(|r| format!("{r:?}"));
        (name.to_owned(), rows)
    })
    .collect()
}
#[tokio::test]
async fn prose_cannot_settle_unknown_compaction_or_release_its_fence() {
    let (router, token, temp, provider, agent, _) = ready_edit_agent(1).await;
    provider.set_compaction_mode(3);
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent}/compact"),
        None,
        Some("original-unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.compaction_calls(), 1);
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let owner: String = db
        .query_row(
            "SELECT run_id FROM session_compaction_runs WHERE session_id=?1",
            [&agent],
            |r| r.get(0),
        )
        .unwrap();
    let before = protected_compaction_rows(&db);
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    for phase in ["failed", "completed"] {
        let (status,error)=call(&router,&token,"POST",&format!("/v2/agents/{agent}/compactions/{owner}/resolve"),
            Some(json!({"phase":phase,"evidence":"An assertion is not independent native outcome evidence"})),None).await;
        assert_eq!(status, StatusCode::CONFLICT, "{error}");
        assert_eq!(error["error"]["code"], "compaction_resolution_unsupported");
        assert_family(
            &error,
            "compaction",
            &agent,
            "run_id",
            &owner,
            "compactions",
        );
        assert_eq!(protected_compaction_rows(&db), before);
        assert_eq!(provider.compaction_calls(), 1);
        let (status, record) = call(
            &router,
            &token,
            "GET",
            &format!("/v2/agents/{agent}/compactions/{owner}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(record["phase"], "manual_review");
        assert!(record["review_evidence"].is_null());
        let (status, _) = call(
            &router,
            &token,
            "POST",
            &format!("/v2/agents/{agent}/turns"),
            Some(json!({"input":[{"type":"text","text":"must remain fenced"}]})),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(provider.compaction_calls(), 1);
        assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
        assert_eq!(protected_compaction_rows(&db), before);
    }
}

use std::sync::atomic::Ordering;

#[tokio::test]
async fn edit_completion_receipt_fault_keeps_the_known_correlated_replacement() {
    let (router, token, temp, provider, agent, turn) = ready_edit_agent(1).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    db.execute_batch("CREATE TRIGGER reject_edit_completion_receipt BEFORE UPDATE OF phase ON session_edit_reruns
        WHEN OLD.phase='fork_observed' AND NEW.phase='completed'
        BEGIN SELECT RAISE(ABORT,'injected edit completion receipt fault'); END;").unwrap();
    let uri = format!("/v2/agents/{agent}/turns/{turn}/edit-rerun");
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("completion-receipt-fault"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    let saved = edit_row(&db, &agent);
    let owner = saved["operation_id"].as_str().unwrap();
    let replacement = correlated_replacement(&db, &agent, owner);
    assert_family(&error, "edit_rerun", &agent, "operation_id", owner, "edits");
    assert_eq!(
        error["error"]["details"]["replacement_turn_id"],
        replacement.0
    );
    assert!(error["error"]["details"].get("dispatch_outcome").is_none());
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("injected edit completion receipt fault"));
    assert_eq!(saved["phase"], "fork_observed");
    assert!(saved["replacement_turn_id"].is_null());
    assert!(saved["child_provider_session_ref"]
        .as_str()
        .unwrap()
        .starts_with("child-of-"));
    assert_ne!(replacement.0, turn);
    assert_eq!(replacement.1, "dispatched");
    assert_eq!(replacement.2, Some(format!("native-{}", replacement.0)));
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends + 1);
    db.execute_batch("DROP TRIGGER reject_edit_completion_receipt")
        .unwrap();
    let router = reopen_controls(&temp, &token, provider.clone()).await;
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("completion-receipt-fault"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(
        replay, saved,
        "inspection cannot replay or repair the native operation"
    );
    assert_eq!(correlated_replacement(&db, &agent, owner), replacement);
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends + 1);
}

#[tokio::test]
async fn known_not_performed_compaction_receipt_fault_is_not_native_unknown() {
    let (router, token, temp, provider, agent, _) = ready_edit_agent(1).await;
    provider.set_compaction_mode(2);
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    db.execute_batch("CREATE TRIGGER reject_not_performed_receipt BEFORE INSERT ON runtime_events
        WHEN NEW.kind='compaction.request_state_changed' AND json_extract(NEW.payload_json,'$.phase')='not_performed'
        BEGIN SELECT RAISE(ABORT,'injected not-performed receipt fault'); END;").unwrap();
    let uri = format!("/v2/agents/{agent}/compact");
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &uri,
        None,
        Some("known-not-performed-receipt-fault"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    let raw: String = db
        .query_row(
            "SELECT record_json FROM session_compaction_runs WHERE session_id=?1",
            [&agent],
            |row| row.get(0),
        )
        .unwrap();
    let saved: Value = serde_json::from_str(&raw).unwrap();
    let owner = saved["run_id"].as_str().unwrap();
    assert_family(&error, "compaction", &agent, "run_id", owner, "compactions");
    assert_ne!(error["error"]["code"], "native_compaction_outcome_unknown");
    assert!(error["error"]["details"].get("dispatch_outcome").is_none());
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("injected not-performed receipt fault"));
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not_performed"));
    assert_eq!(saved["phase"], "intended");
    assert!(saved["outcome"].is_null());
    assert!(saved["error_code"].is_null());
    assert_eq!(provider.compaction_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
    let receipts: i64 = db.query_row("SELECT COUNT(*) FROM runtime_events WHERE kind='compaction.request_state_changed' AND json_extract(payload_json,'$.run_id')=?1", [owner], |row| row.get(0)).unwrap();
    assert_eq!(receipts, 0);
    let before = protected_compaction_rows(&db);
    db.execute_batch("DROP TRIGGER reject_not_performed_receipt")
        .unwrap();
    let router = reopen_controls(&temp, &token, provider.clone()).await;
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        None,
        Some("known-not-performed-receipt-fault"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, saved);
    assert_eq!(protected_compaction_rows(&db), before);
    assert_eq!(provider.compaction_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
}

fn edit_row(db: &Connection, agent: &str) -> Value {
    let count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM session_edit_reruns WHERE session_id=?1",
            [agent],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "one real edit owner");
    let raw: String = db
        .query_row(
            "SELECT record_json FROM session_edit_reruns WHERE session_id=?1",
            [agent],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

fn correlated_replacement(
    db: &Connection,
    agent: &str,
    owner: &str,
) -> (String, String, Option<String>) {
    let count: i64 = db.query_row("SELECT COUNT(*) FROM turn_admissions WHERE session_id=?1 AND json_extract(correlation_json,'$.correlation_id')=?2", rusqlite::params![agent, owner], |row| row.get(0)).unwrap();
    assert_eq!(count, 1, "replacement has real durable correlation");
    db.query_row("SELECT turn_id,dispatch_state,provider_native_turn_id FROM turn_admissions WHERE session_id=?1 AND json_extract(correlation_json,'$.correlation_id')=?2", rusqlite::params![agent, owner], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap()
}

async fn reopen_controls(
    temp: &tempfile::TempDir,
    token: &str,
    provider: Arc<TestProvider>,
) -> Router {
    let store: Arc<dyn runtime_core::RuntimeStore> =
        Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp.path().join("runtime.sqlite3"),
        }));
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(provider).unwrap();
    let (router, _, _) = assemble_test_router(
        temp.path(),
        store,
        Arc::new(providers),
        token,
        TeamMcpPolicy::default(),
    )
    .await;
    // Reconstructs HTTP/runtime without invoking recover_startup or granting
    // another native attachment attempt.
    router
}

fn assert_edit_legacy_pair(db: &Connection, saved: &Value, agent: &str) {
    let owner = saved["operation_id"].as_str().unwrap();
    let observation = &saved["legacy_native_recovery_evidence"];
    let recovery: runtime_core::legacy_spawn_obligations::LegacyNativeRecovery =
        serde_json::from_value(observation["recovery"].clone()).unwrap();
    let details = recovery.error_details();
    assert_eq!(details["kind"], "blocked");
    assert_eq!(details["reason"], "exact_session");
    assert_eq!(
        details["coordinate"]["operation_id"],
        "historical/edit ?#%é"
    );
    assert_eq!(details["coordinate"]["team_id"], "original-edit-team");
    assert_eq!(details["coordinate"]["stage"], "session_created");
    assert_eq!(details["native_retry_authorized"], false);
    assert_eq!(
        details["inspection_path"],
        runtime_core::error::legacy_team_operation_inspection_path("historical/edit ?#%é")
    );
    let count: i64 = db.query_row("SELECT COUNT(*) FROM runtime_events WHERE kind='session_control.legacy_native_recovery_observed' AND json_extract(payload_json,'$.owner.operation_id')=?1", [owner], |row| row.get(0)).unwrap();
    assert_eq!(count, 1);
    let event: (String, i64, String, String, String, i64) = db.query_row("SELECT payload_json,created_at,scope,scope_id,session_id,critical FROM runtime_events WHERE kind='session_control.legacy_native_recovery_observed' AND json_extract(payload_json,'$.owner.operation_id')=?1", [owner], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?))).unwrap();
    let payload: Value = serde_json::from_str(&event.0).unwrap();
    assert_eq!(
        payload["owner"],
        json!({"family":"edit_rerun","agent_id":agent,"operation_id":owner})
    );
    assert_eq!(payload["legacy_recovery"], observation["recovery"]);
    assert_eq!(payload["native_retry_authorized"], false);
    assert_eq!(json!(event.1), observation["observed_at_ms"]);
    assert_eq!(event.2, "session");
    assert_eq!(event.3, agent);
    assert_eq!(event.4, agent);
    assert_eq!(event.5, 1);
}

#[tokio::test]
async fn edit_preparation_not_dispatched_keeps_owner_without_fork_or_resume() {
    let (router, token, temp, provider, agent, turn) = ready_edit_agent(3).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    let resumes = provider.resumed_requests().await.len();
    let attachment = provider
        .fake_observe_session(&agent)
        .await
        .unwrap()
        .unwrap();
    let uri = format!("/v2/agents/{agent}/turns/{turn}/edit-rerun");
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("preparation-not-dispatched"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    let saved = edit_row(&db, &agent);
    let owner = saved["operation_id"].as_str().unwrap();
    assert_family(&error, "edit_rerun", &agent, "operation_id", owner, "edits");
    assert_eq!(
        error["error"]["code"],
        "fixture_edit_preparation_not_dispatched"
    );
    assert_eq!(
        error["error"]["details"]["dispatch_outcome"],
        "not_dispatched"
    );
    assert!(error["error"]["details"]
        .get("replacement_turn_id")
        .is_none());
    assert_eq!(saved["phase"], "manual_review");
    assert_eq!(
        saved["source_provider_session_ref"],
        attachment.provider_session_ref
    );
    assert!(saved["child_provider_session_ref"].is_null());
    assert!(saved["replacement_turn_id"].is_null());
    assert_eq!(provider.edit_calls(), 0);
    assert_eq!(provider.resumed_requests().await.len(), resumes);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
    let after = provider
        .fake_observe_session(&agent)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.provider_session_ref, attachment.provider_session_ref);
    assert_eq!(
        after.canonical_provider_session_ref,
        attachment.canonical_provider_session_ref
    );
    let router = reopen_controls(&temp, &token, provider.clone()).await;
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("preparation-not-dispatched"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, saved);
    assert_eq!(provider.edit_calls(), 0);
    assert_eq!(provider.resumed_requests().await.len(), resumes);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
}

#[tokio::test]
async fn known_fork_keeps_real_replacement_when_its_first_dispatch_is_legacy_blocked() {
    let (router, token, temp, provider, agent, turn) = ready_edit_agent(1).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    // This owned fixture inserts the blocker after real replacement admission
    // and before its separate Pending -> Dispatching transaction.
    db.execute_batch("CREATE TRIGGER legacy_after_edit_replacement AFTER INSERT ON turn_admissions
        WHEN EXISTS (SELECT 1 FROM session_edit_reruns e WHERE e.session_id=NEW.session_id AND e.phase='fork_observed'
          AND e.operation_id=json_extract(NEW.correlation_json,'$.correlation_id'))
        BEGIN INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES ('historical/edit ?#%é','original-edit-team','spawn_member_with_worktree','session_created',
          json_object('spawned_session_id',NEW.session_id),10,20); END;").unwrap();
    let uri = format!("/v2/agents/{agent}/turns/{turn}/edit-rerun");
    let (status, saved) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("correlated-legacy-refusal"),
    )
    .await;
    // The fork and actual replacement admission are known. The edit's success
    // receipt does not assert that its replacement has been dispatched.
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["phase"], "completed");
    let owner = saved["operation_id"].as_str().unwrap();
    let replacement = correlated_replacement(&db, &agent, owner);
    assert_ne!(replacement.0, turn);
    assert_eq!(saved["replacement_turn_id"], replacement.0);
    assert_eq!(replacement.1, "pending");
    assert_eq!(replacement.2, None);
    assert!(saved["child_provider_session_ref"]
        .as_str()
        .unwrap()
        .starts_with("child-of-"));
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
    assert_edit_legacy_pair(&db, &saved, &agent);
    assert_eq!(edit_row(&db, &agent), saved);
    db.execute_batch("DROP TRIGGER legacy_after_edit_replacement")
        .unwrap();
    assert_eq!(
        db.execute(
            "DELETE FROM team_operation_journal WHERE operation_id=?1",
            ["historical/edit ?#%é"]
        )
        .unwrap(),
        1
    );
    let router = reopen_controls(&temp, &token, provider.clone()).await;
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("correlated-legacy-refusal"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, saved);
    assert_eq!(correlated_replacement(&db, &agent, owner), replacement);
    assert_edit_legacy_pair(&db, &replay, &agent);
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
}

fn assert_current_legacy_cause(error: &Value) {
    let cause = &error["error"]["details"]["legacy_recovery"];
    assert_eq!(cause["kind"], "blocked");
    assert_eq!(cause["reason"], "exact_session");
    assert_eq!(cause["coordinate"]["operation_id"], "historical/edit ?#%é");
    assert_eq!(cause["coordinate"]["team_id"], "original-edit-team");
    assert_eq!(cause["coordinate"]["stage"], "session_created");
    assert_eq!(
        cause["inspection_path"],
        runtime_core::error::legacy_team_operation_inspection_path("historical/edit ?#%é")
    );
    assert_eq!(cause["native_retry_authorized"], false);
}

#[tokio::test]
async fn legacy_after_verified_fork_preserves_cause_without_replacement_admission() {
    let (router, token, temp, provider, agent, turn) = ready_edit_agent(1).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    db.execute_batch("CREATE TRIGGER legacy_after_verified_edit_fork AFTER UPDATE OF phase ON session_edit_reruns
        WHEN OLD.phase='native_started' AND NEW.phase='fork_observed'
        BEGIN INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES ('historical/edit ?#%é','original-edit-team','spawn_member_with_worktree','session_created',
          json_object('spawned_session_id',NEW.session_id),10,20); END;").unwrap();
    let uri = format!("/v2/agents/{agent}/turns/{turn}/edit-rerun");
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("legacy-after-fork"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "replacement_admission_unverified");
    let saved = edit_row(&db, &agent);
    let owner = saved["operation_id"].as_str().unwrap();
    assert_family(&error, "edit_rerun", &agent, "operation_id", owner, "edits");
    assert_current_legacy_cause(&error);
    assert_eq!(error["error"]["details"]["dispatch_outcome"], "unknown");
    assert!(error["error"]["details"]
        .get("replacement_turn_id")
        .is_none());
    assert_eq!(saved["phase"], "manual_review");
    assert!(saved["replacement_turn_id"].is_null());
    assert!(saved["child_provider_session_ref"]
        .as_str()
        .unwrap()
        .starts_with("child-of-"));
    let replacements: i64 = db.query_row("SELECT COUNT(*) FROM turn_admissions WHERE session_id=?1 AND json_extract(correlation_json,'$.correlation_id')=?2", rusqlite::params![agent, owner], |r| r.get(0)).unwrap();
    assert_eq!(replacements, 0);
    assert_edit_legacy_pair(&db, &saved, &agent);
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
    db.execute_batch("DROP TRIGGER legacy_after_verified_edit_fork")
        .unwrap();
    db.execute(
        "DELETE FROM team_operation_journal WHERE operation_id=?1",
        ["historical/edit ?#%é"],
    )
    .unwrap();
    let router = reopen_controls(&temp, &token, provider.clone()).await;
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("legacy-after-fork"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, saved);
    assert_edit_legacy_pair(&db, &replay, &agent);
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
}

#[tokio::test]
async fn paired_cause_receipt_failure_keeps_actual_replacement_and_current_typed_cause() {
    let (router, token, temp, provider, agent, turn) = ready_edit_agent(1).await;
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let sends = provider.send_invocations.load(Ordering::SeqCst);
    db.execute_batch("CREATE TRIGGER legacy_after_actual_edit_replacement AFTER INSERT ON turn_admissions
        WHEN EXISTS (SELECT 1 FROM session_edit_reruns e WHERE e.session_id=NEW.session_id AND e.phase='fork_observed'
          AND e.operation_id=json_extract(NEW.correlation_json,'$.correlation_id'))
        BEGIN INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES ('historical/edit ?#%é','original-edit-team','spawn_member_with_worktree','session_created',
          json_object('spawned_session_id',NEW.session_id),10,20); END;
        CREATE TRIGGER refuse_edit_recovery_pair BEFORE INSERT ON runtime_events
        WHEN NEW.kind='session_control.legacy_native_recovery_observed'
        BEGIN SELECT RAISE(ABORT,'injected paired legacy observation fault'); END;").unwrap();
    let uri = format!("/v2/agents/{agent}/turns/{turn}/edit-rerun");
    let (status, error) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("pair-receipt-fault"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(
        error["error"]["code"],
        "session_control_legacy_receipt_unavailable"
    );
    let saved = edit_row(&db, &agent);
    let owner = saved["operation_id"].as_str().unwrap();
    let replacement = correlated_replacement(&db, &agent, owner);
    assert_family(&error, "edit_rerun", &agent, "operation_id", owner, "edits");
    assert_current_legacy_cause(&error);
    assert_eq!(
        error["error"]["details"]["replacement_turn_id"],
        replacement.0
    );
    assert_eq!(replacement.1, "pending");
    assert_eq!(replacement.2, None);
    assert_eq!(saved["phase"], "fork_observed");
    assert!(saved["replacement_turn_id"].is_null());
    assert!(saved["legacy_native_recovery_evidence"].is_null());
    let pairs: i64 = db.query_row("SELECT COUNT(*) FROM runtime_events WHERE kind='session_control.legacy_native_recovery_observed'", [], |r| r.get(0)).unwrap();
    assert_eq!(pairs, 0);
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
    db.execute_batch("DROP TRIGGER legacy_after_actual_edit_replacement; DROP TRIGGER refuse_edit_recovery_pair;").unwrap();
    db.execute(
        "DELETE FROM team_operation_journal WHERE operation_id=?1",
        ["historical/edit ?#%é"],
    )
    .unwrap();
    let router = reopen_controls(&temp, &token, provider.clone()).await;
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(edit_input()),
        Some("pair-receipt-fault"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, saved);
    assert_eq!(correlated_replacement(&db, &agent, owner), replacement);
    assert_eq!(provider.edit_calls(), 1);
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), sends);
}
