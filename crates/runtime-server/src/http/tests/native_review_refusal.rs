use super::operator_agent_create::{create_http, fixture};
use super::operator_native_policy::{
    claude_permission_fixture, operator_policy_fixture, permission_body, preference_body,
};
use super::workspace_retirement_support::{retirement_fixture, RetirementProvider};
use super::workspace_worktree_routes::call;
use super::*;
use std::sync::atomic::Ordering;

pub(super) fn database_snapshot(db: &rusqlite::Connection) -> Vec<(String, Vec<Vec<String>>)> {
    let mut tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").unwrap();
    let names = tables
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    names
        .into_iter()
        .map(|name| {
            let mut statement = db
                .prepare(&format!("SELECT * FROM \"{}\"", name.replace('"', "\"\"")))
                .unwrap();
            let columns = statement.column_count();
            let mut rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| row.get_ref(index).map(|value| format!("{value:?}")))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            rows.sort();
            (name, rows)
        })
        .collect()
}
pub(super) fn assert_refusal(status: StatusCode, body: &serde_json::Value, operation_id: &str) {
    assert_eq!(status, StatusCode::CONFLICT, "{body:?}");
    assert_eq!(body["error"]["code"], "outcome_unresolved");
    assert_eq!(body["error"]["details"]["operation_id"], operation_id);
    assert_eq!(
        body["error"]["details"]["inspection_path"],
        format!("/v2/operations/{operation_id}")
    );
    assert_eq!(body["error"]["details"]["native_retry_authorized"], false);
}
pub(super) async fn refuse_prose_variants(
    router: &Router,
    token: &str,
    db: &rusqlite::Connection,
    uri: &str,
    operation_id: &str,
    inputs: Vec<serde_json::Value>,
) {
    let before = database_snapshot(db);
    for input in inputs {
        let mut changed = input.clone();
        changed["evidence"] = serde_json::json!(
            "A different operator assertion about the same original native attempt"
        );
        let mut first = None;
        for (body, key) in [
            (input.clone(), "d4-review-same"),
            (input, "d4-review-same"),
            (changed, "d4-review-different"),
        ] {
            let request_key = uri.ends_with("/reconcile").then_some(key);
            let (status, response) =
                call(router, token, "POST", uri, Some(body), request_key).await;
            assert_refusal(status, &response, operation_id);
            if key == "d4-review-same" {
                if let Some(first) = &first {
                    assert_eq!(&response["error"], first);
                } else {
                    first = Some(response["error"].clone());
                }
            }
            assert_eq!(
                database_snapshot(db),
                before,
                "prose review changed durable authority"
            );
        }
    }
}
#[tokio::test]
async fn create_prose_cannot_settle_or_release_the_original_unknown_attempt() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    provider.set_create_failure_mode(2);
    let calls = provider.create_call_count();
    let request = serde_json::json!({"provider":"codex","model":"test-model"});
    let (status, unknown, _) = create_http(
        &router,
        &token,
        &workspace,
        request.clone(),
        Some("d4-create-unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original = store.get_operation(id).unwrap().unwrap();
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(original.claims.len(), 1);
    let reserved:String=db.query_row("SELECT target_id FROM runtime_operation_effects WHERE operation_id=?1 AND effect_kind='provider_session_create'",[id],|row|row.get(0)).unwrap();
    assert!(provider.native_cwd(&reserved).await.is_some());
    let uri = format!("/v2/workspaces/{workspace}/agents/creations/{id}/resolve");
    let created = serde_json::json!({"resolution":"created","provider":"codex","reserved_agent_id":reserved,"evidence":"Operator asserts the exact reserved native attachment exists","observed_provider_session_ref":format!("test-thread-{reserved}"),"observed_canonical_provider_session_ref":null});
    let absent = serde_json::json!({"resolution":"not_created","provider":"codex","reserved_agent_id":reserved,"evidence":"Operator asserts the exact reserved native attachment is absent","observed_provider_session_ref":null,"observed_canonical_provider_session_ref":null});
    refuse_prose_variants(&router, &token, &db, &uri, id, vec![created, absent]).await;
    let (status, retry, _) = create_http(
        &router,
        &token,
        &workspace,
        request,
        Some("d4-create-unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["details"]["operation_id"], id);
    assert_eq!(store.get_operation(id).unwrap().unwrap(), original);
    assert!(runtime.get_session(&reserved).await.is_err());
    assert!(provider.native_cwd(&reserved).await.is_some());
    assert_eq!(provider.create_call_count(), calls + 1);
}
#[tokio::test]
async fn archive_prose_cannot_settle_a_lost_close_acknowledgement() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    let workspace = &agent.workspace_id;
    provider.set_archive_close_mode(2);
    let calls = provider.archive_close_count();
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace}/agents/{agent_id}/archive"),
        Some(serde_json::json!({"reason":"D4 lost close acknowledgement"})),
        Some("d4-archive-unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original = store.get_operation(id).unwrap().unwrap();
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(original.claims.len(), 1);
    let closed = serde_json::json!({"resolution":"closed","evidence":"Operator asserts the exact original native close completed","provider":session.provider,"original_provider_session_ref":session.provider_session_ref,"original_canonical_provider_session_ref":session.canonical_provider_session_ref,"expected_revision":revision,"session_updated_at":session.updated_at});
    let mut open = closed.clone();
    open["resolution"] = serde_json::json!("not_closed");
    open["observed_provider_session_ref"] = serde_json::json!(session.provider_session_ref);
    open["observed_canonical_provider_session_ref"] =
        serde_json::json!(session.canonical_provider_session_ref);
    let uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/archives/{id}/resolve");
    refuse_prose_variants(&router, &token, &db, &uri, id, vec![closed, open]).await;
    assert_eq!(store.get_operation(id).unwrap().unwrap(), original);
    assert_eq!(
        runtime.get_workspace_agent(workspace, &agent_id).unwrap(),
        agent
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), session);
    assert!(provider.native_cwd(&agent_id).await.is_none());
    assert_eq!(provider.archive_close_count(), calls + 1);
}
#[tokio::test]
async fn restore_prose_cannot_settle_or_reauthorize_an_unknown_resume() {
    let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let (status, archived) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace}/agents/{agent_id}/archive"),
        Some(serde_json::json!({"reason":"D4 known close before unknown resume"})),
        Some("d4-restore-close"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived:?}");
    let agent = runtime.get_workspace_agent(&workspace, &agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    provider.set_restore_failure_mode(2);
    let calls = provider.restore_call_count().await;
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace}/agents/{agent_id}/restore"),
        None,
        Some("d4-restore-unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original = store.get_operation(id).unwrap().unwrap();
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(original.claims.len(), 1);
    let absent = serde_json::json!({"resolution":"not_resumed","evidence":"Operator asserts the exact original native resume did not occur","provider":session.provider,"original_provider_session_ref":session.provider_session_ref,"original_canonical_provider_session_ref":session.canonical_provider_session_ref,"expected_revision":agent.revision,"session_updated_at":session.updated_at});
    let mut resumed = absent.clone();
    resumed["resolution"] = serde_json::json!("resumed");
    resumed["observed_provider_session_ref"] = serde_json::json!(session.provider_session_ref);
    resumed["observed_canonical_provider_session_ref"] =
        serde_json::json!(session.canonical_provider_session_ref);
    let uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/restores/{id}/resolve");
    refuse_prose_variants(&router, &token, &db, &uri, id, vec![resumed, absent]).await;
    assert_eq!(store.get_operation(id).unwrap().unwrap(), original);
    assert_eq!(
        runtime.get_workspace_agent(&workspace, &agent_id).unwrap(),
        agent
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), session);
    assert!(provider.native_cwd(&agent_id).await.is_some());
    assert_eq!(provider.restore_call_count().await, calls + 1);
}
fn policy_review(session: &runtime_core::SessionRecord, revision: u64) -> Vec<serde_json::Value> {
    ["applied","not_applied"].into_iter().map(|resolution|serde_json::json!({"resolution":resolution,"evidence":"Operator asserts a policy state for the exact original native attachment","provider":session.provider,"provider_session_ref":session.provider_session_ref,"canonical_provider_session_ref":session.canonical_provider_session_ref,"expected_revision":revision,"session_updated_at":session.updated_at})).collect()
}
#[tokio::test]
async fn preferences_prose_cannot_change_policy_or_release_native_authority() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    provider.set_preferences_mutation_mode(2);
    let calls = provider.preferences_mutation_calls();
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/preferences"),
        Some(preference_body(revision, "high")),
        Some("d4-preferences-unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original = store.get_operation(id).unwrap().unwrap();
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(original.claims.len(), 1);
    let uri = format!("/v2/agents/{agent_id}/native-policy/{id}/resolve");
    refuse_prose_variants(
        &router,
        &token,
        &db,
        &uri,
        id,
        policy_review(&session, revision),
    )
    .await;
    assert_eq!(store.get_operation(id).unwrap().unwrap(), original);
    assert_eq!(
        runtime.get_active_workspace_agent_by_id(&agent_id).unwrap(),
        agent
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), session);
    assert_eq!(provider.preferences_mutation_calls(), calls + 1);
}
#[tokio::test]
async fn permission_prose_cannot_change_policy_or_release_native_authority() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        claude_permission_fixture().await;
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    provider.set_permission_mutation_mode(2);
    let calls = provider.permission_mutation_calls();
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/permission"),
        Some(permission_body(revision)),
        Some("d4-permission-unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original = store.get_operation(id).unwrap().unwrap();
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(original.claims.len(), 1);
    let uri = format!("/v2/agents/{agent_id}/native-policy/{id}/resolve");
    refuse_prose_variants(
        &router,
        &token,
        &db,
        &uri,
        id,
        policy_review(&session, revision),
    )
    .await;
    assert_eq!(store.get_operation(id).unwrap().unwrap(), original);
    assert_eq!(
        runtime.get_active_workspace_agent_by_id(&agent_id).unwrap(),
        agent
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), session);
    assert_eq!(provider.permission_mutation_calls(), calls + 1);
}
#[tokio::test]
async fn retirement_prose_cannot_substitute_for_a_structured_native_close_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("d4-retirement");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let f = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = f.register(&root).await;
    let agent = f.agent(&workspace.workspace_id, ProviderKind::Codex).await;
    provider.fail_close.store(true, Ordering::SeqCst);
    let revision = f.workspace(&workspace.workspace_id).await.revision;
    let original = f
        .advance(&workspace.workspace_id, revision, "d4-retirement-unknown")
        .await;
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert!(!original.claims.is_empty());
    let effect = original
        .effects
        .iter()
        .find(|e| e.effect_kind == "workspace_provider_close")
        .unwrap();
    assert_eq!(effect.phase, "manual_review");
    let session = f.runtime.get_session(&agent.agent_id).await.unwrap();
    let current_agent = f
        .runtime
        .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
        .unwrap();
    // External observation alone is not a typed receipt for the original operation.
    provider
        .externally_close_for_operator_review(&agent.agent_id)
        .await;
    let input = serde_json::json!({"effect_id":effect.effect_id,"evidence":"Operator reports closure of the exact originally claimed native attachment","resolution":{"kind":"provider_closed","identity":{"agent_id":agent.agent_id,"provider":session.provider,"provider_session_ref":session.provider_session_ref,"canonical_provider_session_ref":session.canonical_provider_session_ref,"agent_revision":current_agent.revision,"session_updated_at":session.updated_at,"active_turn_id":session.active_turn_id}}});
    let id = &original.operation.operation_id;
    let uri = format!(
        "/v2/workspaces/{}/retirement/{id}/reconcile",
        workspace.workspace_id
    );
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    refuse_prose_variants(&f.router, &f.token, &db, &uri, id, vec![input]).await;
    assert_eq!(f.store.get_operation(id).unwrap().unwrap(), original);
    assert_eq!(
        f.runtime
            .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
            .unwrap(),
        current_agent
    );
    assert_eq!(
        f.runtime.get_session(&agent.agent_id).await.unwrap(),
        session
    );
    assert_eq!(
        f.workspace(&workspace.workspace_id).await.lifecycle_state,
        runtime_core::WorkspaceLifecycleState::Active
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(provider.native_session_count().await, 0);
}
