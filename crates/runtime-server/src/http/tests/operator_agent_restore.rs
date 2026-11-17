use super::native_review_refusal::{assert_refusal, refuse_prose_variants};
use super::operator_native_policy::operator_policy_fixture;
use super::workspace_worktree_routes::call;
use super::*;

async fn archived_fixture() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    Arc<RuntimeSessionManager>,
    String,
    String,
    u64,
) {
    let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let workspace_id = agent.workspace_id;
    let (status, archived) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"),
        Some(serde_json::json!({"reason":"paused for operator restoration"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived:?}");
    assert_eq!(archived["lifecycle_state"], "archived");
    let revision = archived["revision"].as_u64().unwrap();
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "closed"
    );
    (
        router,
        token,
        temp,
        provider,
        runtime,
        workspace_id,
        agent_id,
        revision,
    )
}

async fn restore_http(
    router: &Router,
    token: &str,
    workspace_id: &str,
    agent_id: &str,
    key: Option<&str>,
) -> (StatusCode, serde_json::Value, HeaderMap) {
    let uri = format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/restore");
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap(), headers)
}

fn review_input(
    original: &runtime_core::SessionRecord,
    revision: u64,
    resolution: &str,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "resolution":resolution,
        "evidence":"Operator independently inspected the exact original native provider session and verified its resume outcome",
        "provider":original.provider,
        "original_provider_session_ref":original.provider_session_ref,
        "original_canonical_provider_session_ref":original.canonical_provider_session_ref,
        "expected_revision":revision,
        "session_updated_at":original.updated_at,
    });
    if resolution == "resumed" {
        body["observed_provider_session_ref"] = serde_json::json!(original.provider_session_ref);
        body["observed_canonical_provider_session_ref"] =
            serde_json::json!(original.canonical_provider_session_ref);
    }
    body
}

async fn reopen_runtime(
    temp: &tempfile::TempDir,
    provider: Arc<TestProvider>,
) -> Arc<RuntimeSessionManager> {
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider).unwrap();
    Arc::new(RuntimeSessionManager::new(store, Arc::new(registry), 128).unwrap())
}

#[tokio::test]
async fn restore_sqlite_revision_ceiling_rejects_before_native_resume() {
    let (router, token, temp, provider, runtime, workspace, agent_id, _) = archived_fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    // A legal archived SQLite revision cannot advance after native resume.
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX, agent_id],
    )
    .unwrap();
    let before = provider.restore_call_count().await;
    let (status, response, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("restore-signed-ceiling"),
    )
    .await;
    assert_eq!(
        provider.restore_call_count().await,
        before,
        "unrepresentable revision must be rejected BEFORE provider resume: {status} {response:?}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response:?}");
    assert_eq!(response["error"]["code"], "invalid_state");
    let durable: (i64, String, i64) = db
        .query_row(
            "SELECT a.revision,s.status,(SELECT COUNT(*) FROM runtime_operations
             WHERE idempotency_key='restore-signed-ceiling')
             FROM workspace_agents a JOIN sessions s ON s.id=a.session_id
             WHERE a.session_id=?1",
            [&agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(durable, (i64::MAX, "closed".into(), 0));
    // Direct SQLite callers cannot bypass the runtime's pre-native guard.
    let session = runtime.get_session(&agent_id).await.unwrap();
    let request = serde_json::json!({
        "workspace_id":workspace,"agent_id":agent_id,"action":"restore",
    });
    let command = runtime_core::OperatorAgentRestoreCommand {
        operation_id: "op_restore_direct_signed_ceiling".into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: "restore-direct-ceiling".into(),
        workspace_id: workspace.clone(),
        agent_id: agent_id.clone(),
        expected_revision: i64::MAX as u64,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: session.updated_at + 1,
    };
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(matches!(
        store.repository().admit_operator_agent_restore(&command),
        Err(runtime_core::RuntimeError::InvalidState(_))
    ));
    assert!(store
        .repository()
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn restore_prose_refusal_ignores_unrelated_corrupt_session() {
    let (router, token, temp, provider, runtime, workspace, agent_id, revision) =
        archived_fixture().await;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_restore_failure_mode(2);
    let (status, unknown, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("restore-isolated"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let operation_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO sessions
         SELECT 'unrelated_bad_restore_metadata',provider,status,cwd,model,permission_mode,
                system_prompt,'{invalid-json',provider_session_ref,canonical_provider_session_ref,
                NULL,worktree_id,created_at,updated_at,closed_at,failure_code,failure_message
         FROM sessions WHERE id=?1",
        [&agent_id],
    )
    .unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(store.hydrate_runtime_state().is_err());

    let baseline = provider.restore_call_count().await;
    let uri =
        format!("/v2/workspaces/{workspace}/agents/{agent_id}/restores/{operation_id}/resolve");
    let input = review_input(&original, revision, "resumed");
    let original_operation = store.get_operation(operation_id).unwrap().unwrap();
    refuse_prose_variants(&router, &token, &db, &uri, operation_id, vec![input]).await;
    assert_eq!(
        store.get_operation(operation_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), original);
    assert_eq!(provider.restore_call_count().await, baseline);
    assert!(store.hydrate_runtime_state().is_err());
}

#[tokio::test]
async fn operator_restore_key_replays_exact_atomic_roster_receipt_without_native_retry() {
    let (router, token, temp, provider, runtime, workspace, agent_id, revision) =
        archived_fixture().await;
    let baseline = provider.restore_call_count().await;
    let (status, first, headers) =
        restore_http(&router, &token, &workspace, &agent_id, Some("restore-once")).await;
    assert_eq!(status, StatusCode::OK, "{first:?}");
    assert_eq!(first["lifecycle_state"], "active");
    assert_eq!(first["revision"], revision + 1);
    assert_eq!(
        headers.get(header::ETAG).unwrap(),
        &format!("\"{}\"", revision + 1)
    );
    let operation_id = headers.get("x-operation-id").unwrap().to_str().unwrap();
    assert!(operation_id.starts_with("op_restore_"));
    assert_eq!(provider.restore_call_count().await, baseline + 1);
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "ready"
    );

    let (status, replay, retry_headers) =
        restore_http(&router, &token, &workspace, &agent_id, Some("restore-once")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first, replay);
    assert_eq!(
        headers.get("x-operation-id"),
        retry_headers.get("x-operation-id")
    );
    assert_eq!(provider.restore_call_count().await, baseline + 1);

    let (status, noop, noop_headers) =
        restore_http(&router, &token, &workspace, &agent_id, Some("new-noop-key")).await;
    assert_eq!(status, StatusCode::OK, "{noop:?}");
    assert_eq!(noop, first);
    assert_ne!(
        headers.get("x-operation-id"),
        noop_headers.get("x-operation-id")
    );
    assert_eq!(provider.restore_call_count().await, baseline + 1);

    let (status, unkeyed, one_shot) =
        restore_http(&router, &token, &workspace, &agent_id, None).await;
    assert_eq!(status, StatusCode::OK, "{unkeyed:?}");
    assert_eq!(unkeyed, first);
    assert!(one_shot.contains_key("x-operation-id"));
    assert_eq!(provider.restore_call_count().await, baseline + 1);

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let details = store
        .repository()
        .get_operation(operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        details.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert!(details.claims.is_empty(), "native claim was released");
    assert_eq!(details.effects.len(), 1);
    assert_eq!(details.effects[0].phase, "finalized");
    let noop_id = noop_headers
        .get("x-operation-id")
        .unwrap()
        .to_str()
        .unwrap();
    let noop_details = store.repository().get_operation(noop_id).unwrap().unwrap();
    assert!(
        noop_details.effects.is_empty(),
        "no-op has no native effect"
    );
    // An already active row at i64::MAX still supports a newly keyed no-op
    // and its exact replay; neither is allowed to resume the fake provider.
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX, agent_id],
    )
    .unwrap();
    let (status, max_noop, first_headers) =
        restore_http(&router, &token, &workspace, &agent_id, Some("max-noop")).await;
    assert_eq!(status, StatusCode::OK, "{max_noop:?}");
    assert_eq!(max_noop["revision"], i64::MAX);
    let (status, max_replay, replay_headers) =
        restore_http(&router, &token, &workspace, &agent_id, Some("max-noop")).await;
    assert_eq!(status, StatusCode::OK, "{max_replay:?}");
    assert_eq!(max_noop, max_replay);
    assert_eq!(
        first_headers.get("x-operation-id"),
        replay_headers.get("x-operation-id")
    );
    assert_eq!(provider.restore_call_count().await, baseline + 1);
}

#[tokio::test]
async fn not_resumed_prose_keeps_original_restore_key_and_claim() {
    let (router, token, temp, provider, runtime, workspace, agent_id, revision) =
        archived_fixture().await;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_restore_failure_mode(2);
    let baseline = provider.restore_call_count().await;
    let (status, unknown, headers) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("unknown-restore-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    assert_eq!(provider.restore_call_count().await, baseline + 1);
    assert_eq!(headers.get("x-operation-id").unwrap(), op_id);
    let (status, retry, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("unknown-restore-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["details"]["operation_id"], op_id);
    let (status, blocked, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("different-while-unresolved"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(provider.restore_call_count().await, baseline + 1);
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "closed"
    );
    let agent = runtime.get_workspace_agent(&workspace, &agent_id).unwrap();
    assert_eq!(
        agent.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );
    assert_eq!(agent.revision, revision);

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (phase, effect_phase, live_claims): (String, String, i64) = db
        .query_row(
            "SELECT o.phase,e.phase,(SELECT COUNT(*) FROM runtime_operation_resource_claims
         WHERE owner_operation_id=o.operation_id)
         FROM runtime_operations o JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
         WHERE o.operation_id=?1",
            [op_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (phase.as_str(), effect_phase.as_str(), live_claims),
        ("manual_review", "uncertain", 1)
    );
    drop(db);

    let uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/restores/{op_id}/resolve");
    let evidence = review_input(&original, revision, "not_resumed");
    let mut wrong_attachment = evidence.clone();
    wrong_attachment["original_provider_session_ref"] =
        serde_json::json!("not-the-admitted-native-session");
    let (status, rejected) =
        call(&router, &token, "POST", &uri, Some(wrong_attachment), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected:?}");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original_operation = store.get_operation(op_id).unwrap().unwrap();

    let mut contradictory = evidence.clone();
    contradictory["resolution"] = serde_json::json!("resumed");
    contradictory["observed_provider_session_ref"] =
        serde_json::json!(original.provider_session_ref);
    contradictory["observed_canonical_provider_session_ref"] =
        serde_json::json!(original.canonical_provider_session_ref);
    refuse_prose_variants(
        &router,
        &token,
        &db,
        &uri,
        op_id,
        vec![evidence, contradictory],
    )
    .await;

    let (status, old, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("unknown-restore-key"),
    )
    .await;
    assert_refusal(status, &old, op_id);
    provider.set_restore_failure_mode(0);
    let (status, blocked, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("after-refused-not-resumed"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(
        store.get_operation(op_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(original_operation.claims.len(), 1);
    assert_eq!(
        runtime.get_workspace_agent(&workspace, &agent_id).unwrap(),
        agent
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), original);
    assert!(provider.native_cwd(&agent_id).await.is_some());
    assert_eq!(provider.restore_call_count().await, baseline + 1);
}

#[tokio::test]
async fn resumed_prose_cannot_authorize_drifted_or_exact_startup_resume() {
    let (router, token, temp, provider, runtime, workspace, agent_id, revision) =
        archived_fixture().await;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_restore_failure_mode(2);
    let baseline = provider.restore_call_count().await;
    let (status, unknown, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("known-resume-lost-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/restores/{op_id}/resolve");
    let evidence = review_input(&original, revision, "resumed");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original_operation = store.get_operation(op_id).unwrap().unwrap();
    let agent = runtime.get_workspace_agent(&workspace, &agent_id).unwrap();
    refuse_prose_variants(&router, &token, &db, &uri, op_id, vec![evidence]).await;

    let (status, replay, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("known-resume-lost-receipt"),
    )
    .await;
    assert_refusal(status, &replay, op_id);
    let (status, blocked) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({
            "input":[{"type":"text","text":"should be fenced"}]
        })),
        Some("no-native-before-startup"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(provider.restore_call_count().await, baseline + 1);

    for mode in [3, 0] {
        provider.set_restore_failure_mode(mode);
        let reopened = reopen_runtime(&temp, provider.clone()).await;
        reopened.recover_startup().await.unwrap();
        assert_eq!(reopened.get_session(&agent_id).await.unwrap(), original);
        assert_eq!(
            reopened.get_workspace_agent(&workspace, &agent_id).unwrap(),
            agent
        );
        assert_eq!(
            store.get_operation(op_id).unwrap().unwrap(),
            original_operation
        );
        assert_eq!(provider.restore_call_count().await, baseline + 1);
    }
    assert_eq!(original_operation.claims.len(), 1);
    assert!(provider.native_cwd(&agent_id).await.is_some());
}

#[tokio::test]
async fn startup_quarantines_admitted_restore_without_native_dispatch_or_implicit_retry() {
    let (router, token, temp, provider, runtime, workspace, agent_id, revision) =
        archived_fixture().await;
    let agent = runtime.get_workspace_agent(&workspace, &agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    let request = serde_json::json!({
        "workspace_id":workspace, "agent_id":agent_id, "action":"restore",
    });
    let command = runtime_core::OperatorAgentRestoreCommand {
        operation_id: "op_restore_crash_before_native_send".into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: "crash-before-native-send".into(),
        workspace_id: workspace.clone(),
        agent_id: agent_id.clone(),
        expected_revision: revision,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: session.updated_at + 1,
    };
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert_eq!(
        agent.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );
    assert!(matches!(
        store
            .repository()
            .admit_operator_agent_restore(&command)
            .unwrap(),
        runtime_core::OperatorAgentRestoreAdmission::Execute { .. }
    ));
    let baseline = provider.restore_call_count().await;
    let reopened = reopen_runtime(&temp, provider.clone()).await;
    let recovered = reopened.recover_startup().await.unwrap();
    assert!(
        recovered
            .notes
            .iter()
            .any(|note| note.contains(&command.operation_id)
                && note.contains("remains under manual review; no native effect was retried")),
        "{recovered:?}"
    );
    assert_eq!(
        provider.restore_call_count().await,
        baseline,
        "startup must not attempt uncertain restore"
    );
    let details = store
        .repository()
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        details.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(details.claims.len(), 1);
    assert_eq!(details.effects[0].phase, "uncertain");
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap().status,
        "closed"
    );

    let (status, replay, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("crash-before-native-send"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{replay:?}");
    assert_eq!(
        replay["error"]["details"]["operation_id"],
        command.operation_id
    );
    assert_eq!(provider.restore_call_count().await, baseline);
}

#[tokio::test]
async fn atomic_restore_receipt_failure_keeps_archived_authority_and_quarantines_native() {
    let (router, token, temp, provider, runtime, workspace, agent_id, revision) =
        archived_fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER deny_agent_restore_receipt BEFORE UPDATE OF lifecycle_state ON workspace_agents
         BEGIN SELECT RAISE(ABORT, 'forced atomic agent restore receipt failure'); END;",
    ).unwrap();
    let baseline = provider.restore_call_count().await;
    let (status, unknown, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("receipt-fails-after-native"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    assert_eq!(provider.restore_call_count().await, baseline + 1);
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "closed"
    );
    let agent = runtime.get_workspace_agent(&workspace, &agent_id).unwrap();
    assert_eq!(agent.revision, revision);
    assert_eq!(
        agent.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let state: (String, i64) = db.query_row(
        "SELECT o.phase,(SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=o.operation_id)
         FROM runtime_operations o WHERE o.operation_id=?1",
        [op_id], |row| Ok((row.get(0)?,row.get(1)?)),
    ).unwrap();
    assert_eq!(state, ("manual_review".to_string(), 1));
    let (status, again, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("receipt-fails-after-native"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{again:?}");
    assert_eq!(provider.restore_call_count().await, baseline + 1);
}

#[tokio::test]
async fn concurrent_same_key_restore_returns_one_effect_and_one_identical_receipt() {
    let (router, token, _temp, provider, _, workspace, agent_id, _) = archived_fixture().await;
    let baseline = provider.restore_call_count().await;
    let (first, second) = tokio::join!(
        restore_http(
            &router,
            &token,
            &workspace,
            &agent_id,
            Some("concurrent-exact-key")
        ),
        restore_http(
            &router,
            &token,
            &workspace,
            &agent_id,
            Some("concurrent-exact-key")
        ),
    );
    assert_eq!(first.0, StatusCode::OK, "{:?}", first.1);
    assert_eq!(second.0, StatusCode::OK, "{:?}", second.1);
    assert_eq!(first.1, second.1);
    assert_eq!(
        first.2.get("x-operation-id"),
        second.2.get("x-operation-id")
    );
    assert_eq!(first.2.get(header::ETAG), second.2.get(header::ETAG));
    assert_eq!(provider.restore_call_count().await, baseline + 1);
    let (status, other_scope, _) = restore_http(
        &router,
        &token,
        "another-workspace",
        &agent_id,
        Some("concurrent-exact-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{other_scope:?}");
    assert_eq!(provider.restore_call_count().await, baseline + 1);
}

#[tokio::test]
async fn restore_auth_strict_payload_key_and_review_revision_are_checked_before_native_work() {
    let (router, token, _temp, provider, runtime, workspace, agent_id, revision) =
        archived_fixture().await;
    let baseline = provider.restore_call_count().await;
    let restore_uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/restore");
    let unauth = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&restore_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let malformed = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&restore_uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"force":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

    let (status, invalid_key, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("invalid,combined-key"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid_key:?}");
    assert_eq!(provider.restore_call_count().await, baseline);

    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_restore_failure_mode(2);
    let (status, unknown, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("review-contract-checks"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let review_uri =
        format!("/v2/workspaces/{workspace}/agents/{agent_id}/restores/{op_id}/resolve");
    let evidence = review_input(&original, revision, "not_resumed");
    let incorrect_revision = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&review_uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, "\"999999\"")
                .body(Body::from(evidence.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(incorrect_revision.status(), StatusCode::PRECONDITION_FAILED);
    let mut unknown_field = evidence;
    unknown_field["not_in_contract"] = serde_json::json!(true);
    let (status, strict) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(unknown_field),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{strict:?}");
    assert_eq!(
        provider.restore_call_count().await,
        baseline + 1,
        "invalid review and restore requests cannot redispatch a provider operation"
    );
}
