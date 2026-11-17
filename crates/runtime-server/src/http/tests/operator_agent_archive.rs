use super::native_review_refusal::{assert_refusal, database_snapshot, refuse_prose_variants};
use super::operator_native_policy::operator_policy_fixture;
use super::workspace_worktree_routes::call;
use super::*;

async fn archive_http(
    router: &Router,
    token: &str,
    workspace_id: &str,
    agent_id: &str,
    reason: Option<&str>,
    key: Option<&str>,
) -> (StatusCode, serde_json::Value, HeaderMap) {
    let uri = format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/archive");
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let body = serde_json::json!({"reason":reason});
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap(), headers)
}

fn review_input(
    original: &runtime_core::SessionRecord,
    revision: u64,
    resolution: &str,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "resolution":resolution,
        "evidence":"Independently verified exact native provider attachment and original archive close outcome",
        "provider":original.provider,
        "original_provider_session_ref":original.provider_session_ref,
        "original_canonical_provider_session_ref":original.canonical_provider_session_ref,
        "expected_revision":revision,
        "session_updated_at":original.updated_at,
    });
    if resolution == "not_closed" {
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
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(provider).unwrap();
    Arc::new(RuntimeSessionManager::new(store, Arc::new(providers), 128).unwrap())
}

#[tokio::test]
async fn archive_sqlite_revision_ceiling_rejects_before_native_close() {
    let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let workspace_id = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    // A signed SQLite agent revision may legally equal i64::MAX, but an
    // archive would need to increment it after an irreversible native close.
    connection
        .execute(
            "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
            rusqlite::params![i64::MAX, agent_id],
        )
        .unwrap();
    let before = provider.archive_close_count();
    let (status, response, _) = archive_http(
        &router,
        &token,
        &workspace_id,
        &agent_id,
        Some("signed SQLite ceiling"),
        Some("archive-sqlite-revision-ceiling"),
    )
    .await;
    assert_eq!(
        provider.archive_close_count(),
        before,
        "unrepresentable next revision must be rejected BEFORE provider close: {status} {response:?}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response:?}");
    assert_eq!(response["error"]["code"], "invalid_state");
    let durable: (i64, String, i64) = connection
        .query_row(
            "SELECT a.revision,s.status,(SELECT COUNT(*) FROM runtime_operations
             WHERE idempotency_key='archive-sqlite-revision-ceiling')
             FROM workspace_agents a JOIN sessions s ON s.id=a.session_id
             WHERE a.session_id=?1",
            [&agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(durable, (i64::MAX, "ready".into(), 0));
    // The repository's IMMEDIATE admission must reject direct callers too.
    let session = runtime.get_session(&agent_id).await.unwrap();
    let request = serde_json::json!({
        "workspace_id":workspace_id,"agent_id":agent_id,
        "action":"archive","reason":"signed SQLite ceiling",
    });
    let command = runtime_core::OperatorAgentArchiveCommand {
        operation_id: "op_archive_direct_signed_ceiling".into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: "archive-direct-ceiling".into(),
        workspace_id: workspace_id.clone(),
        agent_id: agent_id.clone(),
        reason: Some("signed SQLite ceiling".into()),
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
        store.repository().admit_operator_agent_archive(&command),
        Err(runtime_core::RuntimeError::InvalidState(_))
    ));
    assert!(store
        .repository()
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn archive_prose_refusal_ignores_unrelated_corrupt_session() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_archive_close_mode(2);
    let (status, unknown, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("quarantine"),
        Some("archive-isolated"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let operation_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO sessions
         SELECT 'unrelated_bad_archive_metadata',provider,status,cwd,model,permission_mode,
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

    let before = provider.archive_close_count();
    let uri =
        format!("/v2/workspaces/{workspace}/agents/{agent_id}/archives/{operation_id}/resolve");
    let input = review_input(&original, revision, "closed");
    let original_operation = store.get_operation(operation_id).unwrap().unwrap();
    refuse_prose_variants(&router, &token, &db, &uri, operation_id, vec![input]).await;
    assert_eq!(
        store.get_operation(operation_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), original);
    assert_eq!(provider.archive_close_count(), before);
    assert!(store.hydrate_runtime_state().is_err());
}

#[tokio::test]
async fn archive_commits_strong_native_close_once_then_replays_exact_receipt() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace_id = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let before = provider.archive_close_count();
    let (status, archived, headers) = archive_http(
        &router,
        &token,
        &workspace_id,
        &agent_id,
        Some("paused"),
        Some("archive-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived:?}");
    assert_eq!(archived["lifecycle_state"], "archived");
    assert_eq!(archived["revision"], revision + 1);
    assert_eq!(archived["archive_reason"], "paused");
    assert_eq!(provider.archive_close_count(), before + 1);
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "closed"
    );
    assert_eq!(
        headers.get(header::ETAG).unwrap(),
        &format!("\"{}\"", revision + 1)
    );
    let op_id = headers.get("x-operation-id").unwrap().to_str().unwrap();
    assert!(op_id.starts_with("op_archive_"));

    let (status, same, same_headers) = archive_http(
        &router,
        &token,
        &workspace_id,
        &agent_id,
        Some("paused"),
        Some("archive-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{same:?}");
    assert_eq!(same, archived);
    assert_eq!(
        headers.get("x-operation-id"),
        same_headers.get("x-operation-id")
    );
    assert_eq!(provider.archive_close_count(), before + 1);

    let (status, changed, _) = archive_http(
        &router,
        &token,
        &workspace_id,
        &agent_id,
        Some("different"),
        Some("archive-once"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{changed:?}");
    let (status, foreign, _) = archive_http(
        &router,
        &token,
        "different-workspace",
        &agent_id,
        Some("paused"),
        Some("archive-once"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{foreign:?}");
    assert_eq!(provider.archive_close_count(), before + 1);

    let (status, noop, noop_headers) = archive_http(
        &router,
        &token,
        &workspace_id,
        &agent_id,
        Some("unused"),
        Some("archive-noop-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{noop:?}");
    assert_eq!(noop, archived);
    assert_ne!(
        headers.get("x-operation-id"),
        noop_headers.get("x-operation-id")
    );
    assert_eq!(provider.archive_close_count(), before + 1);

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let completed = store.repository().get_operation(op_id).unwrap().unwrap();
    assert_eq!(
        completed.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(completed.operation.exact_terminal_result, Some(archived));
    assert!(completed.claims.is_empty());
    assert_eq!(completed.effects.len(), 1);
    assert_eq!(completed.effects[0].phase, "finalized");
    assert_eq!(
        completed.effects[0].evidence.as_ref().unwrap()["strong_provider_close"]["proof_kind"],
        "native_session_close_acknowledged"
    );
    // Rehydrate a new manager over the same durable store and prove that the
    // original key still returns the old receipt without invoking the provider.
    let reopened = reopen_runtime(&temp, provider.clone()).await;
    let (after_restart, same_operation) = reopened
        .archive_workspace_agent_operator(
            &workspace_id,
            &agent_id,
            Some("paused"),
            runtime_core::OperationActor::operator("runtime_operator"),
            "archive-once".into(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(after_restart).unwrap(),
        completed.operation.exact_terminal_result.unwrap()
    );
    assert_eq!(same_operation, op_id);
    assert_eq!(provider.archive_close_count(), before + 1);
    // An already archived row at the same signed ceiling has NO next-revision
    // requirement: a new no-effect receipt and its exact replay remain legal.
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX, agent_id],
    )
    .unwrap();
    let refs = (&router, &token, &workspace_id, &agent_id);
    let call_max_noop = || archive_http(refs.0, refs.1, refs.2, refs.3, None, Some("max-noop"));
    let (status, max_noop, first_headers) = call_max_noop().await;
    assert_eq!(status, StatusCode::OK, "{max_noop:?}");
    assert_eq!(max_noop["revision"], i64::MAX);
    let (status, max_replay, replay_headers) = call_max_noop().await;
    assert_eq!(status, StatusCode::OK, "{max_replay:?}");
    assert_eq!(max_noop, max_replay);
    assert_eq!(
        first_headers.get("x-operation-id"),
        replay_headers.get("x-operation-id")
    );
    assert_eq!(provider.archive_close_count(), before + 1);
}

#[tokio::test]
async fn native_archive_unknown_stays_active_and_key_is_not_redispatched() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_archive_close_mode(2);
    let baseline = provider.archive_close_count();
    let (status, unknown, headers) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("pause"),
        Some("lost-close-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    assert_eq!(headers.get("x-operation-id").unwrap(), op_id);
    assert_eq!(provider.archive_close_count(), baseline + 1);
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "ready"
    );
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(agent.revision, revision);
    assert_eq!(
        agent.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Active
    );

    let (status, repeat, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("pause"),
        Some("lost-close-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{repeat:?}");
    assert_eq!(repeat["error"]["details"]["operation_id"], op_id);
    let (status, blocked, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        None,
        Some("other-archive-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(provider.archive_close_count(), baseline + 1);

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let operation = store.repository().get_operation(op_id).unwrap().unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(operation.effects[0].phase, "uncertain");
    assert_eq!(operation.claims.len(), 1);

    let uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/archives/{op_id}/resolve");
    let evidence = review_input(&original, revision, "closed");
    let mut invalid = evidence.clone();
    invalid["original_provider_session_ref"] = serde_json::json!("wrong-native-ref");
    let (status, wrong) = call(&router, &token, "POST", &uri, Some(invalid), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{wrong:?}");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    refuse_prose_variants(&router, &token, &db, &uri, op_id, vec![evidence]).await;
    let (status, original_key, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("pause"),
        Some("lost-close-ack"),
    )
    .await;
    assert_refusal(status, &original_key, op_id);
    assert_eq!(store.get_operation(op_id).unwrap().unwrap(), operation);
    assert_eq!(
        runtime.get_active_workspace_agent_by_id(&agent_id).unwrap(),
        agent
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), original);
    assert!(provider.native_cwd(&agent_id).await.is_none());

    let resumes = provider.restore_call_count().await;
    let reopened = reopen_runtime(&temp, provider.clone()).await;
    reopened.recover_startup().await.unwrap();
    assert_eq!(reopened.get_session(&agent_id).await.unwrap(), original);
    assert_eq!(store.get_operation(op_id).unwrap().unwrap(), operation);
    assert_eq!(provider.restore_call_count().await, resumes);
    assert_eq!(provider.archive_close_count(), baseline + 1);
}

#[tokio::test]
async fn not_closed_prose_keeps_original_archive_claim_across_restart() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_archive_close_mode(4); // Unsupported, so native session remains open.
    let baseline = provider.archive_close_count();
    let (status, unknown, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("pause"),
        Some("unproven-close"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let operation_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    assert_eq!(provider.archive_close_count(), baseline + 1);
    let review_uri =
        format!("/v2/workspaces/{workspace}/agents/{agent_id}/archives/{operation_id}/resolve");
    let body = review_input(&original, revision, "not_closed");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let operation = store.get_operation(operation_id).unwrap().unwrap();
    refuse_prose_variants(&router, &token, &db, &review_uri, operation_id, vec![body]).await;
    let (status, old, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("pause"),
        Some("unproven-close"),
    )
    .await;
    assert_refusal(status, &old, operation_id);
    let (status, blocked, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("retry"),
        Some("different-key-before-startup"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");

    let resumes = provider.restore_call_count().await;
    for mode in [3, 0] {
        provider.set_restore_failure_mode(mode);
        let reopened = reopen_runtime(&temp, provider.clone()).await;
        reopened.recover_startup().await.unwrap();
        assert_eq!(reopened.get_session(&agent_id).await.unwrap(), original);
        assert_eq!(
            reopened
                .get_active_workspace_agent_by_id(&agent_id)
                .unwrap()
                .revision,
            revision
        );
        assert_eq!(
            store.get_operation(operation_id).unwrap().unwrap(),
            operation
        );
        assert_eq!(provider.restore_call_count().await, resumes);
    }

    provider.set_archive_close_mode(0);
    let reopened = reopen_runtime(&temp, provider.clone()).await;
    let denied = reopened
        .archive_workspace_agent_operator(
            &workspace,
            &agent_id,
            Some("retry"),
            runtime_core::OperationActor::operator("runtime_operator"),
            "new-key-after-startup".into(),
        )
        .await;
    assert!(
        denied.is_err(),
        "the original uncertain close still owns native authority"
    );
    assert_eq!(
        store.get_operation(operation_id).unwrap().unwrap(),
        operation
    );
    assert_eq!(operation.claims.len(), 1);
    assert!(provider.native_cwd(&agent_id).await.is_some());
    assert_eq!(provider.archive_close_count(), baseline + 1);
}

#[tokio::test]
async fn startup_quarantines_admitted_archive_intent_without_native_close_retry() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let session = runtime.get_session(&agent_id).await.unwrap();
    let request = serde_json::json!({
        "workspace_id":workspace,"agent_id":agent_id,"action":"archive","reason":"crash"
    });
    let command = runtime_core::OperatorAgentArchiveCommand {
        operation_id: "op_archive_crash_before_native_close".into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: "crash-before-native-close".into(),
        workspace_id: workspace.clone(),
        agent_id: agent_id.clone(),
        reason: Some("crash".into()),
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
    assert!(matches!(
        store
            .repository()
            .admit_operator_agent_archive(&command)
            .unwrap(),
        runtime_core::OperatorAgentArchiveAdmission::Execute { .. }
    ));
    let baseline = provider.archive_close_count();
    let reopened = reopen_runtime(&temp, provider.clone()).await;
    let recovery = reopened.recover_startup().await.unwrap();
    assert!(
        recovery
            .notes
            .iter()
            .any(|note| note.contains(&command.operation_id) && note.contains("manual_review")),
        "{recovery:?}"
    );
    assert_eq!(provider.archive_close_count(), baseline);
    let operation = store
        .repository()
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(operation.claims.len(), 1);
    assert_eq!(operation.effects[0].phase, "uncertain");
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap().status,
        "ready"
    );

    let (status, retry, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("crash"),
        Some("crash-before-native-close"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(
        retry["error"]["details"]["operation_id"],
        command.operation_id
    );
    assert_eq!(provider.archive_close_count(), baseline);
    let second = reopen_runtime(&temp, provider.clone()).await;
    second.recover_startup().await.unwrap();
    assert_eq!(provider.archive_close_count(), baseline);
}

#[tokio::test]
async fn native_close_receipt_sql_failure_keeps_authority_despite_prose() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let original = runtime.get_session(&agent_id).await.unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER deny_native_archive_receipt
        BEFORE UPDATE OF lifecycle_state ON workspace_agents
        BEGIN SELECT RAISE(ABORT, 'injected archive receipt failure'); END;",
    )
    .unwrap();
    let baseline = provider.archive_close_count();
    let (status, ambiguous, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("checkpoint"),
        Some("archive-sql-failure"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{ambiguous:?}");
    assert_eq!(ambiguous["error"]["code"], "outcome_unresolved");
    assert_eq!(provider.archive_close_count(), baseline + 1);
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "ready"
    );
    assert_eq!(
        runtime
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        revision
    );
    let operation_id = ambiguous["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let operation = store
        .repository()
        .get_operation(operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(operation.effects[0].phase, "uncertain");
    assert_eq!(operation.claims.len(), 1);

    let uri =
        format!("/v2/workspaces/{workspace}/agents/{agent_id}/archives/{operation_id}/resolve");
    let body = review_input(&original, revision, "closed");
    let before = database_snapshot(&db);
    let (status, failure) = call(&router, &token, "POST", &uri, Some(body.clone()), None).await;
    assert_refusal(status, &failure, operation_id);
    assert_eq!(database_snapshot(&db), before);
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), original);
    assert_eq!(
        runtime
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        revision
    );

    db.execute_batch("DROP TRIGGER deny_native_archive_receipt;")
        .unwrap();
    refuse_prose_variants(&router, &token, &db, &uri, operation_id, vec![body]).await;
    assert_eq!(
        store.get_operation(operation_id).unwrap().unwrap(),
        operation
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), original);
    assert!(provider.native_cwd(&agent_id).await.is_none());
    assert_eq!(provider.archive_close_count(), baseline + 1);
}

#[tokio::test]
async fn concurrent_archive_key_and_strict_operator_input_do_not_duplicate_close() {
    let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/archive");
    let denied = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let invalid = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"reason":"paused","force":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let baseline = provider.archive_close_count();

    let (a, b) = tokio::join!(
        archive_http(
            &router,
            &token,
            &workspace,
            &agent_id,
            Some("paused"),
            Some("archive-race-key")
        ),
        archive_http(
            &router,
            &token,
            &workspace,
            &agent_id,
            Some("paused"),
            Some("archive-race-key")
        ),
    );
    assert_eq!(a.0, StatusCode::OK, "{:?}", a.1);
    assert_eq!(b.0, StatusCode::OK, "{:?}", b.1);
    assert_eq!(a.1, b.1);
    assert_eq!(a.2.get("x-operation-id"), b.2.get("x-operation-id"));
    assert_eq!(a.2.get(header::ETAG), b.2.get(header::ETAG));
    assert_eq!(provider.archive_close_count(), baseline + 1);

    let (status, unkeyed, headers) =
        archive_http(&router, &token, &workspace, &agent_id, None, None).await;
    assert_eq!(status, StatusCode::OK, "{unkeyed:?}");
    assert!(headers.contains_key("x-operation-id"));
    assert_eq!(provider.archive_close_count(), baseline + 1);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let archived_receipts: i64 = db.query_row(
        "SELECT COUNT(*) FROM runtime_operations WHERE kind='operator_agent_archive' AND phase='completed'",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(
        archived_receipts, 2,
        "one native close and one unkeyed no-op each have a durable terminal receipt"
    );
}

#[tokio::test]
async fn mismatched_provider_close_ack_never_archives_or_retries_the_native_effect() {
    let (router, token, _temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    provider.set_archive_close_mode(3);
    let baseline = provider.archive_close_count();
    let (status, unknown, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        None,
        Some("wrong-native-close-proof"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    assert_eq!(provider.archive_close_count(), baseline + 1);
    assert_eq!(
        runtime
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        revision
    );
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "ready"
    );
    let (status, same, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        None,
        Some("wrong-native-close-proof"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{same:?}");
    assert_eq!(
        same["error"]["details"]["operation_id"],
        unknown["error"]["details"]["operation_id"]
    );
    assert_eq!(provider.archive_close_count(), baseline + 1);
}

#[tokio::test]
async fn archive_review_rejects_stale_etag_and_unknown_evidence_fields_before_mutation() {
    let (router, token, _temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let workspace = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let original = runtime.get_session(&agent_id).await.unwrap();
    provider.set_archive_close_mode(4);
    let (status, unknown, _) = archive_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        None,
        Some("review-preconditions"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}/archives/{op_id}/resolve");
    let correct = review_input(&original, revision, "not_closed");
    let baseline = provider.archive_close_count();

    let mismatch = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, "\"99999\"")
                .body(Body::from(correct.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mismatch.status(), StatusCode::PRECONDITION_FAILED);
    let mut malformed = correct.clone();
    malformed["force"] = serde_json::json!(true);
    let (status, invalid) = call(&router, &token, "POST", &uri, Some(malformed), None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{invalid:?}");

    let forbidden_key = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "review-would-repeat-native")
                .body(Body::from(correct.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden_key.status(), StatusCode::BAD_REQUEST);
    assert_eq!(provider.archive_close_count(), baseline);
    assert_eq!(
        runtime
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        revision
    );

    let success = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, format!("\"{revision}\""))
                .body(Body::from(correct.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = success.status();
    let bytes = to_bytes(success.into_body(), usize::MAX).await.unwrap();
    let refused: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_refusal(status, &refused, op_id);
    assert_eq!(provider.archive_close_count(), baseline);
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), original);
    assert_eq!(
        runtime
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        revision
    );
}
