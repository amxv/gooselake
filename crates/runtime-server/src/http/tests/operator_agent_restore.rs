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
}

#[tokio::test]
async fn unknown_restore_consumes_key_and_not_resumed_review_permits_new_attempt_only() {
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
    let (status, result) = call(&router, &token, "POST", &uri, Some(evidence.clone()), None).await;
    assert_eq!(status, StatusCode::OK, "{result:?}");
    assert_eq!(result["resolution"], "not_resumed");
    assert_eq!(result["restart_required"], false);
    let (status, same) = call(&router, &token, "POST", &uri, Some(evidence.clone()), None).await;
    assert_eq!(status, StatusCode::OK, "{same:?}");
    assert_eq!(result, same);
    let mut contradictory = evidence;
    contradictory["resolution"] = serde_json::json!("resumed");
    contradictory["observed_provider_session_ref"] =
        serde_json::json!(original.provider_session_ref);
    let (status, _) = call(&router, &token, "POST", &uri, Some(contradictory), None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, old, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("unknown-restore-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{old:?}");
    assert_eq!(old["error"]["code"], "operator_verified_not_resumed");
    assert_eq!(provider.restore_call_count().await, baseline + 1);
    provider.set_restore_failure_mode(0);
    let (status, restored, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("after-reviewed-not-resumed"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{restored:?}");
    assert_eq!(restored["revision"], revision + 1);
    assert_eq!(provider.restore_call_count().await, baseline + 2);
}

#[tokio::test]
async fn reviewed_resumed_restore_is_fenced_until_exact_startup_reattachment() {
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
    let (status, reviewed) =
        call(&router, &token, "POST", &uri, Some(evidence.clone()), None).await;
    assert_eq!(status, StatusCode::OK, "{reviewed:?}");
    assert_eq!(reviewed["resolution"], "resumed");
    assert_eq!(reviewed["restart_required"], true);
    assert_eq!(reviewed["agent_revision"], revision + 1);
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "restore_recovery_required"
    );
    assert_eq!(
        runtime
            .get_workspace_agent(&workspace, &agent_id)
            .unwrap()
            .lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Active
    );
    assert_eq!(provider.restore_call_count().await, baseline + 1);

    let (status, replay, _) = restore_http(
        &router,
        &token,
        &workspace,
        &agent_id,
        Some("known-resume-lost-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay["lifecycle_state"], "active");
    let (status, blocked) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"should be fenced"}]})),
        Some("no-native-before-startup"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(provider.restore_call_count().await, baseline + 1);

    let (status, same) = call(&router, &token, "POST", &uri, Some(evidence), None).await;
    assert_eq!(status, StatusCode::OK, "{same:?}");
    assert_eq!(same, reviewed);
    provider.set_restore_failure_mode(3);
    let wrong_attachment = reopen_runtime(&temp, provider.clone()).await;
    let mismatch = wrong_attachment.recover_startup().await.unwrap();
    assert!(
        mismatch
            .notes
            .iter()
            .any(|note| note.contains("still fenced")),
        "{mismatch:?}"
    );
    let still_fenced = wrong_attachment.get_session(&agent_id).await.unwrap();
    assert_eq!(still_fenced.status, "restore_recovery_required");
    assert_eq!(
        still_fenced.failure_code.as_deref(),
        Some("startup_restore_identity_changed")
    );
    assert_eq!(provider.restore_call_count().await, baseline + 2);
    provider.set_restore_failure_mode(0);
    let reopened = reopen_runtime(&temp, provider.clone()).await;
    let recovery = reopened.recover_startup().await.unwrap();
    assert!(
        recovery
            .notes
            .iter()
            .any(|note| note.contains("restore reattached with durable settings")),
        "{recovery:?}"
    );
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap().status,
        "ready"
    );
    assert_eq!(provider.restore_call_count().await, baseline + 3);
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
            .any(|note| note.contains("op_restore_crash_before_native_send")
                && note.contains("manual_review")),
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
