use super::workspace_worktree_routes::call;
use super::*;

pub(super) async fn fixture() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    Arc<RuntimeSessionManager>,
    String,
) {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    let root = temp.path().join("operator-agent-create");
    std::fs::create_dir_all(&root).unwrap();
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{workspace:?}");
    let workspace_id = workspace["workspace"]["workspace_id"]
        .as_str()
        .unwrap()
        .to_owned();
    (router, token, temp, provider, runtime, workspace_id)
}

pub(super) async fn create_http(
    router: &Router,
    token: &str,
    workspace_id: &str,
    input: serde_json::Value,
    key: Option<&str>,
) -> (StatusCode, serde_json::Value, HeaderMap) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/v2/workspaces/{workspace_id}/agents"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = key {
        builder = builder.header("idempotency-key", key);
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(input.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let raw = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&raw).unwrap(), headers)
}

pub(super) async fn reopened_runtime(
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
async fn keyed_create_replays_the_same_session_profile_and_onboarding_after_restart() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    let input = serde_json::json!({
        "provider":"codex", "model":"test-model", "title":"Researcher",
        "onboarding":{"prompt":"Review the module and report findings"},
    });
    let baseline = provider.create_call_count();
    let (status, created, headers) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("create-keyed-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created:?}");
    assert_eq!(created["lifecycle_state"], "active");
    assert_eq!(created["revision"], 0);
    assert_eq!(created["profile"]["title"], "Researcher");
    let agent_id = created["agent_id"].as_str().unwrap();
    assert!(agent_id.starts_with("sess_"));
    assert_eq!(provider.create_call_count(), baseline + 1);
    let op = headers.get("x-operation-id").unwrap().to_str().unwrap();
    assert!(op.starts_with("op_agent_create_"));
    assert_eq!(headers.get(header::ETAG).unwrap(), "\"0\"");
    let (status, again, retry_headers) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("create-keyed-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{again:?}");
    assert_eq!(again, created);
    assert_eq!(
        headers.get("x-operation-id"),
        retry_headers.get("x-operation-id")
    );
    assert_eq!(provider.create_call_count(), baseline + 1);

    let (status, changed, _) = create_http(
        &router,
        &token,
        &workspace,
        serde_json::json!({"provider":"codex","model":"test-model","title":"Different"}),
        Some("create-keyed-once"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{changed:?}");
    let reopened = reopened_runtime(&temp, provider.clone()).await;
    let (restored, restored_op) = reopened
        .admit_workspace_agent_create_operator(
            &workspace,
            &serde_json::from_value(input).unwrap(),
            runtime_core::OperationActor::operator("runtime_operator"),
            "create-keyed-once".into(),
        )
        .await
        .unwrap();
    let (replay_after_restart, _) = restored.into_replay_result().unwrap();
    assert_eq!(serde_json::to_value(replay_after_restart).unwrap(), created);
    assert!(restored_op.is_none());
    assert_eq!(provider.create_call_count(), baseline + 1);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let details = store.repository().get_operation(op).unwrap().unwrap();
    assert_eq!(
        details.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(
        details.operation.exact_terminal_result.as_ref(),
        Some(&created)
    );
    assert!(details.claims.is_empty());
    assert_eq!(details.effects.len(), 1);
    assert_eq!(details.effects[0].phase, "finalized");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (owner, notices): (String, i64) = db
        .query_row(
            "SELECT source_operation_id,
         (SELECT COUNT(*) FROM workspace_system_notices WHERE recipient_agent_id=?1)
         FROM workspace_session_ownership WHERE session_id=?1",
            [agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(owner, op);
    assert!(notices >= 1);
    assert_eq!(runtime.get_session(agent_id).await.unwrap().status, "ready");
}

#[tokio::test]
async fn ambiguous_native_create_keeps_reserved_identity_and_never_resubmits() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    provider.set_create_failure_mode(2);
    let baseline = provider.create_call_count();
    let (status, uncertain, headers) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("native-ack-lost"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{uncertain:?}");
    assert_eq!(uncertain["error"]["code"], "outcome_unresolved");
    let op = uncertain["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    assert_eq!(provider.create_call_count(), baseline + 1);
    assert_eq!(headers.get("x-operation-id").unwrap(), op);
    assert!(runtime
        .list_workspace_agents(&workspace, None)
        .unwrap()
        .is_empty());
    let (status, retry, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("native-ack-lost"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["details"]["operation_id"], op);
    assert_eq!(provider.create_call_count(), baseline + 1);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let details = store.repository().get_operation(op).unwrap().unwrap();
    assert_eq!(
        details.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(details.effects[0].phase, "uncertain");
    assert_eq!(details.claims.len(), 1);
    let reserved = details.claims[0].resource_id.clone();
    assert!(reserved.starts_with("sess_"));

    // The operator must independently verify the exact reserved native ref;
    // review does not invoke the provider a second time.
    let review_uri = format!("/v2/workspaces/{workspace}/agents/creations/{op}/resolve");
    let reviewed_input = serde_json::json!({
        "resolution":"created",
        "evidence":"Operator independently checked native provider attachment created during lost acknowledgement",
        "reserved_agent_id":reserved,
        "provider":"codex",
        "observed_provider_session_ref":format!("test-thread-{reserved}"),
        "observed_canonical_provider_session_ref":null,
    });
    let (status, review) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(reviewed_input.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{review:?}");
    assert_eq!(review["restart_required"], true);
    assert_eq!(review["resolution"], "created");
    assert_eq!(provider.create_call_count(), baseline + 1);
    assert_eq!(
        runtime.get_session(&reserved).await.unwrap().status,
        "create_recovery_required"
    );
    let (status, replay, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("native-ack-lost"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay["agent_id"], reserved);
    assert_eq!(provider.create_call_count(), baseline + 1);
    let (status, blocked) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{reserved}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"blocked before reattachment"}]})),
        Some("no-early-turn"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");

    provider.set_create_failure_mode(0);
    let reopened = reopened_runtime(&temp, provider.clone()).await;
    let recovered = reopened.recover_startup().await.unwrap();
    assert!(
        recovered
            .notes
            .iter()
            .any(|note| note.contains("create reattached with durable settings")),
        "{recovered:?}"
    );
    assert_eq!(
        reopened.get_session(&reserved).await.unwrap().status,
        "ready"
    );
    assert_eq!(provider.create_call_count(), baseline + 1);
}

#[tokio::test]
async fn keyed_agent_new_worktree_journals_parent_before_git_and_never_creates_twice() {
    let (router, token, temp, provider, workspace, _, _) =
        super::workspace_worktree_routes::fixture().await;
    let input = serde_json::json!({
        "provider":"codex","model":"test-model",
        "onboarding":{"prompt":"Review only the selected checkout"},
        "worktree":{"mode":"new","worktree_name":"native-agent-child-checkout"},
    });
    let baseline = provider.create_call_count();
    let (status, agent, headers) = create_http(
        &router,
        &token,
        &workspace.workspace_id,
        input.clone(),
        Some("agent-with-child-git"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let root = agent["recreation_policy"]["authoritative_cwd"]
        .as_str()
        .unwrap();
    assert_ne!(root, workspace.canonical_root);
    assert!(std::path::Path::new(root).is_dir());
    assert!(agent["provider_session_ref"].is_string());
    let agent_id = agent["agent_id"].as_str().unwrap();
    let parent_op = headers.get("x-operation-id").unwrap().to_str().unwrap();
    assert_eq!(provider.create_call_count(), baseline + 1);

    let (status, replay, same_headers) = create_http(
        &router,
        &token,
        &workspace.workspace_id,
        input.clone(),
        Some("agent-with-child-git"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay, agent);
    assert_eq!(
        headers.get("x-operation-id"),
        same_headers.get("x-operation-id")
    );
    assert_eq!(provider.create_call_count(), baseline + 1);

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (child_operations, claims, recorded_owner): (i64,i64,String) = db.query_row(
        "SELECT
          (SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_worktree_create'
              AND idempotency_key LIKE 'agent-create-wt:%'),
          (SELECT COUNT(*) FROM managed_worktree_claims WHERE session_id=?1 AND released_at IS NULL),
          (SELECT source_operation_id FROM workspace_session_ownership WHERE session_id=?1)",
        [agent_id],|row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    ).unwrap();
    assert_eq!(
        child_operations, 1,
        "exactly one child Git effect was admitted"
    );
    assert_eq!(claims, 1, "agent must own its selected managed checkout");
    assert_eq!(recorded_owner, parent_op);
    let effect_json: String = db
        .query_row(
            "SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1
         AND effect_kind='provider_session_create'",
            [parent_op],
            |row| row.get(0),
        )
        .unwrap();
    let evidence: serde_json::Value = serde_json::from_str(&effect_json).unwrap();
    let child_id = evidence["launch_snapshot"]["worktree_operation_id"]
        .as_str()
        .unwrap();
    let child: (String, String) = db
        .query_row(
            "SELECT phase,idempotency_key FROM runtime_operations WHERE operation_id=?1",
            [child_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(child.0, "completed");
    assert_eq!(child.1, format!("agent-create-wt:{parent_op}"));

    let mut changed = input;
    changed["worktree"]["worktree_name"] = serde_json::json!("different-child");
    let (status, reused, _) = create_http(
        &router,
        &token,
        &workspace.workspace_id,
        changed,
        Some("agent-with-child-git"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{reused:?}");
    assert_eq!(provider.create_call_count(), baseline + 1);
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_worktree_create'
          AND idempotency_key LIKE 'agent-create-wt:%'",
            [],
            |row| row.get(0)
        )
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn verified_absent_native_create_consumes_original_key_and_allows_new_key() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    provider.set_create_failure_mode(4);
    let baseline = provider.create_call_count();
    let (status, unknown, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("confirmed-not-created"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    assert_eq!(provider.create_call_count(), baseline + 1);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let details = store.repository().get_operation(op).unwrap().unwrap();
    assert_eq!(details.claims.len(), 1);
    let reserved = &details.claims[0].resource_id;
    let review_uri = format!("/v2/workspaces/{workspace}/agents/creations/{op}/resolve");
    let evidence = serde_json::json!({
        "resolution":"not_created",
        "reserved_agent_id":reserved,
        "provider":"codex",
        "evidence":"Independent inspection verified no provider session exists with the original reserved identity",
    });
    let (status, verified) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(evidence.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{verified:?}");
    assert_eq!(verified["resolution"], "not_created");
    assert_eq!(verified["restart_required"], false);
    let (status, same_review) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(evidence.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{same_review:?}");
    assert_eq!(same_review, verified);
    let (status, original_key, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("confirmed-not-created"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{original_key:?}");
    assert_eq!(
        original_key["error"]["code"],
        "operator_verified_not_created"
    );
    assert!(runtime
        .list_workspace_agents(&workspace, None)
        .unwrap()
        .is_empty());
    assert_eq!(provider.create_call_count(), baseline + 1);

    provider.set_create_failure_mode(0);
    let (status, new_created, _) = create_http(
        &router,
        &token,
        &workspace,
        input,
        Some("new-after-verified-absence"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{new_created:?}");
    assert_ne!(new_created["agent_id"].as_str(), Some(reserved.as_str()));
    assert_eq!(provider.create_call_count(), baseline + 2);
}

#[tokio::test]
async fn sql_commit_failure_after_native_create_remains_quarantined_until_exact_review() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER deny_agent_create_receipt BEFORE INSERT ON workspace_agents
         BEGIN SELECT RAISE(ABORT, 'injected atomic agent creation receipt failure'); END;",
    )
    .unwrap();
    let baseline = provider.create_call_count();
    let (status, unknown, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("native-succeeded-sql-failed"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    assert_eq!(provider.create_call_count(), baseline + 1);
    let op = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let (phase, claim, session_count): (String, i64, i64) = db
        .query_row(
            "SELECT phase,
            (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1),
            (SELECT COUNT(*) FROM workspace_session_ownership WHERE source_operation_id=?1)
         FROM runtime_operations WHERE operation_id=?1",
            [op],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(phase, "manual_review");
    assert_eq!(claim, 1);
    assert_eq!(
        session_count, 0,
        "failed SQLite receipt must roll back all agent ownership"
    );
    assert!(runtime
        .list_workspace_agents(&workspace, None)
        .unwrap()
        .is_empty());
    let (status, unresolved_again, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("native-succeeded-sql-failed"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unresolved_again:?}");
    assert_eq!(provider.create_call_count(), baseline + 1);

    let reserved: String = db
        .query_row(
            "SELECT target_id FROM runtime_operation_effects WHERE operation_id=?1",
            [op],
            |row| row.get(0),
        )
        .unwrap();
    let review_uri = format!("/v2/workspaces/{workspace}/agents/creations/{op}/resolve");
    let evidence = serde_json::json!({
        "resolution":"created", "provider":"codex",
        "reserved_agent_id":reserved,
        "evidence":"Independently observed native provider session after durable SQLite creation failed",
        "observed_provider_session_ref":format!("test-thread-{reserved}"),
        "observed_canonical_provider_session_ref":null,
    });
    let (status, blocked) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(evidence.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{blocked:?}");
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM workspace_agents WHERE session_id=?1",
            [&reserved],
            |row| row.get(0)
        )
        .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER deny_agent_create_receipt")
        .unwrap();
    let (status, reviewed) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(evidence.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reviewed:?}");
    assert_eq!(reviewed["resolution"], "created");
    assert_eq!(reviewed["restart_required"], true);
    assert_eq!(
        runtime.get_session(&reserved).await.unwrap().status,
        "create_recovery_required"
    );
    let (status, replay, _) = create_http(
        &router,
        &token,
        &workspace,
        input,
        Some("native-succeeded-sql-failed"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay["agent_id"], reserved);
    assert_eq!(provider.create_call_count(), baseline + 1);
    let reopened = reopened_runtime(&temp, provider.clone()).await;
    let recovery = reopened.recover_startup().await.unwrap();
    assert!(
        recovery
            .notes
            .iter()
            .any(|note| note.contains("create reattached with durable settings")),
        "{recovery:?}"
    );
    assert_eq!(
        reopened.get_session(&reserved).await.unwrap().status,
        "ready"
    );
}

#[tokio::test]
async fn restart_quarantines_admitted_agent_create_without_reexecuting_any_native_work() {
    let (router, token, temp, provider, _runtime, workspace) = fixture().await;
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    let typed: runtime_core::WorkspaceAgentCreateRequest =
        serde_json::from_value(input.clone()).unwrap();
    let request = serde_json::json!({
        "workspace_id":workspace,"action":"create","request":typed,
    });
    let command = runtime_core::OperatorAgentCreateCommand {
        operation_id: "op_agent_create_crashed_before_native".into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: "crash-before-create".into(),
        workspace_id: workspace.clone(),
        reserved_agent_id: "sess_codex_reserved_precrash".into(),
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: 100,
        worktree_operation_id: None,
    };
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let admission = store
        .repository()
        .admit_operator_agent_create(&command)
        .unwrap();
    assert!(matches!(
        admission,
        runtime_core::OperatorAgentCreateAdmission::Execute { .. }
    ));
    let baseline = provider.create_call_count();
    let reopened = reopened_runtime(&temp, provider.clone()).await;
    let recovered = reopened.recover_startup().await.unwrap();
    assert!(
        recovered
            .notes
            .iter()
            .any(|note| note.contains(&command.operation_id) && note.contains("manual_review")),
        "{recovered:?}"
    );
    assert_eq!(provider.create_call_count(), baseline);
    let persisted = store
        .repository()
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(persisted.effects[0].phase, "uncertain");
    assert_eq!(persisted.claims.len(), 1);
    let (status, retry, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("crash-before-create"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(
        retry["error"]["details"]["operation_id"],
        command.operation_id
    );
    assert_eq!(provider.create_call_count(), baseline);

    // A native session cannot be attested from only a reserved ID; missing
    // frozen launch authority must fail closed rather than minting an agent.
    let uri = format!(
        "/v2/workspaces/{workspace}/agents/creations/{}/resolve",
        command.operation_id
    );
    let pretend = serde_json::json!({
        "resolution":"created","provider":"codex",
        "reserved_agent_id":command.reserved_agent_id,
        "evidence":"A reserved name is not sufficient proof of the approved native launch",
        "observed_provider_session_ref":"fabricated-native-ref",
    });
    let (status, rejected) = call(&router, &token, "POST", &uri, Some(pretend), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected:?}");
    assert_eq!(provider.create_call_count(), baseline);

    let not_created = serde_json::json!({
        "resolution":"not_created","provider":"codex",
        "reserved_agent_id":command.reserved_agent_id,
        "evidence":"Independent provider inspection showed no native session was ever launched",
    });
    let (status, final_review) = call(&router, &token, "POST", &uri, Some(not_created), None).await;
    assert_eq!(status, StatusCode::OK, "{final_review:?}");
    assert_eq!(final_review["restart_required"], false);
    assert!(reopened
        .get_session(&command.reserved_agent_id)
        .await
        .is_err());
    assert_eq!(provider.create_call_count(), baseline);
}
