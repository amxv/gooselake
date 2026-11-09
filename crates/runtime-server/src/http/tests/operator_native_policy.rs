use super::workspace_worktree_routes::call;
use super::*;

async fn operator_policy_fixture() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    Arc<RuntimeSessionManager>,
    String,
    u64,
) {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    provider.enable_thinking_mutation();
    let root = temp.path().join("operator-native-policy");
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
    let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let agent_id = agent["agent_id"].as_str().unwrap().to_string();
    finish_test_onboarding(&runtime, workspace_id, &agent_id).await;
    let revision = agent["revision"].as_u64().unwrap();
    (router, token, temp, provider, runtime, agent_id, revision)
}

fn preference_body(revision: u64, effort: &str) -> serde_json::Value {
    serde_json::json!({
        "expected_revision":revision,
        "current_preferences":{"thinking_effort":effort},
    })
}

async fn claude_permission_fixture() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestClaudeProvider>,
    Arc<RuntimeSessionManager>,
    String,
    u64,
) {
    let (router, token, temp, _acp, claude, runtime) =
        build_mixed_provider_test_router_with_claude().await;
    let root = temp.path().join("operator-native-claude-permission");
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
    let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(serde_json::json!({"provider":"claude","model":"test-claude-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let agent_id = agent["agent_id"].as_str().unwrap().to_string();
    finish_test_onboarding(&runtime, workspace_id, &agent_id).await;
    let revision = agent["revision"].as_u64().unwrap();
    (router, token, temp, claude, runtime, agent_id, revision)
}

fn permission_body(revision: u64) -> serde_json::Value {
    serde_json::json!({
        "expected_revision":revision,
        "permission_intent":{"kind":"inherit_provider_configuration"},
    })
}

#[tokio::test]
async fn native_claude_permission_key_replays_exact_receipt_without_second_mutation() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        claude_permission_fixture().await;
    let uri = format!("/v2/agents/{agent_id}/permission");
    let body = permission_body(revision);
    let baseline = provider.permission_mutation_calls();
    let (status, first) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("claude-permission-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first:?}");
    assert_eq!(first["revision"], revision + 1);
    assert_eq!(
        first["permission_intent"]["kind"],
        "inherit_provider_configuration"
    );
    assert_eq!(provider.permission_mutation_calls(), baseline + 1);
    let (status, again) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body),
        Some("claude-permission-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{again:?}");
    assert_eq!(again, first);
    assert_eq!(provider.permission_mutation_calls(), baseline + 1);
    let policy = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(policy.revision, revision + 1);
    assert_eq!(
        policy.recreation_policy.permission_intent,
        runtime_core::ProviderPermissionIntent::InheritProviderConfiguration
    );
    let connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (phase, receipt): (String, String) = connection
        .query_row(
            "SELECT phase, exact_terminal_result_json FROM runtime_operations WHERE idempotency_key='claude-permission-once'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(phase, "completed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&receipt).unwrap(),
        first
    );
}

#[tokio::test]
async fn ambiguous_claude_permission_ack_remains_fenced_without_native_retry() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        claude_permission_fixture().await;
    provider.set_permission_mutation_mode(2);
    let uri = format!("/v2/agents/{agent_id}/permission");
    let body = permission_body(revision);
    let baseline = provider.permission_mutation_calls();
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("claude-permission-lost-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    let operation_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    assert_eq!(provider.permission_mutation_calls(), baseline + 1);
    let (status, retry) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body),
        Some("claude-permission-lost-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["details"]["operation_id"], operation_id);
    assert_eq!(provider.permission_mutation_calls(), baseline + 1);
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(agent.revision, revision);
    assert_eq!(
        agent.recreation_policy.permission_intent,
        runtime_core::ProviderPermissionIntent::ProviderDefault
    );
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let phase: String = db
        .query_row(
            "SELECT phase FROM runtime_operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(phase, "manual_review");
    let claims: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE resource_kind='operator_provider_policy_native' AND owner_operation_id=?1",
            [operation_id], |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claims, 1);
}

#[tokio::test]
async fn sqlite_receipt_failure_after_native_ack_quarantines_without_committing_policy() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    // A failed authoritative policy CAS after native acknowledgement must
    // neither invent success nor attempt a compensating native write.
    db.execute_batch(
        "CREATE TRIGGER deny_native_policy_receipt BEFORE UPDATE OF recreation_policy_json ON workspace_agents
         BEGIN SELECT RAISE(ABORT, 'forced atomic policy receipt rejection'); END;",
    )
    .unwrap();
    let uri = format!("/v2/agents/{agent_id}/preferences");
    let body = preference_body(revision, "high");
    let baseline = provider.preferences_mutation_calls();
    let (status, failed) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("post-native-sqlite-receipt-failure"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{failed:?}");
    assert_eq!(failed["error"]["code"], "outcome_unresolved");
    assert_eq!(provider.preferences_mutation_calls(), baseline + 1);
    let operation_id = failed["error"]["details"]["operation_id"].as_str().unwrap();
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(agent.revision, revision);
    assert_eq!(
        agent.recreation_policy.current_preferences,
        runtime_core::ProviderSessionPreferences::default(),
        "session/provider acknowledgement alone cannot advance SQLite authority"
    );
    let phase: String = db
        .query_row(
            "SELECT phase FROM runtime_operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(phase, "manual_review");
    let unresolved_claims: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims
             WHERE resource_kind='operator_provider_policy_native' AND owner_operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unresolved_claims, 1);
    db.execute_batch("DROP TRIGGER deny_native_policy_receipt;")
        .unwrap();
    for key in [
        "post-native-sqlite-receipt-failure",
        "another-key-after-failure",
    ] {
        let (status, retry) =
            call(&router, &token, "POST", &uri, Some(body.clone()), Some(key)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
        assert_eq!(retry["error"]["code"], "outcome_unresolved");
        assert_eq!(provider.preferences_mutation_calls(), baseline + 1);
    }
}

#[tokio::test]
async fn concurrent_same_key_native_preference_calls_converge_on_one_effect() {
    let (router, token, temp, provider, _runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let uri = format!("/v2/agents/{agent_id}/preferences");
    let body = preference_body(revision, "high");
    let baseline = provider.preferences_mutation_calls();
    let (first, second) = tokio::join!(
        call(
            &router,
            &token,
            "POST",
            &uri,
            Some(body.clone()),
            Some("simultaneous-native-policy-key")
        ),
        call(
            &router,
            &token,
            "POST",
            &uri,
            Some(body),
            Some("simultaneous-native-policy-key")
        ),
    );
    assert_eq!(first.0, StatusCode::OK, "{:?}", first.1);
    assert_eq!(second.0, StatusCode::OK, "{:?}", second.1);
    assert_eq!(first.1, second.1);
    assert_eq!(provider.preferences_mutation_calls(), baseline + 1);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let operations: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE idempotency_key='simultaneous-native-policy-key'",
            [], |row| row.get(0),
        )
        .unwrap();
    assert_eq!(operations, 1);
}

#[tokio::test]
async fn crash_after_durable_policy_intent_before_native_call_becomes_manual_review() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let before = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    let operation_id = "op_policy_crashed_after_intent";
    let request = serde_json::json!({
        "agent_id":agent_id,
        "kind":"preferences",
        "expected_revision":revision,
        "current_preferences":{"thinking_effort":"high"},
    });
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let native_calls = provider.preferences_mutation_calls();
    let admit = store
        .repository()
        .admit_operator_native_policy(&runtime_core::OperatorNativePolicyCommand {
            operation_id: operation_id.into(),
            actor: runtime_core::OperationActor::operator("runtime_operator"),
            idempotency_key: "crash-between-intent-and-native-effect".into(),
            workspace_id: before.workspace_id,
            agent_id: agent_id.clone(),
            kind: runtime_core::OperatorNativePolicyKind::Preferences,
            expected_revision: revision,
            provider: session.provider.clone(),
            provider_session_ref: session.provider_session_ref.clone(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            session_updated_at: session.updated_at,
            normalized_request: request.clone(),
            normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
            requested_at: session.updated_at + 1,
        })
        .unwrap();
    assert!(matches!(
        admit,
        runtime_core::OperatorNativePolicyAdmission::Execute { .. }
    ));
    assert_eq!(provider.preferences_mutation_calls(), native_calls);

    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(provider.clone()).unwrap();
    let reopened = Arc::new(
        RuntimeSessionManager::new(
            Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
                database_path: temp.path().join("runtime.sqlite3"),
            })),
            Arc::new(providers),
            128,
        )
        .unwrap(),
    );
    let summary = reopened.recover_startup().await.unwrap();
    assert!(
        summary
            .notes
            .iter()
            .any(|note| { note.contains(operation_id) && note.contains("became manual_review") }),
        "{summary:?}"
    );
    assert!(
        summary
            .notes
            .iter()
            .any(|note| { note.contains(operation_id) && note.contains("resume withheld") }),
        "{summary:?}"
    );
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (phase, effect_phase): (String, String) = db
        .query_row(
            "SELECT o.phase,e.phase FROM runtime_operations o
         JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
         WHERE o.operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(phase, "manual_review");
    assert_eq!(effect_phase, "uncertain");
    let active_claims: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims
         WHERE owner_operation_id=?1 AND resource_kind='operator_provider_policy_native'",
            [operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_claims, 1);
    let second_recovery = reopened.recover_startup().await.unwrap();
    assert!(
        !second_recovery
            .notes
            .iter()
            .any(|note| note.contains("became manual_review")),
        "startup quarantine is idempotent, no duplicate operation transitions"
    );
    let (status, retry) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/preferences"),
        Some(preference_body(revision, "high")),
        Some("crash-between-intent-and-native-effect"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["code"], "outcome_unresolved");
    assert_eq!(retry["error"]["details"]["operation_id"], operation_id);
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
}

#[tokio::test]
async fn native_preference_key_replays_exact_receipt_after_revision_and_reopen() {
    let (router, token, temp, provider, _runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let url = format!("/v2/agents/{agent_id}/preferences");
    let body = preference_body(revision, "high");
    let baseline_calls = provider.preferences_mutation_calls();
    let (status, created) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(body.clone()),
        Some("native-prefs-replay-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created:?}");
    assert_eq!(created["revision"], revision + 1);
    assert_eq!(created["current_preferences"]["thinking_effort"], "high");
    assert_eq!(provider.preferences_mutation_calls(), baseline_calls + 1);
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(body.clone()),
        Some("native-prefs-replay-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay, created);
    assert_eq!(
        provider.preferences_mutation_calls(),
        baseline_calls + 1,
        "retry must not redispatch native mutation"
    );

    let connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (op_id, phase, exact): (String, String, String) = connection
        .query_row(
            "SELECT operation_id,phase,exact_terminal_result_json FROM runtime_operations
         WHERE idempotency_key='native-prefs-replay-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(phase, "completed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&exact).unwrap(),
        created
    );
    assert!(op_id.starts_with("op_policy_"));
    let active_claims: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims
         WHERE resource_kind='operator_provider_policy_native' AND resource_id=?1",
            [&agent_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        active_claims, 0,
        "confirmed policy must release exact native claim"
    );
    drop(connection);

    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let exact_request = serde_json::json!({
        "agent_id":agent_id,"kind":"preferences","expected_revision":revision,
        "current_preferences":{"thinking_effort":"high"},
    });
    let durable = reopened
        .repository()
        .replay_operator_native_policy(
            &runtime_core::OperationActor::operator("runtime_operator"),
            "native-prefs-replay-1",
            &agent_id,
            runtime_core::OperatorNativePolicyKind::Preferences,
            &runtime_core::normalized_json_hash(&exact_request).unwrap(),
        )
        .unwrap()
        .unwrap();
    let (stored, receipt_op): (
        runtime_core::ProviderSessionPreferencesMutationResult,
        String,
    ) = durable.into_replay_result().unwrap();
    assert_eq!(stored.revision, revision + 1);
    assert_eq!(receipt_op, op_id);

    let (status, changed) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(preference_body(revision, "low")),
        Some("native-prefs-replay-1"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{changed:?}");
    assert_eq!(changed["error"]["code"], "conflict");
    assert_eq!(provider.preferences_mutation_calls(), baseline_calls + 1);
    let (status, stale) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(body),
        Some("native-prefs-another-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale:?}");
    assert_eq!(provider.preferences_mutation_calls(), baseline_calls + 1);
}

#[tokio::test]
async fn ambiguous_native_preference_ack_quarantines_and_fences_new_turns() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    provider.set_preferences_mutation_mode(2);
    let url = format!("/v2/agents/{agent_id}/preferences");
    let body = preference_body(revision, "high");
    let baseline = provider.preferences_mutation_calls();
    let turn_baseline = provider
        .send_invocations
        .load(std::sync::atomic::Ordering::SeqCst);
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(body.clone()),
        Some("lost-native-policy-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .expect("operation id");
    assert_eq!(provider.preferences_mutation_calls(), baseline + 1);
    let (status, retry) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(body.clone()),
        Some("lost-native-policy-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["details"]["operation_id"], op_id);
    assert_eq!(provider.preferences_mutation_calls(), baseline + 1);
    let (status, new_key) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(body),
        Some("lost-native-policy-new-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{new_key:?}");
    assert_eq!(new_key["error"]["code"], "outcome_unresolved");
    assert_eq!(provider.preferences_mutation_calls(), baseline + 1);

    let (status,turn)=call(&router,&token,"POST",&format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"must not dispatch after unknown policy"}]})),
        Some("turn-after-ambiguous-policy")).await;
    assert_eq!(status, StatusCode::CONFLICT, "{turn:?}");
    assert_eq!(turn["error"]["code"], "outcome_unresolved");
    assert_eq!(
        provider
            .send_invocations
            .load(std::sync::atomic::Ordering::SeqCst),
        turn_baseline
    );

    let connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let phase: String = connection
        .query_row(
            "SELECT phase FROM runtime_operations WHERE operation_id=?1",
            [op_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(phase, "manual_review");
    let retained_claim:i64=connection.query_row("SELECT COUNT(*) FROM runtime_operation_resource_claims
        WHERE resource_kind='operator_provider_policy_native' AND resource_id=?1 AND owner_operation_id=?2",
        rusqlite::params![agent_id,op_id],|row|row.get(0)).unwrap();
    assert_eq!(retained_claim, 1);
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(
        agent.revision, revision,
        "unknown native effect cannot invent a durable policy commit"
    );

    let reloaded_store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let reloaded =
        Arc::new(RuntimeSessionManager::new(reloaded_store, Arc::new(registry), 128).unwrap());
    let summary = reloaded.recover_startup().await.unwrap();
    assert!(
        summary
            .notes
            .iter()
            .any(|note| note.contains(op_id) && note.contains("resume withheld")),
        "startup must respect the exact unresolved policy attachment: {:?}",
        summary.notes
    );
    assert_eq!(provider.preferences_mutation_calls(), baseline + 1);
}
