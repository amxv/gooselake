use super::native_review_refusal::{assert_refusal, database_snapshot, refuse_prose_variants};
use super::operator_native_policy::{
    claude_permission_fixture, operator_policy_fixture, permission_body, preference_body,
};
use super::workspace_worktree_routes::call;
use super::*;

struct ChangedNativePolicyAttachmentProvider {
    resume_calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl RuntimeProvider for ChangedNativePolicyAttachmentProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "Changed native attachment test".into(),
            enabled: true,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn resume_session_with_policy(
        &self,
        req: runtime_core::ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.resume_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: format!("{}-unexpected-new-attachment", req.provider_session_ref),
            canonical_provider_session_ref: req.canonical_provider_session_ref,
        })
    }
}

async fn review_body(
    runtime: &Arc<RuntimeSessionManager>,
    agent_id: &str,
    revision: u64,
    resolution: &str,
) -> serde_json::Value {
    let session = runtime.get_session(agent_id).await.unwrap();
    serde_json::json!({
        "resolution":resolution,
        "evidence":"Operator independently inspected exact native provider session and verified its policy state",
        "provider":session.provider,
        "provider_session_ref":session.provider_session_ref,
        "canonical_provider_session_ref":session.canonical_provider_session_ref,
        "expected_revision":revision,
        "session_updated_at":session.updated_at,
    })
}

async fn reopened_runtime(
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

fn policy_intent_without_native_dispatch(
    temp: &tempfile::TempDir,
    agent_id: &str,
    agent: &runtime_core::WorkspaceAgentRecord,
    session: &runtime_core::SessionRecord,
    key: &str,
    operation_id: &str,
) {
    let repo = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let request = serde_json::json!({
        "agent_id":agent_id,"kind":"preferences","expected_revision":agent.revision,
        "current_preferences":{"thinking_effort":"high"},
    });
    let command = runtime_core::OperatorNativePolicyCommand {
        operation_id: operation_id.into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: key.into(),
        workspace_id: agent.workspace_id.clone(),
        agent_id: agent_id.into(),
        kind: runtime_core::OperatorNativePolicyKind::Preferences,
        expected_revision: agent.revision,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: session.updated_at + 1,
    };
    assert!(matches!(
        repo.repository()
            .admit_operator_native_policy(&command)
            .unwrap(),
        runtime_core::OperatorNativePolicyAdmission::Execute { .. }
    ));
    repo.repository()
        .mark_operator_native_policy_uncertain(
            operation_id,
            "operator observed native effect was never dispatched",
            session.updated_at + 2,
        )
        .unwrap();
}

#[tokio::test]
async fn native_policy_prose_refusal_is_isolated_from_unrelated_corrupt_metadata() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    let operation_id = "op_policy_review_isolated_hydration";
    policy_intent_without_native_dispatch(
        &temp,
        &agent_id,
        &agent,
        &session,
        "review-isolated-hydration-key",
        operation_id,
    );

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO sessions
         SELECT 'unrelated_bad_metadata',provider,status,cwd,model,permission_mode,
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

    let baseline_native_calls = provider.preferences_mutation_calls();
    let body = review_body(&runtime, &agent_id, revision, "not_applied").await;
    let path = format!("/v2/agents/{agent_id}/native-policy/{operation_id}/resolve");
    let original_operation = store.get_operation(operation_id).unwrap().unwrap();
    refuse_prose_variants(&router, &token, &db, &path, operation_id, vec![body]).await;
    assert_eq!(
        store.get_operation(operation_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), session);
    assert_eq!(
        runtime.get_active_workspace_agent_by_id(&agent_id).unwrap(),
        agent
    );
    assert_eq!(provider.preferences_mutation_calls(), baseline_native_calls);
    assert!(
        store.hydrate_runtime_state().is_err(),
        "unrelated row stays corrupt"
    );
}

#[tokio::test]
async fn concurrent_applied_policy_prose_keeps_original_unknown_authority() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    provider.set_preferences_mutation_mode(2);
    let uri = format!("/v2/agents/{agent_id}/preferences");
    let body = preference_body(revision, "high");
    let native_calls = provider.preferences_mutation_calls();
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("applied-review-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    assert_eq!(provider.preferences_mutation_calls(), native_calls + 1);

    let input = review_body(&runtime, &agent_id, revision, "applied").await;
    let review_uri = format!("/v2/agents/{agent_id}/native-policy/{op_id}/resolve");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original_operation = store.get_operation(op_id).unwrap().unwrap();
    let agent_before = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session_before = runtime.get_session(&agent_id).await.unwrap();
    let before = database_snapshot(&db);
    let (a, b) = tokio::join!(
        call(
            &router,
            &token,
            "POST",
            &review_uri,
            Some(input.clone()),
            None
        ),
        call(
            &router,
            &token,
            "POST",
            &review_uri,
            Some(input.clone()),
            None
        ),
    );
    assert_refusal(a.0, &a.1, op_id);
    assert_refusal(b.0, &b.1, op_id);
    assert_eq!(
        a.1["error"], b.1["error"],
        "concurrent prose requests retain the same original unresolved coordinate"
    );
    assert_eq!(database_snapshot(&db), before);
    assert_eq!(
        store.get_operation(op_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(
        runtime.get_active_workspace_agent_by_id(&agent_id).unwrap(),
        agent_before
    );
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap(),
        session_before
    );

    let (status, original_key) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body),
        Some("applied-review-key"),
    )
    .await;
    assert_refusal(status, &original_key, op_id);
    let (status, blocked) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({
            "input":[{"type":"text","text":"no stale native dispatch"}]
        })),
        Some("turn-before-policy-reattachment"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(provider.preferences_mutation_calls(), native_calls + 1);

    let (phase, effect_phase, claim_count, transitions): (String, String, i64, i64) =
        db.query_row(
            "SELECT o.phase,e.phase,
             (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=o.operation_id),
             (SELECT COUNT(*) FROM runtime_operation_transitions WHERE operation_id=o.operation_id)
             FROM runtime_operations o JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
             WHERE o.operation_id=?1",
            [op_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).unwrap();
    assert_eq!(
        (
            phase.as_str(),
            effect_phase.as_str(),
            claim_count,
            transitions
        ),
        ("manual_review", "uncertain", 1, 2)
    );

    let resumes = provider.restore_call_count().await;
    let reopened = reopened_runtime(&temp, provider.clone()).await;
    reopened.recover_startup().await.unwrap();
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap(),
        session_before
    );
    assert_eq!(
        reopened
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap(),
        agent_before
    );
    assert_eq!(
        store.get_operation(op_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(provider.restore_call_count().await, resumes);
    assert_eq!(provider.preferences_mutation_calls(), native_calls + 1);
}

#[tokio::test]
async fn not_applied_prose_preserves_original_policy_claim_without_dispatch() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session = runtime.get_session(&agent_id).await.unwrap();
    let op_id = "op_policy_verified_not_applied";
    policy_intent_without_native_dispatch(
        &temp,
        &agent_id,
        &agent,
        &session,
        "review-not-applied-key",
        op_id,
    );
    let native_calls = provider.preferences_mutation_calls();
    let input = review_body(&runtime, &agent_id, revision, "not_applied").await;
    let path = format!("/v2/agents/{agent_id}/native-policy/{op_id}/resolve");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original_operation = store.get_operation(op_id).unwrap().unwrap();
    refuse_prose_variants(&router, &token, &db, &path, op_id, vec![input]).await;

    let (status, original_key) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/preferences"),
        Some(preference_body(revision, "high")),
        Some("review-not-applied-key"),
    )
    .await;
    assert_refusal(status, &original_key, op_id);
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
    assert_eq!(
        runtime.get_active_workspace_agent_by_id(&agent_id).unwrap(),
        agent
    );
    assert_eq!(runtime.get_session(&agent_id).await.unwrap(), session);

    let (status, blocked) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/preferences"),
        Some(preference_body(revision, "low")),
        Some("another-key-before-reattach"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(provider.preferences_mutation_calls(), native_calls);

    let resumes = provider.restore_call_count().await;
    let reopened = reopened_runtime(&temp, provider.clone()).await;
    reopened.recover_startup().await.unwrap();
    assert_eq!(reopened.get_session(&agent_id).await.unwrap(), session);
    assert_eq!(
        reopened
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap(),
        agent
    );
    assert_eq!(
        store.get_operation(op_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(original_operation.claims.len(), 1);
    assert_eq!(provider.restore_call_count().await, resumes);
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
}

#[tokio::test]
async fn native_policy_review_rejects_wrong_attachment_and_never_reaches_write_trigger() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    provider.set_preferences_mutation_mode(2);
    let uri = format!("/v2/agents/{agent_id}/preferences");
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(preference_body(revision, "high")),
        Some("review-cas-failure"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let native_calls = provider.preferences_mutation_calls();
    let path = format!("/v2/agents/{agent_id}/native-policy/{op_id}/resolve");
    let input = review_body(&runtime, &agent_id, revision, "applied").await;
    let mut wrong = input.clone();
    wrong["provider_session_ref"] = serde_json::json!("another-native-provider-session");
    let (status, rejected) = call(&router, &token, "POST", &path, Some(wrong), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected:?}");
    let mut stale = input.clone();
    stale["expected_revision"] = serde_json::json!(revision + 1);
    let (status, rejected) = call(&router, &token, "POST", &path, Some(stale), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected:?}");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER deny_native_policy_review
        BEFORE UPDATE OF recreation_policy_json ON workspace_agents
        BEGIN SELECT RAISE(ABORT,'test policy review transaction failure'); END;",
    )
    .unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original_operation = store.get_operation(op_id).unwrap().unwrap();
    let before = database_snapshot(&db);
    let (status, failed) = call(&router, &token, "POST", &path, Some(input.clone()), None).await;
    assert_refusal(status, &failed, op_id);
    assert_eq!(database_snapshot(&db), before);

    let (phase, claim_count, session_status): (String, i64, String) = db.query_row(
        "SELECT o.phase,
         (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=o.operation_id),
         (SELECT s.status FROM sessions s WHERE s.id=?2)
         FROM runtime_operations o WHERE o.operation_id=?1",
        rusqlite::params![op_id, agent_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert_eq!(
        (phase.as_str(), claim_count, session_status.as_str()),
        ("manual_review", 1, "ready")
    );

    db.execute_batch("DROP TRIGGER deny_native_policy_review")
        .unwrap();
    refuse_prose_variants(&router, &token, &db, &path, op_id, vec![input]).await;
    assert_eq!(
        store.get_operation(op_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
}

#[tokio::test]
async fn unkeyed_public_native_policies_are_one_shot_journaled_instead_of_untraceable() {
    let (router, token, temp, provider, _runtime, agent_id, revision) =
        operator_policy_fixture().await;
    let before = provider.preferences_mutation_calls();
    let (status, accepted) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/preferences"),
        Some(preference_body(revision, "high")),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{accepted:?}");
    assert_eq!(accepted["revision"], revision + 1);
    assert_eq!(provider.preferences_mutation_calls(), before + 1);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (id,key,phase,effect): (String,String,String,String) = db.query_row(
        "SELECT o.operation_id,o.idempotency_key,o.phase,e.phase
         FROM runtime_operations o JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
         WHERE o.kind='operator_provider_policy_mutation' AND o.normalized_request_json LIKE '%preferences%'",
        [],|row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
    ).unwrap();
    assert!(id.starts_with("op_policy_"));
    assert!(key.starts_with("implicit-policy-"), "{key}");
    assert_eq!(
        (phase.as_str(), effect.as_str()),
        ("completed", "finalized")
    );

    let (router, token, temp, provider, _runtime, agent_id, revision) =
        operator_policy_fixture().await;
    provider.set_preferences_mutation_mode(2);
    let baseline = provider.preferences_mutation_calls();
    let uri = format!("/v2/agents/{agent_id}/preferences");
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(preference_body(revision, "high")),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    assert_eq!(unknown["error"]["code"], "outcome_unresolved");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (key, phase, claim): (String, String, i64) = db
        .query_row(
            "SELECT o.idempotency_key,o.phase,
         (SELECT COUNT(*) FROM runtime_operation_resource_claims c
          WHERE c.owner_operation_id=o.operation_id)
         FROM runtime_operations o WHERE o.operation_id=?1",
            [op_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(key.starts_with("implicit-policy-"));
    assert_eq!((phase.as_str(), claim), ("manual_review", 1));
    let (status, retry) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(preference_body(revision, "high")),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["details"]["operation_id"], op_id);
    assert_eq!(provider.preferences_mutation_calls(), baseline + 1);

    let (router, token, temp, claude, _runtime, agent_id, revision) =
        claude_permission_fixture().await;
    let baseline = claude.permission_mutation_calls();
    let (status, applied) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/permission"),
        Some(permission_body(revision)),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{applied:?}");
    assert_eq!(applied["revision"], revision + 1);
    assert_eq!(claude.permission_mutation_calls(), baseline + 1);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (key, phase): (String, String) = db
        .query_row(
            "SELECT idempotency_key,phase FROM runtime_operations
         WHERE kind='operator_provider_policy_mutation'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(key.starts_with("implicit-policy-"));
    assert_eq!(phase, "completed");
}

#[tokio::test]
async fn claude_permission_prose_cannot_settle_a_lost_native_receipt() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        claude_permission_fixture().await;
    provider.set_permission_mutation_mode(2);
    let old_calls = provider.permission_mutation_calls();
    let url = format!("/v2/agents/{agent_id}/permission");
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(permission_body(revision)),
        Some("claude-manual-review-applied"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let mut input = review_body(&runtime, &agent_id, revision, "applied").await;
    input["evidence"] =
        serde_json::json!("Checked exact Claude session's native permission mode after lost reply");
    let path = format!("/v2/agents/{agent_id}/native-policy/{id}/resolve");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original_operation = store.get_operation(id).unwrap().unwrap();
    let agent_before = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    let session_before = runtime.get_session(&agent_id).await.unwrap();
    refuse_prose_variants(&router, &token, &db, &path, id, vec![input]).await;

    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(permission_body(revision)),
        Some("claude-manual-review-applied"),
    )
    .await;
    assert_refusal(status, &replay, id);
    let policy = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(policy, agent_before);
    assert_eq!(policy.revision, revision);
    assert_eq!(
        policy.recreation_policy.permission_intent,
        runtime_core::ProviderPermissionIntent::ProviderDefault
    );
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap(),
        session_before
    );
    assert_eq!(
        store.get_operation(id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(
        original_operation.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(original_operation.claims.len(), 1);
    assert_eq!(provider.permission_mutation_calls(), old_calls + 1);
}

#[tokio::test]
async fn policy_prose_cannot_authorize_drifted_or_exact_startup_attachment() {
    let (router, token, temp, provider, runtime, agent_id, revision) =
        operator_policy_fixture().await;
    provider.set_preferences_mutation_mode(2);
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/preferences"),
        Some(preference_body(revision, "high")),
        Some("policy-identity-drift"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op_id = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let body = review_body(&runtime, &agent_id, revision, "applied").await;
    let path = format!("/v2/agents/{agent_id}/native-policy/{op_id}/resolve");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let original_operation = store.get_operation(op_id).unwrap().unwrap();
    let original_session = runtime.get_session(&agent_id).await.unwrap();
    let original_agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    refuse_prose_variants(&router, &token, &db, &path, op_id, vec![body]).await;
    let native_calls = provider.preferences_mutation_calls();
    let resumes = provider.restore_call_count().await;

    let probe = Arc::new(ChangedNativePolicyAttachmentProvider {
        resume_calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(probe.clone()).unwrap();
    let changed = Arc::new(
        RuntimeSessionManager::new(
            Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
                database_path: temp.path().join("runtime.sqlite3"),
            })),
            Arc::new(registry),
            128,
        )
        .unwrap(),
    );
    changed.recover_startup().await.unwrap();
    assert_eq!(
        probe.resume_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "an unresolved policy cannot authorize even a drift-capable resume"
    );
    assert_eq!(
        changed.get_session(&agent_id).await.unwrap(),
        original_session
    );
    assert_eq!(
        changed.get_active_workspace_agent_by_id(&agent_id).unwrap(),
        original_agent
    );
    assert_eq!(
        store.get_operation(op_id).unwrap().unwrap(),
        original_operation
    );

    let reopened = reopened_runtime(&temp, provider.clone()).await;
    reopened.recover_startup().await.unwrap();
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap(),
        original_session
    );
    assert_eq!(
        reopened
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap(),
        original_agent
    );
    assert_eq!(
        store.get_operation(op_id).unwrap().unwrap(),
        original_operation
    );
    assert_eq!(provider.restore_call_count().await, resumes);
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
}
