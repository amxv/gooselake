use super::operator_native_policy::{
    claude_permission_fixture, operator_policy_fixture, permission_body, preference_body,
};
use super::workspace_worktree_routes::call;
use super::*;

struct ChangedNativePolicyAttachmentProvider;

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
async fn reviewed_applied_native_policy_commits_once_and_requires_verified_reattachment() {
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
    assert_eq!(a.0, StatusCode::OK, "{:?}", a.1);
    assert_eq!(b.0, StatusCode::OK, "{:?}", b.1);
    assert_eq!(
        a.1, b.1,
        "duplicate reviews replay the exact durable receipt"
    );
    assert_eq!(a.1["resolution"], "applied");
    assert_eq!(a.1["agent_revision"], revision + 1);
    assert_eq!(a.1["restart_required"], true);
    let agent = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(agent.revision, revision + 1);
    assert_eq!(
        agent.recreation_policy.current_preferences.thinking_effort,
        Some(runtime_core::ProviderThinkingEffort::High)
    );
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "policy_recovery_required"
    );
    let (status, original) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("applied-review-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{original:?}");
    assert_eq!(original["current_preferences"]["thinking_effort"], "high");
    assert_eq!(original["revision"], revision + 1);
    let (status, blocked) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"no stale native dispatch"}]})),
        Some("turn-before-policy-reattachment"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(blocked["error"]["code"], "conflict");
    assert_eq!(provider.preferences_mutation_calls(), native_calls + 1);

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let (phase,effect_phase,claim_count,transitions): (String,String,i64,i64) = db.query_row(
        "SELECT o.phase,e.phase,
          (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=o.operation_id),
          (SELECT COUNT(*) FROM runtime_operation_transitions WHERE operation_id=o.operation_id)
         FROM runtime_operations o JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
         WHERE o.operation_id=?1", [op_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
    ).unwrap();
    assert_eq!(
        (
            phase.as_str(),
            effect_phase.as_str(),
            claim_count,
            transitions
        ),
        ("completed", "finalized", 0, 3)
    );
    drop(db);

    let reopened = reopened_runtime(&temp, provider.clone()).await;
    let summary = reopened.recover_startup().await.unwrap();
    assert!(
        summary
            .notes
            .iter()
            .any(|note| note.contains("policy reattached with durable settings")),
        "{summary:?}"
    );
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap().status,
        "ready"
    );
    assert_eq!(provider.preferences_mutation_calls(), native_calls + 1);
}

#[tokio::test]
async fn reviewed_not_applied_policy_consumes_original_key_without_native_dispatch() {
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
    let (status, reviewed) = call(&router, &token, "POST", &path, Some(input.clone()), None).await;
    assert_eq!(status, StatusCode::OK, "{reviewed:?}");
    assert_eq!(reviewed["resolution"], "not_applied");
    assert_eq!(reviewed["agent_revision"], revision);
    let (status, again) = call(&router, &token, "POST", &path, Some(input.clone()), None).await;
    assert_eq!(status, StatusCode::OK, "{again:?}");
    assert_eq!(again, reviewed);
    let (status, original) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/preferences"),
        Some(preference_body(revision, "high")),
        Some("review-not-applied-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{original:?}");
    assert_eq!(original["error"]["code"], "operator_verified_not_applied");
    assert_eq!(original["error"]["details"]["operation_id"], op_id);
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
    assert_eq!(
        runtime
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        revision
    );
    assert_eq!(
        runtime.get_session(&agent_id).await.unwrap().status,
        "policy_recovery_required"
    );
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

    let reopened = reopened_runtime(&temp, provider.clone()).await;
    reopened.recover_startup().await.unwrap();
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap().status,
        "ready"
    );
    assert_eq!(
        reopened
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        revision
    );
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
}

#[tokio::test]
async fn native_policy_review_rejects_wrong_attachment_and_rolls_back_failed_receipt() {
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
    let (status, _failed) = call(&router, &token, "POST", &path, Some(input.clone()), None).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let (phase,claim_count,session_status):(String,i64,String)=db.query_row(
        "SELECT o.phase,
         (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=o.operation_id),
         (SELECT s.status FROM sessions s WHERE s.id=?2)
         FROM runtime_operations o WHERE o.operation_id=?1",
        rusqlite::params![op_id,agent_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    ).unwrap();
    assert_eq!(
        (phase.as_str(), claim_count, session_status.as_str()),
        ("manual_review", 1, "ready")
    );
    db.execute_batch("DROP TRIGGER deny_native_policy_review")
        .unwrap();
    let (status, verified) = call(&router, &token, "POST", &path, Some(input.clone()), None).await;
    assert_eq!(status, StatusCode::OK, "{verified:?}");
    let mut altered = input.clone();
    altered["evidence"] =
        serde_json::json!("Another different independently checked provider report");
    let (status, rejected) = call(&router, &token, "POST", &path, Some(altered), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected:?}");
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
async fn operator_can_confirm_applied_claude_permission_after_lost_native_receipt() {
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
    let (status, confirmed) = call(&router, &token, "POST", &path, Some(input), None).await;
    assert_eq!(status, StatusCode::OK, "{confirmed:?}");
    assert_eq!(confirmed["agent_revision"], revision + 1);
    assert_eq!(confirmed["restart_required"], true);
    let policy = runtime.get_active_workspace_agent_by_id(&agent_id).unwrap();
    assert_eq!(
        policy.recreation_policy.permission_intent,
        runtime_core::ProviderPermissionIntent::InheritProviderConfiguration
    );
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(permission_body(revision)),
        Some("claude-manual-review-applied"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay["revision"], revision + 1);
    assert_eq!(provider.permission_mutation_calls(), old_calls + 1);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let db_phase: String = db
        .query_row(
            "SELECT phase FROM runtime_operations WHERE operation_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(db_phase, "completed");
}

#[tokio::test]
async fn reviewed_policy_recovery_refuses_provider_identity_drift_until_exact_reattachment() {
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
    let (status, resolved) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/native-policy/{op_id}/resolve"),
        Some(body),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resolved:?}");
    let native_calls = provider.preferences_mutation_calls();

    let mut registry = runtime_core::ProviderRegistry::new();
    registry
        .register(Arc::new(ChangedNativePolicyAttachmentProvider))
        .unwrap();
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
    let summary = changed.recover_startup().await.unwrap();
    assert!(
        summary
            .notes
            .iter()
            .any(|note| note.contains("still fenced")),
        "{summary:?}"
    );
    let withheld = changed.get_session(&agent_id).await.unwrap();
    assert_eq!(withheld.status, "policy_recovery_required");
    assert_eq!(
        withheld.failure_code.as_deref(),
        Some("startup_policy_identity_changed")
    );
    assert!(!withheld
        .provider_session_ref
        .as_deref()
        .unwrap()
        .contains("unexpected-new"));

    let reopened = reopened_runtime(&temp, provider.clone()).await;
    let recovered = reopened.recover_startup().await.unwrap();
    assert!(
        recovered
            .notes
            .iter()
            .any(|note| note.contains("policy reattached with durable settings")),
        "{recovered:?}"
    );
    assert_eq!(
        reopened.get_session(&agent_id).await.unwrap().status,
        "ready"
    );
    assert_eq!(provider.preferences_mutation_calls(), native_calls);
}
