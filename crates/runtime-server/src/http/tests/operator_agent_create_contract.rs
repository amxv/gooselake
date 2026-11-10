use super::operator_agent_create::{create_http, fixture, reopened_runtime};
use super::workspace_worktree_routes::call;
use super::*;

#[tokio::test]
async fn concurrent_same_key_agent_creation_never_launches_a_second_session() {
    let (router, token, _temp, provider, runtime, workspace) = fixture().await;
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    let baseline = provider.create_call_count();
    let (a, b) = tokio::join!(
        create_http(
            &router,
            &token,
            &workspace,
            input.clone(),
            Some("same-key-race")
        ),
        create_http(
            &router,
            &token,
            &workspace,
            input.clone(),
            Some("same-key-race")
        ),
    );
    assert!(
        matches!(a.0, StatusCode::OK | StatusCode::CONFLICT),
        "{a:?}"
    );
    assert!(
        matches!(b.0, StatusCode::OK | StatusCode::CONFLICT),
        "{b:?}"
    );
    assert!(
        a.0 == StatusCode::OK || b.0 == StatusCode::OK,
        "at least one concurrent caller must get the canonical completion"
    );
    let (status, exact, headers) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("same-key-race"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{exact:?}");
    let op_id = headers.get("x-operation-id").unwrap().to_str().unwrap();
    assert!(op_id.starts_with("op_agent_create_"));
    for response in [&a, &b] {
        if response.0 == StatusCode::OK {
            assert_eq!(response.1, exact);
        } else {
            assert_eq!(response.1["error"]["code"], "outcome_unresolved");
            assert_eq!(response.1["error"]["details"]["operation_id"], op_id);
        }
    }
    assert_eq!(
        runtime
            .list_workspace_agents(&workspace, None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(provider.create_call_count(), baseline + 1);

    let (status, foreign, _) = create_http(
        &router,
        &token,
        "another-workspace",
        input,
        Some("same-key-race"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{foreign:?}");
    assert_eq!(provider.create_call_count(), baseline + 1);
}

#[tokio::test]
async fn native_create_auth_strict_inputs_and_review_guards_precede_side_effects() {
    let (router, token, _temp, provider, runtime, workspace) = fixture().await;
    let uri = format!("/v2/workspaces/{workspace}/agents");
    let without_auth = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"provider":"codex"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(without_auth.status(), StatusCode::UNAUTHORIZED);
    let unexpected = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"provider":"codex","unknown_native_option":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unexpected.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let invalid_key = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "invalid,combined")
                .body(Body::from(r#"{"provider":"codex","model":"test-model"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_key.status(), StatusCode::BAD_REQUEST);
    assert_eq!(provider.create_call_count(), 0);
    assert!(runtime
        .list_workspace_agents(&workspace, None)
        .unwrap()
        .is_empty());

    provider.set_create_failure_mode(4);
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    let (status, unknown, _) = create_http(
        &router,
        &token,
        &workspace,
        input,
        Some("strict-review-gate"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let reserved = {
        // The original ID exists in operation evidence, not in a fabricated
        // agent row. This scoped endpoint must reveal only its exact target.
        let (status, details) = call(
            &router,
            &token,
            "GET",
            &format!("/v2/operations/{op}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{details:?}");
        details["effects"][0]["target_id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let review_uri = format!("/v2/workspaces/{workspace}/agents/creations/{op}/resolve");
    let evidence = serde_json::json!({
        "resolution":"not_created","provider":"codex",
        "reserved_agent_id":reserved,
        "evidence":"Independently verified provider has no native session under reserved identity",
    });
    let forbidden_precondition = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&review_uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, "\"0\"")
                .body(Body::from(evidence.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden_precondition.status(), StatusCode::BAD_REQUEST);
    let forbidden_key = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&review_uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "review-should-not-be-keyed")
                .body(Body::from(evidence.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden_key.status(), StatusCode::BAD_REQUEST);
    let mut extra = evidence.clone();
    extra["force"] = serde_json::json!(true);
    let (status, malformed) = call(&router, &token, "POST", &review_uri, Some(extra), None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{malformed:?}");
    let mut wrong_ref = evidence.clone();
    wrong_ref["reserved_agent_id"] = serde_json::json!("not-original-reservation");
    let (status, wrong) = call(&router, &token, "POST", &review_uri, Some(wrong_ref), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{wrong:?}");
    assert_eq!(provider.create_call_count(), 1);
    assert!(runtime
        .list_workspace_agents(&workspace, None)
        .unwrap()
        .is_empty());
    let (status, valid) = call(&router, &token, "POST", &review_uri, Some(evidence), None).await;
    assert_eq!(status, StatusCode::OK, "{valid:?}");
    assert_eq!(valid["resolution"], "not_created");
}

#[tokio::test]
async fn independently_verified_created_native_attachment_rejects_drift_then_reattaches() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    provider.set_create_failure_mode(2);
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    let baseline = provider.create_call_count();
    let (status, unknown, _) = create_http(
        &router,
        &token,
        &workspace,
        input,
        Some("native-create-drift"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown:?}");
    let op = unknown["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let (status, details) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{op}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let reserved = details["effects"][0]["target_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let review = serde_json::json!({
        "resolution":"created","provider":"codex","reserved_agent_id":reserved,
        "observed_provider_session_ref":format!("test-thread-{reserved}"),
        "evidence":"Independently observed the original native session after provider acknowledged nothing",
    });
    let path = format!("/v2/workspaces/{workspace}/agents/creations/{op}/resolve");
    let (status, verified) = call(&router, &token, "POST", &path, Some(review), None).await;
    assert_eq!(status, StatusCode::OK, "{verified:?}");
    assert_eq!(
        runtime.get_session(&reserved).await.unwrap().status,
        "create_recovery_required"
    );
    provider.set_restore_failure_mode(3);
    let changed = reopened_runtime(&temp, provider.clone()).await;
    let first_recovery = changed.recover_startup().await.unwrap();
    assert!(
        first_recovery
            .notes
            .iter()
            .any(|note| note.contains("still fenced")),
        "{first_recovery:?}"
    );
    let still_blocked = changed.get_session(&reserved).await.unwrap();
    assert_eq!(still_blocked.status, "create_recovery_required");
    assert_eq!(
        still_blocked.failure_code.as_deref(),
        Some("startup_create_identity_changed")
    );
    assert_eq!(provider.create_call_count(), baseline + 1);
    provider.set_restore_failure_mode(0);
    let correct = reopened_runtime(&temp, provider.clone()).await;
    let recovery = correct.recover_startup().await.unwrap();
    assert!(
        recovery
            .notes
            .iter()
            .any(|note| note.contains("create reattached with durable settings")),
        "{recovery:?}"
    );
    assert_eq!(
        correct.get_session(&reserved).await.unwrap().status,
        "ready"
    );
    assert_eq!(provider.create_call_count(), baseline + 1);
}

#[tokio::test]
async fn missing_caller_key_is_a_one_shot_journal_not_implicit_replay() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    let (status, first, first_headers) =
        create_http(&router, &token, &workspace, input.clone(), None).await;
    assert_eq!(status, StatusCode::OK, "{first:?}");
    let (status, second, second_headers) =
        create_http(&router, &token, &workspace, input, None).await;
    assert_eq!(status, StatusCode::OK, "{second:?}");
    assert_ne!(first["agent_id"], second["agent_id"]);
    assert_ne!(
        first_headers.get("x-operation-id"),
        second_headers.get("x-operation-id")
    );
    assert_eq!(first_headers.get(header::ETAG).unwrap(), "\"0\"");
    assert_eq!(second_headers.get(header::ETAG).unwrap(), "\"0\"");
    assert_eq!(provider.create_call_count(), 2);
    assert_eq!(
        runtime
            .list_workspace_agents(&workspace, None)
            .unwrap()
            .len(),
        2
    );
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE kind='operator_agent_create'
         AND idempotency_key LIKE 'server-agent-create-%' AND phase='completed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn provider_reply_with_wrong_reserved_runtime_id_is_quarantined_without_roster_insertion() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    provider.set_create_failure_mode(5);
    let input = serde_json::json!({"provider":"codex","model":"test-model"});
    let baseline = provider.create_call_count();
    let (status, failure, _) = create_http(
        &router,
        &token,
        &workspace,
        input.clone(),
        Some("invalid-runtime-identity"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{failure:?}");
    assert_eq!(failure["error"]["code"], "outcome_unresolved");
    assert_eq!(provider.create_call_count(), baseline + 1);
    assert!(runtime
        .list_workspace_agents(&workspace, None)
        .unwrap()
        .is_empty());
    let op = failure["error"]["details"]["operation_id"]
        .as_str()
        .unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let detail = store.repository().get_operation(op).unwrap().unwrap();
    assert_eq!(
        detail.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(detail.claims.len(), 1);
    assert_eq!(detail.effects[0].phase, "uncertain");
    let (status, retry, _) = create_http(
        &router,
        &token,
        &workspace,
        input,
        Some("invalid-runtime-identity"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{retry:?}");
    assert_eq!(retry["error"]["details"]["operation_id"], op);
    assert_eq!(provider.create_call_count(), baseline + 1);
}

#[tokio::test]
async fn preprovider_create_reservation_blocks_workspace_retirement_across_sqlite_instances() {
    let (router, token, temp, provider, runtime, workspace) = fixture().await;
    let raw = serde_json::json!({"provider":"codex","model":"test-model"});
    let typed: runtime_core::WorkspaceAgentCreateRequest =
        serde_json::from_value(raw.clone()).unwrap();
    let request = serde_json::json!({
        "workspace_id":workspace,"action":"create","request":typed,
    });
    let command = runtime_core::OperatorAgentCreateCommand {
        operation_id: "op_agent_create_retirement_race".into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: "prevent-retirement-between-intent-and-provider".into(),
        workspace_id: workspace.clone(),
        reserved_agent_id: "sess_codex_preprovider_retirement_guard".into(),
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: 100,
        worktree_operation_id: None,
    };
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(matches!(
        store
            .repository()
            .admit_operator_agent_create(&command)
            .unwrap(),
        runtime_core::OperatorAgentCreateAdmission::Execute { .. }
    ));
    assert_eq!(provider.create_call_count(), 0);
    let second_store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let preflight = second_store
        .repository()
        .workspace_retirement_preflight(&workspace)
        .unwrap();
    assert!(preflight.blockers.iter().any(|blocker| blocker.code=="operations_unresolved"),
        "retirement must discover the parent agent create intent even with no agent/session row: {preflight:?}");
    let (status, denied) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace}/retire"),
        Some(serde_json::json!({"expected_revision":0})),
        Some("retire-while-agent-create-is-pending"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{denied:?}");
    assert_eq!(provider.create_call_count(), 0);
    assert!(runtime
        .list_workspace_agents(&workspace, None)
        .unwrap()
        .is_empty());

    store
        .repository()
        .mark_operator_agent_create_uncertain(
            &command.operation_id,
            "native create dispatch unknown after crash",
            200,
        )
        .unwrap();
    let uri = format!(
        "/v2/workspaces/{workspace}/agents/creations/{}/resolve",
        command.operation_id
    );
    let independent = serde_json::json!({
        "resolution":"not_created",
        "reserved_agent_id":command.reserved_agent_id,
        "provider":"codex",
        "evidence":"Independent provider inspection confirmed no session ever launched",
    });
    let (status, reviewed) = call(&router, &token, "POST", &uri, Some(independent), None).await;
    assert_eq!(status, StatusCode::OK, "{reviewed:?}");
    let preflight = second_store
        .repository()
        .workspace_retirement_preflight(&workspace)
        .unwrap();
    assert!(!preflight
        .blockers
        .iter()
        .any(|blocker| blocker.code == "operations_unresolved"));
    assert_eq!(provider.create_call_count(), 0);
}
