use super::workspace_retirement_support::*;
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn operator_reconciles_unknown_native_close_after_restart_and_resumes_original_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("review-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    provider.fail_close.store(true, Ordering::SeqCst);
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let unknown = fixture
        .advance(
            &workspace.workspace_id,
            revision,
            "review-original-retirement",
        )
        .await;
    assert_eq!(
        unknown.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    let effect = unknown
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    assert_eq!(effect.phase, "manual_review");
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);

    // Independent provider control verifies the close. Runtime review must not
    // replay the unknown native request, including from a restarted manager.
    provider
        .externally_close_for_operator_review(&agent.agent_id)
        .await;
    let restarted_provider = RetirementProvider::new(ProviderKind::Codex);
    let restarted = retirement_fixture(temp.path(), &[restarted_provider.clone()]).await;
    restarted.runtime.recover_startup().await.unwrap();
    let session = restarted
        .store
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .into_iter()
        .find(|session| session.id == agent.agent_id)
        .unwrap();
    let current_agent = restarted
        .runtime
        .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
        .unwrap();
    let input = serde_json::json!({
        "effect_id":effect.effect_id,
        "evidence":"Operator verified the exact native session is closed using the provider control surface",
        "resolution":{"kind":"provider_closed","identity":{
            "agent_id":agent.agent_id,"provider":"codex",
            "provider_session_ref":session.provider_session_ref,
            "canonical_provider_session_ref":session.canonical_provider_session_ref,
            "agent_revision":current_agent.revision,"session_updated_at":session.updated_at,
            "active_turn_id":session.active_turn_id,
        }},
    });
    let uri = format!(
        "/v2/workspaces/{}/retirement/{}/reconcile",
        workspace.workspace_id, unknown.operation.operation_id
    );
    let scoped = runtime_core::mint_scoped_mcp_token(&restarted.token, &agent.agent_id);
    let (status, _) = super::workspace_worktree_routes::call(
        &restarted.router,
        &scoped,
        "POST",
        &uri,
        Some(input.clone()),
        Some("operator-review"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let response = restarted
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "operator-review")
                .body(Body::from(input.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let (status, _) = restarted
        .call("POST", &uri, Some(input.clone()), None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for (field, value) in [
        (
            "provider_session_ref",
            serde_json::json!("different-native"),
        ),
        (
            "canonical_provider_session_ref",
            serde_json::json!("different-canonical"),
        ),
        (
            "agent_revision",
            serde_json::json!(current_agent.revision + 1),
        ),
        (
            "session_updated_at",
            serde_json::json!(session.updated_at + 1),
        ),
        ("active_turn_id", serde_json::json!("different-turn")),
    ] {
        let mut mismatch = input.clone();
        mismatch["resolution"]["identity"][field] = value;
        let (status, body) = restarted
            .call(
                "POST",
                &uri,
                Some(mismatch),
                Some(&format!("mismatch-{field}")),
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
    }
    let mut unbounded = input.clone();
    unbounded["evidence"] = serde_json::json!("x".repeat(2049));
    let (status, _) = restarted
        .call("POST", &uri, Some(unbounded), Some("unbounded-evidence"))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        restarted
            .store
            .get_operation(&unknown.operation.operation_id)
            .unwrap()
            .unwrap(),
        unknown
    );

    let (status, receipt) = restarted
        .call("POST", &uri, Some(input.clone()), Some("operator-review"))
        .await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["operation_id"], unknown.operation.operation_id);
    assert_eq!(receipt["operation_phase"], "requested");
    assert_eq!(receipt["settled"], true);
    assert_eq!(
        restarted
            .runtime
            .get_session(&agent.agent_id)
            .await
            .unwrap()
            .status,
        "closed"
    );
    let completed = timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            let operation = restarted
                .store
                .get_operation(&unknown.operation.operation_id)
                .unwrap()
                .unwrap();
            if operation.operation.phase == runtime_core::OperationPhase::Completed {
                break operation;
            }
            assert_eq!(
                operation.operation.phase,
                runtime_core::OperationPhase::Requested,
                "{operation:?}"
            );
            tick.tick().await;
        }
    })
    .await
    .expect("review did not resume the exact original retirement worker");
    assert_eq!(
        completed.operation.operation_id,
        unknown.operation.operation_id
    );
    assert_eq!(
        completed.operation.idempotency_key,
        unknown.operation.idempotency_key
    );
    assert_eq!(
        completed.operation.normalized_request,
        unknown.operation.normalized_request
    );
    assert_eq!(completed.effects.len(), unknown.effects.len());
    let close = completed
        .effects
        .iter()
        .find(|effect| effect.effect_id == receipt["effect_id"].as_str().unwrap())
        .unwrap();
    assert_eq!(close.attempt_count, 1);
    assert_eq!(
        close.evidence.as_ref().unwrap()["proof_kind"],
        "operator_verified_closed"
    );
    assert_eq!(
        close.evidence.as_ref().unwrap()["prior_evidence"]["outcome"],
        "native_close_unresolved"
    );
    assert!(
        close.evidence.as_ref().unwrap()["prior_evidence"]["prior_evidence"]["started_at"]
            .is_number()
    );
    assert_eq!(
        restarted
            .workspace(&workspace.workspace_id)
            .await
            .lifecycle_state,
        runtime_core::WorkspaceLifecycleState::Retired
    );
    assert_eq!(restarted_provider.closes.load(Ordering::SeqCst), 0);
    assert_eq!(restarted_provider.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    let events = restarted
        .store
        .list_runtime_events(None, None, 512)
        .unwrap();
    let (status, replay) = restarted
        .call("POST", &uri, Some(input.clone()), Some("operator-review"))
        .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, receipt);
    assert_eq!(
        restarted
            .store
            .get_operation(&unknown.operation.operation_id)
            .unwrap()
            .unwrap(),
        completed
    );
    assert_eq!(
        restarted
            .store
            .list_runtime_events(None, None, 512)
            .unwrap(),
        events
    );
    let mut conflict = input;
    conflict["evidence"] =
        serde_json::json!("Different evidence cannot overwrite the original receipt");
    let (status, _) = restarted
        .call("POST", &uri, Some(conflict), Some("operator-review"))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
}
