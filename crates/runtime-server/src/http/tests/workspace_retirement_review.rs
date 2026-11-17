use super::native_review_refusal::{
    assert_refusal as assert_prose_refusal, database_snapshot as review_snapshot,
};
use super::workspace_retirement_support::*;
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn retirement_prose_refusal_preserves_coordinate_despite_unrelated_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-review-target");
    let unrelated_root = temp.path().join("retirement-review-unrelated");
    fs::create_dir_all(&root).unwrap();
    fs::create_dir_all(&unrelated_root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let unrelated_workspace = fixture.register(&unrelated_root).await;
    let unrelated = fixture
        .agent(&unrelated_workspace.workspace_id, ProviderKind::Codex)
        .await;

    provider.fail_close.store(true, Ordering::SeqCst);
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let unknown = fixture
        .advance(
            &workspace.workspace_id,
            revision,
            "review-corrupt-other-session",
        )
        .await;
    assert_eq!(
        unknown.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    let effect = unknown
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    provider
        .externally_close_for_operator_review(&agent.agent_id)
        .await;
    let session = fixture
        .store
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .into_iter()
        .find(|session| session.id == agent.agent_id)
        .unwrap();
    let current_agent = fixture
        .runtime
        .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
        .unwrap();

    // The unrelated session belongs to a different valid workspace; it must
    // not poison a durable operator receipt after native closure is verified.
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid-json' WHERE id=?1",
        [&unrelated.agent_id],
    )
    .unwrap();
    assert!(fixture.store.hydrate_runtime_state().is_err());
    let input = serde_json::json!({
        "effect_id":effect.effect_id,
        "evidence":"Operator independently verified native closure for the exact reviewed attachment",
        "resolution":{"kind":"provider_closed","identity":{
            "agent_id":agent.agent_id,"provider":"codex",
            "provider_session_ref":session.provider_session_ref,
            "canonical_provider_session_ref":session.canonical_provider_session_ref,
            "agent_revision":current_agent.revision,
            "session_updated_at":session.updated_at,
            "active_turn_id":session.active_turn_id,
        }},
    });
    let uri = format!(
        "/v2/workspaces/{}/retirement/{}/reconcile",
        workspace.workspace_id, unknown.operation.operation_id
    );
    let before = review_snapshot(&db);
    for _ in 0..2 {
        let (status, response) = fixture
            .call(
                "POST",
                &uri,
                Some(input.clone()),
                Some("review-corrupt-history"),
            )
            .await;
        assert_prose_refusal(status, &response, &unknown.operation.operation_id);
        assert_eq!(review_snapshot(&db), before);
        assert_eq!(
            fixture
                .store
                .get_operation(&unknown.operation.operation_id)
                .unwrap()
                .unwrap(),
            unknown
        );
        assert_eq!(
            fixture.runtime.get_session(&agent.agent_id).await.unwrap(),
            session
        );
        assert_eq!(
            fixture
                .runtime
                .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
                .unwrap(),
            current_agent
        );
        assert!(fixture.store.hydrate_runtime_state().is_err());
    }
    let reviews: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM workspace_retirement_reviews WHERE operation_id=?1",
            [&unknown.operation.operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reviews, 0);
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    assert_eq!(provider.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(
        unknown
            .effects
            .iter()
            .find(|row| row.effect_id == effect.effect_id)
            .unwrap()
            .phase,
        "manual_review"
    );
}

#[tokio::test]
async fn retirement_prose_refusal_after_restart_keeps_original_unknown_owner() {
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

    // This external observation is not a structured receipt supplied to the review.
    // Restart cannot turn identity plus prose into settlement authority.
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

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let before = review_snapshot(&db);
    let events = restarted
        .store
        .list_runtime_events(None, None, 512)
        .unwrap();
    for (key, changed_prose) in [
        ("operator-review", false),
        ("operator-review", false),
        ("operator-review", true),
        ("different-operator-review", false),
    ] {
        let mut body = input.clone();
        if changed_prose {
            body["evidence"] = serde_json::json!(
                "Different evidence text still supplies no independent native receipt"
            );
        }
        let (status, response) = restarted.call("POST", &uri, Some(body), Some(key)).await;
        assert_prose_refusal(status, &response, &unknown.operation.operation_id);
        assert_eq!(review_snapshot(&db), before);
        assert_eq!(
            restarted
                .store
                .get_operation(&unknown.operation.operation_id)
                .unwrap()
                .unwrap(),
            unknown
        );
        assert_eq!(
            restarted
                .store
                .list_runtime_events(None, None, 512)
                .unwrap(),
            events
        );
        assert_eq!(
            restarted
                .runtime
                .get_session(&agent.agent_id)
                .await
                .unwrap(),
            session
        );
        assert_eq!(
            restarted
                .runtime
                .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
                .unwrap(),
            current_agent
        );
        assert_eq!(
            restarted
                .workspace(&workspace.workspace_id)
                .await
                .lifecycle_state,
            runtime_core::WorkspaceLifecycleState::Active
        );
    }
    let retained = restarted
        .store
        .get_operation(&unknown.operation.operation_id)
        .unwrap()
        .unwrap();
    let close = retained
        .effects
        .iter()
        .find(|row| row.effect_id == effect.effect_id)
        .unwrap();
    assert_eq!(close.phase, "manual_review");
    assert_eq!(close.attempt_count, 1);
    let evidence = close.evidence.as_ref().unwrap();
    assert_eq!(evidence["outcome"], "native_close_unresolved");
    assert!(evidence["prior_evidence"]["started_at"].is_number());
    assert!(evidence["operator_review"].is_null());
    assert!(!retained.claims.is_empty());
    let reviews: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM workspace_retirement_reviews WHERE operation_id=?1",
            [&unknown.operation.operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reviews, 0);
    assert_eq!(restarted_provider.closes.load(Ordering::SeqCst), 0);
    assert_eq!(restarted_provider.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    let again_provider = RetirementProvider::new(ProviderKind::Codex);
    let again = retirement_fixture(temp.path(), &[again_provider.clone()]).await;
    again.runtime.recover_startup().await.unwrap();
    assert_eq!(
        again
            .store
            .get_operation(&unknown.operation.operation_id)
            .unwrap()
            .unwrap(),
        unknown
    );
    assert_eq!(again_provider.closes.load(Ordering::SeqCst), 0);
    assert_eq!(again_provider.resumes.load(Ordering::SeqCst), 0);
}
