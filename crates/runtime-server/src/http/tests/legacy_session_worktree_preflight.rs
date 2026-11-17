use super::workspace_retirement_support::*;
use super::*;

#[tokio::test]
async fn legacy_direct_create_refuses_managed_checkout_paths_without_decoding_foreign_history() {
    let temp = tempfile::tempdir().unwrap();
    let active_root = temp.path().join("active-workspace");
    let retired_root = temp.path().join("retired-workspace");
    let unrelated_root = temp.path().join("unrelated-workspace");
    let active_checkout = temp.path().join("managed-checkout-active");
    let retired_checkout = temp.path().join("managed-checkout-retired");
    for path in [
        &active_root,
        &retired_root,
        &unrelated_root,
        &active_checkout,
        &retired_checkout,
    ] {
        fs::create_dir_all(path).unwrap();
    }

    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let active = fixture.register(&active_root).await;
    let retired = fixture.register(&retired_root).await;
    let foreign = fixture.register(&unrelated_root).await;
    let foreign_agent = fixture
        .agent(&foreign.workspace_id, ProviderKind::Codex)
        .await;
    for (id, root, cwd) in [
        ("active-worktree", &active_root, &active_checkout),
        ("retired-worktree", &retired_root, &retired_checkout),
    ] {
        fixture
            .store
            .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
                id: id.into(),
                repo_root: root.to_string_lossy().into(),
                worktree_root: cwd.to_string_lossy().into(),
                worktree_cwd: cwd.to_string_lossy().into(),
                branch_name: format!("fixture/{id}"),
                worktree_name: id.into(),
                unified_workspace_path: cwd.to_string_lossy().into(),
                deletion_policy: "retain_on_last_claim".into(),
                created_by_session_id: None,
                created_by_operation_id: None,
                created_at: 100,
                updated_at: 100,
            })
            .unwrap();
    }
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspaces SET lifecycle_state='retired' WHERE workspace_id=?1",
        [&retired.workspace_id],
    )
    .unwrap();
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid-json' WHERE id=?1",
        [&foreign_agent.agent_id],
    )
    .unwrap();
    assert!(fixture.store.hydrate_runtime_state().is_err());
    let native_before = provider.native_session_count().await;

    let events_before: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();
    // Both managed checkout identities remain inspectable history; neither
    // can authorize the unjournaled direct native creation entry point.
    for cwd in [&active_checkout, &retired_checkout] {
        let error = fixture
            .runtime
            .create_session(
                serde_json::from_value(json!({
                    "provider":"codex", "cwd":cwd, "permission_mode":null, "metadata":{},
                }))
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(&error, RuntimeError::Unsupported(_)));
        let response = ApiError::from(error).into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(provider.native_session_count().await, native_before);
    }
    let events_after: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events_after, events_before);
    assert!(fixture.store.hydrate_runtime_state().is_err());
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM sessions WHERE id<>?1",
            [foreign_agent.agent_id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        active.lifecycle_state,
        runtime_core::WorkspaceLifecycleState::Active
    );
}
