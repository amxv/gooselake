use super::workspace_retirement_support::*;
use super::*;

#[tokio::test]
async fn legacy_native_create_preflight_uses_worktree_paths_without_unrelated_session_hydration() {
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

    // A legacy session created inside a managed checkout still guards its
    // owning workspace, but foreign corrupt sessions cannot mask the lookup.
    let (status, response) = fixture
        .call(
            "POST",
            "/v1/sessions",
            Some(serde_json::json!({
                "provider":"codex",
                "cwd":active_checkout,
                "permission_mode":null,
                "metadata":{}
            })),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(provider.native_session_count().await, native_before + 1);
    let session_id = response["id"].as_str().expect("legacy session ID");
    assert_eq!(
        fixture.runtime.get_session(session_id).await.unwrap().cwd,
        Some(active_checkout.to_string_lossy().into())
    );

    // The retired workspace must remain fenced even though its checkout is
    // outside the registered workspace canonical root. No native retry.
    let (blocked, body) = fixture
        .call(
            "POST",
            "/v1/sessions",
            Some(serde_json::json!({
                "provider":"codex",
                "cwd":retired_checkout,
                "permission_mode":null,
                "metadata":{}
            })),
            None,
        )
        .await;
    assert_eq!(blocked, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        provider.native_session_count().await,
        native_before + 1,
        "retired checkout must fail before any further native session creation"
    );
    assert!(fixture.store.hydrate_runtime_state().is_err());
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM sessions WHERE id<>?1",
            [foreign_agent.agent_id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        active.lifecycle_state,
        runtime_core::WorkspaceLifecycleState::Active
    );
}
