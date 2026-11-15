use super::workspace_retirement_support::*;
use super::*;

#[tokio::test]
async fn legacy_operator_worktree_list_ignores_unrelated_corrupt_history_and_keeps_ties() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("foreign-agent-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let foreign = fixture.register(&root).await;
    let agent = fixture
        .agent(&foreign.workspace_id, ProviderKind::Codex)
        .await;

    // Original hydrated list orders by (created_at,id) and then stably sorts
    // created_at, so a replacement projection must not reorder equal times.
    for (id, created_at, tombstone) in [
        ("wt_b", 100_i64, false),
        ("wt_late", 150, false),
        ("wt_hidden", 90, true),
        ("wt_a", 100, false),
    ] {
        let cwd = if tombstone {
            format!("__gg_tombstoned__/{id}")
        } else {
            temp.path().join(id).display().to_string()
        };
        fixture
            .store
            .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
                id: id.into(),
                repo_root: root.display().to_string(),
                worktree_root: temp.path().display().to_string(),
                worktree_cwd: cwd.clone(),
                branch_name: format!("fixture/{id}"),
                worktree_name: id.into(),
                unified_workspace_path: cwd,
                deletion_policy: "retain_on_last_claim".into(),
                created_by_session_id: None,
                created_by_operation_id: None,
                created_at,
                updated_at: created_at,
            })
            .unwrap();
    }

    let (before_status, before) = fixture.call("GET", "/v1/worktrees", None, None).await;
    assert_eq!(before_status, StatusCode::OK, "{before}");
    let names = before
        .as_array()
        .expect("legacy worktree array")
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, ["wt_a", "wt_b", "wt_late"]);

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid-json' WHERE id=?1",
        [&agent.agent_id],
    )
    .unwrap();
    assert!(fixture.store.hydrate_runtime_state().is_err());
    let native_count = provider.native_session_count().await;

    let (after_status, after) = fixture.call("GET", "/v1/worktrees", None, None).await;
    assert_eq!(after_status, StatusCode::OK, "{after}");
    assert_eq!(
        after, before,
        "historical foreign corruption must not affect exact list or tie order"
    );
    assert_eq!(provider.native_session_count().await, native_count);
    assert!(fixture.store.hydrate_runtime_state().is_err());
    let auth_failure = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/worktrees")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(auth_failure.status(), StatusCode::UNAUTHORIZED);
}
