use super::*;

/// Explicit disposable Git setup, separate from database-only history seeding.
/// This helper creates a real checkout but no provider/session/team history.
pub(super) fn prepare_historical_spawn_checkout(
    app: &Arc<RuntimeApp>,
    repo: &Path,
    team_id: &str,
    key: &str,
    mut input: Value,
) -> runtime_core::ManagedWorktreeRecord {
    input["team_id"] = serde_json::json!(team_id);
    input["idempotency_key"] = serde_json::json!(key);
    let request: runtime_core::TeamMemberSpawnRequest = serde_json::from_value(input).unwrap();
    let worktree = request
        .worktree
        .as_ref()
        .expect("explicit historical checkout");
    assert_eq!(worktree.mode.as_deref().unwrap_or("create"), "create");
    assert_eq!(
        worktree.run_init_script,
        Some(false),
        "fixture never executes an init hook"
    );
    let digest =
        runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap();
    let name = worktree.name.as_deref().unwrap();
    let branch = format!(
        "{}/{}",
        worktree.branch_prefix.as_deref().unwrap_or("gg"),
        name
    );
    let repo = std::fs::canonicalize(repo).unwrap();
    let root = Path::new(&app.worktree_settings.root_dir).join("historical-http");
    std::fs::create_dir_all(&root).unwrap();
    let cwd = root.join(name);
    super::workspace_worktree_routes::git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            &branch,
            cwd.to_str().unwrap(),
            worktree.base_ref.as_deref().unwrap_or("HEAD"),
        ],
    );
    runtime_core::ManagedWorktreeRecord {
        id: format!("wt_http_history_{digest}"),
        repo_root: repo.display().to_string(),
        worktree_root: root.display().to_string(),
        worktree_cwd: cwd.display().to_string(),
        branch_name: branch,
        worktree_name: name.into(),
        unified_workspace_path: "historical-http".into(),
        deletion_policy: "delete_on_last_claim".into(),
        created_by_session_id: Some(request.source_session_id),
        created_by_operation_id: Some(format!("op_spawn_http_history_{digest}")),
        created_at: 1000,
        updated_at: 1000,
    }
}
