use super::workspace_retirement_support::*;
use super::*;

#[tokio::test]
async fn legacy_operator_worktree_claim_ignores_unrelated_corrupt_history_without_double_claim() {
    let temp = tempfile::tempdir().unwrap();
    let owner_root = temp.path().join("owner-workspace");
    let foreign_root = temp.path().join("foreign-workspace");
    fs::create_dir_all(&owner_root).unwrap();
    fs::create_dir_all(&foreign_root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let owner = fixture.register(&owner_root).await;
    let foreign = fixture.register(&foreign_root).await;
    let owner_agent = fixture
        .agent(&owner.workspace_id, ProviderKind::Codex)
        .await;
    let foreign_agent = fixture
        .agent(&foreign.workspace_id, ProviderKind::Codex)
        .await;

    for id in ["wt_owned", "wt_other", "wt_hidden"] {
        let cwd = if id == "wt_hidden" {
            format!("__gg_tombstoned__/{id}")
        } else {
            temp.path().join(id).display().to_string()
        };
        fixture
            .store
            .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
                id: id.into(),
                repo_root: owner_root.display().to_string(),
                worktree_root: temp.path().display().to_string(),
                worktree_cwd: cwd.clone(),
                branch_name: format!("fixture/{id}"),
                worktree_name: id.into(),
                unified_workspace_path: cwd,
                deletion_policy: "retain_on_last_claim".into(),
                created_by_session_id: None,
                created_by_operation_id: None,
                created_at: 100,
                updated_at: 100,
            })
            .unwrap();
    }

    let (lookup, _) = fixture
        .call("GET", "/v1/worktrees/wt_owned", None, None)
        .await;
    assert_eq!(lookup, StatusCode::OK);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid-json' WHERE id=?1",
        [&foreign_agent.agent_id],
    )
    .unwrap();
    assert!(fixture.store.hydrate_runtime_state().is_err());
    let native_count = provider.native_session_count().await;
    let request = serde_json::json!({"session_id":owner_agent.agent_id, "claim_role":"owner"});

    let (claim_status, claimed) = fixture
        .call(
            "POST",
            "/v1/worktrees/wt_owned/claims",
            Some(request.clone()),
            None,
        )
        .await;
    assert_eq!(claim_status, StatusCode::OK, "{claimed}");
    assert_eq!(claimed["worktree"]["id"], "wt_owned");
    assert_eq!(claimed["claim"]["session_id"], owner_agent.agent_id);
    assert_eq!(claimed["claim"]["released_at"], serde_json::Value::Null);

    let (repeat_status, repeated) = fixture
        .call(
            "POST",
            "/v1/worktrees/wt_owned/claims",
            Some(request.clone()),
            None,
        )
        .await;
    assert_eq!(repeat_status, StatusCode::OK, "{repeated}");
    assert_eq!(
        repeated, claimed,
        "exact retry must preserve claim and timestamp"
    );

    let (conflict_status, conflict) = fixture
        .call(
            "POST",
            "/v1/worktrees/wt_other/claims",
            Some(request.clone()),
            None,
        )
        .await;
    assert_eq!(conflict_status, StatusCode::BAD_REQUEST, "{conflict}");
    assert!(
        conflict["error"]
            .as_str()
            .unwrap_or_default()
            .contains("already has an active claim"),
        "{conflict}"
    );
    let (hidden_status, _) = fixture
        .call(
            "POST",
            "/v1/worktrees/wt_hidden/claims",
            Some(request),
            None,
        )
        .await;
    assert_eq!(hidden_status, StatusCode::NOT_FOUND);

    let claim_count: i64 = db
        .query_row("SELECT COUNT(*) FROM managed_worktree_claims", [], |row| {
            row.get(0)
        })
        .unwrap();
    let event_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_events WHERE kind='worktree.claimed' AND scope_id='wt_owned'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!((claim_count, event_count), (1, 1));
    assert_eq!(provider.native_session_count().await, native_count);
    assert!(fixture.store.hydrate_runtime_state().is_err());

    let unauthorized = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/worktrees/wt_owned/claims")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn legacy_public_worktree_claim_rejects_internal_rebind_reservations_and_invalid_roles() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("public-claim-workspace");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    fixture
        .store
        .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
            id: "wt_public_claim_role".into(),
            repo_root: root.display().to_string(),
            worktree_root: temp.path().join("worktrees").display().to_string(),
            worktree_cwd: temp.path().join("worktrees/public").display().to_string(),
            branch_name: "gg/public".into(),
            worktree_name: "public".into(),
            unified_workspace_path: "public_claim_fixture".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 100,
            updated_at: 100,
        })
        .unwrap();
    let native_before = provider.native_session_count().await;
    let uri = "/v1/worktrees/wt_public_claim_role/claims";

    // The only producer of rebind_reservation must be the native provider
    // rebind admission transaction. A public claim with that role cannot be
    // released via the ordinary API and would strand the managed checkout.
    for role in [
        "rebind_reservation",
        " ReBiNd_ReSeRvAtIoN ",
        "unknown_role",
        "   ",
    ] {
        let (status, denied) = fixture
            .call(
                "POST",
                uri,
                Some(serde_json::json!({"session_id":agent.agent_id,"claim_role":role})),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "role {role:?}: {denied}");
        assert!(
            denied["error"]
                .as_str()
                .unwrap_or_default()
                .contains("claim_role"),
            "role {role:?}: {denied}"
        );
    }
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let counts = || {
        (
            db.query_row("SELECT COUNT(*) FROM managed_worktree_claims WHERE worktree_id='wt_public_claim_role'", [], |row| row.get::<_,i64>(0)).unwrap(),
            db.query_row("SELECT COUNT(*) FROM runtime_events WHERE scope='worktree' AND scope_id='wt_public_claim_role' AND kind LIKE 'worktree.%'", [], |row| row.get::<_,i64>(0)).unwrap(),
        )
    };
    assert_eq!(
        counts(),
        (0, 0),
        "invalid role must not mutate the claim or journal"
    );

    let (status, owner) = fixture
        .call(
            "POST",
            uri,
            Some(serde_json::json!({"session_id":agent.agent_id,"claim_role":" OwNeR "})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{owner}");
    assert_eq!(owner["claim"]["claim_role"], "owner");
    let (status, released) = fixture
        .call(
            "POST",
            "/v1/worktrees/wt_public_claim_role/release",
            Some(serde_json::json!({"session_id":agent.agent_id,"cleanup_if_last_claim":false})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{released}");
    let (status, consumer) = fixture
        .call(
            "POST",
            uri,
            Some(serde_json::json!({"session_id":agent.agent_id,"claim_role":"consumer"})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{consumer}");
    assert_eq!(consumer["claim"]["claim_role"], "consumer");
    assert_eq!(
        counts(),
        (1, 3),
        "only valid claim/release/reclaim append Critical events"
    );
    assert_eq!(provider.native_session_count().await, native_before);
}
