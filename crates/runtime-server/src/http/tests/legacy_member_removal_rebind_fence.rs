use super::workspace_worktree_routes::{call, fixture, fixture_with_runtime};
use super::*;

#[tokio::test]
async fn legacy_member_removal_does_not_unreserve_unresolved_native_provider_rebind() {
    let (_initial_router, token, temp, provider, workspace, agent, linked, _initial_runtime) =
        fixture_with_runtime().await;
    let history = build_history_records(vec![json!({
        "provider":"codex", "model":"test-model", "cwd":workspace.canonical_root,
    })]);
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    provider.seed_historical_session(&history[0]).await;
    persist_historical_session(store.as_ref(), &history[0]);
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    // All work below starts on readers opened after the historical lead was
    // persisted, before any rebind claim or team-removal side effect.
    let (router, _runtime, _app) = assemble_test_router(
        temp.path(),
        store,
        Arc::new(registry),
        &token,
        TeamMcpPolicy::default(),
    )
    .await;
    let lead = historical_session_json(&history[0]);
    let (status, team) = call(
        &router,
        &token,
        "POST",
        "/v1/teams",
        Some(serde_json::json!({
            "name":"unresolved provider rebind fence",
            "lead_agent_id": lead["id"],
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{team}");
    let team_id = team["team"]["id"].as_str().unwrap();
    let (status, joined) = call(
        &router,
        &token,
        "POST",
        &format!("/v1/teams/{team_id}/members"),
        Some(serde_json::json!({
            "agent_id":agent.agent_id,"title":"rebinding worker",
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{joined}");

    provider.set_rebind_mode(Some("unknown")).await;
    let (status, failed) = call(
        &router,
        &token,
        "POST",
        &format!(
            "/v2/workspaces/{}/agents/{}/worktree",
            workspace.workspace_id, agent.agent_id
        ),
        Some(serde_json::json!({"worktree_id":"wt_linked","expected_revision":0})),
        Some("team-remove-must-preserve-unknown-route"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{failed}");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let operation = store
        .get_workspace_agent_rebind_by_key(
            &workspace.workspace_id,
            &agent.agent_id,
            "team-remove-must-preserve-unknown-route",
        )
        .unwrap()
        .unwrap();
    assert_eq!(operation.phase, "manual_review");
    assert!(store
        .unresolved_workspace_agent_rebind(&agent.agent_id)
        .unwrap());
    let reservation = store
        .get_managed_worktree_claim("wt_linked", &agent.agent_id)
        .unwrap()
        .unwrap();
    assert_eq!(reservation.claim_role, "rebind_reservation");
    assert_eq!(reservation.released_at, None);

    // Legacy startup normalization also had a separate unjournaled upsert
    // writer. It must not replace or terminalize a real pending destination
    // reservation after the provider returned an unknown native outcome.
    assert!(
        matches!(
            store.upsert_managed_worktree_claim(&runtime_core::ManagedWorktreeClaimRecord {
                claim_role: "owner".into(),
                released_at: Some(reservation.created_at + 1),
                ..reservation.clone()
            }),
            Err(RuntimeError::Conflict(_)),
        ),
        "stale historical UPSERT must not rewrite an unresolved destination reservation"
    );
    assert_eq!(
        store
            .get_managed_worktree_claim("wt_linked", &agent.agent_id)
            .unwrap(),
        Some(reservation.clone()),
    );

    let before = store
        .list_runtime_events(
            Some((runtime_core::RuntimeEventScope::Worktree, "wt_linked")),
            None,
            100,
        )
        .unwrap();
    let native_before = std::process::Command::new("git")
        .arg("-C")
        .arg(&workspace.canonical_root)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .unwrap();
    assert!(native_before.status.success());
    let (status, removed) = call(
        &router,
        &token,
        "DELETE",
        &format!("/v1/teams/{team_id}/members/{}", agent.agent_id),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{removed}");
    assert!(removed["members"]
        .as_array()
        .unwrap()
        .iter()
        .all(|member| { member["agent_id"] != agent.agent_id }));

    assert_eq!(
        store
            .get_managed_worktree_claim("wt_linked", &agent.agent_id)
            .unwrap(),
        Some(reservation),
        "team-member removal must not release a reservation for an unverified provider rebind"
    );
    assert!(store
        .unresolved_workspace_agent_rebind(&agent.agent_id)
        .unwrap());
    assert_eq!(
        store
            .get_workspace_agent_rebind_by_key(
                &workspace.workspace_id,
                &agent.agent_id,
                "team-remove-must-preserve-unknown-route",
            )
            .unwrap()
            .unwrap()
            .phase,
        "manual_review"
    );
    assert_eq!(
        store
            .list_runtime_events(
                Some((runtime_core::RuntimeEventScope::Worktree, "wt_linked")),
                None,
                100
            )
            .unwrap(),
        before,
        "denied release must not append a misleading Critical worktree event"
    );
    let conn = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let failed_releases: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM team_operation_diagnostics
         WHERE team_id=?1 AND code='worktree_claim_release_failed'",
            [team_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        failed_releases, 1,
        "denied cleanup must be diagnosed for operator review"
    );
    assert_eq!(provider.rebind_calls().await, 1);
    assert!(Path::new(&linked).is_dir());
    let native_after = std::process::Command::new("git")
        .arg("-C")
        .arg(&workspace.canonical_root)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .unwrap();
    assert!(native_after.status.success());
    assert_eq!(native_after.stdout, native_before.stdout);
}

#[tokio::test]
async fn unresolved_root_rebind_fences_new_direct_claim_transaction_without_a_destination_reservation(
) {
    let (_, _, temp, provider, workspace, agent, _) = fixture().await;
    let db_path = temp.path().join("runtime.sqlite3");
    let db = rusqlite::Connection::open(&db_path).unwrap();
    // A root-directed native rebind has no destination managed-worktree
    // reservation. The persisted unresolved operation itself is its fence.
    assert_eq!(
        db.execute(
            "INSERT INTO workspace_agent_rebinds
             (operation_id,workspace_id,agent_id,phase,record_json,created_at,updated_at)
             VALUES ('op_unknown_root',?1,?2,'manual_review','{}',100,100)",
            rusqlite::params![workspace.workspace_id, agent.agent_id],
        )
        .unwrap(),
        1
    );
    let plan = db
        .prepare(
            "EXPLAIN QUERY PLAN SELECT EXISTS(
               SELECT 1 FROM workspace_agent_rebinds
               WHERE agent_id = ?1 AND phase IN ('intended','manual_review'))",
        )
        .unwrap()
        .query_map([&agent.agent_id], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        plan.contains("idx_workspace_agent_rebind_pending"),
        "{plan}"
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: db_path,
    });
    let claim = runtime_core::ManagedWorktreeClaimRecord {
        claim_generation: Some(1),
        worktree_id: "wt_linked".into(),
        session_id: agent.agent_id.clone(),
        claim_role: "consumer".into(),
        created_at: 1000,
        released_at: None,
    };
    // Startup's historical claim repair uses this unjournaled UPSERT rather
    // than the normal evented Claim transition. It must not bypass the
    // existing IMMEDIATE unresolved-provider-rebind authority fence.
    assert!(
        matches!(
            store.upsert_managed_worktree_claim(&claim),
            Err(RuntimeError::Conflict(_)),
        ),
        "legacy repair UPSERT must not admit a claim behind unresolved native rebind"
    );
    assert!(matches!(
        store.transition_managed_worktree_claim(
            &claim,
            runtime_core::ManagedWorktreeClaimTransition::Claim,
        ),
        Err(RuntimeError::Conflict(_)),
    ), "an unresolved root rebind must reject new direct worktree claims inside the SQLite write transaction");
    assert_eq!(
        store
            .get_managed_worktree_claim("wt_linked", &agent.agent_id)
            .unwrap(),
        None,
    );
    assert!(
        store
            .list_runtime_events(
                Some((runtime_core::RuntimeEventScope::Worktree, "wt_linked")),
                None,
                100
            )
            .unwrap()
            .is_empty(),
        "denied claim must not emit a fake Critical journal event"
    );
    assert_eq!(provider.rebind_calls().await, 0);
    assert_eq!(
        db.execute(
            "UPDATE workspace_agent_rebinds SET phase='intended'
             WHERE operation_id='op_unknown_root' AND phase='manual_review'",
            [],
        )
        .unwrap(),
        1
    );
    assert!(
        matches!(
            store.transition_managed_worktree_claim(
                &claim,
                runtime_core::ManagedWorktreeClaimTransition::Claim,
            ),
            Err(RuntimeError::Conflict(_)),
        ),
        "a newly intended rebind must retain the same atomic claim fence"
    );
    // The disposable fixture models a verified terminal rejection by updating
    // only its synthetic phase. The real handler terminalizes full authority
    // and releases reservations in one SQLite transaction.
    assert_eq!(
        db.execute(
            "UPDATE workspace_agent_rebinds SET phase='rejected'
             WHERE operation_id='op_unknown_root' AND phase='intended'",
            [],
        )
        .unwrap(),
        1
    );
    assert_eq!(
        store
            .transition_managed_worktree_claim(
                &claim,
                runtime_core::ManagedWorktreeClaimTransition::Claim,
            )
            .unwrap(),
        claim,
        "proven terminal rejection must not permanently block later claims"
    );
    let events = store
        .list_runtime_events(
            Some((runtime_core::RuntimeEventScope::Worktree, "wt_linked")),
            None,
            100,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "worktree.claimed");
}
