use super::operator_agent_create::create_http;
use super::workspace_worktree_routes::{call, seeded_repo};
use super::*;

/// Keep the actual ACP request, immutable launch authority and checkout
/// identity oracles together. The two callers cover New and Existing; neither
/// route is established by synthetic saved session history.
pub(super) async fn assert_canonical_acp_worktree_route(mode: &str) {
    let (router, token, temp, acp, _claude, runtime, app) =
        build_mixed_provider_test_router_with_services().await;
    let repo = temp.path().join("canonical-acp-repository");
    seeded_repo(&repo);
    let (status, registered) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":repo})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{registered}");
    let registered: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_value(registered).unwrap();
    let workspace = &registered.workspace;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let git_operations = || -> i64 {
        db.query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_worktree_create'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    let native_inventory = || {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&workspace.canonical_root)
            .args(["worktree", "list", "--porcelain"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    };

    // Capture the COMPLETE prepared record and revision before agent create.
    // A result merely self-consistent with a substituted route must not pass.
    let original = if mode == "existing" {
        let (status, prepared) = call(
            &router,
            &token,
            "POST",
            &format!("/v2/workspaces/{}/worktrees", workspace.workspace_id),
            Some(serde_json::json!({"worktree_name":"acp-existing", "run_init_script":false})),
            Some("canonical-acp-existing-checkout"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{prepared}");
        let prepared: runtime_core::WorktreeCreateResponse =
            serde_json::from_value(prepared).unwrap();
        assert!(prepared.created);
        let revision = app
            .services
            .store
            .managed_worktree_revision(&prepared.worktree.id)
            .unwrap();
        Some((prepared.worktree, revision))
    } else {
        assert_eq!(mode, "new");
        None
    };
    let route = match &original {
        Some((record, _)) => serde_json::json!({"mode":"existing","worktree_id":record.id}),
        None => serde_json::json!({"mode":"new","worktree_name":"acp-new","run_init_script":false}),
    };
    let previous_git = git_operations();
    let inventory_before = native_inventory();
    let previous_creates = acp.created_sessions().await.len();
    let input =
        serde_json::json!({"provider":"acp", "title":"Canonical ACP route", "worktree":route});
    let key = format!("canonical-acp-{mode}");
    let (status, created, headers) = create_http(
        &router,
        &token,
        &workspace.workspace_id,
        input.clone(),
        Some(&key),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let operation_id = headers.get("x-operation-id").unwrap().to_str().unwrap();
    let agent: runtime_core::WorkspaceAgentRecord =
        serde_json::from_value(created.clone()).unwrap();
    assert_eq!(agent.recreation_policy.provider, ProviderKind::Acp);
    assert_eq!(agent.workspace_id, workspace.workspace_id);
    let session = runtime.get_session(&agent.agent_id).await.unwrap();
    assert_eq!(session.provider, "acp");
    let worktree_id = session.worktree_id.as_deref().unwrap();
    let selected = app
        .services
        .store
        .get_managed_worktree(worktree_id)
        .unwrap()
        .unwrap();
    let selected_revision = app
        .services
        .store
        .managed_worktree_revision(worktree_id)
        .unwrap();
    assert_eq!(session.cwd.as_deref(), Some(selected.worktree_cwd.as_str()));
    assert_eq!(
        agent.recreation_policy.authoritative_cwd,
        selected.worktree_cwd
    );
    assert_ne!(selected.worktree_cwd, workspace.canonical_root);
    assert!(Path::new(&selected.worktree_cwd).is_dir());
    assert!(session
        .provider_session_ref
        .as_ref()
        .is_some_and(|value| !value.is_empty()));
    assert!(session.canonical_provider_session_ref.is_some());
    assert_eq!(session.provider_session_ref, agent.provider_session_ref);
    assert_eq!(
        session.canonical_provider_session_ref,
        agent.canonical_provider_session_ref
    );

    let captured = acp.created_sessions().await;
    assert_eq!(captured.len(), previous_creates + 1);
    let matching = captured
        .iter()
        .filter(|row| row.runtime_session_id == session.id)
        .collect::<Vec<_>>();
    assert_eq!(
        matching.len(),
        1,
        "exactly one actual ACP launch for the admitted identity"
    );
    assert_eq!(
        matching[0].cwd.as_deref(),
        Some(selected.worktree_cwd.as_str())
    );

    // Canonical initial attachment owns its exact route even when the checkout
    // predates this agent. Legacy reused-consumer semantics must not leak here.
    let claim = app
        .services
        .store
        .get_managed_worktree_claim(worktree_id, &session.id)
        .unwrap()
        .unwrap();
    assert_eq!(claim.worktree_id, selected.id);
    assert_eq!(claim.session_id, session.id);
    assert_eq!(claim.claim_role, "owner");
    assert!(claim.released_at.is_none());
    assert_eq!(
        app.services
            .store
            .list_active_managed_worktree_claims_for_session(&session.id)
            .unwrap(),
        vec![claim.clone()],
        "exactly the selected owner claim is active"
    );
    let owner: String = db
        .query_row(
            "SELECT source_operation_id FROM workspace_session_ownership WHERE session_id=?1",
            [&session.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner, operation_id);

    // Compare the actual attachment and provider request with the immutable
    // launch snapshot committed before native create, not just the response.
    let (effect_phase, attempts, evidence): (String, i64, String) = db
        .query_row(
            "SELECT phase,attempt_count,evidence_json FROM runtime_operation_effects
         WHERE operation_id=?1 AND effect_kind='provider_session_create' AND target_id=?2",
            rusqlite::params![operation_id, session.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(effect_phase, "finalized");
    assert_eq!(attempts, 1);
    let evidence: Value = serde_json::from_str(&evidence).unwrap();
    let launch: runtime_core::OperatorAgentCreateLaunchSnapshot =
        serde_json::from_value(evidence["launch_snapshot"].clone()).unwrap();
    assert_eq!(launch.session.id, session.id);
    assert_eq!(launch.session.provider, session.provider);
    assert_eq!(launch.session.cwd, session.cwd);
    assert_eq!(launch.session.worktree_id, session.worktree_id);
    assert_eq!(launch.session.model, session.model);
    assert_eq!(launch.session.permission_mode, session.permission_mode);
    assert_eq!(launch.session.system_prompt, session.system_prompt);
    assert!(launch.session.provider_session_ref.is_none());
    assert!(launch.session.canonical_provider_session_ref.is_none());
    assert_eq!(launch.agent.agent_id, agent.agent_id);
    assert_eq!(launch.agent.workspace_id, agent.workspace_id);
    assert_eq!(launch.agent.recreation_policy, agent.recreation_policy);
    assert_eq!(launch.session.id, matching[0].runtime_session_id);
    assert_eq!(launch.session.cwd, matching[0].cwd);

    let inventory_after = native_inventory();
    match &original {
        Some((record, revision)) => {
            assert_eq!(session.worktree_id.as_deref(), Some(record.id.as_str()));
            assert_eq!(session.cwd.as_deref(), Some(record.worktree_cwd.as_str()));
            assert_eq!(
                matching[0].cwd.as_deref(),
                Some(record.worktree_cwd.as_str())
            );
            assert_eq!(
                selected, *record,
                "Existing preserves the entire prepared checkout and provenance"
            );
            assert_eq!(selected_revision, *revision);
            assert_eq!(
                git_operations(),
                previous_git,
                "Existing admits no new Git child"
            );
            assert_eq!(
                inventory_after, inventory_before,
                "Existing performs no native Git mutation"
            );
            assert!(
                launch.worktree_operation_id.is_none(),
                "Existing has no creation child link"
            );
        }
        None => {
            assert_eq!(
                git_operations(),
                previous_git + 1,
                "New admits exactly one Git child"
            );
            let checkout_line = format!("worktree {}", selected.worktree_cwd);
            assert!(String::from_utf8_lossy(&inventory_after)
                .lines()
                .any(|line| line == checkout_line));
            let child = selected.created_by_operation_id.as_deref().unwrap();
            assert_eq!(launch.worktree_operation_id.as_deref(), Some(child));
            let (phase, child_key): (String, String) = db
                .query_row(
                    "SELECT phase,idempotency_key FROM runtime_operations
                 WHERE operation_id=?1 AND kind='workspace_worktree_create'",
                    [child],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(phase, "completed");
            assert_eq!(child_key, format!("agent-create-wt:{operation_id}"));
        }
    }
    let expected_git = previous_git + i64::from(original.is_none());
    let (status, operation) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{operation}");
    assert_eq!(operation["operation"]["phase"], "completed");

    let (status, replay, replay_headers) =
        create_http(&router, &token, &workspace.workspace_id, input, Some(&key)).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, created);
    assert_eq!(
        replay_headers.get("x-operation-id"),
        headers.get("x-operation-id")
    );
    assert_eq!(acp.created_sessions().await.len(), previous_creates + 1);
    assert_eq!(git_operations(), expected_git);
    assert_eq!(native_inventory(), inventory_after);
    assert_eq!(
        app.services
            .store
            .get_managed_worktree(worktree_id)
            .unwrap()
            .unwrap(),
        selected
    );
    assert_eq!(
        app.services
            .store
            .managed_worktree_revision(worktree_id)
            .unwrap(),
        selected_revision
    );
    assert_eq!(
        app.services
            .store
            .get_managed_worktree_claim(worktree_id, &session.id)
            .unwrap(),
        Some(claim.clone())
    );

    let (status, cleanup) = call(
        &router,
        &token,
        "POST",
        &format!("/v1/worktrees/{worktree_id}/cleanup"),
        Some(serde_json::json!({})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cleanup}");
    assert_eq!(cleanup["status"], "skipped_live_claims");
    assert!(
        Path::new(&selected.worktree_cwd).is_dir(),
        "live provider route remains intact"
    );
    assert_eq!(
        app.services
            .store
            .list_active_managed_worktree_claims_for_session(&session.id)
            .unwrap(),
        vec![claim]
    );
    assert_eq!(
        app.services
            .store
            .get_managed_worktree(worktree_id)
            .unwrap()
            .unwrap(),
        selected
    );
    assert_eq!(
        app.services
            .store
            .managed_worktree_revision(worktree_id)
            .unwrap(),
        selected_revision
    );
    assert_eq!(git_operations(), expected_git);
    assert_eq!(native_inventory(), inventory_after);
    assert_eq!(acp.created_sessions().await.len(), previous_creates + 1);
}
