use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::*;

#[test]
fn workspace_status_roster_is_indexed_bounded_and_preserves_out_of_page_creator_and_lead() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    let unrelated_root = temp.path().join("other-workspace");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&unrelated_root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let unrelated = registered_workspace(&repository, &unrelated_root);

    // Created in reverse alias order: the DB, not insertion order, decides
    // which 100 members enter the model's bounded projection.
    for index in (0..135).rev() {
        let id = format!("sess_status_{index:03}");
        let (session, mut agent) =
            workspace_agent_fixture(&workspace, &id, &format!("friendly-roster-{index:03}"));
        if index == 0 {
            agent.profile.added_by = "sess_status_134".into();
            agent.profile.creator_session_id = Some("sess_status_134".into());
        }
        repository.create_workspace_agent(&session, &agent).unwrap();
    }
    for index in 0..25 {
        let id = format!("sess_unrelated_{index:03}");
        let (session, agent) =
            workspace_agent_fixture(&unrelated, &id, &format!("other-roster-{index:03}"));
        repository.create_workspace_agent(&session, &agent).unwrap();
    }

    let projection = repository
        .list_workspace_status_roster(&workspace.workspace_id, Some("sess_status_134"), 100)
        .unwrap();
    assert_eq!(projection.total_active_members, 135);
    let route_projection = repository
        .list_workspace_active_agent_routes(&workspace.workspace_id)
        .unwrap();
    let full_active = repository
        .list_workspace_agents(
            &workspace.workspace_id,
            Some(runtime_core::WorkspaceAgentLifecycleState::Active),
        )
        .unwrap()
        .into_iter()
        .map(|agent| runtime_core::WorkspaceActiveAgentRoute {
            agent_id: agent.agent_id,
            authoritative_cwd: agent.recreation_policy.authoritative_cwd,
        })
        .collect::<Vec<_>>();
    assert_eq!(route_projection, full_active);
    assert_eq!(route_projection.len(), 135);
    assert!(route_projection
        .iter()
        .all(|route| route.agent_id.starts_with("sess_status_")
            && !route.authoritative_cwd.is_empty()));
    assert_eq!(projection.members.len(), 100);
    assert_eq!(projection.members[0].alias, "friendly-roster-000");
    assert_eq!(projection.members[99].alias, "friendly-roster-099");
    assert_eq!(projection.members[0].profile.added_by, "sess_status_134");
    assert_eq!(
        projection.alias_by_agent_id.get("sess_status_134").unwrap(),
        "friendly-roster-134",
        "lead/creator outside the first 100 must retain full friendly provenance"
    );
    assert_eq!(projection.alias_by_agent_id.len(), 101);
    assert!(!projection
        .alias_by_agent_id
        .contains_key("sess_unrelated_000"));
    assert!(repository
        .list_workspace_status_roster(&workspace.workspace_id, None, 0)
        .is_err());
    assert!(repository
        .list_workspace_status_roster(&workspace.workspace_id, None, 257)
        .is_err());
    let one = repository
        .list_workspace_status_roster(&workspace.workspace_id, None, 1)
        .unwrap();
    assert_eq!(one.members.len(), 1);
    assert_eq!(one.total_active_members, 135);
    assert_eq!(
        one.alias_by_agent_id.len(),
        2,
        "creator fetched in one lookup"
    );
    assert!(repository
        .list_workspace_status_roster(&workspace.workspace_id, Some("sess_unrelated_000"), 1)
        .unwrap()
        .alias_by_agent_id
        .get("sess_unrelated_000")
        .is_none());

    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        reopened
            .list_workspace_active_agent_routes(&workspace.workspace_id)
            .unwrap(),
        route_projection,
        "active worktree attachment routes must be reconstructible after restart"
    );
    assert_eq!(
        reopened
            .list_workspace_status_roster(&workspace.workspace_id, Some("sess_status_134"), 100)
            .unwrap(),
        projection,
        "roster page, count and external alias lookup must reconstruct on restart"
    );
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let plan = |sql: &str| {
        let mut statement = db.prepare(sql).unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" | ")
    };
    let ordered = plan(
        "EXPLAIN QUERY PLAN SELECT a.session_id, a.identity_alias
         FROM workspace_agents a
         JOIN workspace_agent_profiles p ON p.session_id=a.session_id AND p.workspace_id=a.workspace_id
         JOIN sessions s ON s.id=a.session_id
         WHERE a.workspace_id='workspace' AND a.lifecycle_state='active'
         ORDER BY a.identity_alias ASC,a.session_id ASC LIMIT 100",
    );
    assert!(
        ordered.contains("idx_workspace_agents_status_alias"),
        "bounded status must use alias order index: {ordered}"
    );
    assert!(!ordered.contains("USE TEMP B-TREE"), "sort: {ordered}");
    let active_routes = plan(
        "EXPLAIN QUERY PLAN SELECT a.session_id, a.recreation_policy_json
         FROM workspace_agents a
         JOIN workspace_agent_profiles p
           ON p.session_id=a.session_id AND p.workspace_id=a.workspace_id
         JOIN sessions s ON s.id=a.session_id
         WHERE a.workspace_id='workspace' AND a.lifecycle_state='active'
         ORDER BY a.created_at ASC, a.session_id ASC",
    );
    assert!(
        active_routes.contains("idx_workspace_agents_workspace_lifecycle"),
        "thin route projection must use workspace+active+creation index: {active_routes}"
    );
    assert!(
        !active_routes.contains("SCAN a"),
        "full agent scan: {active_routes}"
    );
    assert!(
        !active_routes.contains("USE TEMP B-TREE"),
        "sort: {active_routes}"
    );
    let count = plan(
        "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM workspace_agents
         WHERE workspace_id='workspace' AND lifecycle_state='active'",
    );
    assert!(
        count.contains("USING COVERING INDEX"),
        "count plan: {count}"
    );
}

#[test]
fn schema_26_alias_index_migrates_version_25_without_rewriting_existing_agents() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&db_path, 25);
    let repository = repo(&temp);
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "sess_legacy_25", "legacy-stable");
    repository.create_workspace_agent(&session, &agent).unwrap();

    repository.initialize_schema().unwrap();
    let snapshot = repository
        .list_workspace_status_roster(&workspace.workspace_id, Some(&session.id), 100)
        .unwrap();
    assert_eq!(snapshot.total_active_members, 1);
    assert_eq!(snapshot.members, [agent]);
    assert_eq!(
        snapshot.alias_by_agent_id.get(&session.id).unwrap(),
        "legacy-stable"
    );
    let db = Connection::open(db_path).unwrap();
    let latest: i64 = db
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(latest, crate::schema::SCHEMA_VERSION);
    let new_index: String = db
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='index'
             AND name='idx_workspace_agents_status_alias'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(new_index, "idx_workspace_agents_status_alias");
}
