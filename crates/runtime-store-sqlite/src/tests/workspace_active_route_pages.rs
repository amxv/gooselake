use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::RuntimeError;

#[test]
fn migration_35_indexes_only_requested_active_routes_and_matches_full_authority_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 34);
    let repository = SqliteRuntimeRepository::new(path.clone());
    let root = temp.path().join("workspace");
    let other_root = temp.path().join("other-workspace");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&other_root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    let unrelated = registered_workspace(&repository, &other_root);
    let target_a = root.join("feature-a").display().to_string();
    let target_b = root.join("feature-b").display().to_string();

    for index in 0..420 {
        let id = format!("selected_{index:04}");
        let (mut session, mut agent) = workspace_agent_fixture(&workspace, &id, &id);
        let cwd = if index < 34 {
            target_a.clone()
        } else if index < 68 {
            target_b.clone()
        } else {
            root.join(format!("unused-{index:04}"))
                .display()
                .to_string()
        };
        session.cwd = Some(cwd.clone());
        agent.recreation_policy.authoritative_cwd = cwd;
        repository.create_workspace_agent(&session, &agent).unwrap();
    }
    for index in 0..40 {
        let id = format!("other_{index:04}");
        let (mut session, mut agent) = workspace_agent_fixture(&unrelated, &id, &id);
        session.cwd = Some(target_a.clone());
        agent.recreation_policy.authoritative_cwd = target_a.clone();
        repository.create_workspace_agent(&session, &agent).unwrap();
    }
    let connection = Connection::open(&path).unwrap();
    // An archived member cannot become an attached agent even when its policy
    // still points at one of these selected paths.
    connection
        .execute(
            "UPDATE workspace_agents SET lifecycle_state='archived', archived_at=200
         WHERE session_id='selected_0000'",
            [],
        )
        .unwrap();
    let count_before: i64 = connection
        .query_row("SELECT COUNT(*) FROM workspace_agents", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count_before, 460);
    drop(connection);

    // Upgrade a genuine disposable schema34 file; preserve all original rows.
    repository.initialize_schema().unwrap();
    let reopened = SqliteRuntimeRepository::new(path.clone());
    let full = reopened
        .list_workspace_active_agent_routes(&workspace.workspace_id)
        .unwrap();
    assert_eq!(full.len(), 419);
    let expected = full
        .iter()
        .filter(|route| route.authoritative_cwd == target_a || route.authoritative_cwd == target_b)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), 67);
    let selected = reopened
        .list_workspace_active_agent_routes_for_cwds(
            &workspace.workspace_id,
            &[target_a.clone(), target_b.clone(), target_a.clone()],
        )
        .unwrap();
    assert_eq!(
        selected, expected,
        "selected CWD projection must match the full authenticated roster"
    );
    assert_eq!(
        reopened
            .list_workspace_active_agent_routes_for_cwds(
                &workspace.workspace_id,
                &[target_b.clone()]
            )
            .unwrap(),
        full.iter()
            .filter(|route| route.authoritative_cwd == target_b)
            .cloned()
            .collect::<Vec<_>>()
    );
    assert!(reopened
        .list_workspace_active_agent_routes_for_cwds(&workspace.workspace_id, &[])
        .unwrap()
        .is_empty());
    assert!(reopened
        .list_workspace_active_agent_routes_for_cwds(
            &workspace.workspace_id,
            &["/not-an-agent-route".into()]
        )
        .unwrap()
        .is_empty());
    assert!(reopened
        .list_workspace_active_agent_routes_for_cwds(
            &workspace.workspace_id,
            &vec![target_a.clone(); 102]
        )
        .is_err());

    let connection = Connection::open(&path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM workspace_agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        count_before
    );
    let sql = format!(
        "EXPLAIN QUERY PLAN SELECT a.session_id, a.recreation_policy_json
         FROM workspace_agents a
         JOIN workspace_agent_profiles p ON p.session_id=a.session_id AND p.workspace_id=a.workspace_id
         JOIN sessions s ON s.id=a.session_id
         WHERE a.workspace_id='workspace' AND a.lifecycle_state='active'
           AND json_extract(a.recreation_policy_json, '$.authoritative_cwd') IN ('{}')
         ORDER BY a.created_at ASC, a.session_id ASC",
        target_a.replace('\'', "''")
    );
    let query_plan = connection
        .prepare(&sql)
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        query_plan.contains("idx_workspace_agents_active_route_cwd"),
        "{query_plan}"
    );
    assert!(
        !query_plan.contains("SCAN a"),
        "selected route may not scan entire roster: {query_plan}"
    );

    // A corrupt persisted policy which claims a selected CWD is never
    // transformed into an assignment approval; the decoder remains authority.
    let original_policy: String = connection
        .query_row(
            "SELECT recreation_policy_json FROM workspace_agents WHERE session_id='selected_0001'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection.execute(
        "UPDATE workspace_agents SET recreation_policy_json=?1 WHERE session_id='selected_0001'",
        [serde_json::json!({"provider":"forged-provider", "authoritative_cwd":target_a}).to_string()],
    ).unwrap();
    assert!(matches!(
        reopened.list_workspace_active_agent_routes_for_cwds(
            &workspace.workspace_id,
            &[target_a.clone()]
        ),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    connection.execute(
        "UPDATE workspace_agents SET recreation_policy_json=?1 WHERE session_id='selected_0001'",
        [original_policy],
    ).unwrap();
    assert_eq!(
        reopened
            .list_workspace_active_agent_routes_for_cwds(
                &workspace.workspace_id,
                &[target_a, target_b]
            )
            .unwrap(),
        expected
    );
}
