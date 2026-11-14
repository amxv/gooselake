use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::WorkspaceAgentLifecycleState;
use std::collections::HashSet;

fn query_plan(connection: &Connection, query: &str) -> String {
    connection
        .prepare(&format!("EXPLAIN QUERY PLAN {query}"))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ")
}

fn assert_indexed_plan(plan: &str, index: &str) {
    assert!(plan.contains(index), "expected {index}: {plan}");
    assert!(
        !plan.contains("USE TEMP B-TREE"),
        "pagination must not sort the full input: {plan}"
    );
}

fn workspace_page_ids(repository: &SqliteRuntimeRepository, size: usize) -> Vec<String> {
    let mut ids = Vec::new();
    let mut cursor = None;
    let mut last_key = None;
    loop {
        let rows = repository.list_workspaces_page(cursor, size).unwrap();
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let key = (row.created_at, row.workspace_id.clone());
            assert!(last_key.as_ref().is_none_or(|previous| previous < &key));
            last_key = Some(key);
            ids.push(row.workspace_id.clone());
        }
        cursor = last_key.clone();
    }
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
    ids
}

fn agent_page_ids(
    repository: &SqliteRuntimeRepository,
    workspace_id: &str,
    lifecycle: Option<WorkspaceAgentLifecycleState>,
    size: usize,
) -> Vec<String> {
    let mut ids = Vec::new();
    let mut cursor = None;
    let mut last_key = None;
    loop {
        let rows = repository
            .list_workspace_agents_page(workspace_id, lifecycle, cursor, size)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            assert_eq!(row.workspace_id, workspace_id);
            assert!(lifecycle.is_none_or(|selected| row.lifecycle_state == selected));
            let key = (row.created_at, row.agent_id.clone());
            assert!(last_key.as_ref().is_none_or(|previous| previous < &key));
            last_key = Some(key);
            ids.push(row.agent_id.clone());
        }
        cursor = last_key.clone();
    }
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
    ids
}

#[test]
fn schema_36_workspace_keyset_seeks_and_reopens_without_reordering_2400_rows() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 35);
    let repository = SqliteRuntimeRepository::new(path.clone());
    let mut connection = open_connection(&path).unwrap();
    let transaction = connection.transaction().unwrap();
    for n in 0..2400 {
        transaction
            .execute(
                "INSERT INTO workspaces (workspace_id,canonical_root,display_name,
               lifecycle_state,revision,created_at,updated_at)
             VALUES (?1,?2,'synthetic','active',0,?3,?3)",
                params![
                    format!("ws_{n:04}"),
                    format!("/disposable/ws_{n:04}"),
                    10_000 + n / 7,
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let before = repository.list_workspaces().unwrap();
    assert_eq!(before.len(), 2400);
    drop(connection);
    repository.initialize_schema().unwrap();
    let reopened = SqliteRuntimeRepository::new(path.clone());
    let connection = open_connection(&path).unwrap();
    let expected = before
        .iter()
        .map(|row| row.workspace_id.clone())
        .collect::<Vec<_>>();
    for size in [1, 29, 100] {
        assert_eq!(workspace_page_ids(&reopened, size), expected);
    }
    assert!(reopened.list_workspaces_page(None, 0).is_err());
    assert!(reopened.list_workspaces_page(None, 102).is_err());
    assert!(reopened
        .list_workspaces_page(Some((10_000, "missing".into())), 1)
        .is_err());
    assert!(reopened
        .list_workspaces_page(Some((10_001, "ws_0000".into())), 1)
        .is_err());

    let plan = query_plan(
        &connection,
        "SELECT workspace_id,canonical_root,created_at FROM workspaces
         ORDER BY created_at,workspace_id LIMIT 30",
    );
    assert_indexed_plan(&plan, "idx_workspaces_created_page");
    let plan = query_plan(
        &connection,
        "SELECT workspace_id,canonical_root,created_at FROM workspaces
         WHERE (created_at,workspace_id) > (10100,'ws_0700')
         ORDER BY created_at,workspace_id LIMIT 30",
    );
    assert_indexed_plan(&plan, "idx_workspaces_created_page");
    assert!(
        plan.contains("SEARCH"),
        "continuation should seek, not full-scan: {plan}"
    );

    // SQLite WAL snapshots hold the initial view while another writer commits.
    // Each API page obtains its own one-transaction cursor+rows snapshot; pages
    // across requests deliberately do not promise a pinned global generation.
    connection.execute_batch("BEGIN DEFERRED").unwrap();
    let before_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
        .unwrap();
    let writer = open_connection(&path).unwrap();
    writer
        .execute(
            "INSERT INTO workspaces (workspace_id,canonical_root,display_name,lifecycle_state,
         revision,created_at,updated_at)
         VALUES ('ws_new','/disposable/new','new','active',0,999999,999999)",
            [],
        )
        .unwrap();
    let snapshot_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
        .unwrap();
    assert_eq!(snapshot_count, before_count);
    connection.execute_batch("COMMIT").unwrap();
    assert_eq!(workspace_page_ids(&reopened, 51).len(), 2401);
    let restarted = SqliteRuntimeRepository::new(path.clone());
    assert_eq!(
        workspace_page_ids(&restarted, 51),
        workspace_page_ids(&reopened, 51)
    );
    assert_eq!(
        connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
}

#[test]
fn schema_36_agent_pages_use_lifecycle_and_mixed_indexes_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 35);
    let repository = SqliteRuntimeRepository::new(path.clone());
    let root = temp.path().join("chosen");
    let other_root = temp.path().join("unrelated");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&other_root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    let foreign = registered_workspace(&repository, &other_root);
    for n in 0..1220 {
        let id = format!("selected_{n:04}");
        let (mut session, mut agent) = workspace_agent_fixture(&workspace, &id, &id);
        let timestamp = 1000 + n / 7;
        session.created_at = timestamp;
        agent.created_at = timestamp;
        agent.updated_at = timestamp;
        repository.create_workspace_agent(&session, &agent).unwrap();
    }
    for n in 0..110 {
        let id = format!("foreign_{n:04}");
        let (session, agent) = workspace_agent_fixture(&foreign, &id, &id);
        repository.create_workspace_agent(&session, &agent).unwrap();
    }
    let connection = open_connection(&path).unwrap();
    connection
        .execute(
            "UPDATE workspace_agents SET lifecycle_state='archived', archived_at=created_at+1
         WHERE workspace_id=?1 AND CAST(SUBSTR(session_id,10) AS INTEGER) % 3=0",
            [workspace.workspace_id.as_str()],
        )
        .unwrap();
    let before_all = repository
        .list_workspace_agents(&workspace.workspace_id, None)
        .unwrap();
    assert_eq!(before_all.len(), 1220);
    drop(connection);
    repository.initialize_schema().unwrap();
    let reopened = SqliteRuntimeRepository::new(path.clone());
    let connection = open_connection(&path).unwrap();

    for lifecycle in [
        None,
        Some(WorkspaceAgentLifecycleState::Active),
        Some(WorkspaceAgentLifecycleState::Archived),
    ] {
        let expected = reopened
            .list_workspace_agents(&workspace.workspace_id, lifecycle)
            .unwrap()
            .into_iter()
            .map(|agent| agent.agent_id)
            .collect::<Vec<_>>();
        assert!(expected.len() > 250);
        for size in [1, 37, 100] {
            assert_eq!(
                agent_page_ids(&reopened, &workspace.workspace_id, lifecycle, size),
                expected
            );
        }
    }
    assert_eq!(
        agent_page_ids(&reopened, &foreign.workspace_id, None, 17).len(),
        110
    );
    assert!(reopened
        .list_workspace_agents_page(&workspace.workspace_id, None, None, 0)
        .is_err());
    assert!(reopened
        .list_workspace_agents_page(&workspace.workspace_id, None, None, 102)
        .is_err());
    let active = reopened
        .list_workspace_agents_page(
            &workspace.workspace_id,
            Some(WorkspaceAgentLifecycleState::Active),
            None,
            5,
        )
        .unwrap();
    let anchor = (active[0].created_at, active[0].agent_id.clone());
    assert!(reopened
        .list_workspace_agents_page(
            &workspace.workspace_id,
            Some(WorkspaceAgentLifecycleState::Archived),
            Some(anchor.clone()),
            5
        )
        .is_err());
    assert!(reopened
        .list_workspace_agents_page(&foreign.workspace_id, None, Some(anchor.clone()), 5)
        .is_err());
    assert!(reopened
        .list_workspace_agents_page(
            &workspace.workspace_id,
            None,
            Some((anchor.0 + 1, anchor.1)),
            5
        )
        .is_err());

    let all_query = format!(
        "{} WHERE a.workspace_id='{}'
         AND (a.created_at,a.session_id) > (1100,'selected_0700')
         ORDER BY a.created_at,a.session_id LIMIT 40",
        crate::repository_workspace_agent::AGENT_SELECT,
        workspace.workspace_id,
    );
    assert_indexed_plan(
        &query_plan(&connection, &all_query),
        "idx_workspace_agents_created_page",
    );
    let active_query = format!(
        "{} WHERE a.workspace_id='{}' AND a.lifecycle_state='active'
         AND (a.created_at,a.session_id) > (1100,'selected_0700')
         ORDER BY a.created_at,a.session_id LIMIT 40",
        crate::repository_workspace_agent::AGENT_SELECT,
        workspace.workspace_id,
    );
    let plan = query_plan(&connection, &active_query);
    assert_indexed_plan(&plan, "idx_workspace_agents_workspace_lifecycle");
    assert!(
        plan.contains("SEARCH"),
        "filtered continuation should seek: {plan}"
    );

    let reader = open_connection(&path).unwrap();
    reader.execute_batch("BEGIN DEFERRED").unwrap();
    let before_active: i64 = reader.query_row(
        "SELECT COUNT(*) FROM workspace_agents WHERE workspace_id=?1 AND lifecycle_state='active'",
        [workspace.workspace_id.as_str()], |row| row.get(0)).unwrap();
    let writer = open_connection(&path).unwrap();
    writer
        .execute(
            "UPDATE workspace_agents SET lifecycle_state='archived', archived_at=created_at+1
         WHERE session_id='selected_0001'",
            [],
        )
        .unwrap();
    let still_active: i64 = reader.query_row(
        "SELECT COUNT(*) FROM workspace_agents WHERE workspace_id=?1 AND lifecycle_state='active'",
        [workspace.workspace_id.as_str()], |row| row.get(0)).unwrap();
    assert_eq!(still_active, before_active);
    reader.execute_batch("COMMIT").unwrap();
    assert_eq!(
        agent_page_ids(
            &reopened,
            &workspace.workspace_id,
            Some(WorkspaceAgentLifecycleState::Active),
            71
        )
        .len() as i64,
        before_active - 1
    );
    let restarted = SqliteRuntimeRepository::new(path);
    for lifecycle in [
        None,
        Some(WorkspaceAgentLifecycleState::Active),
        Some(WorkspaceAgentLifecycleState::Archived),
    ] {
        assert_eq!(
            agent_page_ids(&reopened, &workspace.workspace_id, lifecycle, 79),
            agent_page_ids(&restarted, &workspace.workspace_id, lifecycle, 23)
        );
    }
}
