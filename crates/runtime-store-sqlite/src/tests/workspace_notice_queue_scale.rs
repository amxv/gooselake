use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;

fn populated_notice_queue() -> (tempfile::TempDir, SqliteRuntimeRepository) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "queue_agent", "queue-agent");
    repository
        .create_workspace_agent_with_onboarding(&session, &agent, &Default::default())
        .unwrap();

    let mut db = open_connection(repository.database_path.as_path()).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    let tx = db.transaction().unwrap();
    // Reverse insertion order and tie timestamps; priority depends on kind,
    // not rowid, state or notice_id. Settled rows carry large private inputs
    // but must never be hydrated by the outstanding-notice queue.
    for index in (0..3_600usize).rev() {
        let id = format!("queue_{index:05}");
        let kind = if index % 7 == 0 {
            "onboarding"
        } else {
            "member_joined"
        };
        let state = match index % 5 {
            0 => "pending",
            1 => "injecting",
            2 => "delivered",
            3 => "manual_review",
            _ => "cancelled",
        };
        let payload = serde_json::json!([{
            "type": "text",
            "text": format!("private-{index}-{}", "x".repeat(512)),
        }]);
        tx.execute(
            "INSERT INTO workspace_system_notices(
                notice_id,workspace_id,recipient_agent_id,kind,correlation_id,
                input_json,state,created_at_ms,updated_at_ms
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8)",
            params![
                id,
                workspace.workspace_id,
                agent.agent_id,
                kind,
                format!("queue_cor_{index:05}"),
                payload.to_string(),
                state,
                1 + (index / 11) as i64,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    (temp, repository)
}

fn reference_ids(repository: &SqliteRuntimeRepository) -> Vec<String> {
    let db = open_connection(repository.database_path.as_path()).unwrap();
    let mut statement = db
        .prepare(
            "SELECT notice_id FROM workspace_system_notices
         WHERE state IN ('pending','injecting')
         ORDER BY created_at_ms,
           CASE WHEN kind = 'onboarding' THEN 0 ELSE 1 END,notice_id",
        )
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn assert_exact_queue_pages(repository: &SqliteRuntimeRepository, size: usize) {
    let expected = reference_ids(repository);
    let mut observed = Vec::new();
    let mut cursor = None;
    while observed.len() < expected.len() {
        let page = repository
            .list_workspace_system_notices_after(cursor.as_ref(), size)
            .unwrap();
        assert!(!page.is_empty(), "notices lost before EOF");
        assert!(page.len() <= size.min(256), "page exceeded bound");
        for notice in &page {
            assert!(matches!(notice.state.as_str(), "pending" | "injecting"));
            if notice.notice_id.starts_with("queue_") {
                assert!(notice.input[0]["text"]
                    .as_str()
                    .unwrap()
                    .starts_with("private-"));
            }
            observed.push(notice.notice_id.clone());
        }
        cursor = page.last().cloned();
        assert!(observed.len() <= expected.len(), "repeated queue page");
    }
    assert!(repository
        .list_workspace_system_notices_after(cursor.as_ref(), size)
        .unwrap()
        .is_empty());
    assert_eq!(observed, expected, "ordered no-gap/no-repeat parity");
}

#[test]
fn outstanding_notice_pages_use_partial_order_index_and_preserve_priority() {
    let (_temp, repository) = populated_notice_queue();
    let db = open_connection(repository.database_path.as_path()).unwrap();
    for (sql, required) in [
        (
            "EXPLAIN QUERY PLAN SELECT notice_id FROM workspace_system_notices
             INDEXED BY idx_workspace_system_notices_active_order
             WHERE state IN ('pending','injecting')
             ORDER BY created_at_ms,CASE WHEN kind='onboarding' THEN 0 ELSE 1 END,
                      notice_id LIMIT 37",
            "idx_workspace_system_notices_active_order",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT notice_id FROM workspace_system_notices
             INDEXED BY idx_workspace_system_notices_active_order
             WHERE state IN ('pending','injecting') AND
               (created_at_ms,CASE WHEN kind='onboarding' THEN 0 ELSE 1 END,notice_id)
                 > (35,1,'queue_00355')
             ORDER BY created_at_ms,CASE WHEN kind='onboarding' THEN 0 ELSE 1 END,
                      notice_id LIMIT 37",
            "created_at_ms>?",
        ),
    ] {
        let details = db
            .prepare(sql)
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(details.contains(required), "wrong index/seek: {details}");
        assert!(
            !details.contains("USE TEMP B-TREE"),
            "unexpected sort: {details}"
        );
    }

    for size in [17, 79, 256, 500] {
        assert_exact_queue_pages(&repository, size);
    }
    assert_eq!(
        repository
            .list_workspace_system_notices_after(None, 0)
            .unwrap()
            .len(),
        0,
    );
    assert_eq!(
        repository
            .list_workspace_system_notices_after(None, 1)
            .unwrap()[0]
            .notice_id,
        reference_ids(&repository)[0],
    );
}

#[test]
fn notice_active_order_migration_preserves_queue_and_tracks_wal_state_changes() {
    let (temp, repository) = populated_notice_queue();
    let expected = reference_ids(&repository);
    let path = temp.path().join("runtime.sqlite3");
    let db = open_connection(&path).unwrap();
    // Simulate an already populated version-40 SQLite database, then apply
    // only the additive version-41 read index. No original data is rebuilt.
    db.execute_batch(
        "DROP INDEX idx_workspace_system_notices_active_order;
         DELETE FROM schema_migrations WHERE version=41;",
    )
    .unwrap();
    drop(db);
    repository.initialize_schema().unwrap();
    assert_eq!(reference_ids(&repository), expected);
    let reopened = SqliteRuntimeRepository::new(path.clone());
    assert_exact_queue_pages(&reopened, 53);

    let mut writer = open_connection(&path).unwrap();
    let tx = writer.transaction().unwrap();
    tx.execute(
        "UPDATE workspace_system_notices SET state='pending'
         WHERE notice_id='queue_00002'",
        [],
    )
    .unwrap();
    // Uncommitted writer changes must be invisible to the queue reader.
    assert_eq!(reference_ids(&reopened), expected);
    tx.commit().unwrap();
    let after = reference_ids(&reopened);
    assert_eq!(after.len(), expected.len() + 1);
    assert!(after.contains(&"queue_00002".to_string()));
    assert_exact_queue_pages(&reopened, 37);
    assert_eq!(
        open_connection(&path)
            .unwrap()
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        crate::schema::SCHEMA_VERSION,
    );
}
