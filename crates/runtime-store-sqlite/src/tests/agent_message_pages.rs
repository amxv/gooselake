use super::*;
use runtime_core::{
    prepare_workspace_registration, AgentMessageListRequest, OperationActor, RuntimeError,
    WorkspaceRegisterRequest,
};

fn populated_message_history() -> (tempfile::TempDir, SqliteRuntimeRepository, [String; 2]) {
    let temp = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema");
    let workspaces = std::array::from_fn(|index| {
        let root = temp.path().join(format!("workspace-{index}"));
        std::fs::create_dir(&root).expect("workspace directory");
        repository
            .register_workspace(
                &prepare_workspace_registration(
                    WorkspaceRegisterRequest {
                        canonical_root: root.display().to_string(),
                        display_name: None,
                    },
                    OperationActor::operator("history_test"),
                    None,
                )
                .expect("register command"),
            )
            .expect("register workspace")
            .workspace
            .workspace_id
    });
    let mut connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let tx = connection.transaction().unwrap();
    // Deliberately reverse insertion order. Three messages share each
    // timestamp, proving the cursor must break ties on the durable ID.
    for index in (0..360).rev() {
        let (workspace_id, context_kind, scope): (Option<&str>, &str, &str) = match index % 3 {
            0 => (Some(workspaces[0].as_str()), "workspace_team", "broadcast"),
            1 => (Some(workspaces[1].as_str()), "workspace_team", "broadcast"),
            _ => (None, "global_direct", "direct"),
        };
        tx.execute(
            "INSERT INTO agent_messages(
                 id,scope,context_kind,workspace_id,sender_agent_id,
                 recipient_agent_ids_json,input_json,image_paths_json,
                 priority,policy,created_at
             ) VALUES (?1,?2,?3,?4,?5,'[\"recipient\"]',?6,'[]','normal',
                       'non_interrupting',?7)",
            params![
                format!("msg_{index:04}"),
                scope,
                context_kind,
                workspace_id,
                if index % 2 == 0 { "alice" } else { "bob" },
                serde_json::json!([{"type":"text","text":format!("private-{index}")}]).to_string(),
                index / 3
            ],
        )
        .expect("insert canonical message");
    }
    tx.commit().unwrap();
    (temp, repository, workspaces)
}

fn assert_page_walk(
    repository: &SqliteRuntimeRepository,
    mut request: AgentMessageListRequest,
    expected: Vec<String>,
) {
    let mut seen = Vec::<String>::new();
    let mut pages = 0;
    loop {
        request.limit = Some(17);
        let page = repository
            .list_agent_messages_page(&request)
            .expect("bounded durable page");
        assert!(page.messages.len() <= 17);
        for message in &page.messages {
            assert!(message.input[0]["text"]
                .as_str()
                .unwrap()
                .starts_with("private-"));
            seen.push(message.id.clone());
        }
        pages += 1;
        if let Some(next) = page.next_cursor {
            assert_eq!(
                page.messages.last().map(|m| m.id.as_str()),
                Some(next.as_str())
            );
            request.cursor = Some(next);
        } else {
            break;
        }
        assert!(pages < 50, "cursor failed to advance");
    }
    assert_eq!(
        seen, expected,
        "no duplicates, gaps, or unstable tie ordering"
    );
}

#[test]
fn message_history_uses_durable_bounded_keysets_for_global_workspace_sender_and_combined() {
    let (temp, repository, [workspace_a, workspace_b]) = populated_message_history();
    let expected = |predicate: &dyn Fn(i32) -> bool| {
        (0..360)
            .filter(|index| predicate(*index))
            .map(|index| format!("msg_{index:04}"))
            .collect::<Vec<_>>()
    };
    assert_page_walk(
        &repository,
        AgentMessageListRequest::default(),
        expected(&|_| true),
    );
    assert_page_walk(
        &repository,
        AgentMessageListRequest {
            workspace_id: Some(workspace_a.clone()),
            ..Default::default()
        },
        expected(&|index| index % 3 == 0),
    );
    assert_page_walk(
        &repository,
        AgentMessageListRequest {
            workspace_id: Some(workspace_b),
            sender_agent_id: Some("bob".into()),
            ..Default::default()
        },
        expected(&|index| index % 3 == 1 && index % 2 == 1),
    );
    assert_page_walk(
        &repository,
        AgentMessageListRequest {
            sender_agent_id: Some("alice".into()),
            ..Default::default()
        },
        expected(&|index| index % 2 == 0),
    );

    for request in [
        AgentMessageListRequest {
            cursor: Some("no_such_message".into()),
            ..Default::default()
        },
        AgentMessageListRequest {
            workspace_id: Some(workspace_a),
            cursor: Some("msg_0001".into()),
            ..Default::default()
        },
        AgentMessageListRequest {
            sender_agent_id: Some("bob".into()),
            cursor: Some("msg_0000".into()),
            ..Default::default()
        },
        AgentMessageListRequest {
            limit: Some(501),
            ..Default::default()
        },
        AgentMessageListRequest {
            limit: Some(0),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            repository.list_agent_messages_page(&request),
            Err(RuntimeError::InvalidState(_))
        ));
    }

    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_page_walk(
        &reopened,
        AgentMessageListRequest::default(),
        expected(&|_| true),
    );
}

#[test]
fn message_page_queries_use_indexes_without_temporary_sort_and_migration_preserves_history() {
    let (temp, repository, [workspace_a, _]) = populated_message_history();
    let path = temp.path().join("runtime.sqlite3");
    let db = open_connection(&path).unwrap();
    for (sql, expected_index) in [
        (
            "EXPLAIN QUERY PLAN SELECT id FROM agent_messages
             WHERE (created_at,id)>(20,'msg_0062')
             ORDER BY created_at,id LIMIT 18",
            "idx_agent_messages_created",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id FROM agent_messages
             WHERE workspace_id='existing' AND (created_at,id)>(20,'msg_0062')
             ORDER BY created_at,id LIMIT 18",
            "idx_agent_messages_workspace_created",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id FROM agent_messages
             WHERE sender_agent_id='alice' AND (created_at,id)>(20,'msg_0062')
             ORDER BY created_at,id LIMIT 18",
            "idx_agent_messages_sender_created",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id FROM agent_messages
             WHERE workspace_id='existing' AND sender_agent_id='alice'
             AND (created_at,id)>(20,'msg_0062')
             ORDER BY created_at,id LIMIT 18",
            "idx_agent_messages_workspace_sender_created",
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
        assert!(details.contains(expected_index), "query plan: {details}");
        assert!(!details.contains("USE TEMP B-TREE"), "sort plan: {details}");
    }

    // Simulate an already-populated v23 database: the additive migration
    // recreates the indexes without rewriting IDs, JSON, or cursor keys.
    let old_json: String = db
        .query_row(
            "SELECT input_json FROM agent_messages WHERE id='msg_0000'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    db.execute_batch(
        "DROP INDEX idx_agent_messages_created;
         DROP INDEX idx_agent_messages_workspace_sender_created;
         DELETE FROM schema_migrations WHERE version=24;",
    )
    .unwrap();
    drop(db);
    repository
        .initialize_schema()
        .expect("upgrade populated v23");
    let db = open_connection(&path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT input_json FROM agent_messages WHERE id='msg_0000'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        old_json
    );
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        super::super::schema::SCHEMA_VERSION
    );
    assert_page_walk(
        &repository,
        AgentMessageListRequest {
            workspace_id: Some(workspace_a),
            ..Default::default()
        },
        (0..360)
            .filter(|index| index % 3 == 0)
            .map(|index| format!("msg_{index:04}"))
            .collect(),
    );
}
