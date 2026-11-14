use super::*;
use runtime_core::AgentDeliveryListRequest;

#[test]
fn recipient_filtered_delivery_pages_seek_one_recipient_in_large_fanout() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let mut db = open_connection(&path).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    let tx = db.transaction().unwrap();
    tx.execute(
        "INSERT INTO workspaces(workspace_id,canonical_root,display_name,lifecycle_state,
           revision,created_at,updated_at)
         VALUES('ws_fanout','/disposable/fanout','Fanout','active',0,1,1)",
        [],
    )
    .unwrap();
    for message_id in ["msg_fanout", "msg_other"] {
        tx.execute(
            "INSERT INTO agent_messages(id,scope,context_kind,workspace_id,
               sender_agent_id,recipient_agent_ids_json,input_json,image_paths_json,
               priority,policy,created_at)
             VALUES(?1,'broadcast','workspace_team','ws_fanout','sender',
               '[]','[\"private original input\"]','[]','normal','non_interrupting',100)",
            [message_id],
        )
        .unwrap();
    }
    // A single historical message can have thousands of distinct recipients.
    // Reverse insertion order and tie timestamps to exercise the durable ID
    // tie-breaker, not SQLite row insertion order.
    for index in (0..2_400usize).rev() {
        tx.execute(
            "INSERT INTO agent_deliveries(id,message_id,recipient_agent_id,provider,status,
               created_at,updated_at)
             VALUES(?1,'msg_fanout',?2,'inert','pending',?3,?3)",
            params![
                format!("delivery_{index:05}"),
                format!("agent_{index:05}"),
                100 + (index / 7) as i64
            ],
        )
        .unwrap();
    }
    for index in 0..20 {
        tx.execute(
            "INSERT INTO agent_deliveries(id,message_id,recipient_agent_id,provider,status,
               created_at,updated_at)
             VALUES(?1,'msg_other',?2,'inert','pending',?3,?3)",
            params![
                format!("foreign_delivery_{index:05}"),
                format!("agent_{index:05}"),
                100 + (index / 7) as i64
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    // The original unfiltered message-ordered index remains authoritative for
    // full fanout pages; the new index must seek both equality predicates
    // before the page keyset, without sorting unrelated recipients.
    for (sql, index_name, required) in [
        (
            "EXPLAIN QUERY PLAN SELECT id FROM agent_deliveries
             WHERE message_id=?1 ORDER BY created_at,id LIMIT 18",
            "idx_agent_deliveries_message",
            "message_id=?",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id FROM agent_deliveries
             WHERE message_id=?1 AND recipient_agent_id=?2
             ORDER BY created_at,id LIMIT 18",
            "idx_agent_deliveries_message_recipient_created",
            "recipient_agent_id=?",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id FROM agent_deliveries
             WHERE message_id=?1 AND recipient_agent_id=?2
               AND (created_at,id)>(?3,?4)
             ORDER BY created_at,id LIMIT 18",
            "idx_agent_deliveries_message_recipient_created",
            "recipient_agent_id=?",
        ),
    ] {
        let mut statement = db.prepare(sql).unwrap();
        let args: Vec<rusqlite::types::Value> = if sql.contains("(?3,?4)") {
            vec![
                "msg_fanout".to_string().into(),
                "agent_02399".to_string().into(),
                200_i64.into(),
                "delivery_00700".to_string().into(),
            ]
        } else if sql.contains("recipient_agent_id") {
            vec![
                "msg_fanout".to_string().into(),
                "agent_02399".to_string().into(),
            ]
        } else {
            vec!["msg_fanout".to_string().into()]
        };
        let details = statement
            .query_map(rusqlite::params_from_iter(args), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(
            details.contains(index_name),
            "missing {index_name}: {details}"
        );
        assert!(details.contains(required), "not an indexed seek: {details}");
        assert!(
            !details.contains("USE TEMP B-TREE"),
            "unexpected sort: {details}"
        );
    }
    drop(db);

    let full = AgentDeliveryListRequest {
        message_id: Some("msg_fanout".into()),
        recipient_agent_id: None,
    };
    let mut all_ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = repository
            .list_agent_delivery_records_page(&full, cursor.clone(), 37)
            .unwrap();
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|row| (row.created_at, row.id.clone()));
        all_ids.extend(page.into_iter().map(|row| row.id));
    }
    assert_eq!(all_ids.len(), 2_400);
    assert_eq!(all_ids.first().unwrap(), "delivery_00000");
    assert_eq!(all_ids.last().unwrap(), "delivery_02399");
    for index in [0, 7, 1_201, 2_399] {
        let recipient = format!("agent_{index:05}");
        let filtered = AgentDeliveryListRequest {
            message_id: Some("msg_fanout".into()),
            recipient_agent_id: Some(recipient.clone()),
        };
        let page = repository
            .list_agent_delivery_records_page(&filtered, None, 2)
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].recipient_agent_id, recipient);
        assert_eq!(page[0].id, format!("delivery_{index:05}"));
        assert!(repository
            .list_agent_delivery_records_page(
                &filtered,
                Some((page[0].created_at, page[0].id.clone())),
                2,
            )
            .unwrap()
            .is_empty());
    }

    // No migration may rewrite recipient identities, page keys, or private
    // payload. Simulate a populated schema-39 database, then re-apply v40.
    let before = repository
        .list_agent_delivery_records_page(&full, None, 10)
        .unwrap();
    let db = open_connection(&path).unwrap();
    let old_input: String = db
        .query_row(
            "SELECT input_json FROM agent_messages WHERE id='msg_fanout'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    db.execute_batch(
        "DROP INDEX idx_agent_deliveries_message_recipient_created;
         DELETE FROM schema_migrations WHERE version=40;",
    )
    .unwrap();
    drop(db);
    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT max(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert_eq!(
        db.query_row(
            "SELECT input_json FROM agent_messages WHERE id='msg_fanout'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        old_input
    );
    drop(db);
    let reopened = SqliteRuntimeRepository::new(path);
    assert_eq!(
        reopened
            .list_agent_delivery_records_page(&full, None, 10)
            .unwrap(),
        before
    );
}
