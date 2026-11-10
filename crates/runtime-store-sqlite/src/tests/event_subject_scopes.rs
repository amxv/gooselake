use super::*;

#[test]
fn populated_prior_schema_backfills_subjects_without_rewriting_canonical_events() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();

    // Construct the exact previous schema on an isolated disposable database.
    let db = open_connection(&path).unwrap();
    db.execute_batch(
        "DROP TRIGGER runtime_event_subjects_after_insert;
         DROP TABLE runtime_event_subjects;
         DELETE FROM schema_migrations WHERE version=27;",
    )
    .unwrap();
    for (event_id, scope, scope_id, turn_id, payload) in [
        (
            "subject_old_session",
            "session",
            "sess_old",
            Some("turn_alpha"),
            serde_json::json!({"message_id":"msg_alpha", "operation_id":"op_alpha"}),
        ),
        (
            "subject_old_workspace",
            "workspace",
            "ws_old",
            Some("turn_alpha"),
            serde_json::json!({"subject":{"message_id":"msg_alpha"}}),
        ),
        (
            "subject_old_process",
            "process",
            "proc_old",
            None,
            serde_json::json!({"operation_id":"op_alpha"}),
        ),
    ] {
        db.execute(
            "INSERT INTO runtime_events
             (event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at)
             VALUES (?1,?2,?3,?4,1,'fixture.legacy',1,?5,100)",
            params![event_id, scope, scope_id, turn_id, payload.to_string()],
        )
        .unwrap();
    }
    let original = repository.list_runtime_events(None, None, 100).unwrap();
    assert_eq!(original.len(), 3);
    drop(db);

    repository.initialize_schema().unwrap();
    repository.initialize_schema().unwrap();
    let restarted = SqliteRuntimeRepository::new(path.clone());
    assert_eq!(
        restarted.list_runtime_events(None, None, 100).unwrap(),
        original
    );
    for (scope, subject_id, expected) in [
        (RuntimeEventScope::Turn, "turn_alpha", vec![1, 2]),
        (RuntimeEventScope::Message, "msg_alpha", vec![1, 2]),
        (RuntimeEventScope::Operation, "op_alpha", vec![1, 3]),
    ] {
        let first = restarted
            .list_runtime_events(Some((scope, subject_id)), None, 1)
            .unwrap();
        let second = restarted
            .list_runtime_events(Some((scope, subject_id)), Some(first[0].row_id), 1)
            .unwrap();
        let actual = vec![first[0].row_id, second[0].row_id];
        assert_eq!(actual, expected, "scope {scope:?}");
        assert!(restarted
            .list_runtime_events(Some((scope, subject_id)), Some(second[0].row_id), 1)
            .unwrap()
            .is_empty());
        // Secondary events retain their ORIGINAL parent type, scope sequence,
        // and event ID; the derived cursor is the global row ID.
        assert_eq!(first[0], original[(expected[0] - 1) as usize]);
        assert_eq!(second[0], original[(expected[1] - 1) as usize]);
        let summaries = restarted
            .list_operator_events_page(Some((scope, subject_id)), None, 3)
            .unwrap();
        assert_eq!(
            summaries.iter().map(|e| e.row_id).collect::<Vec<_>>(),
            expected
        );
        assert!(restarted
            .list_operator_events_page(Some((scope, subject_id)), Some(2_000), 1)
            .is_err());
    }
    assert!(restarted
        .list_operator_events_page(Some((RuntimeEventScope::Operation, "op_alpha")), Some(2), 1)
        .is_err());

    let db = open_connection(&path).unwrap();
    let plan = db
        .prepare("EXPLAIN QUERY PLAN SELECT e.id FROM runtime_event_subjects s JOIN runtime_events e ON e.id=s.event_row_id WHERE s.subject_scope='turn' AND s.subject_id='turn_alpha' AND s.event_row_id>1 ORDER BY s.event_row_id LIMIT 2")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" ");
    assert!(
        plan.contains("runtime_event_subjects") || plan.contains("PRIMARY KEY"),
        "{plan}"
    );
    assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
}

#[test]
fn subject_links_commit_atomically_with_parent_and_replay_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();

    let first = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "subject_live_one".into(),
            scope: RuntimeEventScope::Workspace,
            scope_id: "ws_subject".into(),
            session_id: Some("sess_subject".into()),
            team_id: None,
            turn_id: Some("turn_subject".into()),
            kind: "operation.changed".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"operation_id":"op_subject", "message_id":"msg_subject"}),
            provider: None,
            provider_seq: None,
            created_at: 100,
        })
        .unwrap();
    assert_eq!(
        repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: first.event_id.clone(),
                scope: RuntimeEventScope::System,
                scope_id: "unused_duplicate".into(),
                session_id: None,
                team_id: None,
                turn_id: None,
                kind: "other".into(),
                criticality: RuntimeEventCriticality::Droppable,
                payload: serde_json::json!({}),
                provider: None,
                provider_seq: None,
                created_at: 999,
            })
            .unwrap(),
        first
    );
    let db = open_connection(&path).unwrap();
    let mut found = 0;
    for (scope, id) in [
        ("turn", "turn_subject"),
        ("message", "msg_subject"),
        ("operation", "op_subject"),
    ] {
        found += db.query_row(
            "SELECT COUNT(*) FROM runtime_event_subjects WHERE subject_scope=?1 AND subject_id=?2 AND event_row_id=?3",
            params![scope, id, first.row_id],
            |row| row.get::<_, i64>(0),
        ).unwrap();
    }
    assert_eq!(found, 3);
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    db.execute("INSERT INTO runtime_events (event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at) VALUES ('subject_rollback','system','rollback','turn_rollback',1,'rollback',1,'{\"message_id\":\"msg_rollback\"}',101)", []).unwrap();
    let projected: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_event_subjects WHERE subject_id='turn_rollback'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(projected, 1);
    db.execute_batch("ROLLBACK").unwrap();
    drop(db);
    let reopened = SqliteRuntimeRepository::new(path);
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap(),
        vec![first.clone()]
    );
    assert!(reopened
        .list_runtime_events(Some((RuntimeEventScope::Turn, "turn_rollback")), None, 10)
        .unwrap()
        .is_empty());
    for (scope, id) in [
        (RuntimeEventScope::Turn, "turn_subject"),
        (RuntimeEventScope::Message, "msg_subject"),
        (RuntimeEventScope::Operation, "op_subject"),
    ] {
        assert_eq!(
            reopened
                .list_runtime_events(Some((scope, id)), None, 2)
                .unwrap(),
            vec![first.clone()]
        );
    }
}
