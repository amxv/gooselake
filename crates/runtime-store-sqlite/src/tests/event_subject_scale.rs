use super::*;
use std::collections::HashSet;
use std::sync::{Arc, Barrier};

#[test]
fn subject_projection_pages_through_two_thousand_mixed_parent_events_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    repo(&temp).initialize_schema().unwrap();
    let mut connection = open_connection(&path).unwrap();
    let transaction = connection.transaction().unwrap();
    let primary_scopes = [
        ("workspace", "ws_subject_scale"),
        ("session", "sess_subject_scale"),
        ("process", "proc_subject_scale"),
        ("worktree", "wt_subject_scale"),
        ("system", "sys_subject_scale"),
    ];
    let mut expected_turn = Vec::new();
    let mut expected_message = Vec::new();
    let mut expected_operation = Vec::new();
    for index in 0..2_000i64 {
        let (scope, scope_id) = primary_scopes[index as usize % primary_scopes.len()];
        let row_id = index + 1;
        let turn_id = (index % 2 == 0).then_some("turn_subject_scale");
        let message_id = (index % 3 == 0).then_some("msg_subject_scale");
        let operation_id = (index % 5 == 0).then_some("op_subject_scale");
        if turn_id.is_some() {
            expected_turn.push(row_id);
        }
        if message_id.is_some() {
            expected_message.push(row_id);
        }
        if operation_id.is_some() {
            expected_operation.push(row_id);
        }
        transaction
            .execute(
                "INSERT INTO runtime_events
                 (event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at)
                 VALUES (?1,?2,?3,?4,?5,'test.subject_scale',1,?6,?7)",
                params![
                    format!("event_subject_scale_{index:04}"),
                    scope,
                    scope_id,
                    turn_id,
                    index / primary_scopes.len() as i64 + 1,
                    serde_json::json!({
                        "message_id": message_id,
                        "operation_id": operation_id,
                    })
                    .to_string(),
                    10_000 + index,
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);

    let reopened = SqliteRuntimeRepository::new(path.clone());
    for (scope, id, expected) in [
        (
            RuntimeEventScope::Turn,
            "turn_subject_scale",
            &expected_turn,
        ),
        (
            RuntimeEventScope::Message,
            "msg_subject_scale",
            &expected_message,
        ),
        (
            RuntimeEventScope::Operation,
            "op_subject_scale",
            &expected_operation,
        ),
    ] {
        let mut after_id = 0;
        let mut observed = Vec::new();
        let mut history = Vec::new();
        loop {
            let page = reopened
                .list_runtime_events(Some((scope, id)), Some(after_id), 13)
                .unwrap();
            if page.is_empty() {
                break;
            }
            for row in page {
                assert!(row.row_id > after_id, "subject row IDs must increase");
                assert!(
                    matches!(
                        row.scope,
                        RuntimeEventScope::Workspace
                            | RuntimeEventScope::Session
                            | RuntimeEventScope::Process
                            | RuntimeEventScope::Worktree
                            | RuntimeEventScope::System
                    ),
                    "subject projection must preserve its parent scope"
                );
                after_id = row.row_id;
                observed.push(row.row_id);
            }
        }
        after_id = 0;
        loop {
            let page = reopened
                .list_operator_events_page(Some((scope, id)), Some(after_id), 17)
                .unwrap();
            if page.is_empty() {
                break;
            }
            for row in page {
                assert!(row.row_id > after_id);
                after_id = row.row_id;
                history.push(row.row_id);
            }
        }
        assert_eq!(&observed, expected, "journal replay for {scope:?}");
        assert_eq!(&history, expected, "operator history for {scope:?}");
        assert_eq!(
            observed.len(),
            observed.iter().collect::<HashSet<_>>().len()
        );
        assert!(reopened
            .list_runtime_events(Some((scope, id)), Some(after_id), 13)
            .unwrap()
            .is_empty());
    }
    assert_eq!(expected_turn.len(), 1_000);
    assert_eq!(expected_message.len(), 667);
    assert_eq!(expected_operation.len(), 400);

    let appended = reopened
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "event_subject_scale_after_restart".into(),
            scope: RuntimeEventScope::Workspace,
            scope_id: "ws_subject_scale".into(),
            session_id: None,
            team_id: None,
            turn_id: Some("turn_subject_scale".into()),
            kind: "test.subject_scale_restart".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({
                "message_id":"msg_subject_scale",
                "operation_id":"op_subject_scale",
            }),
            provider: None,
            provider_seq: None,
            created_at: 12_001,
        })
        .unwrap();
    assert_eq!(appended.row_id, 2_001);
    assert_eq!(appended.seq, 401);
    for (scope, id, prior_tail) in [
        (
            RuntimeEventScope::Turn,
            "turn_subject_scale",
            *expected_turn.last().unwrap(),
        ),
        (
            RuntimeEventScope::Message,
            "msg_subject_scale",
            *expected_message.last().unwrap(),
        ),
        (
            RuntimeEventScope::Operation,
            "op_subject_scale",
            *expected_operation.last().unwrap(),
        ),
    ] {
        assert_eq!(
            reopened
                .list_runtime_events(Some((scope, id)), Some(prior_tail), 13)
                .unwrap(),
            vec![appended.clone()]
        );
    }

    let connection = open_connection(&path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        2_001,
        "subject links never duplicate canonical journal rows"
    );
    for (scope, subject_id) in [
        ("turn", "turn_subject_scale"),
        ("message", "msg_subject_scale"),
        ("operation", "op_subject_scale"),
    ] {
        let plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN SELECT e.id FROM runtime_event_subjects s
                 JOIN runtime_events e ON e.id=s.event_row_id
                 WHERE s.subject_scope='{scope}' AND s.subject_id='{subject_id}'
                   AND s.event_row_id>1000 ORDER BY s.event_row_id LIMIT 13"
            ))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(
            plan.contains("PRIMARY KEY") || plan.contains("runtime_event_subjects"),
            "{plan}"
        );
        assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
    }
}

#[test]
fn independent_writers_preserve_ordered_multi_subject_links_and_idempotent_replay() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    repo(&temp).initialize_schema().unwrap();
    let barrier = Arc::new(Barrier::new(4));
    let workers = (0..4)
        .map(|worker| {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let repository = SqliteRuntimeRepository::new(path);
                barrier.wait();
                for index in 0..40 {
                    let is_workspace = index % 2 == 0;
                    let event = NewRuntimeEvent {
                        event_id: format!("event_subject_race_{worker}_{index}"),
                        scope: if is_workspace {
                            RuntimeEventScope::Workspace
                        } else {
                            RuntimeEventScope::Process
                        },
                        scope_id: if is_workspace {
                            "ws_subject_race"
                        } else {
                            "proc_subject_race"
                        }
                        .into(),
                        session_id: None,
                        team_id: None,
                        turn_id: Some("turn_subject_race".into()),
                        kind: "test.subject_race".into(),
                        criticality: RuntimeEventCriticality::Critical,
                        payload: serde_json::json!({
                            "message_id":"msg_subject_race",
                            "operation_id":"op_subject_race",
                        }),
                        provider: None,
                        provider_seq: None,
                        created_at: 2000 + index,
                    };
                    let first = repository.append_runtime_event(&event).unwrap();
                    assert_eq!(repository.append_runtime_event(&event).unwrap(), first);
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }

    let reopened = SqliteRuntimeRepository::new(path.clone());
    let expected = (1..=160i64).collect::<Vec<_>>();
    for (scope, id) in [
        (RuntimeEventScope::Turn, "turn_subject_race"),
        (RuntimeEventScope::Message, "msg_subject_race"),
        (RuntimeEventScope::Operation, "op_subject_race"),
    ] {
        let mut after_id = 0;
        let mut observed = Vec::new();
        loop {
            let page = reopened
                .list_runtime_events(Some((scope, id)), Some(after_id), 11)
                .unwrap();
            if page.is_empty() {
                break;
            }
            for row in page {
                assert_eq!(row.row_id, after_id + 1);
                after_id = row.row_id;
                observed.push(row.row_id);
            }
        }
        assert_eq!(observed, expected, "no missing/duplicate subject links");
    }
    let connection = open_connection(&path).unwrap();
    for (table, count) in [("runtime_events", 160), ("runtime_event_subjects", 480)] {
        let actual: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(actual, count, "{table} count");
    }
}
