//! Two independently lagging readers resume different cursor domains across
//! WAL snapshot boundaries. This is read-side proof, not cross-process SSE wake.

use super::*;

fn append(
    writer: &SqliteRuntimeRepository,
    id: &str,
    scope: RuntimeEventScope,
    scope_id: &str,
    with_subjects: bool,
) -> runtime_core::RuntimeEventRecord {
    writer
        .append_runtime_event(&NewRuntimeEvent {
            event_id: id.into(),
            scope,
            scope_id: scope_id.into(),
            session_id: None,
            team_id: None,
            turn_id: with_subjects.then(|| "turn_shared".into()),
            kind: "synthetic.two_readers".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: if with_subjects {
                serde_json::json!({"message_id":"msg_shared","operation_id":"op_shared"})
            } else {
                serde_json::json!({"unrelated":"process event"})
            },
            provider: None,
            provider_seq: None,
            created_at: 100,
        })
        .unwrap()
}

fn ids(rows: &[runtime_core::RuntimeEventRecord]) -> Vec<&str> {
    rows.iter().map(|event| event.event_id.as_str()).collect()
}

#[test]
fn two_reader_wal_snapshots_do_not_advance_persisted_cursor_until_replayed() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let writer = repo(&temp);
    writer.initialize_schema().unwrap();

    let first = append(
        &writer,
        "first_ws",
        RuntimeEventScope::Workspace,
        "ws_a",
        true,
    );
    let other = append(
        &writer,
        "first_proc",
        RuntimeEventScope::Process,
        "proc_b",
        false,
    );
    let third = append(
        &writer,
        "second_ws",
        RuntimeEventScope::Workspace,
        "ws_a",
        true,
    );

    let reader_global = SqliteRuntimeRepository::new(path.clone());
    let reader_workspace = SqliteRuntimeRepository::new(path.clone());
    let global_page = reader_global.list_runtime_events(None, None, 2).unwrap();
    assert_eq!(ids(&global_page), vec!["first_ws", "first_proc"]);
    let global_cursor = global_page[1].row_id;
    assert_eq!(global_cursor, other.row_id);

    let workspace = Some((RuntimeEventScope::Workspace, "ws_a"));
    let workspace_page = reader_workspace
        .list_runtime_events(workspace, None, 1)
        .unwrap();
    assert_eq!(ids(&workspace_page), vec!["first_ws"]);
    let workspace_cursor = workspace_page[0].seq;
    assert_eq!(workspace_cursor, first.seq);
    let subject = Some((RuntimeEventScope::Turn, "turn_shared"));
    assert_eq!(
        ids(&reader_workspace
            .list_runtime_events(subject, None, 1)
            .unwrap()),
        vec!["first_ws"]
    );

    // These are two actual SQLite WAL reader snapshots, pinned before any
    // subsequent commit. A writer may commit while BOTH still see the old tip.
    let snapshot_a = Connection::open(&path).unwrap();
    let snapshot_b = Connection::open(&path).unwrap();
    for db in [&snapshot_a, &snapshot_b] {
        db.execute_batch("BEGIN DEFERRED").unwrap();
        assert_eq!(
            db.query_row("SELECT MAX(id) FROM runtime_events", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            third.row_id
        );
    }

    let fourth = append(
        &writer,
        "second_proc",
        RuntimeEventScope::Process,
        "proc_b",
        false,
    );
    let fifth = append(
        &writer,
        "third_ws",
        RuntimeEventScope::Workspace,
        "ws_a",
        true,
    );
    assert_eq!(fifth.seq, third.seq + 1);
    for db in [&snapshot_a, &snapshot_b] {
        assert_eq!(
            db.query_row("SELECT MAX(id) FROM runtime_events", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            third.row_id,
            "a pinned WAL reader must not claim another reader's newer cursor"
        );
    }

    // Reader A releases its snapshot and resumes from its *own* last-seen
    // global cursor. Reader B deliberately remains pinned across another write.
    snapshot_a.execute_batch("COMMIT").unwrap();
    assert_eq!(
        ids(&reader_global
            .list_runtime_events(None, Some(global_cursor), 2)
            .unwrap()),
        vec!["second_ws", "second_proc"]
    );
    let sixth = append(
        &writer,
        "fourth_ws",
        RuntimeEventScope::Workspace,
        "ws_a",
        true,
    );
    assert_eq!(
        snapshot_b
            .query_row("SELECT MAX(id) FROM runtime_events", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        third.row_id
    );
    snapshot_b.execute_batch("COMMIT").unwrap();

    // Simulate independent consumer restarts at DIFFERENT page boundaries.
    // Workspace uses per-scope `seq`; global and derived subjects use row ID.
    let resumed_global = SqliteRuntimeRepository::new(path.clone());
    let resumed_workspace = SqliteRuntimeRepository::new(path);
    let mut cursor = global_cursor;
    let mut global_tail = Vec::new();
    loop {
        let page = resumed_global
            .list_runtime_events(None, Some(cursor), 2)
            .unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.iter().all(|row| row.row_id > cursor));
        cursor = page.last().unwrap().row_id;
        global_tail.extend(page);
    }
    assert_eq!(
        ids(&global_tail),
        vec!["second_ws", "second_proc", "third_ws", "fourth_ws"]
    );
    assert_eq!(ids(&global_tail)[0], third.event_id);
    assert_eq!(global_tail[1].row_id, fourth.row_id);
    assert_eq!(global_tail[2].row_id, fifth.row_id);
    assert_eq!(global_tail[3].row_id, sixth.row_id);

    let mut seq_cursor = workspace_cursor;
    let mut workspace_tail = Vec::new();
    loop {
        let page = resumed_workspace
            .list_runtime_events(workspace, Some(seq_cursor), 1)
            .unwrap();
        if page.is_empty() {
            break;
        }
        assert_eq!(page[0].seq, seq_cursor + 1);
        seq_cursor = page[0].seq;
        workspace_tail.extend(page);
    }
    assert_eq!(
        ids(&workspace_tail),
        vec!["second_ws", "third_ws", "fourth_ws"]
    );
    assert_eq!(seq_cursor, sixth.seq);

    for kind in [
        RuntimeEventScope::Turn,
        RuntimeEventScope::Message,
        RuntimeEventScope::Operation,
    ] {
        let id = match kind {
            RuntimeEventScope::Turn => "turn_shared",
            RuntimeEventScope::Message => "msg_shared",
            _ => "op_shared",
        };
        let collection = Some((kind, id));
        let mut cursor = first.row_id;
        let mut tail = Vec::new();
        loop {
            let page = resumed_workspace
                .list_runtime_events(collection, Some(cursor), 1)
                .unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page[0].row_id > cursor);
            cursor = page[0].row_id;
            tail.extend(page);
        }
        assert_eq!(ids(&tail), ids(&workspace_tail));
        assert_eq!(cursor, sixth.row_id, "subject cursor is a global row ID");
        let operator = resumed_workspace
            .list_operator_events_page(collection, Some(first.row_id), 10)
            .unwrap();
        assert_eq!(
            operator.iter().map(|row| row.row_id).collect::<Vec<_>>(),
            tail.iter().map(|row| row.row_id).collect::<Vec<_>>()
        );
    }
    assert!(resumed_global
        .list_runtime_events(None, Some(sixth.row_id), 2)
        .unwrap()
        .is_empty());
}
