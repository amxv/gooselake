//! Independently reconstruct the *read-side* subject index from retained
//! canonical event rows. This oracle is intentionally not the migration's SQL
//! and never repairs/mutates a user database.

use super::*;
use std::collections::BTreeSet;

type SubjectLink = (String, String, i64);

fn projected_text(value: Option<&serde_json::Value>) -> Option<&str> {
    match value {
        Some(serde_json::Value::String(value)) if (1..=256).contains(&value.chars().count()) => {
            Some(value)
        }
        _ => None,
    }
}

fn canonical_subject_links(connection: &Connection) -> BTreeSet<SubjectLink> {
    let mut statement = connection
        .prepare(
            "SELECT id, scope, scope_id, turn_id, payload_json
                  FROM runtime_events ORDER BY id",
        )
        .unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .unwrap();
    let mut expected = BTreeSet::new();
    for row in rows {
        let (row_id, scope, scope_id, turn_id, payload) = row.unwrap();
        if matches!(scope.as_str(), "turn" | "message" | "operation") && !scope_id.is_empty() {
            expected.insert((scope, scope_id, row_id));
        }
        if let Some(turn_id) = turn_id.filter(|id| !id.is_empty()) {
            expected.insert(("turn".into(), turn_id, row_id));
        }
        // A malformed historical payload contributes no payload-derived
        // subject. A non-null non-string top-level message_id takes precedence
        // over a valid nested subject.message_id, matching SQLite COALESCE.
        if let Ok(payload) = serde_json::from_str::<serde_json::Value>(&payload) {
            let message = payload
                .get("message_id")
                .filter(|value| !value.is_null())
                .or_else(|| payload.pointer("/subject/message_id"));
            if let Some(id) = projected_text(message) {
                expected.insert(("message".into(), id.into(), row_id));
            }
            if let Some(id) = projected_text(payload.get("operation_id")) {
                expected.insert(("operation".into(), id.into(), row_id));
            }
        }
    }
    expected
}

fn stored_subject_links(connection: &Connection) -> BTreeSet<SubjectLink> {
    connection
        .prepare("SELECT subject_scope, subject_id, event_row_id FROM runtime_event_subjects")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<BTreeSet<_>, _>>()
        .unwrap()
}

#[test]
fn independent_rebuild_matches_mixed_retained_critical_sources_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut db = open_connection(&path).unwrap();
    let transaction = db.transaction().unwrap();
    let scopes = [
        "workspace",
        "session",
        "process",
        "worktree",
        "system",
        "team",
        "turn",
        "message",
        "operation",
    ];
    for index in 0..1_269i64 {
        let scope = scopes[index as usize % scopes.len()];
        let payload = match index % 7 {
            0 => serde_json::json!({"message_id":"msg_rebuild", "operation_id":"op_rebuild"}).to_string(),
            1 => serde_json::json!({"subject":{"message_id":"msg_rebuild"}, "operation_id":"op_rebuild"}).to_string(),
            2 => serde_json::json!({"message_id":null, "subject":{"message_id":"msg_rebuild"}}).to_string(),
            3 => serde_json::json!({"message_id":42, "subject":{"message_id":"msg_rebuild"}}).to_string(),
            4 => serde_json::json!({"message_id":"", "operation_id":""}).to_string(),
            5 => serde_json::json!({"message_id":"m".repeat(257), "operation_id":true}).to_string(),
            _ => "{not-valid-json".into(),
        };
        transaction
            .execute(
                "INSERT INTO runtime_events
                 (event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at)
                 VALUES (?1,?2,?3,?4,?5,'test.canonical_rebuild',?6,?7,?8)",
                params![
                    format!("rebuild_source_{index:04}"),
                    scope,
                    format!("{scope}_rebuild"),
                    (index % 2 == 0).then_some("turn_rebuild"),
                    index / scopes.len() as i64 + 1,
                    i64::from(index % 5 != 0),
                    payload,
                    index + 100,
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let before = canonical_subject_links(&db);
    assert!(before.len() > 1_000, "mixed canonical event coverage");
    assert_eq!(stored_subject_links(&db), before);
    drop(db);
    repository.initialize_schema().unwrap();
    let reopened = open_connection(&path).unwrap();
    assert_eq!(canonical_subject_links(&reopened), before);
    assert_eq!(stored_subject_links(&reopened), before);
    let expected_message_ids = before
        .iter()
        .filter(|(scope, id, _)| scope == "message" && id == "msg_rebuild")
        .map(|(_, _, event_id)| *event_id)
        .collect::<Vec<_>>();
    let mut cursor = 0;
    let mut actual = Vec::new();
    loop {
        let page = repository
            .list_runtime_events(
                Some((RuntimeEventScope::Message, "msg_rebuild")),
                Some(cursor),
                17,
            )
            .unwrap();
        if page.is_empty() {
            break;
        }
        cursor = page.last().unwrap().row_id;
        actual.extend(page.iter().map(|event| event.row_id));
    }
    assert_eq!(actual, expected_message_ids);
}

#[test]
fn failed_critical_projection_insert_never_commits_its_parent_and_tampering_is_detectable() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let clean = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_reconcile_clean".into(),
            scope: RuntimeEventScope::Workspace,
            scope_id: "ws_reconcile".into(),
            session_id: None,
            team_id: None,
            turn_id: Some("turn_reconcile".into()),
            kind: "test.critical_reconcile".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"message_id":"msg_reconcile", "operation_id":"op_reconcile"}),
            provider: None,
            provider_seq: None,
            created_at: 100,
        })
        .unwrap();
    let db = open_connection(&path).unwrap();
    let expected = canonical_subject_links(&db);
    assert_eq!(stored_subject_links(&db), expected);

    db.execute_batch(
        "CREATE TRIGGER injected_projection_fault BEFORE INSERT ON runtime_event_subjects
         WHEN NEW.subject_scope='message' AND NEW.subject_id='msg_fault'
         BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END;",
    )
    .unwrap();
    assert!(db
        .execute(
            "INSERT INTO runtime_events
         (event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at)
         VALUES ('evt_reconcile_fault','workspace','ws_reconcile','turn_fault',2,
                 'test.faulted_critical',1,'{\"message_id\":\"msg_fault\"}',101)",
            [],
        )
        .is_err());
    db.execute_batch("DROP TRIGGER injected_projection_fault;")
        .unwrap();
    assert_eq!(canonical_subject_links(&db), expected);
    assert_eq!(stored_subject_links(&db), expected);
    assert!(repository
        .get_operator_event("evt_reconcile_fault")
        .unwrap()
        .is_none());

    // This is a disposable corruption fixture, NOT a production repair.
    db.execute(
        "DELETE FROM runtime_event_subjects
         WHERE event_row_id=?1 AND subject_scope='message' AND subject_id='msg_reconcile'",
        params![clean.row_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO runtime_event_subjects(subject_scope,subject_id,event_row_id)
         VALUES ('operation','op_forged',?1)",
        params![clean.row_id],
    )
    .unwrap();
    drop(db);

    // Re-running applied migrations does not silently alter user data or
    // manufacture missing history. The retained parent still reveals the
    // exact absent and forged subject projections after restart.
    repository.initialize_schema().unwrap();
    let reopened = open_connection(&path).unwrap();
    assert_eq!(canonical_subject_links(&reopened), expected);
    let found = stored_subject_links(&reopened);
    let missing = expected.difference(&found).cloned().collect::<Vec<_>>();
    let unexpected = found.difference(&expected).cloned().collect::<Vec<_>>();
    assert_eq!(
        missing,
        [("message".into(), "msg_reconcile".into(), clean.row_id)]
    );
    assert_eq!(
        unexpected,
        [("operation".into(), "op_forged".into(), clean.row_id)]
    );
    assert!(repository
        .list_runtime_events(
            Some((RuntimeEventScope::Message, "msg_reconcile")),
            None,
            10
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        repository.list_runtime_events(None, None, 10).unwrap(),
        [clean]
    );
}

#[test]
fn startup_fails_closed_if_required_subject_projection_objects_were_lost() {
    for missing_schema_sql in [
        "DROP TRIGGER runtime_event_subjects_after_insert",
        "DROP TABLE runtime_event_subjects",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite3");
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let original = repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: "evt_subject_attest".into(),
                scope: RuntimeEventScope::Workspace,
                scope_id: "ws_subject_attest".into(),
                session_id: None,
                team_id: None,
                turn_id: Some("turn_subject_attest".into()),
                kind: "test.required_projection".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"message_id":"msg_subject_attest"}),
                provider: None,
                provider_seq: None,
                created_at: 100,
            })
            .unwrap();
        open_connection(&path)
            .unwrap()
            .execute_batch(missing_schema_sql)
            .unwrap();
        let reason = repository.initialize_schema().unwrap_err().to_string();
        assert!(
            reason.contains("event subject projection table or append trigger"),
            "{reason}"
        );
        assert_eq!(
            repository.list_runtime_events(None, None, 5).unwrap(),
            [original]
        );
        // No auto-repair, migration-marker rewrite or fabricated critical row.
        let db = open_connection(&path).unwrap();
        let migrations: i64 = db
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(migrations, crate::schema::SCHEMA_VERSION);
    }
}

#[test]
fn startup_refuses_same_named_weakened_projection_sql_without_rewriting_events() {
    for altered_trigger in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite3");
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let original = repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: "evt_subject_schema_attest".into(),
                scope: RuntimeEventScope::Workspace,
                scope_id: "ws_subject_schema_attest".into(),
                session_id: None,
                team_id: None,
                turn_id: Some("turn_subject_schema_attest".into()),
                kind: "test.projection_schema_attestation".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"message_id":"msg_subject_schema_attest"}),
                provider: None,
                provider_seq: None,
                created_at: 100,
            })
            .unwrap();
        let db = open_connection(&path).unwrap();
        let original_links = stored_subject_links(&db);
        assert_eq!(original_links, canonical_subject_links(&db));
        if altered_trigger {
            // Old rows still look healthy, but future appended events would
            // lose every linked subject if name-only verification passed.
            db.execute_batch(
                "DROP TRIGGER runtime_event_subjects_after_insert;
                 CREATE TRIGGER runtime_event_subjects_after_insert
                 AFTER INSERT ON runtime_events BEGIN SELECT 1; END;",
            )
            .unwrap();
        } else {
            // Preserve every existing link while removing the original
            // primary key and foreign-key enforcement under the same name.
            db.execute_batch(
                "DROP TABLE runtime_event_subjects;
                 CREATE TABLE runtime_event_subjects (
                   subject_scope TEXT, subject_id TEXT, event_row_id INTEGER
                 );",
            )
            .unwrap();
            for (scope, id, row_id) in &original_links {
                db.execute(
                    "INSERT INTO runtime_event_subjects VALUES (?1,?2,?3)",
                    params![scope, id, row_id],
                )
                .unwrap();
            }
        }
        assert_eq!(stored_subject_links(&db), original_links);
        drop(db);
        let reason = repository.initialize_schema().unwrap_err().to_string();
        assert!(
            reason.contains("differs from canonical migration 27"),
            "{reason}"
        );
        let reopened = open_connection(&path).unwrap();
        assert_eq!(stored_subject_links(&reopened), original_links);
        assert_eq!(canonical_subject_links(&reopened), original_links);
        assert_eq!(
            repository.list_runtime_events(None, None, 10).unwrap(),
            [original]
        );
        let migration_count: i64 = reopened
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(migration_count, crate::schema::SCHEMA_VERSION);
    }
}

#[test]
fn independent_rebuild_reads_one_consistent_snapshot_during_concurrent_commits() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    repo(&temp).initialize_schema().unwrap();
    let event = |index: usize| NewRuntimeEvent {
        event_id: format!("evt_rebuild_concurrent_{index:03}"),
        scope: RuntimeEventScope::Workspace,
        scope_id: "ws_rebuild_concurrent".into(),
        session_id: None,
        team_id: None,
        turn_id: Some("turn_rebuild_concurrent".into()),
        kind: "test.concurrent_critical".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"message_id":"msg_rebuild_concurrent","operation_id":"op_rebuild_concurrent"}),
        provider: None,
        provider_seq: None,
        created_at: index as i64,
    };
    let repository = SqliteRuntimeRepository::new(path.clone());
    for index in 0..40 {
        repository.append_runtime_event(&event(index)).unwrap();
    }
    let mut db = open_connection(&path).unwrap();
    let snapshot = db.transaction().unwrap();
    let old_count: i64 = snapshot
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(old_count, 40);
    let writer = std::thread::spawn(move || {
        for index in 40..80 {
            repository.append_runtime_event(&event(index)).unwrap();
        }
    });
    writer.join().unwrap();
    let original = canonical_subject_links(&snapshot);
    assert_eq!(stored_subject_links(&snapshot), original);
    assert_eq!(
        snapshot
            .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        40
    );
    snapshot.commit().unwrap();
    let new_expected = canonical_subject_links(&db);
    assert_eq!(stored_subject_links(&db), new_expected);
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        80
    );
    assert_eq!(new_expected.len(), original.len() * 2);
}
