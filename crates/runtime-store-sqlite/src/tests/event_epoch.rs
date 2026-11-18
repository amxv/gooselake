//! Only owned disposable databases and inert child test processes.
use super::*;
use crate::repository_event_cursor::read_journal_epoch;
use runtime_core::{
    EventCollection, EventCursor, EventCursorRejection, EventReadError, RuntimeEventRecord,
};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

const CHILD_PATH: &str = "GOOSELAKE_N07_EPOCH_FIXTURE";
const CHILD_RECEIPT: &str = "GOOSELAKE_N07_EPOCH_RECEIPT";

fn epoch_fixture_bytes(path: &Path) -> Vec<Option<Vec<u8>>> {
    ["", "-wal"]
        .into_iter()
        .map(|suffix| {
            let mut name = path.as_os_str().to_os_string();
            name.push(suffix);
            match std::fs::read(PathBuf::from(name)) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("cannot inspect owned fixture bytes: {error}"),
            }
        })
        .collect()
}

fn marker_carrier(db: &Connection) -> (String, Vec<(String, String, String)>) {
    let schema = db
        .query_row(
            "SELECT sql FROM main.sqlite_master WHERE type='table' AND name='schema_migrations'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    let mut statement = db.prepare(
        "SELECT typeof(version),quote(version),quote(applied_at) FROM main.schema_migrations ORDER BY version",
    ).unwrap();
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    (schema, rows)
}

#[test]
fn event_epoch_rejects_extra_and_real_markers_without_repairing_fixture_bytes() {
    for real_versions in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let event = seed_event(&repository, "marker_boundary", "workspace_epoch");
        let collection = EventCollection::history(None).unwrap();
        let page = repository
            .list_operator_events_page_bound(&collection, None, 1)
            .unwrap();
        let cursor = EventCursor::for_event(
            &page.epoch,
            &collection,
            event.row_id,
            event.seq,
            &event.event_id,
        )
        .unwrap();
        // Retain an owned connection to avoid last-reader WAL cleanup changing
        // the byte oracle; SHM reader bookkeeping is deliberately not compared.
        let db = Connection::open(&repository.database_path).unwrap();
        let supported = crate::schema::SCHEMA_VERSION;
        assert_eq!(versions(&db), (1..=supported).collect::<Vec<_>>());
        if real_versions {
            db.execute_batch("BEGIN IMMEDIATE;
                CREATE TABLE malformed_markers(version,applied_at);
                INSERT INTO malformed_markers SELECT CAST(version AS REAL),applied_at FROM schema_migrations ORDER BY version;
                DROP TABLE schema_migrations;
                ALTER TABLE malformed_markers RENAME TO schema_migrations;
                COMMIT;").unwrap();
        } else {
            db.execute(
                "INSERT INTO schema_migrations(version,applied_at) VALUES(?1,100)",
                [supported + 1],
            )
            .unwrap();
        }
        let carrier_before = marker_carrier(&db);
        if real_versions {
            assert_eq!(carrier_before.1.len(), supported as usize);
            assert!(carrier_before.1.iter().all(|row| row.0 == "real"));
        } else {
            assert_eq!(carrier_before.1.len(), supported as usize + 1);
            assert!(carrier_before.1.iter().all(|row| row.0 == "integer"));
            assert_eq!(
                carrier_before.1.last().unwrap().1,
                (supported + 1).to_string()
            );
        }
        let checkpoint: (i64, i64, i64) = db
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(checkpoint.0, 0);
        let bytes_before = epoch_fixture_bytes(&repository.database_path);
        for _ in 0..2 {
            assert!(matches!(
                repository.list_operator_events_page_bound(&collection, Some(&cursor), 1),
                Err(EventReadError::Runtime(_))
            ));
            assert!(read_journal_epoch(&db).is_err());
            assert!(repository.initialize_schema().is_err());
            assert_eq!(marker_carrier(&db), carrier_before);
            assert_eq!(
                db.query_row(
                    "SELECT epoch FROM main.runtime_event_journal_identity",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
                page.epoch.as_str()
            );
            assert_eq!(
                repository.list_runtime_events(None, None, 8).unwrap(),
                vec![event.clone()]
            );
            assert_eq!(
                epoch_fixture_bytes(&repository.database_path),
                bytes_before,
                "refusal must not repair or rewrite the rejected fixture"
            );
        }
    }
}

pub(super) fn schema_52(path: &Path) {
    let mut db = Connection::open(path).unwrap();
    for migration in crate::schema::MIGRATIONS.iter().filter(|m| m.version <= 52) {
        let tx = db.transaction().unwrap();
        tx.execute_batch(migration.sql).unwrap();
        tx.execute(
            "INSERT INTO schema_migrations(version,applied_at) VALUES(?1,100)",
            [migration.version],
        )
        .unwrap();
        tx.commit().unwrap();
    }
}

pub(super) fn seed_event(
    repository: &SqliteRuntimeRepository,
    id: &str,
    scope_id: &str,
) -> RuntimeEventRecord {
    repository.append_runtime_event(&NewRuntimeEvent {
        event_id: id.into(), scope: RuntimeEventScope::Workspace, scope_id: scope_id.into(),
        session_id: None, team_id: None, turn_id: Some("turn_epoch".into()),
        kind: "fixture.epoch".into(), criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"message_id":"message_epoch","operation_id":"operation_epoch","raw":"preserved"}),
        provider: None, provider_seq: None, created_at: 100,
    }).unwrap()
}

fn versions(db: &Connection) -> Vec<i64> {
    db.prepare("SELECT version FROM schema_migrations ORDER BY version")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn child_epoch_initialize_and_read() {
    let Some(path) = std::env::var_os(CHILD_PATH) else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let repository = SqliteRuntimeRepository::new(path.clone());
    repository.initialize_schema().unwrap();
    let epoch = read_journal_epoch(&Connection::open(&path).unwrap()).unwrap();
    let receipt = PathBuf::from(std::env::var_os(CHILD_RECEIPT).expect("owned child receipt path"));
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(receipt)
        .unwrap();
    output.write_all(epoch.as_str().as_bytes()).unwrap();
    output.sync_all().unwrap();
}

fn receipt_path(path: &Path, tag: &str) -> PathBuf {
    path.with_extension(format!("{tag}.child-epoch"))
}

fn child(path: &Path, tag: &str) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::event_epoch::child_epoch_initialize_and_read",
            "--nocapture",
        ])
        .env_clear()
        .env(CHILD_PATH, path)
        .env(CHILD_RECEIPT, receipt_path(path, tag))
        .env("RUST_TEST_THREADS", "1")
        .spawn()
        .unwrap()
}

#[test]
fn event_epoch_migrates_once_and_survives_concurrent_process_initialization_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    schema_52(&repository.database_path);
    let original = seed_event(&repository, "original_epoch_event", "workspace_epoch");
    let prior = repository.list_runtime_events(None, None, 100).unwrap();
    assert_eq!(prior, vec![original]);
    let mut a = child(&repository.database_path, "init-a");
    let mut b = child(&repository.database_path, "init-b");
    assert!(a.wait().unwrap().success());
    assert!(b.wait().unwrap().success());
    let db = Connection::open(&repository.database_path).unwrap();
    let epoch = read_journal_epoch(&db).unwrap();
    assert_eq!(versions(&db), (1..=53).collect::<Vec<_>>());
    for tag in ["init-a", "init-b"] {
        assert_eq!(
            std::fs::read_to_string(receipt_path(&repository.database_path, tag)).unwrap(),
            epoch.as_str()
        );
    }
    repository.initialize_schema().unwrap();
    assert_eq!(read_journal_epoch(&db).unwrap(), epoch);
    assert_eq!(
        repository.list_runtime_events(None, None, 100).unwrap(),
        prior
    );
    let mut restarted = child(&repository.database_path, "restart");
    assert!(restarted.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(receipt_path(&repository.database_path, "restart")).unwrap(),
        epoch.as_str()
    );
    assert_eq!(read_journal_epoch(&db).unwrap(), epoch);
    assert_eq!(
        repository.list_runtime_events(None, None, 100).unwrap(),
        prior
    );
}

#[test]
fn event_epoch_initialization_and_version_marker_roll_back_together() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    schema_52(&repository.database_path);
    let event = seed_event(&repository, "migration_failure_event", "workspace_epoch");
    let db = Connection::open(&repository.database_path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER epoch_marker_fault BEFORE INSERT ON schema_migrations
        WHEN NEW.version=53 BEGIN SELECT RAISE(ABORT,'injected after epoch insert'); END;",
    )
    .unwrap();
    for _ in 0..2 {
        assert!(repository.initialize_schema().is_err());
        assert_eq!(versions(&db), (1..=52).collect::<Vec<_>>());
        assert_eq!(
            db.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='runtime_event_journal_identity'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        assert_eq!(
            repository.list_runtime_events(None, None, 100).unwrap(),
            vec![event.clone()]
        );
    }
    db.execute_batch("DROP TRIGGER epoch_marker_fault").unwrap();
    repository.initialize_schema().unwrap();
    let epoch = read_journal_epoch(&db).unwrap();
    repository.initialize_schema().unwrap();
    assert_eq!(read_journal_epoch(&db).unwrap(), epoch);
    assert_eq!(
        repository.list_runtime_events(None, None, 100).unwrap(),
        vec![event]
    );
}

#[test]
fn event_epoch_current_damage_never_mints_a_replacement_on_reopen_or_read() {
    for damage in [
        "DELETE FROM runtime_event_journal_identity",
        "PRAGMA ignore_check_constraints=ON; UPDATE runtime_event_journal_identity SET epoch='bad'",
        "PRAGMA ignore_check_constraints=ON; UPDATE runtime_event_journal_identity SET epoch=zeroblob(16)",
        "DROP TABLE runtime_event_journal_identity",
        "PRAGMA ignore_check_constraints=ON; INSERT INTO runtime_event_journal_identity VALUES(2,'ffeeddccbbaa99887766554433221100')",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let event = seed_event(&repository, "damaged_epoch_event", "workspace_epoch");
        let db = Connection::open(&repository.database_path).unwrap();
        db.execute_batch(damage).unwrap();
        let schema = db.query_row("SELECT COALESCE(group_concat(sql,';'),'') FROM sqlite_master WHERE name='runtime_event_journal_identity'",[],|r|r.get::<_,String>(0)).unwrap();
        let identity = || -> Option<Vec<(i64,String,String)>> {
            let mut s=db.prepare("SELECT singleton,typeof(epoch),quote(epoch) FROM runtime_event_journal_identity ORDER BY singleton").ok()?;
            Some(s.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap())
        };
        let before=identity();
        for _ in 0..2 {
            assert!(repository.initialize_schema().is_err(), "{damage}");
            assert!(matches!(repository.list_operator_events_page_bound(&EventCollection::history(None).unwrap(), None, 1), Err(EventReadError::Runtime(_))), "{damage}");
            assert_eq!(versions(&db), (1..=53).collect::<Vec<_>>());
            assert_eq!(identity(),before);
            assert_eq!(db.query_row("SELECT COALESCE(group_concat(sql,';'),'') FROM sqlite_master WHERE name='runtime_event_journal_identity'",[],|r|r.get::<_,String>(0)).unwrap(),schema);
            assert_eq!(repository.list_runtime_events(None,None,100).unwrap(),vec![event.clone()]);
        }
    }
}

#[test]
fn event_epoch_unmarked_existing_identity_is_not_overwritten_or_adopted() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    schema_52(&repository.database_path);
    let db = Connection::open(&repository.database_path).unwrap();
    db.execute_batch(include_str!("../schema_migration_53.sql"))
        .unwrap();
    let epoch: String = db
        .query_row(
            "SELECT epoch FROM runtime_event_journal_identity",
            [],
            |row| row.get(0),
        )
        .unwrap();
    for _ in 0..2 {
        assert!(repository.initialize_schema().is_err());
        assert!(read_journal_epoch(&db).is_err());
        assert_eq!(
            db.query_row(
                "SELECT epoch FROM runtime_event_journal_identity",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            epoch
        );
        assert!(matches!(
            repository.list_operator_events_page_bound(
                &EventCollection::history(None).unwrap(),
                None,
                1
            ),
            Err(EventReadError::Runtime(_))
        ));
        assert_eq!(versions(&db), (1..=52).collect::<Vec<_>>());
    }
}

#[test]
fn event_epoch_weakened_table_or_view_cannot_adopt_an_original_valid_value() {
    for view in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let event = seed_event(&repository, "structure_epoch", "workspace_epoch");
        let collection = EventCollection::history(None).unwrap();
        let page = repository
            .list_operator_events_page_bound(&collection, None, 1)
            .unwrap();
        let cursor = EventCursor::for_event(
            &page.epoch,
            &collection,
            event.row_id,
            event.seq,
            &event.event_id,
        )
        .unwrap();
        let db = Connection::open(&repository.database_path).unwrap();
        db.execute_batch("DROP TABLE runtime_event_journal_identity")
            .unwrap();
        if view {
            db.execute_batch(&format!(
                "CREATE VIEW runtime_event_journal_identity AS SELECT 1 AS singleton,'{}' AS epoch",
                page.epoch.as_str()
            ))
            .unwrap();
        } else {
            db.execute_batch("CREATE TABLE runtime_event_journal_identity(singleton,epoch)")
                .unwrap();
            db.execute(
                "INSERT INTO runtime_event_journal_identity VALUES(1,?1)",
                [page.epoch.as_str()],
            )
            .unwrap();
        }
        let before: String = db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='runtime_event_journal_identity'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        for _ in 0..2 {
            assert!(repository.initialize_schema().is_err());
            assert!(matches!(
                repository.list_operator_events_page_bound(&collection, Some(&cursor), 1),
                Err(EventReadError::Runtime(_))
            ));
            assert_eq!(
                db.query_row(
                    "SELECT sql FROM sqlite_master WHERE name='runtime_event_journal_identity'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
                before
            );
            assert_eq!(
                db.query_row(
                    "SELECT epoch FROM runtime_event_journal_identity",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
                page.epoch.as_str()
            );
            assert_eq!(
                repository.list_runtime_events(None, None, 100).unwrap(),
                vec![event.clone()]
            );
            assert_eq!(versions(&db), (1..=53).collect::<Vec<_>>());
        }
    }
}

#[test]
fn event_epoch_prepared_copy_rejects_old_cursor_but_unprepared_copy_is_explicitly_indistinguishable(
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let event = seed_event(&repository, "copied_event", "workspace_epoch");
    let collection = EventCollection::history(None).unwrap();
    let page = repository
        .list_operator_events_page_bound(&collection, None, 1)
        .unwrap();
    let cursor = EventCursor::for_event(
        &page.epoch,
        &collection,
        event.row_id,
        event.seq,
        &event.event_id,
    )
    .unwrap();
    // Every connection is closed. These are owned, checkpointed disposable
    // fixtures; never copy an active database or its WAL this way.
    let source_bytes = std::fs::read(&repository.database_path).unwrap();
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(!Path::new(&format!("{}{suffix}", repository.database_path.display())).exists());
    }
    let target = temp.path().join("copy.sqlite3");
    std::fs::copy(&repository.database_path, &target).unwrap();
    let copied = SqliteRuntimeRepository::new(target.clone());
    let mut restarted = child(&target, "copy-initial");
    assert!(restarted.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(receipt_path(&target, "copy-initial")).unwrap(),
        page.epoch.as_str()
    );
    assert!(
        copied
            .list_operator_events_page_bound(&collection, Some(&cursor), 1)
            .unwrap()
            .rows
            .is_empty(),
        "an unprepared byte copy preserves the epoch and cannot be distinguished from restart"
    );
    let before = copied.list_runtime_events(None, None, 100).unwrap();
    let db = Connection::open(&target).unwrap();
    // Test-only preparation of this distinct inactive target. There is no
    // operational restore/activation writer or automatic startup rotation.
    db.execute("UPDATE runtime_event_journal_identity SET epoch=lower(hex(randomblob(16))) WHERE singleton=1",[]).unwrap();
    let prepared_epoch = read_journal_epoch(&db).unwrap();
    assert_ne!(prepared_epoch, page.epoch);
    drop(db);
    let mut restarted = child(&target, "copy-prepared");
    assert!(restarted.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(receipt_path(&target, "copy-prepared")).unwrap(),
        prepared_epoch.as_str()
    );
    assert!(matches!(
        copied.list_operator_events_page_bound(&collection, Some(&cursor), 1),
        Err(EventReadError::Cursor(EventCursorRejection::EpochMismatch))
    ));
    assert_eq!(copied.list_runtime_events(None, None, 100).unwrap(), before);
    assert_eq!(
        std::fs::read(&repository.database_path).unwrap(),
        source_bytes
    );
    assert_ne!(std::fs::read(&target).unwrap(), source_bytes);
}
