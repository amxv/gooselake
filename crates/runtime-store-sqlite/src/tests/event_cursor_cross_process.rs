//! Real OS-process boundaries, not two SQLite handles in one Rust process.
//! This proves committed read-side visibility only: the commit notification
//! broadcast is deliberately process-local and does not wake remote SSE.

use super::*;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::TryRecvError;

const CHILD_DIR: &str = "GOOSELAKE_CROSS_PROCESS_EVENT_FIXTURE_DIR";

fn wait_for(path: &Path) {
    let until = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < until,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn insert_child_row(db: &Connection, id: &str, scope: &str, seq: i64) {
    let payload = if scope == "workspace" {
        serde_json::json!({"message_id":"msg_external","operation_id":"op_external","private":"not for operator summaries"})
    } else {
        serde_json::json!({"unrelated_process":"no derived subjects"})
    };
    db.execute(
        "INSERT INTO runtime_events
         (event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at)
         VALUES (?1,?2,?3,?4,?5,'synthetic.cross_process',1,?6,200)",
        params![
            id,
            scope,
            if scope == "workspace" {
                "ws_external"
            } else {
                "proc_external"
            },
            (scope == "workspace").then_some("turn_external"),
            seq,
            payload.to_string(),
        ],
    )
    .unwrap();
}

/// Invoked only by a child test executable with the private fixture directory.
/// A missing environment variable makes ordinary `cargo test` a harmless no-op.
#[test]
fn child_process_writes_rolled_back_and_committed_events() {
    let Some(dir) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let db = Connection::open(dir.join("runtime.sqlite3")).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL; BEGIN IMMEDIATE;")
        .unwrap();
    insert_child_row(&db, "child_rolled_back", "workspace", 2);
    std::fs::write(dir.join("rollback_pending"), b"ready").unwrap();
    wait_for(&dir.join("allow_rollback"));
    db.execute_batch("ROLLBACK; BEGIN IMMEDIATE;").unwrap();
    std::fs::write(dir.join("rollback_done"), b"ready").unwrap();

    insert_child_row(&db, "child_workspace", "workspace", 2);
    insert_child_row(&db, "child_process", "process", 1);
    std::fs::write(dir.join("commit_pending"), b"ready").unwrap();
    wait_for(&dir.join("allow_commit"));
    db.execute_batch("COMMIT;").unwrap();
    // The parent waits for actual process termination, not for a flag that
    // could be published before the SQLite transaction commits.
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn separate_process_wal_commit_is_readable_from_live_and_restarted_observers() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let seed = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "parent_seed".into(),
            scope: RuntimeEventScope::Workspace,
            scope_id: "ws_external".into(),
            session_id: None,
            team_id: None,
            turn_id: Some("turn_external".into()),
            kind: "synthetic.seed".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"message_id":"msg_external","operation_id":"op_external"}),
            provider: None,
            provider_seq: None,
            created_at: 100,
        })
        .unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    let mut wakeups = store.subscribe_event_commits().unwrap();
    let reader = SqliteRuntimeRepository::new(path.clone());

    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::event_cursor_cross_process::child_process_writes_rolled_back_and_committed_events",
                "--nocapture",
            ])
            .env(CHILD_DIR, temp.path())
            .spawn()
            .expect("spawn independently executing Rust test process"),
    );
    wait_for(&temp.path().join("rollback_pending"));
    assert!(reader
        .list_runtime_events(None, Some(seed.row_id), 8)
        .unwrap()
        .is_empty());
    std::fs::write(temp.path().join("allow_rollback"), b"ok").unwrap();
    wait_for(&temp.path().join("rollback_done"));
    assert!(reader
        .list_runtime_events(None, Some(seed.row_id), 8)
        .unwrap()
        .is_empty());

    wait_for(&temp.path().join("commit_pending"));
    for scope in [
        None,
        Some((RuntimeEventScope::Workspace, "ws_external")),
        Some((RuntimeEventScope::Turn, "turn_external")),
    ] {
        let cursor = if scope == Some((RuntimeEventScope::Workspace, "ws_external")) {
            seed.seq
        } else {
            seed.row_id
        };
        assert!(
            reader
                .list_runtime_events(scope, Some(cursor), 8)
                .unwrap()
                .is_empty(),
            "WAL must not leak another process's uncommitted event"
        );
    }
    assert!(reader
        .list_operator_events_page(None, Some(seed.row_id), 3)
        .unwrap()
        .is_empty());
    std::fs::write(temp.path().join("allow_commit"), b"ok").unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < until, "cross-process writer did not exit");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "child process must commit successfully");

    let global = reader
        .list_runtime_events(None, Some(seed.row_id), 1)
        .unwrap();
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].event_id, "child_workspace");
    assert_eq!(global[0].seq, seed.seq + 1);
    let second = reader
        .list_runtime_events(None, Some(global[0].row_id), 1)
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].event_id, "child_process");
    assert!(global[0].row_id < second[0].row_id);

    // A retained subject link inherits the parent event's global row cursor,
    // while the primary workspace stream retains its own per-scope sequence.
    assert_eq!(
        reader
            .list_runtime_events(
                Some((RuntimeEventScope::Workspace, "ws_external")),
                Some(seed.seq),
                2,
            )
            .unwrap(),
        global,
    );
    for subject in [
        RuntimeEventScope::Turn,
        RuntimeEventScope::Message,
        RuntimeEventScope::Operation,
    ] {
        let id = match subject {
            RuntimeEventScope::Turn => "turn_external",
            RuntimeEventScope::Message => "msg_external",
            _ => "op_external",
        };
        assert_eq!(
            reader
                .list_runtime_events(Some((subject, id)), Some(seed.row_id), 2)
                .unwrap(),
            global,
        );
        assert_eq!(
            reader
                .list_operator_events_page(Some((subject, id)), Some(seed.row_id), 2)
                .unwrap()[0]
                .row_id,
            global[0].row_id,
        );
    }
    let summaries = reader
        .list_operator_events_page(None, Some(seed.row_id), 3)
        .unwrap();
    assert_eq!(
        summaries.iter().map(|e| e.row_id).collect::<Vec<_>>(),
        vec![global[0].row_id, second[0].row_id]
    );
    assert!(!serde_json::to_string(&summaries)
        .unwrap()
        .contains("private"));
    assert!(
        reader
            .list_operator_events_page(
                Some((RuntimeEventScope::Workspace, "ws_external")),
                Some(second[0].row_id),
                2,
            )
            .is_err(),
        "a different scope's observed global cursor is not valid here"
    );

    let restarted = SqliteRuntimeRepository::new(path);
    assert_eq!(
        restarted
            .list_runtime_events(None, Some(seed.row_id), 8)
            .unwrap(),
        vec![global[0].clone(), second[0].clone()],
    );
    assert!(restarted
        .list_runtime_events(None, Some(second[0].row_id), 8)
        .unwrap()
        .is_empty());
    assert!(matches!(wakeups.try_recv(), Err(TryRecvError::Empty)),
        "an OS-external commit has no in-process notification; this test proves reads, not cross-process SSE wakeups");
}
