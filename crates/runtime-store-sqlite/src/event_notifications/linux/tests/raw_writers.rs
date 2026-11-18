use super::*;
use rusqlite::{params, OpenFlags};

fn cache_spills(db: &Connection, reset: bool) -> i32 {
    let mut current = 0;
    let mut high = 0;
    assert_eq!(
        unsafe {
            rusqlite::ffi::sqlite3_db_status(
                db.handle(),
                rusqlite::ffi::SQLITE_DBSTATUS_CACHE_SPILL,
                &mut current,
                &mut high,
                i32::from(reset),
            )
        },
        rusqlite::ffi::SQLITE_OK
    );
    current
}

/// Only an owned test child receives these fixture variables. Coordination
/// files are outside the watched database directory, so they cannot rescue a
/// broken WAL wake path. process::exit deliberately bypasses connection Drop.
#[test]
fn raw_writer_child() {
    let Some(root) = std::env::var_os("GOOSELAKE_N05_RAW_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    assert_eq!(
        std::fs::read(root.join("n05-fixture")).unwrap(),
        b"disposable-offline"
    );
    let mode = std::env::var("GOOSELAKE_N05_RAW_MODE").unwrap();
    let label = std::env::var("GOOSELAKE_N05_RAW_LABEL").unwrap();
    let signals = root.join("control").join(&label);
    let db = Connection::open_with_flags(
        root.join("data/runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    if mode == "lock_probe" {
        db.busy_timeout(Duration::ZERO).unwrap();
        let error = db
            .execute_batch("BEGIN IMMEDIATE")
            .expect_err("independent process must encounter the parent's main-file record lock");
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy)
        );
        std::fs::write(signals.join("busy"), b"engine-busy").unwrap();
        return;
    }
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    let mode_now: String = db
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode_now, "wal");
    db.execute_batch(
        "PRAGMA cache_size=1; PRAGMA cache_spill=ON; PRAGMA wal_autocheckpoint=0; BEGIN IMMEDIATE;",
    )
    .unwrap();
    let seq: i64 = db.query_row("SELECT COALESCE(MAX(seq),0)+1 FROM runtime_events WHERE scope='workspace' AND scope_id='ws_raw'", [], |r| r.get(0)).unwrap();
    let burst = mode == "burst";
    let payload = serde_json::json!({"message_id":"msg_raw","operation_id":"op_raw",
        "large": "x".repeat(if burst { 256 } else { 512*1024 })})
    .to_string();
    cache_spills(&db, true);
    for i in 0..if burst { 48 } else { 1 } {
        let event_id = if burst {
            format!("raw_{label}_{i:03}")
        } else {
            format!("raw_{label}")
        };
        db.execute("INSERT INTO runtime_events (event_id,scope,scope_id,turn_id,seq,kind,critical,payload_json,created_at)
            VALUES (?1,'workspace','ws_raw','turn_raw',?2,'synthetic.raw',1,?3,2)",
            params![event_id,seq+i,payload]).unwrap();
    }
    let spills = cache_spills(&db, false);
    assert!(
        spills > 0,
        "this held transaction must perform fresh dirty-page spills"
    );
    assert!(
        std::fs::metadata(root.join("data/runtime.sqlite3-wal"))
            .unwrap()
            .len()
            > 32,
        "fixture must actually spill WAL bytes before commit"
    );
    std::fs::write(
        signals.join("held"),
        format!("fresh_cache_spills={spills}\n"),
    )
    .unwrap();
    wait_file(&signals.join("release"));
    if mode == "rollback_open" {
        db.execute_batch("ROLLBACK").unwrap();
        std::fs::write(signals.join("rolled_back"), b"ok").unwrap();
    } else {
        db.execute_batch("COMMIT").unwrap();
        std::fs::write(signals.join("committed"), b"ok").unwrap();
        if mode == "commit_exit" {
            std::process::exit(0);
        }
    }
    wait_file(&signals.join("exit"));
}

#[test]
fn held_raw_wal_commit_wakes_idle_pinned_replay_while_writer_stays_open() {
    let fixture = Fixture::new();
    let seed = fixture.seed("unrelated_seed");
    let (observer, mut subscription, control) = fixture.idle_observer();
    let previous = control.busy_encounters();
    let reads_before = observer.observation_counters().high_water_reads;
    let mut child = fixture.writer("commit_open", "held_commit");
    child.wait("held");
    let busy_at = control.wait_busy_after(previous);
    std::thread::sleep(Duration::from_millis(250));
    assert!(busy_at.elapsed() < Duration::from_secs(4));
    assert!(
        matches!(subscription.try_recv(), Err(TryRecvError::Empty)),
        "uncommitted WAL frames cannot wake committed replay"
    );
    assert_eq!(
        observer.observation_counters().high_water_reads,
        reads_before,
        "the barrier is waiting behind the real external writer"
    );
    child.allow("release");
    child.wait("committed");
    wait_commit(&mut subscription, &control);
    assert!(
        child.child.try_wait().unwrap().is_none(),
        "writer must still hold its connection open"
    );
    assert_eq!(control.control_wakes(), 0);
    let rows = subscription
        .replay
        .list_runtime_events(None, Some(seed.row_id), 8)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.event_id, "raw_held_commit");
    assert_eq!(row.seq, 1);
    assert!(row.row_id > row.seq);
    for (scope, id, cursor) in [
        (RuntimeEventScope::Workspace, "ws_raw", 0),
        (RuntimeEventScope::Turn, "turn_raw", seed.row_id),
        (RuntimeEventScope::Message, "msg_raw", seed.row_id),
        (RuntimeEventScope::Operation, "op_raw", seed.row_id),
    ] {
        assert_eq!(
            subscription
                .replay
                .list_runtime_events(Some((scope, id)), Some(cursor), 8)
                .unwrap(),
            rows
        );
    }
    child.allow("exit");
    child.finish();
}

#[test]
fn rollback_and_owned_writer_death_do_not_publish_phantom_events() {
    for abandon in [false, true] {
        let fixture = Fixture::new();
        let (_observer, mut subscription, control) = fixture.idle_observer();
        let previous = control.busy_encounters();
        let mut child = fixture.writer("rollback_open", "discarded");
        child.wait("held");
        control.wait_busy_after(previous);
        assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
        if abandon {
            child.kill_owned();
        } else {
            child.allow("release");
            child.wait("rolled_back");
            child.allow("exit");
            child.finish();
        }
        assert_no_commit(&mut subscription, &control);
        assert!(subscription
            .replay
            .list_runtime_events(None, None, 8)
            .unwrap()
            .is_empty());
        // The negative check above is settlement evidence. Independent idle
        // wake proof is supplied by the fresh, zero-control-wake tests.
        let mut next = fixture.writer("commit_exit", "after_discard");
        next.wait("held");
        next.allow("release");
        next.wait("committed");
        next.finish();
        wait_commit(&mut subscription, &control);
        let rows = subscription
            .replay
            .list_runtime_events(None, None, 8)
            .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.event_id.as_str()).collect::<Vec<_>>(),
            vec!["raw_after_discard"]
        );
    }
}

#[test]
fn raw_commit_followed_by_exit_without_destructors_wakes_idle_observer() {
    let fixture = Fixture::new();
    let (_observer, mut subscription, control) = fixture.idle_observer();
    let previous = control.busy_encounters();
    let mut child = fixture.writer("commit_exit", "exit_commit");
    child.wait("held");
    control.wait_busy_after(previous);
    child.allow("release");
    child.wait("committed");
    child.finish();
    wait_commit(&mut subscription, &control);
    assert_eq!(control.control_wakes(), 0);
    let rows = subscription
        .replay
        .list_runtime_events(None, None, 8)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event_id, "raw_exit_commit");
}

#[test]
fn writer_exceeding_busy_bound_fails_existing_and_late_subscribers_stickily() {
    let fixture = Fixture::new();
    let (observer, mut subscription, control) = fixture.idle_observer();
    let previous = control.busy_encounters();
    let mut child = fixture.writer("rollback_open", "busy_bound");
    child.wait("held");
    let busy_at = control.wait_busy_after(previous);
    let failure = control.wait_failure();
    let elapsed = busy_at.elapsed();
    assert!(
        elapsed >= Duration::from_millis(4500) && elapsed < Duration::from_secs(8),
        "configured five-second contention bound was {elapsed:?}"
    );
    assert_eq!(failure.code, "event_observer_barrier_failed");
    assert!(matches!(
        *subscription.health.borrow(),
        EventObserverHealth::Failed(_)
    ));
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    child.allow("release");
    child.wait("rolled_back");
    child.allow("exit");
    child.finish();
    let late = observer.subscribe();
    assert_eq!(*late.health.borrow(), EventObserverHealth::Failed(failure));
    assert!(late.replay.list_runtime_events(None, None, 8).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_does_not_join_busy_worker_and_same_store_can_retry_after_teardown() {
    let fixture = Fixture::new();
    let (observer, subscription, control) = fixture.idle_observer();
    let previous = control.busy_encounters();
    let mut child = fixture.writer("rollback_open", "cancel_busy");
    child.wait("held");
    control.wait_busy_after(previous);
    let started = Instant::now();
    drop(subscription);
    drop(observer);
    tokio::task::yield_now().await;
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "async cancellation waited for SQLite's five-second barrier"
    );
    let retry = fixture.store();
    for _ in 0..8 {
        let error = match retry.subscribe_event_commits() {
            Ok(_) => panic!("a second observer overlapped held teardown"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("event_observer_stopping"),
            "{error}"
        );
    }
    assert_eq!(control.starts(), 1);
    child.allow("release");
    child.wait("rolled_back");
    child.allow("exit");
    child.finish();
    control.wait_exited();
    let mut next = retry
        .subscribe_event_commits()
        .expect("same store did not cache transient stopping");
    tokio::time::timeout(Duration::from_secs(3), next.ready())
        .await
        .expect("replacement observer readiness deadline")
        .unwrap();
    let next_control = retry.event_observer_test_control();
    drop(next);
    drop(retry);
    next_control.wait_exited();
}
