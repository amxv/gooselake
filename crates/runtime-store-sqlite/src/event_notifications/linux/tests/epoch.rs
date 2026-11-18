//! Owned disposable SQLite fixtures; no operational epoch rotation API.
use super::*;
use crate::repository_event_cursor::read_journal_epoch;
use runtime_core::{
    BoundEventPage, EventCollection, EventCursor, EventCursorRejection, EventEpoch, EventReadError,
};
use rusqlite::{params, OpenFlags};
use std::fmt::Debug;
use std::sync::mpsc;
mod races;

fn global() -> EventCollection {
    EventCollection::stream(None).unwrap()
}
fn writer(fixture: &Fixture) -> Connection {
    let db = Connection::open_with_flags(
        &fixture.path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        db.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    db
}
fn different_epoch(original: &EventEpoch) -> EventEpoch {
    let first = "11111111111111111111111111111111";
    let second = "22222222222222222222222222222222";
    EventEpoch::from_persisted(if original.as_str() == first {
        second
    } else {
        first
    })
    .unwrap()
}
fn set_epoch(db: &Connection, epoch: &EventEpoch) {
    assert_eq!(
        db.execute(
            "UPDATE runtime_event_journal_identity SET epoch=?1 WHERE singleton=1",
            [epoch.as_str()]
        )
        .unwrap(),
        1
    );
}
fn cursor(
    epoch: &EventEpoch,
    collection: &EventCollection,
    row: &RuntimeEventRecord,
    event_id: &str,
) -> EventCursor {
    EventCursor::for_event(epoch, collection, row.row_id, row.seq, event_id).unwrap()
}
fn assert_runtime<T: Debug>(result: Result<T, EventReadError>, code: &str) {
    let error = result.expect_err("an unverified page must not escape");
    assert!(matches!(&error, EventReadError::Runtime(_)), "{error:?}");
    assert!(error.to_string().contains(code), "{error}");
}
fn assert_cursor<T: Debug>(result: Result<T, EventReadError>, expected: EventCursorRejection) {
    match result.expect_err("invalid client cursor must be rejected") {
        EventReadError::Cursor(actual) => assert_eq!(actual, expected),
        other => panic!("expected request-local rejection, got {other:?}"),
    }
}
fn assert_sticky(
    observer: &Arc<EventObserver>,
    subscription: &EventCommitSubscription,
    control: &TestControl,
    code: &str,
) {
    let failure = control.wait_failure();
    assert_eq!(failure.code, code);
    assert!(!failure.message.contains("test gate"), "{failure:?}");
    assert_eq!(
        *subscription.health.borrow(),
        EventObserverHealth::Failed(failure.clone())
    );
    let late = observer.subscribe();
    assert_eq!(*late.health.borrow(), EventObserverHealth::Failed(failure));
    assert_runtime(
        late.replay.list_runtime_events_bound(&global(), None, 8),
        code,
    );
    let legacy = late
        .replay
        .list_runtime_events(None, None, 8)
        .expect_err("legacy replay retains failed generation");
    assert!(legacy.to_string().contains(code), "{legacy}");
    assert_eq!(control.starts(), 1);
}
fn on_thread<T: Send + 'static>(
    read: impl FnOnce() -> T + Send + 'static,
) -> (mpsc::Receiver<T>, std::thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _ = tx.send(read());
    });
    (rx, thread)
}
fn finish<T>(rx: mpsc::Receiver<T>, thread: std::thread::JoinHandle<()>) -> T {
    let result = rx.recv_timeout(Duration::from_secs(4)).unwrap();
    thread.join().unwrap();
    result
}
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

#[test]
fn committed_same_head_epoch_change_is_sticky_without_control_wakes() {
    let fixture = Fixture::new();
    let original_row = fixture.seed("same_head");
    let db = writer(&fixture);
    let original_epoch = read_journal_epoch(&db).unwrap();
    let changed_epoch = different_epoch(&original_epoch);
    let (observer, mut subscription, control) = fixture.idle_observer();
    set_epoch(&db, &changed_epoch);
    assert_sticky(
        &observer,
        &subscription,
        &control,
        "journal_generation_changed",
    );
    assert_eq!(control.control_wakes(), 0);
    assert_eq!(control.publications(), 0);
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(read_journal_epoch(&db).unwrap(), changed_epoch);
    assert_eq!(
        fixture
            .repository
            .list_runtime_events(None, None, 8)
            .unwrap(),
        vec![original_row]
    );
    set_epoch(&db, &original_epoch);
    assert_sticky(
        &observer,
        &subscription,
        &control,
        "journal_generation_changed",
    );
    drop((subscription, observer));
    control.wait_exited();
}

#[test]
fn spilled_uncommitted_epoch_change_rolls_back_without_failure_or_commit_hint() {
    let fixture = Fixture::new();
    let original_row = fixture.seed("rollback_epoch");
    let db = writer(&fixture);
    db.execute_batch("CREATE TABLE epoch_spill_fixture(payload BLOB NOT NULL)")
        .unwrap();
    let original_epoch = read_journal_epoch(&db).unwrap();
    let (observer, mut subscription, control) = fixture.idle_observer();
    let busy_before = control.busy_encounters();
    let reads_before = observer.observation_counters().high_water_reads;
    db.execute_batch(
        "PRAGMA cache_size=1; PRAGMA cache_spill=ON; PRAGMA wal_autocheckpoint=0; BEGIN IMMEDIATE;",
    )
    .unwrap();
    cache_spills(&db, true);
    set_epoch(&db, &different_epoch(&original_epoch));
    db.execute(
        "INSERT INTO epoch_spill_fixture(payload) VALUES(zeroblob(524288))",
        [],
    )
    .unwrap();
    assert!(
        cache_spills(&db, false) > 0,
        "held transaction must emit actual precommit WAL writes"
    );
    control.wait_busy_after(busy_before);
    assert_eq!(control.control_wakes(), 0);
    assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
    assert_eq!(
        observer.observation_counters().high_water_reads,
        reads_before,
        "observer still behind writer barrier"
    );
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    db.execute_batch("ROLLBACK").unwrap();
    assert_no_commit(&mut subscription, &control);
    assert!(observer.observation_counters().high_water_reads > reads_before);
    assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
    assert_eq!(read_journal_epoch(&db).unwrap(), original_epoch);
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM epoch_spill_fixture", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let page = subscription
        .replay
        .list_runtime_events_bound(&global(), None, 8)
        .unwrap();
    assert_eq!(page.epoch, original_epoch);
    assert_eq!(page.rows, vec![original_row]);
    drop((subscription, observer));
    control.wait_exited();
}

#[test]
fn missing_required_epoch_metadata_fails_and_reads_never_repair_it() {
    for damage in [
        "DELETE FROM runtime_event_journal_identity",
        "DROP TABLE runtime_event_journal_identity",
        "DELETE FROM schema_migrations WHERE version=53",
    ] {
        let fixture = Fixture::new();
        let original_row = fixture.seed("missing_metadata");
        let db = writer(&fixture);
        let (observer, mut subscription, control) = fixture.idle_observer();
        db.execute_batch(damage).unwrap();
        assert_sticky(
            &observer,
            &subscription,
            &control,
            "event_observer_unavailable",
        );
        assert!(read_journal_epoch(&db).is_err(), "{damage}");
        assert_eq!(control.control_wakes(), 0);
        assert_eq!(control.publications(), 0);
        assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(
            fixture
                .repository
                .list_runtime_events(None, None, 8)
                .unwrap(),
            vec![original_row]
        );
        drop((subscription, observer));
        control.wait_exited();
    }
}

#[test]
fn rejected_client_cursors_do_not_fail_a_second_shared_subscriber() {
    let fixture = Fixture::new();
    let anchor = fixture.seed("client_anchor");
    let (observer, first, control) = fixture.idle_observer();
    let mut second = observer.subscribe();
    let collection = global();
    let initial = first
        .replay
        .list_runtime_events_bound(&collection, None, 1)
        .unwrap();
    assert_eq!(initial.rows, vec![anchor.clone()]);
    let good = cursor(&initial.epoch, &collection, &anchor, &anchor.event_id);
    let other_collection =
        EventCollection::stream(Some((RuntimeEventScope::System, "elsewhere".into()))).unwrap();
    for (bad, expected) in [
        (
            cursor(
                &different_epoch(&initial.epoch),
                &collection,
                &anchor,
                &anchor.event_id,
            ),
            EventCursorRejection::EpochMismatch,
        ),
        (
            cursor(&initial.epoch, &collection, &anchor, "different_event"),
            EventCursorRejection::AnchorMismatch,
        ),
        (
            cursor(&initial.epoch, &other_collection, &anchor, &anchor.event_id),
            EventCursorRejection::CollectionMismatch,
        ),
    ] {
        assert_cursor(
            first
                .replay
                .list_runtime_events_bound(&collection, Some(&bad), 8),
            expected,
        );
        assert_eq!(*first.health.borrow(), EventObserverHealth::Ready);
        assert_eq!(*second.health.borrow(), EventObserverHealth::Ready);
    }
    let tail = fixture.seed("after_client_rejections");
    wait_commit(&mut second, &control);
    let page = second
        .replay
        .list_runtime_events_bound(&collection, Some(&good), 8)
        .unwrap();
    assert_eq!(page.epoch, initial.epoch);
    assert_eq!(page.rows, vec![tail]);
    assert_eq!(control.starts(), 1);
    drop((first, second, observer));
    control.wait_exited();
}
