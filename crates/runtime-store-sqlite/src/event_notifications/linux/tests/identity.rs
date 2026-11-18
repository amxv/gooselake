use super::*;
use std::sync::mpsc;

fn open_on_thread(
    path: PathBuf,
) -> (
    mpsc::Receiver<Result<Arc<EventObserver>, Failure>>,
    std::thread::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _ = tx.send(observer_for(&path));
    });
    (rx, thread)
}

fn replace(a: &Path, b: &Path, saved: &Path) {
    std::fs::rename(a, saved).unwrap();
    std::fs::rename(b, a).unwrap();
}

fn restore(a: &Path, b: &Path, saved: &Path) {
    std::fs::rename(a, b).unwrap();
    std::fs::rename(saved, a).unwrap();
}

#[test]
fn initialization_rejects_main_file_aba_at_each_watched_handoff() {
    for phase in [
        Phase::AfterDirectoryWatch,
        Phase::BeforeOpen,
        Phase::AfterOpen,
    ] {
        let a = Fixture::new();
        a.seed("original");
        let b = Fixture::new();
        b.seed("replacement");
        let saved = a.root.path().join("saved-main");
        let control = test_control::install(&a.path);
        let pause = control.arm(phase);
        let (result, thread) = open_on_thread(a.path.clone());
        pause.wait_reached();
        replace(&a.path, &b.path, &saved);
        restore(&a.path, &b.path, &saved);
        pause.release();
        let failure = result
            .recv_timeout(Duration::from_secs(4))
            .unwrap()
            .unwrap_err();
        thread.join().unwrap();
        assert_eq!(
            failure.code, "journal_generation_changed",
            "{phase:?}: {failure:?}"
        );
        assert!(!failure.message.contains("test gate"));
    }
}

#[test]
fn opened_replacement_handle_is_rejected_after_path_restores_original_inode() {
    let a = Fixture::new();
    a.seed("original");
    let b = Fixture::new();
    b.seed("replacement");
    let saved = a.root.path().join("saved-main");
    let control = test_control::install(&a.path);
    let before = control.arm(Phase::BeforeSqliteOpen);
    let (result, thread) = open_on_thread(a.path.clone());
    before.wait_reached();
    replace(&a.path, &b.path, &saved);
    let opened = before.release_and_arm(Phase::AfterSqliteOpen);
    opened.wait_reached();
    // SQLite has opened B. Restore A before pathname/HAS_MOVED checks, so a
    // metadata-only pin would certify the wrong resident SQLite connection.
    restore(&a.path, &b.path, &saved);
    opened.release();
    let failure = result
        .recv_timeout(Duration::from_secs(4))
        .unwrap()
        .unwrap_err();
    thread.join().unwrap();
    assert_eq!(failure.code, "journal_generation_changed");
    assert!(
        failure
            .message
            .contains("opened event database moved or was replaced"),
        "{failure:?}"
    );
}

#[test]
fn directory_watch_remains_bound_to_the_pinned_parent_through_move_and_restore() {
    let fixture = Fixture::new();
    let parent = fixture.path.parent().unwrap().to_path_buf();
    let saved = fixture.root.path().join("saved-directory");
    let control = test_control::install(&fixture.path);
    let pause = control.arm(Phase::AfterDirectoryWatch);
    let (result, thread) = open_on_thread(fixture.path.clone());
    pause.wait_reached();
    std::fs::rename(&parent, &saved).unwrap();
    std::fs::rename(&saved, &parent).unwrap();
    pause.release();
    let failure = result
        .recv_timeout(Duration::from_secs(4))
        .unwrap()
        .unwrap_err();
    thread.join().unwrap();
    assert_eq!(failure.code, "journal_generation_changed");
}

#[test]
fn replay_rejects_replacement_with_lower_equal_or_higher_ids_and_aba() {
    for replacement_rows in [1, 2, 3] {
        for restore_original in [false, true] {
            let a = Fixture::new();
            a.seed("a1");
            a.seed("a2");
            let b = Fixture::new();
            for i in 0..replacement_rows {
                b.seed(&format!("b{i}"));
            }
            let (_observer, subscription, control) = a.idle_observer();
            let pause = control.arm(Phase::AfterReplay);
            let reader = subscription.replay.clone();
            let (tx, rx) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let _ = tx.send(reader.list_runtime_events(None, Some(0), 10));
            });
            pause.wait_reached();
            let saved = a.root.path().join("saved-main");
            replace(&a.path, &b.path, &saved);
            if restore_original {
                restore(&a.path, &b.path, &saved);
            }
            pause.release();
            let error = rx
                .recv_timeout(Duration::from_secs(4))
                .unwrap()
                .expect_err("no page may escape an unverified generation");
            thread.join().unwrap();
            assert!(
                error.to_string().contains("journal_generation_changed"),
                "{error}"
            );
            assert_eq!(control.wait_failure().code, "journal_generation_changed");
            assert!(subscription
                .replay
                .list_runtime_events(None, None, 10)
                .is_err());
        }
    }
}

#[test]
fn missing_path_is_never_recreated_and_aliases_share_one_observer() {
    let fixture = Fixture::new();
    let (observer, subscription, control) = fixture.idle_observer();
    let alias = fixture.root.path().join("database-alias");
    std::os::unix::fs::symlink(&fixture.path, &alias).unwrap();
    let symlink = observer_for(&alias).unwrap();
    assert!(Arc::ptr_eq(&observer, &symlink));
    let mut relative = PathBuf::new();
    for component in std::env::current_dir().unwrap().components() {
        if matches!(component, std::path::Component::Normal(_)) {
            relative.push("..");
        }
    }
    relative.push(fixture.path.strip_prefix("/").unwrap());
    let relative_observer = observer_for(&relative).unwrap();
    assert!(Arc::ptr_eq(&observer, &relative_observer));
    assert_eq!(control.starts(), 1);
    std::fs::rename(&fixture.path, fixture.root.path().join("retained-main")).unwrap();
    assert_eq!(control.wait_failure().code, "journal_generation_changed");
    assert!(subscription
        .replay
        .list_runtime_events(None, None, 8)
        .is_err());
    assert!(fixture.store().subscribe_event_commits().is_err());
    assert!(!fixture.path.exists());
    drop(relative_observer);
    drop(symlink);
    drop(subscription);
    drop(observer);
    control.wait_exited();
}

#[test]
fn hardlinks_are_rejected_initially_and_transient_topology_change_is_sticky() {
    let fixture = Fixture::new();
    let link = fixture.root.path().join("hardlink");
    std::fs::hard_link(&fixture.path, &link).unwrap();
    let store = fixture.store();
    assert!(store
        .subscribe_event_commits()
        .err()
        .unwrap()
        .to_string()
        .contains("exactly one hard link"));
    std::fs::remove_file(&link).unwrap();
    assert!(
        store.subscribe_event_commits().is_err(),
        "ordinary startup failure remains cached"
    );
    let (observer, subscription, control, pause) = fixture.parked_observer();
    std::fs::hard_link(&fixture.path, &link).unwrap();
    std::fs::remove_file(&link).unwrap();
    pause.release();
    let failure = control.wait_failure();
    assert_eq!(failure.code, "journal_generation_changed");
    assert!(failure.message.contains("attributes or hard-link topology"));
    assert!(subscription
        .replay
        .list_runtime_events(None, None, 8)
        .is_err());
    assert_eq!(
        *observer.subscribe().health.borrow(),
        EventObserverHealth::Failed(failure)
    );
}

#[test]
fn startup_does_not_create_missing_database_or_change_non_wal_mode() {
    let fixture = Fixture::new();
    let missing = fixture.root.path().join("never-created.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: missing.clone(),
    });
    assert!(store.subscribe_event_commits().is_err());
    assert!(!missing.exists());
    {
        let db = Connection::open(&fixture.path).unwrap();
        let mode: String = db
            .query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "delete");
    }
    let before = std::fs::read(&fixture.path).unwrap();
    assert!(fixture
        .store()
        .subscribe_event_commits()
        .err()
        .unwrap()
        .to_string()
        .contains("existing WAL database"));
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
    let db = Connection::open(&fixture.path).unwrap();
    let mode: String = db
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
}

#[test]
fn closing_o_path_pin_preserves_another_connection_main_file_record_lock() {
    let fixture = Fixture::new();
    let db = Connection::open(&fixture.path).unwrap();
    db.execute_batch("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE;")
        .unwrap();
    let mut baseline = fixture.writer("lock_probe", "before_pin");
    baseline.wait("busy");
    baseline.finish();
    let pin = DirectoryPin::open(&fixture.path)
        .unwrap()
        .pin_database()
        .unwrap();
    drop(pin);
    let mut after = fixture.writer("lock_probe", "after_pin");
    after.wait("busy");
    after.finish();
    db.execute_batch("ROLLBACK").unwrap();
}
