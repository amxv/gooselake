use super::*;
use std::sync::mpsc;

fn park_quiet_after_checkpoint(
    fixture: &Fixture,
    observer: &Arc<EventObserver>,
    control: &Arc<TestControl>,
    label: &str,
) -> test_control::PauseGuard {
    let mut pause = control.arm(Phase::BeforeWait);
    // An unrelated real filesystem event brings an idle worker to the gate.
    // It is fully consumed before the later positive raw-writer window.
    std::fs::write(
        fixture
            .path
            .parent()
            .unwrap()
            .join(format!("{label}-noise")),
        b"noise",
    )
    .unwrap();
    pause.wait_reached();
    for _ in 0..3 {
        let mut fds = [
            observer.shared.watch_fds[0],
            observer.shared.watch_fds[1],
            observer.shared.wake.fd(),
        ]
        .map(|fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        });
        let pending = unsafe { libc::poll(fds.as_mut_ptr(), 3, 0) };
        assert!(pending >= 0);
        if pending == 0 {
            assert_eq!(*observer.shared.health.borrow(), EventObserverHealth::Ready);
            assert!(!observer.shared.state.lock().unwrap().dirty);
            return pause;
        }
        // A settle ACK may precede its eventfd drain. Let the real worker
        // consume that activity and atomically catch its next idle boundary.
        pause = pause.release_and_arm(Phase::BeforeWait);
        pause.wait_reached();
    }
    panic!("checkpoint observer did not reach an empty bounded idle window");
}

#[test]
fn writes_between_watch_open_and_baseline_are_in_initial_replay() {
    for phase in [
        Phase::AfterDirectoryWatch,
        Phase::AfterOpen,
        Phase::BeforeCycle,
    ] {
        let fixture = Fixture::new();
        let control = test_control::install(&fixture.path);
        let pause = control.arm(phase);
        let (tx, rx) = mpsc::channel();
        let path = fixture.path.clone();
        let constructor = std::thread::spawn(move || {
            let observer = observer_for(&path).unwrap();
            let subscription = observer.subscribe();
            let _ = tx.send((observer, subscription));
        });
        pause.wait_reached();
        let mut child = fixture.writer("commit_exit", "before_baseline");
        child.wait("held");
        child.allow("release");
        child.wait("committed");
        child.finish();
        pause.release();
        let (_observer, mut subscription) = rx.recv_timeout(Duration::from_secs(4)).unwrap();
        constructor.join().unwrap();
        control.wait_ready();
        assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
        assert_eq!(control.control_wakes(), 0);
        let rows = subscription
            .replay
            .list_runtime_events(None, None, 8)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "raw_before_baseline");
        assert_eq!(rows[0].row_id, 1);
        // A baseline includes existing rows without inventing an advanced-head
        // notification. Subscription-before-replay supplies the initial page.
        assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    }
}

#[test]
fn reader_consumed_precommit_hint_reaches_worker_through_dirty_relay() {
    let fixture = Fixture::new();
    let (observer, mut subscription, control, pause) = fixture.parked_observer();
    let previous = control.busy_encounters();
    let mut child = fixture.writer("commit_open", "reader_relay");
    child.wait("held");
    // The parked worker has no State guard. The actual replay reader consumes
    // all precommit inotify bytes, preserves dirty work, and signals eventfd.
    assert!(subscription
        .replay
        .list_runtime_events(None, None, 8)
        .unwrap()
        .is_empty());
    let mut files = observer.shared.watch_fds.map(|fd| libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    });
    assert_eq!(
        unsafe { libc::poll(files.as_mut_ptr(), 2, 0) },
        0,
        "the reader consumed the real filesystem hint queues"
    );
    let mut wake = libc::pollfd {
        fd: observer.shared.wake.fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    assert_eq!(
        unsafe { libc::poll(&mut wake, 1, 0) },
        1,
        "only the reader's relay is pending"
    );
    pause.release();
    // COMMIT is still forbidden. Reaching actual BUSY here therefore requires
    // the relayed dirty state; a later commit write cannot rescue the test.
    control.wait_busy_after(previous);
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(control.control_wakes(), 0);
    child.allow("release");
    child.wait("committed");
    wait_commit(&mut subscription, &control);
    assert_eq!(
        subscription
            .replay
            .list_runtime_events(None, None, 8)
            .unwrap()[0]
            .event_id,
        "raw_reader_relay"
    );
    child.allow("exit");
    child.finish();
}

#[test]
fn hints_after_head_and_rollback_are_preserved_for_a_fresh_probe() {
    for phase in [Phase::AfterHead, Phase::AfterRollback] {
        let fixture = Fixture::new();
        let control = test_control::install(&fixture.path);
        let pause = control.arm(phase);
        let observer = observer_for(&fixture.path).unwrap();
        let mut subscription = observer.subscribe();
        pause.wait_reached();
        assert_eq!(observer.observation_counters().high_water_reads, 1);
        let mut child = fixture.writer("commit_exit", "after_head");
        // If the barrier still holds the writer lock, release it and arm the
        // post-rollback point atomically before waiting for the child write.
        let after_rollback = if phase == Phase::AfterHead {
            Some(pause.release_and_arm(Phase::AfterRollback))
        } else {
            None
        };
        if let Some(gate) = &after_rollback {
            gate.wait_reached();
        }
        child.wait("held");
        child.allow("release");
        child.wait("committed");
        child.finish();
        if let Some(gate) = &after_rollback {
            gate.release();
        } else {
            pause.release();
        }
        control.wait_ready();
        wait_commit(&mut subscription, &control);
        assert_eq!(control.control_wakes(), 0);
        assert!(observer.observation_counters().high_water_reads >= 2);
        let rows = subscription
            .replay
            .list_runtime_events(None, None, 8)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "raw_after_head");
    }
}

#[test]
fn coalesced_external_batch_before_poll_replays_exact_ordered_pages() {
    let fixture = Fixture::new();
    let (_observer, mut subscription, control, pause) = fixture.parked_observer();
    let mut child = fixture.writer("burst", "coalesced");
    child.wait("held");
    child.allow("release");
    child.wait("committed");
    pause.release();
    wait_commit(&mut subscription, &control);
    assert_eq!(control.control_wakes(), 0);
    let mut cursor = 0;
    let mut observed = Vec::new();
    loop {
        let rows = subscription
            .replay
            .list_runtime_events(None, Some(cursor), 7)
            .unwrap();
        assert!(rows.len() <= 7);
        if rows.is_empty() {
            break;
        }
        for row in rows {
            assert!(row.row_id > cursor);
            cursor = row.row_id;
            observed.push((row.row_id, row.event_id));
        }
    }
    let expected = (0..48)
        .map(|i| (i + 1, format!("raw_coalesced_{i:03}")))
        .collect::<Vec<_>>();
    assert_eq!(
        observed, expected,
        "independent writer batch has no holes, duplicates, or reordered rows"
    );
    assert_no_commit(&mut subscription, &control);
    child.allow("exit");
    child.finish();
}

#[test]
fn actual_observer_and_replay_counters_stay_idle_after_writes_rollback_and_checkpoints() {
    let fixture = Fixture::new();
    let (observer, mut subscription, control) = fixture.idle_observer();
    let assert_idle = || {
        control.settle();
        let before = observer.observation_counters();
        std::thread::sleep(Duration::from_millis(350));
        assert_eq!(
            observer.observation_counters(),
            before,
            "no idle probes, head reads, or replay queries"
        );
    };
    assert_idle();
    fixture.seed("checkpoint_seed");
    wait_commit(&mut subscription, &control);
    assert_idle();

    let previous = control.busy_encounters();
    let mut child = fixture.writer("rollback_open", "idle_rollback");
    child.wait("held");
    control.wait_busy_after(previous);
    child.allow("release");
    child.wait("rolled_back");
    child.allow("exit");
    child.finish();
    assert_no_commit(&mut subscription, &control);
    assert_idle();

    let db = Connection::open(&fixture.path).unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
        let (busy, _, _): (i64, i64, i64) = db
            .query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(
            busy, 0,
            "checkpoint {mode} must complete on the isolated database"
        );
        assert_no_commit(&mut subscription, &control);
        assert_idle();
        let label = format!("after_{}", mode.to_ascii_lowercase());
        let control_wakes = control.control_wakes();
        let pause = park_quiet_after_checkpoint(&fixture, &observer, &control, &label);
        assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
        let busy_before = control.busy_encounters();
        let mut next = fixture.writer("commit_exit", &label);
        next.wait("held");
        pause.release();
        control.wait_busy_after(busy_before);
        next.allow("release");
        next.wait("committed");
        next.finish();
        wait_commit(&mut subscription, &control);
        assert_eq!(
            control.control_wakes(),
            control_wakes,
            "no test wake may rescue independent post-checkpoint liveness"
        );
        assert_idle();
    }
    let before = observer.observation_counters();
    std::fs::write(
        fixture.path.parent().unwrap().join("unrelated-file"),
        b"noise",
    )
    .unwrap();
    let unrelated =
        SqliteRuntimeRepository::new(fixture.path.parent().unwrap().join("unrelated.sqlite3"));
    unrelated.initialize_schema().unwrap();
    control.settle();
    assert_eq!(
        observer.observation_counters(),
        before,
        "unrelated files and databases do not trigger a journal probe"
    );
}
