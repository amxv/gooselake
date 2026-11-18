use super::*;
use std::io::Write;

fn queue_limit() -> usize {
    let limit: usize = std::fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        (1..=131072).contains(&limit),
        "fixture queue budget exceeded: {limit}"
    );
    limit
}

#[test]
fn actual_write_hint_queue_overflow_catches_up_without_lost_events() {
    let fixture = Fixture::new();
    let parent = fixture.path.parent().unwrap();
    let mut a = std::fs::File::create(parent.join("noise-a")).unwrap();
    let mut b = std::fs::File::create(parent.join("noise-b")).unwrap();
    let (observer, mut subscription, control, pause) = fixture.parked_observer();
    let limit = queue_limit();
    // Distinct alternating names prevent inotify from coalescing successive
    // identical MODIFY records. Existing files leave lifecycle evidence quiet.
    for i in 0..limit + 1024 {
        if i % 2 == 0 {
            a.write_all(b"x").unwrap();
        } else {
            b.write_all(b"x").unwrap();
        }
    }
    let mut child = fixture.writer("commit_exit", "overflow_commit");
    child.wait("held");
    child.allow("release");
    child.wait("committed");
    child.finish();
    pause.release();
    wait_commit(&mut subscription, &control);
    assert_eq!(control.control_wakes(), 0);
    assert!(
        observer.observation_counters().write_hint_overflows > 0,
        "actual kernel overflow must be observed"
    );
    let rows = subscription
        .replay
        .list_runtime_events(None, None, 8)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event_id, "raw_overflow_commit");
    assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
}

#[test]
fn actual_lifecycle_queue_overflow_fails_existing_and_late_subscribers() {
    let fixture = Fixture::new();
    let (observer, subscription, control, pause) = fixture.parked_observer();
    let noise = fixture.path.parent().unwrap().join("transient-noise");
    for _ in 0..queue_limit() / 2 + 1024 {
        std::fs::write(&noise, b"").unwrap();
        std::fs::remove_file(&noise).unwrap();
    }
    pause.release();
    let failure = control.wait_failure();
    assert_eq!(failure.code, "journal_generation_changed");
    assert!(
        failure.message.contains("lifecycle evidence overflowed"),
        "{failure:?}"
    );
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
fn actual_watch_loss_fails_without_a_commit_and_late_health_stays_failed() {
    let fixture = Fixture::new();
    let (observer, subscription, control) = fixture.idle_observer();
    observer
        .shared
        .state
        .lock()
        .unwrap()
        .watch
        .remove_lifecycle_watch();
    let failure = control.wait_failure();
    assert_eq!(failure.code, "journal_generation_changed");
    assert_eq!(control.control_wakes(), 0);
    assert_eq!(
        *subscription.health.borrow(),
        EventObserverHealth::Failed(failure.clone())
    );
    assert_eq!(
        *observer.subscribe().health.borrow(),
        EventObserverHealth::Failed(failure)
    );
}
