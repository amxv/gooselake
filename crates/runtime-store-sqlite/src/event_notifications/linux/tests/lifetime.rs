use super::*;
use std::sync::mpsc;

#[test]
fn descriptor_failure_and_worker_exit_retain_failure_for_late_subscribers() {
    for fault in [TestFault::InvalidDescriptor, TestFault::ExitWorker] {
        let fixture = Fixture::new();
        let (observer, mut subscription, control, pause) = fixture.parked_observer();
        control.fault_at_next_poll(fault);
        pause.release();
        let failure = control.wait_failure();
        assert_eq!(failure.code, "event_observer_unavailable");
        let expected = match fault {
            TestFault::InvalidDescriptor => "descriptor failed",
            TestFault::ExitWorker => "worker exited unexpectedly",
        };
        assert!(failure.message.contains(expected), "{failure:?}");
        assert_eq!(control.control_wakes(), 0);
        assert_eq!(
            *subscription.health.borrow(),
            EventObserverHealth::Failed(failure.clone())
        );
        assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
        let late = observer.subscribe();
        assert_eq!(*late.health.borrow(), EventObserverHealth::Failed(failure));
        assert!(late.replay.list_runtime_events(None, None, 8).is_err());
        drop(late);
        drop(subscription);
        drop(observer);
        control.wait_exited();
    }
}

#[test]
fn store_can_precede_schema_and_subscription_lease_outlives_the_store_without_a_reactor() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("runtime.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    assert!(!path.exists());
    let repository = SqliteRuntimeRepository::new(path);
    repository.initialize_schema().unwrap();
    let mut subscription = store.subscribe_event_commits().unwrap();
    let control = store.event_observer_test_control();
    control.wait_ready();
    assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
    drop(store);
    let row = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "after_store_drop".into(),
            scope: RuntimeEventScope::System,
            scope_id: "lease".into(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "synthetic.lease".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({}),
            provider: None,
            provider_seq: None,
            created_at: 1,
        })
        .unwrap();
    wait_commit(&mut subscription, &control);
    assert_eq!(
        subscription
            .replay
            .list_runtime_events(None, None, 8)
            .unwrap(),
        vec![row]
    );
    drop(subscription);
    control.wait_exited();
}

#[test]
fn concurrent_shared_and_independent_stores_install_one_generation_and_worker() {
    for independent in [false, true] {
        let fixture = Fixture::new();
        let shared_store = Arc::new(fixture.store());
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let threads = (0..8)
            .map(|_| {
                let store = if independent {
                    Arc::new(fixture.store())
                } else {
                    shared_store.clone()
                };
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let subscription = store.subscribe_event_commits().unwrap();
                    (store, subscription)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let subscriptions = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        let control = subscriptions[0].0.event_observer_test_control();
        control.wait_ready();
        for (_, subscription) in &subscriptions {
            assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
            assert!(Arc::ptr_eq(
                &subscription.replay,
                &subscriptions[0].1.replay
            ));
        }
        assert_eq!(control.starts(), 1);
        drop(subscriptions);
        drop(shared_store);
        control.wait_exited();
    }
}

#[test]
fn cancelled_blocking_constructor_releases_its_unobserved_subscription() {
    let fixture = Fixture::new();
    let control = test_control::install(&fixture.path);
    let pause = control.arm(Phase::BeforeSqliteOpen);
    let store = Arc::new(fixture.store());
    let captured = store.clone();
    let (tx, rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let constructor = std::thread::spawn(move || {
        let subscription = captured.subscribe_event_commits().unwrap();
        // A cancelled awaiting task no longer owns the result receiver. Both
        // the eventual result and captured store must release their leases.
        drop(tx.send(subscription));
        drop(captured);
        done_tx.send(()).unwrap();
    });
    pause.wait_reached();
    drop(rx);
    drop(store);
    pause.release();
    done_rx.recv_timeout(Duration::from_secs(4)).unwrap();
    constructor.join().unwrap();
    control.wait_exited();
}
