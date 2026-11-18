//! Event-driven Linux local-WAL observation. A short writer barrier closes
//! the gap between WAL writes and shared wal-index commit publication. The
//! observer never polls SQLite while idle and never supplies event payloads.
use std::collections::HashMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::{self, JoinHandle};

use runtime_core::{
    EventCommitSubscription, EventEpoch, EventObservationCounters,
    EventObservationFailure as Failure, EventObserverHealth,
};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use std::time::Duration;
use tokio::sync::{broadcast, watch as health_watch};

use crate::repository_event_cursor::read_journal_epoch;

mod database;
mod replay;
#[cfg(test)]
pub(crate) mod test_control;
#[cfg(test)]
mod tests;
mod watch;
use database::{DatabasePin, DirectoryPin};
#[cfg(test)]
use test_control::{Phase, TestControl, TestFault};
use watch::{Wake, Watch};

fn unavailable(context: &str, error: impl Display) -> Failure {
    Failure::unavailable(format!("{context}: {error}"))
}

#[derive(Debug, Default)]
struct Counters {
    probe_attempts: AtomicU64,
    high_water_reads: AtomicU64,
    replay_reads: AtomicU64,
    write_hint_overflows: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> EventObservationCounters {
        EventObservationCounters {
            probe_attempts: self.probe_attempts.load(Ordering::Acquire),
            high_water_reads: self.high_water_reads.load(Ordering::Acquire),
            replay_reads: self.replay_reads.load(Ordering::Acquire),
            write_hint_overflows: self.write_hint_overflows.load(Ordering::Acquire),
        }
    }
}

/// The connection and actual inotify reads share one guard. In particular,
/// the worker cannot hide a consumed lifecycle record in a private buffer
/// while a replay page independently certifies an empty watcher queue.
#[derive(Debug)]
struct State {
    connection: Connection,
    pin: DatabasePin,
    watch: Watch,
    dirty: bool,
    head: Option<i64>,
    epoch: Option<EventEpoch>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    health: health_watch::Sender<EventObserverHealth>,
    commits: broadcast::Sender<()>,
    wake: Wake,
    stop: AtomicBool,
    counters: Counters,
    watch_fds: [i32; 2],
    #[cfg(test)]
    control: Arc<TestControl>,
    // Keep this last: Rust drops fields in declaration order. The registry
    // reservation must outlive SQLite, both watchers, pins and the wake fd,
    // including the interval after Shared's strong count reaches zero.
    reservation: Arc<Reservation>,
}

#[derive(Debug)]
struct Reservation {
    #[cfg(test)]
    control: Arc<TestControl>,
}

#[cfg(test)]
impl Drop for Reservation {
    fn drop(&mut self) {
        // Shared's resource fields have already been destroyed. The control
        // object owns only a Weak<Shared>, so this cannot keep them alive.
        self.control.exited();
    }
}

impl Shared {
    fn fail(&self, failure: Failure) {
        self.health.send_if_modified(|health| {
            if matches!(health, EventObserverHealth::Failed(_)) {
                return false;
            }
            *health = EventObserverHealth::Failed(failure);
            true
        });
        #[cfg(test)]
        {
            let retained = self.health.borrow().clone();
            if let EventObserverHealth::Failed(failure) = retained {
                self.control.failed(failure);
            }
        }
    }

    fn healthy(&self) -> Result<(), Failure> {
        match self.health.borrow().clone() {
            EventObserverHealth::Failed(failure) => Err(failure),
            _ => Ok(()),
        }
    }

    fn scan(&self, state: &mut State, relay: bool) -> Result<(), Failure> {
        self.healthy()?;
        let result = state.watch.scan().and_then(|scan| {
            state.dirty |= scan.dirty;
            self.counters
                .write_hint_overflows
                .fetch_add(scan.overflows, Ordering::AcqRel);
            if scan.dirty && relay {
                self.wake.signal()?;
            }
            state.pin.verify(&state.connection)
        });
        if let Err(failure) = &result {
            // Publish sticky failure before releasing the connection/queue
            // guard. A second reader cannot miss a consumed fatal event.
            self.fail(failure.clone());
        }
        result
    }
}

#[derive(Debug)]
pub(super) struct EventObserver {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct RegistryEntry {
    observer: Weak<EventObserver>,
    reservation: Weak<Reservation>,
}

/// Registry keys are pinned local inode identities. Canonical symlink/relative
/// aliases share an observer; hard-link aliases are rejected before lookup.
pub(super) fn observer_for(path: &Path) -> Result<Arc<EventObserver>, Failure> {
    static REGISTRY: OnceLock<Mutex<HashMap<(u64, u64), RegistryEntry>>> = OnceLock::new();
    let directory = DirectoryPin::open(path)?;
    #[cfg(test)]
    let control = test_control::for_path(&directory.path);
    let mut watch = Watch::new(directory.fd(), &directory.name)?;
    #[cfg(test)]
    control.checkpoint(Phase::AfterDirectoryWatch)?;
    let pin = directory.pin_database()?;
    watch.watch_inode(pin.fd())?;
    let first_scan = watch.scan()?;
    pin.verify_path()?;
    let key = pin.identity();
    let mut registry = REGISTRY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| Failure::unavailable("event observer registry poisoned"))?;
    registry.retain(|_, entry| {
        entry.observer.strong_count() > 0 || entry.reservation.strong_count() > 0
    });
    if let Some(observer) = registry
        .get(&key)
        .and_then(|entry| entry.observer.upgrade())
    {
        {
            let mut state = observer
                .shared
                .state
                .lock()
                .map_err(|_| Failure::unavailable("event observer state poisoned"))?;
            observer.shared.scan(&mut state, true)?;
        }
        return Ok(observer);
    }
    if registry
        .get(&key)
        .is_some_and(|entry| entry.reservation.strong_count() > 0)
    {
        // Final-owner drop must never join a busy SQLite worker on an async
        // executor. Keep its generation reserved until bounded teardown ends,
        // rather than accumulating workers during rapid cancel/resubscribe.
        return Err(Failure {
            code: "event_observer_stopping",
            message: "the previous event observer is completing bounded shutdown; retry subscription after it exits".into(),
        });
    }
    #[cfg(test)]
    control.checkpoint(Phase::BeforeOpen)?;
    #[cfg(not(test))]
    let connection = pin.open_connection()?;
    #[cfg(test)]
    let connection = pin.open_connection(&control)?;
    #[cfg(test)]
    control.checkpoint(Phase::AfterOpen)?;
    let final_scan = watch.scan()?;
    pin.verify(&connection)?;
    let watch_fds = watch.fds();
    let (health, _) = health_watch::channel(EventObserverHealth::Starting);
    let (commits, _) = broadcast::channel(256);
    let observer = Arc::new(EventObserver {
        shared: Arc::new(Shared {
            state: Mutex::new(State {
                connection,
                pin,
                watch,
                dirty: first_scan.dirty || final_scan.dirty,
                head: None,
                epoch: None,
            }),
            health,
            commits,
            wake: Wake::new()?,
            stop: AtomicBool::new(false),
            counters: Counters {
                write_hint_overflows: AtomicU64::new(first_scan.overflows + final_scan.overflows),
                ..Counters::default()
            },
            watch_fds,
            #[cfg(test)]
            control: control.clone(),
            reservation: Arc::new(Reservation {
                #[cfg(test)]
                control,
            }),
        }),
        worker: Mutex::new(None),
    });
    #[cfg(test)]
    observer.shared.control.bind(&observer.shared);
    registry.insert(
        key,
        RegistryEntry {
            observer: Arc::downgrade(&observer),
            reservation: Arc::downgrade(&observer.shared.reservation),
        },
    );
    Ok(observer)
}

impl EventObserver {
    #[cfg(test)]
    pub(super) fn test_control(&self) -> Arc<TestControl> {
        self.shared.control.clone()
    }

    pub(super) fn subscribe(self: &Arc<Self>) -> EventCommitSubscription {
        // Register the first receiver before starting the baseline. Subsequent
        // subscribers also register before their own initial indexed replay.
        let subscription = EventCommitSubscription {
            commits: self.shared.commits.subscribe(),
            health: self.shared.health.subscribe(),
            replay: self.clone(),
        };
        match self.worker.lock() {
            Ok(mut worker) => {
                if worker.is_none()
                    && matches!(*self.shared.health.borrow(), EventObserverHealth::Starting)
                {
                    let shared = self.shared.clone();
                    match thread::Builder::new()
                        .name("gg-event-observer".into())
                        .spawn(move || {
                            #[cfg(test)]
                            shared.control.started();
                            let outcome =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    run(&shared)
                                }));
                            match outcome {
                                Ok(Ok(())) => {}
                                Ok(Err(failure)) => shared.fail(failure),
                                Err(_) => shared.fail(Failure::unavailable(
                                    "event observer worker exited unexpectedly",
                                )),
                            }
                        }) {
                        Ok(handle) => *worker = Some(handle),
                        Err(error) => self
                            .shared
                            .fail(unavailable("start event observer worker", error)),
                    }
                }
            }
            Err(_) => self
                .shared
                .fail(Failure::unavailable("event observer worker state poisoned")),
        }
        subscription
    }
}

impl Drop for EventObserver {
    fn drop(&mut self) {
        // The thread owns Shared, never EventObserver, so the final store or
        // replay lease can stop it. SQLite contention has a finite busy limit.
        // Dropping the JoinHandle detaches: cancellation must not block an
        // async executor. Shared retains the resources and registry reservation
        // until the worker exits, so a new worker cannot overlap this one.
        self.shared.stop.store(true, Ordering::Release);
        #[cfg(test)]
        self.shared.control.cancel_gates();
        let _ = self.shared.wake.signal();
    }
}

fn verified_epoch(
    connection: &Connection,
    expected: Option<&EventEpoch>,
) -> Result<EventEpoch, Failure> {
    let epoch = read_journal_epoch(connection)
        .map_err(|error| unavailable("read required journal epoch", error))?;
    if expected.is_some_and(|expected| expected != &epoch) {
        return Err(Failure::generation_changed(
            "persisted journal epoch changed",
        ));
    }
    Ok(epoch)
}

fn begin_barrier<'a>(
    connection: &'a Connection,
    _shared: &Shared,
) -> Result<Transaction<'a>, Failure> {
    // Separate immediate admission from a bounded contention wait. The first
    // BUSY is an actual SQLite lock result, not an inference from WAL hints.
    // State serializes all use of this resident connection; successful
    // transactions retain RAII rollback on every subsequent error path.
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(|error| unavailable("configure immediate barrier admission", error))?;
    let first = Transaction::new_unchecked(connection, TransactionBehavior::Immediate);
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| unavailable("restore bounded event barrier wait", error))?;
    let result = match first {
        Ok(transaction) => Ok(transaction),
        Err(error) if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) => {
            #[cfg(test)]
            _shared.control.busy_encountered();
            Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
        }
        Err(error) => Err(error),
    };
    result.map_err(|error| Failure {
        code: "event_observer_barrier_failed",
        message: format!(
            "SQLite writer barrier unavailable with a five-second busy bound: {error}"
        ),
    })
}

fn probe(shared: &Shared, state: &mut State) -> Result<bool, Failure> {
    state.pin.verify(&state.connection)?;
    shared
        .counters
        .probe_attempts
        .fetch_add(1, Ordering::AcqRel);
    #[cfg(test)]
    shared.control.checkpoint(Phase::BeforeProbe)?;
    let tx = begin_barrier(&state.connection, shared)?;
    shared
        .counters
        .high_water_reads
        .fetch_add(1, Ordering::AcqRel);
    let head: i64 = tx
        .query_row("SELECT COALESCE(MAX(id), 0) FROM runtime_events", [], |r| {
            r.get(0)
        })
        .map_err(|error| unavailable("read committed event high-water", error))?;
    let epoch = verified_epoch(&tx, state.epoch.as_ref())?;
    #[cfg(test)]
    shared.control.checkpoint(Phase::AfterHead)?;
    // The RAII transaction also rolls back on error/panic. This empty writer
    // barrier never adds a journal row or commits a second event authority.
    tx.rollback()
        .map_err(|error| unavailable("release event observer barrier", error))?;
    #[cfg(test)]
    shared.control.checkpoint(Phase::AfterRollback)?;
    state.pin.verify(&state.connection)?;
    if head < 0 || state.head.is_some_and(|previous| head < previous) {
        return Err(Failure::generation_changed(
            "committed event high-water regressed",
        ));
    }
    let advanced = state.head.is_some_and(|previous| head > previous);
    state.head = Some(head);
    state.epoch = Some(epoch);
    Ok(advanced)
}

fn run(shared: &Shared) -> Result<(), Failure> {
    loop {
        if shared.stop.load(Ordering::Acquire) {
            return Ok(());
        }
        #[cfg(test)]
        shared.control.checkpoint(Phase::BeforeCycle)?;
        #[cfg(test)]
        let settle_ticket = shared.control.requested();
        {
            let mut state = shared
                .state
                .lock()
                .map_err(|_| Failure::unavailable("event observer state poisoned"))?;
            shared.scan(&mut state, false)?;
            if state.head.is_none() || state.dirty {
                // Claim only the hints already scanned. Post-probe events
                // remain dirty and must be handled before returning to sleep.
                state.dirty = false;
                let advanced = match probe(shared, &mut state) {
                    Ok(advanced) => advanced,
                    Err(failure) => {
                        shared.fail(failure.clone());
                        return Err(failure);
                    }
                };
                shared.scan(&mut state, false)?;
                if advanced {
                    let _ = shared.commits.send(());
                    #[cfg(test)]
                    shared.control.published();
                }
                shared.health.send_if_modified(|health| {
                    if !matches!(health, EventObserverHealth::Starting) {
                        return false;
                    }
                    *health = EventObserverHealth::Ready;
                    true
                });
                #[cfg(test)]
                shared.control.readied();
                if state.dirty {
                    continue;
                }
            }
            #[cfg(test)]
            shared.control.acknowledge(settle_ticket);
        }
        if shared.stop.load(Ordering::Acquire) {
            return Ok(());
        }
        #[cfg(test)]
        shared.control.checkpoint(Phase::BeforeWait)?;
        let mut fds =
            [shared.watch_fds[0], shared.watch_fds[1], shared.wake.fd()].map(|fd| libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            });
        #[cfg(test)]
        match shared.control.take_fault() {
            Some(TestFault::ExitWorker) => panic!("injected event observer worker exit"),
            Some(TestFault::InvalidDescriptor) => fds[0].fd = i32::MAX,
            None => {}
        }
        let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(unavailable("wait for event database activity", error));
        }
        if fds
            .iter()
            .any(|fd| fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0)
        {
            return Err(Failure::unavailable("event observer descriptor failed"));
        }
        shared.wake.drain()?;
    }
}
