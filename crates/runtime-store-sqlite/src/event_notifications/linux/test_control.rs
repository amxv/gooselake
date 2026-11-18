//! Deterministic test handoffs. This module is absent from production builds.
//! Control wakes never mark the journal dirty or manufacture a commit hint.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::{Duration, Instant};

use super::{Failure, Shared};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    AfterDirectoryWatch,
    BeforeOpen,
    AfterOpen,
    BeforeSqliteOpen,
    AfterSqliteOpen,
    BeforeCycle,
    BeforeProbe,
    AfterHead,
    AfterRollback,
    BeforeReplay,
    AfterReplayEpoch,
    AfterReplay,
    BeforeWait,
}

#[derive(Debug)]
struct Pause {
    id: u64,
    phase: Phase,
    reached: bool,
    released: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TestFault {
    ExitWorker,
    InvalidDescriptor,
}

#[derive(Debug, Default)]
struct Signals {
    requested: u64,
    acknowledged: u64,
    next_pause: u64,
    pause: Option<Pause>,
    failure: Option<Failure>,
    exited: bool,
    starts: usize,
    ready: bool,
    publications: u64,
    busy_encounters: u64,
    last_busy: Option<Instant>,
    next_fault: Option<TestFault>,
}

#[derive(Debug, Default)]
pub(crate) struct TestControl {
    owner: OnceLock<Weak<Shared>>,
    signals: Mutex<Signals>,
    changed: Condvar,
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<TestControl>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<TestControl>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// One-shot installation for an isolated, existing fixture database. The weak
/// registry neither retains fixtures nor changes production initialization.
pub(crate) fn install(path: &Path) -> Arc<TestControl> {
    let path = path
        .canonicalize()
        .expect("existing isolated fixture database");
    let control = Arc::new(TestControl::default());
    let mut registry = registry()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    registry.retain(|_, entry| entry.strong_count() > 0);
    if registry.contains_key(&path) {
        drop(registry);
        panic!("a control is already installed for {}", path.display());
    }
    registry.insert(path, Arc::downgrade(&control));
    control
}

pub(super) fn for_path(path: &Path) -> Arc<TestControl> {
    registry()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .remove(path)
        .and_then(|entry| entry.upgrade())
        .unwrap_or_default()
}

impl TestControl {
    /// The caller parks the worker before poll before installing this fault.
    /// No real descriptor is closed or reused by the invalid-descriptor case.
    pub(crate) fn fault_at_next_poll(&self, fault: TestFault) {
        self.lock().next_fault = Some(fault);
    }

    pub(super) fn take_fault(&self) -> Option<TestFault> {
        self.lock().next_fault.take()
    }

    fn lock(&self) -> MutexGuard<'_, Signals> {
        self.signals
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub(super) fn bind(&self, shared: &Arc<Shared>) {
        self.owner.set(Arc::downgrade(shared)).unwrap();
    }

    fn wait(&self, description: &str, done: impl Fn(&Signals) -> bool) {
        let until = Instant::now() + Duration::from_secs(12);
        let mut signals = self.lock();
        while !done(&signals) {
            let remaining = until.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let detail = format!("{signals:?}");
                drop(signals);
                panic!("timed out waiting for {description}: {detail}");
            }
            signals = self
                .changed
                .wait_timeout(signals, remaining)
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
    }

    pub(crate) fn arm(self: &Arc<Self>, phase: Phase) -> PauseGuard {
        let mut signals = self.lock();
        if !signals.pause.as_ref().is_none_or(|pause| pause.released) {
            drop(signals);
            panic!("an observer pause is already armed");
        }
        signals.next_pause += 1;
        let id = signals.next_pause;
        signals.pause = Some(Pause {
            id,
            phase,
            reached: false,
            released: false,
        });
        PauseGuard {
            control: self.clone(),
            id,
        }
    }

    pub(super) fn checkpoint(&self, phase: Phase) -> Result<(), Failure> {
        let mut signals = self.lock();
        let Some(pause) = signals
            .pause
            .as_mut()
            .filter(|p| p.phase == phase && !p.released)
        else {
            return Ok(());
        };
        let id = pause.id;
        pause.reached = true;
        self.changed.notify_all();
        let until = Instant::now() + Duration::from_secs(12);
        while signals
            .pause
            .as_ref()
            .is_some_and(|p| p.id == id && !p.released)
        {
            let remaining = until.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Failure::unavailable(format!(
                    "test gate {phase:?} was not released"
                )));
            }
            signals = self
                .changed
                .wait_timeout(signals, remaining)
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
        if signals.pause.as_ref().is_some_and(|p| p.id == id) {
            signals.pause = None;
        }
        Ok(())
    }

    pub(super) fn requested(&self) -> u64 {
        self.lock().requested
    }

    pub(crate) fn control_wakes(&self) -> u64 {
        self.requested()
    }

    pub(super) fn readied(&self) {
        self.lock().ready = true;
        self.changed.notify_all();
    }

    pub(crate) fn wait_ready(&self) {
        self.wait("observer readiness", |s| {
            s.ready || s.failure.is_some() || s.exited
        });
    }

    pub(super) fn published(&self) {
        self.lock().publications += 1;
        self.changed.notify_all();
    }

    pub(crate) fn publications(&self) -> u64 {
        self.lock().publications
    }

    pub(crate) fn wait_publication_after(&self, previous: u64) {
        self.wait("real committed progress", |s| {
            s.publications > previous || s.failure.is_some() || s.exited
        });
    }

    pub(super) fn busy_encountered(&self) {
        let mut signals = self.lock();
        signals.busy_encounters += 1;
        signals.last_busy = Some(Instant::now());
        self.changed.notify_all();
    }

    pub(crate) fn busy_encounters(&self) -> u64 {
        self.lock().busy_encounters
    }

    pub(crate) fn wait_busy_after(&self, previous: u64) -> Instant {
        self.wait("actual SQLite BUSY result", |s| {
            s.busy_encounters > previous || s.failure.is_some() || s.exited
        });
        let signals = self.lock();
        let observed = signals.busy_encounters > previous;
        let at = signals.last_busy;
        let detail = format!("{signals:?}");
        drop(signals);
        assert!(observed, "{detail}");
        at.expect("BUSY encounter timestamp")
    }

    pub(crate) fn wait_failure(&self) -> Failure {
        self.wait("retained observer failure", |s| {
            s.failure.is_some() || s.exited
        });
        self.lock()
            .failure
            .clone()
            .expect("observer failed before exit")
    }

    pub(super) fn acknowledge(&self, ticket: u64) {
        let mut signals = self.lock();
        signals.acknowledged = signals.acknowledged.max(ticket);
        self.changed.notify_all();
    }

    /// Use only for negative/idle assertions after controlled writers finish.
    /// Positive liveness must await the real notification before this call.
    pub(crate) fn settle(&self) {
        let ticket = {
            let mut signals = self.lock();
            signals.requested += 1;
            signals.requested
        };
        {
            let owner = self
                .owner
                .get()
                .unwrap()
                .upgrade()
                .expect("observer still owned");
            owner.wake.signal().unwrap();
        }
        self.wait("post-request observer settle", |s| {
            s.acknowledged >= ticket || s.failure.is_some() || s.exited
        });
        let signals = self.lock();
        let settled = signals.failure.is_none() && signals.acknowledged >= ticket;
        let detail = format!("{signals:?}");
        drop(signals);
        assert!(settled, "{detail}");
    }

    pub(super) fn failed(&self, failure: Failure) {
        let mut signals = self.lock();
        signals.failure.get_or_insert(failure);
        self.changed.notify_all();
    }

    pub(super) fn started(&self) {
        self.lock().starts += 1;
        self.changed.notify_all();
    }

    pub(crate) fn starts(&self) -> usize {
        self.lock().starts
    }

    pub(super) fn exited(&self) {
        self.lock().exited = true;
        self.changed.notify_all();
    }

    pub(crate) fn wait_exited(&self) {
        self.wait("connection and watcher cleanup", |s| s.exited);
    }

    pub(super) fn cancel_gates(&self) {
        if let Some(pause) = self.lock().pause.as_mut() {
            pause.released = true;
        }
        self.changed.notify_all();
    }
}

pub(crate) struct PauseGuard {
    control: Arc<TestControl>,
    id: u64,
}

impl PauseGuard {
    /// Atomically release one reached point and arm the next, so a constructor
    /// cannot run through the second point before the controller is ready.
    pub(crate) fn release_and_arm(&self, phase: Phase) -> PauseGuard {
        let mut signals = self.control.lock();
        if !signals
            .pause
            .as_ref()
            .is_some_and(|p| p.id == self.id && p.reached && !p.released)
        {
            drop(signals);
            panic!("cannot chain an unreached or released observer pause");
        }
        signals.next_pause += 1;
        let id = signals.next_pause;
        signals.pause = Some(Pause {
            id,
            phase,
            reached: false,
            released: false,
        });
        self.control.changed.notify_all();
        PauseGuard {
            control: self.control.clone(),
            id,
        }
    }

    pub(crate) fn wait_reached(&self) {
        self.control.wait("armed observer gate", |s| {
            s.pause
                .as_ref()
                .is_some_and(|p| p.id == self.id && p.reached)
                || s.failure.is_some()
                || s.exited
        });
        let signals = self.control.lock();
        let reached = signals
            .pause
            .as_ref()
            .is_some_and(|p| p.id == self.id && p.reached);
        let detail = format!("{signals:?}");
        drop(signals);
        assert!(reached, "{detail}");
    }

    pub(crate) fn release(&self) {
        let mut signals = self.control.lock();
        if let Some(pause) = signals.pause.as_mut().filter(|p| p.id == self.id) {
            pause.released = true;
        }
        self.control.changed.notify_all();
    }
}

impl Drop for PauseGuard {
    fn drop(&mut self) {
        self.release();
    }
}
