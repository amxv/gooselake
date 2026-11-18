use super::*;
use runtime_core::{NewRuntimeEvent, RuntimeEventCriticality, RuntimeStore};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::TryRecvError;

use crate::{SqliteRuntimeRepository, SqliteRuntimeStore, SqliteStoreConfig};

mod handoffs;
mod identity;
mod lifetime;
mod overflow;
mod raw_writers;

struct Fixture {
    root: tempfile::TempDir,
    path: PathBuf,
    control_dir: PathBuf,
    repository: SqliteRuntimeRepository,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let control_dir = root.path().join("control");
        std::fs::create_dir(&data).unwrap();
        std::fs::create_dir(&control_dir).unwrap();
        std::fs::write(root.path().join("n05-fixture"), b"disposable-offline").unwrap();
        let path = data.join("runtime.sqlite3");
        let repository = SqliteRuntimeRepository::new(path.clone());
        repository.initialize_schema().unwrap();
        Self {
            root,
            path,
            control_dir,
            repository,
        }
    }

    fn store(&self) -> SqliteRuntimeStore {
        SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: self.path.clone(),
        })
    }

    fn seed(&self, id: &str) -> RuntimeEventRecord {
        self.repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: id.into(),
                scope: RuntimeEventScope::System,
                scope_id: "seed".into(),
                session_id: None,
                team_id: None,
                turn_id: None,
                kind: "synthetic.seed".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({}),
                provider: None,
                provider_seq: None,
                created_at: 1,
            })
            .unwrap()
    }

    /// Arrive at the real idle poll through startup alone. Neither readiness
    /// nor pause release writes eventfd; no replay or settle is allowed here.
    fn idle_observer(
        &self,
    ) -> (
        Arc<EventObserver>,
        EventCommitSubscription,
        Arc<TestControl>,
    ) {
        let (observer, subscription, control, pause) = self.parked_observer();
        pause.release();
        (observer, subscription, control)
    }

    fn parked_observer(
        &self,
    ) -> (
        Arc<EventObserver>,
        EventCommitSubscription,
        Arc<TestControl>,
        test_control::PauseGuard,
    ) {
        let control = test_control::install(&self.path);
        let pause = control.arm(Phase::BeforeWait);
        let observer = observer_for(&self.path).unwrap();
        let subscription = observer.subscribe();
        control.wait_ready();
        assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
        pause.wait_reached();
        assert_eq!(control.control_wakes(), 0);
        let mut fd = libc::pollfd {
            fd: observer.shared.wake.fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert_eq!(
            unsafe { libc::poll(&mut fd, 1, 0) },
            0,
            "no residual control/reader wake"
        );
        (observer, subscription, control, pause)
    }

    fn writer(&self, mode: &str, label: &str) -> ChildGuard {
        let signals = self.control_dir.join(label);
        std::fs::create_dir(&signals).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "event_notifications::linux::tests::raw_writers::raw_writer_child",
                "--nocapture",
            ])
            .env("GOOSELAKE_N05_RAW_ROOT", self.root.path())
            .env("GOOSELAKE_N05_RAW_MODE", mode)
            .env("GOOSELAKE_N05_RAW_LABEL", label)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        ChildGuard { child, signals }
    }
}

fn wait_file(path: &Path) {
    let until = Instant::now() + Duration::from_secs(12);
    while !path.exists() {
        assert!(
            Instant::now() < until,
            "fixture handshake timed out: {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

struct ChildGuard {
    child: Child,
    signals: PathBuf,
}

impl ChildGuard {
    fn wait(&self, marker: &str) {
        wait_file(&self.signals.join(marker));
    }
    fn allow(&self, marker: &str) {
        std::fs::write(self.signals.join(marker), b"ok").unwrap();
    }
    fn finish(&mut self) {
        let until = Instant::now() + Duration::from_secs(12);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "raw fixture child failed: {status}");
                return;
            }
            assert!(Instant::now() < until, "raw fixture child did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn kill_owned(&mut self) {
        self.child.kill().unwrap();
        assert!(!self.child.wait().unwrap().success());
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn wait_commit(subscription: &mut EventCommitSubscription, control: &TestControl) {
    loop {
        let previous = control.publications();
        assert_eq!(*subscription.health.borrow(), EventObserverHealth::Ready);
        match subscription.try_recv() {
            Ok(()) => return,
            Err(TryRecvError::Empty) => control.wait_publication_after(previous),
            outcome => panic!("unexpected commit receiver outcome: {outcome:?}"),
        }
    }
}

fn assert_no_commit(subscription: &mut EventCommitSubscription, control: &TestControl) {
    control.settle();
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
}
