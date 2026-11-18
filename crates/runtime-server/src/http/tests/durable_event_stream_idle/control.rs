use super::*;
use runtime_core::{EventObservationFailure, EventObserverHealth};
use std::sync::{Condvar, Mutex};
use std::time::Instant;
use tokio::sync::{watch, Notify};

pub(super) type Chunks = axum::body::BodyDataStream;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReadPhase {
    Subscribe,
    Before(usize),
    After(usize),
}

struct Gate {
    phase: ReadPhase,
    reached: AtomicBool,
    released: Mutex<bool>,
    release: Condvar,
    changed: Notify,
}

pub(super) struct GateHold(Arc<Gate>);

impl GateHold {
    pub(super) async fn reached(&self) {
        wait_value(&self.0.reached, &self.0.changed, true).await;
    }

    pub(super) fn release(&self) {
        // A bounded gate panic may poison this mutex. Cleanup must release
        // the worker without a second panic aborting the whole test binary.
        *self
            .0
            .released
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = true;
        self.0.release.notify_all();
    }
}

impl Drop for GateHold {
    fn drop(&mut self) {
        self.release();
    }
}

struct HealthOverride {
    tx: Mutex<Option<watch::Sender<EventObserverHealth>>>,
    rx: watch::Receiver<EventObserverHealth>,
}

pub(super) struct ReplayControl {
    health: Option<HealthOverride>,
    gates: Mutex<Vec<Arc<Gate>>>,
    pub(super) created: AtomicUsize,
    pub(super) dropped: AtomicUsize,
    pub(super) live: AtomicUsize,
    changed: Notify,
}

pub(super) struct ReaderLifetime(Arc<ReplayControl>);

impl Drop for ReaderLifetime {
    fn drop(&mut self) {
        self.0.reader_dropped();
    }
}

async fn wait_value(value: &AtomicBool, changed: &Notify, target: bool) {
    timeout(Duration::from_secs(3), async {
        loop {
            let notification = changed.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if value.load(Ordering::SeqCst) == target {
                return;
            }
            notification.await;
        }
    })
    .await
    .expect("bounded gate acknowledgement");
}

impl ReplayControl {
    pub(super) fn reader_lifetime(self: &Arc<Self>) -> ReaderLifetime {
        ReaderLifetime(self.clone())
    }

    fn new(health: Option<EventObserverHealth>) -> Arc<Self> {
        Arc::new(Self {
            health: health.map(|value| {
                let (tx, rx) = watch::channel(value);
                HealthOverride {
                    tx: Mutex::new(Some(tx)),
                    rx,
                }
            }),
            gates: Mutex::new(Vec::new()),
            created: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            live: AtomicUsize::new(0),
            changed: Notify::new(),
        })
    }

    pub(super) fn health(&self) -> Option<watch::Receiver<EventObserverHealth>> {
        self.health.as_ref().map(|health| health.rx.clone())
    }

    pub(super) fn set_health(&self, value: EventObserverHealth) {
        self.health
            .as_ref()
            .unwrap()
            .tx
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send_replace(value);
    }

    pub(super) fn fail(&self) {
        self.set_health(EventObserverHealth::Failed(
            EventObservationFailure::unavailable("test retained observer failure"),
        ));
    }

    pub(super) fn close_health(&self) {
        self.health.as_ref().unwrap().tx.lock().unwrap().take();
    }

    pub(super) fn arm(&self, phase: ReadPhase) -> GateHold {
        let gate = Arc::new(Gate {
            phase,
            reached: AtomicBool::new(false),
            released: Mutex::new(false),
            release: Condvar::new(),
            changed: Notify::new(),
        });
        self.gates.lock().unwrap().push(gate.clone());
        GateHold(gate)
    }

    pub(super) fn visit(&self, phase: ReadPhase) {
        let gate = {
            let mut gates = self.gates.lock().unwrap();
            gates
                .iter()
                .position(|gate| gate.phase == phase)
                .map(|index| gates.remove(index))
        };
        let Some(gate) = gate else {
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(12);
        let mut released = gate.released.lock().unwrap();
        gate.reached.store(true, Ordering::SeqCst);
        gate.changed.notify_waiters();
        while !*released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "bounded replay gate expired: {phase:?}"
            );
            released = gate.release.wait_timeout(released, remaining).unwrap().0;
        }
    }

    pub(super) fn reader_created(&self) {
        self.live.fetch_add(1, Ordering::SeqCst);
        self.created.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(super) fn reader_dropped(&self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
        self.dropped.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(super) async fn wait_count(&self, value: &AtomicUsize, target: usize) {
        timeout(Duration::from_secs(3), async {
            loop {
                let notification = self.changed.notified();
                tokio::pin!(notification);
                notification.as_mut().enable();
                if value.load(Ordering::SeqCst) == target {
                    return;
                }
                notification.await;
            }
        })
        .await
        .expect("bounded replay reader lifetime acknowledgement");
    }
}

pub(super) struct Fixture {
    pub(super) temp: tempfile::TempDir,
    pub(super) path: PathBuf,
    pub(super) store: Arc<CountingEventStore>,
    pub(super) control: Arc<ReplayControl>,
}

impl Fixture {
    pub(super) async fn new(health: Option<EventObserverHealth>) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite3");
        let sqlite = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: path.clone(),
        }));
        sqlite.initialize().await.unwrap();
        // The fake health channel never substitutes for actual SQLite startup.
        // Do not retain this extra lease in reader-lifetime assertions.
        let mut baseline = sqlite.subscribe_event_commits().unwrap();
        timeout(Duration::from_secs(3), baseline.ready())
            .await
            .unwrap()
            .unwrap();
        drop(baseline);
        let control = ReplayControl::new(health);
        let store = Arc::new(CountingEventStore {
            sqlite,
            reads: Arc::new(AtomicUsize::new(0)),
            fail_next_read: Arc::new(AtomicBool::new(false)),
            control: Some(control.clone()),
        });
        Self {
            temp,
            path,
            store,
            control,
        }
    }

    pub(super) fn append(&self, name: &str) -> RuntimeEventRecord {
        SqliteRuntimeRepository::new(self.path.clone())
            .append_runtime_event(&super::super::durable_event_stream_routes::new_event(
                name,
                RuntimeEventScope::System,
                "global",
            ))
            .unwrap()
    }

    pub(super) async fn open(&self, after: i64, page: usize, headers: &HeaderMap) -> Response {
        durable_event_stream(self.store.clone(), None, Some(after), page, headers)
            .await
            .unwrap()
            .into_response()
    }
}

pub(super) async fn next(chunks: &mut Chunks) -> String {
    let bytes = timeout(Duration::from_secs(3), chunks.next())
        .await
        .expect("bounded SSE delivery")
        .expect("SSE frame before EOF")
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

pub(super) async fn drain(chunks: &mut Chunks) -> String {
    timeout(Duration::from_secs(3), async {
        let mut text = String::new();
        while let Some(bytes) = chunks.next().await {
            text.push_str(&String::from_utf8(bytes.unwrap().to_vec()).unwrap());
        }
        text
    })
    .await
    .expect("terminal SSE must reach EOF")
}

pub(super) fn ids(text: &str) -> Vec<i64> {
    text.lines()
        .filter_map(|line| line.strip_prefix("id: "))
        .map(|id| id.parse().unwrap())
        .collect()
}

pub(super) fn assert_terminal(text: &str, code: &str) {
    let frames: Vec<_> = text
        .split("\n\n")
        .filter(|frame| !frame.is_empty())
        .collect();
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.contains("event: stream.error\n"))
            .count(),
        1,
        "{text}"
    );
    let terminal = frames.last().unwrap();
    assert!(terminal.contains(code), "{terminal}");
    assert!(terminal.contains(r#""retryable":false"#), "{terminal}");
    assert!(
        !terminal.lines().any(|line| line.starts_with("id:")),
        "{terminal}"
    );
}
