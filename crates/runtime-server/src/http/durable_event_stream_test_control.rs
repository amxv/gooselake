//! Test-only lifecycle observations. This module cannot wake a journal reader,
//! mutate health, retain a reader/store, or affect a production request.
use axum::http::{HeaderMap, HeaderValue};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Notify;

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
static CONTROLS: LazyLock<Mutex<HashMap<String, Weak<StreamControl>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
const HEADER: &str = "x-gg-test-stream-control";

#[derive(Default)]
pub(super) struct StreamControl {
    pending: AtomicBool,
    notification_lagged: AtomicBool,
    readiness_pending: AtomicBool,
    started: AtomicBool,
    finished: AtomicBool,
    changed: Notify,
}

pub(super) fn register() -> (Arc<StreamControl>, HeaderMap) {
    let key = NEXT_ID.fetch_add(1, Ordering::SeqCst).to_string();
    let control = Arc::new(StreamControl::default());
    let mut controls = CONTROLS.lock().unwrap();
    controls.retain(|_, value| value.strong_count() != 0);
    controls.insert(key.clone(), Arc::downgrade(&control));
    let mut headers = HeaderMap::new();
    headers.insert(HEADER, HeaderValue::from_str(&key).unwrap());
    (control, headers)
}

pub(super) fn from_headers(headers: &HeaderMap) -> Option<Arc<StreamControl>> {
    let key = headers.get(HEADER)?.to_str().ok()?;
    CONTROLS.lock().unwrap().get(key)?.upgrade()
}

impl StreamControl {
    pub(super) fn start(self: &Arc<Self>) -> ProducerGuard {
        self.started.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
        ProducerGuard(self.clone())
    }

    pub(super) fn output_pending(&self) {
        self.pending.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(super) fn notification_lagged(&self) {
        self.notification_lagged.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(super) fn readiness_pending(&self) {
        self.readiness_pending.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(super) fn has_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    async fn wait(&self, value: &AtomicBool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if value.load(Ordering::SeqCst) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("bounded stream lifecycle acknowledgement");
    }

    pub(super) async fn pending(&self) {
        self.wait(&self.pending).await;
    }

    pub(super) async fn lagged(&self) {
        self.wait(&self.notification_lagged).await;
    }

    pub(super) async fn waiting_for_readiness(&self) {
        self.wait(&self.readiness_pending).await;
    }

    pub(super) async fn finished(&self) {
        self.wait(&self.finished).await;
    }
}

pub(super) struct ProducerGuard(Arc<StreamControl>);

impl Drop for ProducerGuard {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}
