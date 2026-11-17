use std::sync::{Condvar, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

/// Test-only pause after durable state changes but before the store call returns.
pub(in crate::runtime) struct AuthorityPause {
    entered: Notify,
    released: Mutex<bool>,
    changed: Condvar,
}

impl AuthorityPause {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            entered: Notify::new(),
            released: Mutex::new(false),
            changed: Condvar::new(),
        }
    }

    pub(in crate::runtime) fn pause(&self) {
        self.entered.notify_one();
        let released = self.released.lock().unwrap();
        let (released, _) = self
            .changed
            .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
            .unwrap();
        assert!(
            *released,
            "test authority pause was not released within five seconds"
        );
    }

    pub(in crate::runtime) async fn wait_until_entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.notified())
            .await
            .expect("authority pause not reached");
    }

    pub(in crate::runtime) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
