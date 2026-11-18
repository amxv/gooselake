use runtime_core::{EventCommitSubscription, EventObserverHealth, RuntimeStore};
use tokio::sync::broadcast::error::TryRecvError;

use crate::SqliteRuntimeStore;

pub(super) struct ObservedCommits {
    subscription: EventCommitSubscription,
    #[cfg(target_os = "linux")]
    control: std::sync::Arc<crate::event_notifications::TestControl>,
}

fn health(subscription: &EventCommitSubscription) -> Result<bool, String> {
    let state = subscription.health.borrow().clone();
    if let EventObserverHealth::Failed(failure) = state {
        return Err(format!("{}: {}", failure.code, failure.message));
    }
    if subscription.health.has_changed().is_err() {
        return Err("observer health channel closed".into());
    }
    Ok(state == EventObserverHealth::Ready)
}

pub(super) fn observe_commits(store: &SqliteRuntimeStore) -> ObservedCommits {
    let subscription = store.subscribe_event_commits().expect("open observer");
    #[cfg(target_os = "linux")]
    let control = store.event_observer_test_control();
    #[cfg(target_os = "linux")]
    control.wait_ready();
    assert!(health(&subscription).expect("observer readiness"));
    ObservedCommits {
        subscription,
        #[cfg(target_os = "linux")]
        control,
    }
}

impl ObservedCommits {
    /// A positive assertion must observe the actual filesystem-driven hint;
    /// it deliberately never sends a test control wake before receiving it.
    pub(super) fn wait(&mut self) -> Result<(), String> {
        loop {
            #[cfg(target_os = "linux")]
            let previous = self.control.publications();
            health(&self.subscription)?;
            match self.subscription.try_recv() {
                Ok(()) => return Ok(()),
                Err(TryRecvError::Empty) => {
                    #[cfg(target_os = "linux")]
                    self.control.wait_publication_after(previous);
                    #[cfg(not(target_os = "linux"))]
                    return Err("Linux event observation is unavailable".into());
                }
                outcome => return Err(format!("expected committed progress: {outcome:?}")),
            }
        }
    }

    /// The caller has completed its controlled rollback/no-op. A numbered
    /// post-request scan acknowledges all prior hints before checking Empty.
    pub(super) fn settled_try_recv(&mut self) -> Result<(), TryRecvError> {
        #[cfg(target_os = "linux")]
        self.control.settle();
        self.subscription.try_recv()
    }
}
