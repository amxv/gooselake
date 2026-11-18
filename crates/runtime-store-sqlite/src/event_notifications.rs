//! Lazy, generation-bound committed-event observation. Store construction may
//! precede schema initialization; only subscription opens an existing database.
use std::path::Path;

use runtime_core::{EventCommitSubscription, RuntimeError};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(all(test, target_os = "linux"))]
pub(crate) use linux::test_control::TestControl;

#[derive(Debug, Default)]
pub(crate) struct EventCommitNotifier {
    #[cfg(target_os = "linux")]
    observer: std::sync::OnceLock<
        Result<std::sync::Arc<linux::EventObserver>, runtime_core::EventObservationFailure>,
    >,
}

impl EventCommitNotifier {
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn test_control(&self) -> std::sync::Arc<TestControl> {
        self.observer
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .test_control()
    }

    pub(crate) fn subscribe(&self, path: &Path) -> Result<EventCommitSubscription, RuntimeError> {
        #[cfg(target_os = "linux")]
        {
            if self.observer.get().is_none() {
                let candidate = linux::observer_for(path);
                // This pre-generation error can clear when a detached worker
                // exits. Established generation failures stay cached/sticky.
                if let Err(failure) = &candidate {
                    if failure.code == "event_observer_stopping" {
                        return Err(failure.runtime_error());
                    }
                }
                self.observer.get_or_init(|| candidate);
            }
            match self.observer.get().expect("observer initialized above") {
                Ok(observer) => Ok(observer.subscribe()),
                Err(failure) => Err(failure.runtime_error()),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(RuntimeError::Unsupported(
                "generation-bound SQLite event observation requires supported Linux local WAL primitives".into(),
            ))
        }
    }
}
