use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use runtime_core::{
    OperationPhase, ProcessManager, RuntimeSessionManager, WorkspaceRetirementCommand,
};

type WorkerKey = (usize, String);
type WorkerRegistry = Arc<Mutex<HashMap<WorkerKey, bool>>>;
static WORKERS: OnceLock<WorkerRegistry> = OnceLock::new();

struct WorkerGuard {
    key: WorkerKey,
    registry: WorkerRegistry,
    registered: bool,
}

impl WorkerGuard {
    fn register(registry: WorkerRegistry, key: WorkerKey) -> Option<Self> {
        let Ok(mut workers) = registry.lock() else {
            tracing::error!("retirement worker registry unavailable");
            return None;
        };
        if let Some(wake_pending) = workers.get_mut(&key) {
            // The current worker may already hold a stale ManualReview result.
            // Preserve this request until it has rechecked the durable state.
            *wake_pending = true;
            return None;
        }
        workers.insert(key.clone(), false);
        drop(workers);
        Some(Self {
            key,
            registry,
            registered: true,
        })
    }

    fn wake_pending_or_unregister(&mut self) -> bool {
        let Ok(mut workers) = self.registry.lock() else {
            return false;
        };
        if let Some(wake_pending) = workers.get_mut(&self.key) {
            if *wake_pending {
                *wake_pending = false;
                return true;
            }
        }
        // Removing the entry and deciding to exit share the same lock used by
        // admission/review wakeups. A subsequent wake can start a new worker.
        workers.remove(&self.key);
        self.registered = false;
        false
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if self.registered {
            if let Ok(mut workers) = self.registry.lock() {
                workers.remove(&self.key);
            }
        }
    }
}

/// Only nonterminal, already-admitted retirement work creates a worker. Idle
/// runtimes never poll SQLite, while active retirements resume after crash or
/// a lost HTTP response. Unknown native outcomes stop at manual review.
pub(crate) fn spawn_retirement_worker(
    runtime: Arc<RuntimeSessionManager>,
    processes: Arc<dyn ProcessManager>,
    command: WorkspaceRetirementCommand,
) {
    let key = (Arc::as_ptr(&runtime) as usize, command.operation_id.clone());
    let registry = WORKERS.get_or_init(WorkerRegistry::default).clone();
    let Some(mut guard) = WorkerGuard::register(registry, key) else {
        return;
    };
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let operation_id = command.operation_id.clone();
            let stop = match runtime
                .advance_workspace_retirement(&command, processes.as_ref())
                .await
            {
                Ok(details) => match details.operation.phase {
                    OperationPhase::Completed => {
                        tracing::info!(operation_id, "workspace retirement settled and retired");
                        true
                    }
                    OperationPhase::ManualReview | OperationPhase::Failed => {
                        tracing::warn!(operation_id,
                            "workspace retirement cannot automatically proceed; operator reconciliation required");
                        true
                    }
                    OperationPhase::Requested => false,
                },
                Err(error) => {
                    tracing::warn!(operation_id, error=%error,
                        "workspace retirement worker halted at persisted recovery boundary");
                    true
                }
            };
            if stop {
                if guard.wake_pending_or_unregister() {
                    tick.reset_immediately();
                } else {
                    return;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_review_wake_survives_a_workers_stale_terminal_result() {
        let registry = WorkerRegistry::default();
        let key = (1, "review-resume".into());
        let mut worker = WorkerGuard::register(registry.clone(), key.clone()).unwrap();
        assert!(WorkerGuard::register(registry.clone(), key.clone()).is_none());
        // Review has changed durable state back to requested while this worker
        // was returning a previously read ManualReview result.
        assert!(worker.wake_pending_or_unregister());
        assert!(registry.lock().unwrap().contains_key(&key));
        assert!(!worker.wake_pending_or_unregister());
        assert!(!registry.lock().unwrap().contains_key(&key));
    }

    #[test]
    fn retired_worker_guard_cannot_remove_a_later_resume_worker() {
        let registry = WorkerRegistry::default();
        let key = (2, "new-resume".into());
        let mut old = WorkerGuard::register(registry.clone(), key.clone()).unwrap();
        assert!(!old.wake_pending_or_unregister());
        let new = WorkerGuard::register(registry.clone(), key.clone()).unwrap();
        drop(old);
        assert!(registry.lock().unwrap().contains_key(&key));
        drop(new);
        assert!(!registry.lock().unwrap().contains_key(&key));
    }
}
