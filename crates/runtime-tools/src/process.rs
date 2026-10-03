use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use runtime_core::{
    ManagedProcessRecord, ManagedProcessTerminalUpdate, ProcessCompletionUpdate, ProcessQueueEntry,
    ProcessSchedulerSettings, ProcessSchedulerSnapshot, RuntimeError, RuntimeEventCriticality,
    RuntimeSessionManager, RuntimeStore, PROCESS_COMPLETION_DELIVERED,
    PROCESS_COMPLETION_INJECTING, PROCESS_COMPLETION_PENDING,
};
use serde_json::json;
use tokio::sync::{broadcast, mpsc, Mutex, Notify, RwLock};

use crate::process_helpers::{completion_correlation, file_len};
use crate::{now_ms, os_process, ProcessManagerConfig};

pub(crate) const COMPLETION_CORRELATION_PREFIX: &str = "process_completion:";
pub(crate) const COMPLETION_PREVIEW_BYTES_PER_STREAM: usize = 4 * 1024;
const COMPLETION_RETRY_INTERVAL: Duration = Duration::from_secs(2);
pub(crate) const LAUNCH_AUTH_RETRY_MAX: Duration = Duration::from_secs(1);

pub struct RuntimeProcessManager {
    pub(crate) store: Arc<dyn RuntimeStore>,
    pub(crate) runtime: Option<Arc<RuntimeSessionManager>>,
    pub(crate) config: ProcessManagerConfig,
    pub(crate) settings: Arc<RwLock<ProcessSchedulerSettings>>,
    pub(crate) live_processes: Arc<RwLock<HashMap<String, Arc<LiveProcess>>>>,
    pub(crate) event_tx: broadcast::Sender<runtime_core::RuntimeEventRecord>,
    pub(crate) scheduler_notify: Arc<Notify>,
    pub(crate) completion_notify: Arc<Notify>,
    pub(crate) startup_recovered_processes: Arc<RwLock<Vec<String>>>,
}

#[derive(Debug)]
pub(crate) struct LiveProcess {
    pub(crate) control_tx: Mutex<Option<mpsc::UnboundedSender<ProcessControl>>>,
    pub(crate) stdout_bytes: Mutex<usize>,
    pub(crate) stderr_bytes: Mutex<usize>,
    pub(crate) stdout_truncated: Mutex<bool>,
    pub(crate) stderr_truncated: Mutex<bool>,
}

impl LiveProcess {
    pub(crate) fn from_record(record: &ManagedProcessRecord) -> Self {
        Self {
            control_tx: Mutex::new(None),
            stdout_bytes: Mutex::new(usize::try_from(record.stdout_captured_bytes).unwrap_or(0)),
            stderr_bytes: Mutex::new(usize::try_from(record.stderr_captured_bytes).unwrap_or(0)),
            stdout_truncated: Mutex::new(record.stdout_truncated),
            stderr_truncated: Mutex::new(record.stderr_truncated),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ProcessControl {
    Kill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitCause {
    Natural,
    TimedOut,
    Killed,
}

impl RuntimeProcessManager {
    pub async fn new(
        store: Arc<dyn RuntimeStore>,
        config: ProcessManagerConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        Self::new_internal(store, None, config).await
    }

    pub async fn new_with_runtime(
        store: Arc<dyn RuntimeStore>,
        runtime: Arc<RuntimeSessionManager>,
        config: ProcessManagerConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        Self::new_internal(store, Some(runtime), config).await
    }

    async fn new_internal(
        store: Arc<dyn RuntimeStore>,
        runtime: Option<Arc<RuntimeSessionManager>>,
        config: ProcessManagerConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        store.initialize().await?;
        std::fs::create_dir_all(&config.log_dir).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed to create process log dir {}: {error}",
                config.log_dir.display()
            ))
        })?;

        let default_settings = ProcessSchedulerSettings {
            max_concurrent: config.max_concurrent,
            workspace_max_concurrent: Default::default(),
            capture_limit_bytes: config.max_output_bytes_per_process,
            paused: false,
            pause_reason: None,
            updated_at: now_ms(),
        }
        .normalized();
        let settings = store.load_or_initialize_process_scheduler_settings(&default_settings)?;
        let records = store.list_managed_processes()?;
        let mut live_processes = HashMap::new();
        for record in &records {
            live_processes.insert(
                record.process_id.clone(),
                Arc::new(LiveProcess::from_record(record)),
            );
        }

        let (event_tx, _) = broadcast::channel(16_384);
        let manager = Arc::new(Self {
            store,
            runtime,
            config,
            settings: Arc::new(RwLock::new(settings)),
            live_processes: Arc::new(RwLock::new(live_processes)),
            event_tx,
            scheduler_notify: Arc::new(Notify::new()),
            completion_notify: Arc::new(Notify::new()),
            startup_recovered_processes: Arc::new(RwLock::new(Vec::new())),
        });

        manager.recover_startup(records).await?;
        manager.start_background_tasks();
        manager.scheduler_notify.notify_one();
        manager.completion_notify.notify_one();
        Ok(manager)
    }

    pub async fn startup_recovered_processes(&self) -> Vec<String> {
        self.startup_recovered_processes.read().await.clone()
    }

    pub async fn scheduler_settings(&self) -> ProcessSchedulerSettings {
        self.settings.read().await.clone()
    }

    pub async fn replace_scheduler_settings(
        &self,
        settings: ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        let mut settings = settings.normalized();
        settings.updated_at = now_ms();
        let persisted = self.store.replace_process_scheduler_settings(&settings)?;
        *self.settings.write().await = persisted.clone();
        self.scheduler_notify.notify_one();
        Ok(persisted)
    }

    pub async fn pause_scheduler(
        &self,
        reason: Option<String>,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        let mut settings = self.settings.read().await.clone();
        settings.paused = true;
        settings.pause_reason = reason.or_else(|| Some("operator".to_string()));
        self.replace_scheduler_settings(settings).await
    }

    pub async fn resume_scheduler(&self) -> Result<ProcessSchedulerSettings, RuntimeError> {
        let mut settings = self.settings.read().await.clone();
        settings.paused = false;
        settings.pause_reason = None;
        self.replace_scheduler_settings(settings).await
    }

    pub async fn reorder_queue(
        &self,
        process_id: &str,
        before_process_id: Option<&str>,
        after_process_id: Option<&str>,
    ) -> Result<Vec<String>, RuntimeError> {
        let ids = self.store.reorder_queued_managed_process(
            process_id,
            before_process_id,
            after_process_id,
            now_ms(),
        )?;
        self.scheduler_notify.notify_one();
        Ok(ids)
    }

    pub(crate) async fn build_scheduler_snapshot(
        &self,
    ) -> Result<ProcessSchedulerSnapshot, RuntimeError> {
        let settings = self.settings.read().await.clone();
        let mut queued = Vec::new();
        let mut active = Vec::new();
        for record in self.store.list_managed_processes()? {
            match record.status.as_str() {
                "queued" => queued.push(ProcessQueueEntry::from(&record)),
                "launch_reserved" | "running" => active.push(ProcessQueueEntry::from(&record)),
                _ => {}
            }
        }
        queued.sort_by(|left, right| {
            left.queue_order
                .cmp(&right.queue_order)
                .then(left.admission_order.cmp(&right.admission_order))
                .then(left.process_id.cmp(&right.process_id))
        });
        active.sort_by(|left, right| {
            left.execution_started_at
                .cmp(&right.execution_started_at)
                .then(left.admission_order.cmp(&right.admission_order))
                .then(left.process_id.cmp(&right.process_id))
        });
        Ok(ProcessSchedulerSnapshot {
            settings,
            queued,
            active,
        })
    }

    fn start_background_tasks(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            Self::scheduler_loop(weak).await;
        });
        if let Some(runtime) = self.runtime.clone() {
            let weak = Arc::downgrade(self);
            tokio::spawn(async move {
                Self::completion_loop(weak, runtime).await;
            });
        }
    }

    async fn scheduler_loop(weak: Weak<Self>) {
        loop {
            let Some(manager) = weak.upgrade() else {
                return;
            };
            let notify = Arc::clone(&manager.scheduler_notify);
            drop(manager);
            notify.notified().await;
            let Some(manager) = weak.upgrade() else {
                return;
            };
            loop {
                let settings = manager.settings.read().await.clone();
                let claims = match manager.store.claim_managed_processes(&settings, now_ms()) {
                    Ok(claims) => claims,
                    Err(_) => break,
                };
                if claims.is_empty() {
                    break;
                }
                for claim in claims {
                    let manager = Arc::clone(&manager);
                    tokio::spawn(async move {
                        manager.launch_claim(claim).await;
                    });
                }
            }
        }
    }

    async fn completion_loop(weak: Weak<Self>, runtime: Arc<RuntimeSessionManager>) {
        let mut runtime_events = runtime.subscribe_events();
        loop {
            let Some(manager) = weak.upgrade() else {
                return;
            };
            let notify = Arc::clone(&manager.completion_notify);
            drop(manager);
            tokio::select! {
                _ = notify.notified() => {},
                _ = tokio::time::sleep(COMPLETION_RETRY_INTERVAL) => {},
                result = runtime_events.recv() => {
                    if result.is_err() {
                        continue;
                    }
                }
            }
            let Some(manager) = weak.upgrade() else {
                return;
            };
            let records = match manager.store.list_managed_processes() {
                Ok(records) => records,
                Err(_) => continue,
            };
            for record in records {
                if matches!(
                    record.completion_state.as_str(),
                    PROCESS_COMPLETION_PENDING | PROCESS_COMPLETION_INJECTING
                ) {
                    let _ = manager.try_deliver_completion(&runtime, &record).await;
                }
            }
        }
    }

    async fn recover_startup(
        self: &Arc<Self>,
        records: Vec<ManagedProcessRecord>,
    ) -> Result<(), RuntimeError> {
        let mut recovered = Vec::new();
        for record in records {
            match record.status.as_str() {
                "queued" => {}
                "launch_reserved" => {
                    if self
                        .store
                        .requeue_managed_process_claim(
                            &record.process_id,
                            record.claim_generation,
                            now_ms(),
                        )?
                        .is_some()
                    {
                        recovered.push(record.process_id.clone());
                    }
                }
                "running" => {
                    self.reconcile_running_process(&record).await?;
                    recovered.push(record.process_id.clone());
                }
                _ => {}
            }

            if record.completion_state == PROCESS_COMPLETION_INJECTING {
                if let (Some(runtime), Some(owner)) =
                    (self.runtime.as_ref(), record.owner_session_id.as_deref())
                {
                    let correlation = completion_correlation(&record.process_id);
                    if let Some(admission) = runtime
                        .latest_turn_admission_for_correlation(owner, &correlation)
                        .await
                    {
                        match admission.dispatch_state {
                            runtime_core::TurnDispatchState::Dispatched => {
                                self.store.update_managed_process_completion(
                                    &record.process_id,
                                    &ProcessCompletionUpdate {
                                        state: PROCESS_COMPLETION_DELIVERED.to_string(),
                                        turn_id: Some(admission.turn_id),
                                        attempt_count: record.completion_attempt_count,
                                        last_error: None,
                                        updated_at: now_ms(),
                                    },
                                )?;
                            }
                            runtime_core::TurnDispatchState::NotDispatched => {
                                self.store.update_managed_process_completion(
                                    &record.process_id,
                                    &ProcessCompletionUpdate {
                                        state: PROCESS_COMPLETION_PENDING.to_string(),
                                        turn_id: None,
                                        attempt_count: record.completion_attempt_count,
                                        last_error: Some(
                                            "startup_recovered_proven_not_dispatched_completion"
                                                .to_string(),
                                        ),
                                        updated_at: now_ms(),
                                    },
                                )?;
                            }
                            _ => {}
                        }
                    } else {
                        self.store.update_managed_process_completion(
                            &record.process_id,
                            &ProcessCompletionUpdate {
                                state: PROCESS_COMPLETION_PENDING.to_string(),
                                turn_id: None,
                                attempt_count: record.completion_attempt_count,
                                last_error: Some(
                                    "startup_recovered_pre_admission_completion_attempt"
                                        .to_string(),
                                ),
                                updated_at: now_ms(),
                            },
                        )?;
                    }
                }
            }
        }
        *self.startup_recovered_processes.write().await = recovered;
        Ok(())
    }

    async fn reconcile_running_process(
        &self,
        record: &ManagedProcessRecord,
    ) -> Result<(), RuntimeError> {
        let pid_i64 = record.pid.ok_or_else(|| {
            RuntimeError::InvalidState(format!(
                "startup process {} is running without a persisted pid; refusing guessed reconciliation",
                record.process_id
            ))
        })?;
        let pid = u32::try_from(pid_i64).map_err(|_| {
            RuntimeError::InvalidState(format!(
                "startup process {} has invalid pid {pid_i64}",
                record.process_id
            ))
        })?;
        let identity = record.os_start_identity.as_deref().ok_or_else(|| {
            RuntimeError::InvalidState(format!(
                "startup process {} is running without verifiable OS start identity",
                record.process_id
            ))
        })?;
        let termination = os_process::terminate_process_group(pid, identity).map_err(|error| {
            RuntimeError::Io(format!(
                "failed reconciling startup process {}: {error}",
                record.process_id
            ))
        })?;
        if termination == os_process::ProcessTerminationOutcome::IdentityMismatch {
            return Err(RuntimeError::InvalidState(format!(
                "startup process {} pid {} has a different OS start identity; refusing to kill a possibly reused pid",
                record.process_id, pid
            )));
        }

        let ended_at = now_ms();
        let stdout_bytes = file_len(&record.stdout_path);
        let stderr_bytes = file_len(&record.stderr_path);
        let update = ManagedProcessTerminalUpdate {
            status: "interrupted".to_string(),
            terminal_reason: Some("startup_reconciliation".to_string()),
            exit_code: None,
            signal: None,
            ended_at,
            execution_duration_ms: record
                .execution_started_at
                .map(|started| ended_at.saturating_sub(started)),
            stdout_captured_bytes: i64::try_from(stdout_bytes).unwrap_or(i64::MAX),
            stderr_captured_bytes: i64::try_from(stderr_bytes).unwrap_or(i64::MAX),
            stdout_truncated: record.stdout_truncated,
            stderr_truncated: record.stderr_truncated,
            completion_required: self.runtime.is_some() && record.owner_session_id.is_some(),
        };
        self.store
            .terminalize_managed_process(&record.process_id, &update)?;
        self.append_process_event(
            &record.process_id,
            record.owner_session_id.clone(),
            "process.interrupted",
            RuntimeEventCriticality::Critical,
            json!({
                "process_id": record.process_id,
                "status": "interrupted",
                "reason": "startup_reconciliation",
            }),
        )
        .await;
        Ok(())
    }
}
