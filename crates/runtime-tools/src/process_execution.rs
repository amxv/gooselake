use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use runtime_core::{
    process_status_is_terminal, ManagedProcessRecord, ManagedProcessTerminalUpdate,
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Child;
use tokio::sync::mpsc;

use crate::process::{
    LiveProcess, ProcessControl, RuntimeProcessManager, WaitCause, LAUNCH_AUTH_RETRY_MAX,
};
use crate::process_helpers::{build_process_command, command_text, file_len};
use crate::{exit_status_signal, now_ms, os_process};

const TERMINAL_PERSIST_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

impl RuntimeProcessManager {
    pub(crate) async fn launch_claim(self: Arc<Self>, claim: ManagedProcessRecord) {
        let command = match command_text(&claim.command) {
            Ok(command) => command,
            Err(error) => {
                self.terminalize_spawn_failure(&claim, "invalid_command", Some(error.to_string()))
                    .await;
                return;
            }
        };
        let cwd = claim.cwd.clone().unwrap_or_else(|| ".".to_string());
        let launch_gate = self
            .config
            .log_dir
            .join(format!("{}.launch", claim.process_id));
        let _ = tokio::fs::remove_file(&launch_gate).await;
        let mut command_builder =
            match build_process_command(&command, &cwd, &launch_gate, self.config.allow_shell) {
                Ok(command) => command,
                Err(error) => {
                    self.terminalize_spawn_failure(
                        &claim,
                        "invalid_command",
                        Some(error.to_string()),
                    )
                    .await;
                    return;
                }
            };
        let mut child = match command_builder.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.terminalize_spawn_failure(&claim, "spawn_failed", Some(error.to_string()))
                    .await;
                return;
            }
        };
        let Some(pid) = child.id() else {
            let _ = child.start_kill();
            let _ = child.wait().await;
            self.terminalize_spawn_failure(&claim, "missing_pid", None)
                .await;
            return;
        };
        let identity = match os_process::capture_managed_process_identity(pid) {
            Ok(identity) => identity,
            Err(error) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                self.terminalize_spawn_failure(
                    &claim,
                    "process_identity_unavailable",
                    Some(error.to_string()),
                )
                .await;
                return;
            }
        };
        let started_at = now_ms();
        let running = match self
            .commit_running_with_retry(&claim, pid, &identity, started_at)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                let _ = os_process::terminate_process_group(pid, &identity);
                let _ = child.wait().await;
                let _ = tokio::fs::remove_file(&launch_gate).await;
                self.scheduler_notify.notify_one();
                return;
            }
            Err(error) => {
                let _ = os_process::terminate_process_group(pid, &identity);
                let _ = child.wait().await;
                let _ = tokio::fs::remove_file(&launch_gate).await;
                self.terminalize_spawn_failure(
                    &claim,
                    "launch_commit_failed",
                    Some(error.to_string()),
                )
                .await;
                return;
            }
        };

        if let Err(error) = tokio::fs::write(&launch_gate, []).await {
            let _ = os_process::terminate_process_group(pid, &identity);
            let _ = child.wait().await;
            self.terminalize_running_failure(
                &running,
                "launch_gate_failed",
                Some(error.to_string()),
            )
            .await;
            return;
        }

        let live = self.live_state(&running).await;
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        *live.control_tx.lock().await = Some(control_tx.clone());
        let durable_cancel_requested = self
            .store
            .get_managed_process(&running.process_id)
            .ok()
            .flatten()
            .is_some_and(|record| record.cancel_requested);
        if running.cancel_requested || durable_cancel_requested {
            let _ = control_tx.send(ProcessControl::Kill);
        }

        self.append_process_event(
            &running.process_id,
            running.owner_session_id.clone(),
            "process.started",
            RuntimeEventCriticality::Critical,
            json!({
                "process_id": running.process_id,
                "pid": pid,
                "cwd": running.cwd,
                "execution_started_at": started_at,
            }),
        )
        .await;

        self.run_lifecycle(running, live, child, control_rx).await;
    }

    async fn commit_running_with_retry(
        &self,
        claim: &ManagedProcessRecord,
        pid: u32,
        identity: &str,
        started_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        let mut delay = Duration::from_millis(25);
        loop {
            match self.store.mark_managed_process_running(
                &claim.process_id,
                claim.claim_generation,
                i64::from(pid),
                identity,
                started_at,
            ) {
                Ok(record) => return Ok(record),
                Err(error) => {
                    match self.store.get_managed_process(&claim.process_id) {
                        Ok(Some(record))
                            if record.status == "running"
                                && record.pid == Some(i64::from(pid))
                                && record.os_start_identity.as_deref() == Some(identity) =>
                        {
                            return Ok(Some(record));
                        }
                        Ok(Some(record)) if record.status != "launch_reserved" => return Ok(None),
                        Ok(None) => return Ok(None),
                        Ok(Some(_)) | Err(_) => {}
                    }
                    if delay >= LAUNCH_AUTH_RETRY_MAX {
                        return Err(error);
                    }
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(LAUNCH_AUTH_RETRY_MAX);
                }
            }
        }
    }

    async fn run_lifecycle(
        self: Arc<Self>,
        record: ManagedProcessRecord,
        live: Arc<LiveProcess>,
        mut child: Child,
        mut control_rx: mpsc::UnboundedReceiver<ProcessControl>,
    ) {
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdout_task = stdout.map(|stream| {
            tokio::spawn(Self::pump_stream(
                Arc::clone(&self),
                Arc::clone(&live),
                record.process_id.clone(),
                record.owner_session_id.clone(),
                "stdout",
                stream,
                PathBuf::from(&record.stdout_path),
                usize::try_from(record.capture_limit_bytes).unwrap_or(1),
            ))
        });
        let stderr_task = stderr.map(|stream| {
            tokio::spawn(Self::pump_stream(
                Arc::clone(&self),
                Arc::clone(&live),
                record.process_id.clone(),
                record.owner_session_id.clone(),
                "stderr",
                stream,
                PathBuf::from(&record.stderr_path),
                usize::try_from(record.capture_limit_bytes).unwrap_or(1),
            ))
        });

        let timeout_ms = record
            .timeout_ms
            .and_then(|value| u64::try_from(value).ok());
        let started = Instant::now();
        let (wait_result, cause) = if let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) {
            tokio::select! {
                result = child.wait() => (result, WaitCause::Natural),
                _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => {
                    let _ = self.terminate_live_process(&record);
                    (child.wait().await, WaitCause::TimedOut)
                }
                control = control_rx.recv() => {
                    if control.is_some() {
                        let _ = self.terminate_live_process(&record);
                    }
                    (child.wait().await, WaitCause::Killed)
                }
            }
        } else {
            tokio::select! {
                result = child.wait() => (result, WaitCause::Natural),
                control = control_rx.recv() => {
                    if control.is_some() {
                        let _ = self.terminate_live_process(&record);
                    }
                    (child.wait().await, WaitCause::Killed)
                }
            }
        };

        if let Some(task) = stdout_task {
            let _ = task.await;
        }
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
        *live.control_tx.lock().await = None;

        let (status, reason, exit_code, signal) = match (wait_result, cause) {
            (Ok(status), WaitCause::TimedOut) => (
                "timed_out".to_string(),
                Some("execution_timeout".to_string()),
                status.code().map(i64::from),
                exit_status_signal(&status).map(i64::from),
            ),
            (Ok(status), WaitCause::Killed) => (
                "killed".to_string(),
                Some("cancel_requested".to_string()),
                status.code().map(i64::from),
                exit_status_signal(&status).map(i64::from),
            ),
            (Ok(status), WaitCause::Natural) if status.success() => (
                "completed".to_string(),
                None,
                status.code().map(i64::from),
                exit_status_signal(&status).map(i64::from),
            ),
            (Ok(status), WaitCause::Natural) => (
                "failed".to_string(),
                Some("nonzero_exit".to_string()),
                status.code().map(i64::from),
                exit_status_signal(&status).map(i64::from),
            ),
            (Err(error), _) => (
                "failed".to_string(),
                Some(format!("wait_failed:{error}")),
                None,
                error.raw_os_error().map(i64::from),
            ),
        };
        let stdout_bytes = *live.stdout_bytes.lock().await;
        let stderr_bytes = *live.stderr_bytes.lock().await;
        let stdout_truncated = *live.stdout_truncated.lock().await;
        let stderr_truncated = *live.stderr_truncated.lock().await;
        let ended_at = now_ms();
        let update = ManagedProcessTerminalUpdate {
            status: status.clone(),
            terminal_reason: reason.clone(),
            exit_code,
            signal,
            ended_at,
            execution_duration_ms: Some(
                i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
            ),
            stdout_captured_bytes: i64::try_from(stdout_bytes).unwrap_or(i64::MAX),
            stderr_captured_bytes: i64::try_from(stderr_bytes).unwrap_or(i64::MAX),
            stdout_truncated,
            stderr_truncated,
            completion_required: self.runtime.is_some() && record.owner_session_id.is_some(),
        };
        let Some(terminal) = self
            .persist_terminal_update(&record.process_id, &update)
            .await
        else {
            self.scheduler_notify.notify_one();
            return;
        };
        let event_kind = match terminal.status.as_str() {
            "completed" => "process.completed",
            "timed_out" => "process.timed_out",
            "killed" => "process.killed",
            _ => "process.failed",
        };
        self.append_process_event(
            &record.process_id,
            record.owner_session_id.clone(),
            event_kind,
            RuntimeEventCriticality::Critical,
            json!({
                "process_id": record.process_id,
                "status": terminal.status,
                "reason": terminal.terminal_reason,
                "exit_code": terminal.exit_code,
                "signal": terminal.signal,
                "execution_duration_ms": terminal.execution_duration_ms,
                "stdout_captured_bytes": terminal.stdout_captured_bytes,
                "stderr_captured_bytes": terminal.stderr_captured_bytes,
                "stdout_truncated": terminal.stdout_truncated,
                "stderr_truncated": terminal.stderr_truncated,
            }),
        )
        .await;
        self.completion_notify.notify_one();
        self.scheduler_notify.notify_one();
    }

    async fn persist_terminal_update(
        &self,
        process_id: &str,
        update: &ManagedProcessTerminalUpdate,
    ) -> Option<ManagedProcessRecord> {
        let mut delay = Duration::from_millis(25);
        loop {
            match self.store.terminalize_managed_process(process_id, update) {
                Ok(record) => return record,
                Err(_) => match self.store.get_managed_process(process_id) {
                    Ok(Some(record)) if process_status_is_terminal(&record.status) => {
                        return Some(record);
                    }
                    Ok(None) => return None,
                    Ok(Some(_)) | Err(_) => {
                        tokio::time::sleep(delay).await;
                        delay = delay
                            .saturating_mul(2)
                            .min(TERMINAL_PERSIST_RETRY_MAX_DELAY);
                    }
                },
            }
        }
    }

    fn terminate_live_process(
        &self,
        record: &ManagedProcessRecord,
    ) -> Result<os_process::ProcessTerminationOutcome, RuntimeError> {
        let pid = record.pid.ok_or_else(|| {
            RuntimeError::InvalidState(format!("process {} has no pid", record.process_id))
        })?;
        let pid = u32::try_from(pid).map_err(|_| {
            RuntimeError::InvalidState(format!("process {} has invalid pid", record.process_id))
        })?;
        let identity = record.os_start_identity.as_deref().ok_or_else(|| {
            RuntimeError::InvalidState(format!(
                "process {} has no OS start identity",
                record.process_id
            ))
        })?;
        let outcome = os_process::terminate_process_group(pid, identity).map_err(|error| {
            RuntimeError::Io(format!("failed to terminate process group: {error}"))
        })?;
        if outcome == os_process::ProcessTerminationOutcome::IdentityMismatch {
            return Err(RuntimeError::InvalidState(format!(
                "process {} pid {} identity mismatch; refusing unsafe termination",
                record.process_id, pid
            )));
        }
        Ok(outcome)
    }

    async fn terminalize_spawn_failure(
        &self,
        record: &ManagedProcessRecord,
        reason: &str,
        error: Option<String>,
    ) {
        let ended_at = now_ms();
        let terminal_reason = error
            .as_deref()
            .map(|error| format!("{reason}:{error}"))
            .unwrap_or_else(|| reason.to_string());
        let update = ManagedProcessTerminalUpdate {
            status: "failed".to_string(),
            terminal_reason: Some(terminal_reason.clone()),
            exit_code: None,
            signal: None,
            ended_at,
            execution_duration_ms: None,
            stdout_captured_bytes: i64::try_from(file_len(&record.stdout_path)).unwrap_or(i64::MAX),
            stderr_captured_bytes: i64::try_from(file_len(&record.stderr_path)).unwrap_or(i64::MAX),
            stdout_truncated: record.stdout_truncated,
            stderr_truncated: record.stderr_truncated,
            completion_required: self.runtime.is_some() && record.owner_session_id.is_some(),
        };
        let Some(terminal) = self
            .persist_terminal_update(&record.process_id, &update)
            .await
        else {
            self.scheduler_notify.notify_one();
            return;
        };
        self.append_process_event(
            &record.process_id,
            record.owner_session_id.clone(),
            "process.failed",
            RuntimeEventCriticality::Critical,
            json!({
                "process_id": record.process_id,
                "status": terminal.status,
                "reason": terminal.terminal_reason,
            }),
        )
        .await;
        self.completion_notify.notify_one();
        self.scheduler_notify.notify_one();
    }

    async fn terminalize_running_failure(
        &self,
        record: &ManagedProcessRecord,
        reason: &str,
        error: Option<String>,
    ) {
        self.terminalize_spawn_failure(record, reason, error).await;
    }

    async fn pump_stream<R: AsyncRead + Unpin + Send + 'static>(
        manager: Arc<Self>,
        live: Arc<LiveProcess>,
        process_id: String,
        session_id: Option<String>,
        stream_name: &'static str,
        mut reader: R,
        path: PathBuf,
        capture_limit: usize,
    ) {
        let mut file = match tokio::fs::OpenOptions::new().append(true).open(&path).await {
            Ok(file) => file,
            Err(_) => return,
        };
        let sample_bytes = manager.config.output_event_sample_bytes.max(1);
        let mut buffer = vec![0_u8; 8192];
        let mut emitted_budget = 0_usize;

        loop {
            let read = match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(size) => size,
                Err(_) => break,
            };
            let chunk = &buffer[..read];
            let (bytes_written, truncated_now, total, truncated) = {
                let bytes_counter = if stream_name == "stdout" {
                    &live.stdout_bytes
                } else {
                    &live.stderr_bytes
                };
                let truncated_flag = if stream_name == "stdout" {
                    &live.stdout_truncated
                } else {
                    &live.stderr_truncated
                };
                let mut used = bytes_counter.lock().await;
                let mut truncated = truncated_flag.lock().await;
                let remaining = capture_limit.saturating_sub(*used);
                let to_write = remaining.min(chunk.len());
                let truncated_now = to_write < chunk.len();
                if to_write > 0 {
                    let _ = file.write_all(&chunk[..to_write]).await;
                    *used += to_write;
                }
                if truncated_now {
                    *truncated = true;
                }
                (to_write, truncated_now, *used, *truncated)
            };
            manager.persist_capture_progress(&process_id, stream_name, total, truncated);
            emitted_budget = emitted_budget.saturating_add(read);
            if emitted_budget >= sample_bytes || truncated_now {
                emitted_budget = 0;
                manager
                    .append_process_event(
                        &process_id,
                        session_id.clone(),
                        "process.output",
                        RuntimeEventCriticality::Droppable,
                        json!({
                            "process_id": process_id,
                            "stream": stream_name,
                            "bytes_seen": read,
                            "bytes_written": bytes_written,
                            "captured_bytes": total,
                            "truncated": truncated,
                        }),
                    )
                    .await;
            }
        }
    }

    fn persist_capture_progress(
        &self,
        process_id: &str,
        stream: &str,
        captured_bytes: usize,
        truncated: bool,
    ) {
        let _ = self.store.update_managed_process_capture_progress(
            process_id,
            stream,
            i64::try_from(captured_bytes).unwrap_or(i64::MAX),
            truncated,
            now_ms(),
        );
    }

    async fn live_state(&self, record: &ManagedProcessRecord) -> Arc<LiveProcess> {
        if let Some(existing) = self
            .live_processes
            .read()
            .await
            .get(&record.process_id)
            .cloned()
        {
            return existing;
        }
        let live = Arc::new(LiveProcess::from_record(record));
        self.live_processes
            .write()
            .await
            .insert(record.process_id.clone(), Arc::clone(&live));
        live
    }

    pub(crate) async fn append_process_event(
        &self,
        process_id: &str,
        session_id: Option<String>,
        kind: &str,
        criticality: RuntimeEventCriticality,
        payload: Value,
    ) {
        let event_id = format!("evt_proc_{}_{:032x}", process_id, rand::random::<u128>());
        if let Ok(record) = self.store.append_runtime_event(&NewRuntimeEvent {
            event_id,
            scope: RuntimeEventScope::Process,
            scope_id: process_id.to_string(),
            session_id,
            team_id: None,
            turn_id: None,
            kind: kind.to_string(),
            criticality,
            payload,
            provider: None,
            provider_seq: None,
            created_at: now_ms(),
        }) {
            let _ = self.event_tx.send(record);
        }
    }
}
