use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    NewRuntimeEvent, ProcessDetails, ProcessGetRequest, ProcessKillRequest, ProcessListRequest,
    ProcessLogReadRequest, ProcessLogsChunk, ProcessManager, ProcessRecord, ProcessRunRequest,
    ProcessSummary, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, RuntimeStore,
};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex, OwnedSemaphorePermit, RwLock, Semaphore};

use crate::{exit_status_signal, now_ms, parse_process_sequence, ProcessManagerConfig};

pub struct RuntimeProcessManager {
    store: Arc<dyn RuntimeStore>,
    pub(crate) config: ProcessManagerConfig,
    semaphore: Arc<Semaphore>,
    next_process_id: Arc<AtomicU64>,
    next_event_id: Arc<AtomicU64>,
    processes: Arc<RwLock<HashMap<String, Arc<ManagedProcess>>>>,
    event_tx: broadcast::Sender<runtime_core::RuntimeEventRecord>,
    startup_recovered_processes: Arc<RwLock<Vec<String>>>,
}

#[derive(Debug)]
struct ManagedProcess {
    record: Mutex<ProcessRecord>,
    child: Mutex<Option<tokio::process::Child>>,
    stdout_bytes: Mutex<usize>,
    stderr_bytes: Mutex<usize>,
    stdout_truncated: Mutex<bool>,
    stderr_truncated: Mutex<bool>,
    kill_requested: Mutex<bool>,
    timed_out: Mutex<bool>,
}

impl ManagedProcess {
    fn new(record: ProcessRecord, child: Option<tokio::process::Child>) -> Self {
        Self {
            record: Mutex::new(record),
            child: Mutex::new(child),
            stdout_bytes: Mutex::new(0),
            stderr_bytes: Mutex::new(0),
            stdout_truncated: Mutex::new(false),
            stderr_truncated: Mutex::new(false),
            kill_requested: Mutex::new(false),
            timed_out: Mutex::new(false),
        }
    }
}

impl RuntimeProcessManager {
    pub async fn new(
        store: Arc<dyn RuntimeStore>,
        config: ProcessManagerConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        let _ = store.initialize().await;
        std::fs::create_dir_all(&config.log_dir).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed to create process log dir {}: {error}",
                config.log_dir.display()
            ))
        })?;

        let hydrated = store.hydrate_runtime_state()?;
        let mut processes = HashMap::new();
        let mut max_seq = 0_u64;
        let (event_tx, _) = broadcast::channel(16_384);
        let mut startup_recovered_processes = Vec::new();

        for mut record in hydrated.processes {
            if let Some(seq) = parse_process_sequence(record.id.as_str()) {
                max_seq = max_seq.max(seq);
            }
            if record.status == "running" || record.status == "queued" {
                record.status = "failed".to_string();
                record.ended_at = Some(now_ms());
                store.upsert_process(&record)?;
                startup_recovered_processes.push(record.id.clone());
            }
            processes.insert(
                record.id.clone(),
                Arc::new(ManagedProcess::new(record, None)),
            );
        }

        Ok(Arc::new(Self {
            store,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent.max(1))),
            config,
            next_process_id: Arc::new(AtomicU64::new(max_seq + 1)),
            next_event_id: Arc::new(AtomicU64::new(1)),
            processes: Arc::new(RwLock::new(processes)),
            event_tx,
            startup_recovered_processes: Arc::new(RwLock::new(startup_recovered_processes)),
        }))
    }

    pub async fn startup_recovered_processes(&self) -> Vec<String> {
        self.startup_recovered_processes.read().await.clone()
    }

    async fn append_process_event(
        &self,
        process_id: &str,
        session_id: Option<String>,
        kind: &str,
        criticality: RuntimeEventCriticality,
        payload: Value,
    ) {
        let event_id = format!(
            "evt_proc_{}_{}",
            process_id,
            self.next_event_id.fetch_add(1, Ordering::Relaxed)
        );
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

    async fn process_from_id(&self, process_id: &str) -> Result<Arc<ManagedProcess>, RuntimeError> {
        let processes = self.processes.read().await;
        processes
            .get(process_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("process {process_id}")))
    }

    async fn process_from_pid(&self, pid: i64) -> Result<Arc<ManagedProcess>, RuntimeError> {
        let processes = self.processes.read().await;
        for process in processes.values() {
            let record = process.record.lock().await;
            if record.pid == Some(pid) {
                return Ok(Arc::clone(process));
            }
        }
        Err(RuntimeError::NotFound(format!("process pid {pid}")))
    }

    pub(crate) async fn process_id_from_pid(&self, pid: i64) -> Result<String, RuntimeError> {
        let process = self.process_from_pid(pid).await?;
        let record = process.record.lock().await;
        Ok(record.id.clone())
    }

    async fn ensure_ownership(
        &self,
        process: &ManagedProcess,
        caller_session_id: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let Some(caller_session_id) = caller_session_id else {
            return Ok(());
        };
        let record = process.record.lock().await;
        if record.session_id.as_deref() == Some(caller_session_id) {
            return Ok(());
        }
        Err(RuntimeError::InvalidState(format!(
            "process {} belongs to a different session",
            record.id
        )))
    }

    async fn cleanup_expired_terminal(&self) {
        if self.config.completed_retention_ms == 0 {
            return;
        }
        let now = now_ms();
        let retention_ms = self.config.completed_retention_ms as i64;

        let snapshots = {
            let processes = self.processes.read().await;
            let mut rows = Vec::with_capacity(processes.len());
            for (id, process) in processes.iter() {
                let record = process.record.lock().await;
                rows.push((id.clone(), record.status.clone(), record.ended_at));
            }
            rows
        };

        let mut to_remove = Vec::new();
        for (id, status, ended_at) in snapshots {
            if !matches!(
                status.as_str(),
                "completed" | "failed" | "timed_out" | "killed"
            ) {
                continue;
            }
            if let Some(ended_at) = ended_at {
                if now.saturating_sub(ended_at) >= retention_ms {
                    to_remove.push(id);
                }
            }
        }

        if !to_remove.is_empty() {
            let mut processes = self.processes.write().await;
            for id in to_remove {
                processes.remove(id.as_str());
            }
        }
    }

    async fn list_process_entries(
        &self,
        caller_session_id: Option<&str>,
        include_completed: bool,
    ) -> Result<Vec<ProcessSummary>, RuntimeError> {
        self.cleanup_expired_terminal().await;

        let processes = self.processes.read().await;
        let mut rows = Vec::new();
        for process in processes.values() {
            let record = process.record.lock().await;
            if let Some(caller_session_id) = caller_session_id {
                if record.session_id.as_deref() != Some(caller_session_id) {
                    continue;
                }
            }
            if !include_completed
                && matches!(
                    record.status.as_str(),
                    "completed" | "failed" | "timed_out" | "killed"
                )
            {
                continue;
            }
            rows.push(ProcessSummary {
                process_id: record.id.clone(),
                session_id: record.session_id.clone(),
                pid: record.pid,
                status: record.status.clone(),
                command: record.command.clone(),
                cwd: record.cwd.clone(),
                started_at: record.started_at,
                ended_at: record.ended_at,
            });
        }
        rows.sort_by(|left, right| right.started_at.cmp(&left.started_at));
        Ok(rows)
    }

    async fn detail_from_process(process: &ManagedProcess) -> ProcessDetails {
        let record = process.record.lock().await;
        let stdout_bytes = *process.stdout_bytes.lock().await;
        let stderr_bytes = *process.stderr_bytes.lock().await;
        let stdout_truncated = *process.stdout_truncated.lock().await;
        let stderr_truncated = *process.stderr_truncated.lock().await;

        ProcessDetails {
            process: ProcessSummary {
                process_id: record.id.clone(),
                session_id: record.session_id.clone(),
                pid: record.pid,
                status: record.status.clone(),
                command: record.command.clone(),
                cwd: record.cwd.clone(),
                started_at: record.started_at,
                ended_at: record.ended_at,
            },
            exit_code: record.exit_code,
            signal: record.signal,
            timeout_ms: record.timeout_ms,
            stdout_path: record.stdout_path.clone(),
            stderr_path: record.stderr_path.clone(),
            stdout_bytes,
            stderr_bytes,
            stdout_truncated,
            stderr_truncated,
        }
    }

    async fn run_lifecycle(
        self: Arc<Self>,
        process: Arc<ManagedProcess>,
        _spawn_permit: OwnedSemaphorePermit,
    ) {
        let (mut child, process_id, session_id, stdout_path, stderr_path, timeout_ms) = {
            let mut child_lock = process.child.lock().await;
            let Some(child) = child_lock.take() else {
                return;
            };
            let record = process.record.lock().await;
            (
                child,
                record.id.clone(),
                record.session_id.clone(),
                record
                    .stdout_path
                    .as_deref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| {
                        self.config
                            .log_dir
                            .join(format!("{}.stdout.log", record.id))
                    }),
                record
                    .stderr_path
                    .as_deref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| {
                        self.config
                            .log_dir
                            .join(format!("{}.stderr.log", record.id))
                    }),
                record.timeout_ms.map(|value| value as u64),
            )
        };

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let stdout_task = stdout.map(|stream| {
            tokio::spawn(Self::pump_stream(
                Arc::clone(&self),
                Arc::clone(&process),
                process_id.clone(),
                session_id.clone(),
                "stdout",
                stream,
                stdout_path.clone(),
            ))
        });
        let stderr_task = stderr.map(|stream| {
            tokio::spawn(Self::pump_stream(
                Arc::clone(&self),
                Arc::clone(&process),
                process_id.clone(),
                session_id.clone(),
                "stderr",
                stream,
                stderr_path.clone(),
            ))
        });

        let timeout = timeout_ms.unwrap_or(self.config.default_timeout_ms).max(1);
        let wait_result = tokio::select! {
            result = child.wait() => result.map(|status| (status.code(), exit_status_signal(&status))),
            _ = tokio::time::sleep(std::time::Duration::from_millis(timeout)) => {
                {
                    let mut timed_out = process.timed_out.lock().await;
                    *timed_out = true;
                }
                let _ = child.start_kill();
                child.wait().await.map(|status| (status.code(), exit_status_signal(&status)))
            }
        };

        if let Some(task) = stdout_task {
            let _ = task.await;
        }
        if let Some(task) = stderr_task {
            let _ = task.await;
        }

        let (status, exit_code, signal) = match wait_result {
            Ok((code, signal)) => {
                let timed_out = *process.timed_out.lock().await;
                let killed = *process.kill_requested.lock().await;
                if timed_out {
                    ("timed_out".to_string(), code, signal)
                } else if killed {
                    ("killed".to_string(), code, signal)
                } else if code == Some(0) {
                    ("completed".to_string(), code, signal)
                } else {
                    ("failed".to_string(), code, signal)
                }
            }
            Err(error) => ("failed".to_string(), None, error.raw_os_error()),
        };

        {
            let mut record = process.record.lock().await;
            record.status = status.clone();
            record.exit_code = exit_code.map(i64::from);
            record.signal = signal.map(i64::from);
            record.ended_at = Some(now_ms());
            let _ = self.store.upsert_process(&record);
        }

        let event_kind = match status.as_str() {
            "completed" => "process.completed",
            "timed_out" => "process.timed_out",
            "killed" => "process.killed",
            _ => "process.failed",
        };

        self.append_process_event(
            process_id.as_str(),
            session_id,
            event_kind,
            RuntimeEventCriticality::Critical,
            json!({
                "process_id": process_id,
                "status": status,
                "exit_code": exit_code,
                "signal": signal,
            }),
        )
        .await;
    }

    async fn pump_stream<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        manager: Arc<Self>,
        process: Arc<ManagedProcess>,
        process_id: String,
        session_id: Option<String>,
        stream_name: &'static str,
        mut reader: R,
        path: PathBuf,
    ) {
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(_) => return,
        };

        let max_bytes = manager.config.max_output_bytes_per_process;
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

            let (bytes_written, truncated_now) = {
                let bytes_counter = if stream_name == "stdout" {
                    &process.stdout_bytes
                } else {
                    &process.stderr_bytes
                };
                let mut used = bytes_counter.lock().await;
                let remaining = max_bytes.saturating_sub(*used);
                let to_write = remaining.min(chunk.len());
                let truncated_now = to_write < chunk.len();

                if to_write > 0 {
                    let _ =
                        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk[..to_write]).await;
                    *used += to_write;
                }

                if truncated_now {
                    let truncated_flag = if stream_name == "stdout" {
                        &process.stdout_truncated
                    } else {
                        &process.stderr_truncated
                    };
                    let mut truncated = truncated_flag.lock().await;
                    *truncated = true;
                }

                (to_write, truncated_now)
            };

            emitted_budget = emitted_budget.saturating_add(read);
            if emitted_budget >= sample_bytes || truncated_now {
                emitted_budget = 0;
                manager
                    .append_process_event(
                        process_id.as_str(),
                        session_id.clone(),
                        "process.output",
                        RuntimeEventCriticality::Droppable,
                        json!({
                            "process_id": process_id,
                            "stream": stream_name,
                            "bytes_seen": read,
                            "bytes_written": bytes_written,
                            "truncated": truncated_now,
                        }),
                    )
                    .await;
            }
        }
    }

    async fn teardown_untracked_child(child: &mut tokio::process::Child) {
        let _ = child.start_kill();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await;
    }
}

#[async_trait]
impl ProcessManager for RuntimeProcessManager {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn run_process(
        &self,
        request: ProcessRunRequest,
    ) -> Result<ProcessDetails, RuntimeError> {
        if !self.config.enabled {
            return Err(RuntimeError::Unsupported(
                "gg_process tools are disabled".to_string(),
            ));
        }

        let command = request.command.trim();
        if command.is_empty() {
            return Err(RuntimeError::InvalidState(
                "command cannot be empty".to_string(),
            ));
        }
        let spawn_permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::InvalidState("process semaphore closed".to_string()))?;

        let process_sequence = self.next_process_id.fetch_add(1, Ordering::Relaxed);
        let process_id = format!("proc_{process_sequence}");
        let stdout_path = self.config.log_dir.join(format!("{process_id}.stdout.log"));
        let stderr_path = self.config.log_dir.join(format!("{process_id}.stderr.log"));

        let cwd = request
            .cwd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let mut proc = if self.config.allow_shell {
            let mut process = Command::new("sh");
            process.arg("-lc");
            process.arg(command);
            process
        } else {
            let mut split = command.split_whitespace();
            let executable = split
                .next()
                .ok_or_else(|| RuntimeError::InvalidState("command cannot be empty".to_string()))?;
            let mut process = Command::new(executable);
            for arg in split {
                process.arg(arg);
            }
            process
        };

        if let Some(cwd) = cwd.as_deref() {
            proc.current_dir(cwd);
        }

        proc.kill_on_drop(true);
        proc.stdout(std::process::Stdio::piped());
        proc.stderr(std::process::Stdio::piped());

        let mut child = proc
            .spawn()
            .map_err(|error| RuntimeError::Io(format!("failed to spawn process: {error}")))?;

        let pid = child.id().map(i64::from);
        let started_at = now_ms();
        let record = ProcessRecord {
            id: process_id.clone(),
            session_id: request.caller_session_id.clone(),
            tool_call_id: request.tool_call_id,
            pid,
            command: json!({ "command": command }),
            cwd: cwd.clone(),
            status: "running".to_string(),
            exit_code: None,
            signal: None,
            stdout_path: Some(stdout_path.display().to_string()),
            stderr_path: Some(stderr_path.display().to_string()),
            started_at,
            ended_at: None,
            timeout_ms: Some(request.timeout_ms.unwrap_or(self.config.default_timeout_ms) as i64),
        };

        if let Err(error) = self.store.upsert_process(&record) {
            Self::teardown_untracked_child(&mut child).await;
            return Err(error);
        }

        let managed = Arc::new(ManagedProcess::new(record, Some(child)));
        {
            let mut processes = self.processes.write().await;
            processes.insert(process_id.clone(), Arc::clone(&managed));
        }

        self.append_process_event(
            process_id.as_str(),
            request.caller_session_id,
            "process.started",
            RuntimeEventCriticality::Critical,
            json!({
                "process_id": process_id,
                "pid": pid,
                "cwd": cwd,
            }),
        )
        .await;

        let manager = Arc::new(self.clone());
        tokio::spawn(async move {
            manager.run_lifecycle(managed, spawn_permit).await;
        });

        let process = self.process_from_id(process_id.as_str()).await?;
        Ok(Self::detail_from_process(process.as_ref()).await)
    }

    async fn list_processes(
        &self,
        request: ProcessListRequest,
    ) -> Result<Vec<ProcessSummary>, RuntimeError> {
        self.list_process_entries(
            request.caller_session_id.as_deref(),
            request.include_completed,
        )
        .await
    }

    async fn get_process(
        &self,
        request: ProcessGetRequest,
    ) -> Result<ProcessDetails, RuntimeError> {
        let process = self.process_from_id(request.process_id.as_str()).await?;
        self.ensure_ownership(process.as_ref(), request.caller_session_id.as_deref())
            .await?;
        Ok(Self::detail_from_process(process.as_ref()).await)
    }

    async fn read_process_logs(
        &self,
        request: ProcessLogReadRequest,
    ) -> Result<Vec<ProcessLogsChunk>, RuntimeError> {
        let process = self.process_from_id(request.process_id.as_str()).await?;
        self.ensure_ownership(process.as_ref(), request.caller_session_id.as_deref())
            .await?;

        let details = Self::detail_from_process(process.as_ref()).await;
        let mut streams = Vec::new();
        match request.stream.as_deref() {
            Some("stdout") => streams.push((
                "stdout",
                details.stdout_path.clone(),
                details.stdout_truncated,
            )),
            Some("stderr") => streams.push((
                "stderr",
                details.stderr_path.clone(),
                details.stderr_truncated,
            )),
            Some(other) => {
                return Err(RuntimeError::InvalidState(format!(
                    "unsupported stream {}",
                    other
                )))
            }
            None => {
                streams.push((
                    "stdout",
                    details.stdout_path.clone(),
                    details.stdout_truncated,
                ));
                streams.push((
                    "stderr",
                    details.stderr_path.clone(),
                    details.stderr_truncated,
                ));
            }
        }

        let mut chunks = Vec::new();
        for (stream, path, stream_truncated) in streams {
            let Some(path) = path else {
                continue;
            };
            let content = std::fs::read_to_string(Path::new(path.as_str())).unwrap_or_default();
            let lines = content.lines().collect::<Vec<_>>();
            let head = request.head_lines.unwrap_or(0);
            let tail = request.tail_lines.unwrap_or(80);

            let mut out = String::new();
            let mut truncated = false;

            if head > 0 {
                for line in lines.iter().take(head) {
                    out.push_str(line);
                    out.push('\n');
                }
            }

            let tail_start = lines.len().saturating_sub(tail);
            if head > 0 && tail_start > head {
                truncated = true;
                out.push_str("...\n");
            }

            for line in lines.iter().skip(tail_start) {
                out.push_str(line);
                out.push('\n');
            }

            if request.max_bytes.is_some() {
                let max_bytes = request.max_bytes.unwrap_or(64 * 1024);
                if out.as_bytes().len() > max_bytes {
                    let truncated_bytes = &out.as_bytes()[out.as_bytes().len() - max_bytes..];
                    out = String::from_utf8_lossy(truncated_bytes).to_string();
                    truncated = true;
                }
            }

            chunks.push(ProcessLogsChunk {
                process_id: details.process.process_id.clone(),
                stream: stream.to_string(),
                bytes: out.as_bytes().len(),
                content: out,
                head_lines: head,
                tail_lines: tail,
                truncated: truncated || stream_truncated,
            });
        }

        Ok(chunks)
    }

    async fn kill_process(
        &self,
        request: ProcessKillRequest,
    ) -> Result<ProcessDetails, RuntimeError> {
        let process = self.process_from_id(request.process_id.as_str()).await?;
        self.ensure_ownership(process.as_ref(), request.caller_session_id.as_deref())
            .await?;

        {
            let mut kill_requested = process.kill_requested.lock().await;
            *kill_requested = true;
        }

        let mut killed = false;
        {
            let mut child = process.child.lock().await;
            if let Some(child) = child.as_mut() {
                let _ = child.start_kill();
                killed = true;
            }
        }

        if killed {
            let record = process.record.lock().await;
            self.append_process_event(
                record.id.as_str(),
                record.session_id.clone(),
                "process.kill_requested",
                RuntimeEventCriticality::Critical,
                json!({
                    "reason": request.reason.unwrap_or_else(|| "requested".to_string()),
                    "process_id": record.id,
                }),
            )
            .await;
        }

        Ok(Self::detail_from_process(process.as_ref()).await)
    }

    async fn replay_events(
        &self,
        process_id: String,
        caller_session_id: Option<String>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<runtime_core::RuntimeEventRecord>, RuntimeError> {
        let process = self.process_from_id(process_id.as_str()).await?;
        self.ensure_ownership(process.as_ref(), caller_session_id.as_deref())
            .await?;
        self.store.list_runtime_events(
            Some((RuntimeEventScope::Process, process_id.as_str())),
            after_seq,
            limit.max(1),
        )
    }

    fn subscribe_events(&self) -> broadcast::Receiver<runtime_core::RuntimeEventRecord> {
        self.event_tx.subscribe()
    }
}

impl Clone for RuntimeProcessManager {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            config: self.config.clone(),
            semaphore: Arc::clone(&self.semaphore),
            next_process_id: Arc::clone(&self.next_process_id),
            next_event_id: Arc::clone(&self.next_event_id),
            processes: Arc::clone(&self.processes),
            event_tx: self.event_tx.clone(),
            startup_recovered_processes: Arc::clone(&self.startup_recovered_processes),
        }
    }
}
