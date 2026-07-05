use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use runtime_core::{
    CreateSessionInput, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent,
    ProcessDetails, ProcessGetRequest, ProcessKillRequest, ProcessListRequest,
    ProcessLogReadRequest, ProcessLogsChunk, ProcessManager, ProcessRecord, ProcessRunRequest,
    ProcessSummary, ProviderKind, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
    RuntimeSessionManager, RuntimeStore, SessionRecord, TeamBroadcastRequest,
    TeamCancelMessageRequest, TeamCommsService, TeamCreateRequest, TeamDeliveryRecord,
    TeamGetDeliveriesRequest, TeamInterruptAllRequest, TeamInterruptAllResponse, TeamJoinRequest,
    TeamListMessagesRequest, TeamListMessagesResponse, TeamMemberRecord, TeamMemberSpawnRequest,
    TeamMemberSpawnResponse, TeamMemberSpawnWorktreeInput, TeamMessageAck, TeamMessageRecord,
    TeamRemoveMemberRequest, TeamRetryDeliveryRequest, TeamSendDirectRequest, TeamSetLeadRequest,
    TeamViewSnapshotRequest, TeamViewSnapshotResponse, TeamWithMembers, ToolGateway,
    ToolInvokeRequest, WorktreeClaimRequest, WorktreeClaimResponse, WorktreeCleanupRequest,
    WorktreeCleanupResponse, WorktreeCreateRequest, WorktreeCreateResponse,
    WorktreeMemberRemovedRequest, WorktreeMemberRemovedResponse, WorktreeReleaseRequest,
    WorktreeReleaseResponse, WorktreeService,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex, OwnedSemaphorePermit, RwLock, Semaphore};

const GG_PROCESS_RUN: &str = "gg_process_run";
const GG_PROCESS_STATUS: &str = "gg_process_status";
const GG_PROCESS_LOGS: &str = "gg_process_logs";
const GG_PROCESS_KILL: &str = "gg_process_kill";
const GG_TEAM_STATUS: &str = "gg_team_status";
const GG_TEAM_MESSAGE: &str = "gg_team_message";
const GG_TEAM_MANAGE: &str = "gg_team_manage";
const GG_TEAM_ADD_IDEMPOTENCY_CACHE_TTL_SECS: u64 = 10 * 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessManagerConfig {
    pub enabled: bool,
    pub max_concurrent: usize,
    pub default_timeout_ms: u64,
    pub max_output_bytes_per_process: usize,
    pub allow_shell: bool,
    pub completed_retention_ms: u64,
    pub output_event_sample_bytes: usize,
    pub log_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamCommsConfig {
    pub enabled: bool,
    pub max_pending_deliveries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeServiceConfig {
    pub enabled: bool,
    pub root_dir: String,
    pub init_script_path: String,
    pub deletion_policy_default: String,
}

pub struct RuntimeProcessManager {
    store: Arc<dyn RuntimeStore>,
    config: ProcessManagerConfig,
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

impl RuntimeProcessManager {
    async fn teardown_untracked_child(child: &mut tokio::process::Child) {
        let _ = child.start_kill();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await;
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

pub struct RuntimeToolGateway {
    process_manager: Arc<RuntimeProcessManager>,
    runtime: Option<Arc<RuntimeSessionManager>>,
    team_comms: Arc<dyn TeamCommsService>,
    worktrees: Arc<dyn WorktreeService>,
    team_policy: TeamMcpPolicy,
    team_manage_add_idempotency: Arc<Mutex<HashMap<String, ManageAddIdempotencyEntry>>>,
}

pub struct RuntimeToolGatewayDeps {
    pub process_manager: Arc<RuntimeProcessManager>,
    pub runtime: Option<Arc<RuntimeSessionManager>>,
    pub team_comms: Arc<dyn TeamCommsService>,
    pub worktrees: Arc<dyn WorktreeService>,
    pub team_policy: TeamMcpPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamMcpPolicy {
    pub enabled: bool,
    pub non_lead_can_add_members: bool,
    pub non_lead_can_remove_members: bool,
}

#[derive(Debug, Clone)]
struct ManageAddIdempotencyEntry {
    inserted_at: Instant,
    completed_success: Option<Value>,
}

impl Default for TeamMcpPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            non_lead_can_add_members: false,
            non_lead_can_remove_members: false,
        }
    }
}

impl RuntimeToolGateway {
    pub fn new(deps: RuntimeToolGatewayDeps) -> Self {
        Self {
            process_manager: deps.process_manager,
            runtime: deps.runtime,
            team_comms: deps.team_comms,
            worktrees: deps.worktrees,
            team_policy: deps.team_policy,
            team_manage_add_idempotency: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn process_only_for_tests(process_manager: Arc<RuntimeProcessManager>) -> Self {
        Self::new(RuntimeToolGatewayDeps {
            process_manager,
            runtime: None,
            team_comms: Arc::new(StubTeamCommsService::new(TeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1_000,
            })),
            worktrees: Arc::new(StubWorktreeService::new(WorktreeServiceConfig {
                enabled: true,
                root_dir: String::new(),
                init_script_path: String::new(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            })),
            team_policy: TeamMcpPolicy::default(),
        })
    }

    pub fn team_policy(&self) -> &TeamMcpPolicy {
        &self.team_policy
    }

    async fn invoke_process_tool(&self, request: ToolInvokeRequest) -> Value {
        let tool_name = request.tool_name.trim();
        let args = match request.args {
            Value::Object(map) => map,
            _ => {
                return json!({
                    "ok": false,
                    "error": {
                        "code": "bad_request",
                        "message": "tool args must be an object"
                    }
                });
            }
        };

        let result = match tool_name {
            GG_PROCESS_RUN => {
                let command = args
                    .get("command")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                let cwd = args.get("cwd").and_then(Value::as_str).map(str::to_string);
                let timeout_ms = args.get("timeout_ms").and_then(Value::as_u64);
                self.process_manager
                    .run_process(ProcessRunRequest {
                        caller_session_id: Some(request.caller_session_id.clone()),
                        tool_call_id: request.invocation_id.clone(),
                        command,
                        cwd,
                        timeout_ms,
                    })
                    .await
                    .map(|value| json!(value))
            }
            GG_PROCESS_STATUS => {
                if let Some(process_id) = args.get("process_id").and_then(Value::as_str) {
                    self.process_manager
                        .get_process(ProcessGetRequest {
                            process_id: process_id.to_string(),
                            caller_session_id: Some(request.caller_session_id.clone()),
                        })
                        .await
                        .map(|value| json!(value))
                } else if let Some(pid) = args.get("pid").and_then(Value::as_i64) {
                    let process = self.process_manager.process_from_pid(pid).await;
                    match process {
                        Ok(process) => {
                            let record = process.record.lock().await;
                            self.process_manager
                                .get_process(ProcessGetRequest {
                                    process_id: record.id.clone(),
                                    caller_session_id: Some(request.caller_session_id.clone()),
                                })
                                .await
                                .map(|value| json!(value))
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    self.process_manager
                        .list_processes(ProcessListRequest {
                            caller_session_id: Some(request.caller_session_id.clone()),
                            include_completed: false,
                        })
                        .await
                        .map(|rows| json!({ "running": rows }))
                }
            }
            GG_PROCESS_LOGS => {
                let process_id = args
                    .get("process_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                let stream = args
                    .get("stream")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let head_lines = args
                    .get("head_lines")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize);
                let tail_lines = args
                    .get("tail_lines")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize);
                self.process_manager
                    .read_process_logs(ProcessLogReadRequest {
                        process_id,
                        caller_session_id: Some(request.caller_session_id.clone()),
                        stream,
                        head_lines,
                        tail_lines,
                        max_bytes: None,
                    })
                    .await
                    .map(|rows| json!({ "logs": rows }))
            }
            GG_PROCESS_KILL => {
                let process_id = args
                    .get("process_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                self.process_manager
                    .kill_process(ProcessKillRequest {
                        process_id,
                        caller_session_id: Some(request.caller_session_id),
                        reason: Some("gg_process_kill".to_string()),
                    })
                    .await
                    .map(|value| json!(value))
            }
            _ => Err(RuntimeError::Unsupported(format!(
                "Unsupported gg_process tool: {tool_name}"
            ))),
        };

        match result {
            Ok(result) => json!({ "ok": true, "result": result }),
            Err(error) => json!({
                "ok": false,
                "error": {
                    "code": "tool_failed",
                    "message": error.to_string(),
                }
            }),
        }
    }

    async fn invoke_team_tool(&self, request: ToolInvokeRequest) -> Value {
        if !self.team_policy.enabled {
            return json!({
                "ok": false,
                "error": {
                    "code": "feature_disabled",
                    "message": "gg_team MCP tools are disabled"
                }
            });
        }

        let tool_name = request.tool_name.trim().to_string();
        let args = match request.args {
            Value::Object(map) => map,
            _ => {
                return team_tool_error("bad_request", "tool args must be an object");
            }
        };

        let result = match tool_name.as_str() {
            GG_TEAM_STATUS => {
                self.invoke_team_status(request.caller_session_id.as_str(), &args)
                    .await
            }
            GG_TEAM_MESSAGE => {
                self.invoke_team_message(
                    request.caller_session_id.as_str(),
                    request.invocation_id,
                    &args,
                )
                .await
            }
            GG_TEAM_MANAGE => {
                self.invoke_team_manage(
                    request.caller_session_id.as_str(),
                    request.invocation_id,
                    &args,
                )
                .await
            }
            _ => Err(TeamToolFailure::new(
                "bad_request",
                format!("Unsupported gg_team tool: {}", tool_name),
            )),
        };

        match result {
            Ok(result) => json!({ "ok": true, "result": result }),
            Err(error) => team_tool_error(error.code, error.message),
        }
    }

    async fn invoke_team_status(
        &self,
        caller_session_id: &str,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        reject_team_tool_fields(
            args,
            &["caller_agent_id", "sender", "sender_agent_id", "agent_id"],
        )?;
        let team_id = required_string_arg(args, "team_id")?;
        let team = self
            .team_comms
            .get_team(team_id.as_str())
            .await
            .map_err(TeamToolFailure::from_runtime)?;
        ensure_team_member(&team, caller_session_id)?;
        let messages = self
            .team_comms
            .list_messages(TeamListMessagesRequest {
                team_id: team.team.id.clone(),
                cursor: None,
                limit: Some(100),
            })
            .await
            .map(|page| page.messages)
            .unwrap_or_default();

        let mut members = Vec::with_capacity(team.members.len());
        for member in &team.members {
            members.push(self.status_member_row(member, &messages).await);
        }

        Ok(json!({
            "team_id": team.team.id,
            "lead_agent_id": team.team.lead_agent_id,
            "generated_at_ms": now_ms(),
            "members": members,
        }))
    }

    async fn status_member_row(
        &self,
        member: &TeamMemberRecord,
        messages: &[TeamMessageRecord],
    ) -> Value {
        let session = match self.runtime.as_ref() {
            Some(runtime) => runtime.get_session(member.agent_id.as_str()).await.ok(),
            None => None,
        };
        let worktree = match member.worktree_id.as_deref() {
            Some(worktree_id) => self.worktrees.get_worktree(worktree_id).await.ok(),
            None => None,
        };
        let last_message = latest_message_for_member(member.agent_id.as_str(), messages);
        let last_message_at = last_message.map(|message| message.created_at).unwrap_or(0);
        let session_updated_at = session
            .as_ref()
            .map(|session| session.updated_at)
            .unwrap_or(0);
        let state = status_state_for_session(session.as_ref());

        json!({
            "agent_id": member.agent_id,
            "session_id": member.agent_id,
            "title": member.title,
            "state": state,
            "last_activity_at_ms": member.joined_at.max(session_updated_at).max(last_message_at),
            "last_message": last_message.map(member_last_message_output),
            "context_window_remaining_percentage": Value::Null,
            "worktree_cwd": worktree.as_ref().map(|record| record.worktree_cwd.clone()),
            "worktree_name": worktree.as_ref().map(|record| record.worktree_name.clone()),
            "added_by": member.added_by,
        })
    }

    async fn invoke_team_message(
        &self,
        caller_session_id: &str,
        invocation_id: Option<String>,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        reject_team_tool_fields(args, &["caller_agent_id", "sender", "sender_agent_id"])?;
        let team_id = required_string_arg(args, "team_id")?;
        let recipient_agent_id = required_string_arg(args, "recipient_agent_id")?;
        let message = required_string_arg(args, "message")?;
        let image_paths = optional_string_array_arg(args, "image_paths")?;
        let image_count = image_paths.len();
        let input = json!([{ "type": "text", "text": message }]);

        let (scope, ack) = if recipient_agent_id.eq_ignore_ascii_case("broadcast") {
            let ack = self
                .team_comms
                .broadcast(TeamBroadcastRequest {
                    team_id,
                    sender_agent_id: caller_session_id.to_string(),
                    input,
                    image_paths: image_paths.clone(),
                    priority: "normal".to_string(),
                    policy: "non_interrupting".to_string(),
                    include_sender: false,
                    correlation_id: None,
                    idempotency_key: invocation_id,
                })
                .await
                .map_err(TeamToolFailure::from_runtime)?;
            ("broadcast", ack)
        } else {
            let ack = self
                .team_comms
                .send_direct(TeamSendDirectRequest {
                    team_id,
                    sender_agent_id: caller_session_id.to_string(),
                    recipient_agent_id,
                    input,
                    image_paths: image_paths.clone(),
                    priority: "normal".to_string(),
                    policy: "non_interrupting".to_string(),
                    correlation_id: None,
                    reply_to_message_id: None,
                    idempotency_key: invocation_id,
                })
                .await
                .map_err(TeamToolFailure::from_runtime)?;
            ("direct", ack)
        };

        let mut delivery_ids = ack
            .deliveries
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        delivery_ids.sort();

        let mut result = json!({
            "message_id": ack.message.id,
            "delivery_ids": delivery_ids,
            "recipient_count": ack.deliveries.len(),
            "scope": scope,
            "image_count": image_count,
        });
        if image_count > 0 {
            if let Some(object) = result.as_object_mut() {
                object.insert("image_paths".to_string(), json!(image_paths));
            }
        }
        Ok(result)
    }

    async fn invoke_team_manage(
        &self,
        caller_session_id: &str,
        invocation_id: Option<String>,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        reject_team_tool_fields(args, &["caller_agent_id", "sender", "sender_agent_id"])?;
        let team_id = required_string_arg(args, "team_id")?;
        let remove_agent_ids = optional_string_array_arg(args, "remove_agent_ids")?;
        if !remove_agent_ids.is_empty() {
            return self
                .invoke_team_manage_remove(caller_session_id, team_id, remove_agent_ids)
                .await;
        }

        if let Some(invocation_id) = invocation_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            let cache_key = format!("v1:{caller_session_id}:{GG_TEAM_MANAGE}:{invocation_id}");
            if let Some(cached) = self
                .begin_manage_add_idempotent_execution(&cache_key)
                .await?
            {
                return Ok(cached);
            }
            let result = self
                .invoke_team_manage_add(caller_session_id, team_id, args)
                .await;
            match result {
                Ok(value) => {
                    self.complete_manage_add_idempotent_execution(&cache_key, &value)
                        .await;
                    Ok(value)
                }
                Err(error) => {
                    self.abort_manage_add_idempotent_execution(&cache_key).await;
                    Err(error)
                }
            }
        } else {
            self.invoke_team_manage_add(caller_session_id, team_id, args)
                .await
        }
    }

    async fn invoke_team_manage_add(
        &self,
        caller_session_id: &str,
        team_id: String,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        self.ensure_caller_can_manage_membership(
            team_id.as_str(),
            caller_session_id,
            self.team_policy.non_lead_can_add_members,
            "add members to",
        )
        .await?;

        let title = optional_string_arg(args, "title")?;
        let prompt = optional_string_arg(args, "prompt")?;
        let model_preset = optional_string_arg(args, "model_preset")?;
        if model_preset.is_some() {
            return Err(TeamToolFailure::new(
                "bad_request",
                "model_preset is not supported by this runtime gateway; omit it to inherit the caller session model",
            ));
        }
        let image_paths = optional_string_array_arg(args, "image_paths")?;
        if !image_paths.is_empty() {
            return Err(TeamToolFailure::new(
                "bad_request",
                "image_paths are not supported for gg_team_manage add in this runtime gateway",
            ));
        }
        let creator_compaction_subscription =
            optional_creator_compaction_subscription_arg(args, "creator_compaction_subscription")?;
        let worktree_name = optional_string_arg(args, "worktree_name")?;
        let use_existing_worktree =
            optional_bool_arg(args, "use_existing_worktree")?.unwrap_or(false);
        if use_existing_worktree && worktree_name.is_none() {
            return Err(TeamToolFailure::new(
                "bad_request",
                "worktree_name is required when use_existing_worktree is true",
            ));
        }
        let worktree = worktree_name.map(|name| TeamMemberSpawnWorktreeInput {
            mode: Some(if use_existing_worktree {
                "reuse".to_string()
            } else {
                "create".to_string()
            }),
            name: Some(name),
            branch_prefix: None,
            base_ref: None,
            run_init_script: None,
        });

        let spawn = self
            .worktrees
            .spawn_team_member(TeamMemberSpawnRequest {
                team_id: team_id.clone(),
                source_session_id: caller_session_id.to_string(),
                provider: None,
                model: None,
                title,
                prompt,
                permission_mode: None,
                metadata: None,
                worktree,
                creator_agent_id: Some(caller_session_id.to_string()),
                creator_compaction_subscription,
            })
            .await
            .map_err(TeamToolFailure::from_runtime)?;

        Ok(json!({
            "operation": "add",
            "team_id": team_id,
            "operation_id": spawn.operation_id,
            "spawned_agent_id": spawn.spawned_session.id,
            "spawned_session": spawn.spawned_session,
            "spawned_member": spawn.spawned_member,
            "team": spawn.team,
            "worktree": spawn.worktree,
            "worktree_assignment_mode": spawn.worktree_assignment_mode,
            "worktree_created_by_operation": spawn.worktree_created_by_operation,
            "onboarding": spawn.onboarding,
            "journal_stage": spawn.journal_stage,
        }))
    }

    async fn invoke_team_manage_remove(
        &self,
        caller_session_id: &str,
        team_id: String,
        remove_agent_ids: Vec<String>,
    ) -> Result<Value, TeamToolFailure> {
        self.ensure_caller_can_manage_membership(
            team_id.as_str(),
            caller_session_id,
            self.team_policy.non_lead_can_remove_members,
            "remove members from",
        )
        .await?;

        let mut results = Vec::with_capacity(remove_agent_ids.len());
        for agent_id in remove_agent_ids {
            let removal = self
                .team_comms
                .remove_team_member(TeamRemoveMemberRequest {
                    team_id: team_id.clone(),
                    agent_id: agent_id.clone(),
                })
                .await;
            match removal {
                Ok(team) => {
                    let cleanup = self
                        .worktrees
                        .on_member_removed(WorktreeMemberRemovedRequest {
                            team_id: team_id.clone(),
                            agent_id: agent_id.clone(),
                            removed_by: Some(caller_session_id.to_string()),
                        })
                        .await;
                    let cleanup_output = match cleanup {
                        Ok(cleanup) => json!({
                            "ok": true,
                            "released_claim_count": cleanup.released_claims.len(),
                            "cleanup_result_count": cleanup.cleanup_results.len(),
                            "diagnostic_count": cleanup.diagnostics.len(),
                            "released_claims": cleanup.released_claims,
                            "cleanup_results": cleanup.cleanup_results,
                            "diagnostics": cleanup.diagnostics,
                        }),
                        Err(error) => json!({
                            "ok": false,
                            "error": error.to_string(),
                        }),
                    };
                    results.push(json!({
                        "agent_id": agent_id,
                        "ok": true,
                        "team": team,
                        "cleanup": cleanup_output,
                    }));
                }
                Err(error) => {
                    let error_code = team_tool_error_code_for_runtime(&error);
                    results.push(json!({
                        "agent_id": agent_id,
                        "ok": false,
                        "error": {
                            "code": error_code,
                            "message": error.to_string(),
                        }
                    }));
                }
            }
        }

        let removed_count = results
            .iter()
            .filter(|result| result.get("ok").and_then(Value::as_bool) == Some(true))
            .count();
        Ok(json!({
            "operation": "remove",
            "team_id": team_id,
            "removed_count": removed_count,
            "failed_count": results.len().saturating_sub(removed_count),
            "results": results,
        }))
    }

    async fn ensure_caller_can_manage_membership(
        &self,
        team_id: &str,
        caller_session_id: &str,
        allow_non_lead: bool,
        action: &str,
    ) -> Result<TeamWithMembers, TeamToolFailure> {
        let team = self
            .team_comms
            .get_team(team_id)
            .await
            .map_err(TeamToolFailure::from_runtime)?;
        ensure_team_member(&team, caller_session_id)?;
        if team.team.lead_agent_id == caller_session_id || allow_non_lead {
            return Ok(team);
        }
        Err(TeamToolFailure::new(
            "unauthorized",
            format!("agent {caller_session_id} is not allowed to {action} team {team_id}"),
        ))
    }

    async fn begin_manage_add_idempotent_execution(
        &self,
        cache_key: &str,
    ) -> Result<Option<Value>, TeamToolFailure> {
        let now = Instant::now();
        let ttl = Duration::from_secs(GG_TEAM_ADD_IDEMPOTENCY_CACHE_TTL_SECS);
        let mut cache = self.team_manage_add_idempotency.lock().await;
        cache.retain(|_, entry| now.duration_since(entry.inserted_at) <= ttl);
        if let Some(existing) = cache.get(cache_key) {
            if let Some(result) = existing.completed_success.clone() {
                return Ok(Some(result));
            }
            return Err(TeamToolFailure::new(
                "duplicate_tool_invocation_in_progress",
                "Duplicate gg_team_manage add invocation is already in progress",
            ));
        }
        cache.insert(
            cache_key.to_string(),
            ManageAddIdempotencyEntry {
                inserted_at: now,
                completed_success: None,
            },
        );
        Ok(None)
    }

    async fn complete_manage_add_idempotent_execution(&self, cache_key: &str, result: &Value) {
        let mut cache = self.team_manage_add_idempotency.lock().await;
        if let Some(entry) = cache.get_mut(cache_key) {
            entry.inserted_at = Instant::now();
            entry.completed_success = Some(result.clone());
        }
    }

    async fn abort_manage_add_idempotent_execution(&self, cache_key: &str) {
        let mut cache = self.team_manage_add_idempotency.lock().await;
        cache.remove(cache_key);
    }
}

#[async_trait]
impl ToolGateway for RuntimeToolGateway {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        let _ = (&self.team_comms, &self.worktrees);
        self.process_manager.healthcheck().await
    }

    async fn invoke_tool(&self, request: ToolInvokeRequest) -> Result<Value, RuntimeError> {
        let caller_session_id = request.caller_session_id.trim();
        if caller_session_id.is_empty() {
            return Err(RuntimeError::InvalidState(
                "caller_session_id is required".to_string(),
            ));
        }

        if let Some(namespace) = request.namespace.as_deref() {
            if !namespace_matches_tool(namespace, request.tool_name.as_str()) {
                return Err(RuntimeError::InvalidState(
                    "namespace does not match tool_name".to_string(),
                ));
            }
        }

        if request.tool_name.starts_with("gg_process_") {
            return Ok(self.invoke_process_tool(request).await);
        }
        if request.tool_name.starts_with("gg_team_") {
            return Ok(self.invoke_team_tool(request).await);
        }

        Ok(json!({
            "ok": false,
            "error": {
                "code": "bad_request",
                "message": format!("Unsupported tool name: {}", request.tool_name),
            }
        }))
    }

    async fn capabilities(&self) -> Result<Value, RuntimeError> {
        let mut supported_namespaces = vec!["gg_process"];
        let mut tools = vec![
            GG_PROCESS_RUN,
            GG_PROCESS_STATUS,
            GG_PROCESS_LOGS,
            GG_PROCESS_KILL,
        ];
        if self.team_policy.enabled {
            supported_namespaces.push("gg_team");
            tools.extend([GG_TEAM_STATUS, GG_TEAM_MESSAGE, GG_TEAM_MANAGE]);
        }
        Ok(json!({
            "ok": true,
            "result": {
                "ggProcessEnabled": self.process_manager.config.enabled,
                "ggTeamEnabled": self.team_policy.enabled,
                "ggTeamManagePermissions": {
                    "nonLeadCanAddMembers": self.team_policy.non_lead_can_add_members,
                    "nonLeadCanRemoveMembers": self.team_policy.non_lead_can_remove_members,
                },
                "supportedNamespaces": supported_namespaces,
                "tools": tools,
            }
        }))
    }
}

fn namespace_matches_tool(namespace: &str, tool_name: &str) -> bool {
    match namespace.trim() {
        "gg_process" => tool_name.starts_with("gg_process_"),
        "gg_team" => tool_name.starts_with("gg_team_"),
        _ => false,
    }
}

#[derive(Debug)]
struct TeamToolFailure {
    code: &'static str,
    message: String,
}

impl TeamToolFailure {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn from_runtime(error: RuntimeError) -> Self {
        let code = team_tool_error_code_for_runtime(&error);
        Self::new(code, error.to_string())
    }
}

fn team_tool_error_code_for_runtime(error: &RuntimeError) -> &'static str {
    match error {
        RuntimeError::InvalidState(_) => "unauthorized",
        RuntimeError::NotFound(_) => "not_found",
        RuntimeError::Unsupported(_) => "feature_disabled",
        _ => "tool_failed",
    }
}

fn team_tool_error(code: impl AsRef<str>, message: impl AsRef<str>) -> Value {
    json!({
        "ok": false,
        "error": {
            "code": code.as_ref(),
            "message": message.as_ref(),
        }
    })
}

fn reject_team_tool_fields(
    args: &serde_json::Map<String, Value>,
    rejected_fields: &[&str],
) -> Result<(), TeamToolFailure> {
    for field in rejected_fields {
        if args.contains_key(*field) {
            return Err(TeamToolFailure::new(
                "bad_request",
                format!("{field} is supplied by gateway metadata and cannot be provided in args"),
            ));
        }
    }
    Ok(())
}

fn required_string_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<String, TeamToolFailure> {
    let value = args
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| TeamToolFailure::new("bad_request", format!("{field} is required")))?;
    Ok(value.to_string())
}

fn optional_string_array_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Vec<String>, TeamToolFailure> {
    let Some(value) = args.get(field) else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let Some(values) = value.as_array() else {
        return Err(TeamToolFailure::new(
            "bad_request",
            format!("{field} must be an array of non-empty strings"),
        ));
    };
    values
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    TeamToolFailure::new(
                        "bad_request",
                        format!("{field} must contain only non-empty strings"),
                    )
                })
        })
        .collect()
}

fn optional_string_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Option<String>, TeamToolFailure> {
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            TeamToolFailure::new(
                "bad_request",
                format!("{field} must be a non-empty string when provided"),
            )
        })
}

fn optional_bool_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Option<bool>, TeamToolFailure> {
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_bool().map(Some).ok_or_else(|| {
        TeamToolFailure::new(
            "bad_request",
            format!("{field} must be a boolean when provided"),
        )
    })
}

fn optional_creator_compaction_subscription_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Option<String>, TeamToolFailure> {
    let Some(value) = optional_string_arg(args, field)? else {
        return Ok(None);
    };
    match value.as_str() {
        "auto" | "unsubscribed" => Ok(Some(value)),
        _ => Err(TeamToolFailure::new(
            "bad_request",
            format!("{field} must be either auto or unsubscribed"),
        )),
    }
}

fn ensure_team_member(
    team: &TeamWithMembers,
    caller_session_id: &str,
) -> Result<(), TeamToolFailure> {
    if team
        .members
        .iter()
        .any(|member| member.agent_id == caller_session_id)
    {
        return Ok(());
    }
    Err(TeamToolFailure::new(
        "unauthorized",
        format!(
            "agent {} is not a member of team {}",
            caller_session_id, team.team.id
        ),
    ))
}

fn status_state_for_session(session: Option<&SessionRecord>) -> &'static str {
    match session {
        Some(session) if session.status == "failed" => "errored",
        Some(session) if session.active_turn_id.is_some() => "working",
        Some(session) if session.status == "closed" => "closed",
        Some(_) => "idle",
        None => "unknown",
    }
}

fn latest_message_for_member<'a>(
    agent_id: &str,
    messages: &'a [TeamMessageRecord],
) -> Option<&'a TeamMessageRecord> {
    messages
        .iter()
        .filter(|message| {
            message.sender_agent_id == agent_id
                || message
                    .recipient_agent_ids
                    .as_array()
                    .map(|recipients| {
                        recipients
                            .iter()
                            .any(|value| value.as_str() == Some(agent_id))
                    })
                    .unwrap_or(false)
        })
        .max_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        })
}

fn member_last_message_output(message: &TeamMessageRecord) -> Value {
    json!({
        "message_id": message.id,
        "scope": message.scope,
        "sender_agent_id": message.sender_agent_id,
        "created_at_ms": message.created_at,
        "text": message_text(&message.input),
    })
}

fn message_text(input: &Value) -> Option<String> {
    if let Some(text) = input.as_str() {
        return Some(text.to_string());
    }
    input
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[derive(Debug)]
pub struct StubTeamCommsService {
    config: TeamCommsConfig,
}

#[derive(Debug)]
pub struct StubWorktreeService {
    config: WorktreeServiceConfig,
}

impl StubTeamCommsService {
    pub fn new(config: TeamCommsConfig) -> Self {
        Self { config }
    }
}

impl StubWorktreeService {
    pub fn new(config: WorktreeServiceConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl TeamCommsService for StubTeamCommsService {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        if self.config.enabled {
            return Ok(());
        }
        Err(RuntimeError::Bootstrap(
            "team comms service is disabled".to_string(),
        ))
    }

    async fn create_team(
        &self,
        _request: TeamCreateRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn list_teams(&self) -> Result<Vec<TeamWithMembers>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn get_team(&self, _team_id: &str) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn join_team(&self, _request: TeamJoinRequest) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn remove_team_member(
        &self,
        _request: TeamRemoveMemberRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn set_team_lead(
        &self,
        _request: TeamSetLeadRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn delete_team(&self, _team_id: &str) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn interrupt_all_team_turns(
        &self,
        _request: TeamInterruptAllRequest,
    ) -> Result<TeamInterruptAllResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn send_direct(
        &self,
        _request: TeamSendDirectRequest,
    ) -> Result<TeamMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn broadcast(
        &self,
        _request: TeamBroadcastRequest,
    ) -> Result<TeamMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn list_messages(
        &self,
        _request: TeamListMessagesRequest,
    ) -> Result<TeamListMessagesResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn get_deliveries(
        &self,
        _request: TeamGetDeliveriesRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn retry_delivery(
        &self,
        _request: TeamRetryDeliveryRequest,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn cancel_message(
        &self,
        _request: TeamCancelMessageRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    async fn get_view_snapshot(
        &self,
        _request: TeamViewSnapshotRequest,
    ) -> Result<TeamViewSnapshotResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }

    fn replay_team_events(
        &self,
        _team_id: &str,
        _after_seq: Option<i64>,
        _limit: usize,
    ) -> Result<Vec<runtime_core::RuntimeEventRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team comms service is not implemented".to_string(),
        ))
    }
}

#[async_trait]
impl WorktreeService for StubWorktreeService {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        let _enabled = self.config.enabled;
        Ok(())
    }

    async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn get_worktree(
        &self,
        _worktree_id: &str,
    ) -> Result<ManagedWorktreeRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn create_worktree(
        &self,
        _request: WorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn claim_worktree(
        &self,
        _request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn release_worktree(
        &self,
        _request: WorktreeReleaseRequest,
    ) -> Result<WorktreeReleaseResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn cleanup_worktree(
        &self,
        _request: WorktreeCleanupRequest,
    ) -> Result<WorktreeCleanupResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn spawn_team_member(
        &self,
        _request: TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }

    async fn on_member_removed(
        &self,
        _request: WorktreeMemberRemovedRequest,
    ) -> Result<WorktreeMemberRemovedResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "worktree service is not implemented".to_string(),
        ))
    }
}

pub struct RuntimeWorktreeService {
    store: Arc<dyn RuntimeStore>,
    runtime: Arc<RuntimeSessionManager>,
    team_comms: Arc<dyn TeamCommsService>,
    config: WorktreeServiceConfig,
    repo_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    next_worktree_id: AtomicU64,
    next_operation_id: AtomicU64,
    next_event_id: AtomicU64,
    event_id_nonce: String,
}

#[derive(Debug, Clone)]
struct PlannedWorktreePaths {
    repo_root: String,
    worktree_root: String,
    worktree_cwd: String,
    branch_name: String,
    worktree_name: String,
    unified_workspace_path: String,
}

impl RuntimeWorktreeService {
    pub fn new(
        store: Arc<dyn RuntimeStore>,
        runtime: Arc<RuntimeSessionManager>,
        team_comms: Arc<dyn TeamCommsService>,
        config: WorktreeServiceConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        let hydrated = store.hydrate_runtime_state()?;
        Self::repair_startup_state(store.as_ref(), &hydrated)?;
        let hydrated = store.hydrate_runtime_state()?;
        let mut max_worktree_seq = 0_u64;
        for row in hydrated.managed_worktrees {
            if let Some(seq) = row
                .id
                .strip_prefix("wt_")
                .and_then(|value| value.parse::<u64>().ok())
            {
                max_worktree_seq = max_worktree_seq.max(seq);
            }
        }
        let mut max_op_seq = 0_u64;
        for row in hydrated.team_operation_journal {
            if let Some(seq) = row
                .operation_id
                .strip_prefix("op_spawn_")
                .and_then(|value| value.parse::<u64>().ok())
            {
                max_op_seq = max_op_seq.max(seq);
            }
        }
        Ok(Arc::new(Self {
            store,
            runtime,
            team_comms,
            config,
            repo_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            next_worktree_id: AtomicU64::new(max_worktree_seq + 1),
            next_operation_id: AtomicU64::new(max_op_seq + 1),
            next_event_id: AtomicU64::new(1),
            event_id_nonce: format!("{:032x}", rand::random::<u128>()),
        }))
    }

    fn repair_startup_state(
        store: &dyn RuntimeStore,
        hydrated: &runtime_core::RuntimeHydratedState,
    ) -> Result<(), RuntimeError> {
        let now = now_ms();
        let mut session_ids = BTreeSet::new();
        for session in &hydrated.sessions {
            session_ids.insert(session.id.trim().to_string());
        }

        let mut normalized_records_by_id = BTreeMap::<String, ManagedWorktreeRecord>::new();
        for original in &hydrated.managed_worktrees {
            let mut normalized = original.clone();
            normalized.repo_root = normalized.repo_root.trim().to_string();
            normalized.worktree_root = normalized.worktree_root.trim().to_string();
            normalized.worktree_cwd = normalized.worktree_cwd.trim().to_string();
            normalized.branch_name = normalized.branch_name.trim().to_string();
            normalized.worktree_name = normalized.worktree_name.trim().to_string();
            normalized.unified_workspace_path =
                normalized.unified_workspace_path.trim().to_string();
            if normalized.worktree_name.is_empty() {
                normalized.worktree_name = normalized.id.clone();
            }
            if normalized.worktree_root.is_empty() {
                normalized.worktree_root = normalized.worktree_cwd.clone();
            }
            if normalized.unified_workspace_path.is_empty() {
                normalized.unified_workspace_path =
                    Self::derive_unified_workspace_path(normalized.repo_root.as_str());
            }
            normalized_records_by_id.insert(normalized.id.clone(), normalized);
        }

        let mut winner_by_identity = BTreeMap::<(String, String, String), String>::new();
        let mut merged_winners = BTreeMap::<String, ManagedWorktreeRecord>::new();
        let mut loser_to_winner = BTreeMap::<String, String>::new();
        let mut malformed_ids = BTreeSet::new();

        let mut ordered = normalized_records_by_id
            .values()
            .cloned()
            .collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        for record in ordered {
            if record.repo_root.is_empty()
                || record.worktree_cwd.is_empty()
                || record.branch_name.is_empty()
            {
                malformed_ids.insert(record.id.clone());
                continue;
            }
            let key = (
                record.repo_root.clone(),
                record.worktree_cwd.clone(),
                record.branch_name.clone(),
            );
            if let Some(existing_winner_id) = winner_by_identity.get(&key).cloned() {
                loser_to_winner.insert(record.id.clone(), existing_winner_id.clone());
                if let Some(winner) = merged_winners.get_mut(existing_winner_id.as_str()) {
                    winner.deletion_policy = Self::merge_deletion_policy(
                        winner.deletion_policy.as_str(),
                        record.deletion_policy.as_str(),
                    );
                    winner.created_at = winner.created_at.min(record.created_at);
                    winner.updated_at = winner.updated_at.max(record.updated_at);
                    if winner.created_by_session_id.is_none() {
                        winner.created_by_session_id = record.created_by_session_id.clone();
                    }
                    if winner.created_by_operation_id.is_none() {
                        winner.created_by_operation_id = record.created_by_operation_id.clone();
                    }
                }
                continue;
            }
            winner_by_identity.insert(key, record.id.clone());
            merged_winners.insert(record.id.clone(), record);
        }

        for winner in merged_winners.values() {
            store.upsert_managed_worktree(winner)?;
        }

        for loser_id in loser_to_winner.keys() {
            if let Some(loser) = normalized_records_by_id.get(loser_id) {
                store.upsert_managed_worktree(&Self::tombstone_record(loser, now))?;
            }
        }
        for malformed_id in &malformed_ids {
            if let Some(malformed) = normalized_records_by_id.get(malformed_id) {
                store.upsert_managed_worktree(&Self::tombstone_record(malformed, now))?;
            }
        }

        let mut winner_created_at = BTreeMap::<String, i64>::new();
        for (id, record) in &merged_winners {
            winner_created_at.insert(id.clone(), record.created_at);
        }

        let mut claim_by_key = BTreeMap::<(String, String), ManagedWorktreeClaimRecord>::new();
        let mut claims_changed = Vec::<ManagedWorktreeClaimRecord>::new();
        for original in &hydrated.managed_worktree_claims {
            let mut claim = original.clone();
            claim.worktree_id = claim.worktree_id.trim().to_string();
            claim.session_id = claim.session_id.trim().to_string();
            claim.claim_role = claim.claim_role.trim().to_string();
            if claim.claim_role.is_empty() {
                claim.claim_role = "consumer".to_string();
            }
            if claim.worktree_id.is_empty() || claim.session_id.is_empty() {
                if claim.released_at.is_none() {
                    claim.released_at = Some(now);
                }
                claims_changed.push(claim);
                continue;
            }
            let original_worktree_id = claim.worktree_id.clone();
            if let Some(winner_id) = loser_to_winner.get(claim.worktree_id.as_str()) {
                claim.worktree_id = winner_id.clone();
                let mut stale_original = claim.clone();
                stale_original.worktree_id = original_worktree_id;
                if stale_original.released_at.is_none() {
                    stale_original.released_at = Some(now);
                }
                claims_changed.push(stale_original);
            }
            let worktree_exists = merged_winners.contains_key(claim.worktree_id.as_str());
            let session_exists = session_ids.contains(claim.session_id.as_str());
            if !(worktree_exists && session_exists) && claim.released_at.is_none() {
                claim.released_at = Some(now);
            }

            let key = (claim.worktree_id.clone(), claim.session_id.clone());
            match claim_by_key.get_mut(&key) {
                Some(existing) => {
                    existing.created_at = existing.created_at.min(claim.created_at);
                    existing.claim_role = Self::merge_claim_role(
                        existing.claim_role.as_str(),
                        claim.claim_role.as_str(),
                    );
                    existing.released_at =
                        Self::merge_released_at(existing.released_at, claim.released_at);
                }
                None => {
                    claim_by_key.insert(key, claim);
                }
            }
        }

        let mut active_claims_by_session =
            BTreeMap::<String, Vec<ManagedWorktreeClaimRecord>>::new();
        for claim in claim_by_key.values() {
            if claim.released_at.is_none() {
                active_claims_by_session
                    .entry(claim.session_id.clone())
                    .or_default()
                    .push(claim.clone());
            }
        }

        for claims in active_claims_by_session.values_mut() {
            claims.sort_by(|left, right| {
                let left_created_at = winner_created_at
                    .get(left.worktree_id.as_str())
                    .copied()
                    .unwrap_or(i64::MAX);
                let right_created_at = winner_created_at
                    .get(right.worktree_id.as_str())
                    .copied()
                    .unwrap_or(i64::MAX);
                left_created_at
                    .cmp(&right_created_at)
                    .then_with(|| left.worktree_id.cmp(&right.worktree_id))
            });
            for losing in claims.iter().skip(1) {
                let key = (losing.worktree_id.clone(), losing.session_id.clone());
                if let Some(existing) = claim_by_key.get_mut(&key) {
                    if existing.released_at.is_none() {
                        existing.released_at = Some(now);
                    }
                }
            }
        }

        for claim in claim_by_key.into_values() {
            store.upsert_managed_worktree_claim(&claim)?;
        }
        for claim in claims_changed {
            store.upsert_managed_worktree_claim(&claim)?;
        }

        Ok(())
    }

    fn tombstone_record(record: &ManagedWorktreeRecord, now: i64) -> ManagedWorktreeRecord {
        let mut tombstoned = record.clone();
        tombstoned.repo_root = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.worktree_root = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.worktree_cwd = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.branch_name = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.worktree_name = format!("tombstoned-{}", record.id);
        tombstoned.unified_workspace_path = format!("tombstoned_{}", record.id);
        tombstoned.deletion_policy = "retain_on_last_claim".to_string();
        tombstoned.updated_at = now;
        tombstoned
    }

    fn merge_deletion_policy(left: &str, right: &str) -> String {
        if left == "delete_on_last_claim" || right == "delete_on_last_claim" {
            "delete_on_last_claim".to_string()
        } else {
            "retain_on_last_claim".to_string()
        }
    }

    fn merge_claim_role(left: &str, right: &str) -> String {
        if left == "owner" || right == "owner" {
            "owner".to_string()
        } else {
            "consumer".to_string()
        }
    }

    fn merge_released_at(left: Option<i64>, right: Option<i64>) -> Option<i64> {
        match (left, right) {
            (None, None) => None,
            (Some(value), None) | (None, Some(value)) => Some(value),
            (Some(left), Some(right)) => Some(left.min(right)),
        }
    }

    #[cfg(test)]
    fn spawn_test_flag(metadata: &Option<Value>, key: &str) -> bool {
        metadata
            .as_ref()
            .and_then(|value| value.as_object())
            .and_then(|object| object.get(key))
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    }

    fn ensure_enabled(&self) -> Result<(), RuntimeError> {
        if self.config.enabled {
            return Ok(());
        }
        Err(RuntimeError::Unsupported(
            "managed worktrees are disabled".to_string(),
        ))
    }

    async fn lock_for_repo(&self, repo_root: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.repo_locks.lock().await;
        if let Some(existing) = locks.get(repo_root) {
            return Arc::clone(existing);
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(repo_root.to_string(), Arc::clone(&lock));
        lock
    }

    fn allocate_worktree_id(&self) -> String {
        format!(
            "wt_{}",
            self.next_worktree_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn allocate_operation_id(&self) -> String {
        format!(
            "op_spawn_{}",
            self.next_operation_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn normalize_deletion_policy(&self, requested: Option<&str>) -> String {
        let policy = requested
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(self.config.deletion_policy_default.as_str())
            .to_ascii_lowercase();
        match policy.as_str() {
            "delete_on_last_claim" => "delete_on_last_claim".to_string(),
            _ => "retain_on_last_claim".to_string(),
        }
    }

    async fn append_worktree_event(
        &self,
        worktree_id: &str,
        kind: &str,
        payload: Value,
        session_id: Option<String>,
        team_id: Option<String>,
    ) {
        let event_id = format!(
            "evt_worktree_{}_{}_{}",
            worktree_id,
            self.event_id_nonce,
            self.next_event_id.fetch_add(1, Ordering::Relaxed)
        );
        let _ = self.store.append_runtime_event(&NewRuntimeEvent {
            event_id,
            scope: RuntimeEventScope::Worktree,
            scope_id: worktree_id.to_string(),
            session_id,
            team_id,
            turn_id: None,
            kind: kind.to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload,
            provider: None,
            provider_seq: None,
            created_at: now_ms(),
        });
    }

    fn derive_unified_workspace_path(repo_root: &str) -> String {
        let mut value = String::new();
        let mut prev_sep = false;
        for ch in repo_root.chars() {
            if ch == '/' || ch == '\\' {
                if !prev_sep {
                    value.push_str("__");
                }
                prev_sep = true;
                continue;
            }
            prev_sep = false;
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                value.push(ch);
            } else {
                value.push('_');
            }
        }
        let trimmed = value.trim_matches('_');
        if trimmed.is_empty() {
            "workspace".to_string()
        } else {
            trimmed.to_string()
        }
    }

    fn resolve_repo_root_from_source_cwd(source_cwd: &str) -> Result<String, RuntimeError> {
        let output = StdCommand::new("git")
            .arg("-C")
            .arg(source_cwd)
            .args(["rev-parse", "--show-toplevel"])
            .output()
            .map_err(|error| RuntimeError::Io(format!("failed to run git rev-parse: {error}")))?;
        if !output.status.success() {
            return Err(RuntimeError::InvalidState(
                "source session cwd is not inside a git repository".to_string(),
            ));
        }
        let repo_root = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if repo_root.is_empty() {
            return Err(RuntimeError::InvalidState(
                "unable to resolve git repository root".to_string(),
            ));
        }
        Ok(repo_root)
    }

    fn plan_worktree_paths(
        &self,
        repo_root: &str,
        worktree_name: &str,
        branch_prefix: Option<&str>,
    ) -> PlannedWorktreePaths {
        let prefix = branch_prefix
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("gg");
        let branch_name = format!("{prefix}/{}", worktree_name.trim());
        let unified = Self::derive_unified_workspace_path(repo_root);
        let worktree_root_path = Path::new(self.config.root_dir.as_str()).join(unified.as_str());
        let branch_path_component = branch_name.replace('/', "--");
        let worktree_cwd_path = worktree_root_path.join(branch_path_component);
        PlannedWorktreePaths {
            repo_root: repo_root.trim().to_string(),
            worktree_root: worktree_root_path.to_string_lossy().to_string(),
            worktree_cwd: worktree_cwd_path.to_string_lossy().to_string(),
            branch_name,
            worktree_name: worktree_name.trim().to_string(),
            unified_workspace_path: unified,
        }
    }

    fn run_git_for_repo(
        repo_root: &str,
        args: &[&str],
        allowed_exit_codes: &[i32],
    ) -> Result<(String, String, i32), RuntimeError> {
        let output = StdCommand::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(args)
            .output()
            .map_err(|error| {
                RuntimeError::Io(format!("failed to run git {}: {error}", args.join(" ")))
            })?;
        let exit_code = output.status.code().unwrap_or(-1);
        if !output.status.success() && !allowed_exit_codes.contains(&exit_code) {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let message = if stderr.is_empty() {
                format!(
                    "git {} failed with status {}",
                    args.join(" "),
                    output.status
                )
            } else {
                format!("git {} failed: {}", args.join(" "), stderr)
            };
            return Err(RuntimeError::Io(message));
        }
        Ok((
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code,
        ))
    }

    fn run_worktree_init_script(&self, worktree_cwd: &str) -> Result<String, RuntimeError> {
        let configured = Path::new(self.config.init_script_path.as_str());
        let script_path = if configured.is_absolute() {
            configured.to_path_buf()
        } else {
            Path::new(worktree_cwd).join(configured)
        };
        if !script_path.exists() {
            return Ok("skipped_missing".to_string());
        }

        let command = if configured.is_absolute() {
            script_path.to_string_lossy().to_string()
        } else {
            format!("./{}", configured.to_string_lossy())
        };
        let output = StdCommand::new("sh")
            .arg("-lc")
            .arg(command)
            .current_dir(worktree_cwd)
            .output()
            .map_err(|error| {
                RuntimeError::Io(format!("failed to run worktree init script: {error}"))
            })?;
        if output.status.success() {
            return Ok("succeeded".to_string());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Err(RuntimeError::InvalidState(if stderr.is_empty() {
            "worktree init script failed".to_string()
        } else {
            format!("worktree init script failed: {stderr}")
        }))
    }

    fn upsert_worktree_record(
        &self,
        id: String,
        planned: &PlannedWorktreePaths,
        deletion_policy: String,
        created_by_session_id: Option<String>,
        created_by_operation_id: Option<String>,
    ) -> Result<ManagedWorktreeRecord, RuntimeError> {
        let now = now_ms();
        let record = ManagedWorktreeRecord {
            id,
            repo_root: planned.repo_root.clone(),
            worktree_root: planned.worktree_root.clone(),
            worktree_cwd: planned.worktree_cwd.clone(),
            branch_name: planned.branch_name.clone(),
            worktree_name: planned.worktree_name.clone(),
            unified_workspace_path: planned.unified_workspace_path.clone(),
            deletion_policy,
            created_by_session_id,
            created_by_operation_id,
            created_at: now,
            updated_at: now,
        };
        self.store.upsert_managed_worktree(&record)?;
        let hydrated = self.store.hydrate_runtime_state()?;
        self.worktree_by_identity(&hydrated, planned)
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "managed worktree logical upsert did not persist identity for {}",
                    planned.worktree_cwd
                ))
            })
    }

    fn get_worktree_from_hydrated(
        &self,
        worktree_id: &str,
        hydrated: &runtime_core::RuntimeHydratedState,
    ) -> Result<ManagedWorktreeRecord, RuntimeError> {
        hydrated
            .managed_worktrees
            .iter()
            .find(|row| row.id == worktree_id && !Self::is_record_tombstoned(row))
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("worktree {}", worktree_id)))
    }

    fn active_claims_for(
        &self,
        hydrated: &runtime_core::RuntimeHydratedState,
        worktree_id: &str,
    ) -> Vec<ManagedWorktreeClaimRecord> {
        hydrated
            .managed_worktree_claims
            .iter()
            .filter(|row| row.worktree_id == worktree_id && row.released_at.is_none())
            .cloned()
            .collect()
    }

    fn worktree_by_identity(
        &self,
        hydrated: &runtime_core::RuntimeHydratedState,
        planned: &PlannedWorktreePaths,
    ) -> Option<ManagedWorktreeRecord> {
        hydrated
            .managed_worktrees
            .iter()
            .find(|row| {
                !Self::is_record_tombstoned(row)
                    && row.repo_root == planned.repo_root
                    && row.worktree_cwd == planned.worktree_cwd
                    && row.branch_name == planned.branch_name
            })
            .cloned()
    }

    fn is_record_tombstoned(record: &ManagedWorktreeRecord) -> bool {
        record.repo_root.starts_with("__gg_tombstoned__/")
            || record.worktree_cwd.starts_with("__gg_tombstoned__/")
            || record.branch_name.starts_with("__gg_tombstoned__/")
    }

    fn branch_exists_for_record(record: &ManagedWorktreeRecord) -> Result<bool, RuntimeError> {
        let (_, _, exit_code) = Self::run_git_for_repo(
            record.repo_root.as_str(),
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", record.branch_name),
            ],
            &[1],
        )?;
        Ok(exit_code == 0)
    }

    fn has_live_artifacts_for_record(record: &ManagedWorktreeRecord) -> bool {
        if Path::new(record.worktree_cwd.as_str()).exists() {
            return true;
        }
        match Self::branch_exists_for_record(record) {
            Ok(exists) => exists,
            Err(_) => true,
        }
    }

    async fn rollback_spawn_after_join(
        &self,
        team_id: &str,
        operation_id: &str,
        spawned_session_id: &str,
        assigned_worktree_id: Option<&str>,
        created_worktree_id: Option<&str>,
        reason_code: &str,
        reason_message: &str,
        payload: Value,
    ) {
        let mut rollback_diagnostics = Vec::new();

        if let Err(error) = self
            .team_comms
            .remove_team_member(TeamRemoveMemberRequest {
                team_id: team_id.to_string(),
                agent_id: spawned_session_id.to_string(),
            })
            .await
        {
            rollback_diagnostics.push(format!("team_remove_failed:{error}"));
            let _ = self.store.append_team_operation_diagnostic(
                Some(operation_id),
                Some(team_id),
                "spawn_rollback_team_remove_failed",
                error.to_string().as_str(),
                &serde_json::json!({
                    "spawned_session_id": spawned_session_id
                }),
                now_ms(),
            );
        }

        if let Err(error) = self
            .runtime
            .close_session(
                spawned_session_id,
                Some(format!("spawn_rollback_{reason_code}")),
            )
            .await
        {
            rollback_diagnostics.push(format!("session_close_failed:{error}"));
            let _ = self.store.append_team_operation_diagnostic(
                Some(operation_id),
                Some(team_id),
                "spawn_rollback_session_close_failed",
                error.to_string().as_str(),
                &serde_json::json!({
                    "spawned_session_id": spawned_session_id
                }),
                now_ms(),
            );
            if let Err(force_error) = self
                .runtime
                .force_close_session(
                    spawned_session_id,
                    Some(format!("spawn_rollback_{reason_code}")),
                )
                .await
            {
                rollback_diagnostics.push(format!("session_force_close_failed:{force_error}"));
                let _ = self.store.append_team_operation_diagnostic(
                    Some(operation_id),
                    Some(team_id),
                    "spawn_rollback_session_force_close_failed",
                    force_error.to_string().as_str(),
                    &serde_json::json!({
                        "spawned_session_id": spawned_session_id
                    }),
                    now_ms(),
                );
            }
        }

        if let Some(worktree_id) = assigned_worktree_id {
            let _ = self
                .release_worktree(WorktreeReleaseRequest {
                    worktree_id: worktree_id.to_string(),
                    session_id: spawned_session_id.to_string(),
                    cleanup_if_last_claim: Some(false),
                })
                .await;
        }
        if let Some(worktree_id) = created_worktree_id {
            let _ = self
                .cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: worktree_id.to_string(),
                    reason: Some(format!("spawn_rollback_{reason_code}")),
                })
                .await;
        }

        let _ = self.store.append_team_operation_diagnostic(
            Some(operation_id),
            Some(team_id),
            reason_code,
            reason_message,
            &payload,
            now_ms(),
        );
        let _ = self.record_journal(
            operation_id,
            team_id,
            "rolled_back",
            serde_json::json!({
                "reason": reason_code,
                "message": reason_message,
                "payload": payload,
                "rollback_diagnostics": rollback_diagnostics,
            }),
        );
    }

    fn record_journal(
        &self,
        operation_id: &str,
        team_id: &str,
        stage: &str,
        payload: Value,
    ) -> Result<(), RuntimeError> {
        let now = now_ms();
        let existing = self
            .store
            .list_team_operation_journal(Some(team_id))?
            .into_iter()
            .find(|row| row.operation_id == operation_id);
        let created_at = existing.map(|row| row.created_at).unwrap_or(now);
        self.store
            .upsert_team_operation_journal(&runtime_core::TeamOperationJournalRecord {
                operation_id: operation_id.to_string(),
                team_id: team_id.to_string(),
                kind: "spawn_member_with_worktree".to_string(),
                stage: stage.to_string(),
                payload,
                created_at,
                updated_at: now,
            })
    }
}

#[async_trait]
impl WorktreeService for RuntimeWorktreeService {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let mut rows = hydrated
            .managed_worktrees
            .into_iter()
            .filter(|row| !Self::is_record_tombstoned(row))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.created_at.cmp(&right.created_at));
        Ok(rows)
    }

    async fn get_worktree(&self, worktree_id: &str) -> Result<ManagedWorktreeRecord, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        self.get_worktree_from_hydrated(worktree_id, &hydrated)
    }

    async fn create_worktree(
        &self,
        request: WorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        self.ensure_enabled()?;
        let source_session = self
            .runtime
            .get_session(request.source_session_id.as_str())
            .await?;
        let source_cwd = source_session.cwd.clone().ok_or_else(|| {
            RuntimeError::InvalidState(
                "source session has no cwd for worktree planning".to_string(),
            )
        })?;
        let repo_root = match request.repo_root.as_deref() {
            Some(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => Self::resolve_repo_root_from_source_cwd(source_cwd.as_str())?,
        };
        let planned = self.plan_worktree_paths(
            repo_root.as_str(),
            request.worktree_name.as_str(),
            request.branch_prefix.as_deref(),
        );

        let repo_lock = self.lock_for_repo(planned.repo_root.as_str()).await;
        let _repo_guard = repo_lock.lock().await;

        let hydrated_before = self.store.hydrate_runtime_state()?;
        if let Some(existing) = self.worktree_by_identity(&hydrated_before, &planned) {
            let active_claim_count = self
                .active_claims_for(&hydrated_before, existing.id.as_str())
                .len();
            let live_artifacts = Self::has_live_artifacts_for_record(&existing);
            let stale_cleaned = active_claim_count == 0
                && !live_artifacts
                && existing.deletion_policy == "delete_on_last_claim";
            if !stale_cleaned {
                return Ok(WorktreeCreateResponse {
                    worktree: existing,
                    created: false,
                    init_script_status: "skipped_existing".to_string(),
                });
            }
        }

        let branch_ref = format!("refs/heads/{}", planned.branch_name);
        let (_, _, branch_exit_code) = Self::run_git_for_repo(
            planned.repo_root.as_str(),
            &["show-ref", "--verify", "--quiet", branch_ref.as_str()],
            &[1],
        )?;
        if branch_exit_code == 0 || Path::new(planned.worktree_cwd.as_str()).exists() {
            return Err(RuntimeError::InvalidState(format!(
                "worktree name '{}' already exists",
                planned.worktree_name
            )));
        }

        std::fs::create_dir_all(planned.worktree_root.as_str()).map_err(|error| {
            RuntimeError::Io(format!(
                "failed to create worktree root {}: {error}",
                planned.worktree_root
            ))
        })?;

        let mut git_args = vec![
            "worktree",
            "add",
            "-b",
            planned.branch_name.as_str(),
            planned.worktree_cwd.as_str(),
        ];
        let trimmed_base = request
            .base_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(base_ref) = trimmed_base {
            git_args.push(base_ref);
        }
        Self::run_git_for_repo(planned.repo_root.as_str(), git_args.as_slice(), &[])?;

        let init_script_status = if request.run_init_script.unwrap_or(true) {
            match self.run_worktree_init_script(planned.worktree_cwd.as_str()) {
                Ok(status) => status,
                Err(error) => {
                    let _ = self.store.append_team_operation_diagnostic(
                        request.operation_id.as_deref(),
                        request.team_id.as_deref(),
                        "worktree_init_failed",
                        error.to_string().as_str(),
                        &serde_json::json!({
                            "worktree_cwd": planned.worktree_cwd,
                            "branch_name": planned.branch_name
                        }),
                        now_ms(),
                    );
                    let _ = Self::run_git_for_repo(
                        planned.repo_root.as_str(),
                        &[
                            "worktree",
                            "remove",
                            "--force",
                            planned.worktree_cwd.as_str(),
                        ],
                        &[128, 255],
                    );
                    let _ = Self::run_git_for_repo(
                        planned.repo_root.as_str(),
                        &["branch", "-D", planned.branch_name.as_str()],
                        &[1],
                    );
                    return Err(error);
                }
            }
        } else {
            "skipped_disabled".to_string()
        };

        let worktree = self.upsert_worktree_record(
            self.allocate_worktree_id(),
            &planned,
            self.normalize_deletion_policy(request.deletion_policy.as_deref()),
            request.created_by_session_id,
            request.operation_id,
        )?;
        self.append_worktree_event(
            worktree.id.as_str(),
            "worktree.created",
            serde_json::json!({
                "worktree": worktree,
                "init_script_status": init_script_status,
            }),
            Some(source_session.id.clone()),
            request.team_id,
        )
        .await;
        Ok(WorktreeCreateResponse {
            worktree,
            created: true,
            init_script_status,
        })
    }

    async fn claim_worktree(
        &self,
        request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;

        let conflicting_claim = hydrated.managed_worktree_claims.iter().find(|row| {
            row.session_id == request.session_id
                && row.released_at.is_none()
                && row.worktree_id != request.worktree_id
        });
        if let Some(conflict) = conflicting_claim {
            return Err(RuntimeError::InvalidState(format!(
                "session {} already has an active claim on worktree {}",
                request.session_id, conflict.worktree_id
            )));
        }

        let claim = ManagedWorktreeClaimRecord {
            worktree_id: request.worktree_id.clone(),
            session_id: request.session_id.clone(),
            claim_role: request.claim_role.trim().to_ascii_lowercase(),
            created_at: now_ms(),
            released_at: None,
        };
        self.store.upsert_managed_worktree_claim(&claim)?;
        self.append_worktree_event(
            worktree.id.as_str(),
            "worktree.claimed",
            serde_json::json!({ "claim": claim }),
            Some(request.session_id),
            None,
        )
        .await;
        Ok(WorktreeClaimResponse { worktree, claim })
    }

    async fn release_worktree(
        &self,
        request: WorktreeReleaseRequest,
    ) -> Result<WorktreeReleaseResponse, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        let existing_claim = hydrated
            .managed_worktree_claims
            .iter()
            .find(|row| {
                row.worktree_id == request.worktree_id && row.session_id == request.session_id
            })
            .cloned()
            .ok_or_else(|| {
                RuntimeError::NotFound(format!(
                    "worktree claim {}:{}",
                    request.worktree_id, request.session_id
                ))
            })?;
        let released_claim = ManagedWorktreeClaimRecord {
            released_at: Some(now_ms()),
            ..existing_claim
        };
        self.store.upsert_managed_worktree_claim(&released_claim)?;
        self.append_worktree_event(
            worktree.id.as_str(),
            "worktree.released",
            serde_json::json!({ "claim": released_claim }),
            Some(request.session_id),
            None,
        )
        .await;

        let hydrated_after = self.store.hydrate_runtime_state()?;
        let active_claim_count = self
            .active_claims_for(&hydrated_after, worktree.id.as_str())
            .len();
        let cleanup = if request.cleanup_if_last_claim.unwrap_or(true) && active_claim_count == 0 {
            Some(
                self.cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: worktree.id.clone(),
                    reason: Some("release_last_claim".to_string()),
                })
                .await?,
            )
        } else {
            None
        };

        Ok(WorktreeReleaseResponse {
            worktree,
            released_claim,
            active_claim_count,
            cleanup,
        })
    }

    async fn cleanup_worktree(
        &self,
        request: WorktreeCleanupRequest,
    ) -> Result<WorktreeCleanupResponse, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        let active_claim_count = self
            .active_claims_for(&hydrated, worktree.id.as_str())
            .len();
        if active_claim_count > 0 {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: "skipped_live_claims".to_string(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        if worktree.deletion_policy != "delete_on_last_claim" {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: "retained_by_policy".to_string(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        let repo_lock = self.lock_for_repo(worktree.repo_root.as_str()).await;
        let _repo_guard = repo_lock.lock().await;

        let mut diagnostics = Vec::new();
        let mut worktree_path_deleted = false;
        let mut branch_deleted = false;
        if Path::new(worktree.worktree_cwd.as_str()).exists() {
            match Self::run_git_for_repo(
                worktree.repo_root.as_str(),
                &[
                    "worktree",
                    "remove",
                    "--force",
                    worktree.worktree_cwd.as_str(),
                ],
                &[128, 255],
            ) {
                Ok(_) => {
                    worktree_path_deleted = !Path::new(worktree.worktree_cwd.as_str()).exists();
                }
                Err(error) => diagnostics.push(error.to_string()),
            }
        } else {
            worktree_path_deleted = true;
        }
        match Self::run_git_for_repo(
            worktree.repo_root.as_str(),
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", worktree.branch_name),
            ],
            &[1],
        ) {
            Ok((_, _, exit_code)) if exit_code == 1 => {
                branch_deleted = true;
            }
            Ok(_) => match Self::run_git_for_repo(
                worktree.repo_root.as_str(),
                &["branch", "-D", worktree.branch_name.as_str()],
                &[1],
            ) {
                Ok(_) => branch_deleted = true,
                Err(error) => diagnostics.push(error.to_string()),
            },
            Err(error) => diagnostics.push(error.to_string()),
        }

        let status = if diagnostics.is_empty() {
            "deleted".to_string()
        } else {
            "cleanup_failed".to_string()
        };
        if diagnostics.is_empty() {
            self.append_worktree_event(
                worktree.id.as_str(),
                "worktree.cleaned_up",
                serde_json::json!({
                    "worktree_id": worktree.id,
                    "reason": request.reason,
                    "worktree_path_deleted": worktree_path_deleted,
                    "branch_deleted": branch_deleted,
                }),
                None,
                None,
            )
            .await;
        } else {
            let _ = self.store.append_team_operation_diagnostic(
                None,
                None,
                "worktree_cleanup_failed",
                "managed worktree cleanup failed",
                &serde_json::json!({
                    "worktree_id": worktree.id,
                    "diagnostics": diagnostics,
                }),
                now_ms(),
            );
            self.append_worktree_event(
                worktree.id.as_str(),
                "worktree.cleanup_failed",
                serde_json::json!({
                    "worktree_id": worktree.id,
                    "reason": request.reason,
                    "diagnostics": diagnostics,
                }),
                None,
                None,
            )
            .await;
        }

        Ok(WorktreeCleanupResponse {
            worktree_id: worktree.id,
            status,
            deletion_policy: worktree.deletion_policy,
            active_claim_count,
            worktree_path_deleted,
            branch_deleted,
            diagnostics,
        })
    }

    async fn spawn_team_member(
        &self,
        request: TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = request.team_id.trim().to_string();
        if team_id.is_empty() {
            return Err(RuntimeError::InvalidState(
                "team_id is required".to_string(),
            ));
        }
        let operation_id = self.allocate_operation_id();
        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "planned",
            serde_json::json!({
                "source_session_id": request.source_session_id,
                "worktree": request.worktree,
            }),
        )?;

        let source_session = self
            .runtime
            .get_session(request.source_session_id.as_str())
            .await?;
        let source_cwd = source_session
            .cwd
            .clone()
            .ok_or_else(|| RuntimeError::InvalidState("source session has no cwd".to_string()))?;

        let mut worktree_assignment_mode = "none".to_string();
        let mut worktree_created_by_operation = false;
        let mut worktree_record: Option<ManagedWorktreeRecord> = None;
        let mut created_worktree_id: Option<String> = None;

        let worktree_input = request.worktree.clone();
        if let Some(worktree_input) = worktree_input {
            let mode = worktree_input
                .mode
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("create")
                .to_ascii_lowercase();
            let worktree_name = worktree_input
                .name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    RuntimeError::InvalidState("worktree.name is required".to_string())
                })?;
            let repo_root = Self::resolve_repo_root_from_source_cwd(source_cwd.as_str())?;
            let planned = self.plan_worktree_paths(
                repo_root.as_str(),
                worktree_name,
                worktree_input.branch_prefix.as_deref(),
            );
            let reuse_requested = matches!(mode.as_str(), "reuse" | "use_existing");
            if reuse_requested {
                worktree_assignment_mode = "reused".to_string();
                let hydrated = self.store.hydrate_runtime_state()?;
                let existing =
                    if let Some(existing) = self.worktree_by_identity(&hydrated, &planned) {
                        let live_artifacts = Self::has_live_artifacts_for_record(&existing);
                        if !live_artifacts {
                            return Err(RuntimeError::NotFound(format!(
                                "reused worktree identity exists but artifacts are missing: {}",
                                planned.worktree_cwd
                            )));
                        }
                        existing
                    } else {
                        if !Path::new(planned.worktree_cwd.as_str()).exists() {
                            return Err(RuntimeError::NotFound(format!(
                                "reused worktree path not found: {}",
                                planned.worktree_cwd
                            )));
                        }
                        self.upsert_worktree_record(
                            self.allocate_worktree_id(),
                            &planned,
                            "retain_on_last_claim".to_string(),
                            None,
                            None,
                        )?
                    };
                worktree_record = Some(existing);
            } else {
                worktree_assignment_mode = "created".to_string();
                worktree_created_by_operation = true;
                let created = self
                    .create_worktree(WorktreeCreateRequest {
                        team_id: Some(team_id.clone()),
                        source_session_id: source_session.id.clone(),
                        repo_root: Some(repo_root),
                        worktree_name: worktree_name.to_string(),
                        branch_prefix: worktree_input.branch_prefix.clone(),
                        base_ref: worktree_input.base_ref.clone(),
                        deletion_policy: Some("delete_on_last_claim".to_string()),
                        run_init_script: worktree_input.run_init_script,
                        created_by_session_id: Some(source_session.id.clone()),
                        operation_id: Some(operation_id.clone()),
                    })
                    .await?;
                created_worktree_id = Some(created.worktree.id.clone());
                worktree_record = Some(created.worktree.clone());
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "worktree_created",
                    serde_json::json!({ "worktree": created.worktree }),
                )?;
            }
        }
        let assigned_worktree_id = worktree_record.as_ref().map(|row| row.id.clone());

        let provider = match request.provider.as_deref() {
            Some(provider) => ProviderKind::from_str(provider).ok_or_else(|| {
                RuntimeError::InvalidState(format!("unsupported provider {}", provider))
            })?,
            None => ProviderKind::from_str(source_session.provider.as_str()).ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "source session has unsupported provider {}",
                    source_session.provider
                ))
            })?,
        };

        let spawn_cwd = worktree_record
            .as_ref()
            .map(|row| row.worktree_cwd.clone())
            .or_else(|| source_session.cwd.clone());
        let resolved_permission_mode = request
            .permission_mode
            .clone()
            .or(source_session.permission_mode.clone())
            .or_else(|| {
                if provider == ProviderKind::Codex && worktree_record.is_some() {
                    Some("full_auto".to_string())
                } else {
                    None
                }
            });
        let spawned_session = match self
            .runtime
            .create_session(CreateSessionInput {
                provider,
                model: request.model.clone().or(source_session.model.clone()),
                cwd: spawn_cwd,
                permission_mode: resolved_permission_mode,
                metadata: request.metadata.clone(),
            })
            .await
        {
            Ok(session) => session,
            Err(error) => {
                if let Some(worktree_id) = created_worktree_id {
                    let _ = self
                        .cleanup_worktree(WorktreeCleanupRequest {
                            worktree_id,
                            reason: Some("spawn_session_create_failed".to_string()),
                        })
                        .await;
                }
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "rolled_back",
                    serde_json::json!({
                        "reason": "session_create_failed",
                        "error": error.to_string()
                    }),
                )?;
                return Err(error);
            }
        };

        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "session_created",
            serde_json::json!({ "spawned_session_id": spawned_session.id }),
        )?;

        if let Some(worktree) = worktree_record.as_ref() {
            if let Err(error) = self
                .runtime
                .set_session_worktree_id(spawned_session.id.as_str(), Some(worktree.id.clone()))
                .await
            {
                let close_reason = Some("spawn_set_worktree_id_failed".to_string());
                if let Err(close_error) = self
                    .runtime
                    .close_session(spawned_session.id.as_str(), close_reason.clone())
                    .await
                {
                    let _ = self.store.append_team_operation_diagnostic(
                        Some(operation_id.as_str()),
                        Some(team_id.as_str()),
                        "spawn_set_worktree_id_session_close_failed",
                        close_error.to_string().as_str(),
                        &serde_json::json!({
                            "spawned_session_id": spawned_session.id,
                        }),
                        now_ms(),
                    );
                    let _ = self
                        .runtime
                        .force_close_session(spawned_session.id.as_str(), close_reason.clone())
                        .await;
                }
                if let Some(worktree_id) = created_worktree_id.as_deref() {
                    let _ = self
                        .cleanup_worktree(WorktreeCleanupRequest {
                            worktree_id: worktree_id.to_string(),
                            reason: Some("spawn_set_worktree_id_failed".to_string()),
                        })
                        .await;
                }
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "rolled_back",
                    serde_json::json!({
                        "reason": "set_session_worktree_id_failed",
                        "error": error.to_string(),
                    }),
                )?;
                return Err(error);
            }
        }

        let joined_team = match self
            .team_comms
            .join_team(TeamJoinRequest {
                team_id: team_id.clone(),
                agent_id: spawned_session.id.clone(),
                title: request.title.clone(),
                added_by: Some(source_session.id.clone()),
                creator_agent_id: request.creator_agent_id.clone(),
                creator_compaction_subscription: request.creator_compaction_subscription.clone(),
                worktree_id: worktree_record.as_ref().map(|row| row.id.clone()),
            })
            .await
        {
            Ok(team) => team,
            Err(error) => {
                let _ = self
                    .runtime
                    .close_session(
                        spawned_session.id.as_str(),
                        Some("spawn_join_failed".to_string()),
                    )
                    .await;
                if let Some(worktree_id) = created_worktree_id {
                    let _ = self
                        .cleanup_worktree(WorktreeCleanupRequest {
                            worktree_id,
                            reason: Some("spawn_join_failed".to_string()),
                        })
                        .await;
                }
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "rolled_back",
                    serde_json::json!({
                        "reason": "team_join_failed",
                        "error": error.to_string()
                    }),
                )?;
                return Err(error);
            }
        };

        #[cfg(test)]
        if Self::spawn_test_flag(&request.metadata, "__test_force_claim_failure_after_join") {
            let forced_error =
                RuntimeError::InvalidState("forced claim failure after join for test".to_string());
            self.rollback_spawn_after_join(
                team_id.as_str(),
                operation_id.as_str(),
                spawned_session.id.as_str(),
                assigned_worktree_id.as_deref(),
                created_worktree_id.as_deref(),
                "spawn_claim_failed_after_join",
                "spawn worktree claim failed after team join",
                serde_json::json!({
                    "spawned_session_id": spawned_session.id,
                    "forced": true,
                }),
            )
            .await;
            return Err(forced_error);
        }

        if let Some(worktree) = worktree_record.as_ref() {
            if let Err(error) = self
                .claim_worktree(WorktreeClaimRequest {
                    worktree_id: worktree.id.clone(),
                    session_id: spawned_session.id.clone(),
                    claim_role: if worktree_created_by_operation {
                        "owner".to_string()
                    } else {
                        "consumer".to_string()
                    },
                })
                .await
            {
                self.rollback_spawn_after_join(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    "spawn_claim_failed_after_join",
                    "spawn worktree claim failed after team join",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "worktree_id": worktree.id,
                        "error": error.to_string(),
                    }),
                )
                .await;
                return Err(error);
            }
        }

        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "team_joined",
            serde_json::json!({ "spawned_session_id": spawned_session.id }),
        )?;

        #[cfg(test)]
        if Self::spawn_test_flag(
            &request.metadata,
            "__test_force_onboarding_failure_after_join",
        ) {
            let forced_error = RuntimeError::InvalidState(
                "forced onboarding failure after join for test".to_string(),
            );
            self.rollback_spawn_after_join(
                team_id.as_str(),
                operation_id.as_str(),
                spawned_session.id.as_str(),
                assigned_worktree_id.as_deref(),
                created_worktree_id.as_deref(),
                "spawn_onboarding_failed_after_join",
                "spawn onboarding delivery failed after team join",
                serde_json::json!({
                    "spawned_session_id": spawned_session.id,
                    "forced": true,
                }),
            )
            .await;
            return Err(forced_error);
        }

        let onboarding_text = {
            let mut text = format!(
                "You were added to team \"{}\" ({}).\nThe team lead is {}.\nYour name is {}.",
                joined_team.team.name,
                joined_team.team.id,
                joined_team.team.lead_agent_id,
                spawned_session.id
            );
            if let Some(title) = request
                .title
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                text.push_str(format!("\nYour title is {}.", title).as_str());
            }
            if let Some(prompt) = request
                .prompt
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                text.push_str("\n\nRole instructions:\n");
                text.push_str(prompt);
            }
            text
        };
        let onboarding_ack = self
            .team_comms
            .send_direct(TeamSendDirectRequest {
                team_id: team_id.clone(),
                sender_agent_id: source_session.id.clone(),
                recipient_agent_id: spawned_session.id.clone(),
                input: serde_json::json!([{ "type": "text", "text": onboarding_text }]),
                image_paths: Vec::new(),
                priority: "normal".to_string(),
                policy: "start_new_turn_only".to_string(),
                correlation_id: Some(format!("spawn-onboarding:{operation_id}")),
                reply_to_message_id: None,
                idempotency_key: Some(format!("spawn-onboarding:{operation_id}")),
            })
            .await;
        let onboarding_ack = match onboarding_ack {
            Ok(ack) => ack,
            Err(error) => {
                self.rollback_spawn_after_join(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    "spawn_onboarding_failed_after_join",
                    "spawn onboarding delivery failed after team join",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "error": error.to_string(),
                    }),
                )
                .await;
                return Err(error);
            }
        };

        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "onboarding_sent",
            serde_json::json!({
                "message_id": onboarding_ack.message.id,
                "delivery_ids": onboarding_ack.deliveries.iter().map(|row| row.id.clone()).collect::<Vec<_>>()
            }),
        )?;
        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "completed",
            serde_json::json!({
                "spawned_session_id": spawned_session.id,
                "worktree_id": worktree_record.as_ref().map(|row| row.id.clone()),
            }),
        )?;

        let spawned_member = joined_team
            .members
            .iter()
            .find(|member| member.agent_id == spawned_session.id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::InvalidState("spawned member missing after join".to_string())
            })?;

        Ok(TeamMemberSpawnResponse {
            operation_id,
            team: joined_team,
            spawned_session,
            spawned_member,
            worktree: worktree_record,
            worktree_assignment_mode,
            worktree_created_by_operation,
            onboarding: serde_json::json!({
                "status": "sent",
                "message_id": onboarding_ack.message.id,
                "delivery_ids": onboarding_ack.deliveries.into_iter().map(|row| row.id).collect::<Vec<_>>()
            }),
            journal_stage: "completed".to_string(),
        })
    }

    async fn on_member_removed(
        &self,
        request: WorktreeMemberRemovedRequest,
    ) -> Result<WorktreeMemberRemovedResponse, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let mut released_claims = Vec::new();
        let mut cleanup_results = Vec::new();
        let mut diagnostics = Vec::new();
        let active_claims = hydrated
            .managed_worktree_claims
            .iter()
            .filter(|row| row.session_id == request.agent_id && row.released_at.is_none())
            .cloned()
            .collect::<Vec<_>>();
        for claim in active_claims {
            let released = ManagedWorktreeClaimRecord {
                released_at: Some(now_ms()),
                ..claim.clone()
            };
            if let Err(error) = self.store.upsert_managed_worktree_claim(&released) {
                let diag = self.store.append_team_operation_diagnostic(
                    None,
                    Some(request.team_id.as_str()),
                    "worktree_claim_release_failed",
                    error.to_string().as_str(),
                    &serde_json::json!({
                        "worktree_id": claim.worktree_id,
                        "session_id": claim.session_id
                    }),
                    now_ms(),
                )?;
                diagnostics.push(diag);
                continue;
            }
            released_claims.push(released.clone());
            match self
                .cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: released.worktree_id.clone(),
                    reason: Some("team_member_removed".to_string()),
                })
                .await
            {
                Ok(result) => cleanup_results.push(result),
                Err(error) => {
                    let diag = self.store.append_team_operation_diagnostic(
                        None,
                        Some(request.team_id.as_str()),
                        "worktree_cleanup_failed_on_member_remove",
                        error.to_string().as_str(),
                        &serde_json::json!({
                            "worktree_id": released.worktree_id,
                            "session_id": released.session_id
                        }),
                        now_ms(),
                    )?;
                    diagnostics.push(diag);
                }
            }
        }
        Ok(WorktreeMemberRemovedResponse {
            released_claims,
            cleanup_results,
            diagnostics,
        })
    }
}

fn parse_process_sequence(process_id: &str) -> Option<u64> {
    process_id
        .strip_prefix("proc_")
        .and_then(|value| value.parse::<u64>().ok())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_millis().min(i64::MAX as u128)) as i64
}

#[cfg(unix)]
fn exit_status_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_status_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_core::{
        ApprovalRecord, CreateSessionInput, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
        ProcessListRequest, ProviderAuthStatus, ProviderCreateSessionRequest,
        ProviderInterruptTurnRequest, ProviderKind, ProviderMetadata, ProviderModel,
        ProviderRegistry, ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderSession,
        ProviderTurnAck, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest,
        RuntimeProvider, RuntimeTeamCommsConfig, RuntimeTeamCommsService, SessionRecord,
        TeamCreateRequest, TeamDeliveryRecord, TeamMemberRecord, TeamMemberSpawnRequest,
        TeamMemberSpawnWorktreeInput, TeamMessageRecord, TeamOperationDiagnosticRecord,
        TeamOperationJournalRecord, TeamRecord, ToolGateway, TurnRecord, WorktreeClaimRequest,
        WorktreeCreateRequest, WorktreeReleaseRequest,
    };
    use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::Mutex;
    use tokio::sync::Mutex as AsyncMutex;

    #[derive(Default)]
    struct WorktreeTestProviderState {
        sessions: HashMap<String, String>,
    }

    #[derive(Default)]
    struct WorktreeTestProvider {
        state: AsyncMutex<WorktreeTestProviderState>,
    }

    #[async_trait::async_trait]
    impl RuntimeProvider for WorktreeTestProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Codex
        }

        fn metadata(&self) -> ProviderMetadata {
            ProviderMetadata {
                kind: ProviderKind::Codex,
                display_name: "Worktree Test Provider".to_string(),
                enabled: true,
            }
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
            Ok(vec![ProviderModel {
                id: "test-model".to_string(),
                display_name: "Test Model".to_string(),
            }])
        }

        async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
            Ok(ProviderAuthStatus {
                authenticated: true,
                mode: Some("test".to_string()),
                detail: None,
            })
        }

        async fn create_session(
            &self,
            req: ProviderCreateSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            let provider_ref = format!("test-thread-{}", req.runtime_session_id);
            state
                .sessions
                .insert(req.runtime_session_id.clone(), provider_ref.clone());
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id,
                provider_session_ref: provider_ref,
                canonical_provider_session_ref: None,
            })
        }

        async fn resume_session(
            &self,
            req: ProviderResumeSessionRequest,
        ) -> Result<ProviderSession, RuntimeError> {
            let mut state = self.state.lock().await;
            state.sessions.insert(
                req.runtime_session_id.clone(),
                req.provider_session_ref.clone(),
            );
            Ok(ProviderSession {
                runtime_session_id: req.runtime_session_id,
                provider_session_ref: req.provider_session_ref,
                canonical_provider_session_ref: req.canonical_provider_session_ref,
            })
        }

        async fn send_turn(
            &self,
            req: ProviderSendTurnRequest,
        ) -> Result<ProviderTurnAck, RuntimeError> {
            Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
            })
        }

        async fn wait_for_turn(
            &self,
            req: ProviderWaitTurnRequest,
        ) -> Result<ProviderTurnResult, RuntimeError> {
            Ok(ProviderTurnResult {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
                status: ProviderTurnStatus::Completed,
                usage: Some(serde_json::json!({ "last_message": "ok" })),
                error: None,
            })
        }

        async fn interrupt_turn(
            &self,
            _req: ProviderInterruptTurnRequest,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    async fn build_runtime_and_team_comms(
        store: Arc<SqliteRuntimeStore>,
    ) -> (Arc<RuntimeSessionManager>, Arc<RuntimeTeamCommsService>) {
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(WorktreeTestProvider::default()))
            .expect("register test provider");
        let runtime = Arc::new(
            RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512).expect("runtime"),
        );
        let team_comms = RuntimeTeamCommsService::new(
            store,
            runtime.clone(),
            RuntimeTeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1_000,
            },
        )
        .expect("team comms");
        (runtime, team_comms)
    }

    fn setup_git_repo(path: &std::path::Path) {
        std::fs::create_dir_all(path).expect("create repo dir");
        std::fs::write(path.join("README.md"), "runtime-tools\n").expect("write readme");
        assert!(std::process::Command::new("git")
            .arg("init")
            .arg("-b")
            .arg("main")
            .arg(path.as_os_str())
            .status()
            .expect("git init")
            .success());
        assert!(std::process::Command::new("git")
            .arg("-C")
            .arg(path.as_os_str())
            .args(["add", "."])
            .status()
            .expect("git add")
            .success());
        assert!(std::process::Command::new("git")
            .arg("-C")
            .arg(path.as_os_str())
            .args([
                "-c",
                "user.name=Runtime Tools",
                "-c",
                "user.email=runtime-tools@example.com",
                "commit",
                "-m",
                "init",
            ])
            .status()
            .expect("git commit")
            .success());
    }

    async fn create_test_session(runtime: &RuntimeSessionManager, cwd: &str) -> SessionRecord {
        runtime
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: Some("test-model".to_string()),
                cwd: Some(cwd.to_string()),
                permission_mode: None,
                metadata: Some(serde_json::json!({ "suite": "runtime_tools_phase6" })),
            })
            .await
            .expect("create session")
    }

    async fn create_test_team(team_comms: &RuntimeTeamCommsService, lead_id: &str) -> String {
        team_comms
            .create_team(TeamCreateRequest {
                name: "Phase6 Team".to_string(),
                lead_agent_id: lead_id.to_string(),
                member_agent_ids: Vec::new(),
                created_by: Some("test".to_string()),
            })
            .await
            .expect("create team")
            .team
            .id
    }

    async fn build_test_tool_gateway(policy: TeamMcpPolicy) -> RuntimeToolGateway {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("initialize store");
        let process_manager = RuntimeProcessManager::new(
            store,
            ProcessManagerConfig {
                enabled: true,
                max_concurrent: 1,
                default_timeout_ms: 60_000,
                max_output_bytes_per_process: 100_000,
                allow_shell: false,
                completed_retention_ms: 600_000,
                output_event_sample_bytes: 8 * 1024,
                log_dir: temp_dir.path().join("process-logs"),
            },
        )
        .await
        .expect("process manager");

        RuntimeToolGateway::new(RuntimeToolGatewayDeps {
            process_manager,
            runtime: None,
            team_comms: Arc::new(StubTeamCommsService::new(TeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1_000,
            })),
            worktrees: Arc::new(StubWorktreeService::new(WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            })),
            team_policy: policy,
        })
    }

    async fn build_team_gateway_fixture() -> (
        RuntimeToolGateway,
        Arc<RuntimeSessionManager>,
        Arc<RuntimeTeamCommsService>,
        tempfile::TempDir,
    ) {
        build_team_gateway_fixture_with_policy(TeamMcpPolicy::default()).await
    }

    async fn build_team_gateway_fixture_with_policy(
        team_policy: TeamMcpPolicy,
    ) -> (
        RuntimeToolGateway,
        Arc<RuntimeSessionManager>,
        Arc<RuntimeTeamCommsService>,
        tempfile::TempDir,
    ) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("initialize store");
        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let process_manager = RuntimeProcessManager::new(
            store.clone(),
            ProcessManagerConfig {
                enabled: true,
                max_concurrent: 1,
                default_timeout_ms: 60_000,
                max_output_bytes_per_process: 100_000,
                allow_shell: false,
                completed_retention_ms: 600_000,
                output_event_sample_bytes: 8 * 1024,
                log_dir: temp_dir.path().join("process-logs"),
            },
        )
        .await
        .expect("process manager");
        let worktrees = RuntimeWorktreeService::new(
            store,
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("worktree service");
        let gateway = RuntimeToolGateway::new(RuntimeToolGatewayDeps {
            process_manager,
            runtime: Some(runtime.clone()),
            team_comms: team_comms.clone(),
            worktrees,
            team_policy,
        });
        (gateway, runtime, team_comms, temp_dir)
    }

    async fn create_team_gateway_sessions(
        runtime: &RuntimeSessionManager,
        team_comms: &RuntimeTeamCommsService,
        temp_dir: &tempfile::TempDir,
        member_count: usize,
    ) -> (Vec<SessionRecord>, String) {
        let mut sessions = Vec::new();
        for idx in 0..member_count {
            let cwd = temp_dir.path().join(format!("session-{idx}"));
            std::fs::create_dir_all(&cwd).expect("create session cwd");
            sessions.push(create_test_session(runtime, cwd.to_string_lossy().as_ref()).await);
        }
        let lead_id = sessions.first().expect("lead session").id.clone();
        let member_ids = sessions
            .iter()
            .skip(1)
            .map(|session| session.id.clone())
            .collect::<Vec<_>>();
        let team_id = team_comms
            .create_team(TeamCreateRequest {
                name: "MCP Team".to_string(),
                lead_agent_id: lead_id,
                member_agent_ids: member_ids,
                created_by: Some("test".to_string()),
            })
            .await
            .expect("create team")
            .team
            .id;
        (sessions, team_id)
    }

    #[test]
    fn gateway_namespace_validation_accepts_gg_team_tools() {
        assert!(namespace_matches_tool("gg_team", GG_TEAM_STATUS));
        assert!(namespace_matches_tool(" gg_team ", GG_TEAM_MESSAGE));
        assert!(!namespace_matches_tool("gg_process", GG_TEAM_STATUS));
        assert!(!namespace_matches_tool("unsupported", GG_TEAM_STATUS));
    }

    #[tokio::test]
    async fn gateway_capabilities_include_team_tools_when_enabled() {
        let gateway = build_test_tool_gateway(TeamMcpPolicy {
            enabled: true,
            non_lead_can_add_members: true,
            non_lead_can_remove_members: false,
        })
        .await;

        let capabilities = gateway.capabilities().await.expect("capabilities");
        let result = capabilities.get("result").expect("result");
        assert_eq!(
            result.get("ggTeamEnabled").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            result
                .get("ggTeamManagePermissions")
                .and_then(|value| value.get("nonLeadCanAddMembers"))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            result
                .get("ggTeamManagePermissions")
                .and_then(|value| value.get("nonLeadCanRemoveMembers"))
                .and_then(Value::as_bool),
            Some(false)
        );
        let namespaces = result
            .get("supportedNamespaces")
            .and_then(Value::as_array)
            .expect("namespaces");
        assert!(namespaces.iter().any(|value| value == "gg_team"));
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .expect("tools");
        assert!(tools.iter().any(|value| value == GG_TEAM_STATUS));
        assert!(tools.iter().any(|value| value == GG_TEAM_MESSAGE));
        assert!(tools.iter().any(|value| value == GG_TEAM_MANAGE));
    }

    #[tokio::test]
    async fn gateway_capabilities_omit_team_tools_when_disabled() {
        let gateway = build_test_tool_gateway(TeamMcpPolicy {
            enabled: false,
            non_lead_can_add_members: true,
            non_lead_can_remove_members: true,
        })
        .await;

        let capabilities = gateway.capabilities().await.expect("capabilities");
        let result = capabilities.get("result").expect("result");
        assert_eq!(
            result.get("ggTeamEnabled").and_then(Value::as_bool),
            Some(false)
        );
        let namespaces = result
            .get("supportedNamespaces")
            .and_then(Value::as_array)
            .expect("namespaces");
        assert!(!namespaces.iter().any(|value| value == "gg_team"));
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .expect("tools");
        assert!(!tools.iter().any(|value| value == GG_TEAM_STATUS));
        assert!(!tools.iter().any(|value| value == GG_TEAM_MESSAGE));
        assert!(!tools.iter().any(|value| value == GG_TEAM_MANAGE));
    }

    #[tokio::test]
    async fn gateway_rejects_disabled_team_tool_with_feature_disabled() {
        let gateway = build_test_tool_gateway(TeamMcpPolicy {
            enabled: false,
            non_lead_can_add_members: false,
            non_lead_can_remove_members: false,
        })
        .await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_STATUS.to_string(),
                caller_session_id: "sess_caller".to_string(),
                invocation_id: None,
                args: json!({ "team_id": "team_1" }),
            })
            .await
            .expect("invoke");
        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(
            response
                .get("error")
                .and_then(|value| value.get("code"))
                .and_then(Value::as_str),
            Some("feature_disabled")
        );
    }

    #[tokio::test]
    async fn gateway_rejects_team_tool_under_process_namespace() {
        let gateway = build_test_tool_gateway(TeamMcpPolicy::default()).await;
        let error = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_process".to_string()),
                tool_name: GG_TEAM_STATUS.to_string(),
                caller_session_id: "sess_caller".to_string(),
                invocation_id: None,
                args: json!({ "team_id": "team_1" }),
            })
            .await
            .expect_err("namespace mismatch");
        assert!(matches!(error, RuntimeError::InvalidState(_)));
    }

    #[tokio::test]
    async fn team_status_gateway_invoke_returns_member_rows() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 2).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_STATUS.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: None,
                args: json!({ "team_id": team_id }),
            })
            .await
            .expect("status invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(true));
        let result = response.get("result").expect("result");
        assert_eq!(
            result.get("team_id").and_then(Value::as_str),
            Some(team_id.as_str())
        );
        assert_eq!(
            result.get("lead_agent_id").and_then(Value::as_str),
            Some(sessions[0].id.as_str())
        );
        let members = result
            .get("members")
            .and_then(Value::as_array)
            .expect("members");
        assert_eq!(members.len(), 2);
        assert!(members.iter().any(|member| {
            member.get("agent_id").and_then(Value::as_str) == Some(sessions[0].id.as_str())
                && member.get("state").and_then(Value::as_str).is_some()
                && member
                    .get("last_activity_at_ms")
                    .and_then(Value::as_i64)
                    .is_some()
        }));
    }

    #[tokio::test]
    async fn sidecar_forwarded_team_status_payload_executes() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 1).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: "gg_team_status".to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: Some("toolu_status_1".to_string()),
                args: json!({ "team_id": team_id }),
            })
            .await
            .expect("status invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            response
                .get("result")
                .and_then(|result| result.get("members"))
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
    }

    #[tokio::test]
    async fn team_status_unauthorized_for_non_member() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 1).await;
        let outsider =
            create_test_session(&runtime, temp_dir.path().to_string_lossy().as_ref()).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_STATUS.to_string(),
                caller_session_id: outsider.id,
                invocation_id: None,
                args: json!({ "team_id": team_id }),
            })
            .await
            .expect("status invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(
            response
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str),
            Some("unauthorized")
        );
        assert!(!sessions.is_empty());
    }

    #[tokio::test]
    async fn team_status_rejects_spoofing_fields() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 1).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_STATUS.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: None,
                args: json!({ "team_id": team_id, "sender_agent_id": "sess_spoof" }),
            })
            .await
            .expect("status invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(
            response
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str),
            Some("bad_request")
        );
    }

    #[tokio::test]
    async fn team_message_direct_gateway_invoke_creates_delivery_and_event() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 2).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MESSAGE.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: Some("msg_direct_1".to_string()),
                args: json!({
                    "team_id": team_id,
                    "recipient_agent_id": sessions[1].id,
                    "message": "direct hello",
                    "image_paths": ["/tmp/image-a.png"]
                }),
            })
            .await
            .expect("message invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(true));
        let result = response.get("result").expect("result");
        assert_eq!(result.get("scope").and_then(Value::as_str), Some("direct"));
        assert_eq!(
            result.get("recipient_count").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(result.get("image_count").and_then(Value::as_u64), Some(1));
        let message_id = result
            .get("message_id")
            .and_then(Value::as_str)
            .expect("message_id");
        let page = team_comms
            .list_messages(TeamListMessagesRequest {
                team_id: team_id.clone(),
                cursor: None,
                limit: Some(10),
            })
            .await
            .expect("list messages");
        assert!(page.messages.iter().any(|message| message.id == message_id));
        let events = team_comms
            .replay_team_events(team_id.as_str(), None, 100)
            .expect("team events");
        assert!(events
            .iter()
            .any(|event| event.kind == "team_message.created"));
    }

    #[tokio::test]
    async fn team_message_broadcast_gateway_invoke_excludes_sender() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 3).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MESSAGE.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: Some("msg_broadcast_1".to_string()),
                args: json!({
                    "team_id": team_id,
                    "recipient_agent_id": "broadcast",
                    "message": "broadcast hello"
                }),
            })
            .await
            .expect("message invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(true));
        let result = response.get("result").expect("result");
        assert_eq!(
            result.get("scope").and_then(Value::as_str),
            Some("broadcast")
        );
        assert_eq!(
            result.get("recipient_count").and_then(Value::as_u64),
            Some(2)
        );
        let delivery_ids = result
            .get("delivery_ids")
            .and_then(Value::as_array)
            .expect("delivery ids");
        assert_eq!(delivery_ids.len(), 2);
    }

    #[tokio::test]
    async fn team_message_rejects_blank_inputs() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 2).await;

        for args in [
            json!({ "team_id": "", "recipient_agent_id": sessions[1].id, "message": "hi" }),
            json!({ "team_id": team_id, "recipient_agent_id": "", "message": "hi" }),
            json!({ "team_id": team_id, "recipient_agent_id": sessions[1].id, "message": " " }),
        ] {
            let response = gateway
                .invoke_tool(ToolInvokeRequest {
                    namespace: Some("gg_team".to_string()),
                    tool_name: GG_TEAM_MESSAGE.to_string(),
                    caller_session_id: sessions[0].id.clone(),
                    invocation_id: None,
                    args,
                })
                .await
                .expect("message invoke");
            assert_eq!(response.get("ok").and_then(Value::as_bool), Some(false));
            assert_eq!(
                response
                    .get("error")
                    .and_then(|error| error.get("code"))
                    .and_then(Value::as_str),
                Some("bad_request")
            );
        }
    }

    #[tokio::test]
    async fn team_message_rejects_spoofed_sender_fields() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 2).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MESSAGE.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: None,
                args: json!({
                    "team_id": team_id,
                    "recipient_agent_id": sessions[1].id,
                    "message": "hi",
                    "sender_agent_id": sessions[1].id
                }),
            })
            .await
            .expect("message invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(
            response
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str),
            Some("bad_request")
        );
    }

    #[tokio::test]
    async fn team_manage_add_lead_spawns_member_and_duplicate_invocation_replays_result() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 1).await;

        let request = ToolInvokeRequest {
            namespace: Some("gg_team".to_string()),
            tool_name: GG_TEAM_MANAGE.to_string(),
            caller_session_id: sessions[0].id.clone(),
            invocation_id: Some("manage_add_once".to_string()),
            args: json!({
                "team_id": team_id,
                "title": "Implementer",
                "prompt": "Work on phase 3.",
                "creator_compaction_subscription": "unsubscribed"
            }),
        };
        let first = gateway
            .invoke_tool(request.clone())
            .await
            .expect("first manage add");
        let second = gateway
            .invoke_tool(request)
            .await
            .expect("second manage add");

        assert_eq!(first.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(second.get("ok").and_then(Value::as_bool), Some(true));
        let first_result = first.get("result").expect("first result");
        let second_result = second.get("result").expect("second result");
        assert_eq!(
            first_result.get("spawned_agent_id").and_then(Value::as_str),
            second_result
                .get("spawned_agent_id")
                .and_then(Value::as_str),
            "duplicate invocation should replay cached add result"
        );
        assert_eq!(
            first_result
                .get("worktree_assignment_mode")
                .and_then(Value::as_str),
            Some("none")
        );
        let team = team_comms.get_team(team_id.as_str()).await.expect("team");
        assert_eq!(team.members.len(), 2);
    }

    #[tokio::test]
    async fn team_manage_add_non_lead_requires_policy() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 2).await;

        let denied = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: sessions[1].id.clone(),
                invocation_id: Some("manage_add_denied".to_string()),
                args: json!({ "team_id": team_id }),
            })
            .await
            .expect("denied add");
        assert_eq!(denied.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(
            denied
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str),
            Some("unauthorized")
        );

        let (allowed_gateway, allowed_runtime, allowed_team_comms, allowed_temp_dir) =
            build_team_gateway_fixture_with_policy(TeamMcpPolicy {
                enabled: true,
                non_lead_can_add_members: true,
                non_lead_can_remove_members: false,
            })
            .await;
        let (allowed_sessions, allowed_team_id) = create_team_gateway_sessions(
            &allowed_runtime,
            &allowed_team_comms,
            &allowed_temp_dir,
            2,
        )
        .await;
        let allowed = allowed_gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: allowed_sessions[1].id.clone(),
                invocation_id: Some("manage_add_allowed".to_string()),
                args: json!({ "team_id": allowed_team_id }),
            })
            .await
            .expect("allowed add");
        assert_eq!(allowed.get("ok").and_then(Value::as_bool), Some(true));
        let team = allowed_team_comms
            .get_team(allowed_team_id.as_str())
            .await
            .expect("team");
        assert_eq!(team.members.len(), 3);
    }

    #[tokio::test]
    async fn team_manage_remove_lead_returns_partial_success_and_best_effort_cleanup() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 3).await;

        let response = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: Some("manage_remove_partial".to_string()),
                args: json!({
                    "team_id": team_id,
                    "remove_agent_ids": [
                        format!("  {}  ", sessions[1].id),
                        "missing_member"
                    ]
                }),
            })
            .await
            .expect("remove invoke");

        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(true));
        let result = response.get("result").expect("result");
        assert_eq!(
            result.get("operation").and_then(Value::as_str),
            Some("remove")
        );
        assert_eq!(result.get("removed_count").and_then(Value::as_u64), Some(1));
        assert_eq!(result.get("failed_count").and_then(Value::as_u64), Some(1));
        let rows = result
            .get("results")
            .and_then(Value::as_array)
            .expect("results");
        assert!(rows.iter().any(|row| {
            row.get("agent_id").and_then(Value::as_str) == Some(sessions[1].id.as_str())
                && row.get("ok").and_then(Value::as_bool) == Some(true)
                && row
                    .get("cleanup")
                    .and_then(|cleanup| cleanup.get("ok"))
                    .and_then(Value::as_bool)
                    == Some(true)
        }));
        assert!(rows.iter().any(|row| {
            row.get("agent_id").and_then(Value::as_str) == Some("missing_member")
                && row.get("ok").and_then(Value::as_bool) == Some(false)
        }));
        let team = team_comms.get_team(team_id.as_str()).await.expect("team");
        assert!(!team
            .members
            .iter()
            .any(|member| member.agent_id == sessions[1].id));
        assert!(team
            .members
            .iter()
            .any(|member| member.agent_id == sessions[2].id));
    }

    #[tokio::test]
    async fn team_manage_remove_non_lead_requires_policy() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 3).await;

        let denied = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: sessions[1].id.clone(),
                invocation_id: Some("manage_remove_denied".to_string()),
                args: json!({
                    "team_id": team_id,
                    "remove_agent_ids": [sessions[2].id]
                }),
            })
            .await
            .expect("denied remove");
        assert_eq!(denied.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(
            denied
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str),
            Some("unauthorized")
        );

        let (allowed_gateway, allowed_runtime, allowed_team_comms, allowed_temp_dir) =
            build_team_gateway_fixture_with_policy(TeamMcpPolicy {
                enabled: true,
                non_lead_can_add_members: false,
                non_lead_can_remove_members: true,
            })
            .await;
        let (allowed_sessions, allowed_team_id) = create_team_gateway_sessions(
            &allowed_runtime,
            &allowed_team_comms,
            &allowed_temp_dir,
            3,
        )
        .await;
        let allowed = allowed_gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: allowed_sessions[1].id.clone(),
                invocation_id: Some("manage_remove_allowed".to_string()),
                args: json!({
                    "team_id": allowed_team_id,
                    "remove_agent_ids": [allowed_sessions[2].id]
                }),
            })
            .await
            .expect("allowed remove");
        assert_eq!(allowed.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            allowed
                .get("result")
                .and_then(|result| result.get("removed_count"))
                .and_then(Value::as_u64),
            Some(1)
        );
    }

    #[tokio::test]
    async fn team_manage_add_worktree_create_and_reuse_options_are_forwarded() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let repo = temp_dir.path().join("repo");
        setup_git_repo(&repo);
        let lead = create_test_session(&runtime, repo.to_string_lossy().as_ref()).await;
        let team_id = create_test_team(&team_comms, lead.id.as_str()).await;

        let created = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: lead.id.clone(),
                invocation_id: Some("manage_add_worktree_create".to_string()),
                args: json!({
                    "team_id": team_id,
                    "worktree_name": "phase-three"
                }),
            })
            .await
            .expect("create worktree add");
        assert_eq!(created.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            created
                .get("result")
                .and_then(|result| result.get("worktree_assignment_mode"))
                .and_then(Value::as_str),
            Some("created")
        );

        let reused = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: lead.id.clone(),
                invocation_id: Some("manage_add_worktree_reuse".to_string()),
                args: json!({
                    "team_id": team_id,
                    "worktree_name": "phase-three",
                    "use_existing_worktree": true
                }),
            })
            .await
            .expect("reuse worktree add");
        assert_eq!(reused.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            reused
                .get("result")
                .and_then(|result| result.get("worktree_assignment_mode"))
                .and_then(Value::as_str),
            Some("reused")
        );
    }

    #[tokio::test]
    async fn team_manage_remove_releases_spawned_worktree_claims() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let repo = temp_dir.path().join("repo-cleanup");
        setup_git_repo(&repo);
        let lead = create_test_session(&runtime, repo.to_string_lossy().as_ref()).await;
        let team_id = create_test_team(&team_comms, lead.id.as_str()).await;

        let added = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: lead.id.clone(),
                invocation_id: Some("manage_add_cleanup_member".to_string()),
                args: json!({
                    "team_id": team_id,
                    "worktree_name": "cleanup-member"
                }),
            })
            .await
            .expect("add worktree member");
        let spawned_agent_id = added
            .get("result")
            .and_then(|result| result.get("spawned_agent_id"))
            .and_then(Value::as_str)
            .expect("spawned agent id")
            .to_string();

        let removed = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: lead.id.clone(),
                invocation_id: Some("manage_remove_cleanup_member".to_string()),
                args: json!({
                    "team_id": team_id,
                    "remove_agent_ids": [spawned_agent_id]
                }),
            })
            .await
            .expect("remove worktree member");
        assert_eq!(removed.get("ok").and_then(Value::as_bool), Some(true));
        let cleanup = removed
            .get("result")
            .and_then(|result| result.get("results"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("cleanup"))
            .expect("cleanup");
        assert_eq!(cleanup.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            cleanup.get("released_claim_count").and_then(Value::as_u64),
            Some(1)
        );
    }

    #[tokio::test]
    async fn team_manage_add_rejects_unsupported_fields_and_does_not_cache_failure() {
        let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
        let (sessions, team_id) =
            create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 1).await;

        let rejected = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: Some("manage_add_retry_after_failure".to_string()),
                args: json!({
                    "team_id": team_id,
                    "model_preset": "opus"
                }),
            })
            .await
            .expect("rejected add");
        assert_eq!(rejected.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(
            rejected
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str),
            Some("bad_request")
        );

        let retried = gateway
            .invoke_tool(ToolInvokeRequest {
                namespace: Some("gg_team".to_string()),
                tool_name: GG_TEAM_MANAGE.to_string(),
                caller_session_id: sessions[0].id.clone(),
                invocation_id: Some("manage_add_retry_after_failure".to_string()),
                args: json!({ "team_id": team_id }),
            })
            .await
            .expect("retried add");
        assert_eq!(retried.get("ok").and_then(Value::as_bool), Some(true));
        let team = team_comms.get_team(team_id.as_str()).await.expect("team");
        assert_eq!(team.members.len(), 2);
    }

    #[derive(Default)]
    struct FailingProcessUpsertStore {
        last_pid: Mutex<Option<i64>>,
        upsert_process_calls: AtomicU64,
    }

    #[async_trait]
    impl RuntimeStore for FailingProcessUpsertStore {
        async fn initialize(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn healthcheck(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn append_runtime_event(
            &self,
            _event: &NewRuntimeEvent,
        ) -> Result<runtime_core::RuntimeEventRecord, RuntimeError> {
            Err(RuntimeError::Io(
                "event append should not be called in this test".to_string(),
            ))
        }

        fn list_runtime_events(
            &self,
            _scope: Option<(RuntimeEventScope, &str)>,
            _after_seq: Option<i64>,
            _limit: usize,
        ) -> Result<Vec<runtime_core::RuntimeEventRecord>, RuntimeError> {
            Ok(Vec::new())
        }

        fn upsert_session(&self, _record: &SessionRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_turn(&self, _record: &TurnRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_approval(&self, _record: &ApprovalRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team(&self, _record: &TeamRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team_member(&self, _record: &TeamMemberRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn delete_team_member(&self, _team_id: &str, _agent_id: &str) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team_message(&self, _record: &TeamMessageRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_team_delivery(&self, _record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_managed_worktree(
            &self,
            _record: &ManagedWorktreeRecord,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_managed_worktree_claim(
            &self,
            _record: &ManagedWorktreeClaimRecord,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError> {
            self.upsert_process_calls
                .fetch_add(1, AtomicOrdering::Relaxed);
            *self.last_pid.lock().expect("last pid mutex poisoned") = record.pid;
            Err(RuntimeError::Io(
                "forced upsert_process failure".to_string(),
            ))
        }

        fn upsert_team_operation_journal(
            &self,
            _record: &TeamOperationJournalRecord,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn append_team_operation_diagnostic(
            &self,
            _operation_id: Option<&str>,
            _team_id: Option<&str>,
            _code: &str,
            _message: &str,
            _payload: &Value,
            _created_at: i64,
        ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
            Ok(TeamOperationDiagnosticRecord {
                id: 1,
                operation_id: None,
                team_id: None,
                code: "stub".to_string(),
                message: "stub".to_string(),
                payload: serde_json::json!({}),
                created_at: 0,
            })
        }

        fn list_team_operation_journal(
            &self,
            _team_id: Option<&str>,
        ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
            Ok(Vec::new())
        }

        fn list_team_operation_diagnostics(
            &self,
            _team_id: Option<&str>,
            _operation_id: Option<&str>,
        ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
            Ok(Vec::new())
        }

        fn hydrate_runtime_state(
            &self,
        ) -> Result<runtime_core::RuntimeHydratedState, RuntimeError> {
            Ok(runtime_core::RuntimeHydratedState::default())
        }
    }

    #[tokio::test]
    async fn spawn_failure_after_launch_tears_down_child_and_leaves_no_ghost_process() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(FailingProcessUpsertStore::default());
        let manager = RuntimeProcessManager::new(
            store.clone(),
            ProcessManagerConfig {
                enabled: true,
                max_concurrent: 1,
                default_timeout_ms: 60_000,
                max_output_bytes_per_process: 1_000_000,
                allow_shell: true,
                completed_retention_ms: 60_000,
                output_event_sample_bytes: 1024,
                log_dir: temp_dir.path().join("logs"),
            },
        )
        .await
        .expect("build process manager");

        let result = manager
            .run_process(ProcessRunRequest {
                caller_session_id: Some("sess_test".to_string()),
                tool_call_id: None,
                command: "sleep 5".to_string(),
                cwd: None,
                timeout_ms: None,
            })
            .await;
        assert!(matches!(result, Err(RuntimeError::Io(_))));

        // The process start failed after spawn. The fix must fail closed:
        // no retained managed process entry and the pre-handoff child torn down.
        let rows = manager
            .list_processes(ProcessListRequest {
                caller_session_id: Some("sess_test".to_string()),
                include_completed: true,
            })
            .await
            .expect("list processes");
        assert!(
            rows.is_empty(),
            "expected no retained process entries after failed start"
        );
        assert_eq!(
            store.upsert_process_calls.load(AtomicOrdering::Relaxed),
            1,
            "expected one failing upsert_process call"
        );

        #[cfg(unix)]
        {
            let pid = *store.last_pid.lock().expect("last pid mutex poisoned");
            if let Some(pid) = pid {
                let mut still_alive = true;
                for _ in 0..40 {
                    let status = std::process::Command::new("sh")
                        .arg("-lc")
                        .arg(format!("kill -0 {pid} >/dev/null 2>&1"))
                        .status()
                        .expect("kill -0 status");
                    if !status.success() {
                        still_alive = false;
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                assert!(
                    !still_alive,
                    "spawned child pid {pid} remained alive after failed pre-handoff bootstrap"
                );
            }
        }
    }

    #[tokio::test]
    async fn startup_repair_normalizes_identity_and_repairs_conflicting_claims() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("init store");

        let seed_session = |id: &str| SessionRecord {
            id: id.to_string(),
            provider: "codex".to_string(),
            status: "idle".to_string(),
            cwd: Some("/tmp/repo".to_string()),
            model: Some("test-model".to_string()),
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some(format!("thread-{id}")),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: 1,
            updated_at: 1,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        };
        store
            .upsert_session(&seed_session("session_a"))
            .expect("seed session a");
        store
            .upsert_session(&seed_session("session_b"))
            .expect("seed session b");

        let winner = ManagedWorktreeRecord {
            id: "wt_1".to_string(),
            repo_root: "/tmp/repo".to_string(),
            worktree_root: "/tmp/worktrees/repo".to_string(),
            worktree_cwd: "/tmp/worktrees/repo/gg--feature".to_string(),
            branch_name: "gg/feature".to_string(),
            worktree_name: "feature".to_string(),
            unified_workspace_path: "tmp__repo".to_string(),
            deletion_policy: "retain_on_last_claim".to_string(),
            created_by_session_id: Some("session_a".to_string()),
            created_by_operation_id: Some("op_a".to_string()),
            created_at: 10,
            updated_at: 10,
        };
        let loser = ManagedWorktreeRecord {
            id: "wt_2".to_string(),
            repo_root: " /tmp/repo ".to_string(),
            worktree_root: "/tmp/worktrees/repo".to_string(),
            worktree_cwd: " /tmp/worktrees/repo/gg--feature ".to_string(),
            branch_name: " gg/feature ".to_string(),
            worktree_name: "feature-dup".to_string(),
            unified_workspace_path: "tmp__repo".to_string(),
            deletion_policy: "delete_on_last_claim".to_string(),
            created_by_session_id: Some("session_b".to_string()),
            created_by_operation_id: Some("op_b".to_string()),
            created_at: 20,
            updated_at: 20,
        };
        store
            .upsert_managed_worktree(&winner)
            .expect("seed winner worktree");
        store
            .upsert_managed_worktree(&loser)
            .expect("seed loser worktree");
        store
            .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
                worktree_id: "wt_1".to_string(),
                session_id: "session_a".to_string(),
                claim_role: "owner".to_string(),
                created_at: 30,
                released_at: None,
            })
            .expect("seed claim winner");
        store
            .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
                worktree_id: "wt_2".to_string(),
                session_id: "session_a".to_string(),
                claim_role: "consumer".to_string(),
                created_at: 31,
                released_at: None,
            })
            .expect("seed conflicting claim");

        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let _service = RuntimeWorktreeService::new(
            store.clone(),
            runtime,
            team_comms,
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build worktree service");

        let hydrated = store.hydrate_runtime_state().expect("hydrate repaired");
        let live_records = hydrated
            .managed_worktrees
            .iter()
            .filter(|row| !RuntimeWorktreeService::is_record_tombstoned(row))
            .collect::<Vec<_>>();
        assert_eq!(live_records.len(), 1, "duplicate identities must converge");
        assert_eq!(live_records[0].repo_root, "/tmp/repo");
        assert_eq!(
            live_records[0].worktree_cwd,
            "/tmp/worktrees/repo/gg--feature"
        );
        assert_eq!(live_records[0].branch_name, "gg/feature");
        assert_eq!(live_records[0].deletion_policy, "delete_on_last_claim");

        let active_for_session_a = hydrated
            .managed_worktree_claims
            .iter()
            .filter(|row| row.session_id == "session_a" && row.released_at.is_none())
            .collect::<Vec<_>>();
        assert_eq!(
            active_for_session_a.len(),
            1,
            "session must have only one active managed worktree claim after repair"
        );
        assert_eq!(active_for_session_a[0].worktree_id, live_records[0].id);
        let tombstoned_claim = hydrated
            .managed_worktree_claims
            .iter()
            .find(|row| row.worktree_id == "wt_2" && row.session_id == "session_a")
            .expect("stale loser claim row");
        assert!(
            tombstoned_claim.released_at.is_some(),
            "duplicate loser claim must be explicitly released at startup"
        );
    }

    #[tokio::test]
    async fn cleanup_then_recreate_same_identity_recreates_artifacts() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("init store");
        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let service = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms,
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build worktree service");

        let repo_dir = temp_dir.path().join("repo");
        setup_git_repo(repo_dir.as_path());
        let source = create_test_session(&runtime, repo_dir.display().to_string().as_str()).await;

        let first = service
            .create_worktree(WorktreeCreateRequest {
                team_id: None,
                source_session_id: source.id.clone(),
                repo_root: None,
                worktree_name: "feature".to_string(),
                branch_prefix: Some("gg".to_string()),
                base_ref: None,
                deletion_policy: Some("delete_on_last_claim".to_string()),
                run_init_script: Some(false),
                created_by_session_id: Some(source.id.clone()),
                operation_id: None,
            })
            .await
            .expect("first create");
        assert!(first.created);
        assert!(Path::new(first.worktree.worktree_cwd.as_str()).exists());

        service
            .claim_worktree(WorktreeClaimRequest {
                worktree_id: first.worktree.id.clone(),
                session_id: source.id.clone(),
                claim_role: "owner".to_string(),
            })
            .await
            .expect("claim worktree");
        let release = service
            .release_worktree(WorktreeReleaseRequest {
                worktree_id: first.worktree.id.clone(),
                session_id: source.id.clone(),
                cleanup_if_last_claim: Some(true),
            })
            .await
            .expect("release worktree");
        assert_eq!(release.active_claim_count, 0);
        if let Some(cleanup) = release.cleanup {
            assert!(
                cleanup.status == "deleted"
                    || cleanup.status == "cleanup_failed"
                    || cleanup.status == "retained_by_policy"
                    || cleanup.status == "skipped_live_claims"
            );
        }

        let second = service
            .create_worktree(WorktreeCreateRequest {
                team_id: None,
                source_session_id: source.id.clone(),
                repo_root: None,
                worktree_name: "feature".to_string(),
                branch_prefix: Some("gg".to_string()),
                base_ref: None,
                deletion_policy: Some("delete_on_last_claim".to_string()),
                run_init_script: Some(false),
                created_by_session_id: Some(source.id.clone()),
                operation_id: None,
            })
            .await
            .expect("second create");
        assert!(
            second.created,
            "second create must recreate usable artifacts after prior cleanup"
        );
        assert!(Path::new(second.worktree.worktree_cwd.as_str()).exists());
        let fetched = service
            .get_worktree(second.worktree.id.as_str())
            .await
            .expect("second create id should be persisted");
        assert_eq!(fetched.id, second.worktree.id);
        let second_claim = service
            .claim_worktree(WorktreeClaimRequest {
                worktree_id: second.worktree.id.clone(),
                session_id: source.id.clone(),
                claim_role: "owner".to_string(),
            })
            .await
            .expect("second create id should be claimable");
        assert_eq!(second_claim.worktree.id, second.worktree.id);
        service
            .release_worktree(WorktreeReleaseRequest {
                worktree_id: second.worktree.id.clone(),
                session_id: source.id,
                cleanup_if_last_claim: Some(false),
            })
            .await
            .expect("release second claim");
    }

    #[tokio::test]
    async fn forced_claim_failure_after_join_rolls_back_cleanly() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("init store");
        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let service = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build worktree service");

        let repo_dir = temp_dir.path().join("repo");
        setup_git_repo(repo_dir.as_path());
        let source = create_test_session(&runtime, repo_dir.display().to_string().as_str()).await;
        let team_id = create_test_team(team_comms.as_ref(), source.id.as_str()).await;

        let result = service
            .spawn_team_member(TeamMemberSpawnRequest {
                team_id: team_id.clone(),
                source_session_id: source.id.clone(),
                provider: None,
                model: None,
                title: Some("Worker".to_string()),
                prompt: Some("Do work".to_string()),
                permission_mode: None,
                metadata: Some(
                    serde_json::json!({ "__test_force_claim_failure_after_join": true }),
                ),
                worktree: Some(TeamMemberSpawnWorktreeInput {
                    mode: Some("create".to_string()),
                    name: Some("claim-fail-worker".to_string()),
                    branch_prefix: None,
                    base_ref: None,
                    run_init_script: Some(false),
                }),
                creator_agent_id: None,
                creator_compaction_subscription: None,
            })
            .await;
        assert!(
            result.is_err(),
            "spawn should fail under forced claim failure"
        );

        let team = team_comms
            .get_team(team_id.as_str())
            .await
            .expect("get team");
        assert_eq!(team.members.len(), 1, "spawned member must be rolled back");
        assert_eq!(team.members[0].agent_id, source.id);

        let sessions = runtime.list_sessions().await;
        let spawned_sessions = sessions
            .into_iter()
            .filter(|row| row.id != source.id)
            .collect::<Vec<_>>();
        assert_eq!(
            spawned_sessions.len(),
            1,
            "one spawned session should exist"
        );
        assert_eq!(
            spawned_sessions[0].status, "closed",
            "spawned session must be closed by rollback"
        );

        let journal = store
            .list_team_operation_journal(Some(team_id.as_str()))
            .expect("journal rows");
        assert_eq!(journal.len(), 1);
        assert_eq!(journal[0].stage, "rolled_back");

        let diagnostics = store
            .list_team_operation_diagnostics(Some(team_id.as_str()), None)
            .expect("diagnostics");
        assert!(
            diagnostics
                .iter()
                .any(|row| row.code == "spawn_claim_failed_after_join"),
            "rollback diagnostics must include deterministic claim failure code"
        );
    }

    #[tokio::test]
    async fn forced_onboarding_failure_after_join_rolls_back_cleanly() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("init store");
        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let service = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build worktree service");

        let repo_dir = temp_dir.path().join("repo");
        setup_git_repo(repo_dir.as_path());
        let source = create_test_session(&runtime, repo_dir.display().to_string().as_str()).await;
        let team_id = create_test_team(team_comms.as_ref(), source.id.as_str()).await;

        let result = service
            .spawn_team_member(TeamMemberSpawnRequest {
                team_id: team_id.clone(),
                source_session_id: source.id.clone(),
                provider: None,
                model: None,
                title: Some("Worker".to_string()),
                prompt: Some("Do work".to_string()),
                permission_mode: None,
                metadata: Some(
                    serde_json::json!({ "__test_force_onboarding_failure_after_join": true }),
                ),
                worktree: Some(TeamMemberSpawnWorktreeInput {
                    mode: Some("create".to_string()),
                    name: Some("onboarding-fail-worker".to_string()),
                    branch_prefix: None,
                    base_ref: None,
                    run_init_script: Some(false),
                }),
                creator_agent_id: None,
                creator_compaction_subscription: None,
            })
            .await;
        assert!(
            result.is_err(),
            "spawn should fail under forced onboarding failure"
        );

        let team = team_comms
            .get_team(team_id.as_str())
            .await
            .expect("get team");
        assert_eq!(team.members.len(), 1, "spawned member must be rolled back");
        assert_eq!(team.members[0].agent_id, source.id);

        let sessions = runtime.list_sessions().await;
        let spawned_sessions = sessions
            .into_iter()
            .filter(|row| row.id != source.id)
            .collect::<Vec<_>>();
        assert_eq!(
            spawned_sessions.len(),
            1,
            "one spawned session should exist"
        );
        assert_eq!(
            spawned_sessions[0].status, "closed",
            "spawned session must be closed by rollback"
        );

        let journal = store
            .list_team_operation_journal(Some(team_id.as_str()))
            .expect("journal rows");
        assert_eq!(journal.len(), 1);
        assert_eq!(journal[0].stage, "rolled_back");

        let diagnostics = store
            .list_team_operation_diagnostics(Some(team_id.as_str()), None)
            .expect("diagnostics");
        assert!(
            diagnostics
                .iter()
                .any(|row| row.code == "spawn_onboarding_failed_after_join"),
            "rollback diagnostics must include deterministic onboarding failure code"
        );
    }

    #[tokio::test]
    async fn spawn_use_existing_mode_reuses_existing_worktree() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("init store");
        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let service = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build worktree service");

        let repo_dir = temp_dir.path().join("repo");
        setup_git_repo(repo_dir.as_path());
        let source = create_test_session(&runtime, repo_dir.display().to_string().as_str()).await;
        let team_id = create_test_team(team_comms.as_ref(), source.id.as_str()).await;

        let existing = service
            .create_worktree(WorktreeCreateRequest {
                team_id: Some(team_id.clone()),
                source_session_id: source.id.clone(),
                repo_root: None,
                worktree_name: "use-existing-worker".to_string(),
                branch_prefix: Some("gg".to_string()),
                base_ref: None,
                deletion_policy: Some("retain_on_last_claim".to_string()),
                run_init_script: Some(false),
                created_by_session_id: Some(source.id.clone()),
                operation_id: Some("op_seed".to_string()),
            })
            .await
            .expect("create existing worktree");
        assert!(existing.created);

        let spawn = service
            .spawn_team_member(TeamMemberSpawnRequest {
                team_id,
                source_session_id: source.id.clone(),
                provider: None,
                model: None,
                title: Some("Existing Worker".to_string()),
                prompt: Some("Use existing".to_string()),
                permission_mode: None,
                metadata: None,
                worktree: Some(TeamMemberSpawnWorktreeInput {
                    mode: Some("use_existing".to_string()),
                    name: Some("use-existing-worker".to_string()),
                    branch_prefix: Some("gg".to_string()),
                    base_ref: None,
                    run_init_script: Some(false),
                }),
                creator_agent_id: None,
                creator_compaction_subscription: None,
            })
            .await
            .expect("spawn with use_existing");

        assert_eq!(spawn.worktree_assignment_mode, "reused");
        assert!(!spawn.worktree_created_by_operation);
        assert_eq!(
            spawn.worktree.as_ref().expect("spawn worktree").id,
            existing.worktree.id
        );
        assert_eq!(
            spawn.spawned_session.permission_mode.as_deref(),
            Some("full_auto")
        );
    }

    #[tokio::test]
    async fn spawn_worktree_inherits_source_permission_mode_when_present() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("init store");
        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let service = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build worktree service");

        let repo_dir = temp_dir.path().join("repo");
        setup_git_repo(repo_dir.as_path());
        let source = runtime
            .create_session(CreateSessionInput {
                provider: ProviderKind::Codex,
                model: Some("test-model".to_string()),
                cwd: Some(repo_dir.display().to_string()),
                permission_mode: Some("danger-full-access".to_string()),
                metadata: Some(serde_json::json!({ "suite": "runtime_tools_phase6" })),
            })
            .await
            .expect("create source session with permission mode");
        let team_id = create_test_team(team_comms.as_ref(), source.id.as_str()).await;

        let spawn = service
            .spawn_team_member(TeamMemberSpawnRequest {
                team_id,
                source_session_id: source.id,
                provider: None,
                model: None,
                title: Some("Inherited Mode Worker".to_string()),
                prompt: Some("Do work".to_string()),
                permission_mode: None,
                metadata: None,
                worktree: Some(TeamMemberSpawnWorktreeInput {
                    mode: Some("create".to_string()),
                    name: Some("inherited-mode-worker".to_string()),
                    branch_prefix: None,
                    base_ref: None,
                    run_init_script: Some(false),
                }),
                creator_agent_id: None,
                creator_compaction_subscription: None,
            })
            .await
            .expect("spawn with inherited source mode");

        assert_eq!(
            spawn.spawned_session.permission_mode.as_deref(),
            Some("danger-full-access")
        );
    }

    #[tokio::test]
    async fn forced_onboarding_failure_with_reused_worktree_releases_claim() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("init store");
        let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
        let service = RuntimeWorktreeService::new(
            store.clone(),
            runtime.clone(),
            team_comms.clone(),
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp_dir.path().join("worktrees").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".to_string(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            },
        )
        .expect("build worktree service");

        let repo_dir = temp_dir.path().join("repo");
        setup_git_repo(repo_dir.as_path());
        let source = create_test_session(&runtime, repo_dir.display().to_string().as_str()).await;
        let team_id = create_test_team(team_comms.as_ref(), source.id.as_str()).await;

        let existing = service
            .create_worktree(WorktreeCreateRequest {
                team_id: Some(team_id.clone()),
                source_session_id: source.id.clone(),
                repo_root: None,
                worktree_name: "reused-onboarding-fail-worker".to_string(),
                branch_prefix: Some("gg".to_string()),
                base_ref: None,
                deletion_policy: Some("retain_on_last_claim".to_string()),
                run_init_script: Some(false),
                created_by_session_id: Some(source.id.clone()),
                operation_id: Some("op_seed_reuse".to_string()),
            })
            .await
            .expect("create existing worktree");

        let result = service
            .spawn_team_member(TeamMemberSpawnRequest {
                team_id: team_id.clone(),
                source_session_id: source.id.clone(),
                provider: None,
                model: None,
                title: Some("Worker".to_string()),
                prompt: Some("Do work".to_string()),
                permission_mode: None,
                metadata: Some(
                    serde_json::json!({ "__test_force_onboarding_failure_after_join": true }),
                ),
                worktree: Some(TeamMemberSpawnWorktreeInput {
                    mode: Some("use_existing".to_string()),
                    name: Some("reused-onboarding-fail-worker".to_string()),
                    branch_prefix: Some("gg".to_string()),
                    base_ref: None,
                    run_init_script: Some(false),
                }),
                creator_agent_id: None,
                creator_compaction_subscription: None,
            })
            .await;
        assert!(
            result.is_err(),
            "spawn should fail under forced onboarding failure"
        );

        let team = team_comms
            .get_team(team_id.as_str())
            .await
            .expect("get team");
        assert_eq!(team.members.len(), 1, "spawned member must be rolled back");
        assert_eq!(team.members[0].agent_id, source.id);

        let sessions = runtime.list_sessions().await;
        let spawned_sessions = sessions
            .into_iter()
            .filter(|row| row.id != source.id)
            .collect::<Vec<_>>();
        assert_eq!(
            spawned_sessions.len(),
            1,
            "one spawned session should exist"
        );
        let spawned = &spawned_sessions[0];
        assert_eq!(
            spawned.status, "closed",
            "spawned session must be closed by rollback"
        );

        let hydrated = store.hydrate_runtime_state().expect("hydrate claims");
        let leaked_active_claim = hydrated.managed_worktree_claims.iter().find(|row| {
            row.worktree_id == existing.worktree.id
                && row.session_id == spawned.id
                && row.released_at.is_none()
        });
        assert!(
            leaked_active_claim.is_none(),
            "rollback must release active claim for reused worktree"
        );

        let follow_up_session =
            create_test_session(&runtime, repo_dir.display().to_string().as_str()).await;
        let follow_up_claim = service
            .claim_worktree(WorktreeClaimRequest {
                worktree_id: existing.worktree.id.clone(),
                session_id: follow_up_session.id.clone(),
                claim_role: "consumer".to_string(),
            })
            .await
            .expect("follow-up claim on reused worktree should not be blocked");
        assert_eq!(follow_up_claim.worktree.id, existing.worktree.id);
        service
            .release_worktree(WorktreeReleaseRequest {
                worktree_id: existing.worktree.id,
                session_id: follow_up_session.id,
                cleanup_if_last_claim: Some(false),
            })
            .await
            .expect("release follow-up claim");
    }
}
