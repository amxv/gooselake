use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    process_status_is_terminal, ManagedProcessAdmission, ProcessDetails, ProcessGetRequest,
    ProcessKillRequest, ProcessListRequest, ProcessLogReadRequest, ProcessLogsChunk,
    ProcessManager, ProcessRunRequest, ProcessSchedulerSettings, ProcessSchedulerSnapshot,
    ProcessSummary, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
};
use serde_json::json;
use tokio::sync::broadcast;

use crate::now_ms;
use crate::process::{LiveProcess, ProcessControl, RuntimeProcessManager};
use crate::process_helpers::{
    create_exclusive_log_bundle, remove_log_bundle, summary_from_record, tail_utf8_bytes,
};

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
        let workspace_id = match request.caller_session_id.as_deref() {
            Some(caller) => self.caller_workspace_id(caller)?,
            None => None,
        };
        let cwd = request
            .cwd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let timeout_ms = match request.timeout_ms {
            Some(0) => None,
            Some(value) => Some(i64::try_from(value).unwrap_or(i64::MAX)),
            None => Some(i64::try_from(self.config.default_timeout_ms).unwrap_or(i64::MAX)),
        };
        let capture_limit_bytes = self.settings.read().await.capture_limit_bytes;

        let mut last_collision = None;
        for _ in 0..64 {
            let process_id = format!("proc_{:032x}", rand::random::<u128>());
            let stdout_path = self.config.log_dir.join(format!("{process_id}.stdout.log"));
            let stderr_path = self.config.log_dir.join(format!("{process_id}.stderr.log"));
            if !create_exclusive_log_bundle(&stdout_path, &stderr_path)? {
                last_collision = Some(process_id);
                continue;
            }

            let admission = ManagedProcessAdmission {
                process_id: process_id.clone(),
                owner_session_id: request.caller_session_id.clone(),
                workspace_id: workspace_id.clone(),
                tool_call_id: request.tool_call_id.clone(),
                command: json!({ "command": command }),
                cwd: cwd.clone(),
                timeout_ms,
                stdout_path: stdout_path.display().to_string(),
                stderr_path: stderr_path.display().to_string(),
                capture_limit_bytes: i64::try_from(capture_limit_bytes).unwrap_or(i64::MAX),
                admitted_at: now_ms(),
            };
            let record = match self.store.admit_managed_process(&admission) {
                Ok(record) => record,
                Err(error) => {
                    if self.store.get_managed_process(&process_id)?.is_none() {
                        remove_log_bundle(&stdout_path, &stderr_path);
                    }
                    return Err(error);
                }
            };

            self.live_processes.write().await.insert(
                process_id.clone(),
                Arc::new(LiveProcess::from_record(&record)),
            );
            self.append_process_event(
                &process_id,
                request.caller_session_id.clone(),
                "process.queued",
                RuntimeEventCriticality::Critical,
                json!({
                    "process_id": process_id,
                    "status": "queued",
                    "queue_order": record.queue_order,
                    "workspace_id": record.workspace_id,
                }),
            )
            .await;
            self.scheduler_notify.notify_one();
            return Ok(self.details_from_record(record).await);
        }

        Err(RuntimeError::Conflict(format!(
            "unable to allocate collision-free process id after repeated attempts{}",
            last_collision
                .map(|id| format!(" (last collision: {id})"))
                .unwrap_or_default()
        )))
    }

    async fn list_processes(
        &self,
        request: ProcessListRequest,
    ) -> Result<Vec<ProcessSummary>, RuntimeError> {
        let caller_workspace = match request.caller_session_id.as_deref() {
            Some(caller) => self.caller_workspace_id(caller)?,
            None => None,
        };
        let mut rows = self
            .store
            .list_managed_processes()?
            .into_iter()
            .filter(|record| {
                if let Some(caller) = request.caller_session_id.as_deref() {
                    let owner = record.owner_session_id.as_deref() == Some(caller);
                    let same_workspace = caller_workspace.as_deref().is_some()
                        && caller_workspace.as_deref() == record.workspace_id.as_deref();
                    if !owner && !same_workspace {
                        return false;
                    }
                }
                request.include_completed || !process_status_is_terminal(&record.status)
            })
            .map(|record| summary_from_record(&record))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| right.started_at.cmp(&left.started_at));
        Ok(rows)
    }

    async fn get_process(
        &self,
        request: ProcessGetRequest,
    ) -> Result<ProcessDetails, RuntimeError> {
        let record = self
            .store
            .get_managed_process(&request.process_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("process {}", request.process_id)))?;
        self.ensure_visible(&record, request.caller_session_id.as_deref())?;
        Ok(self.details_from_record(record).await)
    }

    async fn read_process_logs(
        &self,
        request: ProcessLogReadRequest,
    ) -> Result<Vec<ProcessLogsChunk>, RuntimeError> {
        let record = self
            .store
            .get_managed_process(&request.process_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("process {}", request.process_id)))?;
        self.ensure_visible(&record, request.caller_session_id.as_deref())?;

        let mut streams = Vec::new();
        match request.stream.as_deref() {
            Some("stdout") => streams.push((
                "stdout",
                record.stdout_path.clone(),
                record.stdout_truncated,
            )),
            Some("stderr") => streams.push((
                "stderr",
                record.stderr_path.clone(),
                record.stderr_truncated,
            )),
            Some(other) => {
                return Err(RuntimeError::InvalidState(format!(
                    "unsupported stream {other}"
                )))
            }
            None => {
                streams.push((
                    "stdout",
                    record.stdout_path.clone(),
                    record.stdout_truncated,
                ));
                streams.push((
                    "stderr",
                    record.stderr_path.clone(),
                    record.stderr_truncated,
                ));
            }
        }

        let mut chunks = Vec::new();
        for (stream, path, stream_truncated) in streams {
            let content = std::fs::read_to_string(Path::new(&path)).unwrap_or_default();
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
                out.push_str(
                    "...
",
                );
            }
            for line in lines.iter().skip(tail_start) {
                out.push_str(line);
                out.push('\n');
            }
            if let Some(max_bytes) = request.max_bytes {
                if out.len() > max_bytes {
                    out = tail_utf8_bytes(&out, max_bytes);
                    truncated = true;
                }
            }

            chunks.push(ProcessLogsChunk {
                process_id: record.process_id.clone(),
                stream: stream.to_string(),
                bytes: out.len(),
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
        let record = self
            .store
            .get_managed_process(&request.process_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("process {}", request.process_id)))?;
        self.ensure_cancel_owner(&record, request.caller_session_id.as_deref())?;
        if process_status_is_terminal(&record.status) {
            return Ok(self.details_from_record(record).await);
        }

        if matches!(record.status.as_str(), "queued" | "launch_reserved") {
            if let Some(canceled) = self.store.cancel_queued_managed_process(
                &record.process_id,
                request.caller_session_id.as_deref(),
                now_ms(),
            )? {
                self.append_process_event(
                    &record.process_id,
                    record.owner_session_id.clone(),
                    "process.canceled",
                    RuntimeEventCriticality::Critical,
                    json!({
                        "process_id": record.process_id,
                        "status": "canceled",
                        "reason": request.reason.unwrap_or_else(|| "requested".to_string()),
                        "before_launch": true,
                    }),
                )
                .await;
                self.scheduler_notify.notify_one();
                return Ok(self.details_from_record(canceled).await);
            }
        }

        let updated = self
            .store
            .request_managed_process_cancel(
                &record.process_id,
                request.caller_session_id.as_deref(),
                now_ms(),
            )?
            .ok_or_else(|| RuntimeError::NotFound(format!("process {}", record.process_id)))?;
        if updated.status == "running" {
            if let Some(live) = self
                .live_processes
                .read()
                .await
                .get(&record.process_id)
                .cloned()
            {
                if let Some(control_tx) = live.control_tx.lock().await.as_ref() {
                    let _ = control_tx.send(ProcessControl::Kill);
                }
            }
            self.append_process_event(
                &record.process_id,
                record.owner_session_id.clone(),
                "process.kill_requested",
                RuntimeEventCriticality::Critical,
                json!({
                    "process_id": record.process_id,
                    "reason": request.reason.unwrap_or_else(|| "requested".to_string()),
                }),
            )
            .await;
        }
        Ok(self.details_from_record(updated).await)
    }

    async fn scheduler_snapshot(&self) -> Result<ProcessSchedulerSnapshot, RuntimeError> {
        self.build_scheduler_snapshot().await
    }

    async fn update_scheduler_settings(
        &self,
        settings: ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSnapshot, RuntimeError> {
        self.replace_scheduler_settings(settings).await?;
        self.build_scheduler_snapshot().await
    }

    async fn reorder_process_queue(
        &self,
        process_id: String,
        before_process_id: Option<String>,
        after_process_id: Option<String>,
    ) -> Result<ProcessSchedulerSnapshot, RuntimeError> {
        self.reorder_queue(
            &process_id,
            before_process_id.as_deref(),
            after_process_id.as_deref(),
        )
        .await?;
        self.build_scheduler_snapshot().await
    }

    async fn replay_events(
        &self,
        process_id: String,
        caller_session_id: Option<String>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<runtime_core::RuntimeEventRecord>, RuntimeError> {
        let record = self
            .store
            .get_managed_process(&process_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("process {process_id}")))?;
        self.ensure_visible(&record, caller_session_id.as_deref())?;
        self.store.list_runtime_events(
            Some((RuntimeEventScope::Process, &process_id)),
            after_seq,
            limit.max(1),
        )
    }

    fn subscribe_events(&self) -> broadcast::Receiver<runtime_core::RuntimeEventRecord> {
        self.event_tx.subscribe()
    }
}
