use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::{ProcessSchedulerSettings, ProcessSchedulerSnapshot, RuntimeError, RuntimeEventRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRunRequest {
    pub caller_session_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub command: String,
    pub cwd: Option<String>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessListRequest {
    pub caller_session_id: Option<String>,
    pub include_completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessGetRequest {
    pub process_id: String,
    pub caller_session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessLogReadRequest {
    pub process_id: String,
    pub caller_session_id: Option<String>,
    pub stream: Option<String>,
    pub head_lines: Option<usize>,
    pub tail_lines: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessKillRequest {
    pub process_id: String,
    pub caller_session_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessSummary {
    pub process_id: String,
    pub session_id: Option<String>,
    pub pid: Option<i64>,
    pub status: String,
    pub command: Value,
    pub cwd: Option<String>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessDetails {
    pub process: ProcessSummary,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub timeout_ms: Option<i64>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessLogsChunk {
    pub process_id: String,
    pub stream: String,
    pub content: String,
    pub head_lines: usize,
    pub tail_lines: usize,
    pub truncated: bool,
    pub bytes: usize,
}

#[async_trait]
pub trait ProcessManager: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn run_process(&self, request: ProcessRunRequest)
        -> Result<ProcessDetails, RuntimeError>;

    async fn list_processes(
        &self,
        request: ProcessListRequest,
    ) -> Result<Vec<ProcessSummary>, RuntimeError>;

    async fn get_process(&self, request: ProcessGetRequest)
        -> Result<ProcessDetails, RuntimeError>;

    async fn read_process_logs(
        &self,
        request: ProcessLogReadRequest,
    ) -> Result<Vec<ProcessLogsChunk>, RuntimeError>;

    async fn kill_process(
        &self,
        request: ProcessKillRequest,
    ) -> Result<ProcessDetails, RuntimeError>;

    async fn scheduler_snapshot(&self) -> Result<ProcessSchedulerSnapshot, RuntimeError>;

    async fn update_scheduler_settings(
        &self,
        settings: ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSnapshot, RuntimeError>;

    async fn reorder_process_queue(
        &self,
        process_id: String,
        before_process_id: Option<String>,
        after_process_id: Option<String>,
    ) -> Result<ProcessSchedulerSnapshot, RuntimeError>;

    async fn replay_events(
        &self,
        process_id: String,
        caller_session_id: Option<String>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError>;

    fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEventRecord>;
}
