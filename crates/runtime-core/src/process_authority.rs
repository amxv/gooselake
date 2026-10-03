use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROCESS_COMPLETION_NOT_REQUIRED: &str = "not_required";
pub const PROCESS_COMPLETION_PENDING: &str = "pending";
pub const PROCESS_COMPLETION_INJECTING: &str = "injecting";
pub const PROCESS_COMPLETION_DELIVERED: &str = "delivered";

pub fn process_status_is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "timed_out" | "killed" | "canceled" | "interrupted"
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedProcessAdmission {
    pub process_id: String,
    pub owner_session_id: Option<String>,
    pub workspace_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub command: Value,
    pub cwd: Option<String>,
    pub timeout_ms: Option<i64>,
    pub stdout_path: String,
    pub stderr_path: String,
    pub capture_limit_bytes: i64,
    pub admitted_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedProcessRecord {
    pub process_id: String,
    pub owner_session_id: Option<String>,
    pub workspace_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub command: Value,
    pub cwd: Option<String>,
    pub timeout_ms: Option<i64>,
    pub status: String,
    pub admission_order: i64,
    pub queue_order: i64,
    pub claim_generation: i64,
    pub claimed_at: Option<i64>,
    pub capture_limit_bytes: i64,
    pub pid: Option<i64>,
    pub os_start_identity: Option<String>,
    pub execution_started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub execution_duration_ms: Option<i64>,
    pub terminal_recorded_at: Option<i64>,
    pub terminal_reason: Option<String>,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub stdout_path: String,
    pub stderr_path: String,
    pub stdout_captured_bytes: i64,
    pub stderr_captured_bytes: i64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub cancel_requested: bool,
    pub completion_state: String,
    pub completion_turn_id: Option<String>,
    pub completion_attempt_count: i64,
    pub completion_last_error: Option<String>,
    pub completion_updated_at: Option<i64>,
    pub admitted_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedProcessTerminalUpdate {
    pub status: String,
    pub terminal_reason: Option<String>,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub ended_at: i64,
    pub execution_duration_ms: Option<i64>,
    pub stdout_captured_bytes: i64,
    pub stderr_captured_bytes: i64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub completion_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessCompletionUpdate {
    pub state: String,
    pub turn_id: Option<String>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSchedulerSettings {
    pub max_concurrent: usize,
    #[serde(default)]
    pub workspace_max_concurrent: BTreeMap<String, usize>,
    pub capture_limit_bytes: usize,
    pub paused: bool,
    pub pause_reason: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessQueueEntry {
    pub process_id: String,
    pub owner_session_id: Option<String>,
    pub workspace_id: Option<String>,
    pub command: Value,
    pub cwd: Option<String>,
    pub timeout_ms: Option<i64>,
    pub status: String,
    pub admission_order: i64,
    pub queue_order: i64,
    pub claim_generation: i64,
    pub claimed_at: Option<i64>,
    pub pid: Option<i64>,
    pub admitted_at: i64,
    pub execution_started_at: Option<i64>,
}

impl From<&ManagedProcessRecord> for ProcessQueueEntry {
    fn from(record: &ManagedProcessRecord) -> Self {
        Self {
            process_id: record.process_id.clone(),
            owner_session_id: record.owner_session_id.clone(),
            workspace_id: record.workspace_id.clone(),
            command: record.command.clone(),
            cwd: record.cwd.clone(),
            timeout_ms: record.timeout_ms,
            status: record.status.clone(),
            admission_order: record.admission_order,
            queue_order: record.queue_order,
            claim_generation: record.claim_generation,
            claimed_at: record.claimed_at,
            pid: record.pid,
            admitted_at: record.admitted_at,
            execution_started_at: record.execution_started_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSchedulerSnapshot {
    pub settings: ProcessSchedulerSettings,
    pub queued: Vec<ProcessQueueEntry>,
    pub active: Vec<ProcessQueueEntry>,
}

impl ProcessSchedulerSettings {
    pub fn normalized(mut self) -> Self {
        const DEFAULT_MAX_CONCURRENT: usize = 32;
        const MAX_CONCURRENT: usize = 1_024;
        const DEFAULT_CAPTURE_LIMIT: usize = 20_000_000;
        const MAX_CAPTURE_LIMIT: usize = 1_000_000_000;

        self.max_concurrent = if self.max_concurrent == 0 {
            DEFAULT_MAX_CONCURRENT
        } else {
            self.max_concurrent.min(MAX_CONCURRENT)
        };
        self.capture_limit_bytes = if self.capture_limit_bytes == 0 {
            DEFAULT_CAPTURE_LIMIT
        } else {
            self.capture_limit_bytes.min(MAX_CAPTURE_LIMIT)
        };
        self.workspace_max_concurrent = self
            .workspace_max_concurrent
            .into_iter()
            .filter_map(|(workspace_id, limit)| {
                let workspace_id = workspace_id.trim();
                if workspace_id.is_empty() || limit == 0 {
                    None
                } else {
                    Some((workspace_id.to_string(), limit.min(MAX_CONCURRENT)))
                }
            })
            .collect();
        if !self.paused {
            self.pause_reason = None;
        }
        self
    }
}
