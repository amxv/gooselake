use std::path::PathBuf;

use serde::{Deserialize, Serialize};

mod gateway;
mod process;
#[cfg(test)]
mod tests;
mod worktree;

pub use gateway::{
    RuntimeToolGateway, RuntimeToolGatewayDeps, StubTeamCommsService, StubWorktreeService,
    TeamMcpPolicy, TeamModelPreset,
};
pub use process::RuntimeProcessManager;
pub use worktree::RuntimeWorktreeService;

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

pub(crate) const GG_PROCESS_RUN: &str = "gg_process_run";
pub(crate) const GG_PROCESS_STATUS: &str = "gg_process_status";
pub(crate) const GG_PROCESS_LOGS: &str = "gg_process_logs";
pub(crate) const GG_PROCESS_KILL: &str = "gg_process_kill";
pub(crate) const GG_TEAM_STATUS: &str = "gg_team_status";
pub(crate) const GG_TEAM_MESSAGE: &str = "gg_team_message";
pub(crate) const GG_TEAM_MANAGE: &str = "gg_team_manage";
pub(crate) const GG_TEAM_ADD_IDEMPOTENCY_CACHE_TTL_SECS: u64 = 10 * 60;

pub(crate) fn parse_process_sequence(process_id: &str) -> Option<u64> {
    process_id
        .strip_prefix("proc_")
        .and_then(|value| value.parse::<u64>().ok())
}

pub(crate) fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_millis().min(i64::MAX as u128)) as i64
}

#[cfg(unix)]
pub(crate) fn exit_status_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;

    status.signal()
}

#[cfg(not(unix))]
pub(crate) fn exit_status_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}
