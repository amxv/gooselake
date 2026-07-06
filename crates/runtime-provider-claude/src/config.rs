use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::paths::{default_bridge_args, default_bridge_command, default_gg_mcp_server_command};

pub(crate) const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
pub(crate) const DEFAULT_WAIT_TIMEOUT_MS: u32 = 300_000;
pub(crate) const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 10_000;
pub(crate) const DEFAULT_HEARTBEAT_FAILURE_THRESHOLD: u64 = 3;
pub(crate) const DEFAULT_MAX_BRIDGE_PROCESSES: usize = 4;
pub(crate) const DEFAULT_SESSIONS_PER_BRIDGE_SOFT_LIMIT: usize = 4;
pub(crate) const CLAUDE_BRIDGE_STDIN_QUEUE_CAPACITY: usize = 256;
pub(crate) const CLAUDE_BRIDGE_STDIN_FLUSH_BATCH_MAX: usize = 32;
pub(crate) const CLAUDE_STDOUT_WORKER_LANE_COUNT: usize = 16;
pub(crate) const CLAUDE_STDOUT_WORKER_QUEUE_CAPACITY: usize = 128;
pub(crate) const GG_MCP_MISSING_BAD_REQUEST: &str =
    "Missing ggMcpServer config for SDK mode session";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeGgMcpConfig {
    pub enabled: bool,
    pub server_name: String,
    pub command: String,
    pub args: Vec<String>,
    pub enable_process_tools: bool,
    pub gateway_url: Option<String>,
    pub gateway_token: Option<String>,
}

impl Default for ClaudeGgMcpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            server_name: "gg".to_string(),
            command: default_gg_mcp_server_command(),
            args: Vec::new(),
            enable_process_tools: true,
            gateway_url: None,
            gateway_token: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeProviderConfig {
    pub enabled: bool,
    pub config_dir: PathBuf,
    pub bridge_command: String,
    pub bridge_args: Vec<String>,
    pub max_bridges: usize,
    pub max_sessions_per_bridge: usize,
    pub request_timeout_ms: u64,
    pub default_wait_timeout_ms: u32,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_failure_threshold: u64,
    pub gg_mcp: ClaudeGgMcpConfig,
    pub bridge_env: BTreeMap<String, String>,
}

impl Default for ClaudeProviderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            config_dir: PathBuf::from(".gg-runtime/providers/claude/config"),
            bridge_command: default_bridge_command(),
            bridge_args: default_bridge_args(),
            max_bridges: DEFAULT_MAX_BRIDGE_PROCESSES,
            max_sessions_per_bridge: DEFAULT_SESSIONS_PER_BRIDGE_SOFT_LIMIT,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            default_wait_timeout_ms: DEFAULT_WAIT_TIMEOUT_MS,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            heartbeat_failure_threshold: DEFAULT_HEARTBEAT_FAILURE_THRESHOLD,
            gg_mcp: ClaudeGgMcpConfig::default(),
            bridge_env: BTreeMap::new(),
        }
    }
}
