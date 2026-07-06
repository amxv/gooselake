use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub(super) const DEFAULT_PROVIDER_DIR: &str = ".gg-runtime/providers/acp";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 300;
pub(super) const DEFAULT_PROTOCOL_VERSION: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpProviderConfig {
    pub enabled: bool,
    pub provider_dir: PathBuf,
    pub max_instances: usize,
    pub max_sessions_per_instance: usize,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub transport: String,
    pub request_timeout_secs: u64,
    pub wait_timeout_secs: u64,
    pub gg_mcp_enabled: bool,
    pub gg_mcp_server_name: String,
    pub gg_mcp_command: String,
    pub gg_mcp_args: Vec<String>,
    pub gg_mcp_enable_process_tools: bool,
    pub gg_mcp_gateway_url: Option<String>,
    pub gg_mcp_gateway_token: Option<String>,
}

impl Default for AcpProviderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider_dir: PathBuf::from(DEFAULT_PROVIDER_DIR),
            max_instances: 4,
            max_sessions_per_instance: 4,
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            transport: "stdio".to_string(),
            request_timeout_secs: DEFAULT_REQUEST_TIMEOUT_SECS,
            wait_timeout_secs: DEFAULT_WAIT_TIMEOUT_SECS,
            gg_mcp_enabled: true,
            gg_mcp_server_name: "gg".to_string(),
            gg_mcp_command: "gg-mcp-server".to_string(),
            gg_mcp_args: Vec::new(),
            gg_mcp_enable_process_tools: true,
            gg_mcp_gateway_url: None,
            gg_mcp_gateway_token: None,
        }
    }
}
