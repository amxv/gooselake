use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexProviderConfig {
    pub enabled: bool,
    pub home_dir: PathBuf,
    pub max_transports: usize,
    pub max_sessions_per_transport: usize,
    pub gg_mcp: CodexGgMcpConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexGgMcpConfig {
    pub enabled: bool,
    pub server_name: String,
    pub command: String,
    pub args: Vec<String>,
    pub enable_process_tools: bool,
    pub gateway_url: Option<String>,
    pub gateway_token: Option<String>,
}

impl Default for CodexGgMcpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            server_name: "gg".to_string(),
            command: "gg-mcp-server".to_string(),
            args: Vec::new(),
            enable_process_tools: true,
            gateway_url: None,
            gateway_token: None,
        }
    }
}
