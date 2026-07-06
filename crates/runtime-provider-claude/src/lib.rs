mod auth;
mod bridge;
mod config;
mod paths;
mod provider;
mod runtime_provider;

#[cfg(test)]
mod tests;

pub use config::{ClaudeGgMcpConfig, ClaudeProviderConfig};
pub use paths::{standalone_claude_bridge_command_path, standalone_gg_mcp_server_command_path};
pub use provider::ClaudeProvider;
