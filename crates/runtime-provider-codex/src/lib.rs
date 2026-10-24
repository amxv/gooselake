mod auth;
mod config;
mod dynamic_tools;
mod hard_fork;
mod lifecycle;
mod mcp_config;
mod preferences;
mod protocol;
mod provider;
mod rebind;
mod retirement_close;
mod state;
mod transport;

pub use auth::copy_codex_auth_file;
pub use config::{CodexGgMcpConfig, CodexProviderConfig};
pub use provider::CodexProvider;

#[cfg(test)]
mod tests;
