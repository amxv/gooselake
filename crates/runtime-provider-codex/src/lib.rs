mod auth;
mod config;
mod hard_fork;
mod lifecycle;
mod mcp_config;
mod protocol;
mod provider;
mod rebind;
mod state;
mod transport;

pub use auth::copy_codex_auth_file;
pub use config::{CodexGgMcpConfig, CodexProviderConfig};
pub use provider::CodexProvider;

#[cfg(test)]
mod tests;
