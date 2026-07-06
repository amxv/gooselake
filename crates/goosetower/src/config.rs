use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GoosetowerConfig {
    pub server: ServerConfig,
}

impl Default for GoosetowerConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
        }
    }
}

impl GoosetowerConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(config_path) => {
                let content = std::fs::read_to_string(config_path).with_context(|| {
                    format!("failed to read config file {}", config_path.display())
                })?;
                toml::from_str::<Self>(&content).with_context(|| {
                    format!("failed to parse config file {}", config_path.display())
                })
            }
            None => Ok(Self::default()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_address: String,
    pub public_base_url: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8090".to_string(),
            public_base_url: "http://localhost:8090".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_include_gateway_bind_address() {
        let config = GoosetowerConfig::default();
        assert_eq!(config.server.bind_address, "0.0.0.0:8090");
        assert_eq!(config.server.public_base_url, "http://localhost:8090");
    }
}
