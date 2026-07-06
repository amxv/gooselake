use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

pub const GOOSELAKE_RUNTIME_SOURCE_KIND: &str = "gooselake-runtime";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GoosetowerConfig {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub tickets: TicketConfig,
    pub runtimes: RuntimeRegistryConfig,
    pub websocket: WebSocketConfig,
    pub replay: ReplayConfig,
    pub materializer: MaterializerConfig,
    pub lanes: LaneQueueConfig,
    pub debug: DebugConfig,
    #[serde(skip)]
    config_file_dir: Option<PathBuf>,
}

impl Default for GoosetowerConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            auth: AuthConfig::default_dev(),
            tickets: TicketConfig::default(),
            runtimes: RuntimeRegistryConfig::default(),
            websocket: WebSocketConfig::default(),
            replay: ReplayConfig::default(),
            materializer: MaterializerConfig::default(),
            lanes: LaneQueueConfig::default(),
            debug: DebugConfig::default(),
            config_file_dir: None,
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
                let mut config = toml::from_str::<Self>(&content).with_context(|| {
                    format!("failed to parse config file {}", config_path.display())
                })?;
                config.config_file_dir = resolve_config_file_dir(config_path);
                Ok(config)
            }
            None => Ok(Self::default()),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.server.bind_address.trim().is_empty() {
            return Err(anyhow!("server.bind_address cannot be empty"));
        }
        if self.server.public_base_url.trim().is_empty() {
            return Err(anyhow!("server.public_base_url cannot be empty"));
        }
        self.allowed_gooseweb_origins()?;
        self.bootstrap_api_auth()?;
        self.tickets.validate()?;
        self.runtimes.validate()?;
        if self.websocket.max_message_bytes == 0 {
            return Err(anyhow!(
                "websocket.max_message_bytes must be greater than zero"
            ));
        }
        if self.websocket.heartbeat_interval_ms == 0 {
            return Err(anyhow!(
                "websocket.heartbeat_interval_ms must be greater than zero"
            ));
        }
        if self.replay.max_events_per_request == 0 {
            return Err(anyhow!(
                "replay.max_events_per_request must be greater than zero"
            ));
        }
        if self.replay.source_stale_after_ms == 0 {
            return Err(anyhow!(
                "replay.source_stale_after_ms must be greater than zero"
            ));
        }
        if self.materializer.event_buffer_size == 0 {
            return Err(anyhow!(
                "materializer.event_buffer_size must be greater than zero"
            ));
        }
        if self.lanes.critical_capacity == 0
            || self.lanes.state_capacity == 0
            || self.lanes.tokens_capacity == 0
            || self.lanes.bulk_capacity == 0
        {
            return Err(anyhow!("lane queue capacities must be greater than zero"));
        }
        Ok(())
    }

    pub fn allowed_gooseweb_origins(&self) -> Result<Vec<String>> {
        self.server
            .allowed_gooseweb_origins
            .iter()
            .map(|origin| {
                let trimmed = origin.trim();
                if trimmed.is_empty() {
                    return Err(anyhow!("server.allowed_gooseweb_origins cannot contain empty values"));
                }
                if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
                    return Err(anyhow!(
                        "server.allowed_gooseweb_origins entry must start with http:// or https://: {trimmed}"
                    ));
                }
                Ok(trimmed.trim_end_matches('/').to_string())
            })
            .collect()
    }

    pub fn bootstrap_api_auth(&self) -> Result<ResolvedApiAuth> {
        if let Some(token) = self.auth.api_token.as_ref() {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                return Err(anyhow!("auth.api_token cannot be empty"));
            }
            return Ok(ResolvedApiAuth {
                bearer_token: trimmed.to_string(),
                source: AuthTokenSource::InlineConfig,
            });
        }

        let token_path = self.resolve_optional_path(self.auth.api_token_file.as_ref());
        let token_path = token_path
            .ok_or_else(|| anyhow!("auth.api_token or auth.api_token_file is required"))?;
        let token = std::fs::read_to_string(&token_path)
            .with_context(|| format!("failed to read api token file {}", token_path.display()))?;
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "auth.api_token_file cannot point to an empty token file: {}",
                token_path.display()
            ));
        }

        Ok(ResolvedApiAuth {
            bearer_token: trimmed.to_string(),
            source: AuthTokenSource::TokenFile { path: token_path },
        })
    }

    pub fn resolve_runtime_auth(&self, source: &RuntimeSourceConfig) -> Result<Option<String>> {
        if let Some(token) = source.bearer_token.as_ref() {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                return Err(anyhow!(
                    "runtime source {} bearer_token cannot be empty",
                    source.source_id
                ));
            }
            return Ok(Some(trimmed.to_string()));
        }
        let Some(token_path) = self.resolve_optional_path(source.bearer_token_file.as_ref()) else {
            return Ok(None);
        };
        let token = std::fs::read_to_string(&token_path).with_context(|| {
            format!(
                "failed to read runtime source {} bearer token file {}",
                source.source_id,
                token_path.display()
            )
        })?;
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "runtime source {} bearer_token_file cannot be empty",
                source.source_id
            ));
        }
        Ok(Some(trimmed.to_string()))
    }

    fn resolve_optional_path(&self, path: Option<&PathBuf>) -> Option<PathBuf> {
        path.map(|path| {
            if path.is_absolute() {
                path.clone()
            } else if let Some(config_dir) = self.config_file_dir.as_ref() {
                config_dir.join(path)
            } else {
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("."))
                    .join(path)
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_address: String,
    pub public_base_url: String,
    pub allowed_gooseweb_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8090".to_string(),
            public_base_url: "http://localhost:8090".to_string(),
            allowed_gooseweb_origins: vec!["http://localhost:3000".to_string()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub api_token: Option<String>,
    pub api_token_file: Option<PathBuf>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            api_token: None,
            api_token_file: None,
        }
    }
}

impl AuthConfig {
    fn default_dev() -> Self {
        Self {
            api_token: Some("dev-goosetower-token".to_string()),
            api_token_file: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TicketConfig {
    pub issuer: String,
    pub audience: String,
    pub signing_key: Option<String>,
    pub signing_key_file: Option<PathBuf>,
    pub verification_key: Option<String>,
    pub verification_key_file: Option<PathBuf>,
    pub ttl_secs: u64,
}

impl Default for TicketConfig {
    fn default() -> Self {
        Self {
            issuer: "gooseweb".to_string(),
            audience: "goosetower".to_string(),
            signing_key: Some("dev-ticket-signing-key".to_string()),
            signing_key_file: None,
            verification_key: None,
            verification_key_file: None,
            ttl_secs: 60,
        }
    }
}

impl TicketConfig {
    fn validate(&self) -> Result<()> {
        if self.issuer.trim().is_empty() {
            return Err(anyhow!("tickets.issuer cannot be empty"));
        }
        if self.audience.trim().is_empty() {
            return Err(anyhow!("tickets.audience cannot be empty"));
        }
        if self.signing_key.is_none() && self.signing_key_file.is_none() {
            return Err(anyhow!(
                "tickets.signing_key or tickets.signing_key_file is required"
            ));
        }
        if self.ttl_secs == 0 {
            return Err(anyhow!("tickets.ttl_secs must be greater than zero"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeRegistryConfig {
    pub sources: Vec<RuntimeSourceConfig>,
}

impl Default for RuntimeRegistryConfig {
    fn default() -> Self {
        Self {
            sources: vec![RuntimeSourceConfig::default()],
        }
    }
}

impl RuntimeRegistryConfig {
    fn validate(&self) -> Result<()> {
        if self.sources.is_empty() {
            return Err(anyhow!("runtimes.sources must contain at least one source"));
        }
        let mut source_ids = BTreeSet::new();
        for source in &self.sources {
            source.validate()?;
            if !source_ids.insert(source.source_id.clone()) {
                return Err(anyhow!(
                    "runtime source source_id must be unique: {}",
                    source.source_id
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeSourceConfig {
    pub source_id: String,
    pub source_epoch: String,
    pub source_kind: String,
    pub base_url: String,
    pub bearer_token: Option<String>,
    pub bearer_token_file: Option<PathBuf>,
    pub enabled: bool,
    pub display_name: String,
    pub workspace_id: String,
}

impl Default for RuntimeSourceConfig {
    fn default() -> Self {
        Self {
            source_id: "local".to_string(),
            source_epoch: "static-0".to_string(),
            source_kind: GOOSELAKE_RUNTIME_SOURCE_KIND.to_string(),
            base_url: "http://127.0.0.1:8080".to_string(),
            bearer_token: None,
            bearer_token_file: None,
            enabled: true,
            display_name: "Local Gooselake Runtime".to_string(),
            workspace_id: "default".to_string(),
        }
    }
}

impl RuntimeSourceConfig {
    fn validate(&self) -> Result<()> {
        if self.source_id.trim().is_empty() {
            return Err(anyhow!("runtime source source_id cannot be empty"));
        }
        if self.source_epoch.trim().is_empty() {
            return Err(anyhow!(
                "runtime source {} source_epoch cannot be empty",
                self.source_id
            ));
        }
        if self.source_kind != GOOSELAKE_RUNTIME_SOURCE_KIND {
            return Err(anyhow!(
                "runtime source {} source_kind must be {}",
                self.source_id,
                GOOSELAKE_RUNTIME_SOURCE_KIND
            ));
        }
        if !(self.base_url.starts_with("http://") || self.base_url.starts_with("https://")) {
            return Err(anyhow!(
                "runtime source {} base_url must start with http:// or https://",
                self.source_id
            ));
        }
        if self.display_name.trim().is_empty() {
            return Err(anyhow!(
                "runtime source {} display_name cannot be empty",
                self.source_id
            ));
        }
        if self.workspace_id.trim().is_empty() {
            return Err(anyhow!(
                "runtime source {} workspace_id cannot be empty",
                self.source_id
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebSocketConfig {
    pub max_message_bytes: usize,
    pub heartbeat_interval_ms: u64,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: 1024 * 1024,
            heartbeat_interval_ms: 15_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplayConfig {
    pub max_events_per_request: usize,
    pub source_stale_after_ms: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            max_events_per_request: 1000,
            source_stale_after_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MaterializerConfig {
    pub event_buffer_size: usize,
    pub snapshot_cache_size: usize,
}

impl Default for MaterializerConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 8192,
            snapshot_cache_size: 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LaneQueueConfig {
    pub critical_capacity: usize,
    pub state_capacity: usize,
    pub tokens_capacity: usize,
    pub bulk_capacity: usize,
}

impl Default for LaneQueueConfig {
    fn default() -> Self {
        Self {
            critical_capacity: 4096,
            state_capacity: 8192,
            tokens_capacity: 16384,
            bulk_capacity: 2048,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DebugConfig {
    pub endpoints_enabled: bool,
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            endpoints_enabled: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedApiAuth {
    pub bearer_token: String,
    pub source: AuthTokenSource,
}

#[derive(Debug, Clone)]
pub enum AuthTokenSource {
    InlineConfig,
    TokenFile { path: PathBuf },
}

fn resolve_config_file_dir(config_path: &Path) -> Option<PathBuf> {
    let absolute_path = if config_path.is_absolute() {
        config_path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(config_path)
    };
    absolute_path.parent().map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_include_gateway_bind_address() {
        let config = GoosetowerConfig::default();
        assert_eq!(config.server.bind_address, "0.0.0.0:8090");
        assert_eq!(config.server.public_base_url, "http://localhost:8090");
        assert_eq!(config.tickets.issuer, "gooseweb");
        assert_eq!(config.tickets.audience, "goosetower");
        assert_eq!(config.websocket.max_message_bytes, 1024 * 1024);
        assert_eq!(config.websocket.heartbeat_interval_ms, 15_000);
        assert_eq!(config.runtimes.sources.len(), 1);
        assert_eq!(
            config.runtimes.sources[0].source_kind,
            GOOSELAKE_RUNTIME_SOURCE_KIND
        );
        config.validate().expect("default config validates");
    }

    #[test]
    fn origin_allowlist_normalizes_and_rejects_invalid_origins() {
        let mut config = GoosetowerConfig::default();
        config.server.allowed_gooseweb_origins = vec!["https://gooseweb.example.com/".to_string()];
        assert_eq!(
            config.allowed_gooseweb_origins().expect("origins"),
            vec!["https://gooseweb.example.com"]
        );

        config.server.allowed_gooseweb_origins = vec!["gooseweb.example.com".to_string()];
        assert!(config.allowed_gooseweb_origins().is_err());
    }

    #[test]
    fn runtime_registry_parses_static_source_config() {
        let config = toml::from_str::<GoosetowerConfig>(
            r#"
[auth]
api_token = "tower-token"

[tickets]
issuer = "gooseweb-prod"
audience = "goosetower-prod"
signing_key = "secret"

[[runtimes.sources]]
source_id = "vps-primary"
source_kind = "gooselake-runtime"
base_url = "https://runtime.example.com"
bearer_token = "runtime-token"
enabled = true
display_name = "Primary VPS"
workspace_id = "workspace-1"
"#,
        )
        .expect("parse config");

        config.validate().expect("validate config");
        let source = &config.runtimes.sources[0];
        assert_eq!(source.source_id, "vps-primary");
        assert_eq!(source.source_kind, GOOSELAKE_RUNTIME_SOURCE_KIND);
        assert_eq!(source.base_url, "https://runtime.example.com");
        assert_eq!(source.display_name, "Primary VPS");
        assert_eq!(source.workspace_id, "workspace-1");
    }

    #[test]
    fn token_file_resolution_uses_config_file_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let api_token_path = temp_dir.path().join("tower-token");
        let runtime_token_path = temp_dir.path().join("runtime-token");
        std::fs::write(&api_token_path, "tower-secret\n").expect("write api token");
        std::fs::write(&runtime_token_path, "runtime-secret\n").expect("write runtime token");
        let config_path = temp_dir.path().join("goosetower.toml");
        std::fs::write(
            &config_path,
            r#"
[auth]
api_token_file = "tower-token"

[tickets]
issuer = "gooseweb"
audience = "goosetower"
signing_key = "ticket-secret"

[[runtimes.sources]]
source_id = "local"
source_kind = "gooselake-runtime"
base_url = "http://127.0.0.1:8080"
bearer_token_file = "runtime-token"
enabled = true
display_name = "Local"
workspace_id = "default"
"#,
        )
        .expect("write config");

        let config = GoosetowerConfig::load(Some(config_path.as_path())).expect("load config");
        let api_auth = config.bootstrap_api_auth().expect("api auth");
        assert_eq!(api_auth.bearer_token, "tower-secret");
        let runtime_auth = config
            .resolve_runtime_auth(&config.runtimes.sources[0])
            .expect("runtime auth");
        assert_eq!(runtime_auth.as_deref(), Some("runtime-secret"));
    }
}
