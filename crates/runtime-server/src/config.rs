use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeServerConfig {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub data: DataConfig,
    pub providers: ProvidersConfig,
    pub events: EventsConfig,
    pub processes: ProcessesConfig,
    pub teams: TeamsConfig,
    pub worktrees: WorktreesConfig,
    #[serde(skip)]
    config_file_dir: Option<PathBuf>,
}

impl Default for RuntimeServerConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            auth: AuthConfig::default(),
            data: DataConfig::default(),
            providers: ProvidersConfig::default(),
            events: EventsConfig::default(),
            processes: ProcessesConfig::default(),
            teams: TeamsConfig::default(),
            worktrees: WorktreesConfig::default(),
            config_file_dir: None,
        }
    }
}

impl RuntimeServerConfig {
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

    pub fn ensure_data_dirs(&self) -> Result<()> {
        let root_dir = self.resolve_root_dir();
        std::fs::create_dir_all(&root_dir)
            .with_context(|| format!("failed to create {}", root_dir.display()))?;
        std::fs::create_dir_all(self.resolve_data_path(&self.data.logs_dir))
            .with_context(|| "failed to create logs directory".to_string())?;
        std::fs::create_dir_all(self.resolve_data_path(&self.data.providers_dir))
            .with_context(|| "failed to create providers directory".to_string())?;
        std::fs::create_dir_all(self.resolve_worktree_root())
            .with_context(|| "failed to create worktree root directory".to_string())?;
        Ok(())
    }

    pub fn resolve_root_dir(&self) -> PathBuf {
        if self.data.root_dir.is_absolute() {
            return self.data.root_dir.clone();
        }
        if let Some(config_dir) = self.config_file_dir.as_ref() {
            return config_dir.join(&self.data.root_dir);
        }
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(&self.data.root_dir),
            Err(_) => self.data.root_dir.clone(),
        }
    }

    pub fn resolve_data_path(&self, raw: &Path) -> PathBuf {
        if raw.is_absolute() {
            return raw.to_path_buf();
        }
        self.resolve_root_dir().join(raw)
    }

    pub fn resolve_sqlite_path(&self) -> PathBuf {
        self.resolve_data_path(&self.data.sqlite_path)
    }

    pub fn resolve_worktree_root(&self) -> PathBuf {
        if self.worktrees.root_dir.is_absolute() {
            return self.worktrees.root_dir.clone();
        }
        self.resolve_root_dir().join(&self.worktrees.root_dir)
    }

    pub fn resolve_provider_dir(&self, provider_name: &str) -> PathBuf {
        self.resolve_data_path(&self.data.providers_dir)
            .join(provider_name)
    }

    pub fn bootstrap_auth(&self) -> Result<ResolvedAuth> {
        if let Some(token) = self.auth.token.as_ref() {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                return Err(anyhow!("auth.token cannot be empty"));
            }
            return Ok(ResolvedAuth {
                bearer_token: trimmed.to_string(),
                source: AuthBootstrapSource::InlineConfig,
            });
        }

        let token_path = self.resolve_auth_token_file_path();
        if let Ok(token) = std::fs::read_to_string(&token_path) {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Ok(ResolvedAuth {
                    bearer_token: trimmed.to_string(),
                    source: AuthBootstrapSource::TokenFileExisting { path: token_path },
                });
            }
        }

        if let Some(parent) = token_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create token parent directory {}",
                    parent.display()
                )
            })?;
        }

        let token = generate_bearer_token(48);
        std::fs::write(&token_path, &token)
            .with_context(|| format!("failed to write auth token file {}", token_path.display()))?;

        Ok(ResolvedAuth {
            bearer_token: token,
            source: AuthBootstrapSource::TokenFileCreated { path: token_path },
        })
    }

    fn resolve_auth_token_file_path(&self) -> PathBuf {
        match self.auth.token_file.as_ref() {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => self.resolve_root_dir().join(path),
            None => self.resolve_root_dir().join("auth").join("api-token"),
        }
    }
}

fn generate_bearer_token(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
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
            bind_address: "0.0.0.0:8080".to_string(),
            public_base_url: "http://localhost:8080".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub mode: String,
    pub token: Option<String>,
    pub token_file: Option<PathBuf>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: "static_bearer".to_string(),
            token: None,
            token_file: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DataConfig {
    pub root_dir: PathBuf,
    pub sqlite_path: PathBuf,
    pub logs_dir: PathBuf,
    pub providers_dir: PathBuf,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            root_dir: default_runtime_root_dir(),
            sqlite_path: PathBuf::from("runtime.sqlite3"),
            logs_dir: PathBuf::from("logs"),
            providers_dir: PathBuf::from("providers"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProvidersConfig {
    pub codex: ProviderConfig,
    pub claude: ProviderConfig,
    pub acp: AcpServerProviderConfig,
    pub claude_auth_mode: String,
}

impl Default for ProvidersConfig {
    fn default() -> Self {
        Self {
            codex: ProviderConfig {
                enabled: true,
                max_instances: 4,
                max_sessions_per_instance: 8,
            },
            claude: ProviderConfig {
                enabled: true,
                max_instances: 4,
                max_sessions_per_instance: 4,
            },
            acp: AcpServerProviderConfig::default(),
            claude_auth_mode: "host_machine".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderConfig {
    pub enabled: bool,
    pub max_instances: usize,
    pub max_sessions_per_instance: usize,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_instances: 1,
            max_sessions_per_instance: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AcpServerProviderConfig {
    pub enabled: bool,
    pub max_instances: usize,
    pub max_sessions_per_instance: usize,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub transport: String,
    pub request_timeout_secs: u64,
    pub wait_timeout_secs: u64,
}

impl Default for AcpServerProviderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_instances: 4,
            max_sessions_per_instance: 4,
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            transport: "stdio".to_string(),
            request_timeout_secs: 30,
            wait_timeout_secs: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EventsConfig {
    pub live_queue_capacity: usize,
    pub critical_queue_capacity: usize,
    pub team_queue_capacity: usize,
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            live_queue_capacity: 4096,
            critical_queue_capacity: 16384,
            team_queue_capacity: 8192,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProcessesConfig {
    pub enabled: bool,
    pub max_concurrent: usize,
    pub default_timeout_ms: u64,
    pub max_output_bytes_per_process: usize,
    pub allow_shell: bool,
}

impl Default for ProcessesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent: 32,
            default_timeout_ms: 600_000,
            max_output_bytes_per_process: 20_000_000,
            allow_shell: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TeamsConfig {
    pub enabled: bool,
    pub non_lead_can_add_members: bool,
    pub non_lead_can_remove_members: bool,
}

impl Default for TeamsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            non_lead_can_add_members: false,
            non_lead_can_remove_members: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorktreesConfig {
    pub enabled: bool,
    pub root_dir: PathBuf,
    pub init_script_path: PathBuf,
    pub deletion_policy_default: String,
}

impl Default for WorktreesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            root_dir: PathBuf::from("worktrees"),
            init_script_path: PathBuf::from(".agents/gg/worktree-init.sh"),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedAuth {
    pub bearer_token: String,
    pub source: AuthBootstrapSource,
}

#[derive(Debug, Clone)]
pub enum AuthBootstrapSource {
    InlineConfig,
    TokenFileExisting { path: PathBuf },
    TokenFileCreated { path: PathBuf },
}

fn default_runtime_root_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".gg-runtime");
    }
    PathBuf::from(".gg-runtime")
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
    fn defaults_include_expected_scaffolding() {
        let config = RuntimeServerConfig::default();
        assert_eq!(config.server.bind_address, "0.0.0.0:8080");
        assert_eq!(config.events.live_queue_capacity, 4096);
        assert!(config.providers.codex.enabled);
        assert!(config.providers.claude.enabled);
        assert!(!config.providers.acp.enabled);
        assert_eq!(config.providers.acp.transport, "stdio");
        assert_eq!(config.providers.acp.request_timeout_secs, 30);
        assert_eq!(config.providers.acp.wait_timeout_secs, 300);
        assert_eq!(config.providers.claude_auth_mode, "host_machine");
        assert_eq!(config.processes.max_concurrent, 32);
        assert!(config.teams.enabled);
        assert!(!config.teams.non_lead_can_add_members);
        assert!(!config.teams.non_lead_can_remove_members);
        assert!(config.worktrees.enabled);
    }

    #[test]
    fn teams_config_parses_mcp_policy() {
        let config = toml::from_str::<RuntimeServerConfig>(
            r#"
[teams]
enabled = false
non_lead_can_add_members = true
non_lead_can_remove_members = true
"#,
        )
        .expect("parse config");

        assert!(!config.teams.enabled);
        assert!(config.teams.non_lead_can_add_members);
        assert!(config.teams.non_lead_can_remove_members);
        assert!(config.processes.enabled);
        assert!(config.worktrees.enabled);
    }

    #[test]
    fn inline_token_takes_precedence_over_file() {
        let mut config = RuntimeServerConfig::default();
        config.auth.token = Some("top-secret".to_string());
        let resolved = config.bootstrap_auth().expect("bootstrap auth");
        assert_eq!(resolved.bearer_token, "top-secret");
        assert!(matches!(resolved.source, AuthBootstrapSource::InlineConfig));
    }

    #[test]
    fn token_file_bootstrap_creates_and_reuses() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.auth.token_file = Some(PathBuf::from("auth/token"));

        let first = config.bootstrap_auth().expect("first bootstrap");
        let created_path = match first.source {
            AuthBootstrapSource::TokenFileCreated { ref path } => path.clone(),
            _ => panic!("expected token file creation"),
        };
        assert!(created_path.exists());

        let second = config.bootstrap_auth().expect("second bootstrap");
        match second.source {
            AuthBootstrapSource::TokenFileExisting { path } => assert_eq!(path, created_path),
            _ => panic!("expected existing token file source"),
        }
        assert_eq!(first.bearer_token, second.bearer_token);
    }

    #[test]
    fn resolve_data_paths_are_absolute_when_root_is_relative() {
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = PathBuf::from("tmp/runtime-relative-root");

        let resolved_root = config.resolve_root_dir();
        let resolved_provider_dir = config.resolve_provider_dir("codex");
        let resolved_sqlite = config.resolve_sqlite_path();

        assert!(resolved_root.is_absolute());
        assert!(resolved_provider_dir.is_absolute());
        assert!(resolved_sqlite.is_absolute());
        assert!(resolved_provider_dir.starts_with(&resolved_root));
        assert!(resolved_sqlite.starts_with(&resolved_root));
    }

    #[test]
    fn resolve_provider_dir_supports_acp() {
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = PathBuf::from("tmp/runtime-relative-root");

        let resolved_provider_dir = config.resolve_provider_dir("acp");
        assert!(resolved_provider_dir.is_absolute());
        assert!(resolved_provider_dir.ends_with("providers/acp"));
    }

    #[test]
    fn resolve_relative_paths_from_config_file_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config_path = temp_dir.path().join("runtime-server.toml");
        std::fs::write(
            &config_path,
            r#"
[data]
root_dir = ".gg-runtime"
"#,
        )
        .expect("write config");

        let config = RuntimeServerConfig::load(Some(config_path.as_path())).expect("load config");
        assert_eq!(
            config.resolve_root_dir(),
            temp_dir.path().join(".gg-runtime")
        );
        assert_eq!(
            config.resolve_worktree_root(),
            temp_dir.path().join(".gg-runtime").join("worktrees")
        );
        assert_eq!(
            config.resolve_provider_dir("acp"),
            temp_dir
                .path()
                .join(".gg-runtime")
                .join("providers")
                .join("acp")
        );
    }
}
