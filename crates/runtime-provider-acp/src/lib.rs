use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    ProviderAuthStatus, ProviderKind, ProviderMetadata, ProviderModel, RuntimeError,
    RuntimeProvider,
};
use serde::{Deserialize, Serialize};

const DEFAULT_PROVIDER_DIR: &str = ".gg-runtime/providers/acp";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 300;

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
        }
    }
}

#[derive(Debug)]
struct AcpProviderInner {
    config: AcpProviderConfig,
}

#[derive(Clone, Debug)]
pub struct AcpProvider {
    inner: Arc<AcpProviderInner>,
}

impl AcpProvider {
    pub fn new(config: AcpProviderConfig) -> Self {
        Self {
            inner: Arc::new(AcpProviderInner {
                config: AcpProviderConfig {
                    provider_dir: absolutize_path(config.provider_dir.as_path()),
                    ..config
                },
            }),
        }
    }

    pub fn provider_dir(&self) -> &Path {
        self.inner.config.provider_dir.as_path()
    }

    pub fn config(&self) -> &AcpProviderConfig {
        &self.inner.config
    }

    fn runtime_subdirs(&self) -> [PathBuf; 3] {
        [
            self.provider_dir().to_path_buf(),
            self.provider_dir().join("instances"),
            self.provider_dir().join("sessions"),
        ]
    }

    fn validate_config(&self) -> Result<(), RuntimeError> {
        if self.inner.config.transport.trim() != "stdio" {
            return Err(RuntimeError::Configuration(format!(
                "acp transport '{}' is unsupported; expected stdio",
                self.inner.config.transport
            )));
        }
        if self.inner.config.max_instances == 0 {
            return Err(RuntimeError::Configuration(
                "acp max_instances must be greater than zero".to_string(),
            ));
        }
        if self.inner.config.max_sessions_per_instance == 0 {
            return Err(RuntimeError::Configuration(
                "acp max_sessions_per_instance must be greater than zero".to_string(),
            ));
        }
        if self.inner.config.request_timeout_secs == 0 {
            return Err(RuntimeError::Configuration(
                "acp request_timeout_secs must be greater than zero".to_string(),
            ));
        }
        if self.inner.config.wait_timeout_secs == 0 {
            return Err(RuntimeError::Configuration(
                "acp wait_timeout_secs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl RuntimeProvider for AcpProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Acp
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Acp,
            display_name: "ACP".to_string(),
            enabled: self.inner.config.enabled,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap("acp provider disabled".to_string()));
        }
        self.validate_config()?;

        for dir in self.runtime_subdirs() {
            tokio::fs::create_dir_all(&dir).await.map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create acp provider directory {}: {error}",
                    dir.display()
                ))
            })?;
        }

        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(Vec::new())
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        Ok(ProviderAuthStatus {
            authenticated: false,
            mode: Some("not_configured".to_string()),
            detail: Some(match self.inner.config.command.as_deref() {
                Some(command) if !command.trim().is_empty() => format!(
                    "ACP auth is not implemented yet; configured command '{}' will be used in a later phase",
                    command.trim()
                ),
                _ => "ACP command is not configured yet; auth and protocol lifecycle are not implemented in this phase"
                    .to_string(),
            }),
        })
    }
}

fn absolutize_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

#[cfg(test)]
mod tests {
    use runtime_core::RuntimeProvider;

    use super::{AcpProvider, AcpProviderConfig};

    #[tokio::test]
    async fn metadata_reports_acp_provider_identity() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            ..AcpProviderConfig::default()
        });

        let metadata = provider.metadata();
        assert_eq!(metadata.kind.as_str(), "acp");
        assert_eq!(metadata.display_name, "ACP");
        assert!(metadata.enabled);
    }

    #[tokio::test]
    async fn healthcheck_creates_provider_runtime_directories() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider_dir = temp_dir.path().join("acp-runtime");
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            provider_dir: provider_dir.clone(),
        });

        provider.healthcheck().await.expect("healthcheck");

        assert!(provider_dir.is_dir());
        assert!(provider_dir.join("instances").is_dir());
        assert!(provider_dir.join("sessions").is_dir());
    }

    #[tokio::test]
    async fn healthcheck_rejects_disabled_provider() {
        let provider = AcpProvider::new(AcpProviderConfig::default());
        let error = provider.healthcheck().await.expect_err("disabled");
        assert_eq!(error.to_string(), "bootstrap error: acp provider disabled");
    }

    #[tokio::test]
    async fn list_models_is_empty_for_session_scoped_acp_selection() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            ..AcpProviderConfig::default()
        });

        let models = provider.list_models().await.expect("models");
        assert!(models.is_empty());
    }

    #[tokio::test]
    async fn auth_status_is_clear_about_unimplemented_phase_one_state() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            ..AcpProviderConfig::default()
        });

        let status = provider.auth_status().await.expect("auth status");
        assert!(!status.authenticated);
        assert_eq!(status.mode.as_deref(), Some("not_configured"));
        assert!(status
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("not configured yet")));
    }

    #[test]
    fn default_config_matches_phase_two_server_contract() {
        let config = AcpProviderConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_instances, 4);
        assert_eq!(config.max_sessions_per_instance, 4);
        assert!(config.command.is_none());
        assert_eq!(config.transport, "stdio");
        assert_eq!(config.request_timeout_secs, 30);
        assert_eq!(config.wait_timeout_secs, 300);
    }

    #[tokio::test]
    async fn healthcheck_rejects_non_stdio_transport() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            transport: "http".to_string(),
            ..AcpProviderConfig::default()
        });

        let error = provider
            .healthcheck()
            .await
            .expect_err("unsupported transport");
        assert_eq!(
            error.to_string(),
            "configuration error: acp transport 'http' is unsupported; expected stdio"
        );
    }

    #[tokio::test]
    async fn lifecycle_methods_remain_unsupported_in_phase_one() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            ..AcpProviderConfig::default()
        });

        let error = provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_acp_test".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect_err("unsupported");
        assert_eq!(
            error.to_string(),
            "unsupported: provider create_session is not supported"
        );
    }
}
