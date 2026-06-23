use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    ProviderAuthStatus, ProviderKind, ProviderMetadata, ProviderModel, RuntimeError,
    RuntimeProvider,
};
use serde::{Deserialize, Serialize};

const DEFAULT_PROVIDER_DIR: &str = ".gg-runtime/providers/acp";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpProviderConfig {
    pub enabled: bool,
    pub provider_dir: PathBuf,
}

impl Default for AcpProviderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider_dir: PathBuf::from(DEFAULT_PROVIDER_DIR),
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

    fn runtime_subdirs(&self) -> [PathBuf; 3] {
        [
            self.provider_dir().to_path_buf(),
            self.provider_dir().join("instances"),
            self.provider_dir().join("sessions"),
        ]
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
            mode: Some("not_supported".to_string()),
            detail: Some(
                "ACP auth is not implemented yet; Phase 1 exposes only skeleton status".to_string(),
            ),
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
        assert_eq!(status.mode.as_deref(), Some("not_supported"));
        assert!(status
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("not implemented yet")));
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
