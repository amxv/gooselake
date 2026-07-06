use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::config::{RuntimeSourceConfig, RuntimeSourceProvisionerKind};
use crate::runtime::events::SourceHealthState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisioningRequest {
    pub source_id: String,
    pub workspace_id: String,
}

#[derive(Debug, Clone)]
pub struct ProvisioningSourceRecord {
    pub source: RuntimeSourceConfig,
    pub lifecycle: SourceHealthState,
}

pub trait RuntimeSourceProvisioner: Send + Sync {
    fn kind(&self) -> RuntimeSourceProvisionerKind;

    fn list_sources(&self) -> Result<Vec<ProvisioningSourceRecord>>;

    fn provision_source(
        &mut self,
        request: ProvisioningRequest,
    ) -> Result<ProvisioningSourceRecord>;

    fn transition_source(
        &mut self,
        source_id: &str,
        next: SourceHealthState,
    ) -> Result<ProvisioningSourceRecord>;
}

#[derive(Debug, Clone)]
pub struct StaticSourceProvisioner {
    sources: BTreeMap<String, RuntimeSourceConfig>,
}

impl StaticSourceProvisioner {
    pub fn new(sources: impl IntoIterator<Item = RuntimeSourceConfig>) -> Self {
        Self {
            sources: sources
                .into_iter()
                .map(|source| (source.source_id.clone(), source))
                .collect(),
        }
    }
}

impl RuntimeSourceProvisioner for StaticSourceProvisioner {
    fn kind(&self) -> RuntimeSourceProvisionerKind {
        RuntimeSourceProvisionerKind::Static
    }

    fn list_sources(&self) -> Result<Vec<ProvisioningSourceRecord>> {
        Ok(self
            .sources
            .values()
            .cloned()
            .map(|source| ProvisioningSourceRecord {
                lifecycle: source.lifecycle,
                source,
            })
            .collect())
    }

    fn provision_source(
        &mut self,
        request: ProvisioningRequest,
    ) -> Result<ProvisioningSourceRecord> {
        let source = self
            .sources
            .get(&request.source_id)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "static source provider cannot provision unknown source {}",
                    request.source_id
                )
            })?;
        if source.workspace_id != request.workspace_id {
            return Err(anyhow!(
                "static source {} belongs to workspace {}, not {}",
                source.source_id,
                source.workspace_id,
                request.workspace_id
            ));
        }
        Ok(ProvisioningSourceRecord {
            lifecycle: source.lifecycle,
            source,
        })
    }

    fn transition_source(
        &mut self,
        source_id: &str,
        next: SourceHealthState,
    ) -> Result<ProvisioningSourceRecord> {
        let source = self
            .sources
            .get_mut(source_id)
            .ok_or_else(|| anyhow!("unknown static source {source_id}"))?;
        if !source.lifecycle.can_transition_to(next) {
            return Err(anyhow!(
                "invalid source lifecycle transition {:?} -> {:?}",
                source.lifecycle,
                next
            ));
        }
        source.lifecycle = next;
        Ok(ProvisioningSourceRecord {
            source: source.clone(),
            lifecycle: next,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct RunpodSourceProvisioner;

impl RuntimeSourceProvisioner for RunpodSourceProvisioner {
    fn kind(&self) -> RuntimeSourceProvisionerKind {
        RuntimeSourceProvisionerKind::Runpod
    }

    fn list_sources(&self) -> Result<Vec<ProvisioningSourceRecord>> {
        Ok(Vec::new())
    }

    fn provision_source(
        &mut self,
        request: ProvisioningRequest,
    ) -> Result<ProvisioningSourceRecord> {
        Err(anyhow!(
            "RunPod provisioning for source {} is not implemented in this phase",
            request.source_id
        ))
    }

    fn transition_source(
        &mut self,
        source_id: &str,
        _next: SourceHealthState,
    ) -> Result<ProvisioningSourceRecord> {
        Err(anyhow!(
            "RunPod lifecycle control for source {source_id} is not implemented in this phase"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RuntimeSourceCapabilitiesConfig, RuntimeSourceConfig};

    fn fake_source(source_id: &str) -> RuntimeSourceConfig {
        RuntimeSourceConfig {
            source_id: source_id.to_string(),
            display_name: format!("{source_id} source"),
            workspace_id: "workspace".to_string(),
            capabilities: RuntimeSourceCapabilitiesConfig {
                provider_kinds: vec!["codex".to_string()],
                models: vec!["gpt-5-codex".to_string()],
                process_capacity: Some(4),
                supports_worktrees: true,
                supports_teams: true,
                replay_window_events: Some(10_000),
                replay_window_ms: None,
            },
            ..RuntimeSourceConfig::default()
        }
    }

    #[test]
    fn static_provider_lists_configured_sources_with_capacity_metadata() {
        let provider = StaticSourceProvisioner::new([fake_source("west")]);
        let sources = provider.list_sources().expect("source list");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source.source_id, "west");
        assert_eq!(sources[0].source.capabilities.process_capacity, Some(4));
        assert_eq!(sources[0].source.capabilities.provider_kinds, ["codex"]);
    }

    #[test]
    fn mock_provider_provisions_and_drains_fake_source_without_migration() {
        let mut provider = StaticSourceProvisioner::new([fake_source("west")]);
        let record = provider
            .provision_source(ProvisioningRequest {
                source_id: "west".to_string(),
                workspace_id: "workspace".to_string(),
            })
            .expect("static provision");
        assert_eq!(record.lifecycle, SourceHealthState::Configured);

        provider
            .transition_source("west", SourceHealthState::Provisioning)
            .expect("provisioning");
        provider
            .transition_source("west", SourceHealthState::Booting)
            .expect("booting");
        provider
            .transition_source("west", SourceHealthState::Live)
            .expect("live");
        let drained = provider
            .transition_source("west", SourceHealthState::Draining)
            .expect("draining");
        assert_eq!(drained.lifecycle, SourceHealthState::Draining);
    }

    #[test]
    fn runpod_provider_is_placeholder_until_operational_details_exist() {
        let mut provider = RunpodSourceProvisioner;
        let error = provider
            .provision_source(ProvisioningRequest {
                source_id: "runpod-east".to_string(),
                workspace_id: "workspace".to_string(),
            })
            .expect_err("runpod is placeholder");
        assert!(error.to_string().contains("not implemented in this phase"));
    }
}
