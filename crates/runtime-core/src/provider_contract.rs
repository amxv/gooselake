use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};

use crate::{ProviderKind, ProviderModel, RuntimeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCapabilitySupport {
    Supported,
    AgentManaged,
    #[default]
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProviderDiscoveryMode {
    Catalog,
    AgentManaged,
    #[default]
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProviderModelDiscoveryStartupMode {
    #[default]
    Cold,
    StartRuntime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderThinkingEffort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ProviderThinkingEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }

    pub fn parse(value: &str) -> Result<Self, RuntimeError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" => Ok(Self::Xhigh),
            "max" => Ok(Self::Max),
            other => Err(RuntimeError::InvalidState(format!(
                "unsupported thinking effort '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderSettingSource {
    User,
    Project,
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderPermissionIntent {
    ProviderDefault,
    InheritProviderConfiguration,
    Explicit { mode: String },
}

impl Default for ProviderPermissionIntent {
    fn default() -> Self {
        Self::ProviderDefault
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StructuredPermissionIntent {
    ProviderDefault,
    InheritProviderConfiguration,
    Explicit { mode: String },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PermissionIntentWire {
    Structured(StructuredPermissionIntent),
    Legacy(String),
}

impl<'de> Deserialize<'de> for ProviderPermissionIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<PermissionIntentWire>::deserialize(deserializer)?;
        match value {
            None => Ok(Self::ProviderDefault),
            Some(PermissionIntentWire::Structured(StructuredPermissionIntent::ProviderDefault)) => {
                Ok(Self::ProviderDefault)
            }
            Some(PermissionIntentWire::Structured(
                StructuredPermissionIntent::InheritProviderConfiguration,
            )) => Ok(Self::InheritProviderConfiguration),
            Some(PermissionIntentWire::Structured(StructuredPermissionIntent::Explicit {
                mode,
            })) => Self::explicit(mode).map_err(D::Error::custom),
            Some(PermissionIntentWire::Legacy(mode)) => {
                Self::explicit(mode).map_err(D::Error::custom)
            }
        }
    }
}

impl ProviderPermissionIntent {
    pub fn explicit(mode: impl Into<String>) -> Result<Self, RuntimeError> {
        let mode = mode.into().trim().to_string();
        if mode.is_empty() {
            return Err(RuntimeError::InvalidState(
                "explicit permission mode must not be empty".to_string(),
            ));
        }
        Ok(Self::Explicit { mode })
    }

    pub fn resolved_mode(&self) -> Option<String> {
        match self {
            Self::Explicit { mode } => Some(mode.clone()),
            Self::ProviderDefault | Self::InheritProviderConfiguration => None,
        }
    }
}

impl ProviderSettingSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Project => "project",
            Self::Local => "local",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "user" => Ok(Self::User),
            "project" => Ok(Self::Project),
            "local" => Ok(Self::Local),
            other => Err(format!(
                "unknown setting source '{other}'; expected user, project, or local"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderSettingSourcesIntent {
    Standard,
    Explicit { sources: Vec<ProviderSettingSource> },
    Isolated,
}

impl Default for ProviderSettingSourcesIntent {
    fn default() -> Self {
        Self::Standard
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StructuredSettingSourcesIntent {
    Standard,
    Explicit { sources: Vec<ProviderSettingSource> },
    Isolated,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SettingSourcesWire {
    Structured(StructuredSettingSourcesIntent),
    Legacy(Vec<String>),
}

impl<'de> Deserialize<'de> for ProviderSettingSourcesIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match SettingSourcesWire::deserialize(deserializer)? {
            SettingSourcesWire::Structured(StructuredSettingSourcesIntent::Standard) => {
                Ok(Self::Standard)
            }
            SettingSourcesWire::Structured(StructuredSettingSourcesIntent::Explicit {
                sources,
            }) => {
                let intent = Self::Explicit { sources };
                intent.validate().map_err(D::Error::custom)?;
                Ok(intent)
            }
            SettingSourcesWire::Structured(StructuredSettingSourcesIntent::Isolated) => {
                Ok(Self::Isolated)
            }
            SettingSourcesWire::Legacy(values) => {
                if values.is_empty() {
                    return Ok(Self::Isolated);
                }
                let mut sources = Vec::with_capacity(values.len());
                for value in values {
                    sources.push(ProviderSettingSource::parse(&value).map_err(D::Error::custom)?);
                }
                Ok(Self::Explicit { sources }.normalized())
            }
        }
    }
}

impl ProviderSettingSourcesIntent {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        let Self::Explicit { sources } = self else {
            return Ok(());
        };
        if sources.is_empty() {
            return Err(RuntimeError::InvalidState(
                "explicit setting sources must not be empty; use isolated instead".to_string(),
            ));
        }
        let canonical = [
            ProviderSettingSource::User,
            ProviderSettingSource::Project,
            ProviderSettingSource::Local,
        ];
        let mut last_index = None;
        for source in sources {
            let index = canonical
                .iter()
                .position(|candidate| candidate == source)
                .expect("all setting sources are canonical enum variants");
            if last_index.is_some_and(|previous| index <= previous) {
                return Err(RuntimeError::InvalidState(
                    "explicit setting sources must be unique and in canonical user, project, local order"
                        .to_string(),
                ));
            }
            last_index = Some(index);
        }
        Ok(())
    }

    pub fn normalized(self) -> Self {
        let Self::Explicit { sources } = self else {
            return self;
        };
        let mut normalized = Vec::with_capacity(3);
        for source in [
            ProviderSettingSource::User,
            ProviderSettingSource::Project,
            ProviderSettingSource::Local,
        ] {
            if sources.contains(&source) {
                normalized.push(source);
            }
        }
        Self::Explicit {
            sources: normalized,
        }
    }

    pub fn resolved_sources(
        &self,
        cwd: Option<&str>,
    ) -> Result<Vec<ProviderSettingSource>, RuntimeError> {
        let has_cwd = cwd.is_some_and(|value| !value.trim().is_empty());
        let sources = match self {
            Self::Standard if has_cwd => vec![
                ProviderSettingSource::User,
                ProviderSettingSource::Project,
                ProviderSettingSource::Local,
            ],
            Self::Standard => vec![ProviderSettingSource::User],
            Self::Explicit { sources } => {
                self.validate()?;
                let sources = sources.clone();
                if !has_cwd
                    && sources.iter().any(|source| {
                        matches!(
                            source,
                            ProviderSettingSource::Project | ProviderSettingSource::Local
                        )
                    })
                {
                    return Err(RuntimeError::InvalidState(
                        "project/local setting sources require a working directory".to_string(),
                    ));
                }
                sources
            }
            Self::Isolated => Vec::new(),
        };
        Ok(sources)
    }

    pub fn resolved_wire_values(&self, cwd: Option<&str>) -> Result<Vec<String>, RuntimeError> {
        self.resolved_sources(cwd).map(|sources| {
            sources
                .into_iter()
                .map(|source| source.as_str().to_string())
                .collect()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderSessionPreferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_effort: Option<ProviderThinkingEffort>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPermissionMutationRequest {
    pub runtime_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    pub permission_intent: ProviderPermissionIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPermissionMutationResult {
    pub revision: u64,
    pub permission_intent: ProviderPermissionIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSessionPreferencesMutationRequest {
    pub runtime_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    pub current_preferences: ProviderSessionPreferences,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSessionPreferencesMutationResult {
    pub revision: u64,
    pub current_preferences: ProviderSessionPreferences,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderSessionLaunchPolicy {
    #[serde(default)]
    pub permission_intent: ProviderPermissionIntent,
    #[serde(default)]
    pub setting_sources_intent: ProviderSettingSourcesIntent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disallowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness_version_slot: Option<String>,
}

impl ProviderSessionLaunchPolicy {
    pub fn resolved_setting_sources(&self, cwd: Option<&str>) -> Result<Vec<String>, RuntimeError> {
        self.setting_sources_intent.resolved_wire_values(cwd)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderModelCapabilities {
    #[serde(default)]
    pub supports_reasoning: bool,
    #[serde(default)]
    pub supports_thinking_effort: bool,
    #[serde(default)]
    pub supports_tool_calling: bool,
    #[serde(default)]
    pub supports_vision: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_thinking_efforts: Vec<ProviderThinkingEffort>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderModelDescriptor {
    pub provider: ProviderKind,
    pub model_key: String,
    pub display_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    pub capabilities: ProviderModelCapabilities,
}

impl ProviderModelDescriptor {
    pub fn from_legacy(provider: ProviderKind, model: ProviderModel) -> Self {
        let supported_thinking_efforts = model
            .reasoning_levels
            .iter()
            .filter_map(|value| ProviderThinkingEffort::parse(value).ok())
            .collect::<Vec<_>>();
        Self {
            provider,
            model_key: model.id,
            display_label: model.display_name,
            provider_name: None,
            family: None,
            capabilities: ProviderModelCapabilities {
                supports_reasoning: !supported_thinking_efforts.is_empty(),
                supports_thinking_effort: !supported_thinking_efforts.is_empty(),
                supports_tool_calling: false,
                supports_vision: false,
                supported_thinking_efforts,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSkillDescriptor {
    pub provider: ProviderKind,
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderModelDiscoveryRequest {
    pub cwd: Option<String>,
    #[serde(default)]
    pub setting_sources_intent: ProviderSettingSourcesIntent,
    #[serde(default)]
    pub force_refresh: bool,
    #[serde(default)]
    pub startup_mode: ProviderModelDiscoveryStartupMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderSkillDiscoveryRequest {
    pub cwd: Option<String>,
    #[serde(default)]
    pub setting_sources_intent: ProviderSettingSourcesIntent,
    #[serde(default)]
    pub force_refresh: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderModelDiscoveryResponse {
    pub provider: ProviderKind,
    pub mode: ProviderDiscoveryMode,
    pub models: Vec<ProviderModelDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSkillDiscoveryResponse {
    pub provider: ProviderKind,
    pub mode: ProviderDiscoveryMode,
    pub skills: Vec<ProviderSkillDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderContextLimitObservation {
    pub model_context_window: u64,
    pub last_total_tokens: u64,
    pub remaining_percentage: u8,
}

/// Provider-reported context usage for the exact active agent attachment.
/// No percentage is inferred from display history or retained turn messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionContextLimitSnapshot {
    pub agent_id: String,
    pub provider: ProviderKind,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub agent_revision: u64,
    pub observed_at_ms: i64,
    pub observed_turn_id: Option<String>,
    #[serde(flatten)]
    pub observation: ProviderContextLimitObservation,
}

impl SessionContextLimitSnapshot {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.agent_id.trim().is_empty()
            || self.provider_session_ref.trim().is_empty()
            || self.observation.model_context_window == 0
            || self.observation.remaining_percentage > 100
            || self.observed_at_ms < 0
        {
            return Err(RuntimeError::InvalidState(
                "invalid provider context-limit observation or attachment identity".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderWorkspaceRebindRequest {
    pub runtime_session_id: String,
    pub cwd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderWorkspaceRebindEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_provider_session_ref: Option<String>,
    pub runtime_session_id: String,
    pub cwd: String,
    pub provider_session_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCompactSessionRequest {
    pub runtime_session_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCompactSessionOutcome {
    Accepted,
    NotPerformed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderHardForkEditRerunRequest {
    pub runtime_session_id: String,
    pub target_turn_id: String,
    pub edited_input: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderCapabilities {
    pub model_discovery: ProviderDiscoveryMode,
    pub skill_discovery: ProviderDiscoveryMode,
    pub session_resume: ProviderCapabilitySupport,
    pub streaming: ProviderCapabilitySupport,
    pub approvals: ProviderCapabilitySupport,
    #[serde(default)]
    pub permission_mutation: ProviderCapabilitySupport,
    #[serde(default)]
    pub session_preferences: ProviderCapabilitySupport,
    pub interrupt: ProviderCapabilitySupport,
    pub tools: ProviderCapabilitySupport,
    pub images: ProviderCapabilitySupport,
    pub structured_output: ProviderCapabilitySupport,
    pub setting_sources: ProviderCapabilitySupport,
    pub context_limit_observation: ProviderCapabilitySupport,
    pub workspace_rebind: ProviderCapabilitySupport,
    pub manual_compact: ProviderCapabilitySupport,
    pub hard_fork_edit_rerun: ProviderCapabilitySupport,
}

pub fn semantic_tool_contract_manifest() -> Value {
    json!({
        "version": "gooselake-tools-v1",
        "activation": "defined_not_live",
        "identity": {
            "caller": "trusted_runtime",
            "workspace": "trusted_runtime",
            "invocation": "trusted_runtime"
        },
        "validation": {
            "mode_discriminator": "mode",
            "additional_properties": false
        },
        "tools": [
            {
                "name": "gg_team",
                "semantic_modes": ["status", "add", "remove", "assign_worktree"],
                "scope": "workspace",
                "lead_authority": "operator_only"
            },
            {
                "name": "gg_message",
                "semantic_modes": ["direct", "broadcast"],
                "direct_fields": ["mode", "agent_id", "message", "image_paths"],
                "direct_required": ["mode", "agent_id", "message"],
                "broadcast_fields": ["mode", "message", "image_paths"],
                "broadcast_required": ["mode", "message"],
                "image_paths_ordered": true,
                "direct_scope": "runtime_trust_domain",
                "broadcast_scope": "caller_workspace"
            },
            {
                "name": "gg_process",
                "semantic_modes": ["run", "list", "status", "cancel"],
                "run_required": ["mode", "command"],
                "process_identity": "stable_opaque_id",
                "completion_delivery": "runtime_injected",
                "polling": "prohibited",
                "scope": "workspace"
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setting_sources_resolve_standard_explicit_and_isolated() {
        assert_eq!(
            ProviderSettingSourcesIntent::Standard
                .resolved_wire_values(Some("/repo"))
                .unwrap(),
            ["user", "project", "local"]
        );
        assert_eq!(
            ProviderSettingSourcesIntent::Standard
                .resolved_wire_values(None)
                .unwrap(),
            ["user"]
        );
        let explicit = ProviderSettingSourcesIntent::Explicit {
            sources: vec![ProviderSettingSource::User, ProviderSettingSource::Local],
        };
        assert_eq!(
            explicit.resolved_wire_values(Some("/repo")).unwrap(),
            ["user", "local"]
        );
        assert!(explicit.resolved_wire_values(None).is_err());
        assert!(ProviderSettingSourcesIntent::Isolated
            .resolved_wire_values(None)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn structured_setting_sources_reject_empty_duplicates_and_noncanonical_order() {
        for value in [
            json!({"kind":"explicit","sources":[]}),
            json!({"kind":"explicit","sources":["user","user"]}),
            json!({"kind":"explicit","sources":["local","user"]}),
        ] {
            assert!(serde_json::from_value::<ProviderSettingSourcesIntent>(value).is_err());
        }
        let canonical: ProviderSettingSourcesIntent = serde_json::from_value(json!({
            "kind":"explicit",
            "sources":["user","project","local"]
        }))
        .expect("canonical explicit sources");
        assert_eq!(
            canonical.resolved_wire_values(Some("/repo")).unwrap(),
            ["user", "project", "local"]
        );
    }

    #[test]
    fn legacy_setting_source_arrays_remain_readable() {
        let intent: ProviderSettingSourcesIntent =
            serde_json::from_value(json!(["local", "user", "local"]))
                .expect("legacy setting sources");
        assert_eq!(
            intent,
            ProviderSettingSourcesIntent::Explicit {
                sources: vec![ProviderSettingSource::User, ProviderSettingSource::Local]
            }
        );
        let empty: ProviderSettingSourcesIntent =
            serde_json::from_value(json!([])).expect("legacy empty setting sources");
        assert_eq!(empty, ProviderSettingSourcesIntent::Isolated);
    }

    #[test]
    fn permission_intent_reads_legacy_strings_and_serializes_typed_policy() {
        let intent: ProviderPermissionIntent =
            serde_json::from_value(json!("workspace_write")).expect("legacy permission");
        assert_eq!(
            intent,
            ProviderPermissionIntent::Explicit {
                mode: "workspace_write".to_string()
            }
        );
        assert_eq!(intent.resolved_mode().as_deref(), Some("workspace_write"));
        assert_eq!(
            serde_json::to_value(intent).unwrap(),
            json!({"kind":"explicit","mode":"workspace_write"})
        );
        let inherited: ProviderPermissionIntent = serde_json::from_value(json!({
            "kind":"inherit_provider_configuration"
        }))
        .expect("typed permission");
        assert_eq!(
            inherited,
            ProviderPermissionIntent::InheritProviderConfiguration
        );
        assert!(inherited.resolved_mode().is_none());
    }

    #[test]
    fn semantic_manifest_is_deterministic_and_matches_the_future_three_tool_contract() {
        let first = semantic_tool_contract_manifest();
        let second = semantic_tool_contract_manifest();
        assert_eq!(first, second);
        assert_eq!(first["activation"], "defined_not_live");
        assert_eq!(first["identity"]["caller"], "trusted_runtime");
        assert_eq!(first["validation"]["additional_properties"], false);

        let tools = first["tools"].as_array().expect("tool manifest array");
        assert_eq!(tools.len(), 3);
        assert_eq!(tools[0]["name"], "gg_team");
        assert_eq!(
            tools[0]["semantic_modes"],
            json!(["status", "add", "remove", "assign_worktree"])
        );
        assert_eq!(tools[0]["lead_authority"], "operator_only");
        assert_eq!(tools[1]["name"], "gg_message");
        assert_eq!(
            tools[1]["direct_fields"],
            json!(["mode", "agent_id", "message", "image_paths"])
        );
        assert_eq!(
            tools[1]["broadcast_fields"],
            json!(["mode", "message", "image_paths"])
        );
        assert_eq!(tools[1]["direct_scope"], "runtime_trust_domain");
        assert_eq!(tools[2]["name"], "gg_process");
        assert_eq!(
            tools[2]["semantic_modes"],
            json!(["run", "list", "status", "cancel"])
        );
        assert_eq!(tools[2]["polling"], "prohibited");

        let serialized = serde_json::to_string(&first).unwrap();
        for forbidden in [
            "gg_team_status",
            "gg_team_manage",
            "gg_process_run",
            "recipient_agent_id",
            "set_lead",
            "clear_lead",
            "\"kill\"",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "found legacy token {forbidden}"
            );
        }
    }

    #[test]
    fn model_discovery_startup_mode_is_explicit_and_defaults_cold() {
        let cold: ProviderModelDiscoveryRequest =
            serde_json::from_value(json!({})).expect("default discovery request");
        assert_eq!(cold.startup_mode, ProviderModelDiscoveryStartupMode::Cold);

        let start_runtime: ProviderModelDiscoveryRequest =
            serde_json::from_value(json!({"startup_mode":"start_runtime"}))
                .expect("start-runtime discovery request");
        assert_eq!(
            start_runtime.startup_mode,
            ProviderModelDiscoveryStartupMode::StartRuntime
        );
    }
}
