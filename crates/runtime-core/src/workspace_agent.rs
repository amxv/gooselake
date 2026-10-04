use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ProviderKind, ProviderPermissionIntent, ProviderSessionLaunchPolicy,
    ProviderSessionPreferences, ProviderSettingSourcesIntent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAgentLifecycleState {
    Active,
    Archived,
}

impl WorkspaceAgentLifecycleState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "archived" => Some(Self::Archived),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceAgentRecreationPolicy {
    pub provider: ProviderKind,
    pub model: Option<String>,
    #[serde(default)]
    pub permission_intent: ProviderPermissionIntent,
    #[serde(default)]
    pub setting_sources_intent: ProviderSettingSourcesIntent,
    #[serde(default)]
    pub current_preferences: ProviderSessionPreferences,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
    pub authoritative_cwd: String,
    pub harness_version_slot: Option<String>,
}

impl WorkspaceAgentRecreationPolicy {
    pub fn launch_policy(&self) -> ProviderSessionLaunchPolicy {
        ProviderSessionLaunchPolicy {
            permission_intent: self.permission_intent.clone(),
            setting_sources_intent: self.setting_sources_intent.clone(),
            system_prompt: self.system_prompt.clone(),
            allowed_tools: self.allowed_tools.clone(),
            disallowed_tools: self.disallowed_tools.clone(),
            harness_version_slot: self.harness_version_slot.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceAgentProfile {
    pub title: Option<String>,
    pub title_provenance: String,
    pub added_by: String,
    pub creator_session_id: Option<String>,
    pub creator_compaction_subscription: String,
    pub joined_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceAgentRecord {
    pub agent_id: String,
    pub workspace_id: String,
    pub alias: String,
    pub lifecycle_state: WorkspaceAgentLifecycleState,
    pub profile: WorkspaceAgentProfile,
    pub recreation_policy: WorkspaceAgentRecreationPolicy,
    pub provider_session_ref: Option<String>,
    pub canonical_provider_session_ref: Option<String>,
    pub metadata: Value,
    pub archived_at: Option<i64>,
    pub archive_reason: Option<String>,
    pub revision: u64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceAgentCreateRequest {
    pub provider: ProviderKind,
    pub model: Option<String>,
    #[serde(default)]
    pub permission_intent: ProviderPermissionIntent,
    #[serde(default)]
    pub setting_sources_intent: ProviderSettingSourcesIntent,
    #[serde(default)]
    pub current_preferences: ProviderSessionPreferences,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
    pub cwd: Option<String>,
    pub harness_version_slot: Option<String>,
    pub title: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceAgentArchiveRequest {
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderSettingSource, ProviderThinkingEffort};
    use serde_json::json;

    #[test]
    fn recreation_policy_serializes_exact_launch_policy_and_mutable_preferences() {
        let policy = WorkspaceAgentRecreationPolicy {
            provider: ProviderKind::Claude,
            model: Some("claude-opus-5-5".to_string()),
            permission_intent: ProviderPermissionIntent::InheritProviderConfiguration,
            setting_sources_intent: ProviderSettingSourcesIntent::Explicit {
                sources: vec![ProviderSettingSource::User, ProviderSettingSource::Project],
            },
            current_preferences: ProviderSessionPreferences {
                thinking_effort: Some(ProviderThinkingEffort::High),
            },
            system_prompt: Some("system".to_string()),
            allowed_tools: vec!["read".to_string()],
            disallowed_tools: vec!["danger".to_string()],
            authoritative_cwd: "/repo".to_string(),
            harness_version_slot: Some("gooselake-harness-v1".to_string()),
        };

        assert_eq!(
            serde_json::to_value(&policy).expect("serialize recreation policy"),
            json!({
                "provider":"claude",
                "model":"claude-opus-5-5",
                "permission_intent":{"kind":"inherit_provider_configuration"},
                "setting_sources_intent":{"kind":"explicit","sources":["user","project"]},
                "current_preferences":{"thinking_effort":"high"},
                "system_prompt":"system",
                "allowed_tools":["read"],
                "disallowed_tools":["danger"],
                "authoritative_cwd":"/repo",
                "harness_version_slot":"gooselake-harness-v1"
            })
        );
        assert_eq!(
            policy.launch_policy(),
            ProviderSessionLaunchPolicy {
                permission_intent: ProviderPermissionIntent::InheritProviderConfiguration,
                setting_sources_intent: ProviderSettingSourcesIntent::Explicit {
                    sources: vec![ProviderSettingSource::User, ProviderSettingSource::Project],
                },
                system_prompt: Some("system".to_string()),
                allowed_tools: vec!["read".to_string()],
                disallowed_tools: vec!["danger".to_string()],
                harness_version_slot: Some("gooselake-harness-v1".to_string()),
            }
        );
    }

    #[test]
    fn recreation_policy_reads_legacy_permission_and_setting_source_shapes() {
        let explicit: WorkspaceAgentRecreationPolicy = serde_json::from_value(json!({
            "provider": "claude",
            "model": "claude-sonnet-5",
            "permission_intent": "require_approval",
            "setting_sources_intent": ["user", "project"],
            "system_prompt": "legacy system",
            "allowed_tools": ["read"],
            "disallowed_tools": [],
            "authoritative_cwd": "/repo",
            "harness_version_slot": null
        }))
        .expect("legacy explicit recreation policy");

        assert_eq!(
            explicit.permission_intent,
            ProviderPermissionIntent::Explicit {
                mode: "require_approval".to_string()
            }
        );
        assert_eq!(
            explicit.setting_sources_intent,
            ProviderSettingSourcesIntent::Explicit {
                sources: vec![ProviderSettingSource::User, ProviderSettingSource::Project]
            }
        );
        assert_eq!(
            explicit.current_preferences,
            ProviderSessionPreferences::default()
        );

        let defaults: WorkspaceAgentRecreationPolicy = serde_json::from_value(json!({
            "provider": "codex",
            "model": null,
            "permission_intent": null,
            "setting_sources_intent": [],
            "system_prompt": null,
            "allowed_tools": [],
            "disallowed_tools": [],
            "authoritative_cwd": "/repo",
            "harness_version_slot": null
        }))
        .expect("legacy default recreation policy");

        assert_eq!(
            defaults.permission_intent,
            ProviderPermissionIntent::ProviderDefault
        );
        assert_eq!(
            defaults.setting_sources_intent,
            ProviderSettingSourcesIntent::Isolated
        );
        assert_eq!(
            defaults.current_preferences,
            ProviderSessionPreferences::default()
        );
    }
}
