use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ProviderKind;

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
    pub permission_intent: Option<String>,
    #[serde(default)]
    pub setting_sources_intent: Vec<String>,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
    pub authoritative_cwd: String,
    pub harness_version_slot: Option<String>,
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
    pub permission_intent: Option<String>,
    #[serde(default)]
    pub setting_sources_intent: Vec<String>,
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
