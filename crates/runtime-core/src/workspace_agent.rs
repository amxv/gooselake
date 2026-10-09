use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ProviderKind, ProviderPermissionIntent, ProviderSessionLaunchPolicy,
    ProviderSessionPreferences, ProviderSettingSourcesIntent, ProviderWorkspaceRebindEvidence,
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
    #[serde(default)]
    pub worktree: Option<WorkspaceAgentInitialRoute>,
    pub harness_version_slot: Option<String>,
    pub title: Option<String>,
    pub metadata: Option<Value>,
}

/// The initial provider cwd is selected from trusted workspace/repository
/// authority, never inferred from an arbitrary named checkout path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceAgentInitialRoute {
    Root,
    Existing {
        worktree_id: String,
    },
    New {
        worktree_name: String,
        branch_prefix: Option<String>,
        base_ref: Option<String>,
        deletion_policy: Option<String>,
        run_init_script: Option<bool>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceAgentArchiveRequest {
    pub reason: Option<String>,
}

/// An operator-selected route. `None` means the canonical workspace root.
/// Managed worktree IDs, not arbitrary filesystem paths, select linked checkouts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceAgentRebindRequest {
    pub worktree_id: Option<String>,
    pub expected_revision: u64,
    /// Defaults to preserving the previous managed checkout. If true, a
    /// separate recoverable cleanup follows the verified provider rebind.
    #[serde(default)]
    pub cleanup_previous_worktree: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWorktreeInventory {
    pub workspace_id: String,
    pub canonical_repository_root: String,
    pub git_common_dir: String,
    pub repository_fingerprint: String,
    pub worktrees: Vec<WorkspaceWorktreeInventoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWorktreeInventoryEntry {
    pub worktree_id: String,
    /// Durable monotone record revision (including recovery/upsert writes).
    pub revision: u64,
    pub normalized_name: String,
    pub branch_name: String,
    pub worktree_path: String,
    pub retention_policy: String,
    pub lifecycle_state: String,
    pub routing_state: String,
    pub attached_agent_ids: Vec<String>,
    pub eligible_for_assignment: bool,
    pub eligibility_blockers: Vec<String>,
}

/// Persisted before provider dispatch. An unresolved entry is a durable
/// do-not-retry fence for any further turns or workspace route changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceAgentRebindOperation {
    pub operation_id: String,
    pub workspace_id: String,
    pub agent_id: String,
    pub idempotency_key: Option<String>,
    pub expected_revision: u64,
    pub previous_worktree_id: Option<String>,
    pub destination_worktree_id: Option<String>,
    pub previous_cwd: String,
    pub destination_cwd: String,
    #[serde(default)]
    pub cleanup_previous_worktree: bool,
    /// `pending`, `deleted`, `retained_by_policy`, `skipped_live_claims`, or
    /// `preserved`; cleanup never rolls back an already verified cwd change.
    #[serde(default)]
    pub previous_cleanup_status: Option<String>,
    #[serde(default)]
    pub previous_cleanup_diagnostic: Option<String>,
    pub phase: String,
    pub provider_evidence: Option<ProviderWorkspaceRebindEvidence>,
    pub error_code: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceAgentRebindResponse {
    pub operation: WorkspaceAgentRebindOperation,
    pub agent: Option<WorkspaceAgentRecord>,
    /// Internal response marker: retries replay the recorded outcome without
    /// repeating optional native cleanup. Not part of the operator API.
    #[serde(skip)]
    pub newly_completed: bool,
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
