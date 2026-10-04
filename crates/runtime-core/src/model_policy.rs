use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::Deserialize;

use crate::{ProviderKind, ProviderModel, ProviderThinkingEffort};

pub const CLAUDE_OPUS_MODEL: &str = "claude-opus-5-5";
pub const CLAUDE_FABLE_MODEL: &str = "claude-fable-5-1";
pub const CLAUDE_SONNET_MODEL: &str = "claude-sonnet-5-5";

const CLAUDE_ADVANCED_EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max"];

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexModelPolicy {
    pub models: Vec<ModelPolicyEntry>,
    pub defaults: CodexModelDefaults,
    pub thinking_efforts: Vec<String>,
    pub default_presets: Vec<CodexPresetEntry>,
    pub retired_model_migrations: BTreeMap<String, String>,
    pub retired_family_migrations: Vec<FamilyMigration>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelPolicyEntry {
    pub id: String,
    pub label: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexModelDefaults {
    pub runtime: String,
    pub team_lead: String,
    pub git_automation: String,
    pub persisted_fallback: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexPresetEntry {
    pub name: String,
    pub model: String,
    pub thinking_effort: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FamilyMigration {
    pub prefix: String,
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderModelPreset {
    pub name: String,
    pub provider: ProviderKind,
    pub model: String,
    pub thinking_effort: ProviderThinkingEffort,
}

pub fn codex_model_policy() -> &'static CodexModelPolicy {
    static POLICY: OnceLock<CodexModelPolicy> = OnceLock::new();
    POLICY.get_or_init(|| {
        serde_json::from_str(include_str!("../assets/codex-model-policy.json"))
            .expect("embedded Codex model policy must be valid")
    })
}

pub fn codex_model_catalog() -> Vec<ProviderModel> {
    let policy = codex_model_policy();
    policy
        .models
        .iter()
        .map(|model| ProviderModel {
            id: model.id.clone(),
            display_name: model.label.clone(),
            reasoning_levels: policy.thinking_efforts.clone(),
        })
        .collect()
}

pub fn claude_model_catalog() -> Vec<ProviderModel> {
    [
        (CLAUDE_OPUS_MODEL, "Claude Opus 5.5"),
        (CLAUDE_FABLE_MODEL, "Claude Fable 5.1"),
        (CLAUDE_SONNET_MODEL, "Claude Sonnet 5.5"),
    ]
    .into_iter()
    .map(|(id, label)| ProviderModel {
        id: id.to_string(),
        display_name: label.to_string(),
        reasoning_levels: CLAUDE_ADVANCED_EFFORTS
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    })
    .collect()
}

pub fn default_model_presets() -> Vec<ProviderModelPreset> {
    let mut presets = codex_model_policy()
        .default_presets
        .iter()
        .map(|preset| ProviderModelPreset {
            name: preset.name.clone(),
            provider: ProviderKind::Codex,
            model: preset.model.clone(),
            thinking_effort: ProviderThinkingEffort::parse(&preset.thinking_effort)
                .expect("embedded preset thinking effort must be valid"),
        })
        .collect::<Vec<_>>();
    presets.extend([
        ProviderModelPreset {
            name: "fable".to_string(),
            provider: ProviderKind::Claude,
            model: CLAUDE_FABLE_MODEL.to_string(),
            thinking_effort: ProviderThinkingEffort::High,
        },
        ProviderModelPreset {
            name: "opus".to_string(),
            provider: ProviderKind::Claude,
            model: CLAUDE_OPUS_MODEL.to_string(),
            thinking_effort: ProviderThinkingEffort::Medium,
        },
        ProviderModelPreset {
            name: "sonnet".to_string(),
            provider: ProviderKind::Claude,
            model: CLAUDE_SONNET_MODEL.to_string(),
            thinking_effort: ProviderThinkingEffort::High,
        },
    ]);
    presets
}

pub fn migrate_retired_model(provider: ProviderKind, model: &str) -> Option<String> {
    let normalized = model.trim().to_ascii_lowercase();
    match provider {
        ProviderKind::Codex => migrate_retired_codex_model(&normalized),
        ProviderKind::Claude => migrate_retired_claude_model(&normalized),
        ProviderKind::Acp => None,
    }
}

fn migrate_retired_codex_model(model: &str) -> Option<String> {
    let policy = codex_model_policy();
    if policy.models.iter().any(|entry| entry.id == model) {
        return None;
    }
    if let Some(destination) = policy.retired_model_migrations.get(model) {
        return Some(destination.clone());
    }
    for migration in &policy.retired_family_migrations {
        if model.starts_with(&migration.prefix) {
            return Some(migration.destination.clone());
        }
    }
    if model.starts_with("gpt-") {
        return Some(policy.defaults.persisted_fallback.clone());
    }
    None
}

fn migrate_retired_claude_model(model: &str) -> Option<String> {
    if model == CLAUDE_OPUS_MODEL
        || model.starts_with("claude-opus-5-5-")
        || model == CLAUDE_FABLE_MODEL
        || model.starts_with("claude-fable-5-1-")
        || model == CLAUDE_SONNET_MODEL
        || model.starts_with("claude-sonnet-5-5-")
    {
        return None;
    }
    if model == "claude-opus-4"
        || model.starts_with("claude-opus-4-")
        || model == "claude-opus-5"
        || model.starts_with("claude-opus-5-")
    {
        return Some(CLAUDE_OPUS_MODEL.to_string());
    }
    if model == "claude-fable-5" || model.starts_with("claude-fable-5-") {
        return Some(CLAUDE_FABLE_MODEL.to_string());
    }
    if model == "claude-sonnet-5" || model.starts_with("claude-sonnet-5-") {
        return Some(CLAUDE_SONNET_MODEL.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_policy_defaults_and_presets_reference_current_models() {
        let policy = codex_model_policy();
        let current = policy
            .models
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>();
        for model in [
            policy.defaults.runtime.as_str(),
            policy.defaults.team_lead.as_str(),
            policy.defaults.git_automation.as_str(),
            policy.defaults.persisted_fallback.as_str(),
        ] {
            assert!(current.contains(&model), "default {model} must be current");
        }
        for preset in &policy.default_presets {
            assert!(current.contains(&preset.model.as_str()));
        }
        assert_eq!(current, ["gpt-6-astra", "gpt-6.1-sol", "gpt-6-luna"]);
        let claude = claude_model_catalog()
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>();
        assert_eq!(
            claude,
            [CLAUDE_OPUS_MODEL, CLAUDE_FABLE_MODEL, CLAUDE_SONNET_MODEL]
        );
        assert!(default_model_presets()
            .iter()
            .all(|preset| match preset.provider {
                ProviderKind::Codex => current.contains(&preset.model.as_str()),
                ProviderKind::Claude => claude.iter().any(|model| model == &preset.model),
                ProviderKind::Acp => false,
            }));
    }

    #[test]
    fn stale_model_migrations_never_cross_provider_families() {
        assert_eq!(
            migrate_retired_model(ProviderKind::Codex, "gpt-5.6-sol").as_deref(),
            Some("gpt-6.1-sol")
        );
        assert_eq!(
            migrate_retired_model(ProviderKind::Codex, "gpt-5.6-luna").as_deref(),
            Some("gpt-6-luna")
        );
        assert_eq!(
            migrate_retired_model(ProviderKind::Claude, "claude-opus-5").as_deref(),
            Some(CLAUDE_OPUS_MODEL)
        );
        assert_eq!(
            migrate_retired_model(ProviderKind::Claude, "claude-sonnet-5").as_deref(),
            Some(CLAUDE_SONNET_MODEL)
        );
        assert_eq!(
            migrate_retired_model(ProviderKind::Claude, "claude-opus-4").as_deref(),
            Some(CLAUDE_OPUS_MODEL)
        );
        assert_eq!(
            migrate_retired_model(ProviderKind::Claude, "claude-opus-4-9-20260901").as_deref(),
            Some(CLAUDE_OPUS_MODEL)
        );
        for current in [
            "claude-opus-5-5-20260928",
            "claude-fable-5-1-20260928",
            "claude-sonnet-5-5-20260928",
        ] {
            assert!(migrate_retired_model(ProviderKind::Claude, current).is_none());
        }
        assert!(migrate_retired_model(ProviderKind::Codex, "claude-opus-5").is_none());
        assert!(migrate_retired_model(ProviderKind::Claude, "gpt-5.6-sol").is_none());
        assert!(migrate_retired_model(ProviderKind::Acp, "gpt-5.6-sol").is_none());
    }
}
