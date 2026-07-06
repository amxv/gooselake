use std::collections::BTreeMap;
use std::time::Instant;

use runtime_core::ProviderKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamMcpPolicy {
    pub enabled: bool,
    pub non_lead_can_add_members: bool,
    pub non_lead_can_remove_members: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamModelPreset {
    pub name: String,
    pub provider: Option<String>,
    pub model: String,
    pub thinking_effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedModelPreset {
    pub(super) name: String,
    pub(super) provider: ProviderKind,
    pub(super) model: String,
    pub(super) thinking_effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ModelPresetCatalog {
    presets_by_name: BTreeMap<String, ResolvedModelPreset>,
    preset_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct ManageAddIdempotencyEntry {
    pub(super) inserted_at: Instant,
    pub(super) completed_success: Option<Value>,
}

impl Default for TeamMcpPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            non_lead_can_add_members: false,
            non_lead_can_remove_members: false,
        }
    }
}

impl ModelPresetCatalog {
    pub(super) fn from_presets(presets: Vec<TeamModelPreset>) -> Self {
        let mut presets_by_name = BTreeMap::new();
        let mut preset_names = Vec::new();
        for preset in presets {
            let normalized_name = normalize_model_preset_name(preset.name.as_str());
            let model = preset.model.trim().to_string();
            if normalized_name.is_empty() || model.is_empty() {
                continue;
            }
            if presets_by_name.contains_key(normalized_name.as_str()) {
                continue;
            }
            let provider = preset
                .provider
                .as_deref()
                .and_then(ProviderKind::from_str)
                .or_else(|| infer_provider_for_model(model.as_str()))
                .unwrap_or(ProviderKind::Codex);
            preset_names.push(normalized_name.clone());
            presets_by_name.insert(
                normalized_name.clone(),
                ResolvedModelPreset {
                    name: normalized_name,
                    provider,
                    model,
                    thinking_effort: preset
                        .thinking_effort
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                },
            );
        }
        Self {
            presets_by_name,
            preset_names,
        }
    }

    pub(super) fn resolve(&self, name: &str) -> Option<ResolvedModelPreset> {
        self.presets_by_name
            .get(normalize_model_preset_name(name).as_str())
            .cloned()
    }

    pub(super) fn all_names(&self) -> Vec<String> {
        self.preset_names.clone()
    }
}

pub(crate) fn default_team_model_presets() -> Vec<TeamModelPreset> {
    [
        ("planner", None, "gpt-5.5", Some("high")),
        ("designer", Some("claude"), "claude-opus-4-8", Some("high")),
        ("frontend", None, "gpt-5.5", Some("high")),
        ("fast", Some("codex"), "gpt-5.4-mini", Some("low")),
        ("codex", Some("codex"), "gpt-5.5", Some("high")),
        ("deep", Some("claude"), "claude-opus-4-8", Some("high")),
        ("opus", Some("claude"), "claude-opus-4-8", Some("high")),
        ("sonnet", Some("claude"), "claude-sonnet-5", Some("high")),
    ]
    .into_iter()
    .map(|(name, provider, model, thinking_effort)| TeamModelPreset {
        name: name.to_string(),
        provider: provider.map(str::to_string),
        model: model.to_string(),
        thinking_effort: thinking_effort.map(str::to_string),
    })
    .collect()
}

fn normalize_model_preset_name(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = false;
    for character in value.trim().to_ascii_lowercase().chars() {
        if character.is_ascii_alphanumeric() {
            normalized.push(character);
            previous_was_separator = false;
        } else if !previous_was_separator {
            normalized.push('_');
            previous_was_separator = true;
        }
    }
    normalized.trim_matches('_').to_string()
}

fn infer_provider_for_model(model: &str) -> Option<ProviderKind> {
    let model = model.trim().to_ascii_lowercase();
    if model.starts_with("claude-") {
        Some(ProviderKind::Claude)
    } else if model.starts_with("gpt-") || model.starts_with("o") {
        Some(ProviderKind::Codex)
    } else {
        None
    }
}
