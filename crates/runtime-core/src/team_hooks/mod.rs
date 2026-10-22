//! Strict, read-only effective hook configuration. Hooks are not executed by
//! this resolver. External side effects require a durable, operator-trusted
//! admission and receipt, which is deliberately a separate authority boundary.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::RuntimeError;

mod effects;
pub use effects::*;

pub const TEAM_HOOK_CONFIG_RELATIVE_PATH: &str = ".agents/gg/team-hooks.json";
pub const TEAM_HOOK_CONFIG_VERSION: u32 = 1;
const fn default_config_version() -> u32 {
    TEAM_HOOK_CONFIG_VERSION
}
const MAX_CONFIG_BYTES: u64 = 64 * 1024;
const MAX_HOOKS_PER_EVENT: usize = 32;
const MAX_NAME_BYTES: usize = 128;
const MAX_COMMAND_BYTES: usize = 4096;
const MIN_TIMEOUT_MS: u64 = 100;
const DEFAULT_TIMEOUT_MS: u64 = 15_000;
const MAX_TIMEOUT_MS: u64 = 120_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamHookEventName {
    PreTeammateAdd,
    PostTeammateAdd,
    PostTeammateRemove,
    PostDirectMessageSend,
    PostBroadcastMessageSend,
    PostCompaction,
}

impl TeamHookEventName {
    pub const ALL: [Self; 6] = [
        Self::PreTeammateAdd,
        Self::PostTeammateAdd,
        Self::PostTeammateRemove,
        Self::PostDirectMessageSend,
        Self::PostBroadcastMessageSend,
        Self::PostCompaction,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreTeammateAdd => "pre_teammate_add",
            Self::PostTeammateAdd => "post_teammate_add",
            Self::PostTeammateRemove => "post_teammate_remove",
            Self::PostDirectMessageSend => "post_direct_message_send",
            Self::PostBroadcastMessageSend => "post_broadcast_message_send",
            Self::PostCompaction => "post_compaction",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamHookScope {
    User,
    Project,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamHookResolvedEntry {
    pub event_name: TeamHookEventName,
    pub name: String,
    pub command: String,
    pub timeout_ms: u64,
    pub scope: TeamHookScope,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TeamHookEntry {
    name: String,
    command: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TeamHookConfigHooks {
    pre_teammate_add: Vec<TeamHookEntry>,
    post_teammate_add: Vec<TeamHookEntry>,
    post_teammate_remove: Vec<TeamHookEntry>,
    post_direct_message_send: Vec<TeamHookEntry>,
    post_broadcast_message_send: Vec<TeamHookEntry>,
    post_compaction: Vec<TeamHookEntry>,
}

impl TeamHookConfigHooks {
    fn entries(&self, event: TeamHookEventName) -> &[TeamHookEntry] {
        match event {
            TeamHookEventName::PreTeammateAdd => &self.pre_teammate_add,
            TeamHookEventName::PostTeammateAdd => &self.post_teammate_add,
            TeamHookEventName::PostTeammateRemove => &self.post_teammate_remove,
            TeamHookEventName::PostDirectMessageSend => &self.post_direct_message_send,
            TeamHookEventName::PostBroadcastMessageSend => &self.post_broadcast_message_send,
            TeamHookEventName::PostCompaction => &self.post_compaction,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TeamHookConfigFile {
    #[serde(default = "default_config_version")]
    version: u32,
    #[serde(default)]
    hooks: TeamHookConfigHooks,
}

/// Resolve from trusted roots rather than a model-provided cwd or identity.
/// Missing files are no-ops; unreadable/invalid files always fail closed.
/// Project entries override a user entry with the same normalized event/name,
/// but retain the entry's original deterministic position.
pub fn resolve_effective_team_hooks(
    workspace_root: &Path,
    user_home: Option<&Path>,
    event: Option<TeamHookEventName>,
) -> Result<Vec<TeamHookResolvedEntry>, RuntimeError> {
    let mut order = Vec::<(TeamHookEventName, String)>::new();
    let mut merged = HashMap::<(TeamHookEventName, String), TeamHookResolvedEntry>::new();
    let paths = [
        (TeamHookScope::User, user_home.map(config_path_from_root)),
        (
            TeamHookScope::Project,
            Some(config_path_from_root(workspace_root)),
        ),
    ];
    for (scope, path) in paths {
        let Some(path) = path else { continue };
        let Some(config) = load_config(&path)? else {
            continue;
        };
        // Validate all events, including those excluded by a query filter.
        for hook_event in TeamHookEventName::ALL {
            let entries = config.hooks.entries(hook_event);
            if entries.len() > MAX_HOOKS_PER_EVENT {
                return Err(config_error(&path, "too many hooks in one event"));
            }
            let mut seen = HashSet::new();
            for entry in entries {
                let name =
                    normalize_name(&entry.name).map_err(|message| config_error(&path, &message))?;
                let command = entry.command.trim();
                if command.is_empty() || command.len() > MAX_COMMAND_BYTES || command.contains('\0')
                {
                    return Err(config_error(&path, "invalid hook command"));
                }
                let timeout_ms = entry.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
                if !(MIN_TIMEOUT_MS..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
                    return Err(config_error(&path, "hook timeout_ms out of range"));
                }
                if !seen.insert(name.clone()) {
                    return Err(config_error(&path, "duplicate normalized hook name"));
                }
                if event.is_some_and(|filter| filter != hook_event) {
                    continue;
                }
                let key = (hook_event, name.clone());
                if !merged.contains_key(&key) {
                    order.push(key.clone());
                }
                merged.insert(
                    key,
                    TeamHookResolvedEntry {
                        event_name: hook_event,
                        name,
                        command: command.to_string(),
                        timeout_ms,
                        scope,
                    },
                );
            }
        }
    }
    Ok(order
        .into_iter()
        .filter_map(|key| merged.remove(&key))
        .collect())
}

fn config_path_from_root(root: &Path) -> PathBuf {
    root.join(TEAM_HOOK_CONFIG_RELATIVE_PATH)
}

fn load_config(path: &Path) -> Result<Option<TeamHookConfigFile>, RuntimeError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(config_error(path, &format!("cannot stat config: {error}"))),
    };
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        return Err(config_error(
            path,
            "hook config must be a file up to 64 KiB",
        ));
    }
    let bytes = std::fs::read(path)
        .map_err(|error| config_error(path, &format!("cannot read config: {error}")))?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(config_error(path, "hook config exceeds 64 KiB"));
    }
    let config = serde_json::from_slice::<TeamHookConfigFile>(&bytes)
        .map_err(|error| config_error(path, &format!("invalid schema: {error}")))?;
    if config.version != TEAM_HOOK_CONFIG_VERSION {
        return Err(config_error(path, "unsupported config version"));
    }
    Ok(Some(config))
}

fn normalize_name(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_NAME_BYTES {
        return Err("invalid hook name length".to_string());
    }
    let mut name = String::new();
    let mut separated = false;
    for character in trimmed.chars() {
        if character.is_ascii_alphanumeric() {
            name.push(character.to_ascii_lowercase());
            separated = false;
        } else if character == '-' || character == '_' || character.is_ascii_whitespace() {
            if !separated {
                name.push('_');
                separated = true;
            }
        } else {
            return Err("hook names must be ASCII letters, digits, or separators".into());
        }
    }
    let name = name.trim_matches('_').to_string();
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err("invalid normalized hook name".into());
    }
    Ok(name)
}

fn config_error(path: &Path, reason: &str) -> RuntimeError {
    RuntimeError::Configuration(format!(
        "invalid team-hook configuration {}: {reason}",
        path.display()
    ))
}

#[cfg(test)]
mod tests;
