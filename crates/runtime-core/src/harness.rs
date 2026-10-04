use serde::{Deserialize, Serialize};

use crate::{semantic_tool_contract_manifest, ProviderKind, RuntimeError};

pub const HARNESS_VERSION: &str = "gooselake-harness-v1";
const HARNESS_TEXT: &str = include_str!("../assets/gooselake-harness-v1.md");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessInjectionMode {
    DeveloperInstructions,
    SystemPromptAppend,
    ScopedMcpOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessContractMetadata {
    pub version: String,
    pub content_hash: String,
    pub tool_manifest_version: String,
    pub tool_manifest_hash: String,
    pub injection_mode: HarnessInjectionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessSections {
    pub shared: String,
    pub codex: String,
    pub claude: String,
}

pub fn harness_sections() -> Result<HarnessSections, RuntimeError> {
    parse_harness(HARNESS_TEXT)
}

pub fn provider_harness_text(provider: ProviderKind) -> Result<Option<String>, RuntimeError> {
    let sections = harness_sections()?;
    let text = match provider {
        ProviderKind::Codex => Some(format!("{}\n\n{}", sections.shared, sections.codex)),
        ProviderKind::Claude => Some(format!("{}\n\n{}", sections.shared, sections.claude)),
        ProviderKind::Acp => None,
    };
    Ok(text)
}

pub fn harness_contract_metadata(
    provider: ProviderKind,
) -> Result<HarnessContractMetadata, RuntimeError> {
    let tool_manifest = semantic_tool_contract_manifest();
    let tool_manifest_json = serde_json::to_string(&tool_manifest).map_err(|error| {
        RuntimeError::InvalidState(format!(
            "failed serializing semantic tool manifest: {error}"
        ))
    })?;
    Ok(HarnessContractMetadata {
        version: HARNESS_VERSION.to_string(),
        content_hash: stable_hash("harness", HARNESS_TEXT.as_bytes()),
        tool_manifest_version: "gooselake-tools-v1".to_string(),
        tool_manifest_hash: stable_hash("tools", tool_manifest_json.as_bytes()),
        injection_mode: match provider {
            ProviderKind::Codex => HarnessInjectionMode::DeveloperInstructions,
            ProviderKind::Claude => HarnessInjectionMode::SystemPromptAppend,
            ProviderKind::Acp => HarnessInjectionMode::ScopedMcpOnly,
        },
    })
}

fn parse_harness(text: &str) -> Result<HarnessSections, RuntimeError> {
    let mut current = None::<&str>;
    let mut shared = Vec::new();
    let mut codex = Vec::new();
    let mut claude = Vec::new();
    let mut seen = Vec::new();

    for line in text.lines() {
        if let Some(heading) = line.strip_prefix("# ") {
            if !matches!(heading, "Shared" | "Codex" | "Claude") {
                return Err(RuntimeError::InvalidState(format!(
                    "unknown top-level harness section '{heading}'"
                )));
            }
            if seen.contains(&heading) {
                return Err(RuntimeError::InvalidState(format!(
                    "duplicate top-level harness section '{heading}'"
                )));
            }
            seen.push(heading);
            current = Some(heading);
            continue;
        }
        let Some(section) = current else {
            if !line.trim().is_empty() {
                return Err(RuntimeError::InvalidState(
                    "harness must not contain a preamble before # Shared".to_string(),
                ));
            }
            continue;
        };
        match section {
            "Shared" => shared.push(line),
            "Codex" => codex.push(line),
            "Claude" => claude.push(line),
            _ => unreachable!(),
        }
    }

    if seen != ["Shared", "Codex", "Claude"] {
        return Err(RuntimeError::InvalidState(
            "harness top-level sections must be exactly Shared, Codex, Claude in that order"
                .to_string(),
        ));
    }
    Ok(HarnessSections {
        shared: format!("# Shared\n{}", shared.join("\n").trim_end()),
        codex: format!("# Codex\n{}", codex.join("\n").trim_end()),
        claude: format!("# Claude\n{}", claude.join("\n").trim_end()),
    })
}

fn stable_hash(namespace: &str, bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in namespace.bytes().chain([0]).chain(bytes.iter().copied()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{namespace}_v1_{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_is_strictly_partitioned_and_provider_composition_is_stable() {
        let sections = harness_sections().expect("embedded harness");
        assert!(sections.shared.contains("gg_process"));
        assert!(sections.codex.contains("collaboration.*"));
        assert!(sections.claude.contains("AskUserQuestion"));

        let codex = provider_harness_text(ProviderKind::Codex).unwrap().unwrap();
        let claude = provider_harness_text(ProviderKind::Claude)
            .unwrap()
            .unwrap();
        assert!(codex.contains("# Shared"));
        assert!(codex.contains("# Codex"));
        assert!(!codex.contains("# Claude"));
        assert!(claude.contains("# Shared"));
        assert!(claude.contains("# Claude"));
        assert!(!claude.contains("# Codex"));
        assert!(provider_harness_text(ProviderKind::Acp).unwrap().is_none());
    }

    #[test]
    fn contract_hashes_are_stable_and_provider_injection_is_explicit() {
        let first = harness_contract_metadata(ProviderKind::Codex).unwrap();
        let second = harness_contract_metadata(ProviderKind::Codex).unwrap();
        assert_eq!(first, second);
        assert!(first.content_hash.starts_with("harness_v1_"));
        assert!(first.tool_manifest_hash.starts_with("tools_v1_"));
        assert_eq!(
            first.injection_mode,
            HarnessInjectionMode::DeveloperInstructions
        );
        assert_eq!(
            harness_contract_metadata(ProviderKind::Claude)
                .unwrap()
                .injection_mode,
            HarnessInjectionMode::SystemPromptAppend
        );
        assert_eq!(
            harness_contract_metadata(ProviderKind::Acp)
                .unwrap()
                .injection_mode,
            HarnessInjectionMode::ScopedMcpOnly
        );
    }
}
