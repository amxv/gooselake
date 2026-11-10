//! Reproducible release contract metadata generated from the running Rust
//! harness and tool manifest, never from a parallel hand-maintained schema.

use std::fs;
use std::path::Path;

use anyhow::{ensure, Context, Result};
use runtime_core::{harness_contract_metadata, semantic_tool_contract_manifest, ProviderKind};
use serde_json::{json, Value};

pub const CONTRACT_ARTIFACT_VERSION: &str = "gooselake-runtime-contracts-v1";
const HARNESS_ASSET_NAME: &str = "gooselake-harness-v1.md";
const MODEL_POLICY_ASSET_NAME: &str = "codex-model-policy.json";
const HARNESS_ASSET: &[u8] = include_bytes!("../../runtime-core/assets/gooselake-harness-v1.md");
const MODEL_POLICY_ASSET: &[u8] =
    include_bytes!("../../runtime-core/assets/codex-model-policy.json");

pub fn generated_contract_artifact() -> Result<Value> {
    let codex = harness_contract_metadata(ProviderKind::Codex)
        .context("failed reading Codex harness contract")?;
    let claude = harness_contract_metadata(ProviderKind::Claude)
        .context("failed reading Claude harness contract")?;
    let acp = harness_contract_metadata(ProviderKind::Acp)
        .context("failed reading ACP harness contract")?;
    ensure!(
        codex.version == claude.version && codex.version == acp.version,
        "providers disagreed on the embedded harness version"
    );
    ensure!(
        codex.content_hash == claude.content_hash && codex.content_hash == acp.content_hash,
        "providers disagreed on the embedded harness content hash"
    );
    ensure!(
        codex.tool_manifest_hash == claude.tool_manifest_hash
            && codex.tool_manifest_hash == acp.tool_manifest_hash,
        "providers disagreed on the semantic tool contract hash"
    );

    Ok(json!({
        "format_version": CONTRACT_ARTIFACT_VERSION,
        "harness_version": codex.version,
        "harness_content_hash": codex.content_hash,
        "tool_manifest_version": codex.tool_manifest_version,
        "tool_manifest_hash": codex.tool_manifest_hash,
        "provider_harness": {
            "codex": codex.injection_mode,
            "claude": claude.injection_mode,
            "acp": acp.injection_mode
        },
        "semantic_tool_manifest": semantic_tool_contract_manifest()
    }))
}

pub fn generated_contract_artifact_json() -> Result<String> {
    let mut output = serde_json::to_string_pretty(&generated_contract_artifact()?)
        .context("failed serializing runtime contract artifact")?;
    output.push('\n');
    Ok(output)
}

pub fn write_contract_artifact(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|part| !part.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("failed creating contracts directory {}", parent.display()))?;
    // Write the source texts embedded into this exact build. Copying them from
    // a separate source checkout would allow the binary and bundle to drift.
    fs::write(parent.join(HARNESS_ASSET_NAME), HARNESS_ASSET)
        .context("failed exporting embedded harness asset")?;
    fs::write(parent.join(MODEL_POLICY_ASSET_NAME), MODEL_POLICY_ASSET)
        .context("failed exporting embedded model-policy asset")?;
    fs::write(path, generated_contract_artifact_json()?)
        .with_context(|| format!("failed writing runtime contracts to {}", path.display()))
}

pub fn verify_contract_artifact(path: &Path) -> Result<()> {
    let actual: Value = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed reading {}", path.display()))?,
    )
    .with_context(|| format!("invalid contract JSON at {}", path.display()))?;
    ensure!(
        actual == generated_contract_artifact()?,
        "runtime contract artifact does not match this compiled binary: {}",
        path.display()
    );
    let parent = path
        .parent()
        .filter(|part| !part.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    for (name, embedded) in [
        (HARNESS_ASSET_NAME, HARNESS_ASSET),
        (MODEL_POLICY_ASSET_NAME, MODEL_POLICY_ASSET),
    ] {
        let asset_path = parent.join(name);
        let asset = fs::read(&asset_path)
            .with_context(|| format!("missing bundled asset {}", asset_path.display()))?;
        ensure!(
            asset == embedded,
            "bundled asset does not match this compiled binary: {}",
            asset_path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_artifact_is_deterministic_and_traces_back_to_actual_harness() {
        let first = generated_contract_artifact_json().expect("generated contracts");
        assert_eq!(first, generated_contract_artifact_json().unwrap());
        let value: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(value["format_version"], CONTRACT_ARTIFACT_VERSION);
        assert_eq!(value["harness_version"], runtime_core::HARNESS_VERSION);
        assert_eq!(
            value["harness_content_hash"],
            harness_contract_metadata(ProviderKind::Codex)
                .unwrap()
                .content_hash
        );
        assert_eq!(
            value["semantic_tool_manifest"],
            semantic_tool_contract_manifest()
        );
        assert_eq!(value["provider_harness"]["codex"], "developer_instructions");
        assert_eq!(value["provider_harness"]["claude"], "system_prompt_append");
        assert_eq!(value["provider_harness"]["acp"], "scoped_mcp_only");

        let names: Vec<_> = value["semantic_tool_manifest"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["gg_team", "gg_message", "gg_process"]);
    }

    #[test]
    fn verification_rejects_modified_manifest_and_accepts_exact_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime-contracts.json");
        write_contract_artifact(&path).unwrap();
        verify_contract_artifact(&path).unwrap();

        let mut artifact = generated_contract_artifact().unwrap();
        artifact["tool_manifest_hash"] = json!("forged");
        fs::write(&path, serde_json::to_string_pretty(&artifact).unwrap()).unwrap();
        assert!(verify_contract_artifact(&path).is_err());

        fs::write(&path, "not-json").unwrap();
        assert!(verify_contract_artifact(&path).is_err());

        write_contract_artifact(&path).unwrap();
        fs::write(dir.path().join(MODEL_POLICY_ASSET_NAME), b"altered").unwrap();
        assert!(verify_contract_artifact(&path).is_err());
        write_contract_artifact(&path).unwrap();
        fs::remove_file(dir.path().join(HARNESS_ASSET_NAME)).unwrap();
        assert!(verify_contract_artifact(&path).is_err());
    }
}
