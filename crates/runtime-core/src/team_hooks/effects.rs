//! Hook approval and external-effect evidence are separate from mutable
//! project configuration. An admitted effect never re-reads a shell command.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{TeamHookEventName, TeamHookResolvedEntry};
use crate::RuntimeError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookSelection {
    pub event_name: TeamHookEventName,
    pub name: String,
    /// SHA-256 of the exact entry returned by effective-hook discovery.
    /// Required to detect edits between inspection and operator approval.
    pub fingerprint: String,
}

pub fn team_hook_fingerprint(hook: &TeamHookResolvedEntry) -> String {
    use std::fmt::Write;

    // These concrete fields are all scalar and infallibly serialize. Their
    // order is fixed by the v1 structure, not user-supplied map iteration.
    let canonical = serde_json::to_vec(hook).expect("resolved hook must serialize");
    let digest = ring::digest::digest(&ring::digest::SHA256, &canonical);
    let mut encoded = String::with_capacity("sha256:".len() + digest.as_ref().len() * 2);
    encoded.push_str("sha256:");
    for byte in digest.as_ref() {
        write!(&mut encoded, "{byte:02x}").expect("writing fingerprint cannot fail");
    }
    encoded
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookApproval {
    pub workspace_id: String,
    pub revision: u64,
    pub hooks: Vec<TeamHookResolvedEntry>,
    pub approved_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookApprovalUpdate {
    pub workspace_id: String,
    pub expected_revision: u64,
    /// Empty selection revokes all hook execution grants.
    pub hooks: Vec<TeamHookResolvedEntry>,
    pub approved_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamHookEffectPhase {
    Intended,
    Started,
    Succeeded,
    Failed,
    TimedOut,
    ManualReview,
}

impl TeamHookEffectPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Intended => "intended",
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::ManualReview => "manual_review",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "intended" => Self::Intended,
            "started" => Self::Started,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "timed_out" => Self::TimedOut,
            "manual_review" => Self::ManualReview,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookEffectAdmission {
    pub workspace_id: String,
    pub parent_event_id: String,
    pub hook: TeamHookResolvedEntry,
    /// Canonical, bounded input prepared by trusted event code, not tool JSON.
    pub input: Value,
    pub created_at_ms: i64,
}

/// Prepared solely from trusted manager state. The reserved agent ID keeps
/// the pre-add effect stable while no provider or workspace agent exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookPreAddAdmission {
    pub workspace_id: String,
    pub reserved_agent_id: String,
    pub actor_agent_id: Option<String>,
    pub provider: String,
    pub model: Option<String>,
    pub planned_cwd: String,
    pub member_title: Option<String>,
    pub onboarding_prompt: Option<String>,
    pub created_at_ms: i64,
}

/// Private output applied with agent/session/onboarding persistence. Public
/// HTTP JSON cannot fabricate either the internal caller or the model notice.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TeamHookPreAddContext {
    pub parent_event_id: String,
    pub completed_hook_names: Vec<String>,
    pub onboarding_context_append_text: Option<String>,
    pub caller_context_text: Option<String>,
}

#[async_trait]
pub trait RuntimeTeamHookExecutor: Send + Sync {
    async fn execute(
        &self,
        admission: &TeamHookEffectAdmission,
    ) -> Result<TeamHookEffectRecord, RuntimeError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookEffectRecord {
    pub workspace_id: String,
    pub parent_event_id: String,
    pub hook: TeamHookResolvedEntry,
    pub input: Value,
    pub phase: TeamHookEffectPhase,
    pub attempt_count: u32,
    pub receipt: Option<TeamHookEffectReceipt>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Separate durable receipt for application of scoped model context.
    pub application_state: String,
    pub application_error_code: Option<String>,
    pub applied_at_ms: Option<i64>,
}

/// Immutable admission key for stable newest-first operator history pages.
/// Neither phase nor output application can change the cursor ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamHookEffectPageCursor {
    pub created_at_ms: i64,
    pub parent_event_id: String,
    pub event_name: TeamHookEventName,
    pub hook_name: String,
}

/// Bounded operator-list projection. Commands, inputs and execution receipts
/// require explicit inspection of the exact effect instead of list retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookEffectSummary {
    pub workspace_id: String,
    pub parent_event_id: String,
    pub event_name: TeamHookEventName,
    pub hook_name: String,
    pub phase: TeamHookEffectPhase,
    pub attempt_count: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub application_state: String,
    pub application_error_code: Option<String>,
    pub applied_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookEffectReceipt {
    /// Structured script output may only be admitted after separate strict
    /// output-contract and scoped-recipient validation.
    pub validated_output: Option<Value>,
    /// Machine-readable and bounded. Never include raw stdout, stderr or env.
    pub error_code: Option<String>,
}

/// Script output is not an authority token. Parsed results must still be
/// checked against the canonical caller, eligible recipients and worktree
/// route before applying any context or spawn-template mutation.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TeamHookScriptOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_teammate_add: Option<TeamHookPreAddOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inject: Option<TeamHookInjectionOutput>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TeamHookPreAddOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spawn_template_mutation: Option<TeamHookSpawnTemplateMutation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onboarding_context_append_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookSpawnTemplateMutation {
    pub cwd: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TeamHookInjectionOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller: Option<TeamHookInjectionText>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<TeamHookInjectionTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookInjectionText {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamHookInjectionTarget {
    pub agent_id: String,
    pub text: String,
}

pub fn validate_team_hook_script_output(
    event: TeamHookEventName,
    stdout: &[u8],
) -> Result<TeamHookScriptOutput, RuntimeError> {
    if stdout.is_empty() || stdout.len() > 16 * 1024 {
        return Err(RuntimeError::InvalidState(
            "hook output size invalid".into(),
        ));
    }
    let output = serde_json::from_slice::<TeamHookScriptOutput>(stdout)
        .map_err(|_| RuntimeError::InvalidState("hook output JSON/schema invalid".into()))?;
    if event != TeamHookEventName::PreTeammateAdd && output.pre_teammate_add.is_some() {
        return Err(RuntimeError::InvalidState(
            "pre-add output for non-pre-add event".into(),
        ));
    }
    if let Some(pre) = &output.pre_teammate_add {
        if let Some(mutation) = &pre.spawn_template_mutation {
            let cwd = std::path::Path::new(&mutation.cwd);
            if mutation.cwd.len() > 4096 || !cwd.is_absolute() || mutation.cwd.contains('\0') {
                return Err(RuntimeError::InvalidState("invalid pre-add cwd".into()));
            }
        }
        if let Some(text) = &pre.onboarding_context_append_text {
            validate_hook_text(text)?;
        }
    }
    if let Some(inject) = &output.inject {
        if inject.targets.len() > 16 {
            return Err(RuntimeError::InvalidState(
                "too many hook injection targets".into(),
            ));
        }
        if let Some(caller) = &inject.caller {
            validate_hook_text(&caller.text)?;
        }
        let mut seen = std::collections::HashSet::new();
        for target in &inject.targets {
            if target.agent_id.trim().is_empty()
                || target.agent_id.len() > 128
                || !seen.insert(&target.agent_id)
            {
                return Err(RuntimeError::InvalidState(
                    "invalid hook injection target".into(),
                ));
            }
            validate_hook_text(&target.text)?;
        }
    }
    Ok(output)
}

fn validate_hook_text(text: &str) -> Result<(), RuntimeError> {
    if text.trim().is_empty() || text.len() > 8192 || text.contains('\0') {
        return Err(RuntimeError::InvalidState(
            "invalid hook context text".into(),
        ));
    }
    Ok(())
}
