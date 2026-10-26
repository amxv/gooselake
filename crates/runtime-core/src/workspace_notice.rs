use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::workspace::{normalize_idempotency_key, opaque_id, unix_time_ms};
use crate::{
    normalized_json_hash, OperationActor, PersistedUserInputSnapshotInvocation, RuntimeError,
    WorkspaceAgentRecord,
};

/// Operator-owned, durable delivery policy. These flags affect new lifecycle
/// events only; changing them never rewrites already-admitted notices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkspaceNoticePolicy {
    pub lead_transitions: bool,
    pub membership_to_lead: bool,
    pub worktree_reassignment: bool,
    pub workspace_interrupt: bool,
    /// Retain retirement notices as cancelled history because the freeze
    /// prevents admitting any new model input to retiring members.
    pub workspace_retirement: bool,
    /// Creator notices additionally respect each member's subscription.
    pub compaction_to_lead: bool,
    /// Optional team-wide compaction notices. Defaults off to avoid noise.
    pub compaction_to_team: bool,
}

impl Default for WorkspaceNoticePolicy {
    fn default() -> Self {
        Self {
            lead_transitions: true,
            membership_to_lead: true,
            worktree_reassignment: true,
            workspace_interrupt: true,
            workspace_retirement: true,
            compaction_to_lead: true,
            compaction_to_team: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceNoticePolicyRecord {
    pub workspace_id: String,
    pub revision: u64,
    pub policy: WorkspaceNoticePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceNoticePolicyUpdate {
    pub expected_revision: u64,
    pub policy: WorkspaceNoticePolicy,
}

/// Operator mutation admitted and completed with its exact response in the
/// same storage transaction as the notice policy compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceNoticePolicyMutationCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub workspace_id: String,
    pub update: WorkspaceNoticePolicyUpdate,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceNoticePolicyMutationResponse {
    pub operation_id: String,
    pub policy: WorkspaceNoticePolicyRecord,
}

pub fn prepare_workspace_notice_policy_mutation(
    workspace_id: &str,
    update: WorkspaceNoticePolicyUpdate,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<WorkspaceNoticePolicyMutationCommand, RuntimeError> {
    let workspace_id = workspace_id.trim();
    if workspace_id.is_empty() || actor.identifier.trim().is_empty() {
        return Err(RuntimeError::InvalidState(
            "workspace and operator identity are required".into(),
        ));
    }
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = json!({
        "workspace_id": workspace_id,
        "expected_revision": update.expected_revision,
        "policy": &update.policy,
    });
    let normalized_request_hash = normalized_json_hash(&normalized_request)?;
    Ok(WorkspaceNoticePolicyMutationCommand {
        operation_id: opaque_id("op"),
        actor,
        idempotency_key,
        normalized_request,
        normalized_request_hash,
        workspace_id: workspace_id.to_string(),
        update,
        requested_at: unix_time_ms()?,
    })
}

/// User-authored task context is a field of trusted workspace creation, not
/// permission to impersonate the new agent's lead or the runtime itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceAgentOnboardingRequest {
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub image_paths: Vec<String>,
    #[serde(default)]
    pub skill: Option<PersistedUserInputSnapshotInvocation>,
    #[serde(default)]
    pub creator_compaction_subscription: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceSystemNoticeRecord {
    pub notice_id: String,
    pub workspace_id: String,
    pub recipient_agent_id: String,
    pub kind: String,
    pub correlation_id: String,
    pub input: Vec<Value>,
    pub state: String,
    pub turn_id: Option<String>,
    pub attempt_count: u32,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl WorkspaceSystemNoticeRecord {
    pub fn lifecycle(
        workspace_id: &str,
        recipient_agent_id: &str,
        event_id: &str,
        kind: &str,
        message: String,
        now_ms: i64,
    ) -> Self {
        // The operation/event identity, notice kind, and recipient together
        // make recovery and idempotent HTTP replay converge to one notice.
        let notice_id = format!("workspace-lifecycle:{event_id}:{kind}:{recipient_agent_id}");
        Self {
            correlation_id: format!("workspace-system-notice:{notice_id}"),
            notice_id,
            workspace_id: workspace_id.to_owned(),
            recipient_agent_id: recipient_agent_id.to_owned(),
            kind: kind.to_owned(),
            input: vec![json!({"type":"text","text":message})],
            state: "pending".into(),
            turn_id: None,
            attempt_count: 0,
            last_error: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        }
    }

    pub fn onboarding(
        agent: &WorkspaceAgentRecord,
        workspace_name: &str,
        lead_alias: Option<&str>,
        request: &WorkspaceAgentOnboardingRequest,
    ) -> Result<Self, RuntimeError> {
        Self::onboarding_with_hook_context(agent, workspace_name, lead_alias, request, None)
    }

    pub fn onboarding_with_hook_context(
        agent: &WorkspaceAgentRecord,
        workspace_name: &str,
        lead_alias: Option<&str>,
        request: &WorkspaceAgentOnboardingRequest,
        hook_context: Option<&str>,
    ) -> Result<Self, RuntimeError> {
        let mut message = format!(
            "You were added to the workspace team \"{}\".\n",
            workspace_name.trim()
        );
        if let Some(lead) = lead_alias.map(str::trim).filter(|value| !value.is_empty()) {
            message.push_str(&format!("The team lead is {lead}.\n"));
        } else {
            message.push_str("This workspace currently has no team lead.\n");
        }
        message.push_str(&format!("Your name is {}.\n", agent.alias));
        if let Some(title) = agent.profile.title.as_deref() {
            message.push_str(&format!("Your title is {title}.\n"));
        }
        // Title/creator provenance remains in the durable profile and status
        // projection. Do not send its internal sess_* identity or policy
        // bookkeeping back as model-visible onboarding instructions.
        message.push_str("Your role is a team member.\n");
        if let Some(prompt) = request.prompt.as_deref() {
            message.push_str("\nYour initial task:\n");
            message.push_str(prompt.trim());
        }
        if let Some(context) = hook_context {
            message.push_str("\n\nContext from operator-approved workspace setup hooks:\n");
            message.push_str(context);
        }
        let mut input = vec![json!({"type":"text","text":message})];
        input.extend(
            request
                .image_paths
                .iter()
                .map(|path| json!({"type":"image","path":path})),
        );
        if let Some(skill) = request.skill.as_ref() {
            input.push(json!({
                "type": "skill",
                "name": skill.name,
                "display_name": skill.display_name,
                "path": skill.path,
            }));
        }
        let notice_id = format!("workspace-onboarding:{}", agent.agent_id);
        Ok(Self {
            notice_id: notice_id.clone(),
            workspace_id: agent.workspace_id.clone(),
            recipient_agent_id: agent.agent_id.clone(),
            kind: "onboarding".to_string(),
            correlation_id: format!("workspace-system-notice:{notice_id}"),
            input,
            state: "pending".to_string(),
            turn_id: None,
            attempt_count: 0,
            last_error: None,
            created_at_ms: agent.created_at,
            updated_at_ms: agent.created_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn onboarding_request_cannot_forge_recipient_or_lead() {
        assert!(
            serde_json::from_value::<WorkspaceAgentOnboardingRequest>(json!({
                "prompt":"start","lead_agent_id":"sess_fake"
            }))
            .is_err()
        );
    }
}
