use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::workspace::{normalize_idempotency_key, normalized_json_hash, opaque_id, unix_time_ms};
use crate::{
    OperationActor, RuntimeError, WorkspaceAgentLifecycleState, WorkspaceAgentRecord,
    WorkspaceRecord,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceLeadTransitionRequest {
    pub lead_agent_id: Option<String>,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceLeadTransitionCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request_hash: String,
    pub normalized_request: serde_json::Value,
    pub workspace_id: String,
    pub lead_agent_id: Option<String>,
    pub expected_revision: u64,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceLeadTransitionResponse {
    pub operation_id: String,
    pub workspace: WorkspaceRecord,
}

pub fn prepare_workspace_lead_transition(
    workspace_id: &str,
    request: WorkspaceLeadTransitionRequest,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<WorkspaceLeadTransitionCommand, RuntimeError> {
    let workspace_id = normalize_required(workspace_id, "workspace_id")?;
    if actor.identifier.trim().is_empty() {
        return Err(RuntimeError::InvalidState(
            "operation actor identifier cannot be empty".to_string(),
        ));
    }
    let lead_agent_id = request
        .lead_agent_id
        .map(|value| normalize_required(value.as_str(), "lead_agent_id"))
        .transpose()?;
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = json!({
        "workspace_id": workspace_id,
        "lead_agent_id": lead_agent_id,
        "expected_revision": request.expected_revision,
    });
    let normalized_request_hash = normalized_json_hash(&normalized_request)?;
    Ok(WorkspaceLeadTransitionCommand {
        operation_id: opaque_id("op"),
        actor,
        idempotency_key,
        normalized_request_hash,
        normalized_request,
        workspace_id,
        lead_agent_id,
        expected_revision: request.expected_revision,
        requested_at: unix_time_ms()?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceMembershipAction {
    Add,
    Remove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkspaceMembershipPolicy {
    pub non_lead_can_add_members: bool,
    pub non_lead_can_remove_members: bool,
}

pub fn authorize_workspace_membership_mutation(
    workspace: &WorkspaceRecord,
    active_members: &[WorkspaceAgentRecord],
    caller_agent_id: &str,
    action: WorkspaceMembershipAction,
    policy: WorkspaceMembershipPolicy,
) -> Result<(), RuntimeError> {
    let caller_agent_id = normalize_required(caller_agent_id, "caller_agent_id")?;
    let is_active_member = active_members.iter().any(|member| {
        member.agent_id == caller_agent_id
            && member.workspace_id == workspace.workspace_id
            && member.lifecycle_state == WorkspaceAgentLifecycleState::Active
    });
    if !is_active_member {
        return Err(RuntimeError::InvalidState(format!(
            "agent {caller_agent_id} is not an active member of workspace {}",
            workspace.workspace_id
        )));
    }

    let allowed_non_lead = match action {
        WorkspaceMembershipAction::Add => policy.non_lead_can_add_members,
        WorkspaceMembershipAction::Remove => policy.non_lead_can_remove_members,
    };
    if workspace.lead_agent_id.is_none()
        || workspace.lead_agent_id.as_deref() == Some(caller_agent_id.as_str())
        || allowed_non_lead
    {
        return Ok(());
    }

    Err(RuntimeError::InvalidState(format!(
        "agent {caller_agent_id} is not allowed to manage workspace {} membership",
        workspace.workspace_id
    )))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceInterruptCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request_hash: String,
    pub normalized_request: serde_json::Value,
    pub workspace_id: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceInterruptTarget {
    pub agent_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceInterruptPlan {
    pub operation_id: String,
    pub workspace_id: String,
    pub targets: Vec<WorkspaceInterruptTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceInterruptAdmission {
    Replay(WorkspaceInterruptResponse),
    Execute(WorkspaceInterruptPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceInterruptResponse {
    pub operation_id: String,
    pub workspace_id: String,
    pub interrupted_agent_ids: Vec<String>,
    pub skipped_agent_ids: Vec<String>,
}

pub fn prepare_workspace_interrupt(
    workspace_id: &str,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<WorkspaceInterruptCommand, RuntimeError> {
    let workspace_id = normalize_required(workspace_id, "workspace_id")?;
    if actor.identifier.trim().is_empty() {
        return Err(RuntimeError::InvalidState(
            "operation actor identifier cannot be empty".to_string(),
        ));
    }
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = json!({"workspace_id": workspace_id});
    let normalized_request_hash = normalized_json_hash(&normalized_request)?;
    Ok(WorkspaceInterruptCommand {
        operation_id: opaque_id("op"),
        actor,
        idempotency_key,
        normalized_request_hash,
        normalized_request,
        workspace_id: workspace_id.to_string(),
        requested_at: unix_time_ms()?,
    })
}

fn normalize_required(value: &str, field: &str) -> Result<String, RuntimeError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(RuntimeError::InvalidState(format!(
            "{field} cannot be empty"
        )));
    }
    Ok(normalized.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderKind, WorkspaceAgentProfile, WorkspaceAgentRecreationPolicy};

    fn workspace(lead_agent_id: Option<&str>) -> WorkspaceRecord {
        WorkspaceRecord {
            workspace_id: "workspace_1".to_string(),
            canonical_root: "/tmp/workspace".to_string(),
            display_name: "Workspace".to_string(),
            lifecycle_state: crate::WorkspaceLifecycleState::Active,
            lead_agent_id: lead_agent_id.map(str::to_string),
            revision: 0,
            created_at: 1,
            updated_at: 1,
        }
    }

    fn member(agent_id: &str) -> WorkspaceAgentRecord {
        WorkspaceAgentRecord {
            agent_id: agent_id.to_string(),
            workspace_id: "workspace_1".to_string(),
            alias: format!("alias-{agent_id}"),
            lifecycle_state: WorkspaceAgentLifecycleState::Active,
            profile: WorkspaceAgentProfile {
                title: Some("Builder".to_string()),
                title_provenance: "user".to_string(),
                added_by: "operator".to_string(),
                creator_session_id: None,
                creator_compaction_subscription: "auto".to_string(),
                joined_at: 1,
            },
            recreation_policy: WorkspaceAgentRecreationPolicy {
                provider: ProviderKind::Codex,
                model: None,
                permission_intent: crate::ProviderPermissionIntent::ProviderDefault,
                setting_sources_intent: crate::ProviderSettingSourcesIntent::Isolated,
                current_preferences: crate::ProviderSessionPreferences::default(),
                system_prompt: None,
                allowed_tools: Vec::new(),
                disallowed_tools: Vec::new(),
                authoritative_cwd: "/tmp/workspace".to_string(),
                harness_version_slot: None,
            },
            provider_session_ref: None,
            canonical_provider_session_ref: None,
            metadata: json!({}),
            archived_at: None,
            archive_reason: None,
            revision: 0,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn leadless_members_can_manage_but_led_non_leads_follow_policy() {
        let members = vec![member("lead"), member("peer")];
        let deny_non_lead = WorkspaceMembershipPolicy::default();
        assert!(authorize_workspace_membership_mutation(
            &workspace(None),
            &members,
            "peer",
            WorkspaceMembershipAction::Add,
            deny_non_lead,
        )
        .is_ok());
        assert!(authorize_workspace_membership_mutation(
            &workspace(Some("lead")),
            &members,
            "lead",
            WorkspaceMembershipAction::Remove,
            deny_non_lead,
        )
        .is_ok());
        assert!(authorize_workspace_membership_mutation(
            &workspace(Some("lead")),
            &members,
            "peer",
            WorkspaceMembershipAction::Add,
            deny_non_lead,
        )
        .is_err());
        assert!(authorize_workspace_membership_mutation(
            &workspace(Some("lead")),
            &members,
            "peer",
            WorkspaceMembershipAction::Remove,
            WorkspaceMembershipPolicy {
                non_lead_can_add_members: false,
                non_lead_can_remove_members: true,
            },
        )
        .is_ok());
    }
}
