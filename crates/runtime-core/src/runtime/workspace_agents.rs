use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    ProviderKind, ProviderPermissionIntent, ProviderPermissionMutationRequest,
    ProviderPermissionMutationResult, ProviderResumeSessionPolicyRequest,
    ProviderSessionLaunchPolicy, ProviderSessionPreferences,
    ProviderSessionPreferencesMutationRequest, ProviderSessionPreferencesMutationResult,
    ProviderSettingSourcesIntent, RuntimeError, SessionRecord, WorkspaceAgentCreateRequest,
    WorkspaceAgentLifecycleState, WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy,
};

use super::RuntimeSessionManager;

const ALIAS_ADJECTIVES: &[&str] = &[
    "amber", "brisk", "calm", "clever", "daring", "eager", "gentle", "lively", "nimble", "quiet",
    "steady", "swift",
];
const ALIAS_ANIMALS: &[&str] = &[
    "badger", "falcon", "fox", "heron", "lynx", "otter", "panda", "raven", "seal", "tiger",
    "whale", "wolf",
];

impl RuntimeSessionManager {
    pub fn get_active_workspace_agent_by_id(
        &self,
        agent_id: &str,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        let agent = self
            .store
            .get_workspace_agent_by_id(agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active {
            return Err(RuntimeError::Conflict(format!(
                "workspace agent {agent_id} is archived"
            )));
        }
        Ok(agent)
    }

    pub fn list_workspace_agents(
        &self,
        workspace_id: &str,
        lifecycle: Option<WorkspaceAgentLifecycleState>,
    ) -> Result<Vec<WorkspaceAgentRecord>, RuntimeError> {
        self.store
            .list_workspace_agents(workspace_id.trim(), lifecycle)
    }

    pub fn get_workspace_agent(
        &self,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.store
            .get_workspace_agent(workspace_id.trim(), agent_id.trim())?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))
    }

    pub async fn mutate_workspace_agent_permission(
        self: &Arc<Self>,
        _request: ProviderPermissionMutationRequest,
    ) -> Result<ProviderPermissionMutationResult, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "raw workspace-agent permission mutation is unsupported; use the journaled operator policy method".into(),
        ))
    }

    pub async fn mutate_workspace_agent_preferences(
        self: &Arc<Self>,
        _request: ProviderSessionPreferencesMutationRequest,
    ) -> Result<ProviderSessionPreferencesMutationResult, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "raw workspace-agent preference mutation is unsupported; use the journaled operator policy method".into(),
        ))
    }

    pub async fn archive_workspace_agent(
        self: &Arc<Self>,
        _workspace_id: &str,
        _agent_id: &str,
        _reason: Option<&str>,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "raw workspace-agent archive is unsupported; use journaled operator or scoped member archival".into(),
        ))
    }

    pub async fn archive_workspace_agent_with_initiator(
        self: &Arc<Self>,
        _workspace_id: &str,
        _agent_id: &str,
        _reason: Option<&str>,
        _initiator_agent_id: Option<&str>,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "raw workspace-agent archive is unsupported; use journaled operator or scoped member archival".into(),
        ))
    }

    pub async fn restore_workspace_agent(
        self: &Arc<Self>,
        _workspace_id: &str,
        _agent_id: &str,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "raw workspace-agent restore is unsupported; use the journaled operator restore method"
                .into(),
        ))
    }

    pub(super) fn provider_resume_request_for_session(
        &self,
        session: &SessionRecord,
        provider_session_ref: String,
        canonical_provider_session_ref: Option<String>,
    ) -> Result<ProviderResumeSessionPolicyRequest, RuntimeError> {
        if let Some(operation_id) = self
            .store
            .unresolved_operator_agent_archive_for_agent(session.id.as_str())?
        {
            return Err(RuntimeError::operation_unresolved(operation_id,
                "an operator archival is unresolved; do not resume an attachment that may already be closed"));
        }
        if let Some(operation_id) = self
            .store
            .unresolved_operator_agent_restore_for_agent(session.id.as_str())?
        {
            return Err(RuntimeError::operation_unresolved(operation_id,
                "a native agent restoration is unresolved; an unjournaled provider resume is not safe"));
        }
        if let Some(operation_id) = self
            .store
            .unresolved_operator_native_policy_for_agent(session.id.as_str())?
        {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "native provider policy outcome is unresolved; provider resume is not safe",
            ));
        }
        if self
            .store
            .unresolved_workspace_agent_rebind(session.id.as_str())?
        {
            return Err(RuntimeError::Conflict(format!(
                "session {} has an unresolved provider workspace binding",
                session.id
            )));
        }
        if let Some(agent) = self.store.get_workspace_agent_by_id(session.id.as_str())? {
            let policy = agent.recreation_policy;
            return Ok(ProviderResumeSessionPolicyRequest {
                runtime_session_id: session.id.clone(),
                provider_session_ref,
                canonical_provider_session_ref,
                cwd: Some(policy.authoritative_cwd.clone()),
                model: policy.model.clone(),
                launch_policy: policy.launch_policy(),
                current_preferences: policy.current_preferences,
                metadata: Some(agent.metadata),
            });
        }
        let permission_intent = match session.permission_mode.clone() {
            Some(mode) => ProviderPermissionIntent::explicit(mode)?,
            None if session.provider == ProviderKind::Claude.as_str() => {
                ProviderPermissionIntent::InheritProviderConfiguration
            }
            None => ProviderPermissionIntent::ProviderDefault,
        };
        Ok(ProviderResumeSessionPolicyRequest {
            runtime_session_id: session.id.clone(),
            provider_session_ref,
            canonical_provider_session_ref,
            cwd: session.cwd.clone(),
            model: session.model.clone(),
            launch_policy: ProviderSessionLaunchPolicy {
                permission_intent,
                setting_sources_intent: ProviderSettingSourcesIntent::Isolated,
                system_prompt: session.system_prompt.clone(),
                ..ProviderSessionLaunchPolicy::default()
            },
            current_preferences: ProviderSessionPreferences::default(),
            metadata: Some(session.metadata.clone()),
        })
    }
}

pub(super) fn validate_claude_mutable_permission(
    permission_intent: &ProviderPermissionIntent,
) -> Result<(), RuntimeError> {
    match permission_intent {
        ProviderPermissionIntent::InheritProviderConfiguration => Ok(()),
        ProviderPermissionIntent::Explicit { mode } if mode == "dontAsk" => {
            Err(RuntimeError::InvalidState(
                "Claude dontAsk is not exposed as a mutable permission selection".to_string(),
            ))
        }
        ProviderPermissionIntent::Explicit { mode } if mode.trim().is_empty() => {
            Err(RuntimeError::InvalidState(
                "Claude permission mode cannot be empty".to_string(),
            ))
        }
        ProviderPermissionIntent::Explicit { .. } => Ok(()),
        ProviderPermissionIntent::ProviderDefault => Err(RuntimeError::InvalidState(
            "Claude permission mutation requires inherit_provider_configuration or an explicit mode"
                .to_string(),
        )),
    }
}

pub(super) fn normalize_recreation_policy(
    workspace_root: &str,
    request: &WorkspaceAgentCreateRequest,
) -> Result<WorkspaceAgentRecreationPolicy, RuntimeError> {
    let authoritative_cwd = canonical_agent_cwd(workspace_root, request.cwd.as_deref())?;
    let allowed_tools = normalize_string_list(&request.allowed_tools, "allowed_tools")?;
    let disallowed_tools = normalize_string_list(&request.disallowed_tools, "disallowed_tools")?;
    if let Some(overlap) = allowed_tools
        .iter()
        .find(|tool| disallowed_tools.contains(tool))
    {
        return Err(RuntimeError::InvalidState(format!(
            "tool {overlap:?} cannot be both allowed and disallowed"
        )));
    }
    let setting_sources_intent = request.setting_sources_intent.clone();
    setting_sources_intent.resolved_sources(Some(authoritative_cwd.as_str()))?;
    Ok(WorkspaceAgentRecreationPolicy {
        provider: request.provider,
        model: normalize_optional(request.model.as_deref()),
        permission_intent: request.permission_intent.clone(),
        setting_sources_intent,
        current_preferences: request.current_preferences.clone(),
        system_prompt: request
            .system_prompt
            .as_ref()
            .filter(|value| !value.trim().is_empty())
            .cloned(),
        allowed_tools,
        disallowed_tools,
        authoritative_cwd,
        harness_version_slot: normalize_optional(request.harness_version_slot.as_deref()),
    })
}

pub(super) fn canonical_agent_cwd(
    workspace_root: &str,
    requested: Option<&str>,
) -> Result<String, RuntimeError> {
    let root = std::fs::canonicalize(workspace_root).map_err(|error| {
        RuntimeError::InvalidState(format!(
            "workspace root could not be canonicalized: {error}"
        ))
    })?;
    let candidate = normalize_optional(requested)
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                root.join(path)
            }
        })
        .unwrap_or_else(|| root.clone());
    let canonical = std::fs::canonicalize(&candidate).map_err(|error| {
        RuntimeError::InvalidState(format!(
            "workspace agent cwd {} could not be canonicalized: {error}",
            candidate.display()
        ))
    })?;
    if !canonical.is_dir() || !is_within(&canonical, &root) {
        return Err(RuntimeError::InvalidState(format!(
            "workspace agent cwd {} must be a directory within workspace {}",
            canonical.display(),
            root.display()
        )));
    }
    Ok(canonical.to_string_lossy().into_owned())
}

fn is_within(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

fn normalize_string_list(values: &[String], field: &str) -> Result<Vec<String>, RuntimeError> {
    let mut normalized = Vec::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            return Err(RuntimeError::InvalidState(format!(
                "{field} cannot contain empty values"
            )));
        }
        if !normalized.iter().any(|existing| existing == value) {
            normalized.push(value.to_string());
        }
    }
    Ok(normalized)
}

pub(super) fn normalize_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn friendly_alias(agent_id: &str) -> String {
    let hash = fnv1a64(agent_id.as_bytes());
    let adjective = ALIAS_ADJECTIVES[(hash as usize) % ALIAS_ADJECTIVES.len()];
    let animal = ALIAS_ANIMALS[((hash >> 8) as usize) % ALIAS_ANIMALS.len()];
    format!("{adjective}-{animal}-{:06x}", hash & 0x00ff_ffff)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_agent_cwd_is_resolved_under_workspace_root() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let child = temp_dir.path().join("child");
        std::fs::create_dir(&child).expect("child directory");
        let resolved = canonical_agent_cwd(
            temp_dir.path().to_str().expect("workspace path"),
            Some("child"),
        )
        .expect("relative cwd");
        assert_eq!(
            resolved,
            std::fs::canonicalize(child).unwrap().to_string_lossy()
        );
    }
}
