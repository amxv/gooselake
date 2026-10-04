use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    ProviderCloseSessionRequest, ProviderCreateSessionPolicyRequest, ProviderKind,
    ProviderPermissionIntent, ProviderResumeSessionPolicyRequest, ProviderSessionLaunchPolicy,
    ProviderSessionPreferences, ProviderSettingSourcesIntent, RuntimeError, SessionRecord,
    WorkspaceAgentCreateRequest, WorkspaceAgentLifecycleState, WorkspaceAgentProfile,
    WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy, WorkspaceLifecycleState,
};

use super::helpers::now_ms;
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

    pub async fn create_workspace_agent(
        self: &Arc<Self>,
        workspace_id: &str,
        request: WorkspaceAgentCreateRequest,
        added_by: &str,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        let workspace_id = workspace_id.trim();
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::InvalidState(format!(
                "workspace {workspace_id} is not active"
            )));
        }
        let added_by = normalize_optional(Some(added_by)).ok_or_else(|| {
            RuntimeError::InvalidState("workspace agent added_by is required".to_string())
        })?;
        let policy = normalize_recreation_policy(&workspace.canonical_root, &request)?;
        let provider = self.providers.get(policy.provider).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(policy.provider.as_str().to_string())
        })?;
        let session_id = self.allocate_id("sess", policy.provider.as_str());
        let metadata = request.metadata.unwrap_or_else(|| serde_json::json!({}));
        let permission_mode = policy.permission_intent.resolved_mode();
        let provider_session = provider
            .create_session_with_policy(ProviderCreateSessionPolicyRequest {
                runtime_session_id: session_id.clone(),
                model: policy.model.clone(),
                cwd: Some(policy.authoritative_cwd.clone()),
                launch_policy: policy.launch_policy(),
                current_preferences: policy.current_preferences.clone(),
                metadata: Some(metadata.clone()),
            })
            .await?;

        let created_at = now_ms();
        let session = SessionRecord {
            id: session_id.clone(),
            provider: policy.provider.as_str().to_string(),
            status: "ready".to_string(),
            cwd: Some(policy.authoritative_cwd.clone()),
            model: policy.model.clone(),
            permission_mode,
            system_prompt: policy.system_prompt.clone(),
            metadata: metadata.clone(),
            provider_session_ref: Some(provider_session.provider_session_ref.clone()),
            canonical_provider_session_ref: provider_session.canonical_provider_session_ref.clone(),
            active_turn_id: None,
            worktree_id: None,
            created_at,
            updated_at: created_at,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        };
        let record = WorkspaceAgentRecord {
            agent_id: session_id.clone(),
            workspace_id: workspace.workspace_id,
            alias: friendly_alias(&session_id),
            lifecycle_state: WorkspaceAgentLifecycleState::Active,
            profile: WorkspaceAgentProfile {
                title: normalize_optional(request.title.as_deref()),
                title_provenance: "v2_create_request".to_string(),
                added_by,
                creator_session_id: None,
                creator_compaction_subscription: "auto".to_string(),
                joined_at: created_at,
            },
            recreation_policy: policy,
            provider_session_ref: session.provider_session_ref.clone(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            metadata,
            archived_at: None,
            archive_reason: None,
            revision: 0,
            created_at,
            updated_at: created_at,
        };

        if let Err(error) = self.store.create_workspace_agent(&session, &record) {
            let _ = provider
                .close_session(ProviderCloseSessionRequest {
                    runtime_session_id: session_id,
                    reason: Some("workspace_agent_admission_failed".to_string()),
                })
                .await;
            return Err(error);
        }
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session);
        Ok(record)
    }

    pub async fn archive_workspace_agent(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
        reason: Option<&str>,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        let agent = self.get_workspace_agent(workspace_id, agent_id)?;
        if agent.lifecycle_state == WorkspaceAgentLifecycleState::Archived {
            return Ok(agent);
        }
        let mut session = self.get_session(agent.agent_id.as_str()).await?;
        if session.active_turn_id.is_some() {
            return Err(RuntimeError::InvalidState(format!(
                "workspace agent {} has an active turn",
                agent.agent_id
            )));
        }
        let changed_at = now_ms();
        session.status = "closed".to_string();
        session.closed_at = Some(changed_at);
        session.updated_at = changed_at;
        let archived = self.store.set_workspace_agent_lifecycle(
            &session,
            agent.agent_id.as_str(),
            WorkspaceAgentLifecycleState::Archived,
            normalize_optional(reason).as_deref(),
            changed_at,
        )?;
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());

        let provider_kind = ProviderKind::from_str(session.provider.as_str()).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        if let Some(provider) = self.providers.get(provider_kind) {
            let _ = provider
                .close_session(ProviderCloseSessionRequest {
                    runtime_session_id: session.id,
                    reason: Some("workspace_agent_archived".to_string()),
                })
                .await;
        }
        Ok(archived)
    }

    pub async fn restore_workspace_agent(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        let agent = self.get_workspace_agent(workspace_id, agent_id)?;
        if agent.lifecycle_state == WorkspaceAgentLifecycleState::Active {
            return Ok(agent);
        }
        let mut session = self.get_session(agent.agent_id.as_str()).await?;
        let provider = self
            .providers
            .get(agent.recreation_policy.provider)
            .ok_or_else(|| {
                RuntimeError::ProviderNotRegistered(
                    agent.recreation_policy.provider.as_str().to_string(),
                )
            })?;
        let provider_session_ref = agent
            .provider_session_ref
            .clone()
            .or_else(|| session.provider_session_ref.clone())
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "workspace agent {} has no provider session identity",
                    agent.agent_id
                ))
            })?;
        let resumed = provider
            .resume_session_with_policy(ProviderResumeSessionPolicyRequest {
                runtime_session_id: agent.agent_id.clone(),
                provider_session_ref,
                canonical_provider_session_ref: agent
                    .canonical_provider_session_ref
                    .clone()
                    .or_else(|| session.canonical_provider_session_ref.clone()),
                cwd: Some(agent.recreation_policy.authoritative_cwd.clone()),
                model: agent.recreation_policy.model.clone(),
                launch_policy: agent.recreation_policy.launch_policy(),
                current_preferences: agent.recreation_policy.current_preferences.clone(),
                metadata: Some(agent.metadata.clone()),
            })
            .await?;

        let changed_at = now_ms();
        session.provider = agent.recreation_policy.provider.as_str().to_string();
        session.provider_session_ref = Some(resumed.provider_session_ref.clone());
        session.canonical_provider_session_ref = resumed.canonical_provider_session_ref.clone();
        session.cwd = Some(agent.recreation_policy.authoritative_cwd.clone());
        session.model = agent.recreation_policy.model.clone();
        session.permission_mode = agent.recreation_policy.permission_intent.resolved_mode();
        session.system_prompt = agent.recreation_policy.system_prompt.clone();
        session.status = "ready".to_string();
        session.closed_at = None;
        session.failure_code = None;
        session.failure_message = None;
        session.updated_at = changed_at;

        let restored = match self.store.set_workspace_agent_lifecycle(
            &session,
            agent.agent_id.as_str(),
            WorkspaceAgentLifecycleState::Active,
            None,
            changed_at,
        ) {
            Ok(record) => record,
            Err(error) => {
                let _ = provider
                    .close_session(ProviderCloseSessionRequest {
                        runtime_session_id: agent.agent_id,
                        reason: Some("workspace_agent_restore_persist_failed".to_string()),
                    })
                    .await;
                return Err(error);
            }
        };
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session);
        Ok(restored)
    }

    pub(super) fn provider_resume_request_for_session(
        &self,
        session: &SessionRecord,
        provider_session_ref: String,
        canonical_provider_session_ref: Option<String>,
    ) -> Result<ProviderResumeSessionPolicyRequest, RuntimeError> {
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

fn normalize_recreation_policy(
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

fn canonical_agent_cwd(
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

fn normalize_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn friendly_alias(agent_id: &str) -> String {
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
