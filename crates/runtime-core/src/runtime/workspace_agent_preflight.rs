use std::path::Path;

use crate::{
    ProviderKind, RuntimeError, WorkspaceAgentCreateRequest, WorkspaceAgentInitialRoute,
    WorkspaceAgentOnboardingRequest, WorkspaceLifecycleState,
};

use super::workspace_agents::normalize_recreation_policy;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// Advisory read-only preflight before the HTTP `new` route can mutate Git.
    /// This rejects deterministic invalid inputs early, not concurrent
    /// lifecycle changes: authoritative admission still rechecks every
    /// condition under the provider/repository locks during actual creation.
    pub fn preflight_workspace_agent_creation_inputs(
        &self,
        workspace_id: &str,
        request: &WorkspaceAgentCreateRequest,
        added_by: &str,
    ) -> Result<(), RuntimeError> {
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
        if self
            .store
            .workspace_retirement_preflight(workspace_id)?
            .blockers
            .iter()
            .any(|blocker| blocker.code == "retirement_in_progress")
        {
            return Err(RuntimeError::Conflict(format!(
                "workspace {workspace_id} is frozen for retirement"
            )));
        }
        if added_by.trim().is_empty() {
            return Err(RuntimeError::InvalidState(
                "workspace agent added_by is required".into(),
            ));
        }
        validate_initial_agent_route(request)?;
        validate_agent_onboarding(request)?;
        normalize_recreation_policy(&workspace.canonical_root, request)?;
        self.providers.get(request.provider).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(request.provider.as_str().to_string())
        })?;
        Ok(())
    }
}

pub(super) fn validate_initial_agent_route(
    request: &WorkspaceAgentCreateRequest,
) -> Result<(), RuntimeError> {
    if request.cwd.is_some() {
        let error = match request.worktree.as_ref() {
            Some(WorkspaceAgentInitialRoute::Root) => {
                Some("root worktree route cannot also specify an arbitrary cwd")
            }
            Some(WorkspaceAgentInitialRoute::Existing { .. }) => {
                Some("managed worktree route cannot also specify an arbitrary cwd")
            }
            Some(WorkspaceAgentInitialRoute::New { .. }) => {
                Some("new managed worktree selection cannot also specify cwd")
            }
            None => None,
        };
        if let Some(message) = error {
            return Err(RuntimeError::Conflict(message.into()));
        }
    }
    Ok(())
}

/// Shared deterministic validation for both the early read-only preflight
/// and the final provider-creation admission. Never replace the latter with
/// a cached validation receipt: images/skill files may change meanwhile.
pub(super) fn validate_agent_onboarding(
    request: &WorkspaceAgentCreateRequest,
) -> Result<WorkspaceAgentOnboardingRequest, RuntimeError> {
    let mut onboarding = request.onboarding.clone().unwrap_or_default();
    onboarding.image_paths =
        crate::team_comms::validate_message_image_paths(onboarding.image_paths)?;
    crate::team_comms::ensure_message_images_supported(
        request.provider.as_str(),
        !onboarding.image_paths.is_empty(),
    )?;
    if onboarding
        .prompt
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(RuntimeError::InvalidState(
            "onboarding prompt cannot be blank".into(),
        ));
    }
    if let Some(skill) = onboarding.skill.as_ref() {
        if request.provider != ProviderKind::Codex
            || skill.provider != "codex"
            || skill.name.trim().is_empty()
            || !skill
                .path
                .as_deref()
                .is_some_and(|path| Path::new(path).is_file())
        {
            return Err(RuntimeError::Unsupported(
                "onboarding skills require a readable native Codex skill file".into(),
            ));
        }
    }
    let subscription = onboarding
        .creator_compaction_subscription
        .as_deref()
        .unwrap_or("auto");
    if !matches!(subscription, "auto" | "unsubscribed") {
        return Err(RuntimeError::InvalidState(
            "creator_compaction_subscription must be auto or unsubscribed".into(),
        ));
    }
    Ok(onboarding)
}
