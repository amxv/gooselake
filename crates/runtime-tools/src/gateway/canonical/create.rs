use runtime_core::{
    MemberAgentCreatePreset, MemberAgentCreateRoute, ProviderThinkingEffort,
    WorkspaceAgentCreateRequest, WorkspaceAgentOnboardingRequest, WorkspaceAgentRecord,
};

use super::*;

impl RuntimeToolGateway {
    pub(super) async fn canonical_team_add(
        &self,
        caller: &str,
        agent: &WorkspaceAgentRecord,
        mode: TeamMode,
        parent_id: Option<&str>,
    ) -> Result<Value, RuntimeError> {
        let TeamMode::Add {
            title,
            prompt,
            image_paths,
            model_preset,
            worktree_name,
            creator_compaction_subscription,
        } = mode
        else {
            return Err(RuntimeError::ProtocolViolation(
                "not a member Add request".into(),
            ));
        };
        let parent = parent_id.ok_or_else(|| {
            RuntimeError::InvalidState("model creation requires its admitted host operation".into())
        })?;
        let runtime = self
            .runtime
            .as_ref()
            .expect("canonical caller checked runtime");
        let workspace_id = &agent.workspace_id;
        runtime.authorize_workspace_membership(
            workspace_id,
            caller,
            runtime_core::WorkspaceMembershipAction::Add,
            self.membership_policy(),
        )?;
        let preset = model_preset
            .as_deref()
            .map(|name| {
                let preset = self
                    .team_model_presets
                    .resolve(name)
                    .ok_or_else(|| RuntimeError::NotFound(format!("model preset {name}")))?;
                Ok::<_, RuntimeError>(MemberAgentCreatePreset {
                    requested_name: name.into(),
                    provider: preset.provider,
                    model: preset.model,
                    thinking_effort: preset
                        .thinking_effort
                        .as_deref()
                        .map(ProviderThinkingEffort::parse)
                        .transpose()?,
                })
            })
            .transpose()?;
        let provider = preset
            .as_ref()
            .map_or(agent.recreation_policy.provider, |p| p.provider);
        let images = image_paths.unwrap_or_default();
        if !images.is_empty() && provider == runtime_core::ProviderKind::Acp {
            return Err(RuntimeError::Unsupported(
                "ACP onboarding images are not supported".into(),
            ));
        }
        let route = match worktree_name {
            Some(name) => self
                .worktrees
                .plan_member_agent_create_route(workspace_id, &name)?,
            None => MemberAgentCreateRoute::Root {
                canonical_root: runtime_core_store(self)
                    .get_workspace(workspace_id)?
                    .ok_or_else(|| RuntimeError::NotFound("member workspace".into()))?
                    .canonical_root,
            },
        };
        let mut preferences = agent.recreation_policy.current_preferences.clone();
        if let Some(effort) = preset.as_ref().and_then(|p| p.thinking_effort) {
            preferences.thinking_effort = Some(effort);
        }
        let request = WorkspaceAgentCreateRequest {
            provider,
            model: preset
                .as_ref()
                .map(|p| p.model.clone())
                .or_else(|| agent.recreation_policy.model.clone()),
            permission_intent: agent.recreation_policy.permission_intent.clone(),
            setting_sources_intent: agent.recreation_policy.setting_sources_intent.clone(),
            current_preferences: preferences,
            system_prompt: agent.recreation_policy.system_prompt.clone(),
            allowed_tools: agent.recreation_policy.allowed_tools.clone(),
            disallowed_tools: agent.recreation_policy.disallowed_tools.clone(),
            cwd: None,
            worktree: Some(route.request_route()),
            harness_version_slot: agent.recreation_policy.harness_version_slot.clone(),
            title,
            metadata: None,
            onboarding: Some(WorkspaceAgentOnboardingRequest {
                prompt,
                image_paths: images,
                skill: None,
                creator_compaction_subscription,
            }),
        };
        let command = runtime.plan_workspace_agent_create_as_member(
            workspace_id,
            caller,
            parent,
            request,
            preset,
            route,
            self.membership_policy(),
        )?;
        runtime
            .execute_workspace_agent_create_as_member(command, self.worktrees.as_ref())
            .await
    }
}
