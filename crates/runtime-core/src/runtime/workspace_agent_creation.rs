use super::helpers::now_ms;
use super::workspace_agent_preflight::{validate_agent_onboarding, validate_initial_agent_route};
use super::workspace_agents::{friendly_alias, normalize_optional, normalize_recreation_policy};
use super::RuntimeSessionManager;
use crate::{
    OperatorAgentCreateCommand, OperatorAgentCreateLaunchSnapshot, ProviderCloseSessionRequest,
    ProviderCreateSessionPolicyRequest, RuntimeError, SessionRecord, WorkspaceAgentCreateRequest,
    WorkspaceAgentInitialRoute, WorkspaceAgentLifecycleState, WorkspaceAgentProfile,
    WorkspaceAgentRecord, WorkspaceLifecycleState,
};
use std::sync::Arc;

impl RuntimeSessionManager {
    pub async fn create_workspace_agent(
        self: &Arc<Self>,
        workspace_id: &str,
        request: WorkspaceAgentCreateRequest,
        added_by: &str,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.create_workspace_agent_inner(workspace_id, request, added_by, None)
            .await
    }

    pub(super) async fn create_workspace_agent_inner(
        self: &Arc<Self>,
        workspace_id: &str,
        request: WorkspaceAgentCreateRequest,
        added_by: &str,
        operator: Option<&OperatorAgentCreateCommand>,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        let workspace_id = workspace_id.trim();
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        // Native provider creation precedes the durable agent row. Serialize
        // that entire window with quiescent retirement for this repository,
        // then recheck lifecycle under the same lock. Otherwise retirement
        // could observe an empty roster while a new provider starts.
        let repository_lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
        let _repository_guard = repository_lock.read().await;
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
        let added_by = normalize_optional(Some(added_by)).ok_or_else(|| {
            RuntimeError::InvalidState("workspace agent added_by is required".to_string())
        })?;
        validate_initial_agent_route(&request)?;
        let onboarding = validate_agent_onboarding(&request)?;
        let subscription = onboarding
            .creator_compaction_subscription
            .as_deref()
            .unwrap_or("auto");
        let creator_agent_id = self
            .store
            .get_workspace_agent_by_id(&added_by)?
            .filter(|creator| {
                creator.workspace_id == workspace_id
                    && creator.lifecycle_state == WorkspaceAgentLifecycleState::Active
            })
            .map(|creator| creator.agent_id);
        // Creating a provider session in an existing checkout must not race
        // native cleanup before its durable ownership claim is admitted.
        let repository_lock = if matches!(
            request.worktree.as_ref(),
            Some(WorkspaceAgentInitialRoute::Existing { .. })
        ) {
            Some(crate::repository_worktree_lock(&workspace.canonical_root).await)
        } else {
            None
        };
        let _repository_guard = if let Some(lock) = repository_lock.as_ref() {
            Some(lock.lock().await)
        } else {
            None
        };
        let mut policy = normalize_recreation_policy(&workspace.canonical_root, &request)?;
        let initial_worktree_id = match request.worktree.as_ref() {
            None => None,
            Some(WorkspaceAgentInitialRoute::Root) => {
                if request.cwd.is_some() {
                    return Err(RuntimeError::Conflict(
                        "root worktree route cannot also specify an arbitrary cwd".into(),
                    ));
                }
                policy.authoritative_cwd = workspace.canonical_root.clone();
                None
            }
            Some(WorkspaceAgentInitialRoute::Existing { worktree_id }) => {
                if request.cwd.is_some() {
                    return Err(RuntimeError::Conflict(
                        "managed worktree route cannot also specify an arbitrary cwd".into(),
                    ));
                }
                policy.authoritative_cwd =
                    self.eligible_workspace_worktree_cwd(&workspace, worktree_id)?;
                Some(worktree_id.clone())
            }
            Some(WorkspaceAgentInitialRoute::New { .. }) => {
                return Err(RuntimeError::Unsupported(
                    "new named worktrees must first be created by the workspace worktree service"
                        .into(),
                ));
            }
        };
        policy
            .setting_sources_intent
            .resolved_sources(Some(policy.authoritative_cwd.as_str()))?;
        let provider = self.providers.get_enabled(policy.provider).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(policy.provider.as_str().to_string())
        })?;
        let session_id = operator.map_or_else(
            || self.allocate_id("sess", policy.provider.as_str()),
            |cmd| cmd.reserved_agent_id.clone(),
        );
        let (hook_cwd, pre_add_context) = self
            .prepare_pre_add_hooks(
                &workspace,
                &session_id,
                creator_agent_id.as_deref(),
                policy.provider.as_str(),
                policy.model.as_deref(),
                &policy.authoritative_cwd,
                request.worktree.is_some(),
                request.title.as_deref(),
                onboarding.prompt.as_deref(),
            )
            .await?;
        if let Some(cwd) = hook_cwd {
            policy.authoritative_cwd = cwd;
            policy
                .setting_sources_intent
                .resolved_sources(Some(policy.authoritative_cwd.as_str()))?;
        }
        if let Some(context) = pre_add_context.as_ref() {
            self.verify_pre_add_grants_before_provider(workspace_id, context)?;
        }
        let metadata = request.metadata.unwrap_or_else(|| serde_json::json!({}));
        let permission_mode = policy.permission_intent.resolved_mode();
        let created_at = now_ms();
        let mut session = SessionRecord {
            id: session_id.clone(),
            provider: policy.provider.as_str().to_string(),
            status: "ready".to_string(),
            cwd: Some(policy.authoritative_cwd.clone()),
            model: policy.model.clone(),
            permission_mode,
            system_prompt: policy.system_prompt.clone(),
            metadata: metadata.clone(),
            provider_session_ref: None,
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: initial_worktree_id,
            created_at,
            updated_at: created_at,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        };
        let mut record = WorkspaceAgentRecord {
            agent_id: session_id.clone(),
            workspace_id: workspace.workspace_id,
            alias: friendly_alias(&session_id),
            lifecycle_state: WorkspaceAgentLifecycleState::Active,
            profile: WorkspaceAgentProfile {
                title: normalize_optional(request.title.as_deref()),
                title_provenance: if request
                    .title
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                {
                    "v2_create_request".to_string()
                } else {
                    "unspecified".to_string()
                },
                added_by,
                creator_session_id: creator_agent_id,
                creator_compaction_subscription: subscription.to_string(),
                joined_at: created_at,
            },
            recreation_policy: policy.clone(),
            provider_session_ref: session.provider_session_ref.clone(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            metadata: metadata.clone(),
            archived_at: None,
            archive_reason: None,
            revision: 0,
            created_at,
            updated_at: created_at,
        };

        if let Some(command) = operator {
            self.store.freeze_operator_agent_create_launch(
                command,
                &OperatorAgentCreateLaunchSnapshot {
                    session: session.clone(),
                    agent: record.clone(),
                    onboarding: onboarding.clone(),
                    hooks: pre_add_context.clone(),
                    worktree_operation_id: command.worktree_operation_id.clone(),
                },
            )?;
        }
        // This is the first provider create call. For /v2 operator creation,
        // its exact ID, policy, onboard/hook context and native intent have
        // already committed. Unknown outcomes never trigger a second call.
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
        if provider_session.runtime_session_id != session_id
            || provider_session.provider_session_ref.trim().is_empty()
        {
            return Err(RuntimeError::Conflict(
                "provider returned an invalid or different reserved native session identity".into(),
            ));
        }
        let received_at = now_ms().max(created_at);
        session.provider_session_ref = Some(provider_session.provider_session_ref);
        session.canonical_provider_session_ref = provider_session.canonical_provider_session_ref;
        session.updated_at = received_at;
        record.provider_session_ref = session.provider_session_ref.clone();
        record.canonical_provider_session_ref = session.canonical_provider_session_ref.clone();
        record.updated_at = received_at;

        let persisted = if let Some(command) = operator {
            self.store.create_workspace_agent_with_operator_receipt(
                command,
                &session,
                &record,
                &onboarding,
                pre_add_context.as_ref(),
            )
        } else {
            self.store.create_workspace_agent_with_hook_context(
                &session,
                &record,
                &onboarding,
                pre_add_context.as_ref(),
            )
        };
        if let Err(error) = persisted {
            // The operator's unresolved native claim is the recovery owner:
            // don't try a second irreversible provider close/rollback here.
            if operator.is_none() {
                let _ = provider
                    .close_session(ProviderCloseSessionRequest {
                        runtime_session_id: session_id,
                        reason: Some("workspace_agent_admission_failed".to_string()),
                    })
                    .await;
            }
            return Err(error);
        }
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session);
        Ok(record)
    }
}
