//! Blocking workspace setup hooks are admitted before provider creation. Hook
//! output is untrusted data until the manager constrains the working route and
//! target identities. The resulting context commits with the agent onboarding.

use std::path::Path;
use std::sync::Arc;

use crate::{
    team_hooks::{validate_team_hook_script_output, TeamHookScriptOutput},
    RuntimeError, RuntimeTeamHookExecutor, TeamHookEffectAdmission, TeamHookEffectPhase,
    TeamHookEventName, TeamHookPreAddAdmission, TeamHookPreAddContext, WorkspaceRecord,
};

use super::helpers::now_ms;
use super::workspace_agents::canonical_agent_cwd;
use super::RuntimeSessionManager;

const NEW_MEMBER_TARGET: &str = "__new_member__";
const MAX_TOTAL_ONBOARDING_HOOK_TEXT: usize = 32 * 1024;
const MAX_TOTAL_CALLER_HOOK_TEXT: usize = 16 * 1024;

impl RuntimeSessionManager {
    /// Avoid opening a provider session when the operator revoked a hook while
    /// its shell command was running. Final admission rechecks grants inside
    /// the SQLite transaction to close the remaining provider-create race.
    pub(super) fn verify_pre_add_grants_before_provider(
        &self,
        workspace_id: &str,
        context: &TeamHookPreAddContext,
    ) -> Result<(), RuntimeError> {
        let approval = self.store.get_team_hook_approval(workspace_id)?;
        for name in &context.completed_hook_names {
            let effect = self
                .store
                .get_team_hook_effect(
                    workspace_id,
                    &context.parent_event_id,
                    TeamHookEventName::PreTeammateAdd,
                    name,
                )?
                .ok_or_else(|| RuntimeError::Conflict("pre-add hook effect disappeared".into()))?;
            if effect.phase != TeamHookEffectPhase::Succeeded
                || effect.application_state != "pending"
                || !approval.hooks.contains(&effect.hook)
            {
                return Err(RuntimeError::Conflict(
                    "pre-add hook approval changed before provider creation".into(),
                ));
            }
        }
        Ok(())
    }

    pub async fn pre_add_hook_executor_enabled(&self) -> bool {
        self.pre_add_hook_executor.read().await.is_some()
    }

    pub async fn install_pre_add_hook_executor(&self, executor: Arc<dyn RuntimeTeamHookExecutor>) {
        *self.pre_add_hook_executor.write().await = Some(executor);
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare_pre_add_hooks(
        &self,
        workspace: &WorkspaceRecord,
        reserved_agent_id: &str,
        actor_agent_id: Option<&str>,
        provider: &str,
        model: Option<&str>,
        planned_cwd: &str,
        explicit_route: bool,
        title: Option<&str>,
        onboarding_prompt: Option<&str>,
    ) -> Result<(Option<String>, Option<TeamHookPreAddContext>), RuntimeError> {
        let effects = self
            .store
            .admit_pre_add_team_hooks(&TeamHookPreAddAdmission {
                workspace_id: workspace.workspace_id.clone(),
                reserved_agent_id: reserved_agent_id.to_owned(),
                actor_agent_id: actor_agent_id.map(str::to_owned),
                provider: provider.to_owned(),
                model: model.map(str::to_owned),
                planned_cwd: planned_cwd.to_owned(),
                member_title: title.map(str::to_owned),
                onboarding_prompt: onboarding_prompt.map(str::to_owned),
                created_at_ms: now_ms(),
            })?;
        if effects.is_empty() {
            return Ok((None, None));
        }

        // No implementation silently accepts a hook grant without having a
        // native, durably receipted executor configured by the runtime host.
        let executor = self
            .pre_add_hook_executor
            .read()
            .await
            .clone()
            .ok_or_else(|| {
                RuntimeError::Unsupported("approved pre-add hook executor unavailable".into())
            })?;
        let mut selected_cwd = None;
        let mut onboarding_context = None;
        let mut caller_context = None;
        let mut names = Vec::new();
        let parent_event_id = effects[0].parent_event_id.clone();

        for effect in effects {
            if effect.parent_event_id != parent_event_id {
                return Err(RuntimeError::Conflict(
                    "pre-add hook parent event changed".into(),
                ));
            }
            let admission = TeamHookEffectAdmission {
                workspace_id: effect.workspace_id.clone(),
                parent_event_id: effect.parent_event_id.clone(),
                hook: effect.hook.clone(),
                input: effect.input.clone(),
                created_at_ms: effect.created_at_ms,
            };
            let finished = executor.execute(&admission).await?;
            if finished.phase != TeamHookEffectPhase::Succeeded {
                return Err(RuntimeError::InvalidState(format!(
                    "approved pre-add hook {} failed; inspect durable operator receipts",
                    finished.hook.name,
                )));
            }
            let value = finished
                .receipt
                .as_ref()
                .and_then(|receipt| receipt.validated_output.as_ref())
                .ok_or_else(|| {
                    RuntimeError::InvalidState("pre-add hook has no validated output".into())
                })?;
            let output_bytes = serde_json::to_vec(value).map_err(|_| {
                RuntimeError::InvalidState("pre-add hook output cannot serialize".into())
            })?;
            let output = validate_team_hook_script_output(finished.hook.event_name, &output_bytes)?;
            accumulate_pre_add_output(
                output,
                workspace,
                reserved_agent_id,
                actor_agent_id,
                planned_cwd,
                explicit_route,
                &mut selected_cwd,
                &mut onboarding_context,
                &mut caller_context,
            )?;
            names.push(finished.hook.name);
        }

        Ok((
            selected_cwd,
            Some(TeamHookPreAddContext {
                parent_event_id,
                completed_hook_names: names,
                onboarding_context_append_text: onboarding_context,
                caller_context_text: caller_context,
            }),
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn accumulate_pre_add_output(
    output: TeamHookScriptOutput,
    workspace: &WorkspaceRecord,
    reserved_agent_id: &str,
    actor_agent_id: Option<&str>,
    planned_cwd: &str,
    explicit_route: bool,
    selected_cwd: &mut Option<String>,
    onboarding_context: &mut Option<String>,
    caller_context: &mut Option<String>,
) -> Result<(), RuntimeError> {
    if let Some(pre) = output.pre_teammate_add {
        if let Some(mutation) = pre.spawn_template_mutation {
            // Implicit routes may move only within the canonical workspace
            // root. A separately authorized managed worktree may be outside
            // that root; an explicit route is valid only as a *no-op* here.
            let canonical = if explicit_route {
                let proposed = std::fs::canonicalize(&mutation.cwd).map_err(|_| {
                    RuntimeError::InvalidState("pre-add route is not accessible".into())
                })?;
                let planned = std::fs::canonicalize(planned_cwd).map_err(|_| {
                    RuntimeError::InvalidState("selected worktree route is not accessible".into())
                })?;
                if proposed != planned {
                    return Err(RuntimeError::Conflict(
                        "pre-add hook cannot override an explicit worktree route".into(),
                    ));
                }
                planned.to_string_lossy().into_owned()
            } else {
                canonical_agent_cwd(&workspace.canonical_root, Some(&mutation.cwd))?
            };
            if selected_cwd
                .as_deref()
                .is_some_and(|prior| prior != canonical.as_str())
            {
                return Err(RuntimeError::Conflict(
                    "pre-add hooks proposed conflicting working directories".into(),
                ));
            }
            if !Path::new(&canonical).is_dir() {
                return Err(RuntimeError::InvalidState(
                    "pre-add directory is not available".into(),
                ));
            }
            *selected_cwd = Some(canonical);
        }
        if let Some(text) = pre.onboarding_context_append_text {
            append_bounded(onboarding_context, &text, MAX_TOTAL_ONBOARDING_HOOK_TEXT)?;
        }
    }
    if let Some(inject) = output.inject {
        if let Some(caller) = inject.caller {
            if actor_agent_id.is_none() {
                return Err(RuntimeError::Conflict(
                    "pre-add caller is not a workspace agent".into(),
                ));
            }
            append_bounded(caller_context, &caller.text, MAX_TOTAL_CALLER_HOOK_TEXT)?;
        }
        for target in inject.targets {
            if target.agent_id == NEW_MEMBER_TARGET || target.agent_id == reserved_agent_id {
                append_bounded(
                    onboarding_context,
                    &target.text,
                    MAX_TOTAL_ONBOARDING_HOOK_TEXT,
                )?;
            } else if actor_agent_id == Some(target.agent_id.as_str()) {
                append_bounded(caller_context, &target.text, MAX_TOTAL_CALLER_HOOK_TEXT)?;
            } else {
                return Err(RuntimeError::Conflict(
                    "pre-add hook attempted to inject an unauthorized target".into(),
                ));
            }
        }
    }
    Ok(())
}

fn append_bounded(target: &mut Option<String>, text: &str, max: usize) -> Result<(), RuntimeError> {
    let next = match target.as_deref() {
        Some(old) => format!("{old}\n{text}"),
        None => text.to_owned(),
    };
    if next.len() > max || next.contains('\0') {
        return Err(RuntimeError::InvalidState(
            "aggregate pre-add context exceeds bounds".into(),
        ));
    }
    *target = Some(next);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkspaceLifecycleState;

    #[test]
    fn explicit_managed_route_allows_only_the_authorized_location() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("repository");
        let managed = temp.path().join("managed-worktree");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&managed).unwrap();
        let workspace = WorkspaceRecord {
            workspace_id: "ws_scoped".into(),
            canonical_root: root.to_string_lossy().into_owned(),
            display_name: "Scoped".into(),
            lifecycle_state: WorkspaceLifecycleState::Active,
            lead_agent_id: None,
            revision: 0,
            created_at: 1,
            updated_at: 1,
        };
        let input = |cwd: &Path| {
            serde_json::from_value::<TeamHookScriptOutput>(serde_json::json!({"pre_teammate_add": {
                "spawn_template_mutation":{"cwd":cwd}
            }}))
            .unwrap()
        };
        let mut selected = None;
        let mut onboarding = None;
        let mut caller = None;
        accumulate_pre_add_output(
            input(&managed),
            &workspace,
            "sess_scoped",
            None,
            managed.to_str().unwrap(),
            true,
            &mut selected,
            &mut onboarding,
            &mut caller,
        )
        .unwrap();
        assert_eq!(selected.as_deref(), managed.to_str());

        let mut new_selected = None;
        let blocked = accumulate_pre_add_output(
            input(&root),
            &workspace,
            "sess_scoped",
            None,
            managed.to_str().unwrap(),
            true,
            &mut new_selected,
            &mut onboarding,
            &mut caller,
        );
        assert!(matches!(blocked, Err(RuntimeError::Conflict(_))));
        assert!(new_selected.is_none());
    }
}
