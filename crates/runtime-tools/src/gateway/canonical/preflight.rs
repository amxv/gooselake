use super::*;
use crate::gateway::presets::ModelPresetCatalog;

/// Validate immutable caller-supplied team mutation arguments before durable
/// admission of a host invocation. Do not inspect workspace/agent/worktree
/// state here: replaying an already successful operation after the resource
/// changed must return its receipt, not a fresh state validation error.
pub(super) fn validate_team_mode(mode: &TeamMode) -> Result<(), RuntimeError> {
    match mode {
        TeamMode::Status {} => Ok(()),
        TeamMode::Add {
            title,
            prompt,
            image_paths,
            model_preset: _,
            worktree_name,
            creator_compaction_subscription,
        } => {
            if let Some(value) = title.as_deref() {
                nonempty(value, "title")?;
            }
            if let Some(value) = prompt.as_deref() {
                nonempty(value, "prompt")?;
            }
            if let Some(paths) = image_paths {
                image_count_valid(paths)?;
            }
            if let Some(value) = worktree_name.as_deref() {
                nonempty(value, "worktree_name")?;
            }
            if let Some(value) = creator_compaction_subscription.as_deref() {
                if !matches!(value, "auto" | "unsubscribed") {
                    return Err(RuntimeError::InvalidState(
                        "creator_compaction_subscription must be auto or unsubscribed".into(),
                    ));
                }
            }
            Ok(())
        }
        TeamMode::Remove {
            remove_agent_ids,
            preserve_worktrees,
        } => {
            if remove_agent_ids.is_empty() {
                return Err(RuntimeError::InvalidState(
                    "remove_agent_ids cannot be empty".into(),
                ));
            }
            if remove_agent_ids.len() > 100 {
                return Err(RuntimeError::InvalidState(
                    "remove_agent_ids cannot contain more than 100 agents".into(),
                ));
            }
            if *preserve_worktrees == Some(false) {
                return Err(RuntimeError::Unsupported("managed worktree cleanup on model removal is not available; omit preserve_worktrees or set true".into()));
            }
            let mut seen = HashSet::new();
            if remove_agent_ids
                .iter()
                .any(|id| id.trim().is_empty() || !seen.insert(id))
            {
                return Err(RuntimeError::InvalidState(
                    "remove_agent_ids must be distinct nonempty agent IDs".into(),
                ));
            }
            Ok(())
        }
        TeamMode::AssignWorktree { assign_worktree } => {
            nonempty(&assign_worktree.agent_id, "assign_worktree.agent_id")?;
            nonempty(
                &assign_worktree.worktree_name,
                "assign_worktree.worktree_name",
            )?;
            if assign_worktree.preserve_previous_worktree == Some(false) {
                return Err(RuntimeError::Unsupported(
                    "previous-worktree cleanup is not currently available through gg_team; preserve the prior worktree".into(),
                ));
            }
            Ok(())
        }
    }
}

/// Catalog configuration may change across restarts. Apply it only after
/// looking up the original host receipt, and before a new intent is admitted.
pub(super) fn validate_team_model_preset(
    mode: &TeamMode,
    presets: &ModelPresetCatalog,
) -> Result<(), RuntimeError> {
    if let TeamMode::Add {
        model_preset: Some(name),
        ..
    } = mode
    {
        presets
            .resolve(name)
            .ok_or_else(|| RuntimeError::NotFound(format!("model preset {name}")))?;
    }
    Ok(())
}
