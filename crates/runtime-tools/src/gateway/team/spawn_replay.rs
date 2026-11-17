use super::super::presets::ResolvedModelPreset;
use super::*;
use runtime_core::{RuntimeError, TeamMemberSpawnRequest, TeamMemberSpawnWorktreeInput};

fn fresh_spawn_unsupported() -> RuntimeError {
    RuntimeError::Unsupported(
        "fresh legacy gg_team_manage add is unavailable; use workspace-authoritative gg_team add; original keyed history remains inspectable".into(),
    )
}

fn unverified_preset(operation_id: String) -> TeamToolFailure {
    TeamToolFailure::from_legacy_spawn(RuntimeError::operation_unresolved(
        operation_id,
        "the original expanded preset request cannot be certified from current configuration; inspect its saved operation without native retry",
    ))
}

impl RuntimeToolGateway {
    pub(super) async fn invoke_team_manage_add(
        &self,
        caller_session_id: &str,
        team_id: String,
        invocation_id: Option<String>,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        // Preserve the historical pure translation. Current policy, roster,
        // provider capability and filesystem state cannot hide old receipts.
        let title = optional_string_arg(args, "title")?;
        let prompt = optional_string_arg(args, "prompt")?;
        let image_paths = optional_string_array_arg(args, "image_paths")?;
        let preset_name = optional_string_arg(args, "model_preset")?;
        let creator_compaction_subscription =
            optional_creator_compaction_subscription_arg(args, "creator_compaction_subscription")?;
        let worktree_name = optional_string_arg(args, "worktree_name")?;
        let use_existing_worktree =
            optional_bool_arg(args, "use_existing_worktree")?.unwrap_or(false);
        if use_existing_worktree && worktree_name.is_none() {
            return Err(TeamToolFailure::new(
                "bad_request",
                "worktree_name is required when use_existing_worktree is true",
            ));
        }
        let idempotency_key = invocation_id
            .filter(|id| !id.trim().is_empty())
            .map(|id| format!("gg_team_manage:{id}"));
        if let Some(key) = idempotency_key.as_deref() {
            if key.is_empty()
                || key.len() > 512
                || key.trim() != key
                || key.chars().any(char::is_control)
            {
                return Err(TeamToolFailure::new("bad_request",
                    "spawn idempotency key must be 1-512 printable characters without surrounding whitespace"));
            }
        }
        let original_operation = if preset_name.is_some() {
            match idempotency_key.as_deref() {
                Some(key) => self
                    .process_manager
                    .store
                    .find_team_member_spawn_operation(&team_id, caller_session_id, key)
                    .map_err(TeamToolFailure::from_legacy_spawn)?,
                None => None,
            }
        } else {
            None
        };
        let resolved_model_preset = match preset_name.as_deref() {
            Some(name) => match self.team_model_presets.resolve(name) {
                Some(preset) => Some(preset),
                None => {
                    return Err(match original_operation {
                        Some(operation_id) => unverified_preset(operation_id),
                        None => TeamToolFailure::from_legacy_spawn(fresh_spawn_unsupported()),
                    })
                }
            },
            None => None,
        };
        let worktree = worktree_name.map(|name| TeamMemberSpawnWorktreeInput {
            mode: Some(
                if use_existing_worktree {
                    "reuse"
                } else {
                    "create"
                }
                .to_string(),
            ),
            name: Some(name),
            branch_prefix: None,
            base_ref: None,
            run_init_script: None,
        });
        let request = TeamMemberSpawnRequest {
            team_id: team_id.clone(),
            source_session_id: caller_session_id.to_string(),
            idempotency_key,
            provider: resolved_model_preset
                .as_ref()
                .map(|preset| preset.provider.as_str().to_string()),
            model: resolved_model_preset
                .as_ref()
                .map(|preset| preset.model.clone()),
            title,
            prompt,
            permission_mode: None,
            metadata: build_manage_add_metadata(resolved_model_preset.as_ref(), &image_paths),
            worktree,
            creator_agent_id: Some(caller_session_id.to_string()),
            creator_compaction_subscription,
        };
        let spawn = match self.worktrees.inspect_legacy_team_member_spawn(&request) {
            Ok(response) => response,
            Err(RuntimeError::Conflict(_)) if original_operation.is_some() => {
                // The old admission saved only its expanded-request hash. It
                // cannot distinguish preset drift from changed model inputs.
                return Err(unverified_preset(original_operation.unwrap()));
            }
            Err(error) => return Err(TeamToolFailure::from_legacy_spawn(error)),
        };
        Ok(json!({
            "operation": "add", "team_id": team_id,
            "operation_id": spawn.operation_id,
            "spawned_agent_id": spawn.spawned_session.id,
            "spawned_session": spawn.spawned_session,
            "spawned_member": spawn.spawned_member,
            "team": spawn.team, "worktree": spawn.worktree,
            "worktree_assignment_mode": spawn.worktree_assignment_mode,
            "worktree_created_by_operation": spawn.worktree_created_by_operation,
            "onboarding": spawn.onboarding, "journal_stage": spawn.journal_stage,
            "model_preset": resolved_model_preset.as_ref().map(|preset| preset.name.clone()),
            "image_count": image_paths.len(),
        }))
    }
}

fn optional_creator_compaction_subscription_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Option<String>, TeamToolFailure> {
    let Some(value) = optional_string_arg(args, field)? else {
        return Ok(None);
    };
    match value.as_str() {
        "auto" | "unsubscribed" => Ok(Some(value)),
        _ => Err(TeamToolFailure::new(
            "bad_request",
            format!("{field} must be either auto or unsubscribed"),
        )),
    }
}

fn build_manage_add_metadata(
    preset: Option<&ResolvedModelPreset>,
    image_paths: &[String],
) -> Option<Value> {
    let mut object = serde_json::Map::new();
    if let Some(preset) = preset {
        object.insert(
            "model_preset".to_string(),
            Value::String(preset.name.clone()),
        );
        if let Some(thinking_effort) = preset.thinking_effort.as_deref() {
            object.insert(
                "thinking_effort".to_string(),
                Value::String(thinking_effort.to_string()),
            );
        }
    }
    if !image_paths.is_empty() {
        object.insert(
            "gg_team_manage_add_image_paths".to_string(),
            Value::Array(image_paths.iter().cloned().map(Value::String).collect()),
        );
    }
    if object.is_empty() {
        None
    } else {
        Some(Value::Object(object))
    }
}
