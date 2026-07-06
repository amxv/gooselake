use std::time::{Duration, Instant};

use runtime_core::{
    TeamBroadcastRequest, TeamListMessagesRequest, TeamMemberRecord, TeamMessageRecord,
    TeamRemoveMemberRequest, TeamSendDirectRequest, TeamWithMembers, ToolInvokeRequest,
    WorktreeMemberRemovedRequest,
};
use serde_json::{json, Value};

use crate::{
    now_ms, GG_TEAM_ADD_IDEMPOTENCY_CACHE_TTL_SECS, GG_TEAM_MANAGE, GG_TEAM_MESSAGE, GG_TEAM_STATUS,
};

use super::presets::ResolvedModelPreset;
use super::team_helpers::{
    ensure_team_member, latest_message_for_member, member_last_message_output,
    reject_image_paths_for_provider, status_state_for_session, team_tool_error,
    team_tool_error_code_for_runtime, TeamToolFailure,
};
use super::RuntimeToolGateway;

impl RuntimeToolGateway {
    pub(super) async fn invoke_team_tool(&self, request: ToolInvokeRequest) -> Value {
        if !self.team_policy.enabled {
            return json!({
                "ok": false,
                "error": {
                    "code": "feature_disabled",
                    "message": "gg_team MCP tools are disabled"
                }
            });
        }

        let tool_name = request.tool_name.trim().to_string();
        let args = match request.args {
            Value::Object(map) => map,
            _ => {
                return team_tool_error("bad_request", "tool args must be an object");
            }
        };

        let result = match tool_name.as_str() {
            GG_TEAM_STATUS => {
                self.invoke_team_status(request.caller_session_id.as_str(), &args)
                    .await
            }
            GG_TEAM_MESSAGE => {
                self.invoke_team_message(
                    request.caller_session_id.as_str(),
                    request.invocation_id,
                    &args,
                )
                .await
            }
            GG_TEAM_MANAGE => {
                self.invoke_team_manage(
                    request.caller_session_id.as_str(),
                    request.invocation_id,
                    &args,
                )
                .await
            }
            _ => Err(TeamToolFailure::new(
                "bad_request",
                format!("Unsupported gg_team tool: {}", tool_name),
            )),
        };

        match result {
            Ok(result) => json!({ "ok": true, "result": result }),
            Err(error) => team_tool_error(error.code, error.message),
        }
    }

    async fn invoke_team_status(
        &self,
        caller_session_id: &str,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        reject_team_tool_fields(
            args,
            &["caller_agent_id", "sender", "sender_agent_id", "agent_id"],
        )?;
        let team_id = required_string_arg(args, "team_id")?;
        let team = self
            .team_comms
            .get_team(team_id.as_str())
            .await
            .map_err(TeamToolFailure::from_runtime)?;
        ensure_team_member(&team, caller_session_id)?;
        let messages = self
            .team_comms
            .list_messages(TeamListMessagesRequest {
                team_id: team.team.id.clone(),
                cursor: None,
                limit: Some(100),
            })
            .await
            .map(|page| page.messages)
            .unwrap_or_default();

        let mut members = Vec::with_capacity(team.members.len());
        for member in &team.members {
            members.push(self.status_member_row(member, &messages).await);
        }

        Ok(json!({
            "team_id": team.team.id,
            "lead_agent_id": team.team.lead_agent_id,
            "generated_at_ms": now_ms(),
            "members": members,
        }))
    }

    async fn status_member_row(
        &self,
        member: &TeamMemberRecord,
        messages: &[TeamMessageRecord],
    ) -> Value {
        let session = match self.runtime.as_ref() {
            Some(runtime) => runtime.get_session(member.agent_id.as_str()).await.ok(),
            None => None,
        };
        let worktree = match member.worktree_id.as_deref() {
            Some(worktree_id) => self.worktrees.get_worktree(worktree_id).await.ok(),
            None => None,
        };
        let last_message = latest_message_for_member(member.agent_id.as_str(), messages);
        let last_message_at = last_message.map(|message| message.created_at).unwrap_or(0);
        let session_updated_at = session
            .as_ref()
            .map(|session| session.updated_at)
            .unwrap_or(0);
        let state = status_state_for_session(session.as_ref());

        json!({
            "agent_id": member.agent_id,
            "session_id": member.agent_id,
            "title": member.title,
            "state": state,
            "last_activity_at_ms": member.joined_at.max(session_updated_at).max(last_message_at),
            "last_message": last_message.map(member_last_message_output),
            "context_window_remaining_percentage": self.context_window_remaining_percentage(member.agent_id.as_str()).await,
            "worktree_cwd": worktree.as_ref().map(|record| record.worktree_cwd.clone()),
            "worktree_name": worktree.as_ref().map(|record| record.worktree_name.clone()),
            "added_by": member.added_by,
        })
    }

    async fn invoke_team_message(
        &self,
        caller_session_id: &str,
        invocation_id: Option<String>,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        reject_team_tool_fields(args, &["caller_agent_id", "sender", "sender_agent_id"])?;
        let team_id = required_string_arg(args, "team_id")?;
        let recipient_agent_id = required_string_arg(args, "recipient_agent_id")?;
        let message = required_string_arg(args, "message")?;
        let image_paths = optional_string_array_arg(args, "image_paths")?;
        self.ensure_team_message_images_supported(
            caller_session_id,
            team_id.as_str(),
            recipient_agent_id.as_str(),
            &image_paths,
        )
        .await?;
        let image_count = image_paths.len();
        let input = json!([{ "type": "text", "text": message }]);

        let (scope, ack) = if recipient_agent_id.eq_ignore_ascii_case("broadcast") {
            let ack = self
                .team_comms
                .broadcast(TeamBroadcastRequest {
                    team_id,
                    sender_agent_id: caller_session_id.to_string(),
                    input,
                    image_paths: image_paths.clone(),
                    priority: "normal".to_string(),
                    policy: "non_interrupting".to_string(),
                    include_sender: false,
                    correlation_id: None,
                    idempotency_key: invocation_id,
                })
                .await
                .map_err(TeamToolFailure::from_runtime)?;
            ("broadcast", ack)
        } else {
            let ack = self
                .team_comms
                .send_direct(TeamSendDirectRequest {
                    team_id,
                    sender_agent_id: caller_session_id.to_string(),
                    recipient_agent_id,
                    input,
                    image_paths: image_paths.clone(),
                    priority: "normal".to_string(),
                    policy: "non_interrupting".to_string(),
                    correlation_id: None,
                    reply_to_message_id: None,
                    idempotency_key: invocation_id,
                })
                .await
                .map_err(TeamToolFailure::from_runtime)?;
            ("direct", ack)
        };

        let mut delivery_ids = ack
            .deliveries
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        delivery_ids.sort();

        let mut result = json!({
            "message_id": ack.message.id,
            "delivery_ids": delivery_ids,
            "recipient_count": ack.deliveries.len(),
            "scope": scope,
            "image_count": image_count,
        });
        if image_count > 0 {
            if let Some(object) = result.as_object_mut() {
                object.insert("image_paths".to_string(), json!(image_paths));
            }
        }
        Ok(result)
    }

    async fn invoke_team_manage(
        &self,
        caller_session_id: &str,
        invocation_id: Option<String>,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        reject_team_tool_fields(args, &["caller_agent_id", "sender", "sender_agent_id"])?;
        reject_team_tool_fields_with_message(
            args,
            &[
                "agent_id",
                "model",
                "unsubscribe_from_compaction_notifications",
            ],
            |field| format!("{field} is not supported by gg_team_manage"),
        )?;
        let team_id = required_string_arg(args, "team_id")?;
        if args.contains_key("remove_agent_ids") {
            reject_team_tool_fields_with_message(
                args,
                &[
                    "title",
                    "prompt",
                    "image_paths",
                    "model_preset",
                    "worktree_name",
                    "use_existing_worktree",
                    "creator_compaction_subscription",
                ],
                |field| format!("{field} cannot be provided when remove_agent_ids is provided"),
            )?;
            let remove_agent_ids = optional_string_array_arg(args, "remove_agent_ids")?;
            if remove_agent_ids.is_empty() {
                return Err(TeamToolFailure::new(
                    "bad_request",
                    "remove_agent_ids must contain at least one agent id",
                ));
            }
            return self
                .invoke_team_manage_remove(caller_session_id, team_id, remove_agent_ids)
                .await;
        }

        if let Some(invocation_id) = invocation_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            let cache_key = format!("v1:{caller_session_id}:{GG_TEAM_MANAGE}:{invocation_id}");
            if let Some(cached) = self
                .begin_manage_add_idempotent_execution(&cache_key)
                .await?
            {
                return Ok(cached);
            }
            let result = self
                .invoke_team_manage_add(caller_session_id, team_id, args)
                .await;
            match result {
                Ok(value) => {
                    self.complete_manage_add_idempotent_execution(&cache_key, &value)
                        .await;
                    Ok(value)
                }
                Err(error) => {
                    self.abort_manage_add_idempotent_execution(&cache_key).await;
                    Err(error)
                }
            }
        } else {
            self.invoke_team_manage_add(caller_session_id, team_id, args)
                .await
        }
    }

    async fn invoke_team_manage_add(
        &self,
        caller_session_id: &str,
        team_id: String,
        args: &serde_json::Map<String, Value>,
    ) -> Result<Value, TeamToolFailure> {
        self.ensure_caller_can_manage_membership(
            team_id.as_str(),
            caller_session_id,
            self.team_policy.non_lead_can_add_members,
            "add members to",
        )
        .await?;

        let title = optional_string_arg(args, "title")?;
        let prompt = optional_string_arg(args, "prompt")?;
        let image_paths = optional_string_array_arg(args, "image_paths")?;
        let resolved_model_preset = match optional_string_arg(args, "model_preset")? {
            Some(model_preset) => {
                let Some(resolved) = self.team_model_presets.resolve(model_preset.as_str()) else {
                    return Err(TeamToolFailure::new(
                        "unknown_model_preset",
                        format!(
                            "Unknown model_preset `{}` for gg_team_manage. Available presets: {}",
                            model_preset,
                            self.team_model_presets.all_names().join(", ")
                        ),
                    ));
                };
                Some(resolved)
            }
            None => None,
        };
        let creator_compaction_subscription =
            optional_creator_compaction_subscription_arg(args, "creator_compaction_subscription")?;
        self.ensure_manage_add_images_supported(
            caller_session_id,
            resolved_model_preset.as_ref(),
            &image_paths,
        )
        .await?;
        let worktree_name = optional_string_arg(args, "worktree_name")?;
        let use_existing_worktree =
            optional_bool_arg(args, "use_existing_worktree")?.unwrap_or(false);
        if use_existing_worktree && worktree_name.is_none() {
            return Err(TeamToolFailure::new(
                "bad_request",
                "worktree_name is required when use_existing_worktree is true",
            ));
        }
        let worktree = worktree_name.map(|name| runtime_core::TeamMemberSpawnWorktreeInput {
            mode: Some(if use_existing_worktree {
                "reuse".to_string()
            } else {
                "create".to_string()
            }),
            name: Some(name),
            branch_prefix: None,
            base_ref: None,
            run_init_script: None,
        });
        let metadata = build_manage_add_metadata(resolved_model_preset.as_ref(), &image_paths);

        let spawn = self
            .worktrees
            .spawn_team_member(runtime_core::TeamMemberSpawnRequest {
                team_id: team_id.clone(),
                source_session_id: caller_session_id.to_string(),
                provider: resolved_model_preset
                    .as_ref()
                    .map(|preset| preset.provider.as_str().to_string()),
                model: resolved_model_preset
                    .as_ref()
                    .map(|preset| preset.model.clone()),
                title,
                prompt,
                permission_mode: None,
                metadata,
                worktree,
                creator_agent_id: Some(caller_session_id.to_string()),
                creator_compaction_subscription,
            })
            .await
            .map_err(TeamToolFailure::from_runtime)?;

        Ok(json!({
            "operation": "add",
            "team_id": team_id,
            "operation_id": spawn.operation_id,
            "spawned_agent_id": spawn.spawned_session.id,
            "spawned_session": spawn.spawned_session,
            "spawned_member": spawn.spawned_member,
            "team": spawn.team,
            "worktree": spawn.worktree,
            "worktree_assignment_mode": spawn.worktree_assignment_mode,
            "worktree_created_by_operation": spawn.worktree_created_by_operation,
            "onboarding": spawn.onboarding,
            "journal_stage": spawn.journal_stage,
            "model_preset": resolved_model_preset.as_ref().map(|preset| preset.name.clone()),
            "image_count": image_paths.len(),
        }))
    }

    async fn invoke_team_manage_remove(
        &self,
        caller_session_id: &str,
        team_id: String,
        remove_agent_ids: Vec<String>,
    ) -> Result<Value, TeamToolFailure> {
        self.ensure_caller_can_manage_membership(
            team_id.as_str(),
            caller_session_id,
            self.team_policy.non_lead_can_remove_members,
            "remove members from",
        )
        .await?;

        let mut results = Vec::with_capacity(remove_agent_ids.len());
        for agent_id in remove_agent_ids {
            let removal = self
                .team_comms
                .remove_team_member(TeamRemoveMemberRequest {
                    team_id: team_id.clone(),
                    agent_id: agent_id.clone(),
                })
                .await;
            match removal {
                Ok(team) => {
                    let cleanup = self
                        .worktrees
                        .on_member_removed(WorktreeMemberRemovedRequest {
                            team_id: team_id.clone(),
                            agent_id: agent_id.clone(),
                            removed_by: Some(caller_session_id.to_string()),
                        })
                        .await;
                    let cleanup_output = match cleanup {
                        Ok(cleanup) => json!({
                            "ok": true,
                            "released_claim_count": cleanup.released_claims.len(),
                            "cleanup_result_count": cleanup.cleanup_results.len(),
                            "diagnostic_count": cleanup.diagnostics.len(),
                            "released_claims": cleanup.released_claims,
                            "cleanup_results": cleanup.cleanup_results,
                            "diagnostics": cleanup.diagnostics,
                        }),
                        Err(error) => json!({
                            "ok": false,
                            "error": error.to_string(),
                        }),
                    };
                    results.push(json!({
                        "agent_id": agent_id,
                        "ok": true,
                        "team": team,
                        "cleanup": cleanup_output,
                    }));
                }
                Err(error) => {
                    let error_code = team_tool_error_code_for_runtime(&error);
                    results.push(json!({
                        "agent_id": agent_id,
                        "ok": false,
                        "error": {
                            "code": error_code,
                            "message": error.to_string(),
                        }
                    }));
                }
            }
        }

        let removed_count = results
            .iter()
            .filter(|result| result.get("ok").and_then(Value::as_bool) == Some(true))
            .count();
        Ok(json!({
            "operation": "remove",
            "team_id": team_id,
            "removed_count": removed_count,
            "failed_count": results.len().saturating_sub(removed_count),
            "results": results,
        }))
    }

    async fn ensure_caller_can_manage_membership(
        &self,
        team_id: &str,
        caller_session_id: &str,
        allow_non_lead: bool,
        action: &str,
    ) -> Result<TeamWithMembers, TeamToolFailure> {
        let team = self
            .team_comms
            .get_team(team_id)
            .await
            .map_err(TeamToolFailure::from_runtime)?;
        ensure_team_member(&team, caller_session_id)?;
        if team.team.lead_agent_id == caller_session_id || allow_non_lead {
            return Ok(team);
        }
        Err(TeamToolFailure::new(
            "unauthorized",
            format!("agent {caller_session_id} is not allowed to {action} team {team_id}"),
        ))
    }

    async fn ensure_team_message_images_supported(
        &self,
        caller_session_id: &str,
        team_id: &str,
        recipient_agent_id: &str,
        image_paths: &[String],
    ) -> Result<(), TeamToolFailure> {
        if image_paths.is_empty() {
            return Ok(());
        }
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(());
        };

        let team = self
            .team_comms
            .get_team(team_id)
            .await
            .map_err(TeamToolFailure::from_runtime)?;
        ensure_team_member(&team, caller_session_id)?;

        let recipient_ids = if recipient_agent_id.eq_ignore_ascii_case("broadcast") {
            team.members
                .iter()
                .map(|member| member.agent_id.clone())
                .filter(|agent_id| agent_id != caller_session_id)
                .collect::<Vec<_>>()
        } else {
            vec![recipient_agent_id.to_string()]
        };

        for recipient_id in recipient_ids {
            if let Ok(session) = runtime.get_session(recipient_id.as_str()).await {
                reject_image_paths_for_provider(session.provider.as_str(), "gg_team_message")?;
            }
        }

        Ok(())
    }

    async fn ensure_manage_add_images_supported(
        &self,
        caller_session_id: &str,
        resolved_model_preset: Option<&ResolvedModelPreset>,
        image_paths: &[String],
    ) -> Result<(), TeamToolFailure> {
        if image_paths.is_empty() {
            return Ok(());
        }

        if let Some(preset) = resolved_model_preset {
            return reject_image_paths_for_provider(preset.provider.as_str(), "gg_team_manage");
        }

        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(());
        };
        let session = runtime
            .get_session(caller_session_id)
            .await
            .map_err(TeamToolFailure::from_runtime)?;
        reject_image_paths_for_provider(session.provider.as_str(), "gg_team_manage")
    }

    async fn context_window_remaining_percentage(&self, session_id: &str) -> Value {
        let Some(runtime) = self.runtime.as_ref() else {
            return Value::Null;
        };
        let Ok(turns) = runtime.list_session_turns(session_id).await else {
            return Value::Null;
        };
        turns
            .iter()
            .rev()
            .filter_map(|turn| turn.usage.as_ref())
            .find_map(context_remaining_percentage_from_usage)
            .map(|value| json!(value))
            .unwrap_or(Value::Null)
    }

    async fn begin_manage_add_idempotent_execution(
        &self,
        cache_key: &str,
    ) -> Result<Option<Value>, TeamToolFailure> {
        let now = Instant::now();
        let ttl = Duration::from_secs(GG_TEAM_ADD_IDEMPOTENCY_CACHE_TTL_SECS);
        let mut cache = self.team_manage_add_idempotency.lock().await;
        cache.retain(|_, entry| now.duration_since(entry.inserted_at) <= ttl);
        if let Some(existing) = cache.get(cache_key) {
            if let Some(result) = existing.completed_success.clone() {
                return Ok(Some(result));
            }
            return Err(TeamToolFailure::new(
                "duplicate_tool_invocation_in_progress",
                "Duplicate gg_team_manage add invocation is already in progress",
            ));
        }
        cache.insert(
            cache_key.to_string(),
            super::ManageAddIdempotencyEntry {
                inserted_at: now,
                completed_success: None,
            },
        );
        Ok(None)
    }

    async fn complete_manage_add_idempotent_execution(&self, cache_key: &str, result: &Value) {
        let mut cache = self.team_manage_add_idempotency.lock().await;
        if let Some(entry) = cache.get_mut(cache_key) {
            entry.inserted_at = Instant::now();
            entry.completed_success = Some(result.clone());
        }
    }

    async fn abort_manage_add_idempotent_execution(&self, cache_key: &str) {
        let mut cache = self.team_manage_add_idempotency.lock().await;
        cache.remove(cache_key);
    }
}

fn reject_team_tool_fields(
    args: &serde_json::Map<String, Value>,
    rejected_fields: &[&str],
) -> Result<(), TeamToolFailure> {
    reject_team_tool_fields_with_message(args, rejected_fields, |field| {
        format!("{field} is supplied by gateway metadata and cannot be provided in args")
    })
}

fn reject_team_tool_fields_with_message(
    args: &serde_json::Map<String, Value>,
    rejected_fields: &[&str],
    message_for_field: impl Fn(&str) -> String,
) -> Result<(), TeamToolFailure> {
    for field in rejected_fields {
        if args.contains_key(*field) {
            return Err(TeamToolFailure::new(
                "bad_request",
                message_for_field(field),
            ));
        }
    }
    Ok(())
}

fn required_string_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<String, TeamToolFailure> {
    let value = args
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| TeamToolFailure::new("bad_request", format!("{field} is required")))?;
    Ok(value.to_string())
}

fn optional_string_array_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Vec<String>, TeamToolFailure> {
    let Some(value) = args.get(field) else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let Some(values) = value.as_array() else {
        return Err(TeamToolFailure::new(
            "bad_request",
            format!("{field} must be an array of non-empty strings"),
        ));
    };
    values
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    TeamToolFailure::new(
                        "bad_request",
                        format!("{field} must contain only non-empty strings"),
                    )
                })
        })
        .collect()
}

fn optional_string_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Option<String>, TeamToolFailure> {
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            TeamToolFailure::new(
                "bad_request",
                format!("{field} must be a non-empty string when provided"),
            )
        })
}

fn optional_bool_arg(
    args: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Option<bool>, TeamToolFailure> {
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_bool().map(Some).ok_or_else(|| {
        TeamToolFailure::new(
            "bad_request",
            format!("{field} must be a boolean when provided"),
        )
    })
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

fn context_remaining_percentage_from_usage(usage: &Value) -> Option<u8> {
    let context_window = extract_u64_from_usage(
        usage,
        &[
            "contextWindowSize",
            "context_window_size",
            "contextWindow",
            "context_window",
            "model_context_window",
            "modelContextWindow",
        ],
    )
    .or_else(|| {
        usage
            .get("raw_usage")
            .and_then(extract_context_window_from_usage)
    })?;
    if context_window == 0 {
        return None;
    }

    let total_tokens = extract_u64_from_usage(usage, &["last_total_tokens", "lastTotalTokens"])
        .or_else(|| usage.get("last").and_then(extract_total_tokens_from_usage))
        .or_else(|| {
            usage
                .get("last_token_usage")
                .and_then(extract_total_tokens_from_usage)
        })
        .or_else(|| {
            usage
                .get("lastTokenUsage")
                .and_then(extract_total_tokens_from_usage)
        })
        .or_else(|| extract_total_tokens_from_usage(usage))?;
    let remaining_tokens = context_window.saturating_sub(total_tokens);
    let remaining_percentage = (((remaining_tokens as f64) / (context_window as f64)) * 100.0)
        .round()
        .clamp(0.0, 100.0);
    Some(remaining_percentage as u8)
}

fn extract_context_window_from_usage(usage: &Value) -> Option<u64> {
    extract_u64_from_usage(
        usage,
        &[
            "contextWindowSize",
            "context_window_size",
            "contextWindow",
            "context_window",
            "model_context_window",
            "modelContextWindow",
        ],
    )
}

fn extract_total_tokens_from_usage(usage: &Value) -> Option<u64> {
    extract_u64_from_usage(usage, &["total_tokens", "totalTokens", "total"]).or_else(|| {
        if let Some(raw) = usage.get("raw_usage") {
            if let Some(total) = extract_total_tokens_from_usage(raw) {
                return Some(total);
            }
        }
        let input = extract_u64_from_usage(usage, &["inputTokens", "input_tokens"])?;
        let output = extract_u64_from_usage(usage, &["outputTokens", "output_tokens"]).unwrap_or(0);
        let cache_creation = extract_u64_from_usage(
            usage,
            &["cacheCreationInputTokens", "cache_creation_input_tokens"],
        )
        .unwrap_or(0);
        let cache_read =
            extract_u64_from_usage(usage, &["cacheReadInputTokens", "cache_read_input_tokens"])
                .unwrap_or(0);
        Some(
            input
                .saturating_add(output)
                .saturating_add(cache_creation)
                .saturating_add(cache_read),
        )
    })
}

fn extract_u64_from_usage(usage: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        usage.get(*key).and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
        })
    })
}
