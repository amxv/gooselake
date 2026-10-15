//! Canonical workspace-scoped model tools. Legacy split-tool handlers remain
//! private to the operator migration surface, not model discovery.

use std::collections::HashSet;

use runtime_core::{
    normalized_json_hash, AgentBroadcastMessageRequest, AgentDirectMessageRequest,
    AgentToolMutationAdmission, AgentToolMutationCommand, ProcessGetRequest, ProcessKillRequest,
    ProcessListRequest, ProcessManager, ProcessRunRequest, ProviderThinkingEffort, RuntimeError,
    ToolInvokeRequest, WorkspaceAgentCreateRequest, WorkspaceAgentInitialRoute,
    WorkspaceAgentLifecycleState, WorkspaceAgentRebindRequest, WorkspaceMembershipPolicy,
    WorkspaceWorktreeCreateRequest, WorkspaceWorktreeInventoryEntry,
};
use serde::Deserialize;
use serde_json::{json, Value};

use super::team_helpers::status_state_for_session;
use super::RuntimeToolGateway;

const STATUS_MEMBER_LIMIT: usize = 100;
const STATUS_AVAILABLE_WORKTREE_LIMIT: usize = 20;

fn bounded_available_worktrees(
    inventory: &[WorkspaceWorktreeInventoryEntry],
) -> (Vec<WorkspaceWorktreeInventoryEntry>, usize, bool) {
    let mut eligible = inventory
        .iter()
        .filter(|entry| entry.eligible_for_assignment)
        .cloned()
        .collect::<Vec<_>>();
    eligible.sort_by(|one, two| {
        one.normalized_name
            .cmp(&two.normalized_name)
            .then(one.branch_name.cmp(&two.branch_name))
            .then(one.worktree_id.cmp(&two.worktree_id))
    });
    let total = eligible.len();
    eligible.truncate(STATUS_AVAILABLE_WORKTREE_LIMIT);
    (eligible, total, total > STATUS_AVAILABLE_WORKTREE_LIMIT)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum TeamMode {
    Status {},
    Add {
        title: Option<String>,
        prompt: Option<String>,
        image_paths: Option<Vec<String>>,
        model_preset: Option<String>,
        worktree_name: Option<String>,
        creator_compaction_subscription: Option<String>,
    },
    Remove {
        remove_agent_ids: Vec<String>,
        preserve_worktrees: Option<bool>,
    },
    AssignWorktree {
        assign_worktree: WorktreeAssignment,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorktreeAssignment {
    agent_id: String,
    worktree_name: String,
    preserve_previous_worktree: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum MessageMode {
    Direct {
        agent_id: String,
        message: String,
        image_paths: Option<Vec<String>>,
    },
    Broadcast {
        message: String,
        image_paths: Option<Vec<String>>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ProcessMode {
    Run {
        command: String,
        cwd: Option<String>,
        timeout_ms: Option<u64>,
    },
    List {},
    Status {
        process_id: String,
    },
    Cancel {
        process_id: String,
    },
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn worktree_status_is_stably_sorted_bounded_and_reports_full_eligible_count() {
        let inventory = (0..27)
            .rev()
            .map(|index| WorkspaceWorktreeInventoryEntry {
                worktree_id: format!("worktree_{index:02}"),
                revision: 1,
                normalized_name: format!("feature-{index:02}"),
                branch_name: format!("gg/feature-{index:02}"),
                worktree_path: format!("/repo/worktrees/feature-{index:02}"),
                retention_policy: "preserve".into(),
                lifecycle_state: "active".into(),
                routing_state: "available".into(),
                attached_agent_ids: Vec::new(),
                eligible_for_assignment: index != 4 && index != 9,
                eligibility_blockers: Vec::new(),
            })
            .collect::<Vec<_>>();
        let (items, count, truncated) = bounded_available_worktrees(&inventory);
        assert_eq!(count, 25);
        assert!(truncated);
        assert_eq!(items.len(), 20);
        assert_eq!(items[0].normalized_name, "feature-00");
        assert_eq!(items[4].normalized_name, "feature-05");
        assert_eq!(items[19].normalized_name, "feature-21");
        let (repeat, _, _) = bounded_available_worktrees(&inventory);
        assert_eq!(repeat, items);
    }

    #[test]
    fn status_and_list_modes_reject_forged_or_irrelevant_fields() {
        assert!(serde_json::from_value::<TeamMode>(json!({"mode":"status"})).is_ok());
        assert!(serde_json::from_value::<TeamMode>(json!({
            "mode":"status", "workspace_id":"another-workspace"
        }))
        .is_err());
        assert!(serde_json::from_value::<ProcessMode>(json!({"mode":"list"})).is_ok());
        assert!(serde_json::from_value::<ProcessMode>(json!({
            "mode":"list", "caller_agent_id":"other-agent"
        }))
        .is_err());
    }

    #[test]
    fn each_canonical_mode_accepts_its_own_minimal_payload() {
        for args in [
            json!({"mode":"status"}),
            json!({"mode":"add","title":"Worker","model_preset":"fast"}),
            json!({"mode":"remove","remove_agent_ids":["sess_peer"]}),
            json!({"mode":"assign_worktree","assign_worktree":{
                "agent_id":"sess_peer","worktree_name":"feature"
            }}),
        ] {
            assert!(
                serde_json::from_value::<TeamMode>(args.clone()).is_ok(),
                "{args}"
            );
        }
        for args in [
            json!({"mode":"direct","agent_id":"sess_peer","message":"Hi"}),
            json!({"mode":"broadcast","message":"Hi all","image_paths":["/tmp/1.png"]}),
        ] {
            assert!(
                serde_json::from_value::<MessageMode>(args.clone()).is_ok(),
                "{args}"
            );
        }
        for args in [
            json!({"mode":"run","command":"echo hi","timeout_ms":1000}),
            json!({"mode":"list"}),
            json!({"mode":"status","process_id":"proc_1"}),
            json!({"mode":"cancel","process_id":"proc_1"}),
        ] {
            assert!(
                serde_json::from_value::<ProcessMode>(args.clone()).is_ok(),
                "{args}"
            );
        }
    }

    #[test]
    fn cross_mode_fields_and_identity_spoofing_fail_closed() {
        for args in [
            json!({"mode":"status","team_id":"legacy"}),
            json!({"mode":"add","caller_agent_id":"sess_forged"}),
            json!({"mode":"add","workspace_id":"ws_forged"}),
            json!({"mode":"remove","remove_agent_ids":["sess_peer"],"prompt":"hi"}),
            json!({"mode":"remove","agent_id":"sess_peer"}),
            json!({"mode":"assign_worktree","assign_worktree":{
                "agent_id":"sess_peer","worktree_name":"x","workspace_id":"forged"
            }}),
            json!({"mode":"set_lead","agent_id":"sess_peer"}),
        ] {
            assert!(
                serde_json::from_value::<TeamMode>(args.clone()).is_err(),
                "{args}"
            );
        }
        for args in [
            json!({"mode":"direct","recipient_agent_id":"sess_peer","message":"Hi"}),
            json!({"mode":"direct","agent_id":"sess_peer","message":"Hi","sender_agent_id":"forged"}),
            json!({"mode":"broadcast","message":"Hi","agent_id":"sess_peer"}),
            json!({"mode":"broadcast","message":"Hi","team_id":"legacy"}),
            json!({"mode":"direct","agent_id":"sess_peer","message":"Hi","image_paths":"/tmp/a.png"}),
        ] {
            assert!(
                serde_json::from_value::<MessageMode>(args.clone()).is_err(),
                "{args}"
            );
        }
        for args in [
            json!({"mode":"run","command":"echo hi","caller_agent_id":"forged"}),
            json!({"mode":"run","process_id":"proc_1"}),
            json!({"mode":"list","include_completed":true}),
            json!({"mode":"status","pid":123}),
            json!({"mode":"cancel","command":"kill proc_1"}),
            json!({"mode":"logs","process_id":"proc_1"}),
        ] {
            assert!(
                serde_json::from_value::<ProcessMode>(args.clone()).is_err(),
                "{args}"
            );
        }
    }
}

fn nonempty(value: &str, field: &str) -> Result<(), RuntimeError> {
    if value.trim().is_empty() {
        return Err(RuntimeError::InvalidState(format!(
            "{field} must be nonempty"
        )));
    }
    Ok(())
}

fn image_count_valid(paths: &[String]) -> Result<(), RuntimeError> {
    if paths.len() > 8 || paths.iter().any(|path| path.trim().is_empty()) {
        return Err(RuntimeError::InvalidState(
            "image_paths must contain at most 8 nonempty paths".into(),
        ));
    }
    Ok(())
}

fn error_envelope(error: RuntimeError) -> Value {
    let code = match &error {
        RuntimeError::NotFound(_) => "not_found",
        RuntimeError::InvalidState(_) => "bad_request",
        RuntimeError::Conflict(_) => "conflict",
        RuntimeError::Unsupported(_) => "unsupported",
        RuntimeError::ProviderDispatch { outcome, .. } if *outcome == "unknown" => {
            "outcome_unresolved"
        }
        RuntimeError::ProtocolViolation(_) => "provider_error",
        _ => "tool_failed",
    };
    json!({"ok": false, "error": {
        "code": code,
        "message": error.to_string(),
        "retryable": false,
    }})
}

fn invalid_schema(error: serde_json::Error) -> Value {
    json!({"ok": false, "error": {
        "code": "bad_request", "message": format!("invalid tool arguments: {error}"), "retryable": false
    }})
}

fn unresolved_team_operation(operation_id: &str) -> Value {
    json!({"ok":false,"error":{
        "code":"outcome_unresolved",
        "message":"The workspace mutation may have been applied, but its durable receipt is not available. Do not repeat it; inspect workspace status or request operator recovery.",
        "retryable":false,
        "details":{"operation_id":operation_id}
    }})
}

impl RuntimeToolGateway {
    pub(super) async fn invoke_canonical_tool(&self, request: ToolInvokeRequest) -> Value {
        let caller = request.caller_session_id.as_str();
        // Coordinating operations for the same agent are serialized at the
        // authoritative gateway, not just in individual sidecar processes.
        let coordination_guard = if matches!(request.tool_name.as_str(), "gg_team" | "gg_message") {
            let gate = {
                let mut gates = self.canonical_coordination_locks.lock().await;
                gates
                    .entry(caller.to_string())
                    .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
                    .clone()
            };
            Some(gate.lock_owned().await)
        } else {
            None
        };
        // A retried native tool RPC may carry the exact same trusted host ID.
        // Admit the effect once *before* Git/provider/archive work, and replay
        // only a durable successful receipt. An unreceipted execution after
        // crash or transport loss is intentionally outcome_unresolved.
        let mut admitted_team_operation = None;
        if request.tool_name == "gg_team"
            && self.team_policy.enabled
            && request.args.get("mode").and_then(Value::as_str) != Some("status")
        {
            if let Err(error) = serde_json::from_value::<TeamMode>(request.args.clone()) {
                return invalid_schema(error);
            }
            let Some(invocation_id) = request
                .invocation_id
                .as_deref()
                .filter(|id| !id.trim().is_empty() && id.trim() == *id && id.len() <= 128)
            else {
                return json!({"ok":false,"error":{"code":"missing_invocation_id",
                    "message":"mutating workspace tools require host invocation identity",
                    "retryable":false}});
            };
            let agent = match self.canonical_caller(caller) {
                Ok(agent) => agent,
                Err(error) => return error_envelope(error),
            };
            let normalized_request = json!({"tool_name":"gg_team","args":request.args.clone()});
            let request_hash = match normalized_json_hash(&normalized_request) {
                Ok(hash) => hash,
                Err(error) => return error_envelope(error),
            };
            let admission =
                runtime_core_store(self).begin_agent_tool_mutation(&AgentToolMutationCommand {
                    operation_id: format!("op_{:032x}", rand::random::<u128>()),
                    workspace_id: agent.workspace_id,
                    caller_agent_id: caller.into(),
                    idempotency_key: invocation_id.into(),
                    tool_name: "gg_team".into(),
                    normalized_request_hash: request_hash,
                    normalized_request,
                    admitted_at: crate::now_ms(),
                });
            match admission {
                Ok(AgentToolMutationAdmission::Execute { operation_id }) => {
                    admitted_team_operation = Some(operation_id);
                }
                Ok(AgentToolMutationAdmission::Replay { exact_result }) => return exact_result,
                Ok(AgentToolMutationAdmission::Unresolved { operation_id }) => {
                    return unresolved_team_operation(&operation_id);
                }
                Err(error) => return error_envelope(error),
            }
        }
        let result = match request.tool_name.as_str() {
            "gg_team" if !self.team_policy.enabled => {
                return json!({"ok":false,"error":{"code":"feature_disabled","message":"workspace tools are disabled","retryable":false}});
            }
            "gg_message" if !self.team_policy.enabled => {
                return json!({"ok":false,"error":{"code":"feature_disabled","message":"workspace messages are disabled","retryable":false}});
            }
            "gg_team" => match serde_json::from_value::<TeamMode>(request.args) {
                Ok(mode) => {
                    self.canonical_team(caller, mode, request.invocation_id)
                        .await
                }
                Err(error) => return invalid_schema(error),
            },
            "gg_message" => match serde_json::from_value::<MessageMode>(request.args) {
                Ok(mode) => {
                    self.canonical_message(caller, mode, request.invocation_id)
                        .await
                }
                Err(error) => return invalid_schema(error),
            },
            "gg_process" => match serde_json::from_value::<ProcessMode>(request.args) {
                Ok(mode) => {
                    self.canonical_process(caller, mode, request.invocation_id)
                        .await
                }
                Err(error) => return invalid_schema(error),
            },
            _ => Err(RuntimeError::Unsupported("unknown GG tool".into())),
        };
        let envelope = match result {
            Ok(result) => json!({"ok": true, "result": result}),
            Err(error) => error_envelope(error),
        };
        if let Some(operation_id) = admitted_team_operation {
            if envelope["ok"] == true {
                if runtime_core_store(self)
                    .finish_agent_tool_mutation(&operation_id, &envelope, crate::now_ms())
                    .is_err()
                {
                    return unresolved_team_operation(&operation_id);
                }
            }
        }
        // Hold the same-caller serialization gate through durable receipt
        // commit, otherwise a racing identical invocation can see a pending
        // intent between effect completion and receipt persistence.
        drop(coordination_guard);
        envelope
    }

    fn canonical_caller(
        &self,
        caller: &str,
    ) -> Result<runtime_core::WorkspaceAgentRecord, RuntimeError> {
        self.runtime
            .as_ref()
            .ok_or_else(|| RuntimeError::Unsupported("workspace authority unavailable".into()))?
            .get_active_workspace_agent_by_id(caller)
    }

    fn membership_policy(&self) -> WorkspaceMembershipPolicy {
        WorkspaceMembershipPolicy {
            non_lead_can_add_members: self.team_policy.non_lead_can_add_members,
            non_lead_can_remove_members: self.team_policy.non_lead_can_remove_members,
        }
    }

    async fn canonical_team(
        &self,
        caller: &str,
        mode: TeamMode,
        invocation_id: Option<String>,
    ) -> Result<Value, RuntimeError> {
        let agent = self.canonical_caller(caller)?;
        let runtime = self.runtime.as_ref().expect("caller checked runtime");
        let workspace_id = &agent.workspace_id;
        match mode {
            TeamMode::Status {} => {
                let store = runtime_core_store(self);
                let workspace = store
                    .get_workspace(workspace_id)?
                    .ok_or_else(|| RuntimeError::NotFound("workspace".into()))?;
                let mut members = runtime.list_workspace_agents(
                    workspace_id,
                    Some(WorkspaceAgentLifecycleState::Active),
                )?;
                members.sort_by(|one, two| {
                    one.alias
                        .cmp(&two.alias)
                        .then(one.agent_id.cmp(&two.agent_id))
                });
                let member_count = members.len();
                // Status uses the current provider-bound context observation;
                // missing or stale observations remain explicitly unknown.
                // Optional Git inventory must not make roster status fail.
                let (inventory, inventory_error) =
                    match runtime.list_workspace_worktree_inventory(workspace_id) {
                        Ok(inventory) => (inventory.worktrees, None),
                        Err(error) => (Vec::new(), Some(error.to_string())),
                    };
                let (eligible, eligible_total, eligible_truncated) =
                    bounded_available_worktrees(&inventory);
                let lead_alias = workspace.lead_agent_id.as_ref().and_then(|id| {
                    members
                        .iter()
                        .find(|member| &member.agent_id == id)
                        .map(|member| member.alias.clone())
                });
                let mut rows = Vec::with_capacity(member_count.min(STATUS_MEMBER_LIMIT));
                for member in members.iter().take(STATUS_MEMBER_LIMIT) {
                    let session = runtime.get_session(&member.agent_id).await.ok();
                    let latest_context = store.get_session_context_limit(&member.agent_id)?;
                    let remaining = latest_context
                        .as_ref()
                        .filter(|snapshot| {
                            snapshot.provider_session_ref
                                == member.provider_session_ref.clone().unwrap_or_default()
                                && snapshot.agent_revision == member.revision
                        })
                        .map(|snapshot| snapshot.observation.remaining_percentage);
                    let is_lead =
                        workspace.lead_agent_id.as_deref() == Some(member.agent_id.as_str());
                    let worktree = inventory
                        .iter()
                        .find(|entry| {
                            entry.worktree_path == member.recreation_policy.authoritative_cwd
                                && entry.attached_agent_ids.contains(&member.agent_id)
                        })
                        .map(|entry| {
                            json!({
                                "name": entry.normalized_name,
                                "branch": entry.branch_name,
                                "state": entry.routing_state,
                            })
                        });
                    let added_by_alias = if member.profile.added_by == "runtime_operator" {
                        Some("user")
                    } else {
                        members
                            .iter()
                            .find(|creator| creator.agent_id == member.profile.added_by)
                            .map(|creator| creator.alias.as_str())
                    };
                    rows.push(json!({
                        "agent_id": member.agent_id, "alias":member.alias,
                        "title":member.profile.title,
                        "title_provenance":member.profile.title_provenance,
                        "is_lead":is_lead,
                        "role":if is_lead {"lead"} else {"member"},
                        "state":status_state_for_session(session.as_ref()),
                        "last_activity_at_ms":member.updated_at.max(session.as_ref().map_or(0, |s| s.updated_at)),
                        "context_window_remaining_percentage":remaining,
                        "context_remaining_pct":remaining,
                        "worktree_cwd":member.recreation_policy.authoritative_cwd,
                        "worktree":worktree,
                        "added_by":member.profile.added_by,
                        "added_by_alias":added_by_alias,
                    }));
                }
                Ok(json!({
                    "workspace_id":workspace_id,
                    "lead_agent_id":workspace.lead_agent_id,
                    "lead_alias":lead_alias,
                    "leadless":workspace.lead_agent_id.is_none(),
                    "members":rows,
                    "members_total":member_count,
                    "members_truncated":member_count > STATUS_MEMBER_LIMIT,
                    "available_worktrees":eligible,
                    "available_worktrees_total":eligible_total,
                    "available_worktrees_truncated":eligible_truncated,
                    "available_worktrees_error":inventory_error,
                }))
            }
            TeamMode::Add {
                title,
                prompt,
                image_paths,
                model_preset,
                worktree_name,
                creator_compaction_subscription,
            } => {
                runtime.authorize_workspace_membership(
                    workspace_id,
                    caller,
                    runtime_core::WorkspaceMembershipAction::Add,
                    self.membership_policy(),
                )?;
                if let Some(value) = title.as_deref() {
                    nonempty(value, "title")?;
                }
                if let Some(value) = prompt.as_deref() {
                    nonempty(value, "prompt")?;
                }
                let images = image_paths.unwrap_or_default();
                image_count_valid(&images)?;
                if let Some(value) = creator_compaction_subscription.as_deref() {
                    if !matches!(value, "auto" | "unsubscribed") {
                        return Err(RuntimeError::InvalidState(
                            "creator_compaction_subscription must be auto or unsubscribed".into(),
                        ));
                    }
                }
                let preset = match model_preset.as_deref() {
                    Some(name) => {
                        Some(self.team_model_presets.resolve(name).ok_or_else(|| {
                            RuntimeError::NotFound(format!("model preset {name}"))
                        })?)
                    }
                    None => None,
                };
                let provider = preset
                    .as_ref()
                    .map_or(agent.recreation_policy.provider, |p| p.provider);
                if !images.is_empty() && provider == runtime_core::ProviderKind::Acp {
                    return Err(RuntimeError::Unsupported(
                        "ACP onboarding images are not supported".into(),
                    ));
                }
                let route = match worktree_name {
                    Some(name) => {
                        nonempty(&name, "worktree_name")?;
                        let inventory = runtime.list_workspace_worktree_inventory(workspace_id)?;
                        match inventory
                            .worktrees
                            .into_iter()
                            .find(|item| item.normalized_name == name)
                        {
                            Some(existing) if existing.eligible_for_assignment => {
                                Some(WorkspaceAgentInitialRoute::Existing {
                                    worktree_id: existing.worktree_id,
                                })
                            }
                            Some(_) => {
                                return Err(RuntimeError::Conflict(
                                    "named worktree is not eligible for assignment".into(),
                                ))
                            }
                            None => {
                                let created = self
                                    .worktrees
                                    .create_workspace_worktree(WorkspaceWorktreeCreateRequest {
                                        workspace_id: workspace_id.clone(),
                                        worktree_name: name,
                                        branch_prefix: None,
                                        base_ref: None,
                                        deletion_policy: None,
                                        run_init_script: None,
                                    })
                                    .await?;
                                Some(WorkspaceAgentInitialRoute::Existing {
                                    worktree_id: created.worktree.id,
                                })
                            }
                        }
                    }
                    None => Some(WorkspaceAgentInitialRoute::Root),
                };
                let mut preferences = agent.recreation_policy.current_preferences.clone();
                if let Some(preset) = preset.as_ref().and_then(|p| p.thinking_effort.as_deref()) {
                    preferences.thinking_effort = Some(ProviderThinkingEffort::parse(preset)?);
                }
                let onboarding_prompt = prompt.clone();
                let onboarding_images = images.clone();
                let new_agent = runtime.create_workspace_agent_as_member(workspace_id,
                    WorkspaceAgentCreateRequest {
                        provider,model:preset.as_ref().map(|p|p.model.clone()).or_else(||agent.recreation_policy.model.clone()),
                        permission_intent:agent.recreation_policy.permission_intent.clone(),
                        setting_sources_intent:agent.recreation_policy.setting_sources_intent.clone(),
                        current_preferences:preferences,
                        system_prompt:agent.recreation_policy.system_prompt.clone(),
                        allowed_tools:agent.recreation_policy.allowed_tools.clone(),
                        disallowed_tools:agent.recreation_policy.disallowed_tools.clone(),
                        cwd:None,worktree:route,harness_version_slot:agent.recreation_policy.harness_version_slot.clone(),
                        title,
                        metadata:Some(json!({"onboarding":{"prompt":prompt,"image_paths":images,"creator_compaction_subscription":creator_compaction_subscription.unwrap_or_else(||"auto".into())}})),
                    },caller,self.membership_policy()).await?;
                let onboarding = if onboarding_prompt.is_some() || !onboarding_images.is_empty() {
                    let text = onboarding_prompt.unwrap_or_else(|| {
                        format!(
                            "You have joined workspace {} as {}.",
                            workspace_id, new_agent.alias
                        )
                    });
                    let message = self.team_comms.send_agent_direct(AgentDirectMessageRequest {
                        sender_agent_id:caller.to_string(),
                        recipient_agent_id:new_agent.agent_id.clone(),
                        input:json!([{"type":"text","text":text}]),
                        image_paths:onboarding_images,
                        priority:"normal".into(),
                        policy:"non_interrupting".into(),
                        correlation_id:None,
                        reply_to_message_id:None,
                        idempotency_key:None,
                    }).await.map_err(|error| RuntimeError::provider_dispatch_unknown(
                        "onboarding_outcome_unresolved",
                        format!("workspace agent {} was created, but its onboarding delivery could not be confirmed: {error}; inspect workspace status, do not retry add",new_agent.agent_id),
                    ))?;
                    Some(
                        json!({"message_id":message.message.id,"disposition":message.disposition,"deliveries":message.deliveries}),
                    )
                } else {
                    None
                };
                Ok(
                    json!({"operation":"add","agent_id":new_agent.agent_id,"alias":new_agent.alias,
                    "workspace_id":new_agent.workspace_id,"onboarding":onboarding}),
                )
            }
            TeamMode::Remove {
                remove_agent_ids,
                preserve_worktrees,
            } => {
                runtime.authorize_workspace_membership(
                    workspace_id,
                    caller,
                    runtime_core::WorkspaceMembershipAction::Remove,
                    self.membership_policy(),
                )?;
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
                if preserve_worktrees == Some(false) {
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
                for agent_id in &remove_agent_ids {
                    let target = runtime.get_workspace_agent(workspace_id, agent_id)?;
                    if target.lifecycle_state != WorkspaceAgentLifecycleState::Active {
                        return Err(RuntimeError::Conflict(format!(
                            "agent {agent_id} is not active"
                        )));
                    }
                }
                let mut removed = Vec::new();
                for agent_id in remove_agent_ids {
                    let target = runtime
                        .archive_workspace_agent_as_member(
                            workspace_id,
                            &agent_id,
                            caller,
                            Some("gg_team_remove"),
                            self.membership_policy(),
                        )
                        .await?;
                    removed.push(json!({"agent_id":target.agent_id,"alias":target.alias,"lifecycle":target.lifecycle_state}));
                }
                Ok(json!({"operation":"remove","removed":removed,"preserve_worktrees":true}))
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
                runtime.authorize_workspace_membership(
                    workspace_id,
                    caller,
                    runtime_core::WorkspaceMembershipAction::Add,
                    self.membership_policy(),
                )?;
                let target =
                    runtime.get_workspace_agent(workspace_id, &assign_worktree.agent_id)?;
                let inventory = runtime.list_workspace_worktree_inventory(workspace_id)?;
                let worktree_id = inventory
                    .worktrees
                    .into_iter()
                    .find(|entry| {
                        entry.normalized_name == assign_worktree.worktree_name
                            && entry.eligible_for_assignment
                    })
                    .map(|entry| entry.worktree_id)
                    .ok_or_else(|| RuntimeError::NotFound("eligible worktree name".into()))?;
                let response = runtime
                    .rebind_workspace_agent(
                        workspace_id,
                        &target.agent_id,
                        WorkspaceAgentRebindRequest {
                            worktree_id: Some(worktree_id),
                            expected_revision: target.revision,
                            cleanup_previous_worktree: false,
                        },
                        invocation_id,
                    )
                    .await?;
                Ok(
                    json!({"operation":"assign_worktree","agent_id":target.agent_id,"rebind":response}),
                )
            }
        }
    }

    async fn canonical_message(
        &self,
        caller: &str,
        mode: MessageMode,
        invocation_id: Option<String>,
    ) -> Result<Value, RuntimeError> {
        self.canonical_caller(caller)?;
        let ack = match mode {
            MessageMode::Direct {
                agent_id,
                message,
                image_paths,
            } => {
                nonempty(&agent_id, "agent_id")?;
                nonempty(&message, "message")?;
                let images = image_paths.unwrap_or_default();
                image_count_valid(&images)?;
                // The public model may address peers by their durable friendly
                // alias. Resolve under indexed store authority, never assume
                // that a display alias is the opaque session credential.
                let recipient = runtime_core_store(self)
                    .get_workspace_agent_by_alias(agent_id.as_str())?
                    .map(|profile| profile.agent_id)
                    .unwrap_or(agent_id);
                self.team_comms
                    .send_agent_direct(AgentDirectMessageRequest {
                        sender_agent_id: caller.into(),
                        recipient_agent_id: recipient,
                        input: json!([{"type":"text","text":message}]),
                        image_paths: images,
                        priority: "normal".into(),
                        policy: "non_interrupting".into(),
                        correlation_id: None,
                        reply_to_message_id: None,
                        idempotency_key: invocation_id,
                    })
                    .await?
            }
            MessageMode::Broadcast {
                message,
                image_paths,
            } => {
                nonempty(&message, "message")?;
                let images = image_paths.unwrap_or_default();
                image_count_valid(&images)?;
                self.team_comms
                    .broadcast_workspace(AgentBroadcastMessageRequest {
                        sender_agent_id: caller.into(),
                        input: json!([{"type":"text","text":message}]),
                        image_paths: images,
                        priority: "normal".into(),
                        policy: "non_interrupting".into(),
                        correlation_id: None,
                        idempotency_key: invocation_id,
                    })
                    .await?
            }
        };
        Ok(
            json!({"message_id":ack.message.id,"scope":ack.message.scope,"recipient_count":ack.deliveries.len(),"disposition":ack.disposition}),
        )
    }

    async fn canonical_process(
        &self,
        caller: &str,
        mode: ProcessMode,
        invocation_id: Option<String>,
    ) -> Result<Value, RuntimeError> {
        if !self.process_manager.config.enabled {
            return Err(RuntimeError::Unsupported(
                "managed processes are disabled".into(),
            ));
        }
        self.canonical_caller(caller)?;
        match mode {
            ProcessMode::Run {
                command,
                cwd,
                timeout_ms,
            } => {
                nonempty(&command, "command")?;
                if let Some(cwd) = cwd.as_deref() {
                    nonempty(cwd, "cwd")?;
                }
                Ok(json!(
                    self.process_manager
                        .run_process(ProcessRunRequest {
                            caller_session_id: Some(caller.into()),
                            tool_call_id: invocation_id,
                            command,
                            cwd,
                            timeout_ms
                        })
                        .await?
                ))
            }
            ProcessMode::List {} => Ok(
                json!({"running":self.process_manager.list_processes(ProcessListRequest{caller_session_id:Some(caller.into()),include_completed:false}).await?}),
            ),
            ProcessMode::Status { process_id } => {
                nonempty(&process_id, "process_id")?;
                Ok(json!(
                    self.process_manager
                        .get_process(ProcessGetRequest {
                            process_id,
                            caller_session_id: Some(caller.into())
                        })
                        .await?
                ))
            }
            ProcessMode::Cancel { process_id } => {
                nonempty(&process_id, "process_id")?;
                Ok(json!(
                    self.process_manager
                        .kill_process(ProcessKillRequest {
                            process_id,
                            caller_session_id: Some(caller.into()),
                            reason: Some("gg_process_cancel".into())
                        })
                        .await?
                ))
            }
        }
    }
}

fn runtime_core_store(gateway: &RuntimeToolGateway) -> &dyn runtime_core::RuntimeStore {
    // The model gateway reads authority through the shared durable store; the
    // runtime manager never exports its internal mutable session state.
    gateway.process_manager.store.as_ref()
}
