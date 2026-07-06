use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    ProcessGetRequest, ProcessKillRequest, ProcessListRequest, ProcessLogReadRequest,
    ProcessManager, ProcessRunRequest, RuntimeError, RuntimeSessionManager, TeamCommsService,
    ToolGateway, ToolInvokeRequest, WorktreeService,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::{
    RuntimeProcessManager, TeamCommsConfig, WorktreeServiceConfig, GG_PROCESS_KILL,
    GG_PROCESS_LOGS, GG_PROCESS_RUN, GG_PROCESS_STATUS, GG_TEAM_MANAGE, GG_TEAM_MESSAGE,
    GG_TEAM_STATUS,
};

mod presets;
mod stubs;
mod team;
mod team_helpers;

pub use presets::{TeamMcpPolicy, TeamModelPreset};
pub use stubs::{StubTeamCommsService, StubWorktreeService};

#[cfg(test)]
pub(crate) use presets::default_team_model_presets as test_default_team_model_presets;
use presets::{default_team_model_presets, ManageAddIdempotencyEntry, ModelPresetCatalog};

pub struct RuntimeToolGateway {
    process_manager: Arc<RuntimeProcessManager>,
    runtime: Option<Arc<RuntimeSessionManager>>,
    team_comms: Arc<dyn TeamCommsService>,
    worktrees: Arc<dyn WorktreeService>,
    team_policy: TeamMcpPolicy,
    team_model_presets: ModelPresetCatalog,
    team_manage_add_idempotency: Arc<Mutex<HashMap<String, ManageAddIdempotencyEntry>>>,
}

pub struct RuntimeToolGatewayDeps {
    pub process_manager: Arc<RuntimeProcessManager>,
    pub runtime: Option<Arc<RuntimeSessionManager>>,
    pub team_comms: Arc<dyn TeamCommsService>,
    pub worktrees: Arc<dyn WorktreeService>,
    pub team_policy: TeamMcpPolicy,
    pub team_model_presets: Vec<TeamModelPreset>,
}

impl RuntimeToolGateway {
    pub fn new(deps: RuntimeToolGatewayDeps) -> Self {
        Self {
            process_manager: deps.process_manager,
            runtime: deps.runtime,
            team_comms: deps.team_comms,
            worktrees: deps.worktrees,
            team_policy: deps.team_policy,
            team_model_presets: ModelPresetCatalog::from_presets(deps.team_model_presets),
            team_manage_add_idempotency: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn process_only_for_tests(process_manager: Arc<RuntimeProcessManager>) -> Self {
        Self::new(RuntimeToolGatewayDeps {
            process_manager,
            runtime: None,
            team_comms: Arc::new(StubTeamCommsService::new(TeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 1_000,
            })),
            worktrees: Arc::new(StubWorktreeService::new(WorktreeServiceConfig {
                enabled: true,
                root_dir: String::new(),
                init_script_path: String::new(),
                deletion_policy_default: "delete_on_last_claim".to_string(),
            })),
            team_policy: TeamMcpPolicy::default(),
            team_model_presets: default_team_model_presets(),
        })
    }

    pub fn team_policy(&self) -> &TeamMcpPolicy {
        &self.team_policy
    }

    pub fn team_model_preset_names(&self) -> Vec<String> {
        self.team_model_presets.all_names()
    }

    async fn invoke_process_tool(&self, request: ToolInvokeRequest) -> Value {
        let tool_name = request.tool_name.trim();
        let args = match request.args {
            Value::Object(map) => map,
            _ => {
                return json!({
                    "ok": false,
                    "error": {
                        "code": "bad_request",
                        "message": "tool args must be an object"
                    }
                });
            }
        };

        let result = match tool_name {
            GG_PROCESS_RUN => {
                let command = args
                    .get("command")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                let cwd = args.get("cwd").and_then(Value::as_str).map(str::to_string);
                let timeout_ms = args.get("timeout_ms").and_then(Value::as_u64);
                self.process_manager
                    .run_process(ProcessRunRequest {
                        caller_session_id: Some(request.caller_session_id.clone()),
                        tool_call_id: request.invocation_id.clone(),
                        command,
                        cwd,
                        timeout_ms,
                    })
                    .await
                    .map(|value| json!(value))
            }
            GG_PROCESS_STATUS => {
                if let Some(process_id) = args.get("process_id").and_then(Value::as_str) {
                    self.process_manager
                        .get_process(ProcessGetRequest {
                            process_id: process_id.to_string(),
                            caller_session_id: Some(request.caller_session_id.clone()),
                        })
                        .await
                        .map(|value| json!(value))
                } else if let Some(pid) = args.get("pid").and_then(Value::as_i64) {
                    match self.process_manager.process_id_from_pid(pid).await {
                        Ok(process_id) => self
                            .process_manager
                            .get_process(ProcessGetRequest {
                                process_id,
                                caller_session_id: Some(request.caller_session_id.clone()),
                            })
                            .await
                            .map(|value| json!(value)),
                        Err(error) => Err(error),
                    }
                } else {
                    self.process_manager
                        .list_processes(ProcessListRequest {
                            caller_session_id: Some(request.caller_session_id.clone()),
                            include_completed: false,
                        })
                        .await
                        .map(|rows| json!({ "running": rows }))
                }
            }
            GG_PROCESS_LOGS => {
                let process_id = args
                    .get("process_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                let stream = args
                    .get("stream")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let head_lines = args
                    .get("head_lines")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize);
                let tail_lines = args
                    .get("tail_lines")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize);
                self.process_manager
                    .read_process_logs(ProcessLogReadRequest {
                        process_id,
                        caller_session_id: Some(request.caller_session_id.clone()),
                        stream,
                        head_lines,
                        tail_lines,
                        max_bytes: None,
                    })
                    .await
                    .map(|rows| json!({ "logs": rows }))
            }
            GG_PROCESS_KILL => {
                let process_id = args
                    .get("process_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default();
                self.process_manager
                    .kill_process(ProcessKillRequest {
                        process_id,
                        caller_session_id: Some(request.caller_session_id),
                        reason: Some("gg_process_kill".to_string()),
                    })
                    .await
                    .map(|value| json!(value))
            }
            _ => Err(RuntimeError::Unsupported(format!(
                "Unsupported gg_process tool: {tool_name}"
            ))),
        };

        match result {
            Ok(result) => json!({ "ok": true, "result": result }),
            Err(error) => json!({
                "ok": false,
                "error": {
                    "code": "tool_failed",
                    "message": error.to_string(),
                }
            }),
        }
    }
}

#[async_trait]
impl ToolGateway for RuntimeToolGateway {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        let _ = (&self.team_comms, &self.worktrees);
        self.process_manager.healthcheck().await
    }

    async fn invoke_tool(&self, request: ToolInvokeRequest) -> Result<Value, RuntimeError> {
        let caller_session_id = request.caller_session_id.trim();
        if caller_session_id.is_empty() {
            return Err(RuntimeError::InvalidState(
                "caller_session_id is required".to_string(),
            ));
        }

        if let Some(namespace) = request.namespace.as_deref() {
            if !namespace_matches_tool(namespace, request.tool_name.as_str()) {
                return Err(RuntimeError::InvalidState(
                    "namespace does not match tool_name".to_string(),
                ));
            }
        }

        if request.tool_name.starts_with("gg_process_") {
            return Ok(self.invoke_process_tool(request).await);
        }
        if request.tool_name.starts_with("gg_team_") {
            return Ok(self.invoke_team_tool(request).await);
        }

        Ok(json!({
            "ok": false,
            "error": {
                "code": "bad_request",
                "message": format!("Unsupported tool name: {}", request.tool_name),
            }
        }))
    }

    async fn capabilities(&self) -> Result<Value, RuntimeError> {
        let mut supported_namespaces = vec!["gg_process"];
        let mut tools = vec![
            GG_PROCESS_RUN,
            GG_PROCESS_STATUS,
            GG_PROCESS_LOGS,
            GG_PROCESS_KILL,
        ];
        if self.team_policy.enabled {
            supported_namespaces.push("gg_team");
            tools.extend([GG_TEAM_STATUS, GG_TEAM_MESSAGE, GG_TEAM_MANAGE]);
        }
        Ok(json!({
            "ok": true,
            "result": {
                "ggProcessEnabled": self.process_manager.config.enabled,
                "ggTeamEnabled": self.team_policy.enabled,
                "ggTeamManagePermissions": {
                    "nonLeadCanAddMembers": self.team_policy.non_lead_can_add_members,
                    "nonLeadCanRemoveMembers": self.team_policy.non_lead_can_remove_members,
                },
                "ggTeamModelPresets": self.team_model_presets.all_names(),
                "supportedNamespaces": supported_namespaces,
                "tools": tools,
            }
        }))
    }
}

pub(crate) fn namespace_matches_tool(namespace: &str, tool_name: &str) -> bool {
    match namespace.trim() {
        "gg_process" => tool_name.starts_with("gg_process_"),
        "gg_team" => tool_name.starts_with("gg_team_"),
        _ => false,
    }
}
