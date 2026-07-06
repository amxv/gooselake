use std::{collections::HashMap, sync::Arc};

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_router,
};
use serde_json::{Value, json};

use crate::{
    constants::GG_MCP_CALLER_AGENT_ID_ARG_KEY,
    envelope::{
        annotate_envelope_with_caller_agent_id, build_ping_payload, build_team_manage_description,
        envelope_to_call_tool_result,
    },
    gateway::{
        GatewayClient, GatewayClientConfig, TeamModelPresetCapabilitySnapshot,
        gateway_not_configured_envelope, normalize_non_empty, process_tools_enabled_from_env,
    },
    tool_params::{
        GgMarkdownOpenRequest, GgProcessKillRequest, GgProcessRunRequest, GgProcessStatusRequest,
        GgTeamManageRequest, GgTeamMessageRequest, GgTeamStatusRequest, ToolCallMetadata,
    },
};

#[derive(Clone)]
pub(crate) struct GgMcpServer {
    tool_router: ToolRouter<Self>,
    gateway_client_config: Option<GatewayClientConfig>,
    gateway_client: Arc<tokio::sync::RwLock<Option<GatewayClient>>>,
    process_tools_enabled: bool,
    team_call_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    team_model_presets_cache: Arc<tokio::sync::RwLock<Option<TeamModelPresetCapabilitySnapshot>>>,
}

#[tool_router]
impl GgMcpServer {
    pub(crate) fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
            gateway_client_config: GatewayClientConfig::from_env(),
            gateway_client: Arc::new(tokio::sync::RwLock::new(None)),
            process_tools_enabled: process_tools_enabled_from_env(),
            team_call_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            team_model_presets_cache: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    #[tool(description = "Return GG MCP server health and runtime metadata.")]
    async fn gg_ping(&self) -> Result<CallToolResult, McpError> {
        Ok(envelope_to_call_tool_result(build_ping_payload()))
    }

    #[tool(
        description = "Get current status for each team member, including activity state, latest team-message context, backend-observed context window remaining percentage, managed-worktree metadata (`worktree_cwd`/`worktree_name`), and `added_by` provenance."
    )]
    async fn gg_team_status(
        &self,
        params: Parameters<GgTeamStatusRequest>,
    ) -> Result<CallToolResult, McpError> {
        Ok(self
            .invoke_backend_tool_with_metadata(
                "gg_team",
                "gg_team_status",
                &params.0,
                &params.0.tool_call_metadata,
            )
            .await)
    }

    #[tool(
        description = "Send a team-scoped message. Set `recipient_agent_id` to `broadcast` to message the whole team. Optional `image_paths` attaches images from disk in the provided order after the text message. Format `message` using markdown when helpful (line breaks, lists, code blocks)."
    )]
    async fn gg_team_message(
        &self,
        params: Parameters<GgTeamMessageRequest>,
    ) -> Result<CallToolResult, McpError> {
        Ok(self
            .invoke_backend_tool_with_metadata(
                "gg_team",
                "gg_team_message",
                &params.0,
                &params.0.tool_call_metadata,
            )
            .await)
    }

    #[tool(
        description = "Manage team membership. Provide `remove_agent_ids` (array format, for example [\"agent_1\", \"agent_2\"]) to remove existing members. Omit `remove_agent_ids` to add one member with optional `title`, optional `prompt`, optional `image_paths`, optional `model_preset`, optional `creator_compaction_subscription`, optional `worktree_name`, and optional `use_existing_worktree`. For add calls, onboarding instructions from `prompt` are delivered as a canonical direct TeamMessage to the spawned member. `creator_compaction_subscription` defaults to `auto`; set `unsubscribed` to suppress creator-recipient compaction notices for the new member. When `worktree_name` is provided, native branch/worktree/cwd derivation is runtime-owned and `pre_teammate_add` hook `spawn_template_mutation.cwd` is ignored. `post_teammate_remove` hooks run for both `agent_tool` and `ui_command` removals."
    )]
    async fn gg_team_manage(
        &self,
        params: Parameters<GgTeamManageRequest>,
    ) -> Result<CallToolResult, McpError> {
        Ok(self
            .invoke_backend_tool_with_metadata(
                "gg_team",
                "gg_team_manage",
                &params.0,
                &params.0.tool_call_metadata,
            )
            .await)
    }

    #[tool(
        description = "Open a markdown document in GG's native markdown viewer. `path` may be absolute or relative to the resolved target session/worktree cwd. Use `target_agent_id` to target another session by runtime session id or agent alias. Use `branch` to target an active managed worktree branch name. Optionally include either `line` or `anchor` to jump within the document."
    )]
    async fn gg_markdown_open(
        &self,
        params: Parameters<GgMarkdownOpenRequest>,
    ) -> Result<CallToolResult, McpError> {
        Ok(self
            .invoke_backend_tool_with_metadata(
                "gg_markdown",
                "gg_markdown_open",
                &params.0,
                &params.0.tool_call_metadata,
            )
            .await)
    }

    #[tool(
        description = "Run a shell command in the background and return immediately with pid and process metadata."
    )]
    async fn gg_process_run(
        &self,
        params: Parameters<GgProcessRunRequest>,
    ) -> Result<CallToolResult, McpError> {
        Ok(self
            .invoke_backend_process_tool("gg_process_run", &params.0, &params.0.tool_call_metadata)
            .await)
    }

    #[tool(
        description = "Inspect a background process by pid or list all running processes for the caller session."
    )]
    async fn gg_process_status(
        &self,
        params: Parameters<GgProcessStatusRequest>,
    ) -> Result<CallToolResult, McpError> {
        Ok(self
            .invoke_backend_process_tool(
                "gg_process_status",
                &params.0,
                &params.0.tool_call_metadata,
            )
            .await)
    }

    #[tool(description = "Kill a background process started by the caller session.")]
    async fn gg_process_kill(
        &self,
        params: Parameters<GgProcessKillRequest>,
    ) -> Result<CallToolResult, McpError> {
        Ok(self
            .invoke_backend_process_tool("gg_process_kill", &params.0, &params.0.tool_call_metadata)
            .await)
    }
}

impl GgMcpServer {
    async fn tools_with_runtime_metadata(&self) -> Vec<Tool> {
        let mut tools = self.tool_router.list_all();

        if !self.process_tools_enabled {
            tools.retain(|tool| !tool.name.as_ref().starts_with("gg_process_"));
        }

        self.refresh_team_capabilities_cache_for_list().await;
        if self
            .team_model_presets_cache
            .read()
            .await
            .as_ref()
            .is_some_and(|snapshot| !snapshot.team_tools_enabled)
        {
            tools.retain(|tool| !tool.name.as_ref().starts_with("gg_team_"));
            return tools;
        }
        let model_presets = self.resolve_team_model_presets().await;
        if let Some(tool) = tools
            .iter_mut()
            .find(|tool| tool.name.as_ref() == "gg_team_manage")
        {
            tool.description = Some(build_team_manage_description(&model_presets).into());
            inject_team_manage_model_preset_enum_schema(tool, model_presets.as_slice());
        }

        tools
    }

    async fn resolve_team_model_presets(&self) -> Vec<String> {
        self.team_model_presets_cache
            .read()
            .await
            .clone()
            .map(|snapshot| snapshot.presets)
            .unwrap_or_default()
    }

    async fn refresh_team_capabilities_cache_for_list(&self) {
        let Some(gateway_client) = self.get_or_init_gateway_client().await else {
            return;
        };

        let Ok(fetched_snapshot) = gateway_client.fetch_team_model_presets().await else {
            return;
        };

        let mut cache = self.team_model_presets_cache.write().await;
        if cache.as_ref() != Some(&fetched_snapshot) {
            *cache = Some(fetched_snapshot);
        }
    }

    async fn get_or_init_gateway_client(&self) -> Option<GatewayClient> {
        if let Some(existing) = self.gateway_client.read().await.clone() {
            return Some(existing);
        }

        let config = self.gateway_client_config.clone()?;

        let mut gateway_client_guard = self.gateway_client.write().await;
        if let Some(existing) = gateway_client_guard.clone() {
            return Some(existing);
        }

        let initialized = GatewayClient::from_config(config);
        *gateway_client_guard = Some(initialized.clone());
        Some(initialized)
    }

    fn resolve_caller_agent_id(
        &self,
        tool_call_metadata: &ToolCallMetadata,
    ) -> Result<String, Value> {
        if let Some(caller_agent_id) =
            normalize_non_empty(tool_call_metadata.caller_agent_id.as_deref())
        {
            return Ok(caller_agent_id);
        }

        if let Some(default_caller_agent_id) = self
            .gateway_client_config
            .as_ref()
            .and_then(|config| normalize_non_empty(config.default_caller_agent_id.as_deref()))
        {
            return Ok(default_caller_agent_id);
        }

        let caller_required = self
            .gateway_client_config
            .as_ref()
            .map(|config| config.require_tool_caller_agent_id)
            .unwrap_or(false);
        let code = if caller_required {
            "unauthorized"
        } else {
            "backend_unavailable"
        };
        let message = if caller_required {
            format!(
                "Missing required {GG_MCP_CALLER_AGENT_ID_ARG_KEY} tool argument for caller identity"
            )
        } else {
            "GG MCP caller identity is not configured".to_string()
        };

        Err(json!({
            "ok": false,
            "error": {
                "code": code,
                "message": message,
                "details": {
                    "required_tool_argument": GG_MCP_CALLER_AGENT_ID_ARG_KEY,
                    "fallback_env": "GG_MCP_CALLER_AGENT_ID",
                }
            }
        }))
    }

    async fn acquire_team_call_guard(
        &self,
        caller_agent_id: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let caller_key = normalize_non_empty(Some(caller_agent_id))
            .unwrap_or_else(|| "unknown_caller".to_string());
        let caller_lock = {
            let mut locks = self.team_call_locks.lock().await;
            locks
                .entry(caller_key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        caller_lock.lock_owned().await
    }

    async fn invoke_backend_tool_with_metadata<T: serde::Serialize + ?Sized>(
        &self,
        namespace: &str,
        tool_name: &str,
        params: &T,
        tool_call_metadata: &ToolCallMetadata,
    ) -> CallToolResult {
        if self.gateway_client_config.is_none() {
            return envelope_to_call_tool_result(gateway_not_configured_envelope());
        }

        let caller_agent_id = match self.resolve_caller_agent_id(tool_call_metadata) {
            Ok(caller_agent_id) => caller_agent_id,
            Err(envelope) => return envelope_to_call_tool_result(envelope),
        };
        let invocation_id = normalize_non_empty(tool_call_metadata.invocation_id.as_deref());

        self.invoke_backend_tool(
            namespace,
            tool_name,
            params,
            caller_agent_id.as_str(),
            invocation_id.as_deref(),
        )
        .await
    }

    async fn invoke_backend_process_tool<T: serde::Serialize + ?Sized>(
        &self,
        tool_name: &str,
        params: &T,
        tool_call_metadata: &ToolCallMetadata,
    ) -> CallToolResult {
        if !self.process_tools_enabled {
            return envelope_to_call_tool_result(json!({
                "ok": false,
                "error": {
                    "code": "feature_disabled",
                    "message": "gg_process tools are disabled by GG_MCP_ENABLE_PROCESS_TOOLS",
                }
            }));
        }

        self.invoke_backend_tool_with_metadata("gg_process", tool_name, params, tool_call_metadata)
            .await
    }

    async fn invoke_backend_tool<T: serde::Serialize + ?Sized>(
        &self,
        namespace: &str,
        tool_name: &str,
        params: &T,
        caller_agent_id: &str,
        invocation_id: Option<&str>,
    ) -> CallToolResult {
        let _team_call_guard = if namespace == "gg_team" {
            Some(self.acquire_team_call_guard(caller_agent_id).await)
        } else {
            None
        };

        let args = serde_json::to_value(params).unwrap_or_else(|error| {
            json!({
                "__invalid_args__": true,
                "__serialization_error__": error.to_string(),
            })
        });
        let envelope = match self.get_or_init_gateway_client().await {
            Some(gateway_client) => {
                match gateway_client
                    .invoke_tool(namespace, tool_name, args, caller_agent_id, invocation_id)
                    .await
                {
                    Ok(envelope) => envelope,
                    Err(error_message) => json!({
                        "ok": false,
                        "error": {
                            "code": "backend_unavailable",
                            "message": "GG MCP tool gateway invocation failed",
                            "details": {
                                "tool_name": tool_name,
                                "namespace": namespace,
                                "reason": error_message,
                            }
                        }
                    }),
                }
            }
            None => gateway_not_configured_envelope(),
        };
        let envelope = annotate_envelope_with_caller_agent_id(envelope, Some(caller_agent_id));

        envelope_to_call_tool_result(envelope)
    }
}

fn inject_team_manage_model_preset_enum_schema(tool: &mut Tool, model_presets: &[String]) {
    if model_presets.is_empty() {
        return;
    }

    let mut input_schema = (*tool.input_schema).clone();

    let Some(properties) = input_schema
        .get_mut("properties")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    let Some(model_preset_schema) = properties
        .get_mut("model_preset")
        .and_then(Value::as_object_mut)
    else {
        return;
    };

    model_preset_schema.insert(
        "anyOf".to_string(),
        json!([
            {
                "type": "string",
                "minLength": 1,
                "enum": model_presets,
            },
            {
                "type": "null",
            }
        ]),
    );

    tool.input_schema = Arc::new(input_schema);
}

impl ServerHandler for GgMcpServer {
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let tool_call_context =
            rmcp::handler::server::tool::ToolCallContext::new(self, request, context);
        self.tool_router.call(tool_call_context).await
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult {
            tools: self.tools_with_runtime_metadata().await,
            meta: None,
            next_cursor: None,
        })
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        if !self.process_tools_enabled && name.starts_with("gg_process_") {
            return None;
        }

        self.tool_router.get(name).cloned()
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            instructions: Some(
                "Golden Goose MCP server for gg_* runtime control-plane tools over local gateway."
                    .into(),
            ),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests;
