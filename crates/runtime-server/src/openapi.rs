use std::collections::BTreeMap;
use std::path::Path;
use std::sync::OnceLock;

#[path = "openapi_contracts.rs"]
mod contracts;
#[path = "openapi_queries.rs"]
mod queries;
#[path = "openapi_revision.rs"]
mod revision;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

impl HttpMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Default)]
struct RouteSpec {
    methods: std::collections::BTreeSet<HttpMethod>,
    requires_auth: bool,
    agent_scoped_auth: bool,
}

pub fn generated_openapi_yaml() -> &'static str {
    static OPENAPI_YAML: OnceLock<String> = OnceLock::new();
    OPENAPI_YAML.get_or_init(build_openapi_yaml)
}

pub fn write_openapi_artifact(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, generated_openapi_yaml())
}

fn build_openapi_yaml() -> String {
    let routes = collect_routes();
    let mut out = String::new();
    out.push_str("openapi: 3.1.0\n");
    out.push_str("info:\n");
    out.push_str("  title: GG Standalone Agent Runtime API\n");
    out.push_str("  version: 0.1.2\n");
    out.push_str(
        "  summary: Host-owned workspace agents, Codex/Claude/ACP execution, durable operations, three GG tools, and replayable events\n",
    );
    out.push_str("servers:\n");
    out.push_str("  - url: http://localhost:8080\n");
    out.push_str("security:\n");
    out.push_str("  - bearerAuth: []\n");
    out.push_str("components:\n");
    out.push_str("  securitySchemes:\n");
    out.push_str("    bearerAuth:\n");
    out.push_str("      type: http\n");
    out.push_str("      scheme: bearer\n");
    out.push_str("      bearerFormat: Operator API token\n");
    out.push_str("      description: Owner/operator credential; not an agent-scoped MCP token\n");
    out.push_str("    agentScopedBearerAuth:\n");
    out.push_str("      type: http\n");
    out.push_str("      scheme: bearer\n");
    out.push_str("      bearerFormat: Runtime-issued agent MCP token\n");
    out.push_str(
        "      description: Agent-scoped credential tied to an active canonical workspace agent\n",
    );
    out.push_str("  schemas:\n");
    out.push_str("    JsonObject:\n");
    out.push_str("      type: object\n");
    out.push_str("      additionalProperties: true\n");
    contracts::append_schema_components(&mut out);
    out.push_str("paths:\n");
    for (path, route) in routes {
        out.push_str(&format!("  {}:\n", path));
        for method in route.methods {
            let summary = operation_summary(path.as_str(), method);
            out.push_str(&format!("    {}:\n", method.as_str()));
            out.push_str(&format!("      summary: {summary}\n"));
            if !route.requires_auth {
                out.push_str("      security: []\n");
            } else if route.agent_scoped_auth {
                out.push_str("      security:\n");
                out.push_str("        - agentScopedBearerAuth: []\n");
            }
            append_parameters(&mut out, &path, method);
            append_request_body(&mut out, &path, method);
            append_response(&mut out, &path, method);
        }
    }
    out
}

fn operation_summary(path: &str, method: HttpMethod) -> String {
    match (method, path) {
        (HttpMethod::Get, "/health") => "Public health check".to_string(),
        (HttpMethod::Get, "/openapi.yaml") => "Generated OpenAPI schema".to_string(),
        (HttpMethod::Get, "/v1/openapi.yaml") => {
            "Generated OpenAPI schema (authenticated)".to_string()
        }
        (HttpMethod::Get, "/v1/health") => "Authenticated health check".to_string(),
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks") => {
            "Inspect effective user/project team hooks (execution disabled)".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks/approval") => {
            "Inspect operator-approved hook command snapshot and revision".to_string()
        }
        (HttpMethod::Put, "/v2/workspaces/{workspace_id}/hooks/approval") => {
            "Approve or revoke resolved hooks using revisioned operator authority".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks/effects") => {
            "Inspect durable workspace hook effect and recovery diagnostics".to_string()
        }
        (HttpMethod::Get, "/v1/version") => "Runtime version".to_string(),
        (HttpMethod::Get, "/v1/providers") => "List providers".to_string(),
        (HttpMethod::Get, "/v1/providers/{provider}/models") => {
            "List models for provider".to_string()
        }
        (HttpMethod::Get, "/v1/providers/codex/auth/status") => "Codex auth status".to_string(),
        (HttpMethod::Get, "/v1/providers/acp/auth/status") => "ACP auth status".to_string(),
        (HttpMethod::Get, "/v1/providers/claude/auth/status") => "Claude auth status".to_string(),
        (HttpMethod::Post, "/v1/providers/claude/auth/api-key") => "Set Claude API key".to_string(),
        (HttpMethod::Post, "/v1/providers/claude/auth/import-json") => {
            "Import Claude auth JSON".to_string()
        }
        (HttpMethod::Post, "/v1/providers/claude/auth/import-file") => {
            "Import Claude auth file".to_string()
        }
        (HttpMethod::Post, "/v1/providers/claude/auth/logout") => {
            "Clear runtime-managed Claude auth".to_string()
        }
        (HttpMethod::Get, "/v1/events") => "Replay runtime events".to_string(),
        (HttpMethod::Get, "/v1/events/stream") => "Stream runtime events (SSE)".to_string(),
        (HttpMethod::Get, "/v2/events/stream") => {
            "Stream all committed runtime events for the operator (SSE)".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/events/stream") => {
            "Stream committed workspace-scoped events for the operator (SSE)".to_string()
        }
        (HttpMethod::Get, "/v2/agents/{agent_id}/events/stream") => {
            "Stream committed agent/session events for the operator (SSE)".to_string()
        }
        (HttpMethod::Get, "/v2/processes/{process_id}/events/stream") => {
            "Stream committed process events for the operator (SSE)".to_string()
        }
        (HttpMethod::Post, "/v1/sessions") => "Create session".to_string(),
        (HttpMethod::Get, "/v1/sessions") => "List sessions".to_string(),
        (HttpMethod::Get, "/v1/sessions/{session_id}") => "Get session".to_string(),
        (HttpMethod::Post, "/v1/sessions/{session_id}/resume") => "Resume session".to_string(),
        (HttpMethod::Post, "/v1/sessions/{session_id}/close") => "Close session".to_string(),
        (HttpMethod::Post, "/v1/sessions/{session_id}/turns") => "Send session turn".to_string(),
        (HttpMethod::Post, "/v1/sessions/{session_id}/turns/{turn_id}/interrupt") => {
            "Interrupt turn".to_string()
        }
        (HttpMethod::Post, "/v1/sessions/{session_id}/approvals/{approval_id}") => {
            "Respond to approval".to_string()
        }
        (HttpMethod::Get, "/v1/sessions/{session_id}/events") => {
            "Replay session events".to_string()
        }
        (HttpMethod::Get, "/v1/sessions/{session_id}/events/stream") => {
            "Stream session events (SSE)".to_string()
        }
        (HttpMethod::Post, "/v1/processes") => "Start process".to_string(),
        (HttpMethod::Get, "/v1/processes") => "List processes".to_string(),
        (HttpMethod::Get, "/v1/processes/scheduler") => {
            "Inspect durable process scheduler".to_string()
        }
        (HttpMethod::Post, "/v1/processes/scheduler") => {
            "Update durable process scheduler".to_string()
        }
        (HttpMethod::Post, "/v1/processes/queue/reorder") => {
            "Reorder durable queued processes".to_string()
        }
        (HttpMethod::Get, "/v1/processes/{process_id}") => "Get process".to_string(),
        (HttpMethod::Get, "/v1/processes/{process_id}/logs") => "Read process logs".to_string(),
        (HttpMethod::Get, "/v1/processes/{process_id}/events") => {
            "Replay process events".to_string()
        }
        (HttpMethod::Get, "/v1/processes/{process_id}/events/stream") => {
            "Stream process events (SSE)".to_string()
        }
        (HttpMethod::Post, "/v1/processes/{process_id}/kill") => "Kill process".to_string(),
        (HttpMethod::Post, "/v1/worktrees") => "Create worktree".to_string(),
        (HttpMethod::Get, "/v1/worktrees") => "List worktrees".to_string(),
        (HttpMethod::Get, "/v1/worktrees/{worktree_id}") => "Get worktree".to_string(),
        (HttpMethod::Post, "/v1/worktrees/{worktree_id}/claims") => "Claim worktree".to_string(),
        (HttpMethod::Post, "/v1/worktrees/{worktree_id}/release") => {
            "Release worktree claim".to_string()
        }
        (HttpMethod::Post, "/v1/worktrees/{worktree_id}/cleanup") => "Cleanup worktree".to_string(),
        (HttpMethod::Get, "/v1/diagnostics") => "Runtime diagnostics summary".to_string(),
        (HttpMethod::Get, "/v1/diagnostics/providers") => "Provider diagnostics".to_string(),
        (HttpMethod::Get, "/v1/diagnostics/comms") => "Team comms diagnostics".to_string(),
        (HttpMethod::Get, "/v1/diagnostics/processes") => "Process diagnostics".to_string(),
        (HttpMethod::Get, "/v1/diagnostics/worktrees") => "Worktree diagnostics".to_string(),
        (HttpMethod::Get, "/v1/diagnostics/recovery") => "Startup recovery diagnostics".to_string(),
        (HttpMethod::Get, "/v1/diagnostics/team-operations") => {
            "Team operations diagnostics".to_string()
        }
        (HttpMethod::Post, "/v1/teams") => "Create team".to_string(),
        (HttpMethod::Get, "/v1/teams") => "List teams".to_string(),
        (HttpMethod::Get, "/v1/teams/{team_id}") => "Get team".to_string(),
        (HttpMethod::Delete, "/v1/teams/{team_id}") => "Delete team".to_string(),
        (HttpMethod::Post, "/v1/teams/{team_id}/members") => "Join team member".to_string(),
        (HttpMethod::Post, "/v1/teams/{team_id}/members/spawn") => "Spawn team member".to_string(),
        (HttpMethod::Delete, "/v1/teams/{team_id}/members/{agent_id}") => {
            "Remove team member".to_string()
        }
        (HttpMethod::Post, "/v1/teams/{team_id}/lead") => "Set team lead".to_string(),
        (HttpMethod::Get, "/v1/teams/{team_id}/messages") => "List team messages".to_string(),
        (HttpMethod::Post, "/v1/teams/{team_id}/messages") => {
            "Send direct team message".to_string()
        }
        (HttpMethod::Post, "/v1/teams/{team_id}/broadcasts") => "Send team broadcast".to_string(),
        (HttpMethod::Get, "/v1/teams/{team_id}/deliveries") => "List team deliveries".to_string(),
        (HttpMethod::Post, "/v1/teams/{team_id}/deliveries/{delivery_id}/retry") => {
            "Retry team delivery".to_string()
        }
        (HttpMethod::Post, "/v1/teams/{team_id}/messages/{message_id}/cancel") => {
            "Cancel team message".to_string()
        }
        (HttpMethod::Get, "/v1/teams/{team_id}/view") => "Team view snapshot".to_string(),
        (HttpMethod::Get, "/v1/teams/{team_id}/events") => "Replay team events".to_string(),
        (HttpMethod::Get, "/v1/teams/{team_id}/events/stream") => {
            "Stream team events (SSE)".to_string()
        }
        (HttpMethod::Post, "/v1/teams/{team_id}/interrupt-all") => {
            "Interrupt all active team turns".to_string()
        }
        (HttpMethod::Get, "/v2/providers/{provider}/capabilities") => {
            "Get provider capability contract".to_string()
        }
        (HttpMethod::Post, "/v2/providers/{provider}/models/discover") => {
            "Discover provider models".to_string()
        }
        (HttpMethod::Post, "/v2/providers/{provider}/skills/discover") => {
            "Discover provider skills".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces") => "Register workspace".to_string(),
        (HttpMethod::Get, "/v2/workspaces") => "List workspaces".to_string(),
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}") => "Get workspace".to_string(),
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retire") => {
            "Atomically retire a workspace with no outstanding resources".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/retirement-preflight") => {
            "Inspect authoritative obligations before workspace retirement".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retirement/advance") => {
            "Advance recoverable workspace retirement under durable ingress freeze".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile") => {
            "Reconcile an exact quarantined retirement effect with operator evidence".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/worktrees") => {
            "List eligible and blocked managed worktrees".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/worktrees") => {
            "Durably create or reuse managed worktree with native Git/init intent".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree") => {
            "Reassign agent using verified provider cwd evidence".to_string()
        }
        (
            HttpMethod::Get,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}",
        ) => "Inspect durable worktree rebind operation".to_string(),
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup",
        ) => "Retry safe cleanup after verified rebind".to_string(),
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/lead") => {
            "Set, reassign, or clear workspace lead".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/notice-policy") => {
            "Get workspace lifecycle notification policy".to_string()
        }
        (HttpMethod::Put, "/v2/workspaces/{workspace_id}/notice-policy") => {
            "Compare-and-set workspace lifecycle notification policy".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/interrupt") => {
            "Interrupt active workspace turns".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents") => {
            "Create workspace-owned agent".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents") => {
            "List workspace agents".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents/{agent_id}") => {
            "Get workspace agent".to_string()
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding") => {
            "Inspect durable workspace agent onboarding".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive") => {
            "Archive workspace agent".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/restore") => {
            "Restore workspace agent".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve") => {
            "Resolve quarantined native agent creation with independent provider-session evidence".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve") => {
            "Resolve a quarantined provider archive using independent native-close evidence without redispatch".to_string()
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve") => {
            "Resolve a quarantined provider restoration using independent operator evidence without native redispatch".to_string()
        }
        (HttpMethod::Post, "/v2/messages") => {
            "Send agent direct message or workspace broadcast".to_string()
        }
        (HttpMethod::Get, "/v2/messages") => "List canonical agent messages".to_string(),
        (HttpMethod::Get, "/v2/messages/{message_id}/deliveries") => {
            "List canonical message deliveries".to_string()
        }
        (HttpMethod::Post, "/v2/messages/{message_id}/cancel") => {
            "Cancel outstanding canonical message deliveries".to_string()
        }
        (HttpMethod::Post, "/v2/deliveries/{delivery_id}/retry") => {
            "Retry canonical agent delivery".to_string()
        }
        (HttpMethod::Get, "/v2/migrations/workspaces") => {
            "Get legacy workspace migration status".to_string()
        }
        (HttpMethod::Post, "/v2/migrations/workspaces/preview") => {
            "Preview and persist legacy workspace migration".to_string()
        }
        (HttpMethod::Post, "/v2/migrations/workspaces/apply") => {
            "Apply deterministic legacy workspace migration".to_string()
        }
        (HttpMethod::Post, "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}") => {
            "Resolve legacy workspace migration subject".to_string()
        }
        (HttpMethod::Get, "/v2/operations/{operation_id}") => "Get durable operation".to_string(),
        (HttpMethod::Get, "/v2/agents/{agent_id}/context-limit") => {
            "Get current provider-verified context snapshot".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/context-limit/refresh") => {
            "Refresh context snapshot with provider attachment validation".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/preferences") => {
            "Update durable current thinking preference using expected revision".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/permission") => {
            "Update Claude permission selection using expected revision".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve") => {
            "Resolve exact uncertain native policy outcome using independent operator evidence"
                .to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/turns") => {
            "Admit a workspace agent turn with optional typed turn-scoped settings".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun") => {
            "Durably fork a historical user turn and admit an edited replacement".to_string()
        }
        (HttpMethod::Get, "/v2/agents/{agent_id}/edits/{operation_id}") => {
            "Get historical edit operation and native fork/replacement status".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/edits/{operation_id}/resolve") => {
            "Record explicit verified native edit outcome without replaying a fork".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/compact") => {
            "Durably admit manual context compaction before provider dispatch".to_string()
        }
        (HttpMethod::Get, "/v2/agents/{agent_id}/compactions/{run_id}") => {
            "Get current manager compaction status and native outcome".to_string()
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/compactions/{run_id}/resolve") => {
            "Resolve ambiguous native compaction with explicit operator evidence".to_string()
        }
        (HttpMethod::Get, "/v1/mcp/capabilities") => "Runtime MCP capabilities".to_string(),
        (HttpMethod::Post, "/v1/mcp/invoke") => "Invoke runtime MCP tool".to_string(),
        (HttpMethod::Get, "/v2/mcp/capabilities") => "Agent-scoped MCP capabilities".to_string(),
        (HttpMethod::Post, "/v2/mcp/invoke") => "Invoke agent-scoped MCP tool".to_string(),
        _ => format!("{} {}", method.as_str().to_uppercase(), path),
    }
}

fn append_parameters(out: &mut String, path: &str, method: HttpMethod) {
    let mut params = Vec::new();
    let mut index = 0usize;
    while let Some(start) = path[index..].find('{') {
        let absolute_start = index + start + 1;
        if let Some(end) = path[absolute_start..].find('}') {
            let absolute_end = absolute_start + end;
            let name = &path[absolute_start..absolute_end];
            if !name.is_empty() {
                params.push(name.to_string());
            }
            index = absolute_end + 1;
        } else {
            break;
        }
    }
    let accepts_idempotency_key = (method == HttpMethod::Post
        && matches!(
            path,
            "/v2/workspaces"
                | "/v2/workspaces/{workspace_id}/retire"
                | "/v2/workspaces/{workspace_id}/retirement/advance"
                | "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile"
                | "/v2/agents/{agent_id}/compact"
                | "/v2/workspaces/{workspace_id}/lead"
                | "/v2/workspaces/{workspace_id}/worktrees"
                | "/v2/agents/{agent_id}/turns"
                | "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree"
                | "/v2/workspaces/{workspace_id}/agents"
                | "/v2/workspaces/{workspace_id}/interrupt"
                | "/v2/messages"
                | "/v2/messages/{message_id}/cancel"
                | "/v2/deliveries/{delivery_id}/retry"
                | "/v2/migrations/workspaces/apply"
                | "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}"
        ))
        || (method == HttpMethod::Put
            && matches!(
                path,
                "/v2/workspaces/{workspace_id}/notice-policy"
                    | "/v2/workspaces/{workspace_id}/hooks/approval"
            ));
    let query_fields = queries::fields(path, method);
    let v2_event_stream = method == HttpMethod::Get
        && matches!(
            path,
            "/v2/events/stream"
                | "/v2/workspaces/{workspace_id}/events/stream"
                | "/v2/agents/{agent_id}/events/stream"
                | "/v2/processes/{process_id}/events/stream"
        );
    let accepts_if_match = revision::accepts_revision_precondition(path, method);
    let accepts_tool_invocation_id = method == HttpMethod::Post && path == "/v2/mcp/invoke";
    if params.is_empty()
        && !accepts_idempotency_key
        && query_fields.is_none()
        && !accepts_if_match
        && !v2_event_stream
        && !accepts_tool_invocation_id
    {
        return;
    }
    out.push_str("      parameters:\n");
    for name in params {
        out.push_str("        - name: ");
        out.push_str(&name);
        out.push('\n');
        out.push_str("          in: path\n");
        out.push_str("          required: true\n");
        out.push_str("          schema:\n");
        out.push_str("            type: string\n");
    }
    if let Some(fields) = query_fields.as_deref() {
        queries::append(out, fields);
    }
    if v2_event_stream {
        out.push_str("        - name: Last-Event-ID\n");
        out.push_str("          in: header\n");
        out.push_str("          required: false\n");
        out.push_str("          description: Durable event cursor, overridden by after_seq when both are present\n");
        out.push_str("          schema:\n            type: integer\n            minimum: 0\n");
    }
    if accepts_tool_invocation_id {
        out.push_str("        - name: x-gg-tool-invocation-id\n");
        out.push_str("          in: header\n");
        out.push_str("          required: false\n");
        out.push_str("          description: Sidecar-issued invocation identity, mandatory for mutating tool modes and never accepted from an untrusted JSON body\n");
        out.push_str("          schema:\n            type: string\n");
    }
    if accepts_idempotency_key {
        out.push_str("        - name: Idempotency-Key\n");
        out.push_str("          in: header\n");
        out.push_str(if path.contains("/retirement/") {
            "          required: true\n"
        } else {
            "          required: false\n"
        });
        out.push_str("          schema:\n");
        out.push_str("            type: string\n");
    }
    if accepts_if_match {
        out.push_str("        - name: If-Match\n");
        out.push_str("          in: header\n");
        out.push_str("          required: false\n");
        out.push_str("          schema:\n");
        out.push_str("            type: string\n");
    }
}

fn append_request_body(out: &mut String, path: &str, method: HttpMethod) {
    if !matches!(method, HttpMethod::Post | HttpMethod::Put) {
        return;
    }
    let expects_multipart = path == "/v1/providers/claude/auth/import-file";
    let expects_json = matches!(
        path,
        "/v1/mcp/invoke"
            | "/v2/mcp/invoke"
            | "/v1/providers/claude/auth/api-key"
            | "/v1/providers/claude/auth/import-json"
            | "/v1/sessions"
            | "/v1/sessions/{session_id}/turns"
            | "/v1/sessions/{session_id}/approvals/{approval_id}"
            | "/v1/processes"
            | "/v1/processes/scheduler"
            | "/v1/processes/queue/reorder"
            | "/v1/worktrees"
            | "/v1/worktrees/{worktree_id}/claims"
            | "/v1/worktrees/{worktree_id}/release"
            | "/v1/worktrees/{worktree_id}/cleanup"
            | "/v1/teams"
            | "/v1/teams/{team_id}/members"
            | "/v1/teams/{team_id}/members/spawn"
            | "/v1/teams/{team_id}/lead"
            | "/v1/teams/{team_id}/messages"
            | "/v1/teams/{team_id}/broadcasts"
            | "/v1/teams/{team_id}/deliveries/{delivery_id}/retry"
            | "/v1/teams/{team_id}/messages/{message_id}/cancel"
            | "/v1/teams/{team_id}/interrupt-all"
            | "/v2/workspaces"
            | "/v2/workspaces/{workspace_id}/retire"
            | "/v2/workspaces/{workspace_id}/retirement/advance"
            | "/v2/providers/{provider}/models/discover"
            | "/v2/providers/{provider}/skills/discover"
            | "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile"
            | "/v2/agents/{agent_id}/preferences"
            | "/v2/agents/{agent_id}/permission"
            | "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve"
            | "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve"
            | "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve"
            | "/v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve"
            | "/v2/agents/{agent_id}/turns"
            | "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun"
            | "/v2/agents/{agent_id}/edits/{operation_id}/resolve"
            | "/v2/agents/{agent_id}/compactions/{run_id}/resolve"
            | "/v2/workspaces/{workspace_id}/lead"
            | "/v2/workspaces/{workspace_id}/notice-policy"
            | "/v2/workspaces/{workspace_id}/hooks/approval"
            | "/v2/workspaces/{workspace_id}/worktrees"
            | "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree"
            | "/v2/workspaces/{workspace_id}/agents"
            | "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"
            | "/v2/messages"
            | "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}"
    );
    if !expects_multipart && !expects_json {
        return;
    }
    out.push_str("      requestBody:\n");
    out.push_str("        required: true\n");
    out.push_str("        content:\n");
    if expects_multipart {
        out.push_str("          multipart/form-data:\n");
        out.push_str("            schema:\n");
        out.push_str("              type: object\n");
        out.push_str("              properties:\n");
        out.push_str("                file:\n");
        out.push_str("                  type: string\n");
        out.push_str("                  format: binary\n");
    } else {
        out.push_str("          application/json:\n");
        out.push_str("            schema:\n");
        let schema = contracts::request_schema_name(path, method).unwrap_or("JsonObject");
        out.push_str(&format!(
            "              $ref: \"#/components/schemas/{schema}\"\n"
        ));
    }
}

fn start_response_headers(out: &mut String, started: &mut bool) {
    if !*started {
        out.push_str("          headers:\n");
        *started = true;
    }
}

fn append_request_id_header(out: &mut String) {
    out.push_str("            X-Request-ID:\n");
    out.push_str(
        "              description: Server-minted request correlation ID on every operator v2 response\n",
    );
    out.push_str("              schema:\n");
    out.push_str("                type: string\n");
}

fn append_operator_default_error(out: &mut String) {
    out.push_str("        default:\n");
    out.push_str("          description: Typed operator API error response (4xx or 5xx)\n");
    out.push_str("          headers:\n");
    append_request_id_header(out);
    out.push_str("          content:\n");
    out.push_str("            application/json:\n");
    out.push_str("              schema:\n");
    out.push_str("                $ref: \"#/components/schemas/OperatorErrorResponse\"\n");
}

fn append_response(out: &mut String, path: &str, method: HttpMethod) {
    let operator_v2 = path.starts_with("/v2/") && !path.starts_with("/v2/mcp/");
    let mut headers_started = false;
    let status = if method == HttpMethod::Delete {
        "204"
    } else {
        "200"
    };
    out.push_str("      responses:\n");
    out.push_str("        \"");
    out.push_str(status);
    out.push_str("\":\n");
    out.push_str("          description: ");
    if method == HttpMethod::Delete {
        out.push_str("Deleted\n");
        return;
    }
    if path.ends_with("/events/stream") || path == "/v1/events/stream" {
        out.push_str("SSE stream\n");
        if operator_v2 {
            start_response_headers(out, &mut headers_started);
            append_request_id_header(out);
        }
        out.push_str("          content:\n");
        out.push_str("            text/event-stream:\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
        if operator_v2 {
            append_operator_default_error(out);
        }
        return;
    }
    if path == "/openapi.yaml" || path == "/v1/openapi.yaml" {
        out.push_str("OpenAPI YAML\n");
        out.push_str("          content:\n");
        out.push_str("            application/yaml:\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
        return;
    }
    out.push_str("JSON response\n");
    if operator_v2 {
        start_response_headers(out, &mut headers_started);
        append_request_id_header(out);
    }
    if queries::returns_next_cursor(path, method) {
        // Each listed response emits this only when another keyset page
        // exists. The cursor remains an HTTP header, never a fake JSON
        // wrapper around the real array/object return type.
        start_response_headers(out, &mut headers_started);
        out.push_str("            X-Next-Cursor:\n");
        out.push_str("              description: Present when a following keyset page exists; pass as cursor on the next request\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
    }
    if (method == HttpMethod::Get
        || matches!(
            path,
            "/v2/agents/{agent_id}/preferences" | "/v2/agents/{agent_id}/permission"
        ))
        && revision::returns_revision_etag(path, method)
    {
        start_response_headers(out, &mut headers_started);
        revision::append_etag_header(out);
    }
    if method == HttpMethod::Post
        && matches!(
            path,
            "/v2/workspaces/{workspace_id}/worktrees"
                | "/v2/agents/{agent_id}/turns"
                | "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/restore"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve"
                | "/v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve"
                | "/v2/workspaces/{workspace_id}/agents"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve"
        )
    {
        start_response_headers(out, &mut headers_started);
        out.push_str("            X-Operation-ID:\n");
        out.push_str("              description: Durable native operation, including one-shot unkeyed lifecycle intents and independent manual-review resolutions\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
        if revision::returns_revision_etag(path, method) {
            revision::append_etag_header(out);
        }
    }
    if method == HttpMethod::Post && path == "/v2/messages/{message_id}/cancel" {
        start_response_headers(out, &mut headers_started);
        out.push_str("            X-Operation-ID:\n");
        out.push_str("              description: Durable atomic message cancellation operation\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
    }
    if method == HttpMethod::Post && path == "/v2/deliveries/{delivery_id}/retry" {
        start_response_headers(out, &mut headers_started);
        out.push_str("            X-Operation-ID:\n");
        out.push_str("              description: Durable exact retry admission receipt, present only when Idempotency-Key is supplied; not a provider delivery acknowledgment\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
    }
    if (method == HttpMethod::Post
        && matches!(
            path,
            "/v2/workspaces"
                | "/v2/workspaces/{workspace_id}/retire"
                | "/v2/workspaces/{workspace_id}/retirement/advance"
                | "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile"
                | "/v2/workspaces/{workspace_id}/lead"
                | "/v2/workspaces/{workspace_id}/interrupt"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup"
                | "/v2/migrations/workspaces/apply"
                | "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}"
                | "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun"
                | "/v2/agents/{agent_id}/edits/{operation_id}/resolve"
        ))
        || (method == HttpMethod::Put
            && matches!(
                path,
                "/v2/workspaces/{workspace_id}/notice-policy"
                    | "/v2/workspaces/{workspace_id}/hooks/approval"
            ))
    {
        start_response_headers(out, &mut headers_started);
        out.push_str("            X-Operation-ID:\n");
        out.push_str("              description: Exact durable operation identifier returned with this mutation; inspect through the matching operation or resource-detail route\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
        if revision::returns_revision_etag(path, method) {
            revision::append_etag_header(out);
        }
    }
    out.push_str("          content:\n");
    out.push_str("            application/json:\n");
    out.push_str("              schema:\n");
    let schema = contracts::response_schema_name(path, method).unwrap_or("JsonObject");
    out.push_str(&format!(
        "                $ref: \"#/components/schemas/{schema}\"\n"
    ));
    if method == HttpMethod::Post
        && matches!(
            path,
            "/v2/workspaces/{workspace_id}/worktrees"
                | "/v2/agents/{agent_id}/turns"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/restore"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"
                | "/v2/workspaces/{workspace_id}/agents"
        )
    {
        out.push_str("        \"409\":\n");
        out.push_str("          description: Conflict or unresolved native effect; inspect the operation without retrying the effect\n");
        out.push_str("          headers:\n");
        append_request_id_header(out);
        out.push_str("            X-Operation-ID:\n");
        out.push_str(
            "              description: Present when the native outcome requires durable review\n",
        );
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
        out.push_str("          content:\n");
        out.push_str("            application/json:\n");
        out.push_str("              schema:\n");
        out.push_str("                $ref: \"#/components/schemas/OperatorErrorResponse\"\n");
    }
    if method == HttpMethod::Post && path == "/v2/workspaces/{workspace_id}/retirement/advance" {
        out.push_str("        \"202\":\n");
        out.push_str(
            "          description: Retirement remains requested or requires manual review\n",
        );
        out.push_str("          headers:\n");
        append_request_id_header(out);
        out.push_str("            X-Operation-ID:\n");
        out.push_str("              description: Durable frozen retirement operation, not proof of native provider settlement\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
        out.push_str("          content:\n");
        out.push_str("            application/json:\n");
        out.push_str("              schema:\n");
        out.push_str("                $ref: \"#/components/schemas/OperationDetails\"\n");
    }
    if revision::accepts_revision_precondition(path, method) {
        revision::append_revision_precondition_failure(out);
    }
    if operator_v2 {
        append_operator_default_error(out);
    }
}

// Match a compile-time-checked method declared by the same registration
// table as the Axum router; there is no source-string tokenization or
// heuristic search for "get("/"post(" in handler bodies.
macro_rules! route_http_method {
    (get) => {
        HttpMethod::Get
    };
    (post) => {
        HttpMethod::Post
    };
    (put) => {
        HttpMethod::Put
    };
    (delete) => {
        HttpMethod::Delete
    };
}

macro_rules! agent_scoped_route_group {
    (scoped_mcp) => {
        true
    };
    ($other:ident) => {
        false
    };
}

fn collect_routes() -> BTreeMap<String, RouteSpec> {
    macro_rules! collect_declared_routes {
        (
            $(
                $group:ident ($prefix:literal, $auth:literal) {
                    $(
                        $path:literal => $method:ident($handler:ident)
                        $(.$next_method:ident($next_handler:ident))*
                        $(@ $max_body:expr)?;
                    )*
                }
            )*
        ) => {{
            let mut routes = BTreeMap::<String, RouteSpec>::new();
            $(
                $(
                    // Both segments are literal tokens from the runtime
                    // router manifest; the full path is a compile-time
                    // string rather than a fragile extracted source span.
                    let path = concat!($prefix, $path);
                    let entry = routes.entry(path.to_string()).or_default();
                    let scoped_auth = agent_scoped_route_group!($group);
                    assert!(
                        entry.methods.is_empty() || entry.agent_scoped_auth == scoped_auth,
                        "a route must never mix operator and agent-scoped Bearer authentication: {path}"
                    );
                    entry.requires_auth |= $auth;
                    entry.agent_scoped_auth |= scoped_auth;
                    assert!(
                        entry.methods.insert(route_http_method!($method)),
                        "duplicate HTTP method registration: {path}"
                    );
                    $(
                        assert!(
                            entry.methods.insert(route_http_method!($next_method)),
                            "duplicate HTTP method registration: {path}"
                        );
                    )*
                )*
            )*
            routes
        }};
    }
    crate::http::route_table!(collect_declared_routes)
}

#[cfg(test)]
#[path = "openapi_tests.rs"]
mod tests;
