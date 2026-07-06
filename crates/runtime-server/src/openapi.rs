use std::collections::BTreeMap;
use std::path::Path;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HttpMethod {
    Get,
    Post,
    Delete,
}

impl HttpMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Default)]
struct RouteSpec {
    methods: std::collections::BTreeSet<HttpMethod>,
    requires_auth: bool,
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
        "  summary: Single-user hosted runtime for Codex/Claude/ACP sessions, teams, processes, worktrees, and SSE streams\n",
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
    out.push_str("      bearerFormat: API token\n");
    out.push_str("  schemas:\n");
    out.push_str("    JsonObject:\n");
    out.push_str("      type: object\n");
    out.push_str("      additionalProperties: true\n");
    out.push_str("paths:\n");
    for (path, route) in routes {
        out.push_str(&format!("  {}:\n", path));
        for method in route.methods {
            let summary = operation_summary(path.as_str(), method);
            out.push_str(&format!("    {}:\n", method.as_str()));
            out.push_str(&format!("      summary: {summary}\n"));
            if !route.requires_auth {
                out.push_str("      security: []\n");
            }
            append_path_parameters(&mut out, &path);
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
        (HttpMethod::Get, "/v1/mcp/capabilities") => "Runtime MCP capabilities".to_string(),
        (HttpMethod::Post, "/v1/mcp/invoke") => "Invoke runtime MCP tool".to_string(),
        _ => format!("{} {}", method.as_str().to_uppercase(), path),
    }
}

fn append_path_parameters(out: &mut String, path: &str) {
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
    if params.is_empty() {
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
}

fn append_request_body(out: &mut String, path: &str, method: HttpMethod) {
    if method != HttpMethod::Post {
        return;
    }
    let expects_multipart = path == "/v1/providers/claude/auth/import-file";
    let expects_json = matches!(
        path,
        "/v1/mcp/invoke"
            | "/v1/providers/claude/auth/api-key"
            | "/v1/providers/claude/auth/import-json"
            | "/v1/sessions"
            | "/v1/sessions/{session_id}/turns"
            | "/v1/sessions/{session_id}/approvals/{approval_id}"
            | "/v1/processes"
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
        out.push_str("              $ref: \"#/components/schemas/JsonObject\"\n");
    }
}

fn append_response(out: &mut String, path: &str, method: HttpMethod) {
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
        out.push_str("          content:\n");
        out.push_str("            text/event-stream:\n");
        out.push_str("              schema:\n");
        out.push_str("                type: string\n");
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
    out.push_str("          content:\n");
    out.push_str("            application/json:\n");
    out.push_str("              schema:\n");
    out.push_str("                $ref: \"#/components/schemas/JsonObject\"\n");
}

fn collect_routes() -> BTreeMap<String, RouteSpec> {
    let source = include_str!("http/mod.rs");
    let mcp_start = source.find("let mcp = Router::new()").unwrap_or(0);
    let protected_start = source
        .find("let protected = Router::new()")
        .unwrap_or(mcp_start);
    let protected_marker = "let protected = Router::new()";
    let protected_body_start = protected_start + protected_marker.len();
    let root_marker = "Router::new()\n        .route(\"/health\", get(health))";
    let root_start = source[protected_body_start..]
        .find(root_marker)
        .map(|offset| protected_body_start + offset)
        .unwrap_or(protected_body_start);
    let root_end = source[root_start..]
        .find(".with_state(state)")
        .map(|offset| root_start + offset)
        .unwrap_or(source.len());

    let mcp_body_start = mcp_start + "let mcp = Router::new()".len();
    let mcp_block = if mcp_body_start <= protected_start && protected_start <= source.len() {
        source[mcp_body_start..protected_start].to_string()
    } else {
        String::new()
    };
    let protected_block = if protected_body_start <= root_start && root_start <= source.len() {
        source[protected_body_start..root_start].to_string()
    } else {
        String::new()
    };
    let root_body_start = root_start + "Router::new()".len();
    let root_block = if root_body_start <= root_end && root_end <= source.len() {
        source[root_body_start..root_end].to_string()
    } else {
        String::new()
    };

    let mut routes = BTreeMap::<String, RouteSpec>::new();
    collect_block_routes(&mut routes, &mcp_block, "/v1/mcp", true);
    collect_block_routes(&mut routes, &protected_block, "/v1", true);
    collect_block_routes(&mut routes, &root_block, "", false);
    routes
}

fn collect_block_routes(
    routes: &mut BTreeMap<String, RouteSpec>,
    block: &str,
    prefix: &str,
    requires_auth: bool,
) {
    let mut cursor = 0usize;
    while let Some(found) = block[cursor..].find(".route(") {
        let route_start = cursor + found + ".route(".len();
        if let Some((path, methods, close_index)) = parse_route_call(block, route_start) {
            let full_path = if prefix.is_empty() {
                path
            } else {
                format!("{prefix}{path}")
            };
            let entry = routes.entry(full_path).or_default();
            entry.requires_auth = entry.requires_auth || requires_auth;
            for method in methods {
                entry.methods.insert(method);
            }
            cursor = close_index;
        } else {
            cursor = route_start;
        }
    }
}

fn parse_route_call(block: &str, route_start: usize) -> Option<(String, Vec<HttpMethod>, usize)> {
    let mut idx = route_start;
    while idx < block.len() && block.as_bytes()[idx].is_ascii_whitespace() {
        idx += 1;
    }
    if block.as_bytes().get(idx).copied()? != b'"' {
        return None;
    }
    let path_start = idx + 1;
    let path_end = block[path_start..].find('"')? + path_start;
    let path = block[path_start..path_end].to_string();
    let mut comma_index = path_end + 1;
    while comma_index < block.len() && block.as_bytes()[comma_index].is_ascii_whitespace() {
        comma_index += 1;
    }
    if block.as_bytes().get(comma_index).copied()? != b',' {
        return None;
    }
    let args_start = comma_index + 1;
    let mut depth = 1i32;
    let mut i = args_start;
    while i < block.len() {
        match block.as_bytes()[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    let args = &block[args_start..i];
                    let mut methods = Vec::new();
                    if args.contains("get(") {
                        methods.push(HttpMethod::Get);
                    }
                    if args.contains("post(") {
                        methods.push(HttpMethod::Post);
                    }
                    if args.contains("delete(") {
                        methods.push(HttpMethod::Delete);
                    }
                    return Some((path, methods, i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::generated_openapi_yaml;

    #[test]
    fn generated_openapi_includes_core_surface() {
        let yaml = generated_openapi_yaml();
        assert!(yaml.contains("openapi: 3.1.0"));
        assert!(yaml.contains("  /v1/sessions:"));
        assert!(yaml.contains("  /v1/sessions/{session_id}/events/stream:"));
        assert!(yaml.contains("  /v1/teams/{team_id}/messages:"));
        assert!(yaml.contains("  /v1/mcp/invoke:"));
        assert!(yaml.contains("  /v1/providers/acp/auth/status:"));
        assert!(yaml.contains("  /openapi.yaml:"));
    }
}
