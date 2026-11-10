use super::generated_openapi_yaml;

#[test]
fn declarative_route_table_preserves_methods_and_authentication_scopes() {
    let routes = super::collect_routes();
    assert_eq!(routes.len(), 124, "all original v1/v2 and public routes");
    let method_count: usize = routes.values().map(|spec| spec.methods.len()).sum();
    assert_eq!(method_count, 137, "multi-method routes retained");

    let public = routes.get("/health").expect("public health route");
    assert!(!public.requires_auth);
    assert!(public.methods.contains(&super::HttpMethod::Get));

    for path in [
        "/v1/mcp/invoke",
        "/v2/mcp/invoke",
        "/v2/workspaces/{workspace_id}/worktrees",
        "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree",
        "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve",
    ] {
        assert!(
            routes.get(path).is_some_and(|route| route.requires_auth),
            "{path}"
        );
    }
    let worktrees = routes
        .get("/v2/workspaces/{workspace_id}/worktrees")
        .expect("workspace worktrees");
    assert_eq!(
        worktrees.methods,
        [super::HttpMethod::Get, super::HttpMethod::Post].into()
    );
    let notice = routes
        .get("/v2/workspaces/{workspace_id}/notice-policy")
        .expect("notice policy");
    assert_eq!(
        notice.methods,
        [super::HttpMethod::Get, super::HttpMethod::Put].into()
    );
}

#[test]
fn generated_openapi_includes_core_surface() {
    let yaml = generated_openapi_yaml();
    assert!(yaml.contains("openapi: 3.1.0"));
    assert!(yaml.contains("  /v1/sessions:"));
    assert!(yaml.contains("  /v1/sessions/{session_id}/events/stream:"));
    assert!(yaml.contains("  /v1/teams/{team_id}/messages:"));
    assert!(yaml.contains("  /v1/mcp/invoke:"));
    assert!(yaml.contains("  /v2/mcp/invoke:"));
    assert!(yaml.contains("  /v1/providers/acp/auth/status:"));
    assert!(yaml.contains("  /v2/workspaces:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/lead:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/retire:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/retirement-preflight:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/notice-policy:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/interrupt:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/agents:"));
    assert!(
        yaml.contains("  /v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve:")
    );
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/agents/{agent_id}:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/agents/{agent_id}/archive:"));
    assert!(yaml.contains("  /v2/workspaces/{workspace_id}/agents/{agent_id}/restore:"));
    assert!(yaml.contains("  /v2/agents/{agent_id}/context-limit:"));
    assert!(yaml.contains("  /v2/agents/{agent_id}/context-limit/refresh:"));
    assert!(yaml.contains("  /v2/agents/{agent_id}/preferences:"));
    assert!(yaml.contains("  /v2/agents/{agent_id}/permission:"));
    assert!(yaml.contains("  /v2/agents/{agent_id}/native-policy/{operation_id}/resolve:"));
    assert!(yaml.contains("  /v2/messages:"));
    assert!(yaml.contains("  /v2/messages/{message_id}/deliveries:"));
    assert!(yaml.contains("  /v2/messages/{message_id}/cancel:"));
    let cancellation = yaml
        .split("  /v2/messages/{message_id}/cancel:\n")
        .nth(1)
        .expect("canonical cancellation route")
        .split("\n  /v2/")
        .next()
        .expect("cancellation route content");
    assert!(cancellation.contains("        - name: Idempotency-Key\n"));
    assert!(cancellation.contains("            X-Operation-ID:\n"));
    assert!(yaml.contains("  /v2/deliveries/{delivery_id}/retry:"));
    let retry = yaml
        .split("  /v2/deliveries/{delivery_id}/retry:\n")
        .nth(1)
        .expect("canonical retry route")
        .split("\n  /v2/")
        .next()
        .expect("retry route content");
    assert!(retry.contains("        - name: Idempotency-Key\n"));
    assert!(retry.contains("            X-Operation-ID:\n"));
    for path in [
        "/v2/workspaces",
        "/v2/workspaces/{workspace_id}/lead",
        "/v2/workspaces/{workspace_id}/interrupt",
        "/v2/workspaces/{workspace_id}/retire",
        "/v2/workspaces/{workspace_id}/retirement/advance",
        "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile",
        "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree",
        "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup",
        "/v2/migrations/workspaces/apply",
        "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}",
        "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun",
        "/v2/agents/{agent_id}/edits/{operation_id}/resolve",
        "/v2/workspaces/{workspace_id}/notice-policy",
        "/v2/workspaces/{workspace_id}/hooks/approval",
    ] {
        let marker = format!("  {path}:\n");
        let content = yaml
            .split(&marker)
            .nth(1)
            .unwrap_or_else(|| panic!("missing journaled mutation {path}"))
            .split("\n  /v2/")
            .next()
            .unwrap();
        assert!(
            content.contains("            X-Operation-ID:\n"),
            "undocumented operation response header on {path}"
        );
    }
    let advance = yaml
        .split("  /v2/workspaces/{workspace_id}/retirement/advance:\n")
        .nth(1)
        .unwrap()
        .split("\n  /v2/")
        .next()
        .unwrap();
    assert!(advance.contains("        \"202\":\n"));
    assert!(advance.contains("Retirement remains requested or requires manual review\n          headers:\n            X-Operation-ID:\n"));
    for path in [
        "/v2/workspaces/{workspace_id}/lead",
        "/v2/workspaces/{workspace_id}/notice-policy",
        "/v2/workspaces/{workspace_id}/hooks/approval",
    ] {
        let content = yaml.split(&format!("  {path}:\n")).nth(1).unwrap();
        assert!(
            content.contains("            ETag:\n"),
            "revision header on {path}"
        );
    }
    assert!(yaml.contains("  /v2/migrations/workspaces:"));
    assert!(yaml.contains("  /v2/migrations/workspaces/preview:"));
    assert!(yaml.contains("  /v2/migrations/workspaces/apply:"));
    assert!(yaml.contains("  /v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}:"));
    assert!(yaml.contains("  /v2/operations/{operation_id}:"));
    let creation = yaml
        .split("  /v2/workspaces/{workspace_id}/agents:\n")
        .nth(1)
        .unwrap()
        .split("\n  /")
        .next()
        .unwrap();
    assert!(creation.contains("        - name: Idempotency-Key\n"));
    assert!(creation.contains("            X-Operation-ID:\n"));
    assert!(creation.contains("            ETag:\n"));
    assert!(creation.contains("        \"409\":\n"));
    let review = yaml
        .split("  /v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve:\n")
        .nth(1)
        .unwrap()
        .split("\n  /")
        .next()
        .unwrap();
    assert!(review.contains("      requestBody:\n"));
    assert!(review.contains("            X-Operation-ID:\n"));
    assert!(!review.contains("        - name: Idempotency-Key\n"));
    assert!(!review.contains("        - name: If-Match\n"));
    assert!(yaml.contains("        - name: Idempotency-Key"));
    let user_turn = yaml
        .split("  /v2/agents/{agent_id}/turns:\n")
        .nth(1)
        .expect("canonical operator agent turn route")
        .split("\n  /v2/")
        .next()
        .expect("agent turn route content");
    assert!(user_turn.contains("        - name: Idempotency-Key\n"));
    assert!(user_turn.contains("        \"409\":\n"));
    assert!(user_turn.contains("            X-Operation-ID:\n"));
    let worktree_path = yaml
        .split("  /v2/workspaces/{workspace_id}/worktrees:\n")
        .nth(1)
        .expect("managed worktree operator route");
    let worktree_path = worktree_path
        .split("\n  /v2/")
        .next()
        .expect("worktree route contents");
    assert!(worktree_path.contains("      requestBody:\n"));
    assert!(worktree_path.contains("        \"409\":\n"));
    assert!(worktree_path.contains("Present when the native outcome requires durable review"));
    assert!(yaml.contains("  /openapi.yaml:"));

    let resolution_start = yaml
        .find("  /v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}:\n")
        .expect("workspace migration resolution route");
    let resolution_tail = &yaml[resolution_start..];
    let resolution_end = resolution_tail
        .find("\n  /")
        .expect("next route following workspace migration resolution");
    let resolution = &resolution_tail[..resolution_end];
    assert_eq!(resolution.matches("      parameters:\n").count(), 1);
    assert!(resolution.contains("        - name: subject_kind\n"));
    assert!(resolution.contains("        - name: subject_id\n"));
    assert!(resolution.contains("        - name: Idempotency-Key\n"));
}

/// Keep query names, accepted handler ranges and response cursors tied to the
/// same source-derived projection; manually adding an endpoint must update the
/// handler registry rather than inventing extra payload wrappers.
#[test]
fn v2_query_projection_matches_axum_handler_types_and_real_cursor_headers() {
    let yaml = generated_openapi_yaml();
    for (path, method, names) in [
        ("/v2/workspaces", "get", vec!["cursor", "limit"]),
        (
            "/v2/workspaces/{workspace_id}/worktrees",
            "get",
            vec!["cursor", "limit"],
        ),
        (
            "/v2/workspaces/{workspace_id}/agents",
            "get",
            vec!["cursor", "lifecycle", "limit"],
        ),
        ("/v2/workspaces/{workspace_id}/hooks", "get", vec!["event"]),
        (
            "/v2/workspaces/{workspace_id}/hooks/effects",
            "get",
            vec!["cursor", "limit"],
        ),
        (
            "/v2/messages",
            "get",
            vec!["cursor", "limit", "sender_agent_id", "workspace_id"],
        ),
        (
            "/v2/messages/{message_id}/deliveries",
            "get",
            vec!["cursor", "limit", "recipient_agent_id"],
        ),
        (
            "/v2/operations",
            "get",
            vec!["cursor", "limit", "workspace_id"],
        ),
        (
            "/v2/processes",
            "get",
            vec!["cursor", "limit", "workspace_id"],
        ),
        (
            "/v2/events",
            "get",
            vec!["cursor", "limit", "scope", "scope_id"],
        ),
        ("/v2/migrations/workspaces", "get", vec!["cursor", "limit"]),
        (
            "/v2/migrations/workspaces/preview",
            "post",
            vec!["cursor", "limit"],
        ),
    ] {
        let op = route_method(yaml, path, method);
        let parameters = op.split("      responses:\n").next().unwrap();
        let query_names: Vec<&str> = parameters
            .split("        - name: ")
            .skip(1)
            .filter(|parameter| parameter.lines().any(|line| line.trim() == "in: query"))
            .map(|parameter| parameter.lines().next().unwrap())
            .collect();
        assert_eq!(
            query_names, names,
            "query projection drift on {method} {path}"
        );
        if names.contains(&"limit") {
            assert!(parameters.contains("\"minimum\":1"), "{path}");
            assert!(parameters.contains("\"maximum\":100"), "{path}");
            assert!(parameters.contains("\"default\":50"), "{path}");
        }
        assert!(
            !parameters.contains("\"null\""),
            "query fields must not advertise JSON null: {path}"
        );
    }

    let agent_list = route_method(yaml, "/v2/workspaces/{workspace_id}/agents", "get");
    assert!(agent_list.contains("\"enum\":[\"active\",\"archived\",\"all\"]"));
    let hooks = route_method(yaml, "/v2/workspaces/{workspace_id}/hooks", "get");
    assert!(hooks.contains("\"post_direct_message_send\""));
    let events = route_method(yaml, "/v2/events", "get");
    assert!(events.contains(
        "\"enum\":[\"session\",\"workspace\",\"team\",\"process\",\"worktree\",\"system\"]"
    ));
    assert!(events.contains("Scope and scope_id must be supplied together"));

    for path in [
        "/v2/events/stream",
        "/v2/workspaces/{workspace_id}/events/stream",
        "/v2/agents/{agent_id}/events/stream",
        "/v2/processes/{process_id}/events/stream",
    ] {
        let op = route_method(yaml, path, "get");
        assert!(op.contains("        - name: after_seq\n"));
        assert!(op.contains("        - name: limit\n"));
        assert!(op.contains("\"maximum\":10000"));
        assert!(op.contains("\"default\":2000"));
        assert!(op.contains("        - name: Last-Event-ID\n"));
        assert!(
            !op.contains("X-Next-Cursor:"),
            "SSE streams are not keyset lists"
        );
    }
    for (path, method) in [
        ("/v2/workspaces", "get"),
        ("/v2/workspaces/{workspace_id}/worktrees", "get"),
        ("/v2/workspaces/{workspace_id}/agents", "get"),
        ("/v2/workspaces/{workspace_id}/hooks/effects", "get"),
        ("/v2/messages/{message_id}/deliveries", "get"),
        ("/v2/operations", "get"),
        ("/v2/processes", "get"),
        ("/v2/events", "get"),
        ("/v2/migrations/workspaces", "get"),
        ("/v2/migrations/workspaces/preview", "post"),
    ] {
        assert!(
            route_method(yaml, path, method).contains("            X-Next-Cursor:\n"),
            "{path}"
        );
    }
    assert!(
        !route_method(yaml, "/v2/messages", "get").contains("X-Next-Cursor:"),
        "message list embeds next_cursor in its JSON response"
    );
}

#[test]
fn agent_scoped_mcp_and_operator_bearer_contracts_do_not_confuse_authority() {
    let yaml = generated_openapi_yaml();
    assert!(yaml.contains("    bearerAuth:\n      type: http\n"));
    assert!(yaml.contains("    agentScopedBearerAuth:\n      type: http\n"));
    for path in ["/v2/mcp/capabilities", "/v2/mcp/invoke"] {
        let method = if path.ends_with("invoke") {
            "post"
        } else {
            "get"
        };
        let op = route_method(yaml, path, method);
        assert!(
            op.contains("        - agentScopedBearerAuth: []\n"),
            "{path}"
        );
        assert!(!op.contains("        - bearerAuth: []\n"), "{path}");
    }
    let invoke = route_method(yaml, "/v2/mcp/invoke", "post");
    assert!(invoke.contains("        - name: x-gg-tool-invocation-id\n"));
    assert!(invoke.contains("Sidecar-issued invocation identity"));
    assert!(!route_method(yaml, "/v1/mcp/invoke", "post").contains("agentScopedBearerAuth"));
    assert!(!route_method(yaml, "/v2/workspaces", "get").contains("agentScopedBearerAuth"));
    assert!(route_method(yaml, "/health", "get").contains("      security: []\n"));
    assert_eq!(
        super::collect_routes()["/v2/mcp/invoke"].agent_scoped_auth,
        true
    );
    assert_eq!(
        super::collect_routes()["/v2/workspaces"].agent_scoped_auth,
        false
    );
}

fn route_method<'a>(yaml: &'a str, path: &str, method: &str) -> &'a str {
    let marker = format!("  {path}:\n");
    let path_chunk = yaml
        .split(&marker)
        .nth(1)
        .unwrap_or_else(|| panic!("missing {path}"));
    let path_chunk = path_chunk.split("\n  /").next().unwrap();
    let method_marker = format!("    {method}:\n");
    let method_chunk = path_chunk
        .split(&method_marker)
        .nth(1)
        .unwrap_or_else(|| panic!("missing {method} {path}"));
    let end = ["get", "post", "put", "delete"]
        .iter()
        .filter(|candidate| **candidate != method)
        .filter_map(|candidate| method_chunk.find(&format!("\n    {candidate}:\n")))
        .min()
        .unwrap_or(method_chunk.len());
    &method_chunk[..end]
}
