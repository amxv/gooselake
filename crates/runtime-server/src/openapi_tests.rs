use super::generated_openapi_yaml;

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
        .find("\n  /v2/operations/{operation_id}:")
        .expect("route following workspace migration resolution");
    let resolution = &resolution_tail[..resolution_end];
    assert_eq!(resolution.matches("      parameters:\n").count(), 1);
    assert!(resolution.contains("        - name: subject_kind\n"));
    assert!(resolution.contains("        - name: subject_id\n"));
    assert!(resolution.contains("        - name: Idempotency-Key\n"));
}
