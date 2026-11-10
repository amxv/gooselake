use super::HttpMethod;

/// These are the handlers that actually validate the optional strong ETag
/// against the mandatory JSON expected_revision before mutation. Keep this
/// projection synchronized with http::shared::check_if_match_revision calls.
pub(super) fn accepts_revision_precondition(path: &str, method: HttpMethod) -> bool {
    (method == HttpMethod::Post
        && matches!(
            path,
            "/v2/workspaces/{workspace_id}/retire"
                | "/v2/workspaces/{workspace_id}/retirement/advance"
                | "/v2/workspaces/{workspace_id}/lead"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree"
                | "/v2/agents/{agent_id}/preferences"
                | "/v2/agents/{agent_id}/permission"
                | "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve"
        ))
        || (method == HttpMethod::Put
            && matches!(
                path,
                "/v2/workspaces/{workspace_id}/notice-policy"
                    | "/v2/workspaces/{workspace_id}/hooks/approval"
            ))
}

pub(super) fn returns_revision_etag(path: &str, method: HttpMethod) -> bool {
    (method == HttpMethod::Get
        && matches!(
            path,
            "/v2/workspaces/{workspace_id}"
                | "/v2/workspaces/{workspace_id}/agents/{agent_id}"
                | "/v2/agents/{agent_id}"
                | "/v2/workspaces/{workspace_id}/notice-policy"
                | "/v2/workspaces/{workspace_id}/hooks/approval"
        ))
        || (method == HttpMethod::Post
            && matches!(
                path,
                "/v2/workspaces/{workspace_id}/lead"
                    | "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree"
                    | "/v2/agents/{agent_id}/preferences"
                    | "/v2/agents/{agent_id}/permission"
                    | "/v2/workspaces/{workspace_id}/agents/{agent_id}/restore"
                    | "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"
                    | "/v2/workspaces/{workspace_id}/agents"
            ))
        || (method == HttpMethod::Put
            && matches!(
                path,
                "/v2/workspaces/{workspace_id}/notice-policy"
                    | "/v2/workspaces/{workspace_id}/hooks/approval"
            ))
}

pub(super) fn append_etag_header(out: &mut String) {
    out.push_str("            ETag:\n");
    out.push_str("              description: Strong quoted revision of the returned authority\n");
    out.push_str("              schema:\n");
    out.push_str("                type: string\n");
}

pub(super) fn append_revision_precondition_failure(out: &mut String) {
    out.push_str("        \"412\":\n");
    out.push_str("          description: If-Match does not equal the strong ETag of the body expected_revision\n");
    out.push_str("          content:\n");
    out.push_str("            application/json:\n");
    out.push_str("              schema:\n");
    out.push_str("                $ref: \"#/components/schemas/JsonObject\"\n");
}

#[cfg(test)]
mod tests {
    #[test]
    fn emitted_revision_headers_match_revisioned_operator_contracts() {
        let yaml = super::super::generated_openapi_yaml();
        for path in [
            "/v2/workspaces/{workspace_id}/retire",
            "/v2/workspaces/{workspace_id}/retirement/advance",
            "/v2/workspaces/{workspace_id}/lead",
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree",
            "/v2/agents/{agent_id}/preferences",
            "/v2/agents/{agent_id}/permission",
            "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve",
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve",
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve",
            "/v2/workspaces/{workspace_id}/notice-policy",
            "/v2/workspaces/{workspace_id}/hooks/approval",
        ] {
            let route = yaml
                .split(&format!("  {path}:\n"))
                .nth(1)
                .unwrap_or_else(|| panic!("missing revisioned route {path}"))
                .split("\n  /")
                .next()
                .unwrap();
            let method = if path.ends_with("notice-policy") || path.ends_with("hooks/approval") {
                "put"
            } else {
                "post"
            };
            let content = route.split(&format!("    {method}:\n")).nth(1).unwrap();
            assert!(content.contains("        - name: If-Match\n"), "{path}");
            assert!(content.contains("        \"412\":\n"), "{path}");
        }
        for path in [
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree",
            "/v2/workspaces/{workspace_id}/lead",
            "/v2/agents/{agent_id}/preferences",
            "/v2/agents/{agent_id}/permission",
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/restore",
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive",
            "/v2/workspaces/{workspace_id}/agents",
        ] {
            let content = yaml
                .split(&format!("  {path}:\n"))
                .nth(1)
                .unwrap()
                .split("\n  /")
                .next()
                .unwrap();
            assert!(content.contains("            ETag:\n"), "{path}");
        }
        let rebind = yaml
            .split("  /v2/workspaces/{workspace_id}/agents/{agent_id}/worktree:\n")
            .nth(1)
            .unwrap()
            .split("\n  /")
            .next()
            .unwrap();
        assert_eq!(rebind.matches("      parameters:\n").count(), 1);
        assert!(rebind.contains("            X-Operation-ID:\n"));
        assert!(rebind.contains("        \"409\":\n"));
        let restore = yaml
            .split("  /v2/workspaces/{workspace_id}/agents/{agent_id}/restore:\n")
            .nth(1)
            .unwrap()
            .split("\n  /")
            .next()
            .unwrap();
        assert!(restore.contains("            X-Operation-ID:\n"));
        assert!(restore.contains("            ETag:\n"));
        assert!(restore.contains("        \"409\":\n"));
        let review = yaml
            .split("  /v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve:\n")
            .nth(1).unwrap().split("\n  /").next().unwrap();
        assert!(review.contains("        - name: If-Match\n"));
        assert!(review.contains("            X-Operation-ID:\n"));
        assert!(review.contains("        \"412\":\n"));
        for path in [
            "/v2/workspaces/{workspace_id}",
            "/v2/workspaces/{workspace_id}/agents/{agent_id}",
            "/v2/agents/{agent_id}",
            "/v2/workspaces/{workspace_id}/notice-policy",
            "/v2/workspaces/{workspace_id}/hooks/approval",
        ] {
            let route = yaml
                .split(&format!("  {path}:\n"))
                .nth(1)
                .unwrap()
                .split("\n  /")
                .next()
                .unwrap();
            let get = route
                .split("    get:\n")
                .nth(1)
                .unwrap()
                .split("\n    put:\n")
                .next()
                .unwrap();
            assert!(get.contains("            ETag:\n"), "{path}");
        }
    }
}
