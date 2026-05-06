use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize, serde::Serialize, schemars::JsonSchema)]
pub(crate) struct ToolCallMetadata {
    #[serde(default, rename = "__gg_caller_agent_id", skip_serializing)]
    #[schemars(skip)]
    pub(crate) caller_agent_id: Option<String>,
    #[serde(default, rename = "__gg_tool_invocation_id", skip_serializing)]
    #[schemars(skip)]
    pub(crate) invocation_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GgTeamWithIdRequest {
    #[schemars(length(min = 1))]
    pub(crate) team_id: String,
    #[serde(default, flatten)]
    #[schemars(skip)]
    pub(crate) tool_call_metadata: ToolCallMetadata,
}

pub(crate) type GgTeamStatusRequest = GgTeamWithIdRequest;

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CreatorCompactionSubscription {
    Auto,
    Unsubscribed,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GgTeamMessageRequest {
    #[schemars(schema_with = "crate::schema::non_empty_string_schema")]
    pub(crate) team_id: String,
    #[schemars(schema_with = "crate::schema::non_empty_string_schema")]
    pub(crate) recipient_agent_id: String,
    #[schemars(schema_with = "crate::schema::non_empty_string_schema")]
    pub(crate) message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::schema::non_empty_string_array_schema")]
    pub(crate) image_paths: Option<Vec<String>>,
    #[serde(default, flatten)]
    #[schemars(skip)]
    pub(crate) tool_call_metadata: ToolCallMetadata,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GgTeamManageRequest {
    #[schemars(schema_with = "crate::schema::non_empty_string_schema")]
    pub(crate) team_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::schema::non_empty_string_schema")]
    pub(crate) title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        schema_with = "crate::schema::nullable_non_empty_string_schema",
        description = "Optional role instructions for add mode. This content is embedded in the canonical onboarding TeamMessage sent to the spawned member."
    )]
    pub(crate) prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        schema_with = "crate::schema::non_empty_string_array_schema",
        description = "Optional image attachment paths for add mode onboarding. Paths are validated and attached in-order after the onboarding text."
    )]
    pub(crate) image_paths: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::schema::nullable_non_empty_string_schema")]
    pub(crate) model_preset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Optional creator-scoped compaction subscription override for add mode. Defaults to `auto`; set `unsubscribed` to suppress creator-recipient compaction notices for this new member."
    )]
    pub(crate) creator_compaction_subscription: Option<CreatorCompactionSubscription>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        schema_with = "crate::schema::nullable_non_empty_string_schema",
        description = "Optional worktree slug for add mode. Omit, null, or blank to skip native worktree creation. When provided, value is normalized to lowercase and must match `[a-z0-9][a-z0-9-_]*`. Runtime derives native branch/worktree/cwd from settings and the source session repo; hook metadata fields are readable but are not a control plane for native branch/cwd selection."
    )]
    pub(crate) worktree_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Optional add-mode switch for existing-worktree reuse semantics. When true, `worktree_name` targets an already-existing managed worktree instead of creating a new one."
    )]
    pub(crate) use_existing_worktree: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::schema::non_empty_string_array_min1_schema")]
    pub(crate) remove_agent_ids: Option<Vec<String>>,
    #[serde(default, flatten)]
    #[schemars(skip)]
    pub(crate) tool_call_metadata: ToolCallMetadata,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GgProcessRunRequest {
    #[schemars(length(min = 1))]
    pub(crate) command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub(crate) cwd: Option<String>,
    #[serde(default, flatten)]
    #[schemars(skip)]
    pub(crate) tool_call_metadata: ToolCallMetadata,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GgProcessStatusRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pid: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) head_lines: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tail_lines: Option<u64>,
    #[serde(default, flatten)]
    #[schemars(skip)]
    pub(crate) tool_call_metadata: ToolCallMetadata,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GgProcessKillRequest {
    pub(crate) pid: u64,
    #[serde(default, flatten)]
    #[schemars(skip)]
    pub(crate) tool_call_metadata: ToolCallMetadata,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GgMarkdownOpenRequest {
    #[schemars(
        schema_with = "crate::schema::non_empty_string_schema",
        description = "Markdown path to open. Accepts either an absolute file path or a path relative to the resolved target session/worktree cwd. A trailing #fragment is treated as the anchor when `anchor` is omitted."
    )]
    pub(crate) path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        schema_with = "crate::schema::nullable_non_empty_string_schema",
        description = "Optional active managed-worktree branch/ref name used to resolve the target session and relative path base."
    )]
    pub(crate) branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        schema_with = "crate::schema::nullable_non_empty_string_schema",
        description = "Optional runtime session id or agent alias to target a specific session/worktree."
    )]
    pub(crate) target_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        range(min = 1),
        description = "Optional 1-based source line to scroll to after opening."
    )]
    pub(crate) line: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        schema_with = "crate::schema::nullable_non_empty_string_schema",
        description = "Optional heading anchor or fragment id to scroll to after opening. Do not include `#`."
    )]
    pub(crate) anchor: Option<String>,
    #[serde(default, flatten)]
    #[schemars(skip)]
    pub(crate) tool_call_metadata: ToolCallMetadata,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{GgMarkdownOpenRequest, GgTeamManageRequest, GgTeamMessageRequest};

    #[test]
    fn gg_team_message_schema_includes_optional_image_paths_and_required_message() {
        let schema_value =
            serde_json::to_value(schemars::schema_for!(GgTeamMessageRequest)).expect("schema");
        let required = schema_value
            .get("required")
            .and_then(Value::as_array)
            .expect("required fields should be present");
        assert!(
            required
                .iter()
                .any(|entry| entry.as_str() == Some("message"))
        );
        assert!(
            !required
                .iter()
                .any(|entry| entry.as_str() == Some("image_paths"))
        );

        let image_paths_schema = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("image_paths"))
            .expect("image_paths schema should be present");
        let image_items = image_paths_schema
            .get("items")
            .and_then(Value::as_object)
            .expect("image_paths.items should be present");
        assert_eq!(
            image_items.get("type").and_then(Value::as_str),
            Some("string")
        );
        assert_eq!(
            image_items.get("minLength").and_then(Value::as_u64),
            Some(1)
        );
    }

    #[test]
    fn gg_team_manage_schema_includes_add_and_remove_fields() {
        let schema_value =
            serde_json::to_value(schemars::schema_for!(GgTeamManageRequest)).expect("schema");
        let prompt_schema = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("prompt"))
            .and_then(Value::as_object)
            .expect("prompt schema should be present");
        assert!(
            prompt_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("canonical onboarding TeamMessage")
        );
        let prompt_variants = prompt_schema
            .get("anyOf")
            .and_then(Value::as_array)
            .expect("prompt anyOf variants should be present");
        assert!(prompt_variants.iter().any(|variant| {
            variant.get("type").and_then(Value::as_str) == Some("string")
                && variant.get("minLength").and_then(Value::as_u64) == Some(1)
        }));
        assert!(
            prompt_variants
                .iter()
                .any(|variant| variant.get("type").and_then(Value::as_str) == Some("null"))
        );
        let image_paths_schema = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("image_paths"))
            .and_then(Value::as_object)
            .expect("image_paths schema should be present");
        assert_eq!(
            image_paths_schema.get("type").and_then(Value::as_str),
            Some("array")
        );
        let image_items = image_paths_schema
            .get("items")
            .and_then(Value::as_object)
            .expect("image_paths.items should be present");
        assert_eq!(
            image_items.get("type").and_then(Value::as_str),
            Some("string")
        );
        assert_eq!(
            image_items.get("minLength").and_then(Value::as_u64),
            Some(1)
        );
        assert!(
            image_paths_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("attached in-order")
        );
        let worktree_name_schema = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("worktree_name"))
            .and_then(Value::as_object)
            .expect("worktree_name schema should be present");
        assert!(
            worktree_name_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("worktree")
        );
        assert!(
            worktree_name_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("not a control plane")
        );
        let worktree_name_variants = worktree_name_schema
            .get("anyOf")
            .and_then(Value::as_array)
            .expect("worktree_name anyOf variants should be present");
        assert!(worktree_name_variants.iter().any(|variant| {
            variant.get("type").and_then(Value::as_str) == Some("string")
                && variant.get("minLength").and_then(Value::as_u64) == Some(1)
        }));
        assert!(
            worktree_name_variants
                .iter()
                .any(|variant| variant.get("type").and_then(Value::as_str) == Some("null"))
        );
        let use_existing_worktree_schema = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("use_existing_worktree"))
            .and_then(Value::as_object)
            .expect("use_existing_worktree schema should be present");
        let has_boolean_type = use_existing_worktree_schema
            .get("type")
            .and_then(Value::as_str)
            .map(|value| value == "boolean")
            .unwrap_or(false)
            || use_existing_worktree_schema
                .get("type")
                .and_then(Value::as_array)
                .map(|variants| {
                    variants
                        .iter()
                        .any(|variant| variant.as_str() == Some("boolean"))
                })
                .unwrap_or(false)
            || use_existing_worktree_schema
                .get("anyOf")
                .and_then(Value::as_array)
                .map(|variants| {
                    variants.iter().any(|variant| {
                        variant.get("type").and_then(Value::as_str) == Some("boolean")
                    })
                })
                .unwrap_or(false);
        assert!(
            has_boolean_type,
            "use_existing_worktree schema should include a boolean variant"
        );
        assert!(
            use_existing_worktree_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("existing-worktree reuse")
        );
        let creator_compaction_subscription_schema = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("creator_compaction_subscription"))
            .and_then(Value::as_object)
            .expect("creator_compaction_subscription schema should be present");
        assert!(
            creator_compaction_subscription_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("creator-scoped compaction subscription override")
        );
        let subscription_schema_json =
            serde_json::to_string(creator_compaction_subscription_schema)
                .expect("creator_compaction_subscription schema should serialize");
        assert!(
            subscription_schema_json.contains("auto"),
            "creator_compaction_subscription schema should include the `auto` variant"
        );
        assert!(
            subscription_schema_json.contains("unsubscribed"),
            "creator_compaction_subscription schema should include the `unsubscribed` variant"
        );

        let remove_agent_ids_schema = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("remove_agent_ids"))
            .and_then(Value::as_object)
            .expect("remove_agent_ids schema should be present");
        assert_eq!(
            remove_agent_ids_schema.get("type").and_then(Value::as_str),
            Some("array")
        );
        assert_eq!(
            remove_agent_ids_schema
                .get("minItems")
                .and_then(Value::as_u64),
            Some(1)
        );
        let remove_items = remove_agent_ids_schema
            .get("items")
            .and_then(Value::as_object)
            .expect("remove_agent_ids.items should be present");
        assert_eq!(
            remove_items.get("type").and_then(Value::as_str),
            Some("string")
        );
        assert_eq!(
            remove_items.get("minLength").and_then(Value::as_u64),
            Some(1)
        );
        assert!(
            remove_agent_ids_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("must be omitted")
        );
        assert!(
            remove_agent_ids_schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("[\"agent_1\", \"agent_2\"]")
        );
    }

    #[test]
    fn gg_markdown_open_schema_exposes_branch_target_and_navigation_fields() {
        let schema_value =
            serde_json::to_value(schemars::schema_for!(GgMarkdownOpenRequest)).expect("schema");
        let required = schema_value
            .get("required")
            .and_then(Value::as_array)
            .expect("required fields should be present");
        assert!(required.iter().any(|entry| entry.as_str() == Some("path")));

        let properties = schema_value
            .get("properties")
            .and_then(Value::as_object)
            .expect("properties should be present");
        assert!(properties.contains_key("branch"));
        assert!(properties.contains_key("target_agent_id"));
        assert!(properties.contains_key("line"));
        assert!(properties.contains_key("anchor"));

        let line_schema = properties
            .get("line")
            .and_then(Value::as_object)
            .expect("line schema should be present");
        assert_eq!(
            line_schema.get("minimum").and_then(Value::as_f64),
            Some(1.0)
        );
    }
}
