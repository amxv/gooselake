use super::HttpMethod;
use crate::http::{
    AgentDeliveryListQuery, AgentMessageListQuery, MigrationSubjectPageQuery, OperationListQuery,
    OperatorEventHistoryQuery, OperatorEventStreamQuery, OperatorProcessListQuery,
    ResourcePageQuery, WorkspaceAgentListQuery, WorkspaceHookEffectsQuery, WorkspaceHooksQuery,
    WorktreeInventoryPageQuery,
};
use schemars::generate::{Contract, SchemaSettings};
use schemars::JsonSchema;
use serde_json::{json, Value};

/// Actual Axum `Query<T>` handler types are the source of query field names,
/// scalar/enum types, and accepted ranges. Do not keep a second handwritten
/// collection of query DTOs or infer fields by scraping HTTP source strings.
pub(super) fn fields(path: &str, method: HttpMethod) -> Option<Vec<QueryField>> {
    match (method, path) {
        (HttpMethod::Get, "/v2/workspaces") => Some(schema_fields::<ResourcePageQuery>()),
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/worktrees") => {
            Some(schema_fields::<WorktreeInventoryPageQuery>())
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents") => {
            Some(schema_fields::<WorkspaceAgentListQuery>())
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks") => {
            Some(schema_fields::<WorkspaceHooksQuery>())
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks/effects") => {
            Some(schema_fields::<WorkspaceHookEffectsQuery>())
        }
        (HttpMethod::Get, "/v2/messages") => Some(schema_fields::<AgentMessageListQuery>()),
        (HttpMethod::Get, "/v2/messages/{message_id}/deliveries") => {
            Some(schema_fields::<AgentDeliveryListQuery>())
        }
        (HttpMethod::Get, "/v2/operations") => Some(schema_fields::<OperationListQuery>()),
        (HttpMethod::Get, "/v2/processes") => Some(schema_fields::<OperatorProcessListQuery>()),
        (HttpMethod::Get, "/v2/events") => Some(schema_fields::<OperatorEventHistoryQuery>()),
        (HttpMethod::Get, "/v2/migrations/workspaces")
        | (HttpMethod::Post, "/v2/migrations/workspaces/preview") => {
            Some(schema_fields::<MigrationSubjectPageQuery>())
        }
        (HttpMethod::Get, "/v2/events/stream")
        | (HttpMethod::Get, "/v2/workspaces/{workspace_id}/events/stream")
        | (HttpMethod::Get, "/v2/agents/{agent_id}/events/stream")
        | (HttpMethod::Get, "/v2/processes/{process_id}/events/stream") => {
            Some(schema_fields::<OperatorEventStreamQuery>())
        }
        _ => None,
    }
}

#[derive(Debug)]
pub(super) struct QueryField {
    name: String,
    schema: Value,
    required: bool,
}

fn schema_fields<T: JsonSchema>() -> Vec<QueryField> {
    let mut settings = SchemaSettings::draft2020_12();
    settings.contract = Contract::Deserialize;
    settings.inline_subschemas = true;
    settings.meta_schema = None;
    let root = serde_json::to_value(settings.into_generator().into_root_schema_for::<T>())
        .expect("handler Query<T> schema must serialize");
    let properties = root["properties"]
        .as_object()
        .expect("Query<T> must be an object with properties");
    let required = root["required"].as_array();
    properties
        .iter()
        .map(|(name, value)| {
            let mut schema = without_omission_null(value.clone());
            if name == "limit" {
                // The handler validates these ranges; the JsonSchema range
                // annotation on its input provides min/max automatically.
                assert_eq!(schema["minimum"], json!(1), "{name} lower bound");
                assert!(schema["maximum"].is_number(), "{name} upper bound");
                schema["default"] = if schema["maximum"] == 10000 {
                    json!(2000)
                } else {
                    json!(50)
                };
            }
            if name == "lifecycle" {
                // The handler parses this String via its exact three-value
                // match, rather than an enum Deserialize (which would change
                // its existing error envelope on invalid input).
                schema["enum"] = json!(["active", "archived", "all"]);
            }
            if name == "scope" || name == "scope_id" {
                schema["description"] = json!("Scope and scope_id must be supplied together");
            }
            QueryField {
                name: name.clone(),
                schema,
                required: required
                    .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(name))),
            }
        })
        .collect()
}

/// In Axum query parsing an omitted `Option<T>` is absent, not the literal
/// JSON null. Nullable *JSON* body properties must keep null; query strings
/// must advertise only the non-null scalar/enum accepted in the URL.
fn without_omission_null(mut schema: Value) -> Value {
    let non_null_single_type =
        if let Some(types) = schema.get_mut("type").and_then(Value::as_array_mut) {
            types.retain(|ty| ty.as_str() != Some("null"));
            if types.len() == 1 {
                Some(types[0].clone())
            } else {
                None
            }
        } else {
            None
        };
    if let Some(ty) = non_null_single_type {
        schema["type"] = ty;
    }
    if let Some(variants) = schema.get_mut("enum").and_then(Value::as_array_mut) {
        variants.retain(|value| !value.is_null());
    }
    let non_null_single_option =
        if let Some(options) = schema.get_mut("anyOf").and_then(Value::as_array_mut) {
            options.retain(|option| option["type"] != "null");
            if options.len() == 1 {
                Some(options.remove(0))
            } else {
                None
            }
        } else {
            None
        };
    if let Some(Value::Object(properties)) = non_null_single_option {
        let schema_object = schema.as_object_mut().expect("query field schema object");
        schema_object.remove("anyOf");
        schema_object.extend(properties);
    }
    schema
}

pub(super) fn append(out: &mut String, fields: &[QueryField]) {
    for field in fields {
        out.push_str("        - name: ");
        out.push_str(&field.name);
        out.push('\n');
        out.push_str("          in: query\n");
        out.push_str(if field.required {
            "          required: true\n"
        } else {
            "          required: false\n"
        });
        out.push_str("          schema: ");
        out.push_str(&serde_json::to_string(&field.schema).expect("query schema JSON serializes"));
        out.push('\n');
    }
}

/// The exact response handlers emit this header on a following keyset page.
/// `/v2/messages` instead embeds its cursor in `AgentMessageListResponse`.
pub(super) fn returns_next_cursor(path: &str, method: HttpMethod) -> bool {
    matches!(
        (method, path),
        (HttpMethod::Get, "/v2/workspaces")
            | (HttpMethod::Get, "/v2/workspaces/{workspace_id}/worktrees")
            | (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents")
            | (HttpMethod::Get, "/v2/operations")
            | (HttpMethod::Get, "/v2/processes")
            | (HttpMethod::Get, "/v2/events")
            | (HttpMethod::Get, "/v2/migrations/workspaces")
            | (HttpMethod::Post, "/v2/migrations/workspaces/preview")
            | (
                HttpMethod::Get,
                "/v2/workspaces/{workspace_id}/hooks/effects"
            )
            | (HttpMethod::Get, "/v2/messages/{message_id}/deliveries")
    )
}
