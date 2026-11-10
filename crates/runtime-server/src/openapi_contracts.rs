use crate::http::{AgentMessageCreateInput, OperatorErrorResponse};
use runtime_core::{
    AgentDeliveryRecord, AgentMessageAck, AgentMessageListResponse, OperationDetails,
    OperationSummary, WorkspaceInterruptResponse, WorkspaceLeadTransitionRequest,
    WorkspaceLeadTransitionResponse, WorkspaceRecord, WorkspaceRegisterRequest,
    WorkspaceRegisterResponse, WorkspaceRetirementPreflight, WorkspaceRetirementRequest,
    WorkspaceRetirementResponse,
};
use schemars::generate::{Contract, SchemaSettings};
use schemars::JsonSchema;

use super::HttpMethod;

/// Generate OpenAPI 3.1-compatible JSON Schema objects directly from the
/// serde-backed public Rust contract. JSON flow mappings are valid YAML 1.2.
/// No second handwritten set of shape/required/enum/nullability definitions.
pub(super) fn append_schema_components(out: &mut String) {
    append_component::<WorkspaceRegisterRequest>(
        out,
        "WorkspaceRegisterRequest",
        Contract::Deserialize,
    );
    append_component::<WorkspaceRegisterResponse>(
        out,
        "WorkspaceRegisterResponse",
        Contract::Serialize,
    );
    append_component::<WorkspaceRecord>(out, "WorkspaceRecord", Contract::Serialize);
    append_component::<Vec<WorkspaceRecord>>(out, "WorkspaceRecordList", Contract::Serialize);
    append_component::<WorkspaceRetirementRequest>(
        out,
        "WorkspaceRetirementRequest",
        Contract::Deserialize,
    );
    append_component::<WorkspaceRetirementResponse>(
        out,
        "WorkspaceRetirementResponse",
        Contract::Serialize,
    );
    append_component::<WorkspaceRetirementPreflight>(
        out,
        "WorkspaceRetirementPreflight",
        Contract::Serialize,
    );
    append_component::<WorkspaceLeadTransitionRequest>(
        out,
        "WorkspaceLeadTransitionRequest",
        Contract::Deserialize,
    );
    append_component::<WorkspaceLeadTransitionResponse>(
        out,
        "WorkspaceLeadTransitionResponse",
        Contract::Serialize,
    );
    append_component::<WorkspaceInterruptResponse>(
        out,
        "WorkspaceInterruptResponse",
        Contract::Serialize,
    );
    append_component::<OperationDetails>(out, "OperationDetails", Contract::Serialize);
    append_component::<Vec<OperationSummary>>(out, "OperationSummaryList", Contract::Serialize);
    append_component::<AgentMessageCreateInput>(
        out,
        "AgentMessageCreateInput",
        Contract::Deserialize,
    );
    append_component::<AgentMessageAck>(out, "AgentMessageAck", Contract::Serialize);
    append_component::<AgentMessageListResponse>(
        out,
        "AgentMessageListResponse",
        Contract::Serialize,
    );
    append_component::<AgentDeliveryRecord>(out, "AgentDeliveryRecord", Contract::Serialize);
    append_component::<Vec<AgentDeliveryRecord>>(
        out,
        "AgentDeliveryRecordList",
        Contract::Serialize,
    );
    append_component::<OperatorErrorResponse>(out, "OperatorErrorResponse", Contract::Serialize);
}

fn append_component<T: JsonSchema>(out: &mut String, name: &str, contract: Contract) {
    out.push_str("    ");
    out.push_str(name);
    out.push_str(": ");
    out.push_str(&schema_json::<T>(contract));
    out.push('\n');
}

pub(super) fn request_schema_name(path: &str, method: HttpMethod) -> Option<&'static str> {
    match (method, path) {
        (HttpMethod::Post, "/v2/workspaces") => Some("WorkspaceRegisterRequest"),
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retire")
        | (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retirement/advance") => {
            Some("WorkspaceRetirementRequest")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/lead") => {
            Some("WorkspaceLeadTransitionRequest")
        }
        (HttpMethod::Post, "/v2/messages") => Some("AgentMessageCreateInput"),
        _ => None,
    }
}

pub(super) fn response_schema_name(path: &str, method: HttpMethod) -> Option<&'static str> {
    match (method, path) {
        (HttpMethod::Post, "/v2/workspaces") => Some("WorkspaceRegisterResponse"),
        (HttpMethod::Get, "/v2/workspaces") => Some("WorkspaceRecordList"),
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}") => Some("WorkspaceRecord"),
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/retirement-preflight") => {
            Some("WorkspaceRetirementPreflight")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retire") => {
            Some("WorkspaceRetirementResponse")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retirement/advance") => {
            Some("OperationDetails")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/lead") => {
            Some("WorkspaceLeadTransitionResponse")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/interrupt") => {
            Some("WorkspaceInterruptResponse")
        }
        (HttpMethod::Get, "/v2/operations") => Some("OperationSummaryList"),
        (HttpMethod::Get, "/v2/operations/{operation_id}") => Some("OperationDetails"),
        (HttpMethod::Post, "/v2/messages") => Some("AgentMessageAck"),
        (HttpMethod::Get, "/v2/messages") => Some("AgentMessageListResponse"),
        (HttpMethod::Get, "/v2/messages/{message_id}/deliveries")
        | (HttpMethod::Post, "/v2/messages/{message_id}/cancel") => Some("AgentDeliveryRecordList"),
        (HttpMethod::Post, "/v2/deliveries/{delivery_id}/retry") => Some("AgentDeliveryRecord"),
        _ => None,
    }
}

fn schema_json<T: JsonSchema>(contract: Contract) -> String {
    let mut settings = SchemaSettings::draft2020_12();
    settings.contract = contract;
    settings.inline_subschemas = true;
    settings.meta_schema = None;
    let schema = settings.into_generator().into_root_schema_for::<T>();
    serde_json::to_string(&schema).expect("serde-derived JSON Schema is serializable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_schema_follows_serde_request_response_enums_and_nested_fields() {
        let request: serde_json::Value = serde_json::from_str(&schema_json::<
            WorkspaceRegisterRequest,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(request["type"], "object");
        assert_eq!(request["additionalProperties"], false);
        assert_eq!(request["required"], serde_json::json!(["canonical_root"]));
        assert_eq!(request["properties"]["canonical_root"]["type"], "string");
        assert!(request["properties"].get("display_name").is_some());

        let response: serde_json::Value = serde_json::from_str(&schema_json::<
            WorkspaceRegisterResponse,
        >(Contract::Serialize))
        .unwrap();
        assert_eq!(response["properties"]["operation_id"]["type"], "string");
        let workspace = &response["properties"]["workspace"];
        assert_eq!(workspace["properties"]["workspace_id"]["type"], "string");
        assert_eq!(workspace["properties"]["revision"]["type"], "integer");
        assert!(workspace["properties"]["lifecycle_state"]["enum"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("active")));
        assert!(
            !response.to_string().contains("#/$defs/"),
            "nested schema must be inlined"
        );
        let yaml = super::super::generated_openapi_yaml();
        assert!(yaml.contains("    WorkspaceRegisterRequest: {"));
        assert!(yaml.contains("    WorkspaceRegisterResponse: {"));
        assert!(yaml.contains("    WorkspaceRecord: {"));
        assert!(yaml.contains("$ref: \"#/components/schemas/WorkspaceRegisterRequest\""));
        assert!(yaml.contains("$ref: \"#/components/schemas/WorkspaceRegisterResponse\""));
        assert!(yaml.contains("$ref: \"#/components/schemas/WorkspaceRecord\""));
    }

    #[test]
    fn typed_operator_contracts_match_real_request_and_response_shapes() {
        let registration: serde_json::Value =
            serde_json::from_str(&schema_json::<WorkspaceRegisterRequest>(
                Contract::Deserialize,
            ))
            .unwrap();
        assert_eq!(
            registration["required"],
            serde_json::json!(["canonical_root"])
        );
        assert_eq!(registration["additionalProperties"], false);

        let retirement: serde_json::Value = serde_json::from_str(&schema_json::<
            WorkspaceRetirementRequest,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(
            retirement["required"],
            serde_json::json!(["expected_revision"])
        );
        assert_eq!(retirement["additionalProperties"], false);

        let lead: serde_json::Value = serde_json::from_str(&schema_json::<
            WorkspaceLeadTransitionRequest,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(lead["required"], serde_json::json!(["expected_revision"]));
        assert_eq!(lead["additionalProperties"], false);
        assert!(lead["properties"]["lead_agent_id"]
            .to_string()
            .contains("null"));

        let message: serde_json::Value = serde_json::from_str(&schema_json::<
            AgentMessageCreateInput,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(message["additionalProperties"], false);
        assert_eq!(
            message["required"],
            serde_json::json!(["mode", "sender_agent_id", "input"])
        );
        // The actual handler case-folds mode and reports bad modes itself;
        // inventing a schema-only enum would reject valid handler input.
        assert_eq!(message["properties"]["mode"]["type"], "string");

        let messages: serde_json::Value = serde_json::from_str(&schema_json::<
            AgentMessageListResponse,
        >(Contract::Serialize))
        .unwrap();
        assert_eq!(messages["properties"]["messages"]["type"], "array");
        let operations: serde_json::Value =
            serde_json::from_str(&schema_json::<OperationDetails>(Contract::Serialize)).unwrap();
        assert_eq!(operations["properties"]["operation"]["type"], "object");
        assert!(
            operations["properties"]["operation"]["properties"]["phase"]["enum"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("manual_review"))
        );

        let error: serde_json::Value =
            serde_json::from_str(&schema_json::<OperatorErrorResponse>(Contract::Serialize))
                .unwrap();
        assert_eq!(
            error["required"],
            serde_json::json!(["error", "request_id"])
        );
        assert_eq!(
            error["properties"]["error"]["properties"]["retryable"]["type"],
            "boolean"
        );
        // serde_json::Value itself permits null; schemars emits an
        // unconstrained schema for Option<Value>, not a redundant null union.
        assert!(error["properties"]["error"]["properties"]
            .get("details")
            .is_some());
    }

    #[test]
    fn typed_operator_paths_and_error_statuses_do_not_fall_back_to_json_object() {
        let yaml = super::super::generated_openapi_yaml();
        for (path, method, schema) in [
            ("/v2/workspaces", "post", "WorkspaceRegisterRequest"),
            (
                "/v2/workspaces/{workspace_id}/retire",
                "post",
                "WorkspaceRetirementRequest",
            ),
            (
                "/v2/workspaces/{workspace_id}/lead",
                "post",
                "WorkspaceLeadTransitionRequest",
            ),
            ("/v2/messages", "post", "AgentMessageCreateInput"),
        ] {
            let operation = route_operation(yaml, path, method);
            assert!(
                operation.contains(&format!("#/components/schemas/{schema}")),
                "{path}"
            );
            assert!(
                !operation.contains("#/components/schemas/JsonObject"),
                "{path}"
            );
            assert!(
                operation.contains("default:\n"),
                "missing typed operator error: {path}"
            );
        }
        for (path, method, schema) in [
            ("/v2/workspaces", "get", "WorkspaceRecordList"),
            (
                "/v2/workspaces/{workspace_id}/retirement-preflight",
                "get",
                "WorkspaceRetirementPreflight",
            ),
            (
                "/v2/workspaces/{workspace_id}/retirement/advance",
                "post",
                "OperationDetails",
            ),
            ("/v2/operations", "get", "OperationSummaryList"),
            ("/v2/operations/{operation_id}", "get", "OperationDetails"),
            ("/v2/messages", "get", "AgentMessageListResponse"),
            (
                "/v2/messages/{message_id}/deliveries",
                "get",
                "AgentDeliveryRecordList",
            ),
            (
                "/v2/deliveries/{delivery_id}/retry",
                "post",
                "AgentDeliveryRecord",
            ),
        ] {
            let operation = route_operation(yaml, path, method);
            assert!(
                operation.contains(&format!("#/components/schemas/{schema}")),
                "{path}"
            );
        }
        let retired = route_operation(
            yaml,
            "/v2/workspaces/{workspace_id}/retirement/advance",
            "post",
        );
        assert!(retired.contains("        \"202\":\n"));
        assert!(retired.contains("        \"412\":\n"));
        assert!(!retired.contains("#/components/schemas/JsonObject"));
    }

    fn route_operation<'a>(yaml: &'a str, path: &str, method: &str) -> &'a str {
        let route = yaml
            .split(&format!("  {path}:\n"))
            .nth(1)
            .unwrap_or_else(|| panic!("missing route {path}"))
            .split("\n  /")
            .next()
            .unwrap();
        let operation = route
            .split(&format!("    {method}:\n"))
            .nth(1)
            .unwrap_or_else(|| panic!("missing method {method} in {path}"));
        let end = ["get", "post", "put", "delete"]
            .into_iter()
            .filter(|other| *other != method)
            .filter_map(|other| operation.find(&format!("\n    {other}:\n")))
            .min()
            .unwrap_or(operation.len());
        &operation[..end]
    }
}
