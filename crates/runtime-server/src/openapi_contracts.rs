use crate::http::{
    AgentMessageCreateInput, EditReviewInput, ManualCompactionReviewInput, OperatorErrorResponse,
    ProviderCapabilitiesResponse, PublicSendTurnInput, RuntimeCapabilitiesResponse,
    UpdateAgentPermissionInput, UpdateAgentPreferencesInput, WorkspaceWorktreeCreateInput,
};
use runtime_core::{
    AgentDeliveryRecord, AgentMessageAck, AgentMessageListResponse, OperationDetails,
    OperationSummary, OperatorProcessSummary, ProcessDetails, ProviderModelDiscoveryRequest,
    ProviderModelDiscoveryResponse, ProviderPermissionMutationResult,
    ProviderSessionPreferencesMutationResult, ProviderSkillDiscoveryRequest,
    ProviderSkillDiscoveryResponse, SendTurnAccepted, SessionCompactionRun,
    SessionContextLimitSnapshot, WorkspaceAgentArchiveRequest, WorkspaceAgentRebindOperation,
    WorkspaceAgentRebindRequest, WorkspaceInterruptResponse, WorkspaceLeadTransitionRequest,
    WorkspaceLeadTransitionResponse, WorkspaceNoticePolicyRecord, WorkspaceNoticePolicyUpdate,
    WorkspaceRecord, WorkspaceRegisterRequest, WorkspaceRegisterResponse,
    WorkspaceRetirementPreflight, WorkspaceRetirementRequest, WorkspaceRetirementResponse,
    WorkspaceSystemNoticeRecord, WorkspaceWorktreeInventory, WorktreeCreateResponse,
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
    append_component::<RuntimeCapabilitiesResponse>(
        out,
        "RuntimeCapabilitiesResponse",
        Contract::Serialize,
    );
    append_component::<ProviderCapabilitiesResponse>(
        out,
        "ProviderCapabilitiesResponse",
        Contract::Serialize,
    );
    append_component::<ProviderModelDiscoveryRequest>(
        out,
        "ProviderModelDiscoveryRequest",
        Contract::Deserialize,
    );
    append_component::<ProviderModelDiscoveryResponse>(
        out,
        "ProviderModelDiscoveryResponse",
        Contract::Serialize,
    );
    append_component::<ProviderSkillDiscoveryRequest>(
        out,
        "ProviderSkillDiscoveryRequest",
        Contract::Deserialize,
    );
    append_component::<ProviderSkillDiscoveryResponse>(
        out,
        "ProviderSkillDiscoveryResponse",
        Contract::Serialize,
    );
    append_component::<WorkspaceWorktreeCreateInput>(
        out,
        "WorkspaceWorktreeCreateInput",
        Contract::Deserialize,
    );
    append_component::<WorktreeCreateResponse>(out, "WorktreeCreateResponse", Contract::Serialize);
    append_component::<WorkspaceWorktreeInventory>(
        out,
        "WorkspaceWorktreeInventory",
        Contract::Serialize,
    );
    append_component::<WorkspaceAgentRebindRequest>(
        out,
        "WorkspaceAgentRebindRequest",
        Contract::Deserialize,
    );
    append_component::<WorkspaceAgentRebindOperation>(
        out,
        "WorkspaceAgentRebindOperation",
        Contract::Serialize,
    );
    append_component::<WorkspaceAgentArchiveRequest>(
        out,
        "WorkspaceAgentArchiveRequest",
        Contract::Deserialize,
    );
    append_component::<WorkspaceNoticePolicyUpdate>(
        out,
        "WorkspaceNoticePolicyUpdate",
        Contract::Deserialize,
    );
    append_component::<WorkspaceNoticePolicyRecord>(
        out,
        "WorkspaceNoticePolicyRecord",
        Contract::Serialize,
    );
    append_component::<WorkspaceSystemNoticeRecord>(
        out,
        "WorkspaceSystemNoticeRecord",
        Contract::Serialize,
    );
    append_component::<Vec<OperatorProcessSummary>>(
        out,
        "OperatorProcessSummaryList",
        Contract::Serialize,
    );
    append_component::<ProcessDetails>(out, "ProcessDetails", Contract::Serialize);
    append_component::<PublicSendTurnInput>(out, "PublicSendTurnInput", Contract::Deserialize);
    append_component::<SendTurnAccepted>(out, "SendTurnAccepted", Contract::Serialize);
    append_component::<Option<SessionContextLimitSnapshot>>(
        out,
        "NullableSessionContextLimitSnapshot",
        Contract::Serialize,
    );
    append_component::<SessionContextLimitSnapshot>(
        out,
        "SessionContextLimitSnapshot",
        Contract::Serialize,
    );
    append_component::<SessionCompactionRun>(out, "SessionCompactionRun", Contract::Serialize);
    append_component::<ManualCompactionReviewInput>(
        out,
        "ManualCompactionReviewInput",
        Contract::Deserialize,
    );
    append_component::<EditReviewInput>(out, "EditReviewInput", Contract::Deserialize);
    append_component::<UpdateAgentPreferencesInput>(
        out,
        "UpdateAgentPreferencesInput",
        Contract::Deserialize,
    );
    append_component::<ProviderSessionPreferencesMutationResult>(
        out,
        "ProviderSessionPreferencesMutationResult",
        Contract::Serialize,
    );
    append_component::<UpdateAgentPermissionInput>(
        out,
        "UpdateAgentPermissionInput",
        Contract::Deserialize,
    );
    append_component::<ProviderPermissionMutationResult>(
        out,
        "ProviderPermissionMutationResult",
        Contract::Serialize,
    );
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
        (HttpMethod::Post, "/v2/providers/{provider}/models/discover") => {
            Some("ProviderModelDiscoveryRequest")
        }
        (HttpMethod::Post, "/v2/providers/{provider}/skills/discover") => {
            Some("ProviderSkillDiscoveryRequest")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/worktrees") => {
            Some("WorkspaceWorktreeCreateInput")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree") => {
            Some("WorkspaceAgentRebindRequest")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive") => {
            Some("WorkspaceAgentArchiveRequest")
        }
        (HttpMethod::Put, "/v2/workspaces/{workspace_id}/notice-policy") => {
            Some("WorkspaceNoticePolicyUpdate")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/turns") => Some("PublicSendTurnInput"),
        (HttpMethod::Post, "/v2/agents/{agent_id}/preferences") => {
            Some("UpdateAgentPreferencesInput")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/permission") => {
            Some("UpdateAgentPermissionInput")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/edits/{operation_id}/resolve") => {
            Some("EditReviewInput")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/compactions/{run_id}/resolve") => {
            Some("ManualCompactionReviewInput")
        }
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
        (HttpMethod::Get, "/v2/capabilities") => Some("RuntimeCapabilitiesResponse"),
        (HttpMethod::Get, "/v2/providers/{provider}/capabilities") => {
            Some("ProviderCapabilitiesResponse")
        }
        (HttpMethod::Post, "/v2/providers/{provider}/models/discover") => {
            Some("ProviderModelDiscoveryResponse")
        }
        (HttpMethod::Post, "/v2/providers/{provider}/skills/discover") => {
            Some("ProviderSkillDiscoveryResponse")
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/worktrees") => {
            Some("WorkspaceWorktreeInventory")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/worktrees") => {
            Some("WorktreeCreateResponse")
        }
        (
            HttpMethod::Get,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}",
        ) => Some("WorkspaceAgentRebindOperation"),
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/notice-policy")
        | (HttpMethod::Put, "/v2/workspaces/{workspace_id}/notice-policy") => {
            Some("WorkspaceNoticePolicyRecord")
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding") => {
            Some("WorkspaceSystemNoticeRecord")
        }
        (HttpMethod::Get, "/v2/processes") => Some("OperatorProcessSummaryList"),
        (HttpMethod::Get, "/v2/processes/{process_id}") => Some("ProcessDetails"),
        (HttpMethod::Post, "/v2/agents/{agent_id}/turns") => Some("SendTurnAccepted"),
        (HttpMethod::Get, "/v2/agents/{agent_id}/context-limit") => {
            Some("NullableSessionContextLimitSnapshot")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/context-limit/refresh") => {
            Some("SessionContextLimitSnapshot")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/compact")
        | (HttpMethod::Get, "/v2/agents/{agent_id}/compactions/{run_id}")
        | (HttpMethod::Post, "/v2/agents/{agent_id}/compactions/{run_id}/resolve") => {
            Some("SessionCompactionRun")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/preferences") => {
            Some("ProviderSessionPreferencesMutationResult")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/permission") => {
            Some("ProviderPermissionMutationResult")
        }
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

    #[test]
    fn provider_discovery_schema_tracks_real_wire_compatibility_and_capabilities() {
        let model_request: serde_json::Value =
            serde_json::from_str(&schema_json::<ProviderModelDiscoveryRequest>(
                Contract::Deserialize,
            ))
            .unwrap();
        assert_eq!(model_request["additionalProperties"], false);
        assert!(model_request.get("required").is_none());
        assert_eq!(
            model_request["properties"]["startup_mode"]["enum"],
            serde_json::json!(["cold", "start_runtime"])
        );
        let wire = &model_request["properties"]["setting_sources_intent"];
        // The custom Deserialize type recognizes both tagged canonical source
        // objects and legacy arrays. A schema built only from its serializer
        // would silently exclude the backward-compatible array form.
        for token in [
            "standard", "isolated", "explicit", "user", "project", "local", "array",
        ] {
            assert!(
                wire.to_string().contains(token),
                "wire schema missing {token}"
            );
        }
        assert!(
            serde_json::from_value::<runtime_core::ProviderModelDiscoveryRequest>(
                serde_json::json!({"setting_sources_intent":["user","project"]})
            )
            .is_ok()
        );
        assert!(serde_json::from_value::<runtime_core::ProviderModelDiscoveryRequest>(
            serde_json::json!({"setting_sources_intent":{"kind":"isolated"},"force_refresh":true})
        ).is_ok());
        assert!(
            serde_json::from_value::<runtime_core::ProviderModelDiscoveryRequest>(
                serde_json::json!({"setting_sources_intent":{"kind":"standard","force":true}})
            )
            .is_err()
        );

        let models: serde_json::Value = serde_json::from_str(&schema_json::<
            ProviderModelDiscoveryResponse,
        >(Contract::Serialize))
        .unwrap();
        assert!(models["properties"]["mode"]["enum"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("agent_managed")));
        assert_eq!(
            models["properties"]["models"]["items"]["properties"]["capabilities"]["properties"]
                ["supports_vision"]["type"],
            "boolean"
        );
        let capabilities: serde_json::Value = serde_json::from_str(&schema_json::<
            RuntimeCapabilitiesResponse,
        >(Contract::Serialize))
        .unwrap();
        assert_eq!(capabilities["properties"]["tools"]["type"], "array");
        assert_eq!(
            capabilities["properties"]["providers"]["items"]["properties"]["harness"]["properties"]
                ["injection_mode"]["enum"],
            serde_json::json!([
                "developer_instructions",
                "system_prompt_append",
                "scoped_mcp_only"
            ])
        );

        let yaml = super::super::generated_openapi_yaml();
        for (path, method, schema) in [
            (
                "/v2/providers/{provider}/models/discover",
                "post",
                "ProviderModelDiscoveryRequest",
            ),
            (
                "/v2/providers/{provider}/skills/discover",
                "post",
                "ProviderSkillDiscoveryRequest",
            ),
            ("/v2/capabilities", "get", "RuntimeCapabilitiesResponse"),
            (
                "/v2/providers/{provider}/capabilities",
                "get",
                "ProviderCapabilitiesResponse",
            ),
        ] {
            assert!(
                route_operation(yaml, path, method)
                    .contains(&format!("#/components/schemas/{schema}")),
                "{path}"
            );
        }
    }

    #[test]
    fn worktree_notice_and_process_contracts_follow_handler_dtos() {
        let create: serde_json::Value = serde_json::from_str(&schema_json::<
            WorkspaceWorktreeCreateInput,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(create["additionalProperties"], false);
        assert_eq!(create["required"], serde_json::json!(["worktree_name"]));
        assert!(create["properties"].get("workspace_id").is_none());

        let rebind: serde_json::Value = serde_json::from_str(&schema_json::<
            WorkspaceAgentRebindRequest,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(rebind["additionalProperties"], false);
        assert_eq!(rebind["required"], serde_json::json!(["expected_revision"]));
        assert_eq!(
            rebind["properties"]["cleanup_previous_worktree"]["type"],
            "boolean"
        );

        let notice_update: serde_json::Value =
            serde_json::from_str(&schema_json::<WorkspaceNoticePolicyUpdate>(
                Contract::Deserialize,
            ))
            .unwrap();
        assert_eq!(
            notice_update["required"],
            serde_json::json!(["expected_revision", "policy"])
        );
        assert_eq!(
            notice_update["properties"]["policy"]["additionalProperties"],
            false
        );

        let inventory: serde_json::Value = serde_json::from_str(&schema_json::<
            WorkspaceWorktreeInventory,
        >(Contract::Serialize))
        .unwrap();
        assert_eq!(inventory["properties"]["worktrees"]["type"], "array");
        assert_eq!(
            inventory["properties"]["worktrees"]["items"]["properties"]["eligible_for_assignment"]
                ["type"],
            "boolean"
        );
        let process: serde_json::Value =
            serde_json::from_str(&schema_json::<ProcessDetails>(Contract::Serialize)).unwrap();
        assert_eq!(
            process["properties"]["process"]["properties"]["status"]["type"],
            "string"
        );
        assert!(process["properties"].get("stdout_truncated").is_some());

        let yaml = super::super::generated_openapi_yaml();
        for (path, method, schema) in [
            (
                "/v2/workspaces/{workspace_id}/worktrees",
                "post",
                "WorkspaceWorktreeCreateInput",
            ),
            (
                "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree",
                "post",
                "WorkspaceAgentRebindRequest",
            ),
            (
                "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive",
                "post",
                "WorkspaceAgentArchiveRequest",
            ),
            (
                "/v2/workspaces/{workspace_id}/notice-policy",
                "put",
                "WorkspaceNoticePolicyUpdate",
            ),
        ] {
            let operation = route_operation(yaml, path, method);
            assert!(
                operation.contains(&format!("#/components/schemas/{schema}")),
                "{path}"
            );
            let request_body = operation
                .split("      responses:\n")
                .next()
                .expect("operation request metadata");
            assert!(
                !request_body.contains("#/components/schemas/JsonObject"),
                "{path}"
            );
        }
        for (path, method, schema) in [
            (
                "/v2/workspaces/{workspace_id}/worktrees",
                "get",
                "WorkspaceWorktreeInventory",
            ),
            (
                "/v2/workspaces/{workspace_id}/worktrees",
                "post",
                "WorktreeCreateResponse",
            ),
            (
                "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}",
                "get",
                "WorkspaceAgentRebindOperation",
            ),
            (
                "/v2/workspaces/{workspace_id}/notice-policy",
                "get",
                "WorkspaceNoticePolicyRecord",
            ),
            (
                "/v2/workspaces/{workspace_id}/notice-policy",
                "put",
                "WorkspaceNoticePolicyRecord",
            ),
            (
                "/v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding",
                "get",
                "WorkspaceSystemNoticeRecord",
            ),
            ("/v2/processes", "get", "OperatorProcessSummaryList"),
            ("/v2/processes/{process_id}", "get", "ProcessDetails"),
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
        }
    }

    #[test]
    fn agent_turn_context_compaction_and_policy_contracts_follow_serde() {
        let turn: serde_json::Value =
            serde_json::from_str(&schema_json::<PublicSendTurnInput>(Contract::Deserialize))
                .unwrap();
        assert_eq!(turn["additionalProperties"], false);
        assert_eq!(turn["required"], serde_json::json!(["input"]));
        assert!(turn["properties"].get("projection_source").is_none());
        assert_eq!(
            turn["properties"]["turn_settings"]["properties"]["codex_collaboration_mode"]["enum"],
            serde_json::json!(["default", "plan", null])
        );
        assert_eq!(
            turn["properties"]["turn_settings"]["properties"]["thinking_effort"]["enum"],
            serde_json::json!(["low", "medium", "high", "xhigh", "max", null])
        );

        let context: serde_json::Value = serde_json::from_str(&schema_json::<
            Option<SessionContextLimitSnapshot>,
        >(Contract::Serialize))
        .unwrap();
        assert!(context["type"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("null")));
        assert_eq!(
            context["properties"]["remaining_percentage"]["type"],
            "integer"
        );
        assert!(context["properties"].get("model_context_window").is_some());
        assert!(context["properties"].get("observation").is_none());

        let permission: serde_json::Value = serde_json::from_str(&schema_json::<
            UpdateAgentPermissionInput,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(permission["additionalProperties"], false);
        assert_eq!(
            permission["required"],
            serde_json::json!(["expected_revision", "permission_intent"])
        );
        let permitted = permission["properties"]["permission_intent"].to_string();
        for accepted in [
            "provider_default",
            "inherit_provider_configuration",
            "explicit",
            "null",
            "string",
        ] {
            assert!(
                permitted.contains(accepted),
                "missing {accepted} from serde wire schema"
            );
        }
        for valid in [
            serde_json::json!({"expected_revision": 2, "permission_intent": {"kind":"provider_default"}}),
            serde_json::json!({"expected_revision": 2, "permission_intent": "workspace_write"}),
            serde_json::json!({"expected_revision": 2, "permission_intent": null}),
        ] {
            assert!(serde_json::from_value::<UpdateAgentPermissionInput>(valid).is_ok());
        }
        assert!(serde_json::from_value::<UpdateAgentPermissionInput>(serde_json::json!({
            "expected_revision": 2, "permission_intent": {"kind":"provider_default", "force":true}
        })).is_err());

        let compaction: serde_json::Value =
            serde_json::from_str(&schema_json::<SessionCompactionRun>(Contract::Serialize))
                .unwrap();
        assert_eq!(
            compaction["properties"]["outcome"]["enum"],
            serde_json::json!(["accepted", "not_performed", null])
        );
        let review: serde_json::Value = serde_json::from_str(&schema_json::<
            ManualCompactionReviewInput,
        >(Contract::Deserialize))
        .unwrap();
        assert_eq!(review["required"], serde_json::json!(["phase", "evidence"]));
        assert_eq!(review["additionalProperties"], false);

        let yaml = super::super::generated_openapi_yaml();
        for (path, method, schema) in [
            ("/v2/agents/{agent_id}/turns", "post", "PublicSendTurnInput"),
            (
                "/v2/agents/{agent_id}/preferences",
                "post",
                "UpdateAgentPreferencesInput",
            ),
            (
                "/v2/agents/{agent_id}/permission",
                "post",
                "UpdateAgentPermissionInput",
            ),
            (
                "/v2/agents/{agent_id}/edits/{operation_id}/resolve",
                "post",
                "EditReviewInput",
            ),
            (
                "/v2/agents/{agent_id}/compactions/{run_id}/resolve",
                "post",
                "ManualCompactionReviewInput",
            ),
        ] {
            let operation = route_operation(yaml, path, method);
            let request = operation.split("      responses:\n").next().unwrap();
            assert!(
                request.contains(&format!("#/components/schemas/{schema}")),
                "{path}"
            );
            assert!(
                !request.contains("#/components/schemas/JsonObject"),
                "{path}"
            );
        }
        for (path, method, schema) in [
            ("/v2/agents/{agent_id}/turns", "post", "SendTurnAccepted"),
            (
                "/v2/agents/{agent_id}/context-limit",
                "get",
                "NullableSessionContextLimitSnapshot",
            ),
            (
                "/v2/agents/{agent_id}/context-limit/refresh",
                "post",
                "SessionContextLimitSnapshot",
            ),
            (
                "/v2/agents/{agent_id}/compact",
                "post",
                "SessionCompactionRun",
            ),
            (
                "/v2/agents/{agent_id}/compactions/{run_id}",
                "get",
                "SessionCompactionRun",
            ),
            (
                "/v2/agents/{agent_id}/compactions/{run_id}/resolve",
                "post",
                "SessionCompactionRun",
            ),
            (
                "/v2/agents/{agent_id}/preferences",
                "post",
                "ProviderSessionPreferencesMutationResult",
            ),
            (
                "/v2/agents/{agent_id}/permission",
                "post",
                "ProviderPermissionMutationResult",
            ),
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
        }
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
