use super::*;

#[test]
fn workspace_schema_follows_serde_request_response_enums_and_nested_fields() {
    let request: serde_json::Value = serde_json::from_str(
        &schema_json::<WorkspaceRegisterRequest>(Contract::Deserialize),
    )
    .unwrap();
    assert_eq!(request["type"], "object");
    assert_eq!(request["additionalProperties"], false);
    assert_eq!(request["required"], serde_json::json!(["canonical_root"]));
    assert_eq!(request["properties"]["canonical_root"]["type"], "string");
    assert!(request["properties"].get("display_name").is_some());

    let response: serde_json::Value = serde_json::from_str(
        &schema_json::<WorkspaceRegisterResponse>(Contract::Serialize),
    )
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
    let registration: serde_json::Value = serde_json::from_str(&schema_json::<
        WorkspaceRegisterRequest,
    >(Contract::Deserialize))
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

    let message: serde_json::Value = serde_json::from_str(&schema_json::<AgentMessageCreateInput>(
        Contract::Deserialize,
    ))
    .unwrap();
    assert_eq!(message["additionalProperties"], false);
    assert_eq!(
        message["required"],
        serde_json::json!(["mode", "sender_agent_id", "input"])
    );
    // The actual handler case-folds mode and reports bad modes itself;
    // inventing a schema-only enum would reject valid handler input.
    assert_eq!(message["properties"]["mode"]["type"], "string");

    let messages: serde_json::Value = serde_json::from_str(
        &schema_json::<AgentMessageListResponse>(Contract::Serialize),
    )
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
        serde_json::from_str(&schema_json::<OperatorErrorResponse>(Contract::Serialize)).unwrap();
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
    let model_request: serde_json::Value = serde_json::from_str(&schema_json::<
        ProviderModelDiscoveryRequest,
    >(Contract::Deserialize))
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
    assert!(
        serde_json::from_value::<runtime_core::ProviderModelDiscoveryRequest>(
            serde_json::json!({"setting_sources_intent":{"kind":"isolated"},"force_refresh":true})
        )
        .is_ok()
    );
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
            route_operation(yaml, path, method).contains(&format!("#/components/schemas/{schema}")),
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

    let rebind: serde_json::Value = serde_json::from_str(
        &schema_json::<WorkspaceAgentRebindRequest>(Contract::Deserialize),
    )
    .unwrap();
    assert_eq!(rebind["additionalProperties"], false);
    assert_eq!(rebind["required"], serde_json::json!(["expected_revision"]));
    assert_eq!(
        rebind["properties"]["cleanup_previous_worktree"]["type"],
        "boolean"
    );

    let notice_update: serde_json::Value = serde_json::from_str(&schema_json::<
        WorkspaceNoticePolicyUpdate,
    >(Contract::Deserialize))
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
        serde_json::from_str(&schema_json::<PublicSendTurnInput>(Contract::Deserialize)).unwrap();
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
    assert!(
        serde_json::from_value::<UpdateAgentPermissionInput>(serde_json::json!({
            "expected_revision": 2, "permission_intent": {"kind":"provider_default", "force":true}
        }))
        .is_err()
    );

    let compaction: serde_json::Value =
        serde_json::from_str(&schema_json::<SessionCompactionRun>(Contract::Serialize)).unwrap();
    assert_eq!(
        compaction["properties"]["outcome"]["enum"],
        serde_json::json!(["accepted", "not_performed", null])
    );
    let review: serde_json::Value = serde_json::from_str(
        &schema_json::<ManualCompactionReviewInput>(Contract::Deserialize),
    )
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

#[test]
fn workspace_agent_create_roster_recovery_and_edit_rerun_follow_real_serde() {
    let create: serde_json::Value = serde_json::from_str(
        &schema_json::<WorkspaceAgentCreateRequest>(Contract::Deserialize),
    )
    .unwrap();
    assert_eq!(create["additionalProperties"], false);
    assert_eq!(create["required"], serde_json::json!(["provider"]));
    assert!(create["properties"].get("workspace_id").is_none());
    assert_eq!(
        create["properties"]["provider"]["enum"],
        serde_json::json!(["codex", "claude", "acp"])
    );
    let route = create["properties"]["worktree"].to_string();
    for mode in ["root", "existing", "new"] {
        assert!(route.contains(mode), "missing {mode} from route selection");
    }
    assert_eq!(
        create["properties"]["onboarding"]["additionalProperties"],
        false
    );
    assert_eq!(
        create["properties"]["onboarding"]["properties"]["skill"]["additionalProperties"],
        false
    );
    assert!(serde_json::from_value::<WorkspaceAgentCreateRequest>(serde_json::json!({
            "provider":"codex", "worktree":{"mode":"root"},
            "onboarding":{"prompt":"hello","skill":{"provider":"codex", "name":"s", "display_name":null,"path":null}}
        })).is_ok());
    assert!(
        serde_json::from_value::<WorkspaceAgentCreateRequest>(serde_json::json!({
            "provider":"codex", "workspace_id":"untrusted", "worktree":{"mode":"root"}
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<WorkspaceAgentCreateRequest>(serde_json::json!({
            "provider":"codex", "worktree":{"mode":"existing","worktree_id":"wt_1","extra":true}
        }))
        .is_err()
    );

    let agent: serde_json::Value =
        serde_json::from_str(&schema_json::<WorkspaceAgentRecord>(Contract::Serialize)).unwrap();
    assert!(agent["required"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("agent_id")));
    assert_eq!(
        agent["properties"]["lifecycle_state"]["enum"],
        serde_json::json!(["active", "archived"])
    );
    assert!(agent["properties"]["recreation_policy"]["properties"]
        .get("current_preferences")
        .is_some());
    let roster: serde_json::Value = serde_json::from_str(
        &schema_json::<Vec<WorkspaceAgentRecord>>(Contract::Serialize),
    )
    .unwrap();
    assert_eq!(roster["type"], "array");
    assert!(roster["items"]["properties"].get("agent_id").is_some());

    let rebind: serde_json::Value = serde_json::from_str(&schema_json::<
        WorkspaceAgentRebindResponse,
    >(Contract::Serialize))
    .unwrap();
    assert!(rebind["properties"].get("operation").is_some());
    assert!(rebind["properties"].get("agent").is_some());
    assert!(rebind["properties"].get("newly_completed").is_none());

    let edit: serde_json::Value =
        serde_json::from_str(&schema_json::<EditAndRerunRequest>(Contract::Deserialize)).unwrap();
    assert_eq!(edit["additionalProperties"], false);
    assert_eq!(edit["required"], serde_json::json!(["edited_snapshot"]));
    assert_eq!(
        edit["properties"]["edited_snapshot"]["additionalProperties"],
        false
    );
    assert_eq!(
        edit["properties"]["edited_snapshot"]["properties"]["image_refs"]["items"]
            ["additionalProperties"],
        false
    );
    assert_eq!(
        edit["properties"]["edited_snapshot"]["properties"]["invocation"]["additionalProperties"],
        false
    );
    let archive_review: serde_json::Value = serde_json::from_str(&schema_json::<
        OperatorAgentArchiveReviewInput,
    >(Contract::Deserialize))
    .unwrap();
    assert_eq!(archive_review["additionalProperties"], false);
    assert_eq!(
        archive_review["properties"]["resolution"]["enum"],
        serde_json::json!(["closed", "not_closed"])
    );

    let yaml = super::super::generated_openapi_yaml();
    for (path, schema) in [
        (
            "/v2/workspaces/{workspace_id}/agents",
            "WorkspaceAgentCreateRequest",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve",
            "OperatorAgentCreateReviewInput",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve",
            "OperatorAgentArchiveReviewInput",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve",
            "OperatorAgentRestoreReviewInput",
        ),
        (
            "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve",
            "OperatorNativePolicyReviewInput",
        ),
        (
            "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun",
            "EditAndRerunRequest",
        ),
    ] {
        let operation = route_operation(yaml, path, "post");
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
        (
            "/v2/workspaces/{workspace_id}/agents",
            "post",
            "WorkspaceAgentRecord",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents",
            "get",
            "WorkspaceAgentRecordList",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}",
            "get",
            "WorkspaceAgentRecord",
        ),
        ("/v2/agents/{agent_id}", "get", "WorkspaceAgentRecord"),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive",
            "post",
            "WorkspaceAgentRecord",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/restore",
            "post",
            "WorkspaceAgentRecord",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree",
            "post",
            "WorkspaceAgentRebindResponse",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup",
            "post",
            "WorkspaceAgentRebindOperation",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve",
            "post",
            "OperatorAgentCreateReviewResponse",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve",
            "post",
            "OperatorAgentArchiveReviewResponse",
        ),
        (
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve",
            "post",
            "OperatorAgentRestoreReviewResponse",
        ),
        (
            "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve",
            "post",
            "OperatorNativePolicyReviewResponse",
        ),
        (
            "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun",
            "post",
            "SessionEditRerunOperation",
        ),
        (
            "/v2/agents/{agent_id}/edits/{operation_id}",
            "get",
            "SessionEditRerunOperation",
        ),
        (
            "/v2/agents/{agent_id}/edits/{operation_id}/resolve",
            "post",
            "SessionEditRerunOperation",
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

#[test]
fn migration_retirement_event_hook_and_scoped_mcp_contracts_follow_handlers() {
    let migration: serde_json::Value = serde_json::from_str(&schema_json::<
        LegacyWorkspaceMigrationStatus,
    >(Contract::Serialize))
    .unwrap();
    assert_eq!(
        migration["properties"]["cutover_blocked"]["type"],
        "boolean"
    );
    assert_eq!(migration["properties"]["subjects"]["type"], "array");
    assert_eq!(
        migration["properties"]["subjects"]["items"]["properties"]["classification"]["enum"],
        serde_json::json!(["mapped", "archived_history", "unresolved"])
    );
    let resolution: serde_json::Value = serde_json::from_str(&schema_json::<
        LegacyWorkspaceMigrationResolutionRequest,
    >(Contract::Deserialize))
    .unwrap();
    assert_eq!(resolution["additionalProperties"], false);
    assert_eq!(resolution["required"], serde_json::json!(["action"]));
    assert_eq!(
        resolution["properties"]["action"]["enum"],
        serde_json::json!(["map", "archive"])
    );
    assert!(
        serde_json::from_value::<LegacyWorkspaceMigrationResolutionRequest>(serde_json::json!({
            "action":"map", "workspace_id":"ws_1", "force":true
        }))
        .is_err()
    );
    let retirement: serde_json::Value = serde_json::from_str(&schema_json::<
        WorkspaceRetirementReviewRequest,
    >(Contract::Deserialize))
    .unwrap();
    assert_eq!(retirement["additionalProperties"], false);
    assert_eq!(
        retirement["required"],
        serde_json::json!(["effect_id", "evidence", "resolution"])
    );
    let tagged = retirement["properties"]["resolution"].to_string();
    assert!(tagged.contains("provider_closed") && tagged.contains("recheck_settled"));
    assert!(tagged.contains("provider_session_ref") && tagged.contains("agent_revision"));

    let events: serde_json::Value = serde_json::from_str(
        &schema_json::<Vec<OperatorEventSummary>>(Contract::Serialize),
    )
    .unwrap();
    assert_eq!(events["type"], "array");
    assert!(events["items"]["properties"].get("payload").is_none());
    assert!(events["items"]["properties"].get("event_id").is_some());
    let detail: serde_json::Value =
        serde_json::from_str(&schema_json::<RuntimeEventRecord>(Contract::Serialize)).unwrap();
    assert!(detail["properties"].get("payload").is_some());
    let hooks: serde_json::Value =
        serde_json::from_str(&schema_json::<WorkspaceHooksResponse>(Contract::Serialize)).unwrap();
    assert!(hooks["properties"]["hooks"]["items"]["properties"]
        .get("fingerprint")
        .is_some());
    assert!(hooks["properties"]["hooks"]["items"]["properties"]
        .get("command")
        .is_some());
    let approval: serde_json::Value = serde_json::from_str(&schema_json::<
        WorkspaceHookApprovalInput,
    >(Contract::Deserialize))
    .unwrap();
    assert_eq!(approval["additionalProperties"], false);
    assert_eq!(
        approval["required"],
        serde_json::json!(["expected_revision", "hooks"])
    );
    assert!(approval["properties"]["hooks"]["items"]["properties"]
        .get("command")
        .is_none());
    assert!(approval["properties"]["hooks"]["items"]["properties"]
        .get("fingerprint")
        .is_some());
    let effects: serde_json::Value = serde_json::from_str(
        &schema_json::<Vec<TeamHookEffectSummary>>(Contract::Serialize),
    )
    .unwrap();
    assert!(effects["items"]["properties"].get("input").is_none());
    assert!(effects["items"]["properties"].get("hook").is_none());
    let scoped: serde_json::Value = serde_json::from_str(&schema_json::<ScopedMcpInvokeRequest>(
        Contract::Deserialize,
    ))
    .unwrap();
    assert_eq!(scoped["additionalProperties"], false);
    assert_eq!(scoped["required"], serde_json::json!(["tool_name"]));
    assert!(scoped["properties"].get("caller_agent_id").is_none());
    assert!(scoped["properties"].get("invocation_id").is_none());
    assert!(
        serde_json::from_value::<ScopedMcpInvokeRequest>(serde_json::json!({
            "tool_name":"gg_process", "args":{"mode":"status"}, "caller_agent_id":"impostor"
        }))
        .is_err()
    );

    let yaml = super::super::generated_openapi_yaml();
    for (path, method, schema) in [
        ("/v2/migrations/workspaces", "get", "LegacyWorkspaceMigrationStatus"),
        ("/v2/migrations/workspaces/preview", "post", "LegacyWorkspaceMigrationStatus"),
        ("/v2/migrations/workspaces/apply", "post", "LegacyWorkspaceMigrationApplyResponse"),
        ("/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}", "post", "LegacyWorkspaceMigrationResolutionResponse"),
        ("/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile", "post", "WorkspaceRetirementReviewReceipt"),
        ("/v2/events", "get", "OperatorEventSummaryList"),
        ("/v2/events/{event_id}", "get", "RuntimeEventRecord"),
        ("/v2/workspaces/{workspace_id}/hooks", "get", "WorkspaceHooksResponse"),
        ("/v2/workspaces/{workspace_id}/hooks/approval", "get", "TeamHookApproval"),
        ("/v2/workspaces/{workspace_id}/hooks/approval", "put", "TeamHookApproval"),
        ("/v2/workspaces/{workspace_id}/hooks/effects", "get", "TeamHookEffectSummaryList"),
        ("/v2/workspaces/{workspace_id}/hooks/effects/{parent_event_id}/{event_name}/{hook_name}", "get", "TeamHookEffectRecord"),
    ] {
        let operation = route_operation(yaml, path, method);
        assert!(operation.contains(&format!("#/components/schemas/{schema}")), "{path}");
        assert!(!operation.contains("#/components/schemas/JsonObject"), "{path}");
    }
    for (path, method, schema) in [
        (
            "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}",
            "post",
            "LegacyWorkspaceMigrationResolutionRequest",
        ),
        (
            "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile",
            "post",
            "WorkspaceRetirementReviewRequest",
        ),
        (
            "/v2/workspaces/{workspace_id}/hooks/approval",
            "put",
            "WorkspaceHookApprovalInput",
        ),
        ("/v2/mcp/invoke", "post", "ScopedMcpInvokeRequest"),
    ] {
        let operation = route_operation(yaml, path, method);
        let request = operation.split("      responses:\n").next().unwrap();
        assert!(
            request.contains(&format!("#/components/schemas/{schema}")),
            "{path}"
        );
    }
    for path in [
        "/v2/migrations/workspaces/preview",
        "/v2/migrations/workspaces/apply",
    ] {
        assert!(
            !route_operation(yaml, path, "post").contains("requestBody:"),
            "{path}"
        );
    }
    // The tool-gateway handler returns a dynamic serde_json::Value for its
    // variable result payload; do not advertise an invented fixed DTO.
    assert!(
        route_operation(yaml, "/v2/mcp/invoke", "post").contains("#/components/schemas/JsonObject")
    );
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
