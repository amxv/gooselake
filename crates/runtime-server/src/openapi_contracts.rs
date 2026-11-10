use crate::http::{
    AgentMessageCreateInput, EditReviewInput, ManualCompactionReviewInput, OperatorErrorResponse,
    ProviderCapabilitiesResponse, PublicSendTurnInput, RuntimeCapabilitiesResponse,
    ScopedMcpInvokeRequest, UpdateAgentPermissionInput, UpdateAgentPreferencesInput,
    WorkspaceHookApprovalInput, WorkspaceHooksResponse, WorkspaceWorktreeCreateInput,
};
use runtime_core::{
    AgentDeliveryRecord, AgentMessageAck, AgentMessageListResponse, EditAndRerunRequest,
    LegacyWorkspaceMigrationApplyResponse, LegacyWorkspaceMigrationResolutionRequest,
    LegacyWorkspaceMigrationResolutionResponse, LegacyWorkspaceMigrationStatus, OperationDetails,
    OperationSummary, OperatorAgentArchiveReviewInput, OperatorAgentArchiveReviewResponse,
    OperatorAgentCreateReviewInput, OperatorAgentCreateReviewResponse,
    OperatorAgentRestoreReviewInput, OperatorAgentRestoreReviewResponse, OperatorEventSummary,
    OperatorNativePolicyReviewInput, OperatorNativePolicyReviewResponse, OperatorProcessSummary,
    ProcessDetails, ProviderModelDiscoveryRequest, ProviderModelDiscoveryResponse,
    ProviderPermissionMutationResult, ProviderSessionPreferencesMutationResult,
    ProviderSkillDiscoveryRequest, ProviderSkillDiscoveryResponse, RuntimeEventRecord,
    SendTurnAccepted, SessionCompactionRun, SessionContextLimitSnapshot, SessionEditRerunOperation,
    TeamHookApproval, TeamHookEffectRecord, TeamHookEffectSummary, WorkspaceAgentArchiveRequest,
    WorkspaceAgentCreateRequest, WorkspaceAgentRebindOperation, WorkspaceAgentRebindRequest,
    WorkspaceAgentRebindResponse, WorkspaceAgentRecord, WorkspaceInterruptResponse,
    WorkspaceLeadTransitionRequest, WorkspaceLeadTransitionResponse, WorkspaceNoticePolicyRecord,
    WorkspaceNoticePolicyUpdate, WorkspaceRecord, WorkspaceRegisterRequest,
    WorkspaceRegisterResponse, WorkspaceRetirementPreflight, WorkspaceRetirementRequest,
    WorkspaceRetirementResponse, WorkspaceRetirementReviewReceipt,
    WorkspaceRetirementReviewRequest, WorkspaceSystemNoticeRecord, WorkspaceWorktreeInventory,
    WorktreeCreateResponse,
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
    append_component::<WorkspaceAgentCreateRequest>(
        out,
        "WorkspaceAgentCreateRequest",
        Contract::Deserialize,
    );
    append_component::<WorkspaceAgentRecord>(out, "WorkspaceAgentRecord", Contract::Serialize);
    append_component::<Vec<WorkspaceAgentRecord>>(
        out,
        "WorkspaceAgentRecordList",
        Contract::Serialize,
    );
    append_component::<WorkspaceAgentRebindResponse>(
        out,
        "WorkspaceAgentRebindResponse",
        Contract::Serialize,
    );
    append_component::<OperatorAgentCreateReviewInput>(
        out,
        "OperatorAgentCreateReviewInput",
        Contract::Deserialize,
    );
    append_component::<OperatorAgentCreateReviewResponse>(
        out,
        "OperatorAgentCreateReviewResponse",
        Contract::Serialize,
    );
    append_component::<OperatorAgentArchiveReviewInput>(
        out,
        "OperatorAgentArchiveReviewInput",
        Contract::Deserialize,
    );
    append_component::<OperatorAgentArchiveReviewResponse>(
        out,
        "OperatorAgentArchiveReviewResponse",
        Contract::Serialize,
    );
    append_component::<OperatorAgentRestoreReviewInput>(
        out,
        "OperatorAgentRestoreReviewInput",
        Contract::Deserialize,
    );
    append_component::<OperatorAgentRestoreReviewResponse>(
        out,
        "OperatorAgentRestoreReviewResponse",
        Contract::Serialize,
    );
    append_component::<OperatorNativePolicyReviewInput>(
        out,
        "OperatorNativePolicyReviewInput",
        Contract::Deserialize,
    );
    append_component::<OperatorNativePolicyReviewResponse>(
        out,
        "OperatorNativePolicyReviewResponse",
        Contract::Serialize,
    );
    append_component::<EditAndRerunRequest>(out, "EditAndRerunRequest", Contract::Deserialize);
    append_component::<SessionEditRerunOperation>(
        out,
        "SessionEditRerunOperation",
        Contract::Serialize,
    );
    append_component::<LegacyWorkspaceMigrationStatus>(
        out,
        "LegacyWorkspaceMigrationStatus",
        Contract::Serialize,
    );
    append_component::<LegacyWorkspaceMigrationApplyResponse>(
        out,
        "LegacyWorkspaceMigrationApplyResponse",
        Contract::Serialize,
    );
    append_component::<LegacyWorkspaceMigrationResolutionRequest>(
        out,
        "LegacyWorkspaceMigrationResolutionRequest",
        Contract::Deserialize,
    );
    append_component::<LegacyWorkspaceMigrationResolutionResponse>(
        out,
        "LegacyWorkspaceMigrationResolutionResponse",
        Contract::Serialize,
    );
    append_component::<WorkspaceRetirementReviewRequest>(
        out,
        "WorkspaceRetirementReviewRequest",
        Contract::Deserialize,
    );
    append_component::<WorkspaceRetirementReviewReceipt>(
        out,
        "WorkspaceRetirementReviewReceipt",
        Contract::Serialize,
    );
    append_component::<Vec<OperatorEventSummary>>(
        out,
        "OperatorEventSummaryList",
        Contract::Serialize,
    );
    append_component::<RuntimeEventRecord>(out, "RuntimeEventRecord", Contract::Serialize);
    append_component::<WorkspaceHooksResponse>(out, "WorkspaceHooksResponse", Contract::Serialize);
    append_component::<WorkspaceHookApprovalInput>(
        out,
        "WorkspaceHookApprovalInput",
        Contract::Deserialize,
    );
    append_component::<TeamHookApproval>(out, "TeamHookApproval", Contract::Serialize);
    append_component::<Vec<TeamHookEffectSummary>>(
        out,
        "TeamHookEffectSummaryList",
        Contract::Serialize,
    );
    append_component::<TeamHookEffectRecord>(out, "TeamHookEffectRecord", Contract::Serialize);
    append_component::<ScopedMcpInvokeRequest>(
        out,
        "ScopedMcpInvokeRequest",
        Contract::Deserialize,
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
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents") => {
            Some("WorkspaceAgentCreateRequest")
        }
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve",
        ) => Some("OperatorAgentCreateReviewInput"),
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve",
        ) => Some("OperatorAgentArchiveReviewInput"),
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve",
        ) => Some("OperatorAgentRestoreReviewInput"),
        (HttpMethod::Post, "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve") => {
            Some("OperatorNativePolicyReviewInput")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun") => {
            Some("EditAndRerunRequest")
        }
        (HttpMethod::Post, "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}") => {
            Some("LegacyWorkspaceMigrationResolutionRequest")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile") => {
            Some("WorkspaceRetirementReviewRequest")
        }
        (HttpMethod::Put, "/v2/workspaces/{workspace_id}/hooks/approval") => {
            Some("WorkspaceHookApprovalInput")
        }
        (HttpMethod::Post, "/v2/mcp/invoke") => Some("ScopedMcpInvokeRequest"),
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
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents")
        | (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents/{agent_id}")
        | (HttpMethod::Get, "/v2/agents/{agent_id}")
        | (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/archive")
        | (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/restore") => {
            Some("WorkspaceAgentRecord")
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/agents") => {
            Some("WorkspaceAgentRecordList")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree") => {
            Some("WorkspaceAgentRebindResponse")
        }
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup",
        ) => Some("WorkspaceAgentRebindOperation"),
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/creations/{operation_id}/resolve",
        ) => Some("OperatorAgentCreateReviewResponse"),
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/archives/{operation_id}/resolve",
        ) => Some("OperatorAgentArchiveReviewResponse"),
        (
            HttpMethod::Post,
            "/v2/workspaces/{workspace_id}/agents/{agent_id}/restores/{operation_id}/resolve",
        ) => Some("OperatorAgentRestoreReviewResponse"),
        (HttpMethod::Post, "/v2/agents/{agent_id}/native-policy/{operation_id}/resolve") => {
            Some("OperatorNativePolicyReviewResponse")
        }
        (HttpMethod::Post, "/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun")
        | (HttpMethod::Get, "/v2/agents/{agent_id}/edits/{operation_id}")
        | (HttpMethod::Post, "/v2/agents/{agent_id}/edits/{operation_id}/resolve") => {
            Some("SessionEditRerunOperation")
        }
        (HttpMethod::Get, "/v2/migrations/workspaces")
        | (HttpMethod::Post, "/v2/migrations/workspaces/preview") => {
            Some("LegacyWorkspaceMigrationStatus")
        }
        (HttpMethod::Post, "/v2/migrations/workspaces/apply") => {
            Some("LegacyWorkspaceMigrationApplyResponse")
        }
        (HttpMethod::Post, "/v2/migrations/workspaces/resolutions/{subject_kind}/{subject_id}") => {
            Some("LegacyWorkspaceMigrationResolutionResponse")
        }
        (HttpMethod::Post, "/v2/workspaces/{workspace_id}/retirement/{operation_id}/reconcile") => {
            Some("WorkspaceRetirementReviewReceipt")
        }
        (HttpMethod::Get, "/v2/events") => Some("OperatorEventSummaryList"),
        (HttpMethod::Get, "/v2/events/{event_id}") => Some("RuntimeEventRecord"),
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks") => {
            Some("WorkspaceHooksResponse")
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks/approval")
        | (HttpMethod::Put, "/v2/workspaces/{workspace_id}/hooks/approval") => {
            Some("TeamHookApproval")
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks/effects") => {
            Some("TeamHookEffectSummaryList")
        }
        (HttpMethod::Get, "/v2/workspaces/{workspace_id}/hooks/effects/{parent_event_id}/{event_name}/{hook_name}") => {
            Some("TeamHookEffectRecord")
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
#[path = "openapi_contracts_tests.rs"]
mod tests;
