use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::provider_contract::{
    ProviderCapabilities, ProviderCapabilitySupport, ProviderCompactSessionOutcome,
    ProviderCompactSessionRequest, ProviderContextLimitObservation, ProviderDiscoveryMode,
    ProviderHardForkEditRerunRequest, ProviderModelDescriptor, ProviderModelDiscoveryRequest,
    ProviderModelDiscoveryResponse, ProviderPermissionIntent, ProviderSessionLaunchPolicy,
    ProviderSessionPreferences, ProviderSettingSourcesIntent, ProviderSkillDescriptor,
    ProviderSkillDiscoveryRequest, ProviderSkillDiscoveryResponse, ProviderWorkspaceRebindEvidence,
    ProviderWorkspaceRebindRequest,
};
use crate::RuntimeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Codex,
    Claude,
    Acp,
}

impl ProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Acp => "acp",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "codex" => Some(Self::Codex),
            "claude" => Some(Self::Claude),
            "acp" => Some(Self::Acp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderMetadata {
    pub kind: ProviderKind,
    pub display_name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderModel {
    pub id: String,
    pub display_name: String,
    #[serde(default)]
    pub reasoning_levels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderAuthStatus {
    pub authenticated: bool,
    pub mode: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCreateSessionRequest {
    pub runtime_session_id: String,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub permission_mode: Option<String>,
    #[serde(default)]
    pub setting_sources: Vec<String>,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
    pub harness_version_slot: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderCreateSessionPolicyRequest {
    pub runtime_session_id: String,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub launch_policy: ProviderSessionLaunchPolicy,
    #[serde(default)]
    pub current_preferences: ProviderSessionPreferences,
    pub metadata: Option<Value>,
}

impl ProviderCreateSessionPolicyRequest {
    fn into_legacy_transport(self) -> Result<ProviderCreateSessionRequest, RuntimeError> {
        if self.current_preferences != ProviderSessionPreferences::default() {
            return Err(RuntimeError::Unsupported(
                "provider adapter has not implemented typed current session preferences"
                    .to_string(),
            ));
        }
        let permission_mode = self.launch_policy.permission_intent.resolved_mode();
        let setting_sources = self
            .launch_policy
            .resolved_setting_sources(self.cwd.as_deref())?;
        Ok(ProviderCreateSessionRequest {
            runtime_session_id: self.runtime_session_id,
            model: self.model,
            cwd: self.cwd,
            permission_mode,
            setting_sources,
            system_prompt: self.launch_policy.system_prompt,
            allowed_tools: self.launch_policy.allowed_tools,
            disallowed_tools: self.launch_policy.disallowed_tools,
            harness_version_slot: self.launch_policy.harness_version_slot,
            metadata: self.metadata,
        })
    }

    pub fn legacy_compatible(
        runtime_session_id: String,
        model: Option<String>,
        cwd: Option<String>,
        permission_mode: Option<String>,
        metadata: Option<Value>,
    ) -> Result<Self, RuntimeError> {
        let permission_intent = match permission_mode {
            Some(mode) => ProviderPermissionIntent::explicit(mode)?,
            None => ProviderPermissionIntent::ProviderDefault,
        };
        Ok(Self {
            runtime_session_id,
            model,
            cwd,
            launch_policy: ProviderSessionLaunchPolicy {
                permission_intent,
                setting_sources_intent: ProviderSettingSourcesIntent::Isolated,
                ..ProviderSessionLaunchPolicy::default()
            },
            current_preferences: ProviderSessionPreferences::default(),
            metadata,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderResumeSessionRequest {
    pub runtime_session_id: String,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub permission_mode: Option<String>,
    #[serde(default)]
    pub setting_sources: Vec<String>,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
    pub harness_version_slot: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderResumeSessionPolicyRequest {
    pub runtime_session_id: String,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub launch_policy: ProviderSessionLaunchPolicy,
    #[serde(default)]
    pub current_preferences: ProviderSessionPreferences,
    pub metadata: Option<Value>,
}

impl ProviderResumeSessionPolicyRequest {
    fn into_legacy_transport(self) -> Result<ProviderResumeSessionRequest, RuntimeError> {
        if self.current_preferences != ProviderSessionPreferences::default() {
            return Err(RuntimeError::Unsupported(
                "provider adapter has not implemented typed current session preferences"
                    .to_string(),
            ));
        }
        let permission_mode = self.launch_policy.permission_intent.resolved_mode();
        let setting_sources = self
            .launch_policy
            .resolved_setting_sources(self.cwd.as_deref())?;
        Ok(ProviderResumeSessionRequest {
            runtime_session_id: self.runtime_session_id,
            provider_session_ref: self.provider_session_ref,
            canonical_provider_session_ref: self.canonical_provider_session_ref,
            cwd: self.cwd,
            model: self.model,
            permission_mode,
            setting_sources,
            system_prompt: self.launch_policy.system_prompt,
            allowed_tools: self.launch_policy.allowed_tools,
            disallowed_tools: self.launch_policy.disallowed_tools,
            harness_version_slot: self.launch_policy.harness_version_slot,
            metadata: self.metadata,
        })
    }

    pub fn legacy_compatible(
        runtime_session_id: String,
        provider_session_ref: String,
        canonical_provider_session_ref: Option<String>,
        model: Option<String>,
        cwd: Option<String>,
        permission_mode: Option<String>,
        system_prompt: Option<String>,
        metadata: Option<Value>,
    ) -> Result<Self, RuntimeError> {
        let permission_intent = match permission_mode {
            Some(mode) => ProviderPermissionIntent::explicit(mode)?,
            None => ProviderPermissionIntent::ProviderDefault,
        };
        Ok(Self {
            runtime_session_id,
            provider_session_ref,
            canonical_provider_session_ref,
            model,
            cwd,
            launch_policy: ProviderSessionLaunchPolicy {
                permission_intent,
                setting_sources_intent: ProviderSettingSourcesIntent::Isolated,
                system_prompt,
                ..ProviderSessionLaunchPolicy::default()
            },
            current_preferences: ProviderSessionPreferences::default(),
            metadata,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderSendTurnRequest {
    pub runtime_session_id: String,
    pub turn_id: String,
    pub input: Vec<Value>,
    pub expected_turn_id: Option<String>,
    pub permission_mode: Option<String>,
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInterruptTurnRequest {
    pub runtime_session_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderApprovalResponseRequest {
    pub runtime_session_id: String,
    pub turn_id: String,
    pub approval_id: String,
    pub decision: String,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderWaitTurnRequest {
    pub runtime_session_id: String,
    pub turn_id: String,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCloseSessionRequest {
    pub runtime_session_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderSession {
    pub runtime_session_id: String,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderTurnAck {
    pub runtime_session_id: String,
    pub turn_id: String,
    pub provider_native_turn_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTurnStatus {
    InProgress,
    Completed,
    Interrupted,
    Failed,
}

impl ProviderTurnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Interrupted => "interrupted",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderTurnResult {
    pub runtime_session_id: String,
    pub turn_id: String,
    pub status: ProviderTurnStatus,
    pub usage: Option<Value>,
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ProviderRuntimeEvent {
    ApprovalRequested {
        runtime_session_id: String,
        turn_id: String,
        provider_approval_ref: String,
        tool_call_id: Option<String>,
        request: Value,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    Accept,
    Decline,
}

impl ApprovalDecision {
    pub fn parse(value: &str) -> Result<Self, RuntimeError> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "accept" | "accepted" => Ok(Self::Accept),
            "decline" | "declined" | "reject" | "rejected" => Ok(Self::Decline),
            _ => Err(RuntimeError::InvalidState(format!(
                "invalid approval decision '{}'; expected accept or decline",
                value
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Decline => "decline",
        }
    }
}

#[async_trait]
pub trait RuntimeProvider: Send + Sync {
    fn kind(&self) -> ProviderKind;

    fn metadata(&self) -> ProviderMetadata;

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    fn subscribe_events(&self) -> Option<broadcast::Receiver<ProviderRuntimeEvent>> {
        None
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(Vec::new())
    }

    async fn discover_models(
        &self,
        _req: ProviderModelDiscoveryRequest,
    ) -> Result<ProviderModelDiscoveryResponse, RuntimeError> {
        let capabilities = self.capabilities();
        let mode = capabilities.model_discovery;
        let models = if mode == ProviderDiscoveryMode::Catalog {
            self.list_models()
                .await?
                .into_iter()
                .map(|model| {
                    let mut descriptor = ProviderModelDescriptor::from_legacy(self.kind(), model);
                    descriptor.capabilities.supports_tool_calling =
                        capabilities.tools != ProviderCapabilitySupport::Unsupported;
                    descriptor.capabilities.supports_vision =
                        capabilities.images != ProviderCapabilitySupport::Unsupported;
                    descriptor
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(ProviderModelDiscoveryResponse {
            provider: self.kind(),
            mode,
            models,
        })
    }

    async fn list_skills(
        &self,
        _req: ProviderSkillDiscoveryRequest,
    ) -> Result<Vec<ProviderSkillDescriptor>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider skill discovery is not supported".to_string(),
        ))
    }

    async fn discover_skills(
        &self,
        req: ProviderSkillDiscoveryRequest,
    ) -> Result<ProviderSkillDiscoveryResponse, RuntimeError> {
        let mode = self.capabilities().skill_discovery;
        let skills = if mode == ProviderDiscoveryMode::Catalog {
            self.list_skills(req).await?
        } else {
            Vec::new()
        };
        Ok(ProviderSkillDiscoveryResponse {
            provider: self.kind(),
            mode,
            skills,
        })
    }

    async fn observe_context_limit(
        &self,
        _runtime_session_id: &str,
    ) -> Result<ProviderContextLimitObservation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider context-limit observation is not supported".to_string(),
        ))
    }

    async fn rebind_workspace(
        &self,
        _req: ProviderWorkspaceRebindRequest,
    ) -> Result<ProviderWorkspaceRebindEvidence, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider workspace rebinding is not supported".to_string(),
        ))
    }

    async fn compact_session(
        &self,
        _req: ProviderCompactSessionRequest,
    ) -> Result<ProviderCompactSessionOutcome, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider manual compaction is not supported".to_string(),
        ))
    }

    async fn hard_fork_edit_rerun(
        &self,
        _req: ProviderHardForkEditRerunRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider hard-fork edit/rerun is not supported".to_string(),
        ))
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider auth status is not supported".to_string(),
        ))
    }

    async fn auth_set_api_key(&self, _api_key: String) -> Result<ProviderAuthStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider auth api_key is not supported".to_string(),
        ))
    }

    async fn auth_import_json(
        &self,
        _auth_json: Value,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider auth import_json is not supported".to_string(),
        ))
    }

    async fn auth_import_json_text(
        &self,
        _auth_json_text: String,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider auth import_json_text is not supported".to_string(),
        ))
    }

    async fn auth_logout(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider auth logout is not supported".to_string(),
        ))
    }

    async fn create_session_with_policy(
        &self,
        req: ProviderCreateSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.create_session(req.into_legacy_transport()?).await
    }

    async fn resume_session_with_policy(
        &self,
        req: ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.resume_session(req.into_legacy_transport()?).await
    }

    async fn create_session(
        &self,
        _req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider create_session is not supported".to_string(),
        ))
    }

    async fn resume_session(
        &self,
        _req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider resume_session is not supported".to_string(),
        ))
    }

    async fn restore_turn_identity_mapping(
        &self,
        _runtime_session_id: &str,
        _turn_id: &str,
        _provider_native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider turn identity restoration is not supported".to_string(),
        ))
    }

    async fn send_turn(
        &self,
        _req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider send_turn is not supported".to_string(),
        ))
    }

    async fn interrupt_turn(&self, _req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider interrupt_turn is not supported".to_string(),
        ))
    }

    async fn respond_approval(
        &self,
        _req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider respond_approval is not supported".to_string(),
        ))
    }

    async fn wait_for_turn(
        &self,
        _req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider wait_for_turn is not supported".to_string(),
        ))
    }

    async fn close_session(&self, _req: ProviderCloseSessionRequest) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider close_session is not supported".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{ProviderCreateSessionPolicyRequest, ProviderKind, ProviderModel};
    use crate::{
        ProviderPermissionIntent, ProviderSessionLaunchPolicy, ProviderSessionPreferences,
        ProviderSettingSourcesIntent, ProviderThinkingEffort, RuntimeError,
    };
    use serde_json::json;

    #[test]
    fn provider_kind_as_str_includes_acp() {
        assert_eq!(ProviderKind::Acp.as_str(), "acp");
    }

    #[test]
    fn provider_kind_from_str_parses_acp_case_insensitively() {
        assert_eq!(ProviderKind::from_str("acp"), Some(ProviderKind::Acp));
        assert_eq!(ProviderKind::from_str(" ACP "), Some(ProviderKind::Acp));
    }

    #[test]
    fn provider_model_deserializes_legacy_json_without_reasoning_levels() {
        let model: ProviderModel = serde_json::from_value(json!({
            "id": "legacy-model",
            "display_name": "Legacy Model"
        }))
        .expect("deserialize legacy provider model");

        assert!(model.reasoning_levels.is_empty());
    }

    #[test]
    fn provider_model_serializes_client_visible_reasoning_levels_exactly() {
        let model = ProviderModel {
            id: "gpt-6-luna".to_string(),
            display_name: "GPT 6 Luna".to_string(),
            reasoning_levels: vec![
                "low".to_string(),
                "medium".to_string(),
                "high".to_string(),
                "xhigh".to_string(),
                "max".to_string(),
            ],
        };

        assert_eq!(
            serde_json::to_value(model).expect("serialize provider model"),
            json!({
                "id": "gpt-6-luna",
                "display_name": "GPT 6 Luna",
                "reasoning_levels": ["low", "medium", "high", "xhigh", "max"]
            })
        );
    }
    #[test]
    fn typed_launch_policy_projects_exactly_and_refuses_unimplemented_mutable_preferences() {
        let request = ProviderCreateSessionPolicyRequest {
            runtime_session_id: "sess_policy".to_string(),
            model: Some("gpt-6-astra".to_string()),
            cwd: Some("/repo".to_string()),
            launch_policy: ProviderSessionLaunchPolicy {
                permission_intent: ProviderPermissionIntent::Explicit {
                    mode: "workspace_write".to_string(),
                },
                setting_sources_intent: ProviderSettingSourcesIntent::Standard,
                system_prompt: Some("system".to_string()),
                allowed_tools: vec!["read".to_string()],
                disallowed_tools: vec!["danger".to_string()],
                harness_version_slot: Some("gooselake-harness-v1".to_string()),
            },
            current_preferences: ProviderSessionPreferences::default(),
            metadata: Some(json!({"source":"test"})),
        };
        let projected = request
            .clone()
            .into_legacy_transport()
            .expect("default mutable preferences can use the legacy transport");
        assert_eq!(
            projected.permission_mode.as_deref(),
            Some("workspace_write")
        );
        assert_eq!(projected.setting_sources, ["user", "project", "local"]);
        assert_eq!(projected.system_prompt.as_deref(), Some("system"));
        assert_eq!(
            projected.harness_version_slot.as_deref(),
            Some("gooselake-harness-v1")
        );

        let error = ProviderCreateSessionPolicyRequest {
            current_preferences: ProviderSessionPreferences {
                thinking_effort: Some(ProviderThinkingEffort::High),
            },
            ..request
        }
        .into_legacy_transport()
        .expect_err("mutable preferences must not be silently ignored");
        assert!(matches!(error, RuntimeError::Unsupported(_)));
    }
}
