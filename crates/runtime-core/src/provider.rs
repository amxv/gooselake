use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RuntimeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Codex,
    Claude,
}

impl ProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "codex" => Some(Self::Codex),
            "claude" => Some(Self::Claude),
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
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderResumeSessionRequest {
    pub runtime_session_id: String,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub cwd: Option<String>,
    pub metadata: Option<Value>,
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

    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(Vec::new())
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
