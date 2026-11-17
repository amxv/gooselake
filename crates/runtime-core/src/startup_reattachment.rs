use crate::{
    OperationResourceClaimRecord, ProviderKind, ProviderPermissionIntent,
    ProviderResumeSessionPolicyRequest, ProviderSession, ProviderSessionLaunchPolicy,
    ProviderSessionPreferences, ProviderSettingSourcesIntent, RuntimeError, SessionRecord,
    TurnAdmissionRecord, TurnRecord, WorkspaceAgentRecord,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StartupAttachmentScope {
    WorkspaceAgent {
        workspace_id: String,
        agent_revision: u64,
    },
    StoredSession {
        workspace_id: Option<String>,
    },
}
impl StartupAttachmentScope {
    pub fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::WorkspaceAgent { workspace_id, .. } => Some(workspace_id),
            Self::StoredSession { workspace_id } => workspace_id.as_deref(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupReattachCommand {
    pub operation_id: String,
    pub startup_instance_id: String,
    pub session_id: String,
    pub provider: ProviderKind,
    pub request: ProviderResumeSessionPolicyRequest,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupAttachmentAuthority {
    pub scope: StartupAttachmentScope,
    pub provider: ProviderKind,
    pub request: ProviderResumeSessionPolicyRequest,
    pub worktree_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupPendingOperatorTurn {
    pub operation_id: String,
    pub normalized_request_hash: String,
    pub effect_id: String,
    pub claim: OperationResourceClaimRecord,
    pub admission: TurnAdmissionRecord,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupTerminalMetadataWitness {
    pub turn: TurnRecord,
    pub admission: Option<TurnAdmissionRecord>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupReattachIntent {
    pub version: u32,
    pub command: StartupReattachCommand,
    pub authority: StartupAttachmentAuthority,
    pub pending_operator_turn: Option<StartupPendingOperatorTurn>,
    pub terminal_metadata_witness: Option<StartupTerminalMetadataWitness>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupReattachGrant {
    pub intent: StartupReattachIntent,
    pub normalized_request_hash: String,
    pub claim: OperationResourceClaimRecord,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupReattachReceipt {
    pub operation_id: String,
    pub normalized_request_hash: String,
    pub session_id: String,
    pub provider: ProviderKind,
    pub acknowledgement: ProviderSession,
    pub acknowledged_at: i64,
    pub metadata_terminal_event_id: Option<String>,
}
#[derive(Debug, Clone)]
pub struct StartupReattachCompletion {
    pub receipt: StartupReattachReceipt,
    pub session: SessionRecord,
}
#[derive(Debug, Clone)]
pub enum StartupReattachAdmission {
    Execute(StartupReattachGrant),
    Replay(StartupReattachReceipt),
    Unresolved { operation_id: String },
}
#[derive(Debug, Clone)]
pub enum StartupReattachHold {
    Completed(StartupReattachCompletion),
    Held { operation_id: String },
}

pub fn startup_review_status_blocked(status: &str) -> bool {
    matches!(
        status,
        "policy_recovery_required"
            | "restore_recovery_required"
            | "archive_recovery_required"
            | "create_recovery_required"
    )
}
pub fn startup_resume_request_from_authority(
    session: &SessionRecord,
    agent: Option<&WorkspaceAgentRecord>,
) -> Result<(ProviderKind, ProviderResumeSessionPolicyRequest), RuntimeError> {
    let provider = ProviderKind::from_str(&session.provider).ok_or_else(|| {
        RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
    })?;
    let primary = session.provider_session_ref.clone().ok_or_else(|| {
        RuntimeError::InvalidState("startup attachment lacks provider_session_ref".into())
    })?;
    let canonical = session.canonical_provider_session_ref.clone();
    if primary.trim().is_empty()
        || primary.trim() != primary
        || canonical
            .as_deref()
            .is_some_and(|v| v.trim().is_empty() || v.trim() != v)
        || (provider == ProviderKind::Claude && canonical.is_none())
        || (matches!(provider, ProviderKind::Codex | ProviderKind::Acp)
            && canonical.as_deref().is_some_and(|v| v != primary))
    {
        return Err(RuntimeError::InvalidState(
            "startup attachment lacks exact supported native identity".into(),
        ));
    }
    let request = if let Some(agent) = agent {
        let policy = &agent.recreation_policy;
        if agent.agent_id != session.id
            || policy.provider != provider
            || agent.provider_session_ref != session.provider_session_ref
            || agent.canonical_provider_session_ref != session.canonical_provider_session_ref
            || agent.metadata != session.metadata
        {
            return Err(RuntimeError::Conflict(
                "workspace agent and stored session attachment disagree".into(),
            ));
        }
        ProviderResumeSessionPolicyRequest {
            runtime_session_id: session.id.clone(),
            provider_session_ref: primary,
            canonical_provider_session_ref: canonical,
            model: policy.model.clone(),
            cwd: Some(policy.authoritative_cwd.clone()),
            launch_policy: policy.launch_policy(),
            current_preferences: policy.current_preferences.clone(),
            metadata: Some(agent.metadata.clone()),
        }
    } else {
        let permission_intent = match session.permission_mode.clone() {
            Some(mode) => ProviderPermissionIntent::explicit(mode)?,
            None if provider == ProviderKind::Claude => {
                ProviderPermissionIntent::InheritProviderConfiguration
            }
            None => ProviderPermissionIntent::ProviderDefault,
        };
        ProviderResumeSessionPolicyRequest {
            runtime_session_id: session.id.clone(),
            provider_session_ref: primary,
            canonical_provider_session_ref: canonical,
            model: session.model.clone(),
            cwd: session.cwd.clone(),
            launch_policy: ProviderSessionLaunchPolicy {
                permission_intent,
                setting_sources_intent: ProviderSettingSourcesIntent::Isolated,
                system_prompt: session.system_prompt.clone(),
                allowed_tools: Vec::new(),
                disallowed_tools: Vec::new(),
                harness_version_slot: None,
            },
            current_preferences: ProviderSessionPreferences::default(),
            metadata: Some(session.metadata.clone()),
        }
    };
    Ok((provider, request))
}
pub fn startup_ack_identity_matches(intent: &StartupReattachIntent, ack: &ProviderSession) -> bool {
    let request = &intent.authority.request;
    ack.runtime_session_id == request.runtime_session_id
        && ack.provider_session_ref == request.provider_session_ref
        && (ack.canonical_provider_session_ref == request.canonical_provider_session_ref
            || (matches!(
                intent.authority.provider,
                ProviderKind::Codex | ProviderKind::Acp
            ) && request.canonical_provider_session_ref.is_none()
                && ack.canonical_provider_session_ref.as_deref()
                    == Some(request.provider_session_ref.as_str())))
}
pub fn startup_ack_is_identical(left: &ProviderSession, right: &ProviderSession) -> bool {
    left.runtime_session_id == right.runtime_session_id
        && left.provider_session_ref == right.provider_session_ref
        && left.canonical_provider_session_ref == right.canonical_provider_session_ref
}
