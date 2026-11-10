use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    OperationActor, RuntimeError, SessionRecord, TeamHookPreAddContext,
    WorkspaceAgentOnboardingRequest, WorkspaceAgentRecord,
};

/// Immutable operator intent admitted before any worktree, shell hook, or
/// provider create effect. `reserved_agent_id` is allocated only once and is
/// never replaced by a retry of the original key.
#[derive(Debug, Clone)]
pub struct OperatorAgentCreateCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: String,
    pub workspace_id: String,
    pub reserved_agent_id: String,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub requested_at: i64,
    /// Linked, independently journaled Git worktree operation. Recorded only
    /// after its exact receipt is available; not part of caller key identity.
    pub worktree_operation_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum OperatorAgentCreateAdmission {
    Execute {
        operation_id: String,
        reserved_agent_id: String,
    },
    Replay {
        operation_id: String,
        exact_result: WorkspaceAgentRecord,
    },
    Unresolved {
        operation_id: String,
    },
    VerifiedNotCreated {
        operation_id: String,
    },
}

impl OperatorAgentCreateAdmission {
    pub fn into_replay_result(self) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        match self {
            Self::Replay { operation_id, exact_result } => Ok((exact_result, operation_id)),
            Self::Execute { operation_id, .. } | Self::Unresolved { operation_id } => {
                Err(RuntimeError::operation_unresolved(
                    operation_id,
                    "native agent creation has no proven terminal receipt; do not start another provider session or Git checkout",
                ))
            }
            Self::VerifiedNotCreated { operation_id } => {
                Err(RuntimeError::OperatorAgentNotCreated { operation_id })
            }
        }
    }
}

/// Frozen *before* starting the provider. It binds the approved pre-add hook
/// context and onboarding to the exact reserved identity and launch policy.
/// Review after an uncertain native provider result must use this snapshot,
/// never a fresh mutable request/project configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorAgentCreateLaunchSnapshot {
    pub session: SessionRecord,
    pub agent: WorkspaceAgentRecord,
    pub onboarding: WorkspaceAgentOnboardingRequest,
    pub hooks: Option<TeamHookPreAddContext>,
    pub worktree_operation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAgentCreateResolution {
    Created,
    NotCreated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperatorAgentCreateReviewInput {
    pub resolution: OperatorAgentCreateResolution,
    /// Independent native-session observation, not a provider RPC request.
    pub evidence: String,
    pub reserved_agent_id: String,
    pub provider: String,
    /// Required for created, absent for not_created.
    pub observed_provider_session_ref: Option<String>,
    pub observed_canonical_provider_session_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OperatorAgentCreateReviewCommand {
    pub operation_id: String,
    pub workspace_id: String,
    pub actor: OperationActor,
    pub input: OperatorAgentCreateReviewInput,
    pub reviewed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OperatorAgentCreateReviewResponse {
    pub operation_id: String,
    pub reserved_agent_id: String,
    pub resolution: OperatorAgentCreateResolution,
    pub restart_required: bool,
    pub reviewed_at_ms: i64,
}
