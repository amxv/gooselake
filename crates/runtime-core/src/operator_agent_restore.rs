use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{OperationActor, RuntimeError, WorkspaceAgentRecord};

/// An operator restore may attach native provider state before the restored
/// agent can be committed. The immutable request and exact original attachment
/// are stored before that first provider call.
#[derive(Debug, Clone)]
pub struct OperatorAgentRestoreCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: String,
    pub workspace_id: String,
    pub agent_id: String,
    pub expected_revision: u64,
    pub provider: String,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub session_updated_at: i64,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone)]
pub enum OperatorAgentRestoreAdmission {
    Execute {
        operation_id: String,
    },
    Replay {
        operation_id: String,
        exact_result: WorkspaceAgentRecord,
    },
    Unresolved {
        operation_id: String,
    },
    VerifiedNotResumed {
        operation_id: String,
    },
}

impl OperatorAgentRestoreAdmission {
    pub fn into_replay_result(self) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        match self {
            Self::Replay { operation_id, exact_result } => Ok((exact_result, operation_id)),
            Self::Execute { operation_id } | Self::Unresolved { operation_id } => {
                Err(RuntimeError::operation_unresolved(
                    operation_id,
                    "provider restoration has an unproven native outcome; never resubmit without reconciliation",
                ))
            }
            Self::VerifiedNotResumed { operation_id } => {
                Err(RuntimeError::OperatorRestoreNotResumed { operation_id })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAgentRestoreResolution {
    Resumed,
    NotResumed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperatorAgentRestoreReviewInput {
    pub resolution: OperatorAgentRestoreResolution,
    pub evidence: String,
    pub provider: String,
    pub original_provider_session_ref: String,
    pub original_canonical_provider_session_ref: Option<String>,
    pub expected_revision: u64,
    pub session_updated_at: i64,
    /// A verified resumed provider attachment may have different native refs
    /// from the pre-dispatch session. The operator must supply what was seen.
    pub observed_provider_session_ref: Option<String>,
    pub observed_canonical_provider_session_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OperatorAgentRestoreReviewCommand {
    pub operation_id: String,
    pub agent_id: String,
    pub workspace_id: String,
    pub actor: OperationActor,
    pub input: OperatorAgentRestoreReviewInput,
    pub reviewed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OperatorAgentRestoreReviewResponse {
    pub operation_id: String,
    pub agent_id: String,
    pub resolution: OperatorAgentRestoreResolution,
    pub agent_revision: u64,
    pub restart_required: bool,
    pub reviewed_at_ms: i64,
}
