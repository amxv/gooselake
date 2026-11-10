use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{OperationActor, RuntimeError, WorkspaceAgentRecord};

/// Immutable admission identity for a provider-backed archive close. Persist
/// it before the provider can observe the close RPC or process termination.
#[derive(Debug, Clone)]
pub struct OperatorAgentArchiveCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: String,
    pub workspace_id: String,
    pub agent_id: String,
    pub reason: Option<String>,
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
pub enum OperatorAgentArchiveAdmission {
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
    VerifiedNotClosed {
        operation_id: String,
    },
}

impl OperatorAgentArchiveAdmission {
    pub fn into_replay_result(self) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        match self {
            Self::Replay { operation_id, exact_result } => Ok((exact_result, operation_id)),
            Self::Execute { operation_id } | Self::Unresolved { operation_id } => {
                Err(RuntimeError::operation_unresolved(
                    operation_id,
                    "native provider close has no proven terminal receipt; independently reconcile before retrying",
                ))
            }
            Self::VerifiedNotClosed { operation_id } => {
                Err(RuntimeError::OperatorArchiveNotClosed { operation_id })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAgentArchiveResolution {
    Closed,
    NotClosed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperatorAgentArchiveReviewInput {
    pub resolution: OperatorAgentArchiveResolution,
    pub evidence: String,
    pub provider: String,
    pub original_provider_session_ref: String,
    pub original_canonical_provider_session_ref: Option<String>,
    pub expected_revision: u64,
    pub session_updated_at: i64,
    /// A not-closed native attachment may only be accepted as live after an
    /// independent observation and a fresh verified startup reattachment.
    pub observed_provider_session_ref: Option<String>,
    pub observed_canonical_provider_session_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OperatorAgentArchiveReviewCommand {
    pub operation_id: String,
    pub agent_id: String,
    pub workspace_id: String,
    pub actor: OperationActor,
    pub input: OperatorAgentArchiveReviewInput,
    pub reviewed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OperatorAgentArchiveReviewResponse {
    pub operation_id: String,
    pub agent_id: String,
    pub resolution: OperatorAgentArchiveResolution,
    pub agent_revision: u64,
    pub restart_required: bool,
    pub reviewed_at_ms: i64,
}
