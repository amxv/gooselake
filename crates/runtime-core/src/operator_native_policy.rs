use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{OperationActor, RuntimeError};

/// Provider policy writes are native effects. Their immutable operator intent
/// must exist before the provider is called, including when the HTTP reply is
/// subsequently lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorNativePolicyKind {
    Preferences,
    Permission,
}

impl OperatorNativePolicyKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preferences => "preferences",
            Self::Permission => "permission",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperatorNativePolicyCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: String,
    pub workspace_id: String,
    pub agent_id: String,
    pub kind: OperatorNativePolicyKind,
    pub expected_revision: u64,
    pub provider: String,
    pub provider_session_ref: Option<String>,
    pub canonical_provider_session_ref: Option<String>,
    pub session_updated_at: i64,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone)]
pub enum OperatorNativePolicyAdmission {
    Execute {
        operation_id: String,
    },
    Replay {
        operation_id: String,
        exact_result: Value,
    },
    Unresolved {
        operation_id: String,
    },
    VerifiedNotApplied {
        operation_id: String,
    },
}

impl OperatorNativePolicyAdmission {
    pub fn into_replay_result<T: serde::de::DeserializeOwned>(
        self,
    ) -> Result<(T, String), RuntimeError> {
        match self {
            Self::Replay {
                operation_id,
                exact_result,
            } => {
                let receipt = serde_json::from_value(exact_result).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "invalid persisted native-policy receipt: {error}"
                    ))
                })?;
                Ok((receipt, operation_id))
            }
            Self::Unresolved { operation_id } | Self::Execute { operation_id } => {
                Err(RuntimeError::operation_unresolved(
                    operation_id,
                    "native provider policy outcome is not proven; do not dispatch it again",
                ))
            }
            Self::VerifiedNotApplied { operation_id } => {
                Err(RuntimeError::OperatorPolicyNotApplied { operation_id })
            }
        }
    }
}

/// Operator-verified native state, not a request to invoke the provider again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperatorNativePolicyReviewResolution {
    Applied,
    NotApplied,
}

impl OperatorNativePolicyReviewResolution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::NotApplied => "not_applied",
        }
    }
}

/// The operator must verify the *exact* immutable native attachment recorded
/// in the original operation, rather than asserting that any session changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperatorNativePolicyReviewInput {
    pub resolution: OperatorNativePolicyReviewResolution,
    pub evidence: String,
    pub provider: String,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub expected_revision: u64,
    pub session_updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct OperatorNativePolicyReviewCommand {
    pub operation_id: String,
    pub agent_id: String,
    pub actor: OperationActor,
    pub input: OperatorNativePolicyReviewInput,
    pub reviewed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OperatorNativePolicyReviewResponse {
    pub operation_id: String,
    pub agent_id: String,
    pub resolution: OperatorNativePolicyReviewResolution,
    pub agent_revision: u64,
    /// Fresh provider reattachment must consume the reconciled durable policy
    /// before any additional native effects or user turns are permitted.
    pub restart_required: bool,
    pub reviewed_at_ms: i64,
}
