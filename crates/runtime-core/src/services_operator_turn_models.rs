use serde_json::Value;

use crate::{OperationActor, RuntimeError, SendTurnAccepted};

/// Trusted operator request identity, reserved with the logical turn and its
/// provider-dispatch policy in one durable transaction.
#[derive(Debug, Clone)]
pub struct OperatorUserTurnCommand {
    pub operation_id: String,
    pub agent_id: String,
    pub workspace_id: String,
    pub actor: OperationActor,
    pub idempotency_key: String,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone)]
pub enum OperatorUserTurnReplay {
    Completed {
        operation_id: String,
        accepted: SendTurnAccepted,
    },
    NotDispatched {
        operation_id: String,
        code: String,
        message: String,
    },
    Unresolved {
        operation_id: String,
    },
}

impl OperatorUserTurnReplay {
    pub fn into_result(self) -> Result<(SendTurnAccepted, String), RuntimeError> {
        match self {
            Self::Completed {
                operation_id,
                accepted,
            } => Ok((accepted, operation_id)),
            Self::NotDispatched {
                operation_id,
                code,
                message,
            } => Err(RuntimeError::OperatorTurnNotDispatched {
                operation_id,
                code,
                message,
            }),
            Self::Unresolved { operation_id } => Err(RuntimeError::operation_unresolved(
                operation_id.clone(),
                format!("inspect /v2/operations/{operation_id}; the original logical turn must not be resent"),
            )),
        }
    }

    pub fn operation_id(&self) -> &str {
        match self {
            Self::Completed { operation_id, .. }
            | Self::NotDispatched { operation_id, .. }
            | Self::Unresolved { operation_id } => operation_id,
        }
    }
}
