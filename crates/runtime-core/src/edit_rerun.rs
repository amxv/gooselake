use serde::{Deserialize, Serialize};

use crate::{PersistedUserInputSnapshot, ProviderKind, RuntimeError};

/// An operator-owned hard fork and replacement-turn request. The original
/// admitted user input and its provider identity remain historical evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEditRerunOperation {
    pub operation_id: String,
    pub agent_id: String,
    pub target_turn_id: String,
    pub provider: ProviderKind,
    pub source_provider_session_ref: String,
    pub source_canonical_provider_session_ref: Option<String>,
    pub agent_revision: u64,
    pub expected_session_updated_at: i64,
    pub request_hash: String,
    pub idempotency_key: Option<String>,
    pub edited_snapshot: PersistedUserInputSnapshot,
    /// intended -> native_started -> fork_observed -> completed. An unknown
    /// native or commit outcome is manual_review and cannot be re-dispatched.
    pub phase: String,
    pub child_provider_session_ref: Option<String>,
    pub child_canonical_provider_session_ref: Option<String>,
    pub replacement_turn_id: Option<String>,
    pub error_code: Option<String>,
    pub review_evidence: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl SessionEditRerunOperation {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.operation_id.trim().is_empty()
            || self.agent_id.trim().is_empty()
            || self.target_turn_id.trim().is_empty()
            || self.source_provider_session_ref.trim().is_empty()
            || self.request_hash.len() != 64
            || self.created_at_ms < 0
            || self.updated_at_ms < self.created_at_ms
            || !matches!(
                self.phase.as_str(),
                "intended"
                    | "native_started"
                    | "fork_observed"
                    | "completed"
                    | "failed"
                    | "manual_review"
            )
            || self
                .idempotency_key
                .as_deref()
                .is_some_and(|key| key.trim().is_empty() || key.len() > 128 || key.trim() != key)
        {
            return Err(RuntimeError::InvalidState(
                "invalid durable edit-and-rerun operation".into(),
            ));
        }
        Ok(())
    }

    pub fn unresolved(&self) -> bool {
        matches!(
            self.phase.as_str(),
            "intended" | "native_started" | "fork_observed" | "manual_review"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditAndRerunRequest {
    pub edited_snapshot: PersistedUserInputSnapshot,
}
