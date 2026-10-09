use serde::{Deserialize, Serialize};

use crate::{ProviderCompactSessionOutcome, ProviderKind, RuntimeError};

/// Durable manager-owned compaction request. Accepted is not the same as a
/// completed provider compaction; observed terminal evidence advances it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCompactionRun {
    pub run_id: String,
    pub agent_id: String,
    pub provider: ProviderKind,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub agent_revision: u64,
    pub expected_session_updated_at: i64,
    pub idempotency_key: Option<String>,
    pub phase: String,
    pub outcome: Option<ProviderCompactSessionOutcome>,
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_evidence: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl SessionCompactionRun {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.run_id.trim().is_empty()
            || self.agent_id.trim().is_empty()
            || self.provider_session_ref.trim().is_empty()
            || self.created_at_ms < 0
            || self.updated_at_ms < self.created_at_ms
            || !matches!(
                self.phase.as_str(),
                "intended"
                    | "accepted"
                    | "not_performed"
                    | "completed"
                    | "failed"
                    | "manual_review"
            )
        {
            return Err(RuntimeError::InvalidState(
                "invalid manager compaction operation state".into(),
            ));
        }
        if self
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.trim().is_empty() || key.len() > 128)
        {
            return Err(RuntimeError::InvalidState(
                "Idempotency-Key must be 1-128 nonblank characters".into(),
            ));
        }
        Ok(())
    }

    pub fn is_unresolved(&self) -> bool {
        matches!(
            self.phase.as_str(),
            "intended" | "accepted" | "manual_review"
        )
    }
}
