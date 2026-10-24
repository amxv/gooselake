use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::workspace::{normalize_idempotency_key, normalized_json_hash, opaque_id, unix_time_ms};
use crate::{
    OperationActor, OperationActorKind, OperationPhase, ProviderKind, RuntimeError,
    WorkspaceRetirementCommand,
};

/// The exact attachment inspected by the operator. A restore, rebind, archive,
/// or concurrent session settlement invalidates an older assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRetirementNativeIdentity {
    pub agent_id: String,
    pub provider: ProviderKind,
    pub provider_session_ref: String,
    pub canonical_provider_session_ref: Option<String>,
    pub agent_revision: Option<u64>,
    pub session_updated_at: i64,
    pub active_turn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceRetirementReviewResolution {
    /// Explicit operator assertion about an independently verified native
    /// close. This never invokes a provider adapter or repeats a close request.
    ProviderClosed {
        identity: WorkspaceRetirementNativeIdentity,
    },
    /// Inspect current durable terminal facts; prose alone cannot settle work.
    RecheckSettled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRetirementReviewRequest {
    pub effect_id: String,
    pub evidence: String,
    pub resolution: WorkspaceRetirementReviewResolution,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRetirementReviewCommand {
    pub review_id: String,
    pub workspace_id: String,
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: String,
    pub request: WorkspaceRetirementReviewRequest,
    pub normalized_request: Value,
    pub normalized_request_hash: String,
    pub reviewed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRetirementReviewReceipt {
    pub review_id: String,
    pub workspace_id: String,
    pub operation_id: String,
    pub effect_id: String,
    pub settled: bool,
    pub operation_phase: OperationPhase,
    pub unresolved_effect_ids: Vec<String>,
    pub reviewed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRetirementReviewOutcome {
    pub receipt: WorkspaceRetirementReviewReceipt,
    /// Present once, after committing an actual manual-review release. It is
    /// reconstructed from the original operation, never from this review.
    pub resume_command: Option<WorkspaceRetirementCommand>,
}

pub fn prepare_workspace_retirement_review(
    workspace_id: &str,
    operation_id: &str,
    mut request: WorkspaceRetirementReviewRequest,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<WorkspaceRetirementReviewCommand, RuntimeError> {
    request.effect_id = request.effect_id.trim().into();
    request.evidence = request.evidence.trim().into();
    let workspace_id = workspace_id.trim().to_string();
    let operation_id = operation_id.trim().to_string();
    let idempotency_key = normalize_idempotency_key(idempotency_key)?.ok_or_else(|| {
        RuntimeError::InvalidState("retirement review requires Idempotency-Key".into())
    })?;
    let normalized_request = json!({
        "workspace_id":workspace_id,"operation_id":operation_id,"request":request,
    });
    let command = WorkspaceRetirementReviewCommand {
        review_id: opaque_id("retirement_review"),
        workspace_id,
        operation_id,
        actor,
        idempotency_key,
        request,
        normalized_request_hash: normalized_json_hash(&normalized_request)?,
        normalized_request,
        reviewed_at: unix_time_ms()?,
    };
    command.validate()?;
    Ok(command)
}

impl WorkspaceRetirementReviewCommand {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.actor.kind != OperationActorKind::Operator {
            return Err(RuntimeError::Conflict(
                "retirement reconciliation requires operator authority".into(),
            ));
        }
        for (name, value) in [
            ("workspace_id", self.workspace_id.as_str()),
            ("operation_id", self.operation_id.as_str()),
            ("effect_id", self.request.effect_id.as_str()),
            ("actor", self.actor.identifier.as_str()),
            ("review_id", self.review_id.as_str()),
        ] {
            bounded(value, name, 1, 2048)?;
        }
        bounded(&self.request.evidence, "evidence", 8, 2048)?;
        let key = normalize_idempotency_key(Some(self.idempotency_key.clone()))?;
        if key.as_deref() != Some(self.idempotency_key.as_str()) {
            return Err(RuntimeError::InvalidState(
                "invalid retirement review idempotency key".into(),
            ));
        }
        if let WorkspaceRetirementReviewResolution::ProviderClosed { identity } =
            &self.request.resolution
        {
            bounded(&identity.agent_id, "agent_id", 1, 2048)?;
            bounded(
                &identity.provider_session_ref,
                "provider_session_ref",
                1,
                2048,
            )?;
            if let Some(turn_id) = &identity.active_turn_id {
                bounded(turn_id, "active_turn_id", 1, 2048)?;
            }
            if let Some(canonical) = &identity.canonical_provider_session_ref {
                bounded(canonical, "canonical_provider_session_ref", 1, 2048)?;
            }
        }
        let normalized = json!({"workspace_id":self.workspace_id,
            "operation_id":self.operation_id,"request":self.request});
        if normalized != self.normalized_request
            || normalized_json_hash(&normalized)? != self.normalized_request_hash
        {
            return Err(RuntimeError::InvalidState(
                "retirement review input differs from its normalized identity".into(),
            ));
        }
        Ok(())
    }
}

fn bounded(value: &str, field: &str, min: usize, max: usize) -> Result<(), RuntimeError> {
    if value.trim() != value || !(min..=max).contains(&value.len()) {
        return Err(RuntimeError::InvalidState(format!(
            "retirement review {field} must contain {min}..={max} trimmed bytes"
        )));
    }
    Ok(())
}
