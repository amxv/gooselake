use crate::{
    ApprovalRecord, OperationActor, OperatorUserTurnCommand, OperatorUserTurnReplay, RuntimeError,
    SendTurnAccepted, SessionRecord, TurnAdmissionRecord, TurnRecord,
};

/// Preserve the exact operator HTTP retry identity in the same transaction as
/// canonical logical turn admission, never in an HTTP-only response cache.
pub trait OperatorUserTurnStore: Send + Sync {
    /// Preflight before any provider-side preference selection; admission
    /// rechecks the claim transactionally to close concurrent runtime races.
    fn unresolved_operator_user_turn_for_agent(
        &self,
        _agent_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        // In-memory/test stores without keyed operator admission have no
        // operator claims. Durable SQLite overrides this mandatory fence.
        Ok(None)
    }

    fn operator_user_turn_replay(
        &self,
        _actor: &OperationActor,
        _idempotency_key: &str,
        _agent_id: &str,
        _normalized_request_hash: &str,
    ) -> Result<Option<OperatorUserTurnReplay>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operator turn replay is unavailable".into(),
        ))
    }

    fn admit_operator_user_turn(
        &self,
        _command: &OperatorUserTurnCommand,
        _admission: &TurnAdmissionRecord,
        _turn: &TurnRecord,
        _session: &SessionRecord,
        _approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operator turn admission is unavailable".into(),
        ))
    }

    fn complete_operator_user_turn(
        &self,
        _operation_id: &str,
        _accepted: &SendTurnAccepted,
        _at_ms: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operator turn acceptance receipt is unavailable".into(),
        ))
    }

    fn fail_operator_user_turn_not_dispatched(
        &self,
        _operation_id: &str,
        _code: &str,
        _message: &str,
        _at_ms: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operator turn failure receipt is unavailable".into(),
        ))
    }

    fn mark_operator_user_turn_uncertain(
        &self,
        _operation_id: &str,
        _reason: &str,
        _at_ms: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operator turn review is unavailable".into(),
        ))
    }
}
