use runtime_core::{
    ApprovalRecord, OperationActor, OperatorUserTurnCommand, OperatorUserTurnReplay,
    OperatorUserTurnStore, RuntimeError, SendTurnAccepted, SessionRecord, TurnAdmissionRecord,
    TurnRecord,
};

use crate::SqliteRuntimeStore;

impl OperatorUserTurnStore for SqliteRuntimeStore {
    fn unresolved_operator_user_turn_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        self.repository()
            .unresolved_operator_user_turn_for_agent(agent_id)
    }

    fn operator_user_turn_replay(
        &self,
        actor: &OperationActor,
        idempotency_key: &str,
        agent_id: &str,
        normalized_request_hash: &str,
    ) -> Result<Option<OperatorUserTurnReplay>, RuntimeError> {
        self.repository().operator_user_turn_replay(
            actor,
            idempotency_key,
            agent_id,
            normalized_request_hash,
        )
    }

    fn admit_operator_user_turn(
        &self,
        command: &OperatorUserTurnCommand,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        self.repository()
            .admit_operator_user_turn(command, admission, turn, session, approval)
    }

    fn complete_operator_user_turn(
        &self,
        operation_id: &str,
        accepted: &SendTurnAccepted,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        self.repository()
            .complete_operator_user_turn(operation_id, accepted, at_ms)
    }

    fn fail_operator_user_turn_not_dispatched(
        &self,
        operation_id: &str,
        code: &str,
        message: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        self.repository()
            .fail_operator_user_turn_not_dispatched(operation_id, code, message, at_ms)
    }

    fn mark_operator_user_turn_uncertain(
        &self,
        operation_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        self.repository()
            .mark_operator_user_turn_uncertain(operation_id, reason, at_ms)
    }
}
