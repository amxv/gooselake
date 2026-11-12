use super::*;

impl MockStore {
    pub(super) fn record_native_decision_result(
        &self,
        reserved: &ApprovalRecord,
        outcome: &ApprovalRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderApprovalFinalization, RuntimeError> {
        let mut turns = self.turns.lock().unwrap();
        let mut sessions = self.sessions.lock().unwrap();
        let mut approvals = self.approvals.lock().unwrap();
        if approvals.get(&reserved.id) != Some(reserved)
            || turns.get(&before_turn.id) != Some(before_turn)
            || sessions.get(&before_session.id) != Some(before_session)
        {
            return Err(RuntimeError::Conflict(
                "mock native decision authority changed".into(),
            ));
        }
        if (before_turn != after_turn && *self.fail_turn_write.lock().unwrap())
            || (before_session != after_session && *self.fail_session_write.lock().unwrap())
        {
            return Err(RuntimeError::Io(
                "mock native decision projection fault".into(),
            ));
        }
        // Inject journal errors before any projection/cache state is changed.
        let receipt = self.append_runtime_event(event)?;
        approvals.insert(outcome.id.clone(), outcome.clone());
        turns.insert(after_turn.id.clone(), after_turn.clone());
        sessions.insert(after_session.id.clone(), after_session.clone());
        Ok(ProviderApprovalFinalization {
            approval: outcome.clone(),
            turn: after_turn.clone(),
            session: after_session.clone(),
            event: Some(receipt),
        })
    }
}
