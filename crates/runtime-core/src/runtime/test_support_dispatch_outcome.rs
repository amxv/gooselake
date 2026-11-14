use crate::{
    ApprovalRecord, NewRuntimeEvent, ProviderDispatchOutcomeCommit, RuntimeError, RuntimeStore,
    SessionRecord, TurnAdmissionRecord, TurnRecord,
};

use super::MockStore;

impl MockStore {
    pub(super) fn record_dispatch_outcome(
        &self,
        before_admission: &TurnAdmissionRecord,
        after_admission: &TurnAdmissionRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        approvals: &[(ApprovalRecord, ApprovalRecord)],
        event: &NewRuntimeEvent,
    ) -> Result<ProviderDispatchOutcomeCommit, RuntimeError> {
        let mut admissions = self.turn_admissions.lock().unwrap();
        let mut turns = self.turns.lock().unwrap();
        let mut sessions = self.sessions.lock().unwrap();
        let mut approval_cache = self.approvals.lock().unwrap();
        if admissions.get(&before_turn.id) != Some(before_admission)
            || turns.get(&before_turn.id) != Some(before_turn)
            || sessions.get(&before_session.id) != Some(before_session)
            || approvals
                .iter()
                .any(|(before, _)| approval_cache.get(&before.id) != Some(before))
            || *self.fail_turn_write.lock().unwrap()
            || *self.fail_session_write.lock().unwrap()
        {
            return Err(RuntimeError::Conflict(
                "mock dispatch authority changed".into(),
            ));
        }
        let receipt = self.append_runtime_event(event)?;
        admissions.insert(after_admission.turn_id.clone(), after_admission.clone());
        turns.insert(after_turn.id.clone(), after_turn.clone());
        sessions.insert(after_session.id.clone(), after_session.clone());
        for (_, after) in approvals {
            approval_cache.insert(after.id.clone(), after.clone());
        }
        Ok(ProviderDispatchOutcomeCommit {
            admission: after_admission.clone(),
            turn: after_turn.clone(),
            session: after_session.clone(),
            approvals: approvals.iter().map(|(_, after)| after.clone()).collect(),
            event: receipt,
        })
    }
}

macro_rules! delegate_mock_outcome {
    () => {
        fn commit_provider_dispatch_outcome(
            &self,
            before_admission: &TurnAdmissionRecord,
            after_admission: &TurnAdmissionRecord,
            before_turn: &TurnRecord,
            after_turn: &TurnRecord,
            before_session: &SessionRecord,
            after_session: &SessionRecord,
            approvals: &[(ApprovalRecord, ApprovalRecord)],
            event: &NewRuntimeEvent,
        ) -> Result<ProviderDispatchOutcomeCommit, RuntimeError> {
            self.record_dispatch_outcome(
                before_admission,
                after_admission,
                before_turn,
                after_turn,
                before_session,
                after_session,
                approvals,
                event,
            )
        }
    };
}
pub(super) use delegate_mock_outcome;
