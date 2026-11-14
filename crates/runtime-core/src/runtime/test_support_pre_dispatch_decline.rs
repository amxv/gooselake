use super::MockStore;
use crate::{
    ApprovalRecord, NewRuntimeEvent, PreDispatchApprovalDeclineCommit, RuntimeError, RuntimeStore,
    SessionRecord, TurnAdmissionRecord, TurnRecord,
};

impl MockStore {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn record_pre_dispatch_decline(
        &self,
        before_admission: &TurnAdmissionRecord,
        after_admission: &TurnAdmissionRecord,
        before_approval: &ApprovalRecord,
        after_approval: &ApprovalRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        events: &[NewRuntimeEvent],
    ) -> Result<PreDispatchApprovalDeclineCommit, RuntimeError> {
        let mut admissions = self.turn_admissions.lock().unwrap();
        let mut turns = self.turns.lock().unwrap();
        let mut sessions = self.sessions.lock().unwrap();
        let mut approvals = self.approvals.lock().unwrap();
        if admissions.get(&before_turn.id) != Some(before_admission)
            || turns.get(&before_turn.id) != Some(before_turn)
            || sessions.get(&before_session.id) != Some(before_session)
            || approvals.get(&before_approval.id) != Some(before_approval)
            || *self.fail_turn_write.lock().unwrap()
            || *self.fail_session_write.lock().unwrap()
            || events.len() != 2
        {
            return Err(RuntimeError::Conflict(
                "mock local denial owner changed".into(),
            ));
        }
        let mut receipts = Vec::with_capacity(2);
        for event in events {
            match self.append_runtime_event(event) {
                Ok(receipt) => receipts.push(receipt),
                Err(error) => {
                    let ids: Vec<_> = receipts
                        .iter()
                        .map(|row: &crate::RuntimeEventRecord| &row.event_id)
                        .collect();
                    self.events
                        .lock()
                        .unwrap()
                        .retain(|row| !ids.contains(&&row.event_id));
                    return Err(error);
                }
            }
        }
        approvals.insert(after_approval.id.clone(), after_approval.clone());
        admissions.insert(after_admission.turn_id.clone(), after_admission.clone());
        turns.insert(after_turn.id.clone(), after_turn.clone());
        sessions.insert(after_session.id.clone(), after_session.clone());
        Ok(PreDispatchApprovalDeclineCommit {
            admission: after_admission.clone(),
            approval: after_approval.clone(),
            turn: after_turn.clone(),
            session: after_session.clone(),
            events: receipts,
        })
    }
}

macro_rules! delegate_mock_decline {
    () => {
        fn decline_pre_dispatch_approval(
            &self,
            before_admission: &TurnAdmissionRecord,
            after_admission: &TurnAdmissionRecord,
            before_approval: &ApprovalRecord,
            after_approval: &ApprovalRecord,
            before_turn: &TurnRecord,
            after_turn: &TurnRecord,
            before_session: &SessionRecord,
            after_session: &SessionRecord,
            events: &[NewRuntimeEvent],
        ) -> Result<PreDispatchApprovalDeclineCommit, RuntimeError> {
            self.record_pre_dispatch_decline(
                before_admission,
                after_admission,
                before_approval,
                after_approval,
                before_turn,
                after_turn,
                before_session,
                after_session,
                events,
            )
        }
    };
}
pub(super) use delegate_mock_decline;
