use super::MockStore;
use crate::{
    NewRuntimeEvent, ProviderDispatchStartedCommit, RuntimeError, SessionRecord,
    TurnAdmissionRecord, TurnRecord,
};

impl MockStore {
    pub(super) fn commit_start_for_test(
        &self,
        before_admission: &TurnAdmissionRecord,
        after_admission: &TurnAdmissionRecord,
        before_turn: &TurnRecord,
        after_turn: &TurnRecord,
        before_session: &SessionRecord,
        after_session: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderDispatchStartedCommit, RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        let mut admissions = self.turn_admissions.lock().unwrap();
        let mut turns = self.turns.lock().unwrap();
        let mut sessions = self.sessions.lock().unwrap();
        if before_admission.dispatch_state != crate::TurnDispatchState::Dispatching
            || before_admission.dispatch_error.is_some()
            || before_admission.provider_native_turn_id.is_some()
            || before_admission.turn_id != before_turn.id
            || before_admission.session_id != before_turn.session_id
            || before_session.id != before_turn.session_id
            || before_admission.provider != before_session.provider
            || before_session.active_turn_id.as_deref() != Some(before_turn.id.as_str())
            || before_session.closed_at.is_some()
            || before_turn.completed_at.is_some()
            || !matches!(
                before_turn.status.as_str(),
                "admitted" | "waiting_for_approval"
            )
            || !matches!(
                before_session.status.as_str(),
                "turn_admitted" | "waiting_for_approval"
            )
            || admissions.get(&before_admission.turn_id) != Some(before_admission)
            || turns.get(&before_turn.id) != Some(before_turn)
            || sessions.get(&before_session.id) != Some(before_session)
            || *self.fail_turn_write.lock().unwrap()
            || *self.fail_session_write.lock().unwrap()
        {
            return Err(RuntimeError::Conflict(
                "mock native dispatch start authority changed".into(),
            ));
        }
        let event = self.append_runtime_event_locked(event)?;
        admissions.insert(after_admission.turn_id.clone(), after_admission.clone());
        turns.insert(after_turn.id.clone(), after_turn.clone());
        sessions.insert(after_session.id.clone(), after_session.clone());
        Ok(ProviderDispatchStartedCommit {
            admission: after_admission.clone(),
            turn: after_turn.clone(),
            session: after_session.clone(),
            event,
        })
    }
}

macro_rules! delegate_mock_start {
    () => {
        fn upsert_turn_admission(&self, record: &TurnAdmissionRecord) -> Result<(), RuntimeError> {
            self.write_admission_for_test(record)
        }

        fn commit_provider_turn_started(
            &self,
            before_admission: &TurnAdmissionRecord,
            after_admission: &TurnAdmissionRecord,
            before_turn: &TurnRecord,
            after_turn: &TurnRecord,
            before_session: &SessionRecord,
            after_session: &SessionRecord,
            event: &NewRuntimeEvent,
        ) -> Result<ProviderDispatchStartedCommit, RuntimeError> {
            self.commit_start_for_test(
                before_admission,
                after_admission,
                before_turn,
                after_turn,
                before_session,
                after_session,
                event,
            )
        }
    };
}
pub(super) use delegate_mock_start;
