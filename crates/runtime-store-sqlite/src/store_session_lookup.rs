// Keep the session and attached-turn projection delegates out of the large
// store implementation.
macro_rules! delegate_persisted_session_lookup {
    () => {
        fn get_persisted_session(
            &self,
            session_id: &str,
        ) -> Result<Option<runtime_core::SessionRecord>, runtime_core::RuntimeError> {
            self.repository.get_persisted_session(session_id)
        }

        fn get_persisted_session_with_turns(
            &self,
            session_id: &str,
        ) -> Result<
            Option<(runtime_core::SessionRecord, Vec<runtime_core::TurnRecord>)>,
            runtime_core::RuntimeError,
        > {
            self.repository.get_persisted_session_with_turns(session_id)
        }

        fn get_persisted_retirement_review_session(
            &self,
            session_id: &str,
        ) -> Result<
            Option<runtime_core::RetirementReviewSessionProjection>,
            runtime_core::RuntimeError,
        > {
            self.repository
                .get_persisted_retirement_review_session(session_id)
        }

        fn get_persisted_retirement_settled_turn(
            &self,
            turn_id: &str,
        ) -> Result<
            Option<runtime_core::RetirementSettledTurnProjection>,
            runtime_core::RuntimeError,
        > {
            self.repository.get_persisted_retirement_settled_turn(turn_id)
        }

        fn get_persisted_turn_admission_for_correlation(
            &self,
            session_id: &str,
            correlation_id: &str,
        ) -> Result<Option<runtime_core::TurnAdmissionRecord>, runtime_core::RuntimeError> {
            self.repository
                .get_persisted_turn_admission_for_correlation(session_id, correlation_id)
        }
    };
}

pub(crate) use delegate_persisted_session_lookup;
