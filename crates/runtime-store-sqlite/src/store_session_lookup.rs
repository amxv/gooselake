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
    };
}

pub(crate) use delegate_persisted_session_lookup;
