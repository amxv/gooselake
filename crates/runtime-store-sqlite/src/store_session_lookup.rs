// Keep the session primary-key delegate out of the 1000-line store impl.
macro_rules! delegate_persisted_session_lookup {
    () => {
        fn get_persisted_session(
            &self,
            session_id: &str,
        ) -> Result<Option<runtime_core::SessionRecord>, runtime_core::RuntimeError> {
            self.repository.get_persisted_session(session_id)
        }
    };
}

pub(crate) use delegate_persisted_session_lookup;
