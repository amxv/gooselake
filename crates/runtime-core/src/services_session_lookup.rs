// Keep the narrow session projection fallback out of the large RuntimeStore
// trait. SQLite overrides it with an indexed primary-key read.
macro_rules! default_persisted_session_lookup {
    () => {
        fn get_persisted_session(
            &self,
            session_id: &str,
        ) -> Result<Option<crate::SessionRecord>, crate::RuntimeError> {
            Ok(self
                .hydrate_runtime_state()?
                .sessions
                .into_iter()
                .find(|session| session.id == session_id))
        }
    };
}

pub(crate) use default_persisted_session_lookup;
