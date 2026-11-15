// Keep narrow session projection fallbacks out of the large RuntimeStore trait.
// SQLite overrides them with exact-target, one-snapshot queries.
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

        fn get_persisted_session_with_turns(
            &self,
            session_id: &str,
        ) -> Result<Option<(crate::SessionRecord, Vec<crate::TurnRecord>)>, crate::RuntimeError> {
            let state = self.hydrate_runtime_state()?;
            Ok(state
                .sessions
                .into_iter()
                .find(|session| session.id == session_id)
                .map(|session| {
                    let turns = state
                        .turns
                        .into_iter()
                        .filter(|turn| turn.session_id == session_id)
                        .collect();
                    (session, turns)
                }))
        }
    };
}

pub(crate) use default_persisted_session_lookup;
