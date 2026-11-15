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

        /// SQLite overrides this fallback with a single scoped read snapshot.
        /// Other stores preserve their existing hydration/authority semantics.
        fn get_persisted_retirement_review_session(
            &self,
            session_id: &str,
        ) -> Result<Option<crate::RetirementReviewSessionProjection>, crate::RuntimeError> {
            let state = self.hydrate_runtime_state()?;
            let Some(session) = state
                .sessions
                .into_iter()
                .find(|session| session.id == session_id)
            else {
                return Ok(None);
            };
            Ok(Some(crate::RetirementReviewSessionProjection {
                session,
                turns: state
                    .turns
                    .into_iter()
                    .filter(|turn| turn.session_id == session_id)
                    .collect(),
                approvals: state
                    .approvals
                    .into_iter()
                    .filter(|approval| approval.session_id == session_id)
                    .collect(),
                admissions: self
                    .list_turn_admissions()?
                    .into_iter()
                    .filter(|admission| admission.session_id == session_id)
                    .collect(),
            }))
        }
    };
}

pub(crate) use default_persisted_session_lookup;
