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

        /// SQLite overrides with the exact terminal turn's consistent read.
        /// Existing non-SQLite stores keep the historical hydrated fallback.
        fn get_persisted_retirement_settled_turn(
            &self,
            turn_id: &str,
        ) -> Result<Option<crate::RetirementSettledTurnProjection>, crate::RuntimeError> {
            let state = self.hydrate_runtime_state()?;
            let Some(turn) = state.turns.into_iter().find(|turn| turn.id == turn_id) else {
                return Ok(None);
            };
            let session = state
                .sessions
                .into_iter()
                .find(|session| session.id == turn.session_id);
            let approvals = state
                .approvals
                .into_iter()
                .filter(|approval| approval.turn_id == turn_id)
                .collect();
            let admission = self
                .list_turn_admissions()?
                .into_iter()
                .find(|admission| admission.turn_id == turn_id);
            Ok(Some(crate::RetirementSettledTurnProjection {
                turn,
                session,
                approvals,
                admission,
            }))
        }
    };
}

pub(crate) use default_persisted_session_lookup;
