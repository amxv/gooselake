// Test-only atomic positive provider dispatch receipt for the team-comms
// in-memory runtime, preserving the same fault-before-publication boundary
// that the production SQLite repository enforces.
macro_rules! mock_native_start_method {
    () => {
        fn commit_provider_turn_started(
            &self,
            before_admission: &TurnAdmissionRecord,
            after_admission: &TurnAdmissionRecord,
            before_turn: &TurnRecord,
            after_turn: &TurnRecord,
            before_session: &SessionRecord,
            after_session: &SessionRecord,
            event: &NewRuntimeEvent,
        ) -> Result<crate::ProviderDispatchStartedCommit, RuntimeError> {
            let mut hydrated = self.hydrated.lock().expect("hydrated start lock");
            let mut admissions = self
                .turn_admissions
                .lock()
                .expect("turn admissions start lock");
            let current_admission = admissions
                .iter()
                .find(|a| a.turn_id == before_admission.turn_id);
            let current_turn = hydrated.turns.iter().find(|t| t.id == before_turn.id);
            let current_session = hydrated.sessions.iter().find(|s| s.id == before_session.id);
            let existing = self
                .events
                .lock()
                .unwrap()
                .iter()
                .find(|row| row.event_id == event.event_id)
                .cloned();
            if let Some(existing) = existing {
                if current_admission != Some(after_admission)
                    || current_turn != Some(after_turn)
                    || current_session != Some(after_session)
                    || existing.scope != event.scope
                    || existing.scope_id != event.scope_id
                    || existing.session_id != event.session_id
                    || existing.turn_id != event.turn_id
                    || existing.kind != event.kind
                    || existing.criticality != event.criticality
                    || existing.payload != event.payload
                {
                    return Err(RuntimeError::Conflict(
                        "mock native start Critical event clashes with another owner".into(),
                    ));
                }
                return Ok(crate::ProviderDispatchStartedCommit {
                    admission: after_admission.clone(),
                    turn: after_turn.clone(),
                    session: after_session.clone(),
                    event: existing,
                });
            }
            if current_admission != Some(before_admission)
                || current_turn != Some(before_turn)
                || current_session != Some(before_session)
            {
                return Err(RuntimeError::Conflict(
                    "mock native dispatch owner changed".into(),
                ));
            }
            let event = self.append_runtime_event(event)?;
            Self::upsert_with_key(&mut admissions, after_admission.clone(), |a| {
                a.turn_id.clone()
            });
            Self::upsert_with_key(&mut hydrated.turns, after_turn.clone(), |t| t.id.clone());
            Self::upsert_with_key(&mut hydrated.sessions, after_session.clone(), |s| {
                s.id.clone()
            });
            Ok(crate::ProviderDispatchStartedCommit {
                admission: after_admission.clone(),
                turn: after_turn.clone(),
                session: after_session.clone(),
                event,
            })
        }
    };
}
pub(super) use mock_native_start_method;
