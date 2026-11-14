// Test-only atomic projection simulation. A receipt failure must precede
// any durable or manager cache mutation; exact replays do not duplicate it.
macro_rules! mock_terminal_conflict_methods {
    () => {
        fn commit_provider_terminal_conflict(
            &self,
            previous_turn: &crate::TurnRecord,
            previous_session: &crate::SessionRecord,
            failed_session: &crate::SessionRecord,
            event: &crate::NewRuntimeEvent,
        ) -> Result<(crate::SessionRecord, Option<crate::RuntimeEventRecord>), crate::RuntimeError>
        {
            if *self.fail_session_write.lock().unwrap() {
                return Err(crate::RuntimeError::Io(
                    "forced terminal conflict session write rejection".into(),
                ));
            }
            if self.turns.lock().unwrap().get(&previous_turn.id) != Some(previous_turn) {
                return Err(crate::RuntimeError::Conflict(
                    "original terminal turn changed".into(),
                ));
            }
            let mut sessions = self.sessions.lock().unwrap();
            let current = sessions.get(&previous_session.id).cloned().ok_or_else(|| {
                crate::RuntimeError::NotFound(format!("session {}", previous_session.id))
            })?;
            let previous = self
                .events
                .lock()
                .unwrap()
                .iter()
                .find(|row| row.event_id == event.event_id)
                .cloned();
            if let Some(receipt) = previous {
                if receipt.kind != event.kind
                    || receipt.scope != event.scope
                    || receipt.scope_id != event.scope_id
                    || receipt.session_id != event.session_id
                    || receipt.turn_id != event.turn_id
                    || receipt.criticality != event.criticality
                    || receipt.payload != event.payload
                    || (current.failure_message != failed_session.failure_message
                        && current.active_turn_id == previous_session.active_turn_id)
                {
                    return Err(crate::RuntimeError::Conflict(
                        "terminal conflict receipt mismatch".into(),
                    ));
                }
                return Ok((current, None));
            }
            if current != *previous_session {
                return Err(crate::RuntimeError::Conflict(
                    "terminal conflict session owner changed".into(),
                ));
            }
            let receipt = self.append_runtime_event(event)?;
            sessions.insert(failed_session.id.clone(), failed_session.clone());
            Ok((failed_session.clone(), Some(receipt)))
        }
    };
}
pub(super) use mock_terminal_conflict_methods;
