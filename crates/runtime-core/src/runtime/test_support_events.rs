use super::*;

impl MockStore {
    // Caller owns authority_transaction; never acquire it recursively here.
    pub(super) fn append_runtime_event_locked(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        let mut failure = self
            .fail_next_event_append_for_kind
            .lock()
            .expect("event failure mutex");
        if failure.as_deref() == Some(event.kind.as_str()) {
            failure.take();
            return Err(RuntimeError::Io(
                "injected postcommit projection failure".into(),
            ));
        }
        drop(failure);
        let mut events = self.events.lock().expect("events mutex");
        let next_seq = events
            .iter()
            .filter(|row| row.scope == event.scope && row.scope_id == event.scope_id)
            .map(|row| row.seq)
            .max()
            .unwrap_or(0)
            + 1;
        let row_id = i64::try_from(events.len()).unwrap_or(0) + 1;
        let record = RuntimeEventRecord {
            row_id,
            event_id: event.event_id.clone(),
            scope: event.scope,
            scope_id: event.scope_id.clone(),
            session_id: event.session_id.clone(),
            team_id: event.team_id.clone(),
            turn_id: event.turn_id.clone(),
            seq: next_seq,
            kind: event.kind.clone(),
            criticality: event.criticality,
            payload: event.payload.clone(),
            provider: event.provider.clone(),
            provider_seq: event.provider_seq,
            created_at: event.created_at,
        };
        events.push(record.clone());
        Ok(record)
    }
}
