use super::*;

impl MockStore {
    pub(super) fn record_session_lifecycle(
        &self,
        before: Option<&SessionRecord>,
        after: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<(SessionRecord, Option<RuntimeEventRecord>), RuntimeError> {
        let mut sessions = self.sessions.lock().unwrap();
        if sessions.get(&after.id) != before || *self.fail_session_write.lock().unwrap() {
            return Err(RuntimeError::Conflict(
                "mock provider session lifecycle authority changed".into(),
            ));
        }
        // Test fault surfaces before committing either session or event.
        let receipt = self.append_runtime_event(event)?;
        sessions.insert(after.id.clone(), after.clone());
        Ok((after.clone(), Some(receipt)))
    }
}
