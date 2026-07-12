use serde_json::Value;

use crate::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    RuntimeRecordMutation,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    pub(super) async fn append_event(
        &self,
        scope: RuntimeEventScope,
        scope_id: &str,
        session_id: Option<&str>,
        turn_id: Option<&str>,
        kind: &str,
        criticality: RuntimeEventCriticality,
        payload: Value,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.append_event_with_mutations(
            scope,
            scope_id,
            session_id,
            turn_id,
            kind,
            criticality,
            payload,
            &[],
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn append_event_with_mutations(
        &self,
        scope: RuntimeEventScope,
        scope_id: &str,
        session_id: Option<&str>,
        turn_id: Option<&str>,
        kind: &str,
        criticality: RuntimeEventCriticality,
        payload: Value,
        mutations: &[RuntimeRecordMutation],
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        let event = NewRuntimeEvent {
            event_id: self.allocate_id("evt", scope.as_str()),
            scope,
            scope_id: scope_id.to_string(),
            session_id: session_id.map(str::to_string),
            team_id: None,
            turn_id: turn_id.map(str::to_string),
            kind: kind.to_string(),
            criticality,
            payload,
            provider: None,
            provider_seq: None,
            created_at: now_ms(),
        };
        let record = self
            .store
            .append_runtime_event_with_mutations(&event, mutations)?;
        let _ = self.event_tx.send(record.clone());
        Ok(record)
    }
}
