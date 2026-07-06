use runtime_core::{RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceEventLane {
    Critical,
    State,
    Tokens,
    Bulk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealthState {
    Live,
    Replaying,
    Stale,
    Offline,
    GapDetected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealth {
    pub source_id: String,
    pub source_epoch: String,
    pub state: SourceHealthState,
    pub last_source_seq: Option<i64>,
    pub last_error: Option<String>,
    pub updated_at: i64,
}

impl SourceHealth {
    pub fn new(source_id: impl Into<String>, source_epoch: impl Into<String>) -> Self {
        Self {
            source_id: source_id.into(),
            source_epoch: source_epoch.into(),
            state: SourceHealthState::Offline,
            last_source_seq: None,
            last_error: None,
            updated_at: now_ms(),
        }
    }

    pub fn transition(
        &mut self,
        state: SourceHealthState,
        last_source_seq: Option<i64>,
        last_error: Option<String>,
    ) {
        self.state = state;
        if let Some(last_source_seq) = last_source_seq {
            self.last_source_seq = Some(last_source_seq);
        }
        self.last_error = last_error;
        self.updated_at = now_ms();
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceEvent {
    pub source_id: String,
    pub source_epoch: String,
    pub source_seq: i64,
    pub upstream_row_id: i64,
    pub upstream_scoped_seq: i64,
    pub scope: RuntimeEventScope,
    pub scope_id: String,
    pub session_id: Option<String>,
    pub team_id: Option<String>,
    pub turn_id: Option<String>,
    pub kind: String,
    pub criticality: RuntimeEventCriticality,
    pub lane: SourceEventLane,
    pub payload: Value,
    pub provider: Option<String>,
    pub provider_seq: Option<i64>,
    pub created_at: i64,
}

impl SourceEvent {
    pub fn from_runtime_event(
        source_id: impl Into<String>,
        source_epoch: impl Into<String>,
        event: RuntimeEventRecord,
    ) -> Self {
        let lane = map_runtime_event_lane(event.criticality, event.kind.as_str());
        let payload = json!({
            "runtime_event": event.payload,
            "upstream": {
                "event_id": event.event_id,
                "row_id": event.row_id,
                "scoped_seq": event.seq,
                "scope": event.scope,
                "scope_id": event.scope_id,
                "provider": event.provider,
                "provider_seq": event.provider_seq,
                "created_at": event.created_at,
            }
        });

        Self {
            source_id: source_id.into(),
            source_epoch: source_epoch.into(),
            source_seq: event.row_id,
            upstream_row_id: event.row_id,
            upstream_scoped_seq: event.seq,
            scope: event.scope,
            scope_id: event.scope_id,
            session_id: event.session_id,
            team_id: event.team_id,
            turn_id: event.turn_id,
            kind: event.kind,
            criticality: event.criticality,
            lane,
            payload,
            provider: event.provider,
            provider_seq: event.provider_seq,
            created_at: event.created_at,
        }
    }
}

pub fn map_runtime_event_lane(criticality: RuntimeEventCriticality, kind: &str) -> SourceEventLane {
    if criticality == RuntimeEventCriticality::Critical {
        return SourceEventLane::Critical;
    }

    let normalized = kind.to_ascii_lowercase();
    if normalized.contains("approval")
        || normalized.contains("failed")
        || normalized.contains("interrupted")
        || normalized.contains("completed")
        || normalized.contains("auth")
        || normalized.contains("gap")
    {
        SourceEventLane::Critical
    } else if normalized.contains("delta")
        || normalized.contains("token")
        || normalized.contains("text")
        || normalized.contains("stream")
    {
        SourceEventLane::Tokens
    } else if normalized.contains("log")
        || normalized.contains("stdout")
        || normalized.contains("stderr")
        || normalized.contains("diagnostic")
    {
        SourceEventLane::Bulk
    } else {
        SourceEventLane::State
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_runtime_events_to_lanes() {
        assert_eq!(
            map_runtime_event_lane(RuntimeEventCriticality::Critical, "session.updated"),
            SourceEventLane::Critical
        );
        assert_eq!(
            map_runtime_event_lane(RuntimeEventCriticality::Droppable, "turn.text_delta"),
            SourceEventLane::Tokens
        );
        assert_eq!(
            map_runtime_event_lane(RuntimeEventCriticality::Droppable, "process.stdout"),
            SourceEventLane::Bulk
        );
        assert_eq!(
            map_runtime_event_lane(RuntimeEventCriticality::Droppable, "team.member_joined"),
            SourceEventLane::State
        );
    }
}
