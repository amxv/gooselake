pub mod client;
pub mod events;
pub mod sse;

pub use client::{
    CloseSessionRequest, GooselakeRuntimeClient, GooselakeRuntimeClientConfig,
    ProviderListResponse, ProviderModelsResponse, RuntimeClientError, RuntimeDiagnosticsResponse,
    RuntimeHealthResponse, RuntimeVersionResponse, TeamBroadcastInput, TeamCreateInput,
    TeamDirectInput, TeamJoinInput, TeamMemberSpawnInput, TeamSetLeadInput,
};
pub use events::{
    map_runtime_event_lane, SourceEvent, SourceEventLane, SourceHealth, SourceHealthState,
};
pub use sse::{RuntimeSseFanIn, RuntimeSseFanInConfig, SseFrame};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSourceId(pub String);
