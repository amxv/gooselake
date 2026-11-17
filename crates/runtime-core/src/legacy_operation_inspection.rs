//! Historical evidence views are never native execution authority.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const LEGACY_INSPECTION_DEFAULT_DIAGNOSTICS: usize = 50;
pub const LEGACY_INSPECTION_MAX_DIAGNOSTICS: usize = 100;
pub const LEGACY_INSPECTION_MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const LEGACY_INSPECTION_TOTAL_PAYLOAD_BYTES: usize = 256 * 1024;
pub const LEGACY_INSPECTION_MESSAGE_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedLegacyJson {
    pub payload: Option<Value>,
    pub payload_bytes: u64,
    pub payload_decode_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedTeamOperation {
    pub operation_id: String,
    pub team_id: String,
    pub kind: String,
    pub stage: String,
    #[serde(flatten)]
    pub evidence: InspectedLegacyJson,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedSpawnAdmission {
    pub operation_id: String,
    pub team_id: String,
    pub source_session_id: String,
    pub completion_response_present: bool,
    pub completion_response_bytes: u64,
    pub completion_certified: bool,
    pub completion_decode_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedTeamOperationDiagnostic {
    pub id: i64,
    pub operation_id: Option<String>,
    pub team_id: Option<String>,
    pub code: String,
    pub message: String,
    pub message_bytes: u64,
    pub message_truncated: bool,
    pub message_utf8_replaced: bool,
    #[serde(flatten)]
    pub evidence: InspectedLegacyJson,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyTeamOperationInspection {
    pub operation: Option<InspectedTeamOperation>,
    pub admission: Option<InspectedSpawnAdmission>,
    pub diagnostics: Vec<InspectedTeamOperationDiagnostic>,
    pub next_diagnostic_id: Option<i64>,
    pub diagnostics_truncated: bool,
    /// Limits apply to JSON bytes materialized from SQLite, not all metadata
    /// or physical SQLite I/O. Original identity fields remain intact.
    pub max_payload_bytes: usize,
    pub total_payload_budget_bytes: usize,
    pub payload_bytes_loaded: usize,
    pub native_retry_authorized: bool,
    pub requires_manual_review: bool,
}
