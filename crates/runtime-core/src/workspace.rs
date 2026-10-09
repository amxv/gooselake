use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::RuntimeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceLifecycleState {
    Active,
    Retired,
}

impl WorkspaceLifecycleState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Retired => "retired",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "retired" => Some(Self::Retired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationActorKind {
    Operator,
    Agent,
    System,
}

impl OperationActorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Agent => "agent",
            Self::System => "system",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "operator" => Some(Self::Operator),
            "agent" => Some(Self::Agent),
            "system" => Some(Self::System),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationActor {
    pub kind: OperationActorKind,
    pub identifier: String,
}

impl OperationActor {
    pub fn operator(identifier: impl Into<String>) -> Self {
        Self {
            kind: OperationActorKind::Operator,
            identifier: identifier.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    Requested,
    ManualReview,
    Completed,
    Failed,
}

impl OperationPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::ManualReview => "manual_review",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "requested" => Some(Self::Requested),
            "manual_review" => Some(Self::ManualReview),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRecord {
    pub workspace_id: String,
    pub canonical_root: String,
    pub display_name: String,
    pub lifecycle_state: WorkspaceLifecycleState,
    pub lead_agent_id: Option<String>,
    pub revision: u64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub operation_id: String,
    pub workspace_id: Option<String>,
    pub kind: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request_hash: String,
    pub normalized_request: Value,
    pub phase: OperationPhase,
    pub exact_terminal_result: Option<Value>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationResourceClaimRecord {
    pub resource_kind: String,
    pub resource_id: String,
    pub claim_mode: String,
    pub owner_operation_id: String,
    pub fence_generation: u64,
    pub acquired_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationTransitionRecord {
    pub operation_id: String,
    pub sequence: u64,
    pub from_phase: Option<OperationPhase>,
    pub to_phase: OperationPhase,
    pub evidence: Option<Value>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationEffectRecord {
    pub effect_id: String,
    pub operation_id: String,
    pub effect_kind: String,
    pub target_kind: String,
    pub target_id: String,
    pub phase: String,
    pub idempotency_key: String,
    pub evidence: Option<Value>,
    pub attempt_count: u64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationOutboxRecord {
    pub outbox_id: String,
    pub operation_id: String,
    pub delivery_kind: String,
    pub idempotency_key: String,
    pub payload: Value,
    pub state: String,
    pub attempt_count: u64,
    pub next_attempt_at: Option<i64>,
    pub last_error: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
    pub delivered_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationOutboxReceiptRecord {
    pub outbox_id: String,
    pub idempotency_key: String,
    pub received_at: i64,
    pub receipt: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationDetails {
    pub operation: OperationRecord,
    pub claims: Vec<OperationResourceClaimRecord>,
    pub transitions: Vec<OperationTransitionRecord>,
    pub effects: Vec<OperationEffectRecord>,
    pub outbox: Vec<OperationOutboxRecord>,
    pub receipts: Vec<OperationOutboxReceiptRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRegisterRequest {
    pub canonical_root: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRegisterCommand {
    pub operation_id: String,
    pub proposed_workspace_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request_hash: String,
    pub normalized_request: Value,
    pub canonical_root: String,
    pub display_name: String,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRegisterResponse {
    pub operation_id: String,
    pub workspace: WorkspaceRecord,
}

pub fn prepare_workspace_registration(
    request: WorkspaceRegisterRequest,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<WorkspaceRegisterCommand, RuntimeError> {
    if actor.identifier.trim().is_empty() {
        return Err(RuntimeError::InvalidState(
            "operation actor identifier cannot be empty".to_string(),
        ));
    }

    let canonical_root = canonical_workspace_root(request.canonical_root.as_str())?;
    let display_name = workspace_display_name(request.display_name.as_deref(), &canonical_root)?;
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = json!({
        "canonical_root": canonical_root.as_str(),
        "display_name": display_name.as_str(),
    });
    let normalized_request_hash = normalized_json_hash(&normalized_request)?;

    Ok(WorkspaceRegisterCommand {
        operation_id: opaque_id("op"),
        proposed_workspace_id: opaque_id("workspace"),
        actor,
        idempotency_key,
        normalized_request_hash,
        normalized_request,
        canonical_root,
        display_name,
        requested_at: unix_time_ms()?,
    })
}

fn canonical_workspace_root(value: &str) -> Result<String, RuntimeError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(RuntimeError::InvalidState(
            "workspace canonical_root is required".to_string(),
        ));
    }
    let canonical = std::fs::canonicalize(normalized).map_err(|error| {
        RuntimeError::InvalidState(format!(
            "workspace root {normalized:?} could not be canonicalized: {error}"
        ))
    })?;
    if !canonical.is_dir() {
        return Err(RuntimeError::InvalidState(format!(
            "workspace root {} is not a directory",
            canonical.display()
        )));
    }
    Ok(canonical.to_string_lossy().into_owned())
}

fn workspace_display_name(
    value: Option<&str>,
    canonical_root: &str,
) -> Result<String, RuntimeError> {
    let display_name = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            Path::new(canonical_root)
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| canonical_root.to_string());
    if display_name.is_empty() {
        return Err(RuntimeError::InvalidState(
            "workspace display_name is required".to_string(),
        ));
    }
    Ok(display_name)
}

pub(crate) fn normalize_idempotency_key(
    value: Option<String>,
) -> Result<Option<String>, RuntimeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(RuntimeError::InvalidState(
            "Idempotency-Key cannot be empty".to_string(),
        ));
    }
    if normalized.len() > 512 {
        return Err(RuntimeError::InvalidState(
            "Idempotency-Key cannot exceed 512 bytes".to_string(),
        ));
    }
    Ok(Some(normalized.to_string()))
}

pub fn normalized_json_hash(value: &Value) -> Result<String, RuntimeError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed serializing normalized operation input: {error}"
        ))
    })?;
    Ok(sha256_hex(&bytes))
}

fn sha256_hex(input: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut state = [
        0x6a09e667_u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut padded = input.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0_u32; 64];
        for (index, bytes) in chunk.chunks_exact(4).take(16).enumerate() {
            w[index] = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(w[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (slot, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h].into_iter()) {
            *slot = slot.wrapping_add(value);
        }
    }

    let mut output = String::with_capacity(64);
    for value in state {
        output.push_str(&format!("{value:08x}"));
    }
    output
}

pub(crate) fn opaque_id(prefix: &str) -> String {
    format!("{prefix}_{:032x}", rand::random::<u128>())
}

pub(crate) fn unix_time_ms() -> Result<i64, RuntimeError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            RuntimeError::Bootstrap(format!("system clock is before unix epoch: {error}"))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| RuntimeError::Bootstrap("unix timestamp overflow".to_string()))
}

#[cfg(test)]
mod tests {
    use super::sha256_hex;

    #[test]
    fn sha256_matches_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
