use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use runtime_core::ProviderTurnResult;
use serde_json::Value;
use tokio::sync::{oneshot, Mutex};

#[derive(Debug, Clone)]
pub(super) struct PendingApprovalTurn {
    pub(super) turn_id: String,
    pub(super) input: Vec<Value>,
    pub(super) expected_turn_id: Option<String>,
    pub(super) permission_mode: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct AcpAgentCapabilities {
    pub(super) load_session: bool,
    pub(super) resume_session: bool,
    pub(super) close_session: bool,
    pub(super) prompt_image: bool,
    pub(super) prompt_audio: bool,
    pub(super) prompt_embedded_context: bool,
}

#[derive(Debug, Clone)]
pub(super) struct AcpActiveTurnState {
    pub(super) runtime_turn_id: String,
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) assistant_chunks: Arc<Mutex<Vec<String>>>,
    pub(super) last_message_id: Arc<Mutex<Option<String>>>,
    pub(super) usage_update: Arc<Mutex<Option<Value>>>,
    pub(super) tool_calls: Arc<Mutex<Vec<(String, Value)>>>,
}

impl AcpActiveTurnState {
    pub(super) fn new(runtime_turn_id: String) -> Self {
        Self {
            runtime_turn_id,
            cancelled: Arc::new(AtomicBool::new(false)),
            assistant_chunks: Arc::new(Mutex::new(Vec::new())),
            last_message_id: Arc::new(Mutex::new(None)),
            usage_update: Arc::new(Mutex::new(None)),
            tool_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct AcpSessionState {
    pub(super) provider_session_ref: String,
    /// Binding to the exact negotiated stdio child that owns this native ID.
    /// A fresh ACP child can reuse the same session ID after a crash.
    pub(super) connection_id: Option<u64>,
    /// Fences turn dispatch while a new ACP child proves this native session
    /// via its advertised resume/load method.
    pub(super) resuming: bool,
    pub(super) active_turn: Option<AcpActiveTurnState>,
    pub(super) pending_approvals: HashMap<String, PendingApprovalTurn>,
    pub(super) pending_native_permissions: HashMap<String, AcpNativePermission>,
    pub(super) completed_turns: HashMap<String, ProviderTurnResult>,
    pub(super) waiters: HashMap<String, Vec<oneshot::Sender<ProviderTurnResult>>>,
}

#[derive(Debug, Clone)]
pub(super) struct AcpPermissionOption {
    pub(super) option_id: String,
    pub(super) kind: String,
}

#[derive(Debug, Clone)]
pub(super) struct AcpNativePermission {
    pub(super) connection_id: u64,
    pub(super) rpc_id: Value,
    pub(super) turn_id: String,
    pub(super) request: Value,
    pub(super) options: Vec<AcpPermissionOption>,
}
