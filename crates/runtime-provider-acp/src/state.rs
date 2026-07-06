use std::collections::{BTreeMap, HashMap};
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
}

#[derive(Debug, Clone)]
pub(super) struct AcpActiveTurnState {
    pub(super) runtime_turn_id: String,
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) assistant_chunks: Arc<Mutex<Vec<String>>>,
    pub(super) last_message_id: Arc<Mutex<Option<String>>>,
    pub(super) usage_update: Arc<Mutex<Option<Value>>>,
    pub(super) tool_calls: Arc<Mutex<BTreeMap<String, Value>>>,
}

impl AcpActiveTurnState {
    pub(super) fn new(runtime_turn_id: String) -> Self {
        Self {
            runtime_turn_id,
            cancelled: Arc::new(AtomicBool::new(false)),
            assistant_chunks: Arc::new(Mutex::new(Vec::new())),
            last_message_id: Arc::new(Mutex::new(None)),
            usage_update: Arc::new(Mutex::new(None)),
            tool_calls: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct AcpSessionState {
    pub(super) provider_session_ref: String,
    pub(super) active_turn: Option<AcpActiveTurnState>,
    pub(super) pending_approvals: HashMap<String, PendingApprovalTurn>,
    pub(super) completed_turns: HashMap<String, ProviderTurnResult>,
    pub(super) waiters: HashMap<String, Vec<oneshot::Sender<ProviderTurnResult>>>,
}
