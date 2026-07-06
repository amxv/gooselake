use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use runtime_core::ProviderTurnResult;
use serde_json::Value;
use tokio::process::Child;
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::CodexProviderConfig;

#[derive(Debug, Clone)]
pub(super) struct PendingApprovalTurn {
    pub(super) turn_id: String,
    pub(super) input: Vec<Value>,
    pub(super) expected_turn_id: Option<String>,
    pub(super) permission_mode: Option<String>,
}

#[derive(Debug)]
pub(super) struct RunningTurn {
    pub(super) child: Arc<Mutex<Child>>,
    pub(super) interrupt_requested: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
pub(super) struct CodexSessionState {
    pub(super) provider_session_ref: String,
    pub(super) canonical_provider_session_ref: Option<String>,
    pub(super) cwd: Option<String>,
    pub(super) model: Option<String>,
    pub(super) permission_mode: Option<String>,
    pub(super) active_turns: HashMap<String, RunningTurn>,
    pub(super) pending_approvals: HashMap<String, PendingApprovalTurn>,
    pub(super) completed_turns: HashMap<String, ProviderTurnResult>,
    pub(super) waiters: HashMap<String, Vec<oneshot::Sender<ProviderTurnResult>>>,
}

#[derive(Debug)]
pub(super) struct CodexProviderInner {
    pub(super) config: CodexProviderConfig,
    pub(super) sessions: RwLock<HashMap<String, CodexSessionState>>,
}
