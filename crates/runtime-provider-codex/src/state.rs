use std::collections::HashMap;
use std::sync::Arc;

use runtime_core::{ProviderRuntimeEvent, ProviderSessionPreferences, ProviderTurnResult};
use serde_json::Value;
use tokio::sync::{broadcast, oneshot, Mutex, RwLock};

use crate::transport::CodexTransport;
use crate::CodexProviderConfig;

#[derive(Debug, Clone)]
pub(super) struct PendingProviderApproval {
    pub(super) rpc_id: Value,
    pub(super) method: String,
    pub(super) native_turn_id: String,
    pub(super) tool_call_id: Option<String>,
    pub(super) request: Value,
    pub(super) emitted: bool,
}

#[derive(Debug, Clone)]
pub(super) struct PendingTerminalTurn {
    pub(super) status: runtime_core::ProviderTurnStatus,
    pub(super) error: Option<Value>,
}

#[derive(Debug)]
pub(super) struct CodexSessionState {
    pub(super) transport: Arc<CodexTransport>,
    pub(super) provider_session_ref: String,
    pub(super) canonical_provider_session_ref: Option<String>,
    pub(super) cwd: Option<String>,
    pub(super) model: Option<String>,
    pub(super) developer_instructions: String,
    pub(super) permission_mode: Option<String>,
    pub(super) current_preferences: ProviderSessionPreferences,
    pub(super) active_turn_id: Option<String>,
    pub(super) native_to_logical_turns: HashMap<String, String>,
    pub(super) logical_to_native_turns: HashMap<String, String>,
    pub(super) pending_approvals: HashMap<String, PendingProviderApproval>,
    pub(super) completed_turns: HashMap<String, ProviderTurnResult>,
    pub(super) waiters: HashMap<String, Vec<oneshot::Sender<ProviderTurnResult>>>,
    pub(super) last_messages: HashMap<String, String>,
    pub(super) usage_by_native_turn: HashMap<String, Value>,
    pub(super) pending_terminal_by_native: HashMap<String, PendingTerminalTurn>,
    pub(super) model_context_window: Option<u64>,
    pub(super) last_total_tokens: Option<u64>,
    pub(super) compaction_waiters: Vec<oneshot::Sender<()>>,
}

impl CodexSessionState {
    pub(super) fn new(
        transport: Arc<CodexTransport>,
        provider_session_ref: String,
        canonical_provider_session_ref: Option<String>,
        cwd: Option<String>,
        model: Option<String>,
        developer_instructions: String,
        permission_mode: Option<String>,
        current_preferences: ProviderSessionPreferences,
    ) -> Self {
        Self {
            transport,
            provider_session_ref,
            canonical_provider_session_ref,
            cwd,
            model,
            developer_instructions,
            permission_mode,
            current_preferences,
            active_turn_id: None,
            native_to_logical_turns: HashMap::new(),
            logical_to_native_turns: HashMap::new(),
            pending_approvals: HashMap::new(),
            completed_turns: HashMap::new(),
            waiters: HashMap::new(),
            last_messages: HashMap::new(),
            usage_by_native_turn: HashMap::new(),
            pending_terminal_by_native: HashMap::new(),
            model_context_window: None,
            last_total_tokens: None,
            compaction_waiters: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub(super) struct CodexProviderInner {
    pub(super) config: CodexProviderConfig,
    pub(super) sessions: RwLock<HashMap<String, CodexSessionState>>,
    pub(super) events: broadcast::Sender<ProviderRuntimeEvent>,
    pub(super) admission_lock: Mutex<()>,
}
