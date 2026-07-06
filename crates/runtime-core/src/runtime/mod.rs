use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, RwLock};

use crate::{
    ApprovalRecord, ProviderKind, ProviderRegistry, RuntimeError, RuntimeEventRecord, RuntimeStore,
    SessionRecord, TurnRecord,
};

mod events;
mod helpers;
mod recovery;
mod sessions;
mod turns;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionInput {
    pub provider: ProviderKind,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub permission_mode: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendTurnInput {
    pub input: Vec<Value>,
    pub expected_turn_id: Option<String>,
    pub permission_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeSessionInput {
    pub provider_session_ref: Option<String>,
    pub canonical_provider_session_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalResponseInput {
    pub decision: String,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendTurnAccepted {
    pub session_id: String,
    pub turn_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StartupRecoveryProviderStatus {
    pub provider: String,
    pub healthy: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StartupRecoverySummary {
    pub started_at: i64,
    pub completed_at: i64,
    pub sessions_scanned: usize,
    pub turns_scanned: usize,
    pub approvals_scanned: usize,
    pub sessions_reconciled: usize,
    pub turns_reconciled: usize,
    pub approvals_reconciled: usize,
    pub resumed_sessions: usize,
    pub resumed_waits: usize,
    pub deferred_deliveries_retried: usize,
    pub provider_status: Vec<StartupRecoveryProviderStatus>,
    pub notes: Vec<String>,
}

pub struct RuntimeSessionManager {
    pub(super) store: Arc<dyn RuntimeStore>,
    pub(super) providers: Arc<ProviderRegistry>,
    pub(super) sessions: RwLock<HashMap<String, SessionRecord>>,
    pub(super) turns: RwLock<HashMap<String, TurnRecord>>,
    pub(super) approvals: RwLock<HashMap<String, ApprovalRecord>>,
    next_id: AtomicU64,
    pub(super) event_tx: broadcast::Sender<RuntimeEventRecord>,
}

impl RuntimeSessionManager {
    pub fn new(
        store: Arc<dyn RuntimeStore>,
        providers: Arc<ProviderRegistry>,
        live_event_capacity: usize,
    ) -> Result<Self, RuntimeError> {
        let hydrated = store.hydrate_runtime_state()?;
        let sessions = hydrated
            .sessions
            .into_iter()
            .map(|session| (session.id.clone(), session))
            .collect::<HashMap<_, _>>();
        let turns = hydrated
            .turns
            .into_iter()
            .map(|turn| (turn.id.clone(), turn))
            .collect::<HashMap<_, _>>();
        let approvals = hydrated
            .approvals
            .into_iter()
            .map(|approval| (approval.id.clone(), approval))
            .collect::<HashMap<_, _>>();
        let (event_tx, _) = broadcast::channel(live_event_capacity.max(128));

        Ok(Self {
            store,
            providers,
            sessions: RwLock::new(sessions),
            turns: RwLock::new(turns),
            approvals: RwLock::new(approvals),
            next_id: AtomicU64::new(1),
            event_tx,
        })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEventRecord> {
        self.event_tx.subscribe()
    }

    pub(super) fn allocate_id(&self, prefix: &str, suffix: &str) -> String {
        let seq = self.next_id.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{suffix}_{}_{}", helpers::now_ms(), seq)
    }
}
