use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::process::Command as StdCommand;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::{now_ms, WorktreeServiceConfig};
use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeRecord, RuntimeError, RuntimeSessionManager,
    RuntimeStore, TeamCommsService,
};

mod core;
mod service;
mod spawn;

pub struct RuntimeWorktreeService {
    pub(super) store: Arc<dyn RuntimeStore>,
    pub(super) runtime: Arc<RuntimeSessionManager>,
    pub(super) team_comms: Arc<dyn TeamCommsService>,
    pub(super) config: WorktreeServiceConfig,
    pub(super) repo_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    pub(super) next_worktree_id: AtomicU64,
    pub(super) next_operation_id: AtomicU64,
    pub(super) next_event_id: AtomicU64,
    pub(super) event_id_nonce: String,
}

#[derive(Debug, Clone)]
pub(super) struct PlannedWorktreePaths {
    pub(super) repo_root: String,
    pub(super) worktree_root: String,
    pub(super) worktree_cwd: String,
    pub(super) branch_name: String,
    pub(super) worktree_name: String,
    pub(super) unified_workspace_path: String,
}

impl RuntimeWorktreeService {
    pub fn new(
        store: Arc<dyn RuntimeStore>,
        runtime: Arc<RuntimeSessionManager>,
        team_comms: Arc<dyn TeamCommsService>,
        config: WorktreeServiceConfig,
    ) -> Result<Arc<Self>, RuntimeError> {
        let hydrated = store.hydrate_runtime_state()?;
        Self::repair_startup_state(store.as_ref(), &hydrated)?;
        let hydrated = store.hydrate_runtime_state()?;
        let mut max_worktree_seq = 0_u64;
        for row in hydrated.managed_worktrees {
            if let Some(seq) = row
                .id
                .strip_prefix("wt_")
                .and_then(|value| value.parse::<u64>().ok())
            {
                max_worktree_seq = max_worktree_seq.max(seq);
            }
        }
        let mut max_op_seq = 0_u64;
        for row in hydrated.team_operation_journal {
            if let Some(seq) = row
                .operation_id
                .strip_prefix("op_spawn_")
                .and_then(|value| value.parse::<u64>().ok())
            {
                max_op_seq = max_op_seq.max(seq);
            }
        }
        Ok(Arc::new(Self {
            store,
            runtime,
            team_comms,
            config,
            repo_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            next_worktree_id: AtomicU64::new(max_worktree_seq + 1),
            next_operation_id: AtomicU64::new(max_op_seq + 1),
            next_event_id: AtomicU64::new(1),
            event_id_nonce: format!("{:032x}", rand::random::<u128>()),
        }))
    }

    fn repair_startup_state(
        store: &dyn RuntimeStore,
        hydrated: &runtime_core::RuntimeHydratedState,
    ) -> Result<(), RuntimeError> {
        let now = now_ms();
        let mut session_ids = BTreeSet::new();
        for session in &hydrated.sessions {
            session_ids.insert(session.id.trim().to_string());
        }

        let mut normalized_records_by_id = BTreeMap::<String, ManagedWorktreeRecord>::new();
        for original in &hydrated.managed_worktrees {
            let mut normalized = original.clone();
            normalized.repo_root = normalized.repo_root.trim().to_string();
            normalized.worktree_root = normalized.worktree_root.trim().to_string();
            normalized.worktree_cwd = normalized.worktree_cwd.trim().to_string();
            normalized.branch_name = normalized.branch_name.trim().to_string();
            normalized.worktree_name = normalized.worktree_name.trim().to_string();
            normalized.unified_workspace_path =
                normalized.unified_workspace_path.trim().to_string();
            if normalized.worktree_name.is_empty() {
                normalized.worktree_name = normalized.id.clone();
            }
            if normalized.worktree_root.is_empty() {
                normalized.worktree_root = normalized.worktree_cwd.clone();
            }
            if normalized.unified_workspace_path.is_empty() {
                normalized.unified_workspace_path =
                    Self::derive_unified_workspace_path(normalized.repo_root.as_str());
            }
            normalized_records_by_id.insert(normalized.id.clone(), normalized);
        }

        let mut winner_by_identity = BTreeMap::<(String, String, String), String>::new();
        let mut merged_winners = BTreeMap::<String, ManagedWorktreeRecord>::new();
        let mut loser_to_winner = BTreeMap::<String, String>::new();
        let mut malformed_ids = BTreeSet::new();

        let mut ordered = normalized_records_by_id
            .values()
            .cloned()
            .collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        for record in ordered {
            if record.repo_root.is_empty()
                || record.worktree_cwd.is_empty()
                || record.branch_name.is_empty()
            {
                malformed_ids.insert(record.id.clone());
                continue;
            }
            let key = (
                record.repo_root.clone(),
                record.worktree_cwd.clone(),
                record.branch_name.clone(),
            );
            if let Some(existing_winner_id) = winner_by_identity.get(&key).cloned() {
                loser_to_winner.insert(record.id.clone(), existing_winner_id.clone());
                if let Some(winner) = merged_winners.get_mut(existing_winner_id.as_str()) {
                    winner.deletion_policy = Self::merge_deletion_policy(
                        winner.deletion_policy.as_str(),
                        record.deletion_policy.as_str(),
                    );
                    winner.created_at = winner.created_at.min(record.created_at);
                    winner.updated_at = winner.updated_at.max(record.updated_at);
                    if winner.created_by_session_id.is_none() {
                        winner.created_by_session_id = record.created_by_session_id.clone();
                    }
                    if winner.created_by_operation_id.is_none() {
                        winner.created_by_operation_id = record.created_by_operation_id.clone();
                    }
                }
                continue;
            }
            winner_by_identity.insert(key, record.id.clone());
            merged_winners.insert(record.id.clone(), record);
        }

        for winner in merged_winners.values() {
            store.upsert_managed_worktree(winner)?;
        }

        for loser_id in loser_to_winner.keys() {
            if let Some(loser) = normalized_records_by_id.get(loser_id) {
                store.upsert_managed_worktree(&Self::tombstone_record(loser, now))?;
            }
        }
        for malformed_id in &malformed_ids {
            if let Some(malformed) = normalized_records_by_id.get(malformed_id) {
                store.upsert_managed_worktree(&Self::tombstone_record(malformed, now))?;
            }
        }

        let mut winner_created_at = BTreeMap::<String, i64>::new();
        for (id, record) in &merged_winners {
            winner_created_at.insert(id.clone(), record.created_at);
        }

        let mut claim_by_key = BTreeMap::<(String, String), ManagedWorktreeClaimRecord>::new();
        let mut claims_changed = Vec::<ManagedWorktreeClaimRecord>::new();
        for original in &hydrated.managed_worktree_claims {
            let mut claim = original.clone();
            claim.worktree_id = claim.worktree_id.trim().to_string();
            claim.session_id = claim.session_id.trim().to_string();
            claim.claim_role = claim.claim_role.trim().to_string();
            if claim.claim_role.is_empty() {
                claim.claim_role = "consumer".to_string();
            }
            if claim.worktree_id.is_empty() || claim.session_id.is_empty() {
                if claim.released_at.is_none() {
                    claim.released_at = Some(now);
                }
                claims_changed.push(claim);
                continue;
            }
            let original_worktree_id = claim.worktree_id.clone();
            if let Some(winner_id) = loser_to_winner.get(claim.worktree_id.as_str()) {
                claim.worktree_id = winner_id.clone();
                let mut stale_original = claim.clone();
                stale_original.worktree_id = original_worktree_id;
                if stale_original.released_at.is_none() {
                    stale_original.released_at = Some(now);
                }
                claims_changed.push(stale_original);
            }
            let worktree_exists = merged_winners.contains_key(claim.worktree_id.as_str());
            let session_exists = session_ids.contains(claim.session_id.as_str());
            if !(worktree_exists && session_exists) && claim.released_at.is_none() {
                claim.released_at = Some(now);
            }

            let key = (claim.worktree_id.clone(), claim.session_id.clone());
            match claim_by_key.get_mut(&key) {
                Some(existing) => {
                    existing.created_at = existing.created_at.min(claim.created_at);
                    existing.claim_role = Self::merge_claim_role(
                        existing.claim_role.as_str(),
                        claim.claim_role.as_str(),
                    );
                    existing.released_at =
                        Self::merge_released_at(existing.released_at, claim.released_at);
                }
                None => {
                    claim_by_key.insert(key, claim);
                }
            }
        }

        let mut active_claims_by_session =
            BTreeMap::<String, Vec<ManagedWorktreeClaimRecord>>::new();
        for claim in claim_by_key.values() {
            if claim.released_at.is_none() {
                active_claims_by_session
                    .entry(claim.session_id.clone())
                    .or_default()
                    .push(claim.clone());
            }
        }

        for claims in active_claims_by_session.values_mut() {
            claims.sort_by(|left, right| {
                let left_created_at = winner_created_at
                    .get(left.worktree_id.as_str())
                    .copied()
                    .unwrap_or(i64::MAX);
                let right_created_at = winner_created_at
                    .get(right.worktree_id.as_str())
                    .copied()
                    .unwrap_or(i64::MAX);
                left_created_at
                    .cmp(&right_created_at)
                    .then_with(|| left.worktree_id.cmp(&right.worktree_id))
            });
            for losing in claims.iter().skip(1) {
                let key = (losing.worktree_id.clone(), losing.session_id.clone());
                if let Some(existing) = claim_by_key.get_mut(&key) {
                    if existing.released_at.is_none() {
                        existing.released_at = Some(now);
                    }
                }
            }
        }

        for claim in claim_by_key.into_values() {
            store.upsert_managed_worktree_claim(&claim)?;
        }
        for claim in claims_changed {
            store.upsert_managed_worktree_claim(&claim)?;
        }

        Ok(())
    }

    fn tombstone_record(record: &ManagedWorktreeRecord, now: i64) -> ManagedWorktreeRecord {
        let mut tombstoned = record.clone();
        tombstoned.repo_root = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.worktree_root = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.worktree_cwd = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.branch_name = format!("__gg_tombstoned__/{}", record.id);
        tombstoned.worktree_name = format!("tombstoned-{}", record.id);
        tombstoned.unified_workspace_path = format!("tombstoned_{}", record.id);
        tombstoned.deletion_policy = "retain_on_last_claim".to_string();
        tombstoned.updated_at = now;
        tombstoned
    }

    fn merge_deletion_policy(left: &str, right: &str) -> String {
        if left == "delete_on_last_claim" || right == "delete_on_last_claim" {
            "delete_on_last_claim".to_string()
        } else {
            "retain_on_last_claim".to_string()
        }
    }

    fn merge_claim_role(left: &str, right: &str) -> String {
        if left == "owner" || right == "owner" {
            "owner".to_string()
        } else {
            "consumer".to_string()
        }
    }

    fn merge_released_at(left: Option<i64>, right: Option<i64>) -> Option<i64> {
        match (left, right) {
            (None, None) => None,
            (Some(value), None) | (None, Some(value)) => Some(value),
            (Some(left), Some(right)) => Some(left.min(right)),
        }
    }

    #[cfg(test)]
    pub(super) fn spawn_test_flag(metadata: &Option<serde_json::Value>, key: &str) -> bool {
        metadata
            .as_ref()
            .and_then(|value: &serde_json::Value| value.as_object())
            .and_then(|object: &serde_json::Map<String, serde_json::Value>| object.get(key))
            .and_then(|value: &serde_json::Value| value.as_bool())
            .unwrap_or(false)
    }

    pub(super) fn ensure_enabled(&self) -> Result<(), RuntimeError> {
        if self.config.enabled {
            return Ok(());
        }
        Err(RuntimeError::Unsupported(
            "managed worktrees are disabled".to_string(),
        ))
    }
}
