use std::path::Path;

use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope,
};
use serde_json::Value;

use crate::now_ms;

use super::{PlannedWorktreePaths, RuntimeWorktreeService, StdCommand};

impl RuntimeWorktreeService {
    pub(super) async fn lock_for_repo(
        &self,
        repo_root: &str,
    ) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.repo_locks.lock().await;
        if let Some(existing) = locks.get(repo_root) {
            return std::sync::Arc::clone(existing);
        }
        let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(repo_root.to_string(), std::sync::Arc::clone(&lock));
        lock
    }

    pub(super) fn allocate_worktree_id(&self) -> String {
        format!(
            "wt_{}",
            self.next_worktree_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    pub(super) fn allocate_operation_id(&self) -> String {
        format!(
            "op_spawn_{}",
            self.next_operation_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    pub(super) fn normalize_deletion_policy(&self, requested: Option<&str>) -> String {
        let policy = requested
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(self.config.deletion_policy_default.as_str())
            .to_ascii_lowercase();
        match policy.as_str() {
            "delete_on_last_claim" => "delete_on_last_claim".to_string(),
            _ => "retain_on_last_claim".to_string(),
        }
    }

    pub(super) async fn append_worktree_event(
        &self,
        worktree_id: &str,
        kind: &str,
        payload: Value,
        session_id: Option<String>,
        team_id: Option<String>,
    ) {
        let event_id = format!(
            "evt_worktree_{}_{}_{}",
            worktree_id,
            self.event_id_nonce,
            self.next_event_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let _ = self.store.append_runtime_event(&NewRuntimeEvent {
            event_id,
            scope: RuntimeEventScope::Worktree,
            scope_id: worktree_id.to_string(),
            session_id,
            team_id,
            turn_id: None,
            kind: kind.to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload,
            provider: None,
            provider_seq: None,
            created_at: now_ms(),
        });
    }

    pub(super) fn derive_unified_workspace_path(repo_root: &str) -> String {
        let mut value = String::new();
        let mut prev_sep = false;
        for ch in repo_root.chars() {
            if ch == '/' || ch == '\\' {
                if !prev_sep {
                    value.push_str("__");
                }
                prev_sep = true;
                continue;
            }
            prev_sep = false;
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                value.push(ch);
            } else {
                value.push('_');
            }
        }
        let trimmed = value.trim_matches('_');
        if trimmed.is_empty() {
            "workspace".to_string()
        } else {
            trimmed.to_string()
        }
    }

    pub(super) fn resolve_repo_root_from_source_cwd(
        source_cwd: &str,
    ) -> Result<String, RuntimeError> {
        let output = StdCommand::new("git")
            .arg("-C")
            .arg(source_cwd)
            .args(["rev-parse", "--show-toplevel"])
            .output()
            .map_err(|error| RuntimeError::Io(format!("failed to run git rev-parse: {error}")))?;
        if !output.status.success() {
            return Err(RuntimeError::InvalidState(
                "source session cwd is not inside a git repository".to_string(),
            ));
        }
        let repo_root = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if repo_root.is_empty() {
            return Err(RuntimeError::InvalidState(
                "unable to resolve git repository root".to_string(),
            ));
        }
        Ok(repo_root)
    }

    pub(super) fn plan_worktree_paths(
        &self,
        repo_root: &str,
        worktree_name: &str,
        branch_prefix: Option<&str>,
    ) -> PlannedWorktreePaths {
        let prefix = branch_prefix
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("gg");
        let branch_name = format!("{prefix}/{}", worktree_name.trim());
        let unified = Self::derive_unified_workspace_path(repo_root);
        let worktree_root_path = Path::new(self.config.root_dir.as_str()).join(unified.as_str());
        let branch_path_component = branch_name.replace('/', "--");
        let worktree_cwd_path = worktree_root_path.join(branch_path_component);
        PlannedWorktreePaths {
            repo_root: repo_root.trim().to_string(),
            worktree_root: worktree_root_path.to_string_lossy().to_string(),
            worktree_cwd: worktree_cwd_path.to_string_lossy().to_string(),
            branch_name,
            worktree_name: worktree_name.trim().to_string(),
            unified_workspace_path: unified,
        }
    }

    pub(super) fn run_git_for_repo(
        repo_root: &str,
        args: &[&str],
        allowed_exit_codes: &[i32],
    ) -> Result<(String, String, i32), RuntimeError> {
        let output = StdCommand::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(args)
            .output()
            .map_err(|error| {
                RuntimeError::Io(format!("failed to run git {}: {error}", args.join(" ")))
            })?;
        let exit_code = output.status.code().unwrap_or(-1);
        if !output.status.success() && !allowed_exit_codes.contains(&exit_code) {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let message = if stderr.is_empty() {
                format!(
                    "git {} failed with status {}",
                    args.join(" "),
                    output.status
                )
            } else {
                format!("git {} failed: {}", args.join(" "), stderr)
            };
            return Err(RuntimeError::Io(message));
        }
        Ok((
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code,
        ))
    }

    pub(super) fn run_worktree_init_script(
        &self,
        worktree_cwd: &str,
    ) -> Result<String, RuntimeError> {
        let configured = Path::new(self.config.init_script_path.as_str());
        let script_path = if configured.is_absolute() {
            configured.to_path_buf()
        } else {
            Path::new(worktree_cwd).join(configured)
        };
        if !script_path.exists() {
            return Ok("skipped_missing".to_string());
        }

        let command = if configured.is_absolute() {
            script_path.to_string_lossy().to_string()
        } else {
            format!("./{}", configured.to_string_lossy())
        };
        let output = StdCommand::new("sh")
            .arg("-lc")
            .arg(command)
            .current_dir(worktree_cwd)
            .output()
            .map_err(|error| {
                RuntimeError::Io(format!("failed to run worktree init script: {error}"))
            })?;
        if output.status.success() {
            return Ok("succeeded".to_string());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Err(RuntimeError::InvalidState(if stderr.is_empty() {
            "worktree init script failed".to_string()
        } else {
            format!("worktree init script failed: {stderr}")
        }))
    }

    pub(super) fn upsert_worktree_record(
        &self,
        id: String,
        planned: &PlannedWorktreePaths,
        deletion_policy: String,
        created_by_session_id: Option<String>,
        created_by_operation_id: Option<String>,
    ) -> Result<ManagedWorktreeRecord, RuntimeError> {
        let now = now_ms();
        let record = ManagedWorktreeRecord {
            id,
            repo_root: planned.repo_root.clone(),
            worktree_root: planned.worktree_root.clone(),
            worktree_cwd: planned.worktree_cwd.clone(),
            branch_name: planned.branch_name.clone(),
            worktree_name: planned.worktree_name.clone(),
            unified_workspace_path: planned.unified_workspace_path.clone(),
            deletion_policy,
            created_by_session_id,
            created_by_operation_id,
            created_at: now,
            updated_at: now,
        };
        self.store.upsert_managed_worktree(&record)?;
        let hydrated = self.store.hydrate_runtime_state()?;
        self.worktree_by_identity(&hydrated, planned)
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "managed worktree logical upsert did not persist identity for {}",
                    planned.worktree_cwd
                ))
            })
    }

    pub(super) fn get_worktree_from_hydrated(
        &self,
        worktree_id: &str,
        hydrated: &runtime_core::RuntimeHydratedState,
    ) -> Result<ManagedWorktreeRecord, RuntimeError> {
        hydrated
            .managed_worktrees
            .iter()
            .find(|row| row.id == worktree_id && !Self::is_record_tombstoned(row))
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("worktree {}", worktree_id)))
    }

    pub(super) fn active_claims_for(
        &self,
        hydrated: &runtime_core::RuntimeHydratedState,
        worktree_id: &str,
    ) -> Vec<ManagedWorktreeClaimRecord> {
        hydrated
            .managed_worktree_claims
            .iter()
            .filter(|row| row.worktree_id == worktree_id && row.released_at.is_none())
            .cloned()
            .collect()
    }

    pub(super) fn worktree_by_identity(
        &self,
        hydrated: &runtime_core::RuntimeHydratedState,
        planned: &PlannedWorktreePaths,
    ) -> Option<ManagedWorktreeRecord> {
        hydrated
            .managed_worktrees
            .iter()
            .find(|row| {
                !Self::is_record_tombstoned(row)
                    && row.repo_root == planned.repo_root
                    && row.worktree_cwd == planned.worktree_cwd
                    && row.branch_name == planned.branch_name
            })
            .cloned()
    }

    pub(crate) fn is_record_tombstoned(record: &ManagedWorktreeRecord) -> bool {
        record.repo_root.starts_with("__gg_tombstoned__/")
            || record.worktree_cwd.starts_with("__gg_tombstoned__/")
            || record.branch_name.starts_with("__gg_tombstoned__/")
    }

    pub(super) fn branch_exists_for_record(
        record: &ManagedWorktreeRecord,
    ) -> Result<bool, RuntimeError> {
        let (_, _, exit_code) = Self::run_git_for_repo(
            record.repo_root.as_str(),
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", record.branch_name),
            ],
            &[1],
        )?;
        Ok(exit_code == 0)
    }

    pub(super) fn has_live_artifacts_for_record(record: &ManagedWorktreeRecord) -> bool {
        if Path::new(record.worktree_cwd.as_str()).exists() {
            return true;
        }
        match Self::branch_exists_for_record(record) {
            Ok(exists) => exists,
            Err(_) => true,
        }
    }
}
