use std::path::Path;
use std::sync::Arc;

use crate::{
    resolve_repository_identity, ProviderCapabilitySupport, ProviderKind,
    ProviderWorkspaceRebindRequest, RuntimeError, WorkspaceAgentLifecycleState,
    WorkspaceAgentRebindOperation, WorkspaceAgentRebindRequest, WorkspaceAgentRebindResponse,
    WorkspaceLifecycleState, WorkspaceRecord, WorkspaceWorktreeInventory,
    WorkspaceWorktreeInventoryEntry,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    pub(super) fn eligible_workspace_worktree_cwd(
        &self,
        workspace: &WorkspaceRecord,
        worktree_id: &str,
    ) -> Result<String, RuntimeError> {
        let identity = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        let record = self
            .store
            .hydrate_runtime_state()?
            .managed_worktrees
            .into_iter()
            .find(|row| row.id == worktree_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("managed worktree {worktree_id}")))?;
        eligible_managed_route(&identity, &record).map_err(RuntimeError::Conflict)?;
        Ok(record.worktree_cwd)
    }

    pub fn list_workspace_worktree_inventory(
        &self,
        workspace_id: &str,
    ) -> Result<WorkspaceWorktreeInventory, RuntimeError> {
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let identity = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        let state = self.store.hydrate_runtime_state()?;
        let agents = self.store.list_workspace_agents(workspace_id, None)?;
        let mut worktrees = Vec::new();
        for record in state.managed_worktrees {
            let Ok(repository_root) = std::fs::canonicalize(&record.repo_root) else {
                continue;
            };
            if repository_root != Path::new(&identity.canonical_root) {
                continue;
            }
            if record.worktree_cwd.starts_with("__gg_tombstoned__/") {
                continue;
            }
            let mut blockers = Vec::new();
            if let Err(reason) = eligible_managed_route(&identity, &record) {
                blockers.push(reason);
            }
            let mut attached_agent_ids = agents
                .iter()
                .filter(|agent| {
                    agent.lifecycle_state == WorkspaceAgentLifecycleState::Active
                        && agent.recreation_policy.authoritative_cwd == record.worktree_cwd
                })
                .map(|agent| agent.agent_id.clone())
                .collect::<Vec<_>>();
            attached_agent_ids.sort();
            if state.managed_worktree_claims.iter().any(|claim| {
                claim.worktree_id == record.id
                    && claim.released_at.is_none()
                    && !attached_agent_ids.contains(&claim.session_id)
            }) {
                blockers.push("claim_conflicts_with_workspace_roster".to_string());
            }
            let routing_state = if blockers.is_empty() {
                "routable"
            } else {
                "blocked"
            };
            let revision = self.store.managed_worktree_revision(&record.id)?;
            worktrees.push(WorkspaceWorktreeInventoryEntry {
                worktree_id: record.id,
                revision,
                normalized_name: record.worktree_name,
                branch_name: record.branch_name,
                worktree_path: record.worktree_cwd,
                retention_policy: record.deletion_policy,
                lifecycle_state: "active".into(),
                routing_state: routing_state.into(),
                attached_agent_ids,
                eligible_for_assignment: blockers.is_empty(),
                eligibility_blockers: blockers,
            });
        }
        worktrees.sort_by(|a, b| {
            a.normalized_name
                .cmp(&b.normalized_name)
                .then(a.worktree_id.cmp(&b.worktree_id))
        });
        Ok(WorkspaceWorktreeInventory {
            workspace_id: workspace_id.to_string(),
            canonical_repository_root: identity.canonical_root,
            git_common_dir: identity.git_common_dir,
            repository_fingerprint: identity.fingerprint,
            worktrees,
        })
    }

    pub fn get_workspace_agent_rebind(
        &self,
        workspace_id: &str,
        agent_id: &str,
        operation_id: &str,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        let operation = self
            .store
            .get_workspace_agent_rebind(operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("rebind {operation_id}")))?;
        if operation.workspace_id != workspace_id || operation.agent_id != agent_id {
            return Err(RuntimeError::NotFound(format!("rebind {operation_id}")));
        }
        Ok(operation)
    }

    pub fn record_workspace_agent_rebind_cleanup(
        &self,
        workspace_id: &str,
        agent_id: &str,
        operation_id: &str,
        observed_status: &str,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        self.get_workspace_agent_rebind(workspace_id, agent_id, operation_id)?;
        self.store
            .record_workspace_agent_rebind_cleanup(operation_id, observed_status, now_ms())
    }

    pub async fn rebind_workspace_agent(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
        request: WorkspaceAgentRebindRequest,
        idempotency_key: Option<String>,
    ) -> Result<WorkspaceAgentRebindResponse, RuntimeError> {
        // send_turn and policy mutations use this same guard. The provider
        // cannot accept new work between the durable intent and final CAS.
        let _guard = self.session_policy_mutation_lock.lock().await;
        if idempotency_key
            .as_deref()
            .is_some_and(|value| value.trim().is_empty() || value.len() > 128)
        {
            return Err(RuntimeError::InvalidState(
                "Idempotency-Key must be 1-128 nonblank characters".into(),
            ));
        }
        if let Some(key) = idempotency_key.as_deref() {
            if let Some(existing) =
                self.store
                    .get_workspace_agent_rebind_by_key(workspace_id, agent_id, key)?
            {
                if existing.destination_worktree_id != request.worktree_id
                    || existing.expected_revision != request.expected_revision
                    || existing.cleanup_previous_worktree != request.cleanup_previous_worktree
                {
                    return Err(RuntimeError::Conflict(
                        "reused worktree rebind idempotency key with different input".into(),
                    ));
                }
                if existing.phase != "completed" {
                    return Err(RuntimeError::Conflict(format!(
                        "worktree rebind {} is {}; do not replay provider dispatch",
                        existing.operation_id, existing.phase,
                    )));
                }
                return Ok(WorkspaceAgentRebindResponse {
                    agent: Some(self.get_workspace_agent(workspace_id, agent_id)?),
                    operation: existing,
                    newly_completed: false,
                });
            }
        }
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::Conflict("workspace is not active".into()));
        }
        let agent = self.get_workspace_agent(workspace_id, agent_id)?;
        if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active {
            return Err(RuntimeError::Conflict("agent is archived".into()));
        }
        let session = self.get_session(agent_id).await?;
        if session.active_turn_id.is_some()
            || session.status == "closed"
            || session.status == "failed"
        {
            return Err(RuntimeError::Conflict(
                "agent is busy or not routable".into(),
            ));
        }
        if agent.revision != request.expected_revision {
            return Err(RuntimeError::Conflict(format!(
                "worktree reassignment revision conflict: expected {}, actual {}",
                request.expected_revision, agent.revision
            )));
        }
        if session.cwd.as_deref() != Some(agent.recreation_policy.authoritative_cwd.as_str()) {
            return Err(RuntimeError::ProtocolViolation(
                "session cwd differs from durable workspace-agent route".into(),
            ));
        }
        let identity = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        // Serializes destination verification and provider/native rebinding
        // against concurrent checkout creation, claims and cleanup.
        let repository_lock = crate::repository_worktree_lock(&identity.canonical_root).await;
        let _repository_guard = repository_lock.lock().await;
        let destination_worktree_id = request
            .worktree_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let destination_cwd = if let Some(target) = destination_worktree_id.as_deref() {
            let state = self.store.hydrate_runtime_state()?;
            let record = state
                .managed_worktrees
                .iter()
                .find(|record| record.id == target)
                .ok_or_else(|| RuntimeError::NotFound(format!("managed worktree {target}")))?;
            eligible_managed_route(&identity, record).map_err(RuntimeError::Conflict)?;
            record.worktree_cwd.clone()
        } else {
            identity.canonical_root.clone()
        };
        if session.worktree_id == destination_worktree_id
            && canonical_equal(&agent.recreation_policy.authoritative_cwd, &destination_cwd)
        {
            return Err(RuntimeError::Conflict(
                "agent already uses this workspace route".into(),
            ));
        }
        let provider_kind = ProviderKind::from_str(&session.provider)
            .ok_or_else(|| RuntimeError::ProtocolViolation("unknown agent provider".into()))?;
        let provider = self
            .providers
            .get(provider_kind)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(session.provider.clone()))?;
        if provider.capabilities().workspace_rebind != ProviderCapabilitySupport::Supported {
            return Err(RuntimeError::Unsupported(format!(
                "provider {} does not support workspace rebinding",
                session.provider
            )));
        }
        let now = now_ms();
        let proposed = WorkspaceAgentRebindOperation {
            operation_id: self.allocate_id("rebind", provider_kind.as_str()),
            workspace_id: workspace_id.to_string(),
            agent_id: agent_id.to_string(),
            idempotency_key,
            expected_revision: request.expected_revision,
            previous_worktree_id: session.worktree_id.clone(),
            destination_worktree_id: destination_worktree_id.clone(),
            previous_cwd: agent.recreation_policy.authoritative_cwd.clone(),
            destination_cwd: destination_cwd.clone(),
            cleanup_previous_worktree: request.cleanup_previous_worktree,
            previous_cleanup_status: None,
            previous_cleanup_diagnostic: None,
            phase: "intended".into(),
            provider_evidence: None,
            error_code: None,
            created_at: now,
            updated_at: now,
        };
        let operation = self.store.begin_workspace_agent_rebind(&proposed)?;
        if operation.phase == "completed" {
            return Ok(WorkspaceAgentRebindResponse {
                agent: Some(self.get_workspace_agent(workspace_id, agent_id)?),
                operation,
                newly_completed: false,
            });
        }
        if operation.operation_id != proposed.operation_id {
            return Err(RuntimeError::Conflict(format!(
                "worktree rebind {} already admitted; do not repeat provider dispatch",
                operation.operation_id
            )));
        }

        let evidence = match provider
            .rebind_workspace(ProviderWorkspaceRebindRequest {
                runtime_session_id: agent_id.to_string(),
                cwd: destination_cwd.clone(),
            })
            .await
        {
            Ok(evidence) => evidence,
            Err(error) => {
                let safe_rejection = matches!(
                    error,
                    RuntimeError::Unsupported(_) | RuntimeError::Conflict(_)
                );
                let phase = if safe_rejection {
                    "rejected"
                } else {
                    "manual_review"
                };
                let _ = self.store.classify_workspace_agent_rebind(
                    &operation.operation_id,
                    phase,
                    if safe_rejection {
                        "provider_rebind_rejected"
                    } else {
                        "reassignment_recovery_required"
                    },
                    now_ms(),
                );
                if safe_rejection {
                    return Err(error);
                }
                return Err(RuntimeError::provider_dispatch_unknown(
                    "reassignment_recovery_required",
                    format!(
                        "provider workspace binding outcome is unknown; inspect worktree rebind operation {} before taking further action: {error}",
                        operation.operation_id
                    ),
                ));
            }
        };
        if evidence.runtime_session_id != agent_id
            || !canonical_equal(&evidence.cwd, &destination_cwd)
            || evidence
                .provider_session_ref
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            || (provider_kind == ProviderKind::Codex
                && evidence.provider_session_ref != session.provider_session_ref)
            || (provider_kind == ProviderKind::Claude
                && (evidence.binding_generation.is_none()
                    || evidence
                        .canonical_provider_session_ref
                        .as_deref()
                        .is_none_or(|value| value.trim().is_empty())
                    || session
                        .canonical_provider_session_ref
                        .as_deref()
                        .is_some_and(|old| {
                            evidence.canonical_provider_session_ref.as_deref() != Some(old)
                        })))
            || (destination_worktree_id.as_deref().is_some_and(|target| {
                self.store.hydrate_runtime_state().ok().is_none_or(|state| {
                    state
                        .managed_worktrees
                        .iter()
                        .find(|row| row.id == target)
                        .is_none_or(|row| eligible_managed_route(&identity, row).is_err())
                })
            }))
        {
            let _ = self.store.classify_workspace_agent_rebind(
                &operation.operation_id,
                "manual_review",
                "provider_rebind_evidence_mismatch",
                now_ms(),
            );
            return Err(RuntimeError::provider_dispatch_unknown(
                "reassignment_recovery_required",
                format!(
                    "provider workspace rebind lacked authoritative binding evidence; operation {}",
                    operation.operation_id
                ),
            ));
        }
        let mut updated_session = session;
        updated_session.cwd = Some(destination_cwd.clone());
        updated_session.worktree_id = destination_worktree_id;
        updated_session.updated_at = now_ms();
        if let Some(ref provider_ref) = evidence.provider_session_ref {
            updated_session.provider_session_ref = Some(provider_ref.clone());
        }
        if let Some(ref canonical_ref) = evidence.canonical_provider_session_ref {
            updated_session.canonical_provider_session_ref = Some(canonical_ref.clone());
        }
        let mut updated_policy = agent.recreation_policy;
        updated_policy.authoritative_cwd = destination_cwd;
        let outcome = self.store.finalize_workspace_agent_rebind(
            &operation.operation_id,
            &updated_session,
            &updated_policy,
            &evidence,
            updated_session.updated_at,
        );
        let operation = match outcome {
            Ok(operation) => operation,
            Err(error) => {
                let _ = self.store.classify_workspace_agent_rebind(
                    &operation.operation_id,
                    "manual_review",
                    "rebind_authority_commit_failed",
                    now_ms(),
                );
                return Err(RuntimeError::provider_dispatch_unknown(
                    "reassignment_recovery_required",
                    format!("provider changed cwd but durable route commit failed ({error}); operation {}", operation.operation_id),
                ));
            }
        };
        self.sessions
            .write()
            .await
            .insert(agent_id.to_string(), updated_session);
        Ok(WorkspaceAgentRebindResponse {
            agent: Some(self.get_workspace_agent(workspace_id, agent_id)?),
            operation,
            newly_completed: true,
        })
    }
}

fn canonical_equal(left: &str, right: &str) -> bool {
    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// Re-resolve native Git identity rather than trusting a persisted path or
/// legacy team claim. This also rejects stale/tombstoned routes safely.
fn eligible_managed_route(
    identity: &crate::RepositoryIdentity,
    worktree: &crate::ManagedWorktreeRecord,
) -> Result<(), String> {
    if worktree.repo_root.starts_with("__gg_tombstoned__/") {
        return Err("tombstoned".into());
    }
    if !canonical_equal(&worktree.repo_root, &identity.canonical_root) {
        return Err("cross_repository_authority".into());
    }
    let path = Path::new(&worktree.worktree_cwd);
    if !path.is_dir() {
        return Err("worktree_path_missing".into());
    }
    let actual =
        resolve_repository_identity(path).map_err(|_| "invalid_git_worktree".to_string())?;
    if actual.fingerprint != identity.fingerprint
        || actual.git_common_dir != identity.git_common_dir
    {
        return Err("cross_repository_native_identity".into());
    }
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .output()
        .map_err(|_| "worktree_branch_unavailable".to_string())?;
    if !output.status.success()
        || String::from_utf8_lossy(&output.stdout).trim() != worktree.branch_name
    {
        return Err("worktree_branch_mismatch".into());
    }
    Ok(())
}
