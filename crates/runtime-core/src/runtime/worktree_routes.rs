use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::{
    resolve_repository_identity, ManagedWorktreeStatusProjection, ProviderCapabilitySupport,
    ProviderKind, ProviderWorkspaceRebindRequest, RepositoryIdentity, RuntimeError,
    WorkspaceAgentLifecycleState, WorkspaceAgentRebindOperation, WorkspaceAgentRebindRequest,
    WorkspaceAgentRebindResponse, WorkspaceLifecycleState, WorkspaceRecord,
    WorkspaceWorktreeInventory, WorkspaceWorktreeInventoryEntry,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

/// A status-specific, source-derived read model. The complete eligible count
/// is exact, but only the requested top worktrees and displayed agents stay
/// resident while the store walks a single durable SQLite snapshot.
pub struct WorkspaceWorktreeStatusSummary {
    pub available: Vec<WorkspaceWorktreeInventoryEntry>,
    pub eligible_total: usize,
    pub displayed_agent_worktrees: HashMap<String, WorkspaceWorktreeInventoryEntry>,
}

impl RuntimeSessionManager {
    pub(super) fn eligible_workspace_worktree_cwd(
        &self,
        workspace: &WorkspaceRecord,
        worktree_id: &str,
    ) -> Result<String, RuntimeError> {
        let identity = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        let record = self
            .store
            .get_managed_worktree(worktree_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("managed worktree {worktree_id}")))?;
        eligible_managed_route(&identity, &record).map_err(RuntimeError::Conflict)?;
        Ok(record.worktree_cwd)
    }

    pub fn list_workspace_worktree_inventory(
        &self,
        workspace_id: &str,
    ) -> Result<WorkspaceWorktreeInventory, RuntimeError> {
        Ok(self
            .build_workspace_worktree_inventory(workspace_id, None)?
            .0)
    }

    /// HTTP needs only one keyset page, never every persisted worktree in the
    /// repository. The second result preserves the exact original cursor
    /// membership check (including aliases and tombstones).
    pub fn list_workspace_worktree_inventory_page(
        &self,
        workspace_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<(WorkspaceWorktreeInventory, bool), RuntimeError> {
        self.build_workspace_worktree_inventory(workspace_id, Some((after, limit)))
    }

    /// Avoid expanding the entire Git inventory merely to display up to 20
    /// eligible checkouts and the worktrees attached to up to 100 roster rows.
    /// Counting routable worktrees still requires checking every source record
    /// and its native Git identity; this bounds memory, not scan time.
    pub fn summarize_workspace_worktrees_for_status(
        &self,
        workspace_id: &str,
        displayed_agents: &[(String, String)],
        available_limit: usize,
    ) -> Result<WorkspaceWorktreeStatusSummary, RuntimeError> {
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let identity = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        let workspace_active = workspace.lifecycle_state == WorkspaceLifecycleState::Active;
        let displayed_cwds = displayed_agents
            .iter()
            .map(|(id, cwd)| (id.as_str(), cwd.as_str()))
            .collect::<HashMap<_, _>>();
        let mut available = Vec::with_capacity(available_limit.min(20));
        let mut eligible_total = 0;
        // Preserve the old inventory's name/ID ordering when legacy aliases
        // describe two worktrees attached at the same agent path.
        let mut displayed = HashMap::<String, WorkspaceWorktreeInventoryEntry>::new();
        self.store.visit_managed_worktree_status_for_root(
            &identity.canonical_root,
            workspace_id,
            &mut |projection, attached| {
                let Some(entry) = project_worktree_inventory_entry(
                    &identity,
                    workspace_active,
                    projection,
                    attached,
                ) else {
                    return Ok(());
                };
                for id in &entry.attached_agent_ids {
                    if displayed_cwds.get(id.as_str()).copied()
                        != Some(entry.worktree_path.as_str())
                    {
                        continue;
                    }
                    let replace = displayed.get(id).is_none_or(|old| {
                        (&entry.normalized_name, &entry.worktree_id)
                            < (&old.normalized_name, &old.worktree_id)
                    });
                    if replace {
                        displayed.insert(id.clone(), entry.clone());
                    }
                }
                if entry.eligible_for_assignment {
                    eligible_total += 1;
                    if available_limit != 0 {
                        let pos =
                            available.partition_point(|old: &WorkspaceWorktreeInventoryEntry| {
                                (&old.normalized_name, &old.branch_name, &old.worktree_id)
                                    <= (
                                        &entry.normalized_name,
                                        &entry.branch_name,
                                        &entry.worktree_id,
                                    )
                            });
                        if pos < available_limit {
                            available.insert(pos, entry);
                            available.truncate(available_limit);
                        }
                    }
                }
                Ok(())
            },
        )?;
        Ok(WorkspaceWorktreeStatusSummary {
            available,
            eligible_total,
            displayed_agent_worktrees: displayed,
        })
    }

    fn build_workspace_worktree_inventory(
        &self,
        workspace_id: &str,
        page: Option<(Option<(&str, &str)>, usize)>,
    ) -> Result<(WorkspaceWorktreeInventory, bool), RuntimeError> {
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let identity = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        let (scoped, valid_cursor) = if let Some((after, limit)) = page {
            self.store.list_managed_worktree_status_for_root_page(
                &identity.canonical_root,
                after,
                limit.saturating_add(1),
            )?
        } else {
            (
                self.store
                    .list_managed_worktree_status_for_root(&identity.canonical_root)?,
                true,
            )
        };
        let active_routes = if page.is_some() {
            // Only paths in this page can contribute an attached ID. The
            // complete selected worktree claim sets still identify foreign
            // or stale owners and must not be filtered by this optimization.
            let selected_cwds = scoped
                .iter()
                .filter(|row| !row.worktree.worktree_cwd.starts_with("__gg_tombstoned__/"))
                .map(|row| row.worktree.worktree_cwd.clone())
                .collect::<Vec<_>>();
            self.store
                .list_workspace_active_agent_routes_for_cwds(workspace_id, &selected_cwds)?
        } else {
            self.store
                .list_workspace_active_agent_routes(workspace_id)?
        };
        let mut attached_by_cwd: HashMap<&str, Vec<String>> = HashMap::new();
        for route in &active_routes {
            attached_by_cwd
                .entry(route.authoritative_cwd.as_str())
                .or_default()
                .push(route.agent_id.clone());
        }
        for attached in attached_by_cwd.values_mut() {
            attached.sort();
        }
        let mut worktrees = Vec::new();
        for projection in scoped {
            let attached_agent_ids = attached_by_cwd
                .get(projection.worktree.worktree_cwd.as_str())
                .cloned()
                .unwrap_or_default();
            if let Some(entry) = project_worktree_inventory_entry(
                &identity,
                workspace.lifecycle_state == WorkspaceLifecycleState::Active,
                projection,
                attached_agent_ids,
            ) {
                worktrees.push(entry);
            }
        }
        worktrees.sort_by(|a, b| {
            a.normalized_name
                .cmp(&b.normalized_name)
                .then(a.worktree_id.cmp(&b.worktree_id))
        });
        Ok((
            WorkspaceWorktreeInventory {
                workspace_id: workspace_id.to_string(),
                canonical_repository_root: identity.canonical_root,
                git_common_dir: identity.git_common_dir,
                repository_fingerprint: identity.fingerprint,
                worktrees,
            },
            valid_cursor,
        ))
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
        if self.store.unresolved_session_edit_rerun(agent_id)? {
            return Err(RuntimeError::Conflict(
                "cannot change worktree while native edit-and-rerun remains unresolved".into(),
            ));
        }
        if self.store.unresolved_session_compaction(agent_id)? {
            return Err(RuntimeError::Conflict(
                "cannot rebind a workspace agent during unresolved provider compaction".into(),
            ));
        }
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
                    return Err(RuntimeError::provider_dispatch_unknown_for_operation(
                        "reassignment_recovery_required",
                        &existing.operation_id,
                        format!(
                            "worktree rebind {} is {}; do not replay provider dispatch",
                            existing.operation_id, existing.phase,
                        ),
                    ));
                }
                return Ok(WorkspaceAgentRebindResponse {
                    agent: Some(self.get_workspace_agent(workspace_id, agent_id)?),
                    operation: existing,
                    newly_completed: false,
                });
            }
        }
        self.fence_unresolved_operator_turn_native_effect(agent_id)?;
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
            let record = self
                .store
                .get_managed_worktree(target)?
                .ok_or_else(|| RuntimeError::NotFound(format!("managed worktree {target}")))?;
            eligible_managed_route(&identity, &record).map_err(RuntimeError::Conflict)?;
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
            return Err(RuntimeError::provider_dispatch_unknown_for_operation(
                "reassignment_recovery_required",
                &operation.operation_id,
                format!(
                    "worktree rebind {} already admitted; do not repeat provider dispatch",
                    operation.operation_id
                ),
            ));
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
                return Err(RuntimeError::provider_dispatch_unknown_for_operation(
                    "reassignment_recovery_required",
                    &operation.operation_id,
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
                self.store
                    .get_managed_worktree(target)
                    .ok()
                    .flatten()
                    .is_none_or(|row| eligible_managed_route(&identity, &row).is_err())
            }))
        {
            let _ = self.store.classify_workspace_agent_rebind(
                &operation.operation_id,
                "manual_review",
                "provider_rebind_evidence_mismatch",
                now_ms(),
            );
            return Err(RuntimeError::provider_dispatch_unknown_for_operation(
                "reassignment_recovery_required",
                &operation.operation_id,
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
                return Err(RuntimeError::provider_dispatch_unknown_for_operation(
                    "reassignment_recovery_required",
                    &operation.operation_id,
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

fn project_worktree_inventory_entry(
    identity: &RepositoryIdentity,
    workspace_active: bool,
    projection: ManagedWorktreeStatusProjection,
    attached_agent_ids: Vec<String>,
) -> Option<WorkspaceWorktreeInventoryEntry> {
    let record = projection.worktree;
    if record.worktree_cwd.starts_with("__gg_tombstoned__/") {
        return None;
    }
    let mut blockers = Vec::new();
    if !workspace_active {
        blockers.push("workspace_not_active".to_string());
    }
    if let Err(reason) = eligible_managed_route(identity, &record) {
        blockers.push(reason);
    }
    if projection
        .active_claim_session_ids
        .iter()
        .any(|session_id| attached_agent_ids.binary_search(session_id).is_err())
    {
        blockers.push("claim_conflicts_with_workspace_roster".to_string());
    }
    let eligible = blockers.is_empty();
    Some(WorkspaceWorktreeInventoryEntry {
        worktree_id: record.id,
        revision: projection.revision,
        normalized_name: record.worktree_name,
        branch_name: record.branch_name,
        worktree_path: record.worktree_cwd,
        retention_policy: record.deletion_policy,
        lifecycle_state: "active".into(),
        routing_state: if eligible { "routable" } else { "blocked" }.into(),
        attached_agent_ids,
        eligible_for_assignment: eligible,
        eligibility_blockers: blockers,
    })
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
