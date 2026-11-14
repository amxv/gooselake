use super::*;

#[derive(Debug, Serialize)]
pub(super) struct RuntimeDiagnosticsResponse {
    providers: ProviderDiagnosticsResponse,
    comms: CommsDiagnosticsResponse,
    processes: ProcessDiagnosticsResponse,
    worktrees: WorktreeDiagnosticsResponse,
    recovery: RecoveryDiagnosticsResponse,
}

#[derive(Debug, Serialize)]
pub(super) struct ProviderDiagnosticEntry {
    provider: String,
    healthy: bool,
    health_error: Option<String>,
    auth_status: Option<runtime_core::ProviderAuthStatus>,
    auth_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct ProviderDiagnosticsResponse {
    providers: Vec<ProviderDiagnosticEntry>,
}

#[derive(Debug, Serialize)]
pub(super) struct CommsDiagnosticsResponse {
    team_count: usize,
    member_count: usize,
    message_count: usize,
    delivery_total: usize,
    delivery_status_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Serialize)]
pub(super) struct ProcessDiagnosticsResponse {
    process_total: usize,
    process_status_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Serialize)]
pub(super) struct WorktreeDiagnosticsResponse {
    worktree_total: usize,
    active_claim_total: usize,
    orphaned_claim_total: usize,
    deletion_policy_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Serialize)]
pub(super) struct RecoveryDiagnosticsResponse {
    startup: StartupRecoverySummary,
    active_anomalies: Vec<String>,
}

pub(super) async fn runtime_diagnostics(
    State(state): State<AppState>,
) -> Result<Json<RuntimeDiagnosticsResponse>, ApiError> {
    let providers = provider_diagnostics_internal(&state).await;
    let (comms, processes, worktrees, recovery) = diagnostics_from_hydrated_state(&state)?;
    Ok(Json(RuntimeDiagnosticsResponse {
        providers,
        comms,
        processes,
        worktrees,
        recovery,
    }))
}

pub(super) async fn provider_diagnostics(
    State(state): State<AppState>,
) -> Result<Json<ProviderDiagnosticsResponse>, ApiError> {
    Ok(Json(provider_diagnostics_internal(&state).await))
}

pub(super) async fn comms_diagnostics(
    State(state): State<AppState>,
) -> Result<Json<CommsDiagnosticsResponse>, ApiError> {
    // Legacy team diagnostics need only historical row/status counts. SQLite
    // can return these without hydrating the whole runtime or message inputs.
    if let Some(counts) = state.app.services.store.legacy_comms_diagnostic_counts()? {
        return Ok(Json(CommsDiagnosticsResponse::from_counts(counts)));
    }
    let (comms, _, _, _) = diagnostics_from_hydrated_state(&state)?;
    Ok(Json(comms))
}

impl CommsDiagnosticsResponse {
    fn from_counts(counts: runtime_core::LegacyCommsDiagnosticCounts) -> Self {
        Self {
            team_count: counts.team_count,
            member_count: counts.member_count,
            message_count: counts.message_count,
            delivery_total: counts.delivery_total,
            delivery_status_counts: counts.delivery_status_counts,
        }
    }
}

pub(super) async fn process_diagnostics(
    State(state): State<AppState>,
) -> Result<Json<ProcessDiagnosticsResponse>, ApiError> {
    // This endpoint only needs two counters. Do not hydrate the entire runtime
    // (or deserialize every managed process) on SQLite-backed installations.
    if let Some(status_counts) = state.app.services.store.managed_process_status_counts()? {
        return Ok(Json(ProcessDiagnosticsResponse::from_status_counts(
            status_counts,
        )));
    }
    let (_, processes, _, _) = diagnostics_from_hydrated_state(&state)?;
    Ok(Json(processes))
}

impl ProcessDiagnosticsResponse {
    fn from_status_counts(process_status_counts: BTreeMap<String, usize>) -> Self {
        Self {
            process_total: process_status_counts.values().sum(),
            process_status_counts,
        }
    }
}

pub(super) async fn worktree_diagnostics(
    State(state): State<AppState>,
) -> Result<Json<WorktreeDiagnosticsResponse>, ApiError> {
    // Diagnostic counts need no paths, branches, or claim/session records on
    // SQLite. Retain the hydrated fallback for non-SQLite stores.
    if let Some(counts) = state
        .app
        .services
        .store
        .managed_worktree_diagnostic_counts()?
    {
        return Ok(Json(WorktreeDiagnosticsResponse::from_counts(counts)));
    }
    let (_, _, worktrees, _) = diagnostics_from_hydrated_state(&state)?;
    Ok(Json(worktrees))
}

impl WorktreeDiagnosticsResponse {
    fn from_counts(counts: runtime_core::ManagedWorktreeDiagnosticCounts) -> Self {
        Self {
            worktree_total: counts.worktree_total,
            active_claim_total: counts.active_claim_total,
            orphaned_claim_total: counts.orphaned_claim_total,
            deletion_policy_counts: counts.deletion_policy_counts,
        }
    }
}

pub(super) async fn recovery_diagnostics(
    State(state): State<AppState>,
) -> Result<Json<RecoveryDiagnosticsResponse>, ApiError> {
    let (_, _, _, recovery) = diagnostics_from_hydrated_state(&state)?;
    Ok(Json(recovery))
}

pub(super) async fn provider_diagnostics_internal(state: &AppState) -> ProviderDiagnosticsResponse {
    let mut rows = Vec::new();
    for provider in state.app.provider_registry.metadata() {
        let adapter = state.app.provider_registry.get(provider.kind);
        let mut healthy = false;
        let mut health_error = None;
        let mut auth_status = None;
        let mut auth_error = None;
        if let Some(adapter) = adapter {
            match adapter.healthcheck().await {
                Ok(()) => healthy = true,
                Err(error) => health_error = Some(error.to_string()),
            }
            match state.runtime.provider_auth_status(provider.kind).await {
                Ok(status) => auth_status = Some(status),
                Err(error) => auth_error = Some(error.to_string()),
            }
        } else {
            health_error = Some("provider not registered".to_string());
            auth_error = Some("provider not registered".to_string());
        }
        rows.push(ProviderDiagnosticEntry {
            provider: provider.kind.as_str().to_string(),
            healthy,
            health_error,
            auth_status,
            auth_error,
        });
    }
    ProviderDiagnosticsResponse { providers: rows }
}

pub(super) fn diagnostics_from_hydrated_state(
    state: &AppState,
) -> Result<
    (
        CommsDiagnosticsResponse,
        ProcessDiagnosticsResponse,
        WorktreeDiagnosticsResponse,
        RecoveryDiagnosticsResponse,
    ),
    ApiError,
> {
    let hydrated = state
        .app
        .services
        .store
        .hydrate_runtime_state()
        .map_err(ApiError::from)?;

    let comms = if let Some(counts) = state.app.services.store.legacy_comms_diagnostic_counts()? {
        CommsDiagnosticsResponse::from_counts(counts)
    } else {
        let mut delivery_status_counts = BTreeMap::<String, usize>::new();
        for delivery in &hydrated.team_deliveries {
            *delivery_status_counts
                .entry(delivery.status.clone())
                .or_insert(0) += 1;
        }
        CommsDiagnosticsResponse {
            team_count: hydrated.teams.len(),
            member_count: hydrated.team_members.len(),
            message_count: hydrated.team_messages.len(),
            delivery_total: hydrated.team_deliveries.len(),
            delivery_status_counts,
        }
    };

    let (process_status_counts, include_legacy_process_anomalies) =
        if let Some(counts) = state.app.services.store.managed_process_status_counts()? {
            (counts, false)
        } else {
            let managed_processes = match state.app.services.store.list_managed_processes() {
                Ok(rows) => Some(rows),
                Err(runtime_core::RuntimeError::Unsupported(_)) => None,
                Err(error) => return Err(ApiError::from(error)),
            };
            let mut counts = BTreeMap::<String, usize>::new();
            if let Some(managed) = &managed_processes {
                for process in managed {
                    *counts.entry(process.status.clone()).or_insert(0) += 1;
                }
            } else {
                for process in &hydrated.processes {
                    *counts.entry(process.status.clone()).or_insert(0) += 1;
                }
            }
            (counts, managed_processes.is_none())
        };
    let processes = ProcessDiagnosticsResponse::from_status_counts(process_status_counts);

    let worktrees = if let Some(counts) = state
        .app
        .services
        .store
        .managed_worktree_diagnostic_counts()?
    {
        WorktreeDiagnosticsResponse::from_counts(counts)
    } else {
        worktree_diagnostics_from_hydrated_state(&hydrated)
    };

    let recovery = RecoveryDiagnosticsResponse {
        startup: (*state.startup_recovery).clone(),
        active_anomalies: collect_recovery_anomalies(&hydrated, include_legacy_process_anomalies),
    };
    Ok((comms, processes, worktrees, recovery))
}

fn worktree_diagnostics_from_hydrated_state(
    hydrated: &runtime_core::RuntimeHydratedState,
) -> WorktreeDiagnosticsResponse {
    let mut deletion_policy_counts = BTreeMap::<String, usize>::new();
    for worktree in &hydrated.managed_worktrees {
        *deletion_policy_counts
            .entry(worktree.deletion_policy.clone())
            .or_insert(0) += 1;
    }

    let known_sessions = hydrated
        .sessions
        .iter()
        .map(|session| session.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let known_worktrees = hydrated
        .managed_worktrees
        .iter()
        .map(|worktree| worktree.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let orphaned_claim_total = hydrated
        .managed_worktree_claims
        .iter()
        .filter(|claim| {
            claim.released_at.is_none()
                && (!known_sessions.contains(claim.session_id.as_str())
                    || !known_worktrees.contains(claim.worktree_id.as_str()))
        })
        .count();

    WorktreeDiagnosticsResponse {
        worktree_total: hydrated.managed_worktrees.len(),
        active_claim_total: hydrated
            .managed_worktree_claims
            .iter()
            .filter(|claim| claim.released_at.is_none())
            .count(),
        orphaned_claim_total,
        deletion_policy_counts,
    }
}

pub(super) fn collect_recovery_anomalies(
    hydrated: &runtime_core::RuntimeHydratedState,
    include_legacy_process_anomalies: bool,
) -> Vec<String> {
    let mut anomalies = Vec::new();
    let turn_by_id = hydrated
        .turns
        .iter()
        .map(|turn| (turn.id.as_str(), turn))
        .collect::<std::collections::HashMap<_, _>>();
    let session_ids = hydrated
        .sessions
        .iter()
        .map(|session| session.id.as_str())
        .collect::<std::collections::HashSet<_>>();

    for session in &hydrated.sessions {
        if let Some(active_turn_id) = session.active_turn_id.as_deref() {
            match turn_by_id.get(active_turn_id) {
                None => anomalies.push(format!(
                    "session {} references missing active turn {}",
                    session.id, active_turn_id
                )),
                Some(turn) if turn.session_id != session.id => anomalies.push(format!(
                    "session {} active turn {} belongs to {}",
                    session.id, active_turn_id, turn.session_id
                )),
                Some(turn)
                    if matches!(turn.status.as_str(), "completed" | "interrupted" | "failed") =>
                {
                    anomalies.push(format!(
                        "session {} retains terminal active turn {} ({})",
                        session.id, active_turn_id, turn.status
                    ))
                }
                _ => {}
            }
        }
    }

    for approval in &hydrated.approvals {
        if approval.status != "pending" {
            continue;
        }
        match turn_by_id.get(approval.turn_id.as_str()) {
            None => anomalies.push(format!(
                "pending approval {} references missing turn {}",
                approval.id, approval.turn_id
            )),
            Some(turn)
                if matches!(turn.status.as_str(), "completed" | "interrupted" | "failed") =>
            {
                anomalies.push(format!(
                    "pending approval {} references terminal turn {} ({})",
                    approval.id, turn.id, turn.status
                ))
            }
            _ => {}
        }
    }

    for claim in &hydrated.managed_worktree_claims {
        if claim.released_at.is_none() && !session_ids.contains(claim.session_id.as_str()) {
            anomalies.push(format!(
                "active worktree claim {} -> {} references missing session",
                claim.worktree_id, claim.session_id
            ));
        }
    }

    if include_legacy_process_anomalies {
        for process in &hydrated.processes {
            if matches!(process.status.as_str(), "queued" | "running") {
                anomalies.push(format!(
                    "process {} remained {} across restart",
                    process.id, process.status
                ));
            }
        }
    }

    anomalies
}

#[derive(Debug, Deserialize)]
pub(super) struct TeamOperationDiagnosticsQuery {
    team_id: Option<String>,
    operation_id: Option<String>,
}

pub(super) async fn list_team_operation_diagnostics(
    State(state): State<AppState>,
    Query(query): Query<TeamOperationDiagnosticsQuery>,
) -> Result<Json<Vec<runtime_core::TeamOperationDiagnosticRecord>>, ApiError> {
    let rows = state
        .app
        .services
        .store
        .list_team_operation_diagnostics(query.team_id.as_deref(), query.operation_id.as_deref())
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

#[derive(Debug, Serialize)]
pub(super) struct TeamOperationRecoveryView {
    operation: runtime_core::TeamOperationJournalRecord,
    diagnostics: Vec<runtime_core::TeamOperationDiagnosticRecord>,
    /// This endpoint never grants permission to repeat native side effects.
    native_retry_authorized: bool,
    requires_manual_review: bool,
}

/// Exact indexed recovery lookup, including the durable stage that the
/// diagnostics-only list cannot expose. Auth is enforced by the /v1 scope.
pub(super) async fn get_team_operation_recovery(
    State(state): State<AppState>,
    Path(operation_id): Path<String>,
) -> Result<Json<TeamOperationRecoveryView>, ApiError> {
    let operation_id = operation_id.trim();
    let operation = state
        .app
        .services
        .store
        .get_team_operation_journal(operation_id)?
        .ok_or_else(|| ApiError::not_found(format!("team operation {operation_id}")))?;
    let diagnostics = state
        .app
        .services
        .store
        .list_team_operation_diagnostics(None, Some(operation_id))?;
    let requires_manual_review = !matches!(operation.stage.as_str(), "completed" | "rolled_back");
    Ok(Json(TeamOperationRecoveryView {
        operation,
        diagnostics,
        native_retry_authorized: false,
        requires_manual_review,
    }))
}
