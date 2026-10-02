use super::*;
use runtime_core::{
    plan_legacy_workspace_migration, prepare_legacy_workspace_migration_apply,
    prepare_legacy_workspace_migration_resolution, LegacyWorkspaceMigrationResolutionRequest,
    LegacyWorkspaceMigrationSubjectKind, OperationActor,
};

pub(super) async fn preview_workspace_migration(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::LegacyWorkspaceMigrationStatus>, ApiError> {
    Ok(Json(refresh_preview(&state)?))
}

pub(super) async fn get_workspace_migration_status(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::LegacyWorkspaceMigrationStatus>, ApiError> {
    Ok(Json(state.app.services.store.workspace_migration_status()?))
}

pub(super) async fn apply_workspace_migration(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<runtime_core::LegacyWorkspaceMigrationApplyResponse>, ApiError> {
    let command = prepare_legacy_workspace_migration_apply(
        OperationActor::operator(super::workspaces::OPERATOR_PRINCIPAL),
        super::workspaces::parse_idempotency_key(&headers)?,
    )?;
    Ok(Json(
        state
            .app
            .services
            .store
            .apply_workspace_migration(&command)?,
    ))
}

pub(super) async fn resolve_workspace_migration_subject(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((subject_kind, subject_id)): Path<(String, String)>,
    Json(request): Json<LegacyWorkspaceMigrationResolutionRequest>,
) -> Result<Json<runtime_core::LegacyWorkspaceMigrationResolutionResponse>, ApiError> {
    let subject_kind = LegacyWorkspaceMigrationSubjectKind::from_str(subject_kind.trim())
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "invalid workspace migration subject kind {subject_kind:?}"
            ))
        })?;
    let command = prepare_legacy_workspace_migration_resolution(
        subject_kind,
        subject_id,
        request,
        OperationActor::operator(super::workspaces::OPERATOR_PRINCIPAL),
        super::workspaces::parse_idempotency_key(&headers)?,
    )?;
    Ok(Json(
        state
            .app
            .services
            .store
            .resolve_workspace_migration_subject(&command)?,
    ))
}

fn refresh_preview(
    state: &AppState,
) -> Result<runtime_core::LegacyWorkspaceMigrationStatus, ApiError> {
    let legacy = state.app.services.store.hydrate_runtime_state()?;
    let workspaces = state.app.services.store.list_workspaces()?;
    let preview = plan_legacy_workspace_migration(&legacy, &workspaces);
    Ok(state
        .app
        .services
        .store
        .persist_workspace_migration_preview(&preview)?)
}
