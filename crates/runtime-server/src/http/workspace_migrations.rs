use super::*;
use runtime_core::{
    plan_legacy_workspace_migration, prepare_legacy_workspace_migration_apply,
    prepare_legacy_workspace_migration_resolution, LegacyWorkspaceMigrationResolutionRequest,
    LegacyWorkspaceMigrationSubjectKind, OperationActor,
};

pub(super) async fn preview_workspace_migration(
    State(state): State<AppState>,
    Query(query): Query<MigrationSubjectPageQuery>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    require_empty_request_body(&body)?;
    reject_unsupported_if_match(&headers, "workspace migration preview")?;
    reject_unjournaled_idempotency_key(&headers, "workspace migration preview")?;
    let (limit, cursor) = query.parse()?;
    paged_migration_status(refresh_preview(&state)?, limit, cursor)
}

pub(super) async fn get_workspace_migration_status(
    State(state): State<AppState>,
    Query(query): Query<MigrationSubjectPageQuery>,
) -> Result<Response, ApiError> {
    let (limit, cursor) = query.parse()?;
    let status = state.app.services.store.workspace_migration_status()?;
    paged_migration_status(status, limit, cursor)
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct MigrationSubjectPageQuery {
    cursor: Option<String>,
    #[schemars(range(min = 1, max = 100))]
    limit: Option<usize>,
}

impl MigrationSubjectPageQuery {
    fn parse(&self) -> Result<(usize, Option<(u8, String)>), ApiError> {
        let limit = validate_opaque_page_limit(self.limit)?;
        let cursor = match self.cursor.as_deref() {
            Some(encoded) => {
                let (rank, id) = decode_opaque_pair_cursor(encoded)?;
                let rank = rank
                    .parse::<u8>()
                    .ok()
                    .filter(|rank| *rank < 5)
                    .ok_or_else(|| {
                        ApiError::bad_request("invalid migration subject cursor".into())
                    })?;
                Some((rank, id))
            }
            None => None,
        };
        Ok((limit, cursor))
    }
}

fn migration_subject_rank(kind: LegacyWorkspaceMigrationSubjectKind) -> u8 {
    match kind {
        LegacyWorkspaceMigrationSubjectKind::Session => 0,
        LegacyWorkspaceMigrationSubjectKind::Team => 1,
        LegacyWorkspaceMigrationSubjectKind::TeamMember => 2,
        LegacyWorkspaceMigrationSubjectKind::ManagedWorktree => 3,
        LegacyWorkspaceMigrationSubjectKind::WorktreeClaim => 4,
    }
}

/// Totals describe the complete migration state while only a bounded set of
/// subject diagnostics is materialized into the HTTP response. The cursor
/// follows the same `(subject_kind,subject_id)` ordering as `from_subjects`.
fn paged_migration_status(
    mut status: runtime_core::LegacyWorkspaceMigrationStatus,
    limit: usize,
    cursor: Option<(u8, String)>,
) -> Result<Response, ApiError> {
    if let Some((kind, id)) = cursor {
        if !status.subjects.iter().any(|subject| {
            migration_subject_rank(subject.subject_kind) == kind && subject.subject_id == id
        }) {
            return Err(ApiError::bad_request(
                "migration cursor is not in the current subject collection".into(),
            ));
        }
        status.subjects.retain(|subject| {
            (
                migration_subject_rank(subject.subject_kind),
                subject.subject_id.as_str(),
            ) > (kind, id.as_str())
        });
    }
    let has_more = status.subjects.len() > limit;
    status.subjects.truncate(limit);
    let next_cursor = if has_more {
        status.subjects.last().map(|subject| {
            encode_opaque_pair_cursor(
                &migration_subject_rank(subject.subject_kind).to_string(),
                &subject.subject_id,
            )
        })
    } else {
        None
    };
    let mut response = Json(status).into_response();
    if let Some(next_cursor) = next_cursor {
        response.headers_mut().insert(
            "x-next-cursor",
            axum::http::HeaderValue::from_str(&next_cursor)
                .expect("hex migration cursor is valid header text"),
        );
    }
    Ok(response)
}

pub(super) async fn apply_workspace_migration(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    require_empty_request_body(&body)?;
    reject_unsupported_if_match(&headers, "workspace migration apply")?;
    let command = prepare_legacy_workspace_migration_apply(
        OperationActor::operator(super::workspaces::OPERATOR_PRINCIPAL),
        super::workspaces::parse_idempotency_key(&headers)?,
    )?;
    let result = state
        .app
        .services
        .store
        .apply_workspace_migration(&command)?;
    let id = result.operation_id.clone();
    Ok(with_operation_id(Json(result), &id))
}

pub(super) async fn resolve_workspace_migration_subject(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((subject_kind, subject_id)): Path<(String, String)>,
    Json(request): Json<LegacyWorkspaceMigrationResolutionRequest>,
) -> Result<Response, ApiError> {
    reject_unsupported_if_match(&headers, "workspace migration resolution")?;
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
    let result = state
        .app
        .services
        .store
        .resolve_workspace_migration_subject(&command)?;
    let id = result.operation_id.clone();
    Ok(with_operation_id(Json(result), &id))
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
