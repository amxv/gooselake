use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use runtime_core::{
    resolve_repository_identity, LegacyWorkspaceMigrationApplyCommand,
    LegacyWorkspaceMigrationApplyResponse, LegacyWorkspaceMigrationClassification,
    LegacyWorkspaceMigrationResolutionAction, LegacyWorkspaceMigrationResolutionCommand,
    LegacyWorkspaceMigrationResolutionResponse, LegacyWorkspaceMigrationResolutionSource,
    LegacyWorkspaceMigrationStatus, LegacyWorkspaceMigrationSubject,
    LegacyWorkspaceMigrationSubjectKind, OperationPhase, OperationRecord,
    OperationTransitionRecord, RuntimeError, WorkspaceLifecycleState, WorkspaceRecord,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{collect_rows, db_error, json_to_string, open_connection, string_to_json};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::{
    operation_by_idempotency, workspace_by_canonical_root, workspace_by_id,
};
use crate::SqliteRuntimeRepository;

const MIGRATION_APPLY_KIND: &str = "legacy_workspace_migration_apply";
const MIGRATION_RESOLUTION_KIND: &str = "legacy_workspace_migration_resolution";
const MIGRATION_RESOURCE_KIND: &str = "legacy_workspace_migration";
const MIGRATION_RESOURCE_ID: &str = "workspace_authority";
const MIGRATION_SUBJECT_RESOURCE_KIND: &str = "legacy_workspace_migration_subject";

impl SqliteRuntimeRepository {
    pub fn persist_workspace_migration_preview(
        &self,
        status: &LegacyWorkspaceMigrationStatus,
    ) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed to start workspace migration preview transaction",
                    error,
                )
            })?;
        let updated_at = now_ms()?;

        transaction
            .execute(
                "DELETE FROM legacy_workspace_migration_subjects
                 WHERE resolution_source = 'deterministic' AND applied_at IS NULL",
                [],
            )
            .map_err(|error| db_error("failed refreshing migration preview rows", error))?;

        for subject in &status.subjects {
            let protected = migration_subject_by_key(
                &transaction,
                subject.subject_kind,
                subject.subject_id.as_str(),
            )?
            .is_some_and(|existing| {
                existing.resolution_source
                    != LegacyWorkspaceMigrationResolutionSource::Deterministic
                    || existing.applied_at.is_some()
            });
            if protected {
                continue;
            }
            insert_migration_subject(&transaction, subject, updated_at)?;
        }
        transaction
            .execute(
                "INSERT INTO legacy_workspace_migration_state (migration_key, previewed_at)
                 VALUES ('workspace_authority', ?1)
                 ON CONFLICT(migration_key) DO UPDATE SET previewed_at = excluded.previewed_at",
                params![updated_at],
            )
            .map_err(|error| db_error("failed recording workspace migration preview", error))?;

        let persisted = migration_status_from_connection(&transaction)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing workspace migration preview", error))?;
        Ok(persisted)
    }

    pub fn workspace_migration_status(
        &self,
    ) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        migration_status_from_connection(&connection)
    }

    pub fn apply_workspace_migration(
        &self,
        command: &LegacyWorkspaceMigrationApplyCommand,
    ) -> Result<LegacyWorkspaceMigrationApplyResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed to start durable workspace migration transaction",
                    error,
                )
            })?;

        if let Some(response) = replay_operation::<LegacyWorkspaceMigrationApplyResponse>(
            &transaction,
            command.actor.kind,
            command.actor.identifier.as_str(),
            command.idempotency_key.as_deref(),
            command.normalized_request_hash.as_str(),
            "workspace migration",
        )? {
            transaction
                .commit()
                .map_err(|error| db_error("failed committing workspace migration replay", error))?;
            return Ok(response);
        }

        let preview_exists = transaction
            .query_row(
                "SELECT 1 FROM legacy_workspace_migration_state
                 WHERE migration_key = 'workspace_authority'",
                [],
                |_| Ok(()),
            )
            .optional()
            .map_err(|error| db_error("failed checking workspace migration preview", error))?
            .is_some();
        if !preview_exists {
            return Err(RuntimeError::InvalidState(
                "workspace migration preview has not been run; preview before apply".to_string(),
            ));
        }
        let mut subjects = migration_subjects_from_connection(&transaction)?;

        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: None,
            kind: MIGRATION_APPLY_KIND.to_string(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: None,
            created_at: command.requested_at,
        })?;
        let claim = authority.acquire_claim(
            &command.operation_id,
            MIGRATION_RESOURCE_KIND,
            MIGRATION_RESOURCE_ID,
            command.requested_at,
        )?;

        let mut created_workspaces = 0usize;
        let mut mapped_subjects_applied = 0usize;
        let mut archived_subjects_applied = 0usize;
        let mut owned_sessions_created = 0usize;

        for subject in &mut subjects {
            if subject.applied_at.is_some() {
                continue;
            }
            match subject.classification {
                LegacyWorkspaceMigrationClassification::Mapped => {
                    let workspace =
                        resolve_subject_workspace(&transaction, subject, command.requested_at)?;
                    if subject.workspace_id.is_none() {
                        created_workspaces += usize::from(workspace.created);
                        subject.workspace_id = Some(workspace.record.workspace_id.clone());
                        update_subject_workspace(
                            &transaction,
                            subject,
                            &workspace.record.workspace_id,
                            command.requested_at,
                        )?;
                    }
                    if subject.subject_kind == LegacyWorkspaceMigrationSubjectKind::Session {
                        owned_sessions_created += usize::from(ensure_session_authority(
                            &transaction,
                            subject.subject_id.as_str(),
                            &workspace.record.workspace_id,
                            &command.operation_id,
                            command.requested_at,
                        )?);
                    }
                    mark_subject_applied(
                        &transaction,
                        subject.subject_kind,
                        subject.subject_id.as_str(),
                        command.requested_at,
                    )?;
                    subject.applied_at = Some(command.requested_at);
                    mapped_subjects_applied += 1;
                }
                LegacyWorkspaceMigrationClassification::ArchivedHistory => {
                    mark_subject_applied(
                        &transaction,
                        subject.subject_kind,
                        subject.subject_id.as_str(),
                        command.requested_at,
                    )?;
                    subject.applied_at = Some(command.requested_at);
                    archived_subjects_applied += 1;
                }
                LegacyWorkspaceMigrationClassification::Unresolved => {}
            }
        }

        let unresolved_subjects = subjects
            .iter()
            .filter(|subject| {
                subject.classification == LegacyWorkspaceMigrationClassification::Unresolved
            })
            .count();
        let response = LegacyWorkspaceMigrationApplyResponse {
            operation_id: command.operation_id.clone(),
            created_workspaces,
            mapped_subjects_applied,
            archived_subjects_applied,
            owned_sessions_created,
            unresolved_subjects,
            cutover_blocked: unresolved_subjects > 0,
        };
        let exact_result = serde_json::to_value(&response).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed serializing workspace migration result: {error}"
            ))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            None,
            &exact_result,
            None,
            None,
            command.requested_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({
                "created_workspaces": created_workspaces,
                "mapped_subjects_applied": mapped_subjects_applied,
                "archived_subjects_applied": archived_subjects_applied,
                "owned_sessions_created": owned_sessions_created,
                "unresolved_subjects": unresolved_subjects,
                "cutover_blocked": unresolved_subjects > 0,
                "resource_fence_generation": claim.fence_generation,
            })),
            created_at: command.requested_at,
        })?;
        authority.release_claim(&claim)?;

        transaction
            .commit()
            .map_err(|error| db_error("failed committing durable workspace migration", error))?;
        Ok(response)
    }

    pub fn resolve_workspace_migration_subject(
        &self,
        command: &LegacyWorkspaceMigrationResolutionCommand,
    ) -> Result<LegacyWorkspaceMigrationResolutionResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed to start workspace migration resolution transaction",
                    error,
                )
            })?;

        if let Some(response) = replay_operation::<LegacyWorkspaceMigrationResolutionResponse>(
            &transaction,
            command.actor.kind,
            command.actor.identifier.as_str(),
            command.idempotency_key.as_deref(),
            command.normalized_request_hash.as_str(),
            "workspace migration resolution",
        )? {
            transaction.commit().map_err(|error| {
                db_error("failed committing migration resolution replay", error)
            })?;
            return Ok(response);
        }

        let current = migration_subject_by_key(
            &transaction,
            command.subject_kind,
            command.subject_id.as_str(),
        )?
        .ok_or_else(|| {
            RuntimeError::NotFound(format!(
                "workspace migration subject {}:{}",
                command.subject_kind.as_str(),
                command.subject_id
            ))
        })?;
        if current.classification != LegacyWorkspaceMigrationClassification::Unresolved {
            return Err(RuntimeError::Conflict(format!(
                "workspace migration subject {}:{} is already {}",
                command.subject_kind.as_str(),
                command.subject_id,
                current.classification.as_str()
            )));
        }

        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: command.workspace_id.clone(),
            kind: MIGRATION_RESOLUTION_KIND.to_string(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: None,
            created_at: command.requested_at,
        })?;
        let resource_id = format!("{}:{}", command.subject_kind.as_str(), command.subject_id);
        let claim = authority.acquire_claim(
            &command.operation_id,
            MIGRATION_SUBJECT_RESOURCE_KIND,
            &resource_id,
            command.requested_at,
        )?;

        let (classification, resolution_source, workspace) = match command.action {
            LegacyWorkspaceMigrationResolutionAction::Map => {
                let workspace_id = command.workspace_id.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidState(
                        "workspace_id is required for migration mapping".to_string(),
                    )
                })?;
                let workspace = require_active_workspace(&transaction, workspace_id)?;
                if command.subject_kind == LegacyWorkspaceMigrationSubjectKind::Session {
                    ensure_session_authority(
                        &transaction,
                        command.subject_id.as_str(),
                        workspace_id,
                        &command.operation_id,
                        command.requested_at,
                    )?;
                }
                (
                    LegacyWorkspaceMigrationClassification::Mapped,
                    LegacyWorkspaceMigrationResolutionSource::OperatorMap,
                    Some(workspace),
                )
            }
            LegacyWorkspaceMigrationResolutionAction::Archive => {
                if command.subject_kind == LegacyWorkspaceMigrationSubjectKind::Session
                    && session_workspace_id(&transaction, command.subject_id.as_str())?.is_some()
                {
                    return Err(RuntimeError::Conflict(format!(
                        "session {} already has immutable workspace ownership and cannot be archived",
                        command.subject_id
                    )));
                }
                (
                    LegacyWorkspaceMigrationClassification::ArchivedHistory,
                    LegacyWorkspaceMigrationResolutionSource::OperatorArchive,
                    None,
                )
            }
        };

        let evidence = serde_json::json!({
            "previous_reason_code": current.reason_code,
            "previous_evidence": current.evidence,
            "operator_resolution": {
                "action": command.action,
                "actor_kind": command.actor.kind,
                "actor_id": command.actor.identifier,
                "operation_id": command.operation_id,
            }
        });
        transaction
            .execute(
                "UPDATE legacy_workspace_migration_subjects
                 SET classification = ?3, workspace_id = ?4,
                     canonical_root = COALESCE(?5, canonical_root),
                     resolution_source = ?6, reason_code = ?7, evidence_json = ?8,
                     applied_at = ?9, updated_at = ?9
                 WHERE subject_kind = ?1 AND subject_id = ?2",
                params![
                    command.subject_kind.as_str(),
                    command.subject_id,
                    classification.as_str(),
                    workspace
                        .as_ref()
                        .map(|workspace| workspace.workspace_id.as_str()),
                    workspace
                        .as_ref()
                        .map(|workspace| workspace.canonical_root.as_str()),
                    resolution_source.as_str(),
                    match command.action {
                        LegacyWorkspaceMigrationResolutionAction::Map => "operator_mapped",
                        LegacyWorkspaceMigrationResolutionAction::Archive => "operator_archived",
                    },
                    json_to_string(&evidence)?,
                    command.requested_at,
                ],
            )
            .map_err(|error| db_error("failed persisting migration subject resolution", error))?;

        let subject = migration_subject_by_key(
            &transaction,
            command.subject_kind,
            command.subject_id.as_str(),
        )?
        .ok_or_else(|| {
            RuntimeError::Bootstrap(
                "resolved workspace migration subject disappeared before commit".to_string(),
            )
        })?;
        let response = LegacyWorkspaceMigrationResolutionResponse {
            operation_id: command.operation_id.clone(),
            subject,
        };
        let exact_result = serde_json::to_value(&response).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed serializing workspace migration resolution result: {error}"
            ))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            command.workspace_id.as_deref(),
            &exact_result,
            None,
            None,
            command.requested_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({
                "subject_kind": command.subject_kind,
                "subject_id": command.subject_id,
                "action": command.action,
                "workspace_id": command.workspace_id,
                "resource_fence_generation": claim.fence_generation,
            })),
            created_at: command.requested_at,
        })?;
        authority.release_claim(&claim)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing workspace migration resolution", error))?;
        Ok(response)
    }
}

struct ResolvedWorkspace {
    record: WorkspaceRecord,
    created: bool,
}

fn resolve_subject_workspace(
    transaction: &Transaction<'_>,
    subject: &LegacyWorkspaceMigrationSubject,
    created_at: i64,
) -> Result<ResolvedWorkspace, RuntimeError> {
    verify_subject_repository_identity(subject)?;
    if let Some(workspace_id) = subject.workspace_id.as_deref() {
        return Ok(ResolvedWorkspace {
            record: require_active_workspace(transaction, workspace_id)?,
            created: false,
        });
    }
    let canonical_root = subject.canonical_root.as_deref().ok_or_else(|| {
        RuntimeError::InvalidState(format!(
            "mapped migration subject {}:{} has no canonical repository root",
            subject.subject_kind.as_str(),
            subject.subject_id
        ))
    })?;
    if let Some(existing) = workspace_by_canonical_root(transaction, canonical_root)? {
        if existing.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::Conflict(format!(
                "workspace {} for migrated root {} is retired",
                existing.workspace_id, canonical_root
            )));
        }
        return Ok(ResolvedWorkspace {
            record: existing,
            created: false,
        });
    }

    let fingerprint = subject.repository_fingerprint.as_deref().ok_or_else(|| {
        RuntimeError::InvalidState(format!(
            "mapped migration subject {}:{} has no repository fingerprint",
            subject.subject_kind.as_str(),
            subject.subject_id
        ))
    })?;
    let workspace_id = format!("workspace_legacy_{fingerprint}");
    if let Some(existing) = workspace_by_id(transaction, &workspace_id)? {
        return Err(RuntimeError::Conflict(format!(
            "deterministic migration workspace id {} already belongs to root {}",
            workspace_id, existing.canonical_root
        )));
    }
    let display_name = Path::new(canonical_root)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or(canonical_root);
    transaction
        .execute(
            "INSERT INTO workspaces (
                workspace_id, canonical_root, display_name, lifecycle_state,
                revision, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'active', 0, ?4, ?4)",
            params![workspace_id, canonical_root, display_name, created_at],
        )
        .map_err(|error| db_error("failed creating migrated workspace", error))?;
    Ok(ResolvedWorkspace {
        record: workspace_by_id(transaction, &workspace_id)?.ok_or_else(|| {
            RuntimeError::Bootstrap(
                "created migrated workspace disappeared before commit".to_string(),
            )
        })?,
        created: true,
    })
}

fn verify_subject_repository_identity(
    subject: &LegacyWorkspaceMigrationSubject,
) -> Result<(), RuntimeError> {
    let (Some(canonical_root), Some(expected_fingerprint)) = (
        subject.canonical_root.as_deref(),
        subject.repository_fingerprint.as_deref(),
    ) else {
        return Ok(());
    };
    let current = resolve_repository_identity(Path::new(canonical_root)).map_err(|error| {
        RuntimeError::InvalidState(format!(
            "repository evidence for migration subject {}:{} changed after preview: {error}",
            subject.subject_kind.as_str(),
            subject.subject_id
        ))
    })?;
    if current.canonical_root != canonical_root || current.fingerprint != expected_fingerprint {
        return Err(RuntimeError::Conflict(format!(
            "repository evidence for migration subject {}:{} changed after preview; run preview again",
            subject.subject_kind.as_str(),
            subject.subject_id
        )));
    }
    Ok(())
}

fn require_active_workspace(
    connection: &Connection,
    workspace_id: &str,
) -> Result<WorkspaceRecord, RuntimeError> {
    let workspace = workspace_by_id(connection, workspace_id)?
        .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
    if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
        return Err(RuntimeError::Conflict(format!(
            "workspace {workspace_id} is retired; explicit reactivation is required"
        )));
    }
    Ok(workspace)
}

fn ensure_session_authority(
    transaction: &Transaction<'_>,
    session_id: &str,
    workspace_id: &str,
    operation_id: &str,
    created_at: i64,
) -> Result<bool, RuntimeError> {
    let session_exists = transaction
        .query_row(
            "SELECT 1 FROM sessions WHERE id = ?1",
            params![session_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| db_error("failed checking migrated session", error))?
        .is_some();
    if !session_exists {
        return Err(RuntimeError::NotFound(format!(
            "legacy session {session_id}"
        )));
    }

    let existing_workspace = session_workspace_id(transaction, session_id)?;
    let created = match existing_workspace {
        Some(existing) if existing == workspace_id => false,
        Some(existing) => {
            return Err(RuntimeError::Conflict(format!(
                "session {session_id} already belongs to immutable workspace {existing}; cannot map to {workspace_id}"
            )));
        }
        None => {
            transaction
                .execute(
                    "INSERT INTO workspace_session_ownership (
                        session_id, workspace_id, source, source_operation_id, created_at
                     ) VALUES (?1, ?2, 'legacy_migration', ?3, ?4)",
                    params![session_id, workspace_id, operation_id, created_at],
                )
                .map_err(|error| db_error("failed inserting workspace session ownership", error))?;
            true
        }
    };

    let profile_workspace = transaction
        .query_row(
            "SELECT workspace_id FROM workspace_agent_profiles WHERE session_id = ?1",
            params![session_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading migrated workspace agent profile", error))?;
    match profile_workspace {
        Some(existing) if existing == workspace_id => {}
        Some(existing) => {
            return Err(RuntimeError::Conflict(format!(
                "session {session_id} already has a profile in workspace {existing}; cannot map to {workspace_id}"
            )));
        }
        None => {
            let joined_at = transaction
                .query_row(
                    "SELECT created_at FROM sessions WHERE id = ?1",
                    params![session_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|error| {
                    db_error("failed reading migrated session creation time", error)
                })?;
            transaction
                .execute(
                    "INSERT INTO workspace_agent_profiles (
                        session_id, workspace_id, title, title_provenance, added_by,
                        creator_session_id, creator_compaction_subscription, joined_at,
                        revision, created_at, updated_at
                     ) VALUES (?1, ?2, NULL, 'legacy_migration', 'legacy_migration',
                               NULL, 'auto', ?3, 0, ?4, ?4)",
                    params![session_id, workspace_id, joined_at, created_at],
                )
                .map_err(|error| {
                    db_error("failed inserting migrated workspace agent profile", error)
                })?;
        }
    }
    Ok(created)
}

fn session_workspace_id(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<String>, RuntimeError> {
    connection
        .query_row(
            "SELECT workspace_id FROM workspace_session_ownership WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading workspace session ownership", error))
}

fn update_subject_workspace(
    transaction: &Transaction<'_>,
    subject: &LegacyWorkspaceMigrationSubject,
    workspace_id: &str,
    updated_at: i64,
) -> Result<(), RuntimeError> {
    let updated = transaction
        .execute(
            "UPDATE legacy_workspace_migration_subjects
             SET workspace_id = ?3, updated_at = ?4
             WHERE subject_kind = ?1 AND subject_id = ?2 AND classification = 'mapped'",
            params![
                subject.subject_kind.as_str(),
                subject.subject_id,
                workspace_id,
                updated_at
            ],
        )
        .map_err(|error| db_error("failed attaching workspace to migration subject", error))?;
    if updated != 1 {
        return Err(RuntimeError::Conflict(format!(
            "migration subject {}:{} changed while applying",
            subject.subject_kind.as_str(),
            subject.subject_id
        )));
    }
    Ok(())
}

fn mark_subject_applied(
    transaction: &Transaction<'_>,
    kind: LegacyWorkspaceMigrationSubjectKind,
    subject_id: &str,
    applied_at: i64,
) -> Result<(), RuntimeError> {
    let updated = transaction
        .execute(
            "UPDATE legacy_workspace_migration_subjects
             SET applied_at = ?3, updated_at = ?3
             WHERE subject_kind = ?1 AND subject_id = ?2 AND applied_at IS NULL",
            params![kind.as_str(), subject_id, applied_at],
        )
        .map_err(|error| db_error("failed marking migration subject applied", error))?;
    if updated != 1 {
        return Err(RuntimeError::Conflict(format!(
            "migration subject {}:{} changed while applying",
            kind.as_str(),
            subject_id
        )));
    }
    Ok(())
}

fn insert_migration_subject(
    transaction: &Transaction<'_>,
    subject: &LegacyWorkspaceMigrationSubject,
    updated_at: i64,
) -> Result<(), RuntimeError> {
    transaction
        .execute(
            "INSERT INTO legacy_workspace_migration_subjects (
                subject_kind, subject_id, classification, workspace_id,
                canonical_root, git_common_dir, repository_fingerprint,
                reason_code, evidence_json, resolution_source, applied_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                subject.subject_kind.as_str(),
                subject.subject_id,
                subject.classification.as_str(),
                subject.workspace_id,
                subject.canonical_root,
                subject.git_common_dir,
                subject.repository_fingerprint,
                subject.reason_code,
                json_to_string(&subject.evidence)?,
                subject.resolution_source.as_str(),
                subject.applied_at,
                subject.updated_at.unwrap_or(updated_at),
            ],
        )
        .map_err(|error| db_error("failed inserting workspace migration subject", error))?;
    Ok(())
}

fn migration_status_from_connection(
    connection: &Connection,
) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
    Ok(LegacyWorkspaceMigrationStatus::from_subjects(
        migration_subjects_from_connection(connection)?,
    ))
}

fn migration_subjects_from_connection(
    connection: &Connection,
) -> Result<Vec<LegacyWorkspaceMigrationSubject>, RuntimeError> {
    let mut statement = connection
        .prepare(
            "SELECT subject_kind, subject_id, classification, workspace_id,
                    canonical_root, git_common_dir, repository_fingerprint,
                    reason_code, evidence_json, resolution_source, applied_at, updated_at
             FROM legacy_workspace_migration_subjects
             ORDER BY subject_kind, subject_id",
        )
        .map_err(|error| db_error("failed preparing workspace migration status query", error))?;
    let rows = statement
        .query_map([], migration_subject_from_row)
        .map_err(|error| db_error("failed querying workspace migration status", error))?;
    collect_rows(rows)
}

fn migration_subject_by_key(
    connection: &Connection,
    kind: LegacyWorkspaceMigrationSubjectKind,
    subject_id: &str,
) -> Result<Option<LegacyWorkspaceMigrationSubject>, RuntimeError> {
    connection
        .query_row(
            "SELECT subject_kind, subject_id, classification, workspace_id,
                    canonical_root, git_common_dir, repository_fingerprint,
                    reason_code, evidence_json, resolution_source, applied_at, updated_at
             FROM legacy_workspace_migration_subjects
             WHERE subject_kind = ?1 AND subject_id = ?2",
            params![kind.as_str(), subject_id],
            migration_subject_from_row,
        )
        .optional()
        .map_err(|error| db_error("failed querying workspace migration subject", error))
}

fn migration_subject_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LegacyWorkspaceMigrationSubject> {
    let kind_text: String = row.get(0)?;
    let classification_text: String = row.get(2)?;
    let resolution_source_text: String = row.get(9)?;
    Ok(LegacyWorkspaceMigrationSubject {
        subject_kind: LegacyWorkspaceMigrationSubjectKind::from_str(&kind_text).ok_or_else(
            || invalid_text(0, format!("invalid migration subject kind {kind_text:?}")),
        )?,
        subject_id: row.get(1)?,
        classification: LegacyWorkspaceMigrationClassification::from_str(&classification_text)
            .ok_or_else(|| {
                invalid_text(
                    2,
                    format!("invalid migration classification {classification_text:?}"),
                )
            })?,
        workspace_id: row.get(3)?,
        canonical_root: row.get(4)?,
        git_common_dir: row.get(5)?,
        repository_fingerprint: row.get(6)?,
        reason_code: row.get(7)?,
        evidence: string_to_json(row.get(8)?)?,
        resolution_source: LegacyWorkspaceMigrationResolutionSource::from_str(
            &resolution_source_text,
        )
        .ok_or_else(|| {
            invalid_text(
                9,
                format!("invalid migration resolution source {resolution_source_text:?}"),
            )
        })?,
        applied_at: row.get(10)?,
        updated_at: Some(row.get(11)?),
    })
}

fn replay_operation<T>(
    transaction: &Transaction<'_>,
    actor_kind: runtime_core::OperationActorKind,
    actor_id: &str,
    idempotency_key: Option<&str>,
    normalized_request_hash: &str,
    label: &str,
) -> Result<Option<T>, RuntimeError>
where
    T: serde::de::DeserializeOwned,
{
    let Some(existing) =
        operation_by_idempotency(transaction, actor_kind, actor_id, idempotency_key)?
    else {
        return Ok(None);
    };
    if existing.normalized_request_hash != normalized_request_hash {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key already belongs to operation {} with different input",
            existing.operation_id
        )));
    }
    let result = existing.exact_terminal_result.ok_or_else(|| {
        RuntimeError::Conflict(format!(
            "operation {} is not terminal and must be recovered before replay",
            existing.operation_id
        ))
    })?;
    serde_json::from_value(result).map(Some).map_err(|error| {
        RuntimeError::Bootstrap(format!("failed decoding exact {label} result: {error}"))
    })
}

fn now_ms() -> Result<i64, RuntimeError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RuntimeError::Bootstrap(format!("system clock error: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| RuntimeError::Bootstrap("unix timestamp overflow".to_string()))
}

fn invalid_text(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}
