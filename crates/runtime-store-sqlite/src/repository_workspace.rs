use runtime_core::{
    OperationActor, OperationActorKind, OperationDetails, OperationEffectRecord,
    OperationOutboxReceiptRecord, OperationOutboxRecord, OperationPhase, OperationRecord,
    OperationResourceClaimRecord, OperationTransitionRecord, RuntimeError, WorkspaceLifecycleState,
    WorkspaceRecord, WorkspaceRegisterCommand, WorkspaceRegisterResponse,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection, string_to_json};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::SqliteRuntimeRepository;

const WORKSPACE_REGISTER_KIND: &str = "workspace_register";
const WORKSPACE_ROOT_RESOURCE_KIND: &str = "workspace_root";

impl SqliteRuntimeRepository {
    pub fn register_workspace(
        &self,
        command: &WorkspaceRegisterCommand,
    ) -> Result<WorkspaceRegisterResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed to start durable workspace registration transaction",
                    error,
                )
            })?;

        if let Some(existing) = operation_by_idempotency(
            &transaction,
            command.actor.kind,
            command.actor.identifier.as_str(),
            command.idempotency_key.as_deref(),
        )? {
            if existing.normalized_request_hash != command.normalized_request_hash {
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
            let response =
                serde_json::from_value::<WorkspaceRegisterResponse>(result).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "failed decoding exact workspace registration result: {error}"
                    ))
                })?;
            transaction.commit().map_err(|error| {
                db_error("failed committing workspace registration replay", error)
            })?;
            return Ok(response);
        }

        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: None,
            kind: WORKSPACE_REGISTER_KIND.to_string(),
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
            WORKSPACE_ROOT_RESOURCE_KIND,
            &command.canonical_root,
            command.requested_at,
        )?;

        let workspace = match workspace_by_canonical_root(&transaction, &command.canonical_root)? {
            Some(existing) if existing.lifecycle_state == WorkspaceLifecycleState::Active => {
                existing
            }
            Some(existing) => {
                return Err(RuntimeError::Conflict(format!(
                    "workspace {} is retired; explicit reactivation is required",
                    existing.workspace_id
                )));
            }
            None => insert_workspace(&transaction, command)?,
        };

        let response = WorkspaceRegisterResponse {
            operation_id: command.operation_id.clone(),
            workspace: workspace.clone(),
        };
        let exact_result = serde_json::to_value(&response).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed serializing exact workspace registration result: {error}"
            ))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&workspace.workspace_id),
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
                "workspace_id": workspace.workspace_id,
                "canonical_root": workspace.canonical_root,
                "resource_fence_generation": claim.fence_generation,
            })),
            created_at: command.requested_at,
        })?;
        authority.release_claim(&claim)?;

        transaction
            .commit()
            .map_err(|error| db_error("failed committing durable workspace registration", error))?;
        Ok(response)
    }

    pub fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let mut statement = connection
            .prepare(
                "SELECT workspace_id, canonical_root, display_name, lifecycle_state,
                        revision, created_at, updated_at
                 FROM workspaces
                 ORDER BY created_at ASC, workspace_id ASC",
            )
            .map_err(|error| db_error("failed preparing workspace list query", error))?;
        let rows = statement
            .query_map([], workspace_from_row)
            .map_err(|error| db_error("failed listing workspaces", error))?;
        collect_rows(rows)
    }

    pub fn get_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        workspace_by_id(&connection, workspace_id)
    }

    pub fn get_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationDetails>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let Some(operation) = operation_by_id(&connection, operation_id)? else {
            return Ok(None);
        };
        Ok(Some(OperationDetails {
            claims: claims_for_operation(&connection, operation_id)?,
            transitions: transitions_for_operation(&connection, operation_id)?,
            effects: effects_for_operation(&connection, operation_id)?,
            outbox: outbox_for_operation(&connection, operation_id)?,
            receipts: receipts_for_operation(&connection, operation_id)?,
            operation,
        }))
    }
}

fn operation_by_idempotency(
    connection: &Connection,
    actor_kind: OperationActorKind,
    actor_id: &str,
    idempotency_key: Option<&str>,
) -> Result<Option<OperationRecord>, RuntimeError> {
    let Some(idempotency_key) = idempotency_key else {
        return Ok(None);
    };
    connection
        .query_row(
            "SELECT operation_id, workspace_id, kind, actor_kind, actor_id, idempotency_key,
                    normalized_request_hash, normalized_request_json, phase,
                    exact_terminal_result_json, error_code, error_message, created_at, updated_at
             FROM runtime_operations
             WHERE actor_kind = ?1 AND actor_id = ?2 AND idempotency_key = ?3",
            params![actor_kind.as_str(), actor_id, idempotency_key],
            operation_from_row,
        )
        .optional()
        .map_err(|error| db_error("failed querying operation by idempotency identity", error))
}

fn operation_by_id(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<OperationRecord>, RuntimeError> {
    connection
        .query_row(
            "SELECT operation_id, workspace_id, kind, actor_kind, actor_id, idempotency_key,
                    normalized_request_hash, normalized_request_json, phase,
                    exact_terminal_result_json, error_code, error_message, created_at, updated_at
             FROM runtime_operations WHERE operation_id = ?1",
            params![operation_id],
            operation_from_row,
        )
        .optional()
        .map_err(|error| db_error("failed querying operation", error))
}

fn operation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OperationRecord> {
    let actor_kind_text: String = row.get(3)?;
    let actor_kind = OperationActorKind::from_str(&actor_kind_text).ok_or_else(|| {
        invalid_text(
            3,
            format!("invalid operation actor kind {actor_kind_text:?}"),
        )
    })?;
    let phase_text: String = row.get(8)?;
    let phase = OperationPhase::from_str(&phase_text)
        .ok_or_else(|| invalid_text(8, format!("invalid operation phase {phase_text:?}")))?;
    let terminal_json: Option<String> = row.get(9)?;
    Ok(OperationRecord {
        operation_id: row.get(0)?,
        workspace_id: row.get(1)?,
        kind: row.get(2)?,
        actor: OperationActor {
            kind: actor_kind,
            identifier: row.get(4)?,
        },
        idempotency_key: row.get(5)?,
        normalized_request_hash: row.get(6)?,
        normalized_request: string_to_json(row.get(7)?)?,
        phase,
        exact_terminal_result: terminal_json.map(string_to_json).transpose()?,
        error_code: row.get(10)?,
        error_message: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

fn workspace_by_canonical_root(
    connection: &Connection,
    canonical_root: &str,
) -> Result<Option<WorkspaceRecord>, RuntimeError> {
    connection
        .query_row(
            "SELECT workspace_id, canonical_root, display_name, lifecycle_state,
                    revision, created_at, updated_at
             FROM workspaces WHERE canonical_root = ?1",
            params![canonical_root],
            workspace_from_row,
        )
        .optional()
        .map_err(|error| db_error("failed querying workspace by canonical root", error))
}

fn workspace_by_id(
    connection: &Connection,
    workspace_id: &str,
) -> Result<Option<WorkspaceRecord>, RuntimeError> {
    connection
        .query_row(
            "SELECT workspace_id, canonical_root, display_name, lifecycle_state,
                    revision, created_at, updated_at
             FROM workspaces WHERE workspace_id = ?1",
            params![workspace_id],
            workspace_from_row,
        )
        .optional()
        .map_err(|error| db_error("failed querying workspace", error))
}

fn workspace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspaceRecord> {
    let lifecycle_text: String = row.get(3)?;
    let lifecycle_state = WorkspaceLifecycleState::from_str(&lifecycle_text).ok_or_else(|| {
        invalid_text(
            3,
            format!("invalid workspace lifecycle state {lifecycle_text:?}"),
        )
    })?;
    let revision: i64 = row.get(4)?;
    Ok(WorkspaceRecord {
        workspace_id: row.get(0)?,
        canonical_root: row.get(1)?,
        display_name: row.get(2)?,
        lifecycle_state,
        revision: u64::try_from(revision)
            .map_err(|_| invalid_text(4, "negative workspace revision".to_string()))?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn insert_workspace(
    transaction: &Transaction<'_>,
    command: &WorkspaceRegisterCommand,
) -> Result<WorkspaceRecord, RuntimeError> {
    transaction
        .execute(
            "INSERT INTO workspaces (
                workspace_id, canonical_root, display_name, lifecycle_state,
                revision, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'active', 0, ?4, ?4)",
            params![
                command.proposed_workspace_id,
                command.canonical_root,
                command.display_name,
                command.requested_at,
            ],
        )
        .map_err(|error| db_error("failed inserting workspace", error))?;
    workspace_by_id(transaction, &command.proposed_workspace_id)?.ok_or_else(|| {
        RuntimeError::Bootstrap("inserted workspace missing before transaction commit".to_string())
    })
}

fn claims_for_operation(
    connection: &Connection,
    operation_id: &str,
) -> Result<Vec<OperationResourceClaimRecord>, RuntimeError> {
    let mut statement = connection
        .prepare(
            "SELECT resource_kind, resource_id, claim_mode, owner_operation_id,
                    fence_generation, acquired_at
             FROM runtime_operation_resource_claims
             WHERE owner_operation_id = ?1
             ORDER BY resource_kind, resource_id",
        )
        .map_err(|error| db_error("failed preparing operation claim query", error))?;
    let rows = statement
        .query_map(params![operation_id], |row| {
            let generation: i64 = row.get(4)?;
            Ok(OperationResourceClaimRecord {
                resource_kind: row.get(0)?,
                resource_id: row.get(1)?,
                claim_mode: row.get(2)?,
                owner_operation_id: row.get(3)?,
                fence_generation: u64::try_from(generation)
                    .map_err(|_| invalid_text(4, "negative claim fence generation".to_string()))?,
                acquired_at: row.get(5)?,
            })
        })
        .map_err(|error| db_error("failed querying operation claims", error))?;
    collect_rows(rows)
}

fn transitions_for_operation(
    connection: &Connection,
    operation_id: &str,
) -> Result<Vec<OperationTransitionRecord>, RuntimeError> {
    let mut statement = connection
        .prepare(
            "SELECT operation_id, sequence, from_phase, to_phase, evidence_json, created_at
             FROM runtime_operation_transitions
             WHERE operation_id = ?1 ORDER BY sequence ASC",
        )
        .map_err(|error| db_error("failed preparing operation transition query", error))?;
    let rows = statement
        .query_map(params![operation_id], |row| {
            let sequence: i64 = row.get(1)?;
            let from_text: Option<String> = row.get(2)?;
            let to_text: String = row.get(3)?;
            let evidence: Option<String> = row.get(4)?;
            Ok(OperationTransitionRecord {
                operation_id: row.get(0)?,
                sequence: u64::try_from(sequence)
                    .map_err(|_| invalid_text(1, "negative transition sequence".to_string()))?,
                from_phase: from_text
                    .map(|value| {
                        OperationPhase::from_str(&value).ok_or_else(|| {
                            invalid_text(2, format!("invalid transition phase {value:?}"))
                        })
                    })
                    .transpose()?,
                to_phase: OperationPhase::from_str(&to_text).ok_or_else(|| {
                    invalid_text(3, format!("invalid transition phase {to_text:?}"))
                })?,
                evidence: evidence.map(string_to_json).transpose()?,
                created_at: row.get(5)?,
            })
        })
        .map_err(|error| db_error("failed querying operation transitions", error))?;
    collect_rows(rows)
}

fn effects_for_operation(
    connection: &Connection,
    operation_id: &str,
) -> Result<Vec<OperationEffectRecord>, RuntimeError> {
    let mut statement = connection
        .prepare(
            "SELECT effect_id, operation_id, effect_kind, target_kind, target_id, phase,
                    idempotency_key, evidence_json, attempt_count, created_at, updated_at
             FROM runtime_operation_effects WHERE operation_id = ?1 ORDER BY created_at, effect_id",
        )
        .map_err(|error| db_error("failed preparing operation effect query", error))?;
    let rows = statement
        .query_map(params![operation_id], |row| {
            let evidence: Option<String> = row.get(7)?;
            let attempts: i64 = row.get(8)?;
            Ok(OperationEffectRecord {
                effect_id: row.get(0)?,
                operation_id: row.get(1)?,
                effect_kind: row.get(2)?,
                target_kind: row.get(3)?,
                target_id: row.get(4)?,
                phase: row.get(5)?,
                idempotency_key: row.get(6)?,
                evidence: evidence.map(string_to_json).transpose()?,
                attempt_count: u64::try_from(attempts)
                    .map_err(|_| invalid_text(8, "negative effect attempt count".to_string()))?,
                created_at: row.get(9)?,
                updated_at: row.get(10)?,
            })
        })
        .map_err(|error| db_error("failed querying operation effects", error))?;
    collect_rows(rows)
}

fn outbox_for_operation(
    connection: &Connection,
    operation_id: &str,
) -> Result<Vec<OperationOutboxRecord>, RuntimeError> {
    let mut statement = connection
        .prepare(
            "SELECT outbox_id, operation_id, delivery_kind, idempotency_key, payload_json,
                    state, attempt_count, next_attempt_at, last_error_json,
                    created_at, updated_at, delivered_at
             FROM runtime_operation_outbox WHERE operation_id = ?1 ORDER BY created_at, outbox_id",
        )
        .map_err(|error| db_error("failed preparing operation outbox query", error))?;
    let rows = statement
        .query_map(params![operation_id], |row| {
            let attempts: i64 = row.get(6)?;
            let last_error: Option<String> = row.get(8)?;
            Ok(OperationOutboxRecord {
                outbox_id: row.get(0)?,
                operation_id: row.get(1)?,
                delivery_kind: row.get(2)?,
                idempotency_key: row.get(3)?,
                payload: string_to_json(row.get(4)?)?,
                state: row.get(5)?,
                attempt_count: u64::try_from(attempts)
                    .map_err(|_| invalid_text(6, "negative outbox attempt count".to_string()))?,
                next_attempt_at: row.get(7)?,
                last_error: last_error.map(string_to_json).transpose()?,
                created_at: row.get(9)?,
                updated_at: row.get(10)?,
                delivered_at: row.get(11)?,
            })
        })
        .map_err(|error| db_error("failed querying operation outbox", error))?;
    collect_rows(rows)
}

fn receipts_for_operation(
    connection: &Connection,
    operation_id: &str,
) -> Result<Vec<OperationOutboxReceiptRecord>, RuntimeError> {
    let mut statement = connection
        .prepare(
            "SELECT receipt.outbox_id, receipt.idempotency_key, receipt.received_at, receipt.receipt_json
             FROM runtime_operation_outbox_receipts receipt
             JOIN runtime_operation_outbox outbox ON outbox.outbox_id = receipt.outbox_id
             WHERE outbox.operation_id = ?1 ORDER BY receipt.received_at, receipt.outbox_id",
        )
        .map_err(|error| db_error("failed preparing operation receipt query", error))?;
    let rows = statement
        .query_map(params![operation_id], |row| {
            let receipt: Option<String> = row.get(3)?;
            Ok(OperationOutboxReceiptRecord {
                outbox_id: row.get(0)?,
                idempotency_key: row.get(1)?,
                received_at: row.get(2)?,
                receipt: receipt.map(string_to_json).transpose()?,
            })
        })
        .map_err(|error| db_error("failed querying operation receipts", error))?;
    collect_rows(rows)
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
