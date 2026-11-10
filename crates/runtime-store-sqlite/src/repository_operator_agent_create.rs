use runtime_core::{
    OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord,
    OperationTransitionRecord, OperatorAgentCreateAdmission, OperatorAgentCreateCommand,
    OperatorAgentCreateLaunchSnapshot, RuntimeError,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::{operation_by_id, operation_by_idempotency};
use crate::SqliteRuntimeRepository;

pub(crate) const OPERATION_KIND: &str = "operator_agent_create";
pub(crate) const CLAIM_KIND: &str = "operator_agent_create_native";
pub(crate) const EFFECT_KIND: &str = "provider_session_create";

pub(crate) fn classify_create(
    record: OperationRecord,
    workspace_id: &str,
    hash: &str,
) -> Result<OperatorAgentCreateAdmission, RuntimeError> {
    if record.kind != OPERATION_KIND
        || record.workspace_id.as_deref() != Some(workspace_id)
        || record.normalized_request["workspace_id"] != workspace_id
        || record.normalized_request["action"] != "create"
        || record.normalized_request_hash != hash
        || runtime_core::normalized_json_hash(&record.normalized_request)? != hash
    {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key belongs to operation {} for different input",
            record.operation_id,
        )));
    }
    match (record.phase, record.exact_terminal_result) {
        (OperationPhase::Completed, Some(receipt)) => {
            let exact_result = serde_json::from_value(receipt).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid durable agent creation receipt: {error}"))
            })?;
            Ok(OperatorAgentCreateAdmission::Replay {
                operation_id: record.operation_id,
                exact_result,
            })
        }
        (OperationPhase::Failed, _)
            if record.error_code.as_deref() == Some("operator_verified_not_created") =>
        {
            Ok(OperatorAgentCreateAdmission::VerifiedNotCreated {
                operation_id: record.operation_id,
            })
        }
        _ => Ok(OperatorAgentCreateAdmission::Unresolved {
            operation_id: record.operation_id,
        }),
    }
}

pub(crate) fn verify_requested_create(
    tx: &Transaction<'_>,
    command: &OperatorAgentCreateCommand,
) -> Result<(), RuntimeError> {
    let record = operation_by_id(tx, &command.operation_id)?.ok_or_else(|| {
        RuntimeError::NotFound(format!("create operation {}", command.operation_id))
    })?;
    if record.kind != OPERATION_KIND
        || record.phase != OperationPhase::Requested
        || record.workspace_id.as_deref() != Some(&command.workspace_id)
        || record.actor != command.actor
        || record.idempotency_key.as_deref() != Some(command.idempotency_key.as_str())
        || record.normalized_request_hash != command.normalized_request_hash
        || record.normalized_request != command.normalized_request
        || runtime_core::normalized_json_hash(&record.normalized_request)?
            != command.normalized_request_hash
    {
        return Err(RuntimeError::Conflict(
            "immutable agent creation operation authority changed".into(),
        ));
    }
    let owns_claim: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_operation_resource_claims
         WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3
           AND claim_mode='exclusive')",
            params![CLAIM_KIND, command.reserved_agent_id, command.operation_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed verifying agent create native claim", error))?;
    if !owns_claim {
        return Err(RuntimeError::Conflict(
            "agent creation lost its reserved native-session claim".into(),
        ));
    }
    Ok(())
}

pub(crate) fn read_frozen_create_plan(
    tx: &Transaction<'_>,
    command: &OperatorAgentCreateCommand,
) -> Result<Option<OperatorAgentCreateLaunchSnapshot>, RuntimeError> {
    let raw: String = tx.query_row(
        "SELECT evidence_json FROM runtime_operation_effects
         WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3 AND phase IN ('intended','uncertain')",
        params![command.operation_id, EFFECT_KIND, command.reserved_agent_id],
        |row| row.get(0),
    ).optional().map_err(|error| db_error("failed reading original agent create plan", error))?
        .ok_or_else(|| RuntimeError::Conflict("original provider create effect is missing".into()))?;
    let evidence: Value = serde_json::from_str(&raw).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid original agent create evidence: {error}"))
    })?;
    if evidence["reserved_agent_id"] != command.reserved_agent_id
        || evidence["provider"] != command.normalized_request["request"]["provider"]
    {
        return Err(RuntimeError::Conflict(
            "frozen create evidence is for a different agent".into(),
        ));
    }
    if evidence["launch_snapshot"].is_null() {
        return Ok(None);
    }
    let snapshot: OperatorAgentCreateLaunchSnapshot =
        serde_json::from_value(evidence["launch_snapshot"].clone()).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "corrupt original provider launch snapshot: {error}"
            ))
        })?;
    if snapshot.session.id != command.reserved_agent_id
        || snapshot.agent.agent_id != command.reserved_agent_id
        || snapshot.agent.workspace_id != command.workspace_id
        || snapshot.session.provider != snapshot.agent.recreation_policy.provider.as_str()
        || snapshot.session.provider_session_ref.is_some()
        || snapshot.session.canonical_provider_session_ref.is_some()
        || snapshot.agent.provider_session_ref.is_some()
        || snapshot.agent.canonical_provider_session_ref.is_some()
        || snapshot.session.status != "ready"
    {
        return Err(RuntimeError::Conflict(
            "original provider create launch snapshot has changed".into(),
        ));
    }
    Ok(Some(snapshot))
}

impl SqliteRuntimeRepository {
    pub fn operator_agent_create_reattachment_pending(
        &self,
        agent_id: &str,
    ) -> Result<bool, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1 AND status='create_recovery_required')",
            [agent_id], |row| row.get(0),
        ).map_err(|error| db_error("failed checking reviewed native create reattachment", error))
    }

    pub fn replay_operator_agent_create(
        &self,
        actor: &OperationActor,
        key: &str,
        workspace_id: &str,
        hash: &str,
    ) -> Result<Option<OperatorAgentCreateAdmission>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        operation_by_idempotency(&connection, actor.kind, &actor.identifier, Some(key))?
            .map(|record| classify_create(record, workspace_id, hash))
            .transpose()
    }

    /// The operator's exact key and a unique public agent identity are bound
    /// in SQLite *before* any Git checkout/init script, approved pre-add hook,
    /// or provider API call. The unresolved operation blocks retirement too.
    pub fn admit_operator_agent_create(
        &self,
        command: &OperatorAgentCreateCommand,
    ) -> Result<OperatorAgentCreateAdmission, RuntimeError> {
        let provider = &command.normalized_request["request"]["provider"];
        if command.actor.kind != OperationActorKind::Operator
            || command.actor.identifier.trim().is_empty()
            || command.idempotency_key.is_empty()
            || command.idempotency_key.len() > 512
            || command.idempotency_key.trim() != command.idempotency_key
            || !command
                .idempotency_key
                .bytes()
                .all(|byte| (32..=126).contains(&byte) && byte != b',')
            || command.workspace_id.trim().is_empty()
            || command.operation_id.trim().is_empty()
            || !command.reserved_agent_id.starts_with("sess_")
            || command.reserved_agent_id.len() > 128
            || command.normalized_request["workspace_id"] != command.workspace_id
            || command.normalized_request["action"] != "create"
            || !matches!(provider.as_str(), Some("codex" | "claude" | "acp"))
            || command.normalized_request_hash
                != runtime_core::normalized_json_hash(&command.normalized_request)?
        {
            return Err(RuntimeError::InvalidState(
                "invalid immutable operator agent creation identity".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native agent create admission", error))?;
        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            Some(&command.idempotency_key),
        )? {
            return classify_create(
                existing,
                &command.workspace_id,
                &command.normalized_request_hash,
            );
        }
        let workspace: Option<String> = tx
            .query_row(
                "SELECT lifecycle_state FROM workspaces WHERE workspace_id=?1",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking create workspace state", error))?;
        if workspace.as_deref() != Some("active") {
            return Err(RuntimeError::Conflict(
                "agent creation requires an active workspace".into(),
            ));
        }
        let frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking agent creation retirement fence", error))?;
        if frozen {
            return Err(RuntimeError::Conflict(
                "agent creation is frozen by workspace retirement".into(),
            ));
        }
        let duplicate: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1)",
                [&command.reserved_agent_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error("failed checking reserved agent identity collision", error)
            })?;
        if duplicate {
            return Err(RuntimeError::Conflict(
                "reserved agent identity is already in use".into(),
            ));
        }

        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: OPERATION_KIND.into(),
            actor: command.actor.clone(),
            idempotency_key: Some(command.idempotency_key.clone()),
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
            evidence: Some(json!({"native_agent_create":"reserved_before_git_hooks_and_provider"})),
            created_at: command.requested_at,
        })?;
        authority.acquire_claim(
            &command.operation_id,
            CLAIM_KIND,
            &command.reserved_agent_id,
            command.requested_at,
        )?;
        let child_worktree_key = format!("agent-create-wt:{}", command.operation_id);
        authority.insert_effect(&OperationEffectRecord {
            effect_id: format!("fx_{}", command.operation_id),
            operation_id: command.operation_id.clone(),
            effect_kind: EFFECT_KIND.into(),
            target_kind: "provider_session".into(),
            target_id: command.reserved_agent_id.clone(),
            phase: "intended".into(),
            idempotency_key: "exact_reserved_provider_session_create".into(),
            evidence: Some(json!({
                "reserved_agent_id":command.reserved_agent_id,
                "provider":provider,
                "child_worktree_key":child_worktree_key,
                "stage":"reserved_no_provider_launch_snapshot",
            })),
            attempt_count: 0,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing provider creation intent", error))?;
        Ok(OperatorAgentCreateAdmission::Execute {
            operation_id: command.operation_id.clone(),
            reserved_agent_id: command.reserved_agent_id.clone(),
        })
    }

    pub fn freeze_operator_agent_create_launch(
        &self,
        command: &OperatorAgentCreateCommand,
        snapshot: &OperatorAgentCreateLaunchSnapshot,
    ) -> Result<(), RuntimeError> {
        if snapshot.session.id != command.reserved_agent_id
            || snapshot.agent.agent_id != command.reserved_agent_id
            || snapshot.agent.workspace_id != command.workspace_id
            || snapshot.agent.profile.added_by != command.actor.identifier
            || snapshot.session.provider != snapshot.agent.recreation_policy.provider.as_str()
            || snapshot.session.status != "ready"
            || snapshot.session.provider_session_ref.is_some()
            || snapshot.agent.provider_session_ref.is_some()
            || snapshot.session.cwd.as_deref()
                != Some(snapshot.agent.recreation_policy.authoritative_cwd.as_str())
            || snapshot.session.model != snapshot.agent.recreation_policy.model
            || snapshot.agent.revision != 0
            || snapshot.agent.lifecycle_state != runtime_core::WorkspaceAgentLifecycleState::Active
            || snapshot.worktree_operation_id != command.worktree_operation_id
        {
            return Err(RuntimeError::Conflict(
                "invalid exact native agent creation launch plan".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed freezing provider launch plan", error))?;
        verify_requested_create(&tx, command)?;
        let old: String = tx
            .query_row(
                "SELECT evidence_json FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3 AND phase='intended'",
                params![command.operation_id, EFFECT_KIND, command.reserved_agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading admitted provider create effect", error))?
            .ok_or_else(|| {
                RuntimeError::Conflict("native provider creation effect disappeared".into())
            })?;
        let value: Value = serde_json::from_str(&old)
            .map_err(|error| RuntimeError::Bootstrap(format!("invalid create intent: {error}")))?;
        if value["stage"] != "reserved_no_provider_launch_snapshot"
            || value["reserved_agent_id"] != command.reserved_agent_id
            || value["provider"] != command.normalized_request["request"]["provider"]
        {
            return Err(RuntimeError::Conflict(
                "agent create launch snapshot already frozen or changed".into(),
            ));
        }
        let evidence = json!({
            "reserved_agent_id":command.reserved_agent_id,
            "provider":snapshot.session.provider,
            "child_worktree_key":value["child_worktree_key"],
            "stage":"launch_frozen_before_provider",
            "launch_snapshot":snapshot,
        });
        let updated = tx
            .execute(
                "UPDATE runtime_operation_effects SET evidence_json=?2,updated_at=?3
             WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'
               AND evidence_json=?5",
                params![
                    command.operation_id,
                    json_to_string(&evidence)?,
                    snapshot.session.created_at,
                    EFFECT_KIND,
                    old
                ],
            )
            .map_err(|error| {
                db_error("failed recording durable provider launch snapshot", error)
            })?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "provider create launch snapshot raced".into(),
            ));
        }
        tx.commit()
            .map_err(|error| db_error("failed committing frozen provider launch", error))?;
        Ok(())
    }
}
