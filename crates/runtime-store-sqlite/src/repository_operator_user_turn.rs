use runtime_core::{
    OperationActor, OperationPhase, OperationRecord, OperationResourceClaimRecord,
    OperationTransitionRecord, OperatorUserTurnReplay, RuntimeError, SendTurnAccepted,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_turn_authority::{
    OPERATOR_TURN_CLAIM, OPERATOR_TURN_EFFECT, OPERATOR_TURN_KIND,
};
use crate::repository_workspace::{operation_by_id, operation_by_idempotency};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn unresolved_operator_user_turn_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",
                params![OPERATOR_TURN_CLAIM, agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                db_error(
                    "failed reading unresolved native operator turn claim",
                    error,
                )
            })
    }

    pub fn operator_user_turn_replay(
        &self,
        actor: &OperationActor,
        key: &str,
        agent_id: &str,
        normalized_request_hash: &str,
    ) -> Result<Option<OperatorUserTurnReplay>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        operation_by_idempotency(&connection, actor.kind, &actor.identifier, Some(key))?
            .map(|operation| classify_operator_turn(operation, agent_id, normalized_request_hash))
            .transpose()
    }

    pub fn complete_operator_user_turn(
        &self,
        operation_id: &str,
        accepted: &SendTurnAccepted,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        if !matches!(
            accepted.status.as_str(),
            "in_progress" | "waiting_for_approval"
        ) {
            return Err(RuntimeError::InvalidState(
                "operator turn receipt must describe an accepted logical turn".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting operator turn receipt", error))?;
        let operation = requested_operator_turn(&transaction, operation_id)?;
        let turn_id = operation_turn_id(&transaction, operation_id)?;
        if accepted.turn_id != turn_id
            || operation.normalized_request["agent_id"] != accepted.session_id
        {
            return Err(RuntimeError::Conflict(
                "operator turn receipt does not match admitted logical turn".into(),
            ));
        }
        let persisted: Option<(String, String)> = transaction
            .query_row(
                "SELECT t.session_id, a.dispatch_state
                 FROM turns t JOIN turn_admissions a ON a.turn_id=t.id
                 WHERE t.id=?1",
                [&turn_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed verifying accepted logical turn", error))?;
        let Some((session_id, dispatch_state)) = persisted else {
            return Err(RuntimeError::Conflict(
                "operator turn receipt lacks durable turn admission".into(),
            ));
        };
        if session_id != accepted.session_id
            || (accepted.status == "in_progress" && dispatch_state != "dispatched")
            || (accepted.status == "waiting_for_approval" && dispatch_state == "not_dispatched")
        {
            return Err(RuntimeError::Conflict(
                "operator turn receipt has no matching accepted dispatch evidence".into(),
            ));
        }
        let exact = serde_json::to_value(accepted).map_err(|error| {
            RuntimeError::Bootstrap(format!("failed serializing accepted turn: {error}"))
        })?;
        let changed = transaction
            .execute(
                "UPDATE runtime_operation_effects SET phase='finalized',
                 evidence_json=?2, attempt_count=1, updated_at=?3
                 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![
                    operation_id,
                    json_to_string(&exact)?,
                    at_ms,
                    OPERATOR_TURN_EFFECT
                ],
            )
            .map_err(|error| db_error("failed recording accepted operator turn", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "operator native turn intent has already been settled".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.terminalize(
            operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            operation.workspace_id.as_deref(),
            &exact,
            None,
            None,
            at_ms,
        )?;
        append_terminal_transition(&authority, operation_id, OperationPhase::Completed, at_ms)?;
        release_operator_turn_claim(&transaction, operation_id, &session_id)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing exact operator turn receipt", error))
    }

    pub fn fail_operator_user_turn_not_dispatched(
        &self,
        operation_id: &str,
        code: &str,
        message: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting operator non-dispatch receipt", error))?;
        let operation = requested_operator_turn(&transaction, operation_id)?;
        let turn_id = operation_turn_id(&transaction, operation_id)?;
        let state: Option<(String, String)> = transaction
            .query_row(
                "SELECT t.session_id, a.dispatch_state FROM turns t
                 JOIN turn_admissions a ON a.turn_id=t.id WHERE t.id=?1",
                [&turn_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed checking rejected provider dispatch", error))?;
        let Some((session_id, state)) = state else {
            return Err(RuntimeError::Conflict(
                "operator non-dispatch lacks logical turn evidence".into(),
            ));
        };
        if state != "not_dispatched" || operation.normalized_request["agent_id"] != session_id {
            return Err(RuntimeError::Conflict(
                "provider native non-dispatch was not durably proven".into(),
            ));
        }
        let exact = serde_json::json!({
            "code":code,
            "message":message,
            "dispatch_outcome":"not_dispatched",
            "logical_turn_id":turn_id,
        });
        let changed = transaction
            .execute(
                "UPDATE runtime_operation_effects SET phase='finalized',
                 evidence_json=?2, attempt_count=1, updated_at=?3
                 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![
                    operation_id,
                    json_to_string(&exact)?,
                    at_ms,
                    OPERATOR_TURN_EFFECT
                ],
            )
            .map_err(|error| db_error("failed recording provider non-dispatch", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "operator turn non-dispatch receipt was already settled".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.terminalize(
            operation_id,
            OperationPhase::Requested,
            OperationPhase::Failed,
            operation.workspace_id.as_deref(),
            &exact,
            Some("not_dispatched"),
            Some(message),
            at_ms,
        )?;
        append_terminal_transition(&authority, operation_id, OperationPhase::Failed, at_ms)?;
        release_operator_turn_claim(&transaction, operation_id, &session_id)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing exact non-dispatch receipt", error))
    }

    pub fn mark_operator_user_turn_uncertain(
        &self,
        operation_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning uncertain operator turn review", error))?;
        requested_operator_turn(&transaction, operation_id)?;
        let diagnostic = reason.chars().take(400).collect::<String>();
        let changed = transaction
            .execute(
                "UPDATE runtime_operation_effects SET phase='uncertain',
                 attempt_count=1, evidence_json=json_set(evidence_json,'$.diagnostic',?2),
                 updated_at=?3 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![operation_id, diagnostic, at_ms, OPERATOR_TURN_EFFECT],
            )
            .map_err(|error| db_error("failed recording uncertain provider turn", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "operator turn effect is no longer an unconfirmed native intent".into(),
            ));
        }
        let changed = transaction
            .execute(
                "UPDATE runtime_operations SET phase='manual_review',
                 error_code='outcome_unresolved',error_message=?2,updated_at=?3
                 WHERE operation_id=?1 AND phase='requested'",
                params![operation_id, diagnostic, at_ms],
            )
            .map_err(|error| db_error("failed quarantining uncertain operator turn", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "operator turn phase changed before quarantine".into(),
            ));
        }
        OperationAuthorityTransaction::new(&transaction).append_transition(
            &OperationTransitionRecord {
                operation_id: operation_id.into(),
                sequence: 2,
                from_phase: Some(OperationPhase::Requested),
                to_phase: OperationPhase::ManualReview,
                evidence: Some(serde_json::json!({"native_outcome":"unresolved"})),
                created_at: at_ms,
            },
        )?;
        // Preserve the agent claim until the provider/turn identity is
        // independently reconciled; a new key must not dispatch around it.
        transaction
            .commit()
            .map_err(|error| db_error("failed committing uncertain operator turn", error))
    }
}

fn classify_operator_turn(
    operation: OperationRecord,
    agent_id: &str,
    hash: &str,
) -> Result<OperatorUserTurnReplay, RuntimeError> {
    if operation.kind != OPERATOR_TURN_KIND
        || operation.normalized_request_hash != hash
        || operation.normalized_request["agent_id"] != agent_id
    {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key belongs to operation {} with different input",
            operation.operation_id
        )));
    }
    if operation.phase == OperationPhase::Completed {
        let accepted: SendTurnAccepted =
            serde_json::from_value(operation.exact_terminal_result.ok_or_else(|| {
                RuntimeError::Bootstrap("completed operator turn has no exact receipt".into())
            })?)
            .map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid exact operator turn receipt: {error}"))
            })?;
        if accepted.session_id != agent_id {
            return Err(RuntimeError::Bootstrap(
                "operator turn receipt belongs to another agent".into(),
            ));
        }
        return Ok(OperatorUserTurnReplay::Completed {
            operation_id: operation.operation_id,
            accepted,
        });
    }
    if operation.phase == OperationPhase::Failed
        && operation.error_code.as_deref() == Some("not_dispatched")
    {
        let exact = operation.exact_terminal_result.ok_or_else(|| {
            RuntimeError::Bootstrap("failed operator turn has no exact receipt".into())
        })?;
        let code = exact["code"]
            .as_str()
            .ok_or_else(|| RuntimeError::Bootstrap("invalid provider error code".into()))?;
        let message = exact["message"]
            .as_str()
            .ok_or_else(|| RuntimeError::Bootstrap("invalid provider error receipt".into()))?;
        return Ok(OperatorUserTurnReplay::NotDispatched {
            operation_id: operation.operation_id,
            code: code.into(),
            message: message.into(),
        });
    }
    Ok(OperatorUserTurnReplay::Unresolved {
        operation_id: operation.operation_id,
    })
}

fn requested_operator_turn(
    transaction: &Transaction<'_>,
    operation_id: &str,
) -> Result<OperationRecord, RuntimeError> {
    let operation = operation_by_id(transaction, operation_id)?
        .ok_or_else(|| RuntimeError::NotFound(format!("operation {operation_id}")))?;
    if operation.kind != OPERATOR_TURN_KIND || operation.phase != OperationPhase::Requested {
        return Err(RuntimeError::Conflict(format!(
            "operation {operation_id} is not a requested operator logical turn"
        )));
    }
    Ok(operation)
}

fn operation_turn_id(
    transaction: &Transaction<'_>,
    operation_id: &str,
) -> Result<String, RuntimeError> {
    transaction
        .query_row(
            "SELECT target_id FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?2 AND phase='intended'",
            params![operation_id, OPERATOR_TURN_EFFECT],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed loading admitted operator turn effect", error))?
        .ok_or_else(|| RuntimeError::Conflict("operator turn native intent is absent".into()))
}

fn append_terminal_transition(
    authority: &OperationAuthorityTransaction<'_, '_>,
    operation_id: &str,
    phase: OperationPhase,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    authority.append_transition(&OperationTransitionRecord {
        operation_id: operation_id.into(),
        sequence: 2,
        from_phase: Some(OperationPhase::Requested),
        to_phase: phase,
        evidence: Some(serde_json::json!({"logical_turn_receipt":"exact"})),
        created_at: at_ms,
    })
}

fn release_operator_turn_claim(
    transaction: &Transaction<'_>,
    operation_id: &str,
    agent_id: &str,
) -> Result<(), RuntimeError> {
    let claim: Option<(i64, i64)> = transaction
        .query_row(
            "SELECT fence_generation, acquired_at FROM runtime_operation_resource_claims
             WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'
               AND owner_operation_id=?3",
            params![OPERATOR_TURN_CLAIM, agent_id, operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| db_error("failed finding exact native agent turn claim", error))?;
    let (generation, acquired_at) = claim.ok_or_else(|| {
        RuntimeError::Conflict("operator turn no longer owns the agent dispatch claim".into())
    })?;
    OperationAuthorityTransaction::new(transaction).release_claim(&OperationResourceClaimRecord {
        resource_kind: OPERATOR_TURN_CLAIM.into(),
        resource_id: agent_id.into(),
        claim_mode: "exclusive".into(),
        owner_operation_id: operation_id.into(),
        fence_generation: u64::try_from(generation).map_err(|_| {
            RuntimeError::Bootstrap("invalid operator turn claim generation".into())
        })?,
        acquired_at,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runtime_core::{
        normalized_json_hash, prepare_workspace_registration, OperationActor, ProviderKind,
        SessionRecord, TurnAdmissionRecord, TurnCorrelationState, TurnDispatchPolicySnapshot,
        TurnDispatchState, TurnInputProjectionSource, TurnRecord, WorkspaceAgentLifecycleState,
        WorkspaceAgentProfile, WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy,
        WorkspaceRegisterRequest,
    };

    use super::*;

    fn fixture() -> (
        tempfile::TempDir,
        SqliteRuntimeRepository,
        runtime_core::OperatorUserTurnCommand,
        TurnAdmissionRecord,
        TurnRecord,
        SessionRecord,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
        repository.initialize_schema().unwrap();
        let root = temp.path().join("workspace");
        std::fs::create_dir_all(&root).unwrap();
        let workspace = repository
            .register_workspace(
                &prepare_workspace_registration(
                    WorkspaceRegisterRequest {
                        canonical_root: root.display().to_string(),
                        display_name: Some("Operator user turns".into()),
                    },
                    OperationActor::operator("operator"),
                    None,
                )
                .unwrap(),
            )
            .unwrap()
            .workspace;
        let session = SessionRecord {
            id: "sess_operator_turn".into(),
            provider: "codex".into(),
            status: "ready".into(),
            cwd: Some(workspace.canonical_root.clone()),
            model: Some("test-model".into()),
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("native-session".into()),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: 10,
            updated_at: 10,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        };
        let agent = WorkspaceAgentRecord {
            agent_id: session.id.clone(),
            workspace_id: workspace.workspace_id.clone(),
            alias: "quiet-otter-user-turn".into(),
            lifecycle_state: WorkspaceAgentLifecycleState::Active,
            profile: WorkspaceAgentProfile {
                title: None,
                title_provenance: "test".into(),
                added_by: "operator".into(),
                creator_session_id: None,
                creator_compaction_subscription: "auto".into(),
                joined_at: 10,
            },
            recreation_policy: WorkspaceAgentRecreationPolicy {
                provider: ProviderKind::Codex,
                model: Some("test-model".into()),
                permission_intent: Default::default(),
                setting_sources_intent: Default::default(),
                current_preferences: Default::default(),
                system_prompt: None,
                allowed_tools: Vec::new(),
                disallowed_tools: Vec::new(),
                authoritative_cwd: workspace.canonical_root.clone(),
                harness_version_slot: None,
            },
            provider_session_ref: session.provider_session_ref.clone(),
            canonical_provider_session_ref: None,
            metadata: session.metadata.clone(),
            archived_at: None,
            archive_reason: None,
            revision: 0,
            created_at: 10,
            updated_at: 10,
        };
        repository.create_workspace_agent(&session, &agent).unwrap();
        let input = vec![serde_json::json!({"type":"text","text":"one model invocation"})];
        let turn = TurnRecord {
            id: "turn_operator_one".into(),
            session_id: session.id.clone(),
            provider_turn_ref: None,
            status: "admitted".into(),
            input: serde_json::json!(input),
            source: Some("user".into()),
            started_at: Some(20),
            completed_at: None,
            usage: None,
            error: None,
        };
        let admission = TurnAdmissionRecord {
            turn_id: turn.id.clone(),
            session_id: session.id.clone(),
            provider: "codex".into(),
            projection_source: TurnInputProjectionSource::UserVisible,
            user_input_snapshot: runtime_core::PersistedUserInputSnapshot::from_input(&input),
            dispatch_policy: TurnDispatchPolicySnapshot {
                permission_mode: None,
                pre_dispatch_approval_required: false,
                turn_settings: Default::default(),
            },
            correlation: TurnCorrelationState {
                expected_turn_id: None,
                correlation_id: None,
            },
            dispatch_state: TurnDispatchState::Pending,
            provider_native_turn_id: None,
            dispatch_error: None,
            admitted_at: 20,
            updated_at: 20,
        };
        let normalized = serde_json::json!({
            "agent_id":session.id,
            "input":turn.input,
            "expected_turn_id":null,
            "permission_mode":null,
            "turn_settings":admission.dispatch_policy.turn_settings,
        });
        let command = runtime_core::OperatorUserTurnCommand {
            operation_id: "op_turn_test_one".into(),
            agent_id: session.id.clone(),
            workspace_id: workspace.workspace_id,
            actor: OperationActor::operator("operator"),
            idempotency_key: "one-logical-model-turn".into(),
            normalized_request_hash: normalized_json_hash(&normalized).unwrap(),
            normalized_request: normalized,
            requested_at: 20,
        };
        let mut updated_session = session;
        updated_session.active_turn_id = Some(turn.id.clone());
        updated_session.status = "turn_admitted".into();
        updated_session.updated_at = 20;
        (temp, repository, command, admission, turn, updated_session)
    }

    #[test]
    fn operator_turn_admission_and_exact_terminal_receipt_survive_restart_and_write_failures() {
        let (temp, repo, command, mut admission, turn, session) = fixture();
        let db = temp.path().join("runtime.sqlite3");
        open_connection(&db)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse_operator_effect BEFORE INSERT ON runtime_operation_effects
             BEGIN SELECT RAISE(ABORT,'injected operation admission failure'); END;",
            )
            .unwrap();
        assert!(repo
            .admit_operator_user_turn(&command, &admission, &turn, &session, None)
            .is_err());
        assert!(repo.get_operation(&command.operation_id).unwrap().is_none());
        assert!(repo.list_turn_admissions().unwrap().is_empty());
        assert!(repo.hydrate_runtime_state().unwrap().turns.is_empty());
        open_connection(&db)
            .unwrap()
            .execute_batch("DROP TRIGGER refuse_operator_effect;")
            .unwrap();

        repo.admit_operator_user_turn(&command, &admission, &turn, &session, None)
            .unwrap();
        let reopened = SqliteRuntimeRepository::new(db.clone());
        let replay = reopened
            .operator_user_turn_replay(
                &command.actor,
                &command.idempotency_key,
                &command.agent_id,
                &command.normalized_request_hash,
            )
            .unwrap();
        assert!(
            matches!(replay, Some(OperatorUserTurnReplay::Unresolved{operation_id})
            if operation_id==command.operation_id)
        );
        assert!(reopened
            .operator_user_turn_replay(
                &OperationActor::operator("different-principal"),
                &command.idempotency_key,
                &command.agent_id,
                &command.normalized_request_hash,
            )
            .unwrap()
            .is_none());

        admission.dispatch_state = TurnDispatchState::Dispatched;
        admission.updated_at = 21;
        reopened.upsert_turn_admission(&admission).unwrap();
        let accepted = SendTurnAccepted {
            session_id: session.id.clone(),
            turn_id: turn.id.clone(),
            status: "in_progress".into(),
        };
        open_connection(&db)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse_operator_receipt BEFORE UPDATE ON runtime_operations
             BEGIN SELECT RAISE(ABORT,'injected receipt write failure'); END;",
            )
            .unwrap();
        assert!(reopened
            .complete_operator_user_turn(&command.operation_id, &accepted, 22)
            .is_err());
        let unsettled = reopened
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(unsettled.operation.phase, OperationPhase::Requested);
        assert_eq!(unsettled.claims.len(), 1);
        open_connection(&db)
            .unwrap()
            .execute_batch("DROP TRIGGER refuse_operator_receipt;")
            .unwrap();
        reopened
            .complete_operator_user_turn(&command.operation_id, &accepted, 23)
            .unwrap();
        let replay = SqliteRuntimeRepository::new(db)
            .operator_user_turn_replay(
                &command.actor,
                &command.idempotency_key,
                &command.agent_id,
                &command.normalized_request_hash,
            )
            .unwrap();
        match replay {
            Some(OperatorUserTurnReplay::Completed {
                operation_id,
                accepted: original,
            }) => {
                assert_eq!(operation_id, command.operation_id);
                assert_eq!(
                    serde_json::to_value(original).unwrap(),
                    serde_json::to_value(&accepted).unwrap()
                );
            }
            _ => panic!("expected exact original accepted response"),
        }
        let details = reopened
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(details.operation.phase, OperationPhase::Completed);
        assert!(details.claims.is_empty());
        assert_eq!(details.effects[0].phase, "finalized");
        assert!(matches!(
            reopened.operator_user_turn_replay(
                &command.actor,
                &command.idempotency_key,
                "different-agent",
                &command.normalized_request_hash,
            ),
            Err(RuntimeError::Conflict(_))
        ));
    }

    #[test]
    fn uncertain_operator_turn_claim_blocks_a_new_key_even_after_active_session_is_cleared() {
        let (_temp, repo, command, admission, turn, mut session) = fixture();
        repo.admit_operator_user_turn(&command, &admission, &turn, &session, None)
            .unwrap();
        repo.mark_operator_user_turn_uncertain(
            &command.operation_id,
            "unknown native dispatch",
            22,
        )
        .unwrap();
        let details = repo.get_operation(&command.operation_id).unwrap().unwrap();
        assert_eq!(details.operation.phase, OperationPhase::ManualReview);
        assert_eq!(details.claims.len(), 1);
        assert_eq!(details.effects[0].phase, "uncertain");
        session.active_turn_id = None;
        session.status = "ready".into();
        repo.upsert_session(&session).unwrap();
        let mut changed_turn = turn;
        changed_turn.id = "turn_different_key".into();
        let mut changed_admission = admission;
        changed_admission.turn_id = changed_turn.id.clone();
        session.active_turn_id = Some(changed_turn.id.clone());
        assert!(matches!(
            repo.admit_turn(&changed_admission, &changed_turn, &session, None),
            Err(RuntimeError::OperationUnresolved { .. })
        ));
        let mut second_command = command.clone();
        second_command.operation_id = "op_second_key".into();
        second_command.idempotency_key = "a-new-key-cannot-bypass".into();
        assert!(matches!(
            repo.admit_operator_user_turn(
                &second_command,
                &changed_admission,
                &changed_turn,
                &session,
                None,
            ),
            Err(RuntimeError::OperationUnresolved { .. })
        ));
    }

    #[test]
    fn proven_not_dispatched_receipt_frees_agent_and_never_resends_the_original_key() {
        let (_temp, repo, command, mut admission, turn, mut session) = fixture();
        repo.admit_operator_user_turn(&command, &admission, &turn, &session, None)
            .unwrap();
        admission.dispatch_state = TurnDispatchState::NotDispatched;
        repo.upsert_turn_admission(&admission).unwrap();
        session.active_turn_id = None;
        session.status = "ready".into();
        repo.upsert_session(&session).unwrap();
        repo.fail_operator_user_turn_not_dispatched(
            &command.operation_id,
            "capacity_rejected",
            "native turn was not sent",
            22,
        )
        .unwrap();
        let replay = repo
            .operator_user_turn_replay(
                &command.actor,
                &command.idempotency_key,
                &command.agent_id,
                &command.normalized_request_hash,
            )
            .unwrap();
        assert!(matches!(replay,Some(OperatorUserTurnReplay::NotDispatched{
            operation_id,code,message,
        }) if operation_id==command.operation_id && code=="capacity_rejected" && message=="native turn was not sent"));
        assert!(repo
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap()
            .claims
            .is_empty());
        let mut next_command = command;
        next_command.operation_id = "op_retry_new_key".into();
        next_command.idempotency_key = "a-new-real-admission".into();
        let mut next_turn = turn;
        next_turn.id = "turn_new_key".into();
        admission.turn_id = next_turn.id.clone();
        admission.dispatch_state = TurnDispatchState::Pending;
        session.active_turn_id = Some(next_turn.id.clone());
        session.status = "turn_admitted".into();
        repo.admit_operator_user_turn(&next_command, &admission, &next_turn, &session, None)
            .unwrap();
    }

    #[test]
    fn concurrent_same_key_only_one_transaction_can_admit_a_model_input() {
        let (_temp, repository, command, admission, turn, session) = fixture();
        let repository = Arc::new(repository);
        let workers = (0..6)
            .map(|n| {
                let repository = repository.clone();
                let mut command = command.clone();
                let mut admission = admission.clone();
                let mut turn = turn.clone();
                let mut session = session.clone();
                command.operation_id = format!("op_race_{n}");
                turn.id = format!("turn_race_{n}");
                admission.turn_id = turn.id.clone();
                session.active_turn_id = Some(turn.id.clone());
                std::thread::spawn(move || {
                    repository.admit_operator_user_turn(&command, &admission, &turn, &session, None)
                })
            })
            .collect::<Vec<_>>();
        let mut admitted = 0;
        for worker in workers {
            match worker.join().unwrap() {
                Ok(()) => admitted += 1,
                Err(RuntimeError::OperationUnresolved { .. }) => {}
                other => panic!("unexpected concurrent admission outcome: {other:?}"),
            }
        }
        assert_eq!(admitted, 1);
        assert_eq!(repository.list_turn_admissions().unwrap().len(), 1);
    }
}
