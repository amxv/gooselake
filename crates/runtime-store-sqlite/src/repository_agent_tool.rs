use runtime_core::{
    AgentToolMutationAdmission, AgentToolMutationCommand, OperationActor, OperationActorKind,
    OperationPhase, OperationRecord, OperationTransitionRecord, RuntimeError,
};
use rusqlite::TransactionBehavior;
use serde_json::Value;

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::{operation_by_id, operation_by_idempotency};
use crate::SqliteRuntimeRepository;

const MODEL_TOOL_KIND: &str = "agent_model_tool_mutation";

impl SqliteRuntimeRepository {
    pub fn begin_agent_tool_mutation(
        &self,
        command: &AgentToolMutationCommand,
    ) -> Result<AgentToolMutationAdmission, RuntimeError> {
        if command.caller_agent_id.is_empty()
            || command.workspace_id.is_empty()
            || command.tool_name != "gg_team"
            || command.idempotency_key.trim() != command.idempotency_key
            || command.idempotency_key.is_empty()
            || command.idempotency_key.len() > 128
            || command.normalized_request_hash
                != runtime_core::normalized_json_hash(&command.normalized_request)?
        {
            return Err(RuntimeError::InvalidState(
                "invalid authenticated model-tool mutation admission".into(),
            ));
        }

        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting model-tool admission", error))?;

        if let Some(existing) = operation_by_idempotency(
            &transaction,
            OperationActorKind::Agent,
            &command.caller_agent_id,
            Some(&command.idempotency_key),
        )? {
            if existing.kind != MODEL_TOOL_KIND
                || existing.workspace_id.as_deref() != Some(command.workspace_id.as_str())
                || existing.normalized_request_hash != command.normalized_request_hash
            {
                return Err(RuntimeError::Conflict(
                    "model-tool invocation ID was already used for different arguments".into(),
                ));
            }
            return Ok(match existing.exact_terminal_result {
                Some(exact_result) if existing.phase == OperationPhase::Completed => {
                    AgentToolMutationAdmission::Replay { exact_result }
                }
                _ => AgentToolMutationAdmission::Unresolved {
                    operation_id: existing.operation_id,
                },
            });
        }

        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: MODEL_TOOL_KIND.into(),
            actor: OperationActor {
                kind: OperationActorKind::Agent,
                identifier: command.caller_agent_id.clone(),
            },
            idempotency_key: Some(command.idempotency_key.clone()),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.admitted_at,
            updated_at: command.admitted_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: Some(serde_json::json!({"tool_name":command.tool_name})),
            created_at: command.admitted_at,
        })?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing model-tool admission", error))?;
        Ok(AgentToolMutationAdmission::Execute {
            operation_id: command.operation_id.clone(),
        })
    }

    pub fn finish_agent_tool_mutation(
        &self,
        operation_id: &str,
        exact_result: &Value,
        finished_at: i64,
    ) -> Result<(), RuntimeError> {
        if exact_result.get("ok") != Some(&Value::Bool(true)) {
            return Err(RuntimeError::InvalidState(
                "only successfully verified model-tool outcomes can be receipted".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting model-tool terminalization", error))?;
        let operation = operation_by_id(&transaction, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound("model-tool operation".into()))?;
        if operation.kind != MODEL_TOOL_KIND {
            return Err(RuntimeError::Conflict("not a model-tool operation".into()));
        }
        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.terminalize(
            operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            operation.workspace_id.as_deref(),
            exact_result,
            None,
            None,
            finished_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({"receipt":"exact_tool_result"})),
            created_at: finished_at,
        })?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing model-tool result", error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_core::{
        normalized_json_hash, prepare_workspace_registration, WorkspaceRegisterRequest,
    };
    use serde_json::json;

    fn command(workspace_id: &str, input: Value) -> AgentToolMutationCommand {
        AgentToolMutationCommand {
            operation_id: "op_test_model_tool".into(),
            workspace_id: workspace_id.into(),
            caller_agent_id: "sess_caller".into(),
            idempotency_key: "model-tool-request-1".into(),
            tool_name: "gg_team".into(),
            normalized_request_hash: normalized_json_hash(&input).unwrap(),
            normalized_request: input,
            admitted_at: 100,
        }
    }

    #[test]
    fn mutation_receipts_survive_restart_and_unknown_outcomes_never_redispatch() {
        let temp = tempfile::tempdir().unwrap();
        let repo = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite"));
        repo.initialize_schema().unwrap();
        let root = temp.path().join("workspace");
        std::fs::create_dir_all(&root).unwrap();
        let workspace = repo
            .register_workspace(
                &prepare_workspace_registration(
                    WorkspaceRegisterRequest {
                        canonical_root: root.to_string_lossy().to_string(),
                        display_name: None,
                    },
                    OperationActor::operator("operator"),
                    None,
                )
                .unwrap(),
            )
            .unwrap()
            .workspace
            .workspace_id;

        let input = json!({"tool_name":"gg_team","args":{"mode":"add","title":"Worker"}});
        let first = command(&workspace, input.clone());
        assert_eq!(
            repo.begin_agent_tool_mutation(&first).unwrap(),
            AgentToolMutationAdmission::Execute {
                operation_id: first.operation_id.clone()
            }
        );
        let replaying = command(&workspace, input.clone());
        assert_eq!(
            repo.begin_agent_tool_mutation(&replaying).unwrap(),
            AgentToolMutationAdmission::Unresolved {
                operation_id: first.operation_id.clone()
            }
        );
        let changed = command(
            &workspace,
            json!({"tool_name":"gg_team","args":{"mode":"add","title":"Other"}}),
        );
        assert!(matches!(
            repo.begin_agent_tool_mutation(&changed),
            Err(RuntimeError::Conflict(_))
        ));
        assert!(repo
            .finish_agent_tool_mutation(
                &first.operation_id,
                &json!({"ok":false,"error":{"code":"failed"}}),
                101
            )
            .is_err());
        assert_eq!(
            repo.begin_agent_tool_mutation(&replaying).unwrap(),
            AgentToolMutationAdmission::Unresolved {
                operation_id: first.operation_id.clone()
            }
        );

        let result = json!({"ok":true,"result":{"agent_id":"sess_new","alias":"worker"}});
        repo.finish_agent_tool_mutation(&first.operation_id, &result, 102)
            .unwrap();
        drop(repo);
        let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite"));
        assert_eq!(
            reopened.begin_agent_tool_mutation(&replaying).unwrap(),
            AgentToolMutationAdmission::Replay {
                exact_result: result.clone()
            }
        );
        let details = reopened
            .get_operation(&first.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(details.operation.phase, OperationPhase::Completed);
        assert_eq!(details.operation.exact_terminal_result, Some(result));
        assert_eq!(details.transitions.len(), 2);
    }
}
