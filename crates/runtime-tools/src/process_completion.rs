use std::sync::Arc;

use runtime_core::{
    process_status_is_terminal, ManagedProcessRecord, ProcessCompletionUpdate, ProcessDetails,
    RuntimeError, RuntimeSessionManager, SendTurnInput, TurnInputProjectionSource,
    WorkspaceAgentLifecycleState, PROCESS_COMPLETION_DELIVERED, PROCESS_COMPLETION_INJECTING,
    PROCESS_COMPLETION_PENDING,
};

use crate::now_ms;
use crate::process::RuntimeProcessManager;
use crate::process_helpers::{build_completion_input, completion_correlation, summary_from_record};

impl RuntimeProcessManager {
    pub(crate) async fn try_deliver_completion(
        &self,
        runtime: &Arc<RuntimeSessionManager>,
        record: &ManagedProcessRecord,
    ) -> Result<(), RuntimeError> {
        if !process_status_is_terminal(&record.status) {
            return Ok(());
        }
        let Some(owner_session_id) = record.owner_session_id.as_deref() else {
            return Ok(());
        };
        let correlation = completion_correlation(&record.process_id);
        if let Some(admission) = runtime
            .latest_turn_admission_for_correlation(owner_session_id, &correlation)
            .await
        {
            match admission.dispatch_state {
                runtime_core::TurnDispatchState::Dispatched => {
                    self.store.update_managed_process_completion(
                        &record.process_id,
                        &ProcessCompletionUpdate {
                            state: PROCESS_COMPLETION_DELIVERED.to_string(),
                            turn_id: Some(admission.turn_id),
                            attempt_count: record.completion_attempt_count,
                            last_error: None,
                            updated_at: now_ms(),
                        },
                    )?;
                    return Ok(());
                }
                runtime_core::TurnDispatchState::Pending
                | runtime_core::TurnDispatchState::Dispatching
                | runtime_core::TurnDispatchState::Unknown => {
                    return Ok(());
                }
                runtime_core::TurnDispatchState::NotDispatched => {
                    // Proven not dispatched is the one state where retrying cannot
                    // duplicate model-visible work. A later admitted turn uses the
                    // same correlation and becomes the new latest authority row.
                }
            }
        }
        let session = match runtime.get_session(owner_session_id).await {
            Ok(session) => session,
            Err(error) => {
                self.mark_completion_pending(record, Some(error.to_string()))?;
                return Ok(());
            }
        };
        if session.active_turn_id.is_some() {
            return Ok(());
        }

        let attempt_count = record.completion_attempt_count.saturating_add(1);
        self.store.update_managed_process_completion(
            &record.process_id,
            &ProcessCompletionUpdate {
                state: PROCESS_COMPLETION_INJECTING.to_string(),
                turn_id: None,
                attempt_count,
                last_error: None,
                updated_at: now_ms(),
            },
        )?;
        let result = runtime
            .send_turn(
                owner_session_id,
                SendTurnInput {
                    input: build_completion_input(record),
                    expected_turn_id: None,
                    permission_mode: None,
                    projection_source: Some(TurnInputProjectionSource::AutomationContext),
                    user_input_snapshot: None,
                    correlation_id: Some(correlation.clone()),
                },
            )
            .await;

        match result {
            Ok(ack) => {
                self.store.update_managed_process_completion(
                    &record.process_id,
                    &ProcessCompletionUpdate {
                        state: PROCESS_COMPLETION_DELIVERED.to_string(),
                        turn_id: Some(ack.turn_id),
                        attempt_count,
                        last_error: None,
                        updated_at: now_ms(),
                    },
                )?;
            }
            Err(error) => {
                if let Some(admission) = runtime
                    .latest_turn_admission_for_correlation(owner_session_id, &correlation)
                    .await
                {
                    if admission.dispatch_state == runtime_core::TurnDispatchState::Dispatched {
                        self.store.update_managed_process_completion(
                            &record.process_id,
                            &ProcessCompletionUpdate {
                                state: PROCESS_COMPLETION_DELIVERED.to_string(),
                                turn_id: Some(admission.turn_id),
                                attempt_count,
                                last_error: None,
                                updated_at: now_ms(),
                            },
                        )?;
                    } else if admission.dispatch_state
                        == runtime_core::TurnDispatchState::NotDispatched
                    {
                        self.store.update_managed_process_completion(
                            &record.process_id,
                            &ProcessCompletionUpdate {
                                state: PROCESS_COMPLETION_PENDING.to_string(),
                                turn_id: None,
                                attempt_count,
                                last_error: Some(error.to_string()),
                                updated_at: now_ms(),
                            },
                        )?;
                    }
                } else {
                    self.store.update_managed_process_completion(
                        &record.process_id,
                        &ProcessCompletionUpdate {
                            state: PROCESS_COMPLETION_PENDING.to_string(),
                            turn_id: None,
                            attempt_count,
                            last_error: Some(error.to_string()),
                            updated_at: now_ms(),
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    fn mark_completion_pending(
        &self,
        record: &ManagedProcessRecord,
        last_error: Option<String>,
    ) -> Result<(), RuntimeError> {
        self.store.update_managed_process_completion(
            &record.process_id,
            &ProcessCompletionUpdate {
                state: PROCESS_COMPLETION_PENDING.to_string(),
                turn_id: None,
                attempt_count: record.completion_attempt_count,
                last_error,
                updated_at: now_ms(),
            },
        )?;
        Ok(())
    }

    pub(crate) fn caller_workspace_id(
        &self,
        caller_session_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let Some(agent) = self.store.get_workspace_agent_by_id(caller_session_id)? else {
            return Ok(None);
        };
        if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active {
            return Err(RuntimeError::InvalidState(format!(
                "workspace agent {caller_session_id} is archived"
            )));
        }
        Ok(Some(agent.workspace_id))
    }

    pub(crate) fn ensure_visible(
        &self,
        record: &ManagedProcessRecord,
        caller_session_id: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let Some(caller) = caller_session_id else {
            return Ok(());
        };
        if record.owner_session_id.as_deref() == Some(caller) {
            return Ok(());
        }
        let caller_workspace = self.caller_workspace_id(caller)?;
        if caller_workspace.as_deref().is_some()
            && caller_workspace.as_deref() == record.workspace_id.as_deref()
        {
            return Ok(());
        }
        Err(RuntimeError::InvalidState(format!(
            "process {} belongs to a different session or workspace",
            record.process_id
        )))
    }

    pub(crate) fn ensure_cancel_owner(
        &self,
        record: &ManagedProcessRecord,
        caller_session_id: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let Some(caller) = caller_session_id else {
            return Ok(());
        };
        if record.owner_session_id.as_deref() == Some(caller) {
            return Ok(());
        }
        Err(RuntimeError::InvalidState(format!(
            "process {} belongs to a different session",
            record.process_id
        )))
    }

    pub(crate) async fn details_from_record(&self, record: ManagedProcessRecord) -> ProcessDetails {
        let live = self
            .live_processes
            .read()
            .await
            .get(&record.process_id)
            .cloned();
        let (stdout_bytes, stderr_bytes, stdout_truncated, stderr_truncated) =
            if let Some(live) = live {
                (
                    *live.stdout_bytes.lock().await,
                    *live.stderr_bytes.lock().await,
                    *live.stdout_truncated.lock().await,
                    *live.stderr_truncated.lock().await,
                )
            } else {
                (
                    usize::try_from(record.stdout_captured_bytes).unwrap_or(0),
                    usize::try_from(record.stderr_captured_bytes).unwrap_or(0),
                    record.stdout_truncated,
                    record.stderr_truncated,
                )
            };
        ProcessDetails {
            process: summary_from_record(&record),
            exit_code: record.exit_code,
            signal: record.signal,
            timeout_ms: record.timeout_ms,
            stdout_path: Some(record.stdout_path),
            stderr_path: Some(record.stderr_path),
            stdout_bytes,
            stderr_bytes,
            stdout_truncated,
            stderr_truncated,
        }
    }

    pub(crate) async fn process_id_from_pid(&self, pid: i64) -> Result<String, RuntimeError> {
        self.store
            .list_managed_processes()?
            .into_iter()
            .find(|record| record.pid == Some(pid) && !process_status_is_terminal(&record.status))
            .map(|record| record.process_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("process pid {pid}")))
    }
}
