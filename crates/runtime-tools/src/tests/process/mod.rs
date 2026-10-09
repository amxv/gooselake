use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::*;
use runtime_core::{
    prepare_workspace_registration, ManagedProcessAdmission, OperationActor,
    ProcessCompletionUpdate, ProcessGetRequest, ProcessKillRequest, ProcessLogReadRequest,
    ProcessManager, ProcessSchedulerSettings, ProviderCreateSessionRequest, ProviderMetadata,
    ProviderModel, ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderSession,
    ProviderTurnAck, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest,
    RuntimeProvider, WorkspaceAgentCreateRequest, WorkspaceRegisterRequest,
    PROCESS_COMPLETION_DELIVERED, PROCESS_COMPLETION_INJECTING, PROCESS_COMPLETION_NOT_REQUIRED,
};

#[derive(Default)]
struct FlakyCompletionProvider {
    send_attempts: AtomicUsize,
    accepted_sends: AtomicUsize,
}

#[async_trait]
impl RuntimeProvider for FlakyCompletionProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "Flaky Completion Provider".to_string(),
            enabled: true,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(vec![ProviderModel {
            id: "test-model".to_string(),
            display_name: "Test Model".to_string(),
            reasoning_levels: Vec::new(),
        }])
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id.clone(),
            provider_session_ref: format!("flaky:{}", req.runtime_session_id),
            canonical_provider_session_ref: None,
        })
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: req.provider_session_ref,
            canonical_provider_session_ref: req.canonical_provider_session_ref,
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let attempt = self.send_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            return Err(RuntimeError::provider_not_dispatched(
                "temporary_provider_outage",
                "provider unavailable before dispatch",
            ));
        }
        self.accepted_sends.fetch_add(1, Ordering::SeqCst);
        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            provider_native_turn_id: None,
        })
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        Ok(ProviderTurnResult {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            status: ProviderTurnStatus::Completed,
            usage: None,
            error: None,
        })
    }
}

fn process_config(
    temp_dir: &tempfile::TempDir,
    max_concurrent: usize,
    capture_limit_bytes: usize,
) -> ProcessManagerConfig {
    ProcessManagerConfig {
        enabled: true,
        max_concurrent,
        default_timeout_ms: 60_000,
        max_output_bytes_per_process: capture_limit_bytes,
        allow_shell: true,
        completed_retention_ms: 600_000,
        output_event_sample_bytes: 1024,
        log_dir: temp_dir.path().join("process-logs"),
    }
}

fn scheduler_settings(
    max_concurrent: usize,
    capture_limit_bytes: usize,
    paused: bool,
) -> ProcessSchedulerSettings {
    ProcessSchedulerSettings {
        max_concurrent,
        workspace_max_concurrent: BTreeMap::new(),
        capture_limit_bytes,
        paused,
        pause_reason: paused.then(|| "test_pause".to_string()),
        updated_at: 1,
    }
}

fn register_workspace(
    store: &Arc<SqliteRuntimeStore>,
    root: &std::path::Path,
    display_name: &str,
) -> runtime_core::WorkspaceRecord {
    let command = prepare_workspace_registration(
        WorkspaceRegisterRequest {
            canonical_root: root.display().to_string(),
            display_name: Some(display_name.to_string()),
        },
        OperationActor::operator("managed-process-test"),
        None,
    )
    .expect("prepare workspace registration");
    store
        .register_workspace(&command)
        .expect("register workspace")
        .workspace
}

fn workspace_agent_request(root: &std::path::Path, title: &str) -> WorkspaceAgentCreateRequest {
    WorkspaceAgentCreateRequest {
        provider: ProviderKind::Codex,
        model: Some("test-model".to_string()),
        permission_intent: runtime_core::ProviderPermissionIntent::ProviderDefault,
        setting_sources_intent: runtime_core::ProviderSettingSourcesIntent::Isolated,
        current_preferences: runtime_core::ProviderSessionPreferences::default(),
        system_prompt: None,
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        cwd: Some(root.display().to_string()),
        worktree: None,
        harness_version_slot: None,
        title: Some(title.to_string()),
        metadata: None,
    }
}

async fn wait_for_terminal(
    manager: &Arc<RuntimeProcessManager>,
    process_id: &str,
) -> runtime_core::ProcessDetails {
    for _ in 0..200 {
        let details = manager
            .get_process(ProcessGetRequest {
                process_id: process_id.to_string(),
                caller_session_id: None,
            })
            .await
            .expect("get process");
        if runtime_core::process_status_is_terminal(&details.process.status) {
            return details;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("process {process_id} did not reach a terminal state");
}

async fn wait_for_completion_delivery(
    store: &Arc<SqliteRuntimeStore>,
    process_id: &str,
) -> runtime_core::ManagedProcessRecord {
    for _ in 0..300 {
        let record = store
            .get_managed_process(process_id)
            .expect("get managed process")
            .expect("managed process exists");
        if record.completion_state == PROCESS_COMPLETION_DELIVERED {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("process {process_id} completion was not delivered");
}

mod lifecycle;
mod recovery;
