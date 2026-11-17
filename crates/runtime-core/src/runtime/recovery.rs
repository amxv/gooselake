use super::{
    helpers::now_ms, RuntimeSessionManager, StartupRecoveryProviderStatus, StartupRecoverySummary,
};
use crate::{RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope};
use std::sync::Arc;
impl RuntimeSessionManager {
    pub async fn recover_startup(self: &Arc<Self>) -> Result<StartupRecoverySummary, RuntimeError> {
        let _startup = self.startup_recovery_lock.lock().await;
        let started_at = now_ms();
        let mut summary = StartupRecoverySummary {
            started_at,
            ..Default::default()
        };
        for (family, operations) in [
            (
                "workspace rebind",
                self.store
                    .quarantine_started_workspace_agent_rebinds(started_at)?,
            ),
            (
                "operator native policy",
                self.store
                    .quarantine_started_operator_native_policies(started_at)?,
            ),
            (
                "operator agent restore",
                self.store
                    .quarantine_started_operator_agent_restores(started_at)?,
            ),
            (
                "operator agent archive",
                self.store
                    .quarantine_started_operator_agent_archives(started_at)?,
            ),
            (
                "operator agent create",
                self.store
                    .quarantine_started_operator_agent_creates(started_at)?,
            ),
        ] {
            for operation_id in operations {
                summary.notes.push(format!("{family} {operation_id} remains under manual review; no native effect was retried"));
            }
        }
        let hooks = self
            .store
            .quarantine_started_team_hook_effects(started_at)?;
        if hooks > 0 {
            summary.notes.push(format!(
                "{hooks} native hook effects remain unresolved; no hook was retried"
            ));
        }
        for receipt in self.store.recover_session_compactions(started_at)? {
            if let Some(event) = receipt.journal_event {
                let _ = self.event_tx.send(event);
            }
            summary.notes.push(format!(
                "manual compaction {} for {} remains unresolved",
                receipt.run.run_id, receipt.run.agent_id
            ));
        }
        for receipt in self.store.recover_session_edit_reruns(started_at)? {
            let operation = self.publish_edit_transition(receipt);
            summary.notes.push(format!(
                "edit-and-rerun {} for {} remains in {}",
                operation.operation_id, operation.agent_id, operation.phase
            ));
        }
        for metadata in self.providers.metadata() {
            let status = match self.providers.get(metadata.kind) {
                Some(provider) => match provider.healthcheck().await {
                    Ok(()) => StartupRecoveryProviderStatus {
                        provider: metadata.kind.as_str().into(),
                        healthy: true,
                        detail: None,
                    },
                    Err(error) => StartupRecoveryProviderStatus {
                        provider: metadata.kind.as_str().into(),
                        healthy: false,
                        detail: Some(error.to_string()),
                    },
                },
                None => StartupRecoveryProviderStatus {
                    provider: metadata.kind.as_str().into(),
                    healthy: false,
                    detail: Some("provider not registered".into()),
                },
            };
            summary.provider_status.push(status);
        }
        let mut session_ids = self
            .sessions
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        session_ids.sort();
        summary.sessions_scanned = session_ids.len();
        for session_id in session_ids {
            if let Err(error) = self
                .recover_startup_session(&session_id, &mut summary)
                .await
            {
                summary.notes.push(format!(
                    "session {session_id} startup continuation withheld: {error}"
                ));
            }
        }
        summary.completed_at = now_ms();
        Ok(summary)
    }
}

impl RuntimeSessionManager {
    pub async fn emit_startup_recovered_event(
        &self,
        summary: &StartupRecoverySummary,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.append_event(
            RuntimeEventScope::System,
            "startup_recovery",
            None,
            None,
            "runtime.startup_recovered",
            RuntimeEventCriticality::Critical,
            serde_json::json!({ "summary": summary }),
        )
        .await
    }
}
