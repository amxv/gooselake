use super::RuntimeSessionManager;
use crate::RuntimeError;

impl RuntimeSessionManager {
    pub(super) fn fence_legacy_native_session(&self, agent_id: &str) -> Result<(), RuntimeError> {
        use crate::legacy_spawn_obligations::{LegacyNativeTarget, LegacySpawnScanBudget};
        self.store
            .classify_legacy_spawn_obligations(
                &[LegacyNativeTarget::Session(agent_id.to_owned())],
                LegacySpawnScanBudget::default(),
            )?
            .require_all_clear()
    }

    /// Unknown native user-turn dispatch holds an agent-level durable claim.
    /// Other provider-native mutations must honor it too: a stale in-memory
    /// `active_turn_id` cannot establish that the original turn is finished.
    pub(super) fn fence_unresolved_operator_turn_native_effect(
        &self,
        agent_id: &str,
    ) -> Result<(), RuntimeError> {
        self.fence_legacy_native_session(agent_id)?;
        if let Some(operation_id) = self
            .store
            .unresolved_workspace_interrupt_for_session(agent_id)?
        {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "an earlier workspace interrupt has unresolved native effects; inspect its original operation before changing this provider attachment",
            ));
        }
        if let Some(operation_id) = self
            .store
            .unresolved_operator_native_policy_for_agent(agent_id)?
        {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "an operator policy mutation has unresolved native effects; inspect its operation before changing the provider attachment",
            ));
        }
        if let Some(operation_id) = self
            .store
            .unresolved_operator_user_turn_for_agent(agent_id)?
        {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "an operator turn has unresolved native dispatch; reconcile its operation before changing this provider attachment",
            ));
        }
        if let Some(operation_id) = self
            .store
            .unresolved_operator_agent_restore_for_agent(agent_id)?
        {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "an operator agent restoration has unresolved native provider resume; independently review its operation before changing this attachment",
            ));
        }
        if let Some(operation_id) = self
            .store
            .unresolved_operator_agent_archive_for_agent(agent_id)?
        {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "an operator agent archival has unresolved native close; independently review its operation before changing this attachment",
            ));
        }
        if self
            .store
            .operator_native_policy_reattachment_pending(agent_id)?
        {
            return Err(RuntimeError::Conflict(
                "operator-reviewed provider policy requires runtime restart and verified native reattachment before further provider work".into(),
            ));
        }
        if self
            .store
            .operator_agent_restore_reattachment_pending(agent_id)?
        {
            return Err(RuntimeError::Conflict(
                "operator-reviewed native restore requires runtime restart and verified native reattachment before further provider work".into(),
            ));
        }
        if self
            .store
            .operator_agent_archive_reattachment_pending(agent_id)?
        {
            return Err(RuntimeError::Conflict(
                "operator-reviewed native archive requires runtime restart and verified native reattachment before further provider work".into(),
            ));
        }
        if self
            .store
            .operator_agent_create_reattachment_pending(agent_id)?
        {
            return Err(RuntimeError::Conflict(
                "operator-reviewed native creation requires exact provider reattachment after runtime restart before further native work".into(),
            ));
        }
        Ok(())
    }
}
