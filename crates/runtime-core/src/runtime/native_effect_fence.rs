use super::RuntimeSessionManager;
use crate::RuntimeError;

impl RuntimeSessionManager {
    /// Unknown native user-turn dispatch holds an agent-level durable claim.
    /// Other provider-native mutations must honor it too: a stale in-memory
    /// `active_turn_id` cannot establish that the original turn is finished.
    pub(super) fn fence_unresolved_operator_turn_native_effect(
        &self,
        agent_id: &str,
    ) -> Result<(), RuntimeError> {
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
        Ok(())
    }
}
