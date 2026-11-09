macro_rules! delegate_operator_agent_restore_methods {
    () => {
        fn replay_operator_agent_restore(
            &self,
            actor: &runtime_core::OperationActor,
            key: &str,
            workspace_id: &str,
            agent_id: &str,
            hash: &str,
        ) -> Result<Option<runtime_core::OperatorAgentRestoreAdmission>, RuntimeError> {
            self.repository().replay_operator_agent_restore(
                actor,
                key,
                workspace_id,
                agent_id,
                hash,
            )
        }

        fn admit_operator_agent_restore(
            &self,
            command: &runtime_core::OperatorAgentRestoreCommand,
        ) -> Result<runtime_core::OperatorAgentRestoreAdmission, RuntimeError> {
            self.repository().admit_operator_agent_restore(command)
        }

        fn complete_operator_agent_restore(
            &self,
            command: &runtime_core::OperatorAgentRestoreCommand,
            session: &SessionRecord,
            at_ms: i64,
        ) -> Result<runtime_core::WorkspaceAgentRecord, RuntimeError> {
            self.repository()
                .complete_operator_agent_restore(command, session, at_ms)
        }

        fn mark_operator_agent_restore_uncertain(
            &self,
            operation_id: &str,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository()
                .mark_operator_agent_restore_uncertain(operation_id, reason, at_ms)
        }

        fn quarantine_started_operator_agent_restores(
            &self,
            at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            self.repository()
                .quarantine_started_operator_agent_restores(at_ms)
        }

        fn unresolved_operator_agent_restore_for_agent(
            &self,
            agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            self.repository()
                .unresolved_operator_agent_restore_for_agent(agent_id)
        }

        fn operator_agent_restore_reattachment_pending(
            &self,
            agent_id: &str,
        ) -> Result<bool, RuntimeError> {
            self.repository()
                .operator_agent_restore_reattachment_pending(agent_id)
        }

        fn resolve_operator_agent_restore_review(
            &self,
            command: &runtime_core::OperatorAgentRestoreReviewCommand,
        ) -> Result<runtime_core::OperatorAgentRestoreReviewResponse, RuntimeError> {
            self.repository()
                .resolve_operator_agent_restore_review(command)
        }
    };
}

pub(crate) use delegate_operator_agent_restore_methods;
