macro_rules! delegate_operator_agent_archive_methods {
    () => {
        fn replay_operator_agent_archive(
            &self,
            actor: &runtime_core::OperationActor,
            key: &str,
            workspace_id: &str,
            agent_id: &str,
            hash: &str,
        ) -> Result<Option<runtime_core::OperatorAgentArchiveAdmission>, RuntimeError> {
            self.repository().replay_operator_agent_archive(
                actor,
                key,
                workspace_id,
                agent_id,
                hash,
            )
        }

        fn admit_operator_agent_archive(
            &self,
            command: &runtime_core::OperatorAgentArchiveCommand,
        ) -> Result<runtime_core::OperatorAgentArchiveAdmission, RuntimeError> {
            self.repository().admit_operator_agent_archive(command)
        }

        fn complete_operator_agent_archive(
            &self,
            command: &runtime_core::OperatorAgentArchiveCommand,
            session: &SessionRecord,
            evidence: &runtime_core::ProviderRetirementCloseEvidence,
            at_ms: i64,
        ) -> Result<runtime_core::WorkspaceAgentRecord, RuntimeError> {
            self.repository()
                .complete_operator_agent_archive(command, session, evidence, at_ms)
        }

        fn mark_operator_agent_archive_uncertain(
            &self,
            operation_id: &str,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository()
                .mark_operator_agent_archive_uncertain(operation_id, reason, at_ms)
        }

        fn quarantine_started_operator_agent_archives(
            &self,
            at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            self.repository()
                .quarantine_started_operator_agent_archives(at_ms)
        }

        fn unresolved_operator_agent_archive_for_agent(
            &self,
            agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            self.repository()
                .unresolved_operator_agent_archive_for_agent(agent_id)
        }

        fn operator_agent_archive_reattachment_pending(
            &self,
            agent_id: &str,
        ) -> Result<bool, RuntimeError> {
            self.repository()
                .operator_agent_archive_reattachment_pending(agent_id)
        }

        fn resolve_operator_agent_archive_review(
            &self,
            command: &runtime_core::OperatorAgentArchiveReviewCommand,
        ) -> Result<runtime_core::OperatorAgentArchiveReviewResponse, RuntimeError> {
            self.repository()
                .resolve_operator_agent_archive_review(command)
        }
    };
}

pub(crate) use delegate_operator_agent_archive_methods;
