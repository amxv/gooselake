macro_rules! operator_agent_archive_store_methods {
    () => {
        fn replay_operator_agent_archive(
            &self,
            _actor: &crate::OperationActor,
            _key: &str,
            _workspace_id: &str,
            _agent_id: &str,
            _normalized_request_hash: &str,
        ) -> Result<Option<crate::OperatorAgentArchiveAdmission>, RuntimeError> {
            unsupported_store("durable operator agent archive replay unavailable")
        }

        fn admit_operator_agent_archive(
            &self,
            _command: &crate::OperatorAgentArchiveCommand,
        ) -> Result<crate::OperatorAgentArchiveAdmission, RuntimeError> {
            unsupported_store("durable operator agent archive admission unavailable")
        }

        fn complete_operator_agent_archive(
            &self,
            _command: &crate::OperatorAgentArchiveCommand,
            _session: &SessionRecord,
            _close_evidence: &crate::ProviderRetirementCloseEvidence,
            _at_ms: i64,
        ) -> Result<crate::WorkspaceAgentRecord, RuntimeError> {
            unsupported_store("durable operator agent archive receipt unavailable")
        }

        fn mark_operator_agent_archive_uncertain(
            &self,
            _operation_id: &str,
            _reason: &str,
            _at_ms: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable operator agent archive quarantine unavailable")
        }

        fn quarantine_started_operator_agent_archives(
            &self,
            _at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            Ok(Vec::new())
        }

        fn unresolved_operator_agent_archive_for_agent(
            &self,
            _agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            Ok(None)
        }

        fn operator_agent_archive_reattachment_pending(
            &self,
            _agent_id: &str,
        ) -> Result<bool, RuntimeError> {
            Ok(false)
        }

        fn resolve_operator_agent_archive_review(
            &self,
            _command: &crate::OperatorAgentArchiveReviewCommand,
        ) -> Result<crate::OperatorAgentArchiveReviewResponse, RuntimeError> {
            unsupported_store("operator archive independent review unavailable")
        }
    };
}

pub(super) use operator_agent_archive_store_methods;
