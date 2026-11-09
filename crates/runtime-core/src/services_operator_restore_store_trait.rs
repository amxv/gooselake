macro_rules! operator_agent_restore_store_methods {
    () => {
        fn replay_operator_agent_restore(
            &self,
            _actor: &crate::OperationActor,
            _key: &str,
            _workspace_id: &str,
            _agent_id: &str,
            _normalized_request_hash: &str,
        ) -> Result<Option<crate::OperatorAgentRestoreAdmission>, RuntimeError> {
            unsupported_store("durable operator agent restore replay unavailable")
        }

        fn admit_operator_agent_restore(
            &self,
            _command: &crate::OperatorAgentRestoreCommand,
        ) -> Result<crate::OperatorAgentRestoreAdmission, RuntimeError> {
            unsupported_store("durable operator agent restore admission unavailable")
        }

        fn complete_operator_agent_restore(
            &self,
            _command: &crate::OperatorAgentRestoreCommand,
            _session: &SessionRecord,
            _at_ms: i64,
        ) -> Result<crate::WorkspaceAgentRecord, RuntimeError> {
            unsupported_store("durable operator agent restore receipt unavailable")
        }

        fn mark_operator_agent_restore_uncertain(
            &self,
            _operation_id: &str,
            _reason: &str,
            _at_ms: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable operator agent restore quarantine unavailable")
        }

        fn quarantine_started_operator_agent_restores(
            &self,
            _at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            Ok(Vec::new())
        }

        fn unresolved_operator_agent_restore_for_agent(
            &self,
            _agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            Ok(None)
        }

        fn operator_agent_restore_reattachment_pending(
            &self,
            _agent_id: &str,
        ) -> Result<bool, RuntimeError> {
            Ok(false)
        }

        fn resolve_operator_agent_restore_review(
            &self,
            _command: &crate::OperatorAgentRestoreReviewCommand,
        ) -> Result<crate::OperatorAgentRestoreReviewResponse, RuntimeError> {
            unsupported_store("operator restore evidence review unavailable")
        }
    };
}

pub(super) use operator_agent_restore_store_methods;
