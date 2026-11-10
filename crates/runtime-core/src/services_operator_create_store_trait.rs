macro_rules! operator_agent_create_store_methods {
    () => {
        fn replay_operator_agent_create(
            &self,
            _actor: &crate::OperationActor,
            _key: &str,
            _workspace_id: &str,
            _request_hash: &str,
        ) -> Result<Option<crate::OperatorAgentCreateAdmission>, RuntimeError> {
            unsupported_store("durable operator agent creation replay unavailable")
        }

        fn admit_operator_agent_create(
            &self,
            _command: &crate::OperatorAgentCreateCommand,
        ) -> Result<crate::OperatorAgentCreateAdmission, RuntimeError> {
            unsupported_store("durable operator agent creation admission unavailable")
        }

        fn freeze_operator_agent_create_launch(
            &self,
            _command: &crate::OperatorAgentCreateCommand,
            _snapshot: &crate::OperatorAgentCreateLaunchSnapshot,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable provider create launch snapshot unavailable")
        }

        fn create_workspace_agent_with_operator_receipt(
            &self,
            _command: &crate::OperatorAgentCreateCommand,
            _session: &SessionRecord,
            _agent: &WorkspaceAgentRecord,
            _onboarding: &WorkspaceAgentOnboardingRequest,
            _hooks: Option<&TeamHookPreAddContext>,
        ) -> Result<(), RuntimeError> {
            unsupported_store("atomic workspace agent/operator create receipt unavailable")
        }

        fn mark_operator_agent_create_uncertain(
            &self,
            _operation_id: &str,
            _reason: &str,
            _at_ms: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable operator agent creation quarantine unavailable")
        }

        fn quarantine_started_operator_agent_creates(
            &self,
            _at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            Ok(Vec::new())
        }

        fn operator_agent_create_reattachment_pending(
            &self,
            _agent_id: &str,
        ) -> Result<bool, RuntimeError> {
            Ok(false)
        }

        fn resolve_operator_agent_create_review(
            &self,
            _command: &crate::OperatorAgentCreateReviewCommand,
        ) -> Result<crate::OperatorAgentCreateReviewResponse, RuntimeError> {
            unsupported_store("independent operator agent creation review unavailable")
        }
    };
}

pub(super) use operator_agent_create_store_methods;
