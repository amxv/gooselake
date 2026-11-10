macro_rules! delegate_operator_agent_create_methods {
    () => {
        fn replay_operator_agent_create(
            &self,
            actor: &runtime_core::OperationActor,
            key: &str,
            workspace_id: &str,
            hash: &str,
        ) -> Result<Option<runtime_core::OperatorAgentCreateAdmission>, RuntimeError> {
            self.repository()
                .replay_operator_agent_create(actor, key, workspace_id, hash)
        }

        fn admit_operator_agent_create(
            &self,
            command: &runtime_core::OperatorAgentCreateCommand,
        ) -> Result<runtime_core::OperatorAgentCreateAdmission, RuntimeError> {
            self.repository().admit_operator_agent_create(command)
        }

        fn freeze_operator_agent_create_launch(
            &self,
            command: &runtime_core::OperatorAgentCreateCommand,
            snapshot: &runtime_core::OperatorAgentCreateLaunchSnapshot,
        ) -> Result<(), RuntimeError> {
            self.repository()
                .freeze_operator_agent_create_launch(command, snapshot)
        }

        fn create_workspace_agent_with_operator_receipt(
            &self,
            command: &runtime_core::OperatorAgentCreateCommand,
            session: &SessionRecord,
            agent: &WorkspaceAgentRecord,
            onboarding: &WorkspaceAgentOnboardingRequest,
            hooks: Option<&runtime_core::TeamHookPreAddContext>,
        ) -> Result<(), RuntimeError> {
            self.repository()
                .create_workspace_agent_with_operator_receipt(
                    command, session, agent, onboarding, hooks,
                )
        }

        fn mark_operator_agent_create_uncertain(
            &self,
            operation_id: &str,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository()
                .mark_operator_agent_create_uncertain(operation_id, reason, at_ms)
        }

        fn quarantine_started_operator_agent_creates(
            &self,
            at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            self.repository()
                .quarantine_started_operator_agent_creates(at_ms)
        }

        fn operator_agent_create_reattachment_pending(
            &self,
            agent_id: &str,
        ) -> Result<bool, RuntimeError> {
            self.repository()
                .operator_agent_create_reattachment_pending(agent_id)
        }

        fn resolve_operator_agent_create_review(
            &self,
            command: &runtime_core::OperatorAgentCreateReviewCommand,
        ) -> Result<runtime_core::OperatorAgentCreateReviewResponse, RuntimeError> {
            self.repository()
                .resolve_operator_agent_create_review(command)
        }
    };
}

pub(crate) use delegate_operator_agent_create_methods;
