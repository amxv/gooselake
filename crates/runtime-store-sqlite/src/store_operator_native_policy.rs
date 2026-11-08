macro_rules! delegate_operator_native_policy_methods {
    () => {
        fn unresolved_operator_native_policy_for_agent(
            &self,
            agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            self.repository()
                .unresolved_operator_native_policy_for_agent(agent_id)
        }

        fn replay_operator_native_policy(
            &self,
            actor: &runtime_core::OperationActor,
            key: &str,
            agent_id: &str,
            kind: runtime_core::OperatorNativePolicyKind,
            normalized_request_hash: &str,
        ) -> Result<Option<runtime_core::OperatorNativePolicyAdmission>, RuntimeError> {
            self.repository().replay_operator_native_policy(
                actor,
                key,
                agent_id,
                kind,
                normalized_request_hash,
            )
        }

        fn admit_operator_native_policy(
            &self,
            command: &runtime_core::OperatorNativePolicyCommand,
        ) -> Result<runtime_core::OperatorNativePolicyAdmission, RuntimeError> {
            self.repository().admit_operator_native_policy(command)
        }

        fn complete_operator_native_policy(
            &self,
            command: &runtime_core::OperatorNativePolicyCommand,
            session: &SessionRecord,
            policy: &WorkspaceAgentRecreationPolicy,
            result: &Value,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository()
                .complete_operator_native_policy(command, session, policy, result, at_ms)
        }

        fn mark_operator_native_policy_uncertain(
            &self,
            operation_id: &str,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository()
                .mark_operator_native_policy_uncertain(operation_id, reason, at_ms)
        }
    };
}

pub(crate) use delegate_operator_native_policy_methods;
