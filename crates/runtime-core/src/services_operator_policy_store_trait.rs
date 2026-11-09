macro_rules! operator_native_policy_store_methods {
    () => {
        /// A native provider policy operation is independently fenced from
        /// operator turns. A crash can leave provider and SQLite divergent.
        fn unresolved_operator_native_policy_for_agent(
            &self,
            _agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            Ok(None)
        }

        fn replay_operator_native_policy(
            &self,
            _actor: &crate::OperationActor,
            _key: &str,
            _agent_id: &str,
            _kind: crate::OperatorNativePolicyKind,
            _normalized_request_hash: &str,
        ) -> Result<Option<crate::OperatorNativePolicyAdmission>, RuntimeError> {
            unsupported_store("durable native policy replay unavailable")
        }

        fn admit_operator_native_policy(
            &self,
            _command: &crate::OperatorNativePolicyCommand,
        ) -> Result<crate::OperatorNativePolicyAdmission, RuntimeError> {
            unsupported_store("durable native policy admission unavailable")
        }

        fn complete_operator_native_policy(
            &self,
            _command: &crate::OperatorNativePolicyCommand,
            _session: &SessionRecord,
            _policy: &WorkspaceAgentRecreationPolicy,
            _result: &Value,
            _at_ms: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable native policy completion unavailable")
        }

        fn mark_operator_native_policy_uncertain(
            &self,
            _operation_id: &str,
            _reason: &str,
            _at_ms: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable native policy recovery unavailable")
        }

        /// Promote any pre-crash intended native policy effect to explicit
        /// review. A request durable before an uncertain dispatch must never
        /// be replayed automatically on startup.
        fn quarantine_started_operator_native_policies(
            &self,
            _at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            Ok(Vec::new())
        }

        /// Finalize an independently verified native policy outcome without
        /// dispatching provider work. The verified claim and revised policy
        /// must be committed atomically or neither may change.
        fn resolve_operator_native_policy_review(
            &self,
            _command: &crate::OperatorNativePolicyReviewCommand,
        ) -> Result<crate::OperatorNativePolicyReviewResponse, RuntimeError> {
            unsupported_store("operator native policy evidence review unavailable")
        }

        fn operator_native_policy_reattachment_pending(
            &self,
            _agent_id: &str,
        ) -> Result<bool, RuntimeError> {
            Ok(false)
        }
    };
}

pub(super) use operator_native_policy_store_methods;
