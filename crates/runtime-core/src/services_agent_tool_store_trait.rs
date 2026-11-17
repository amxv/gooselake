macro_rules! agent_tool_store_methods {
    () => {
        fn plan_member_agent_archives(
            &self,
            _parent_operation_id: &str,
            _caller_agent_id: &str,
            _policy: crate::WorkspaceMembershipPolicy,
            _planned_at: i64,
        ) -> Result<Vec<crate::OperatorAgentArchiveCommand>, RuntimeError> {
            unsupported_store("durable member archive planning unavailable")
        }
        fn reject_member_agent_archive(
            &self,
            _command: &crate::OperatorAgentArchiveCommand,
            _reason: &str,
            _at_ms: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable member archive rejection unavailable")
        }
        /// Read existing outcomes before evaluating mutable admission policy.
        /// A missing receipt does not reserve or authorize native work.
        fn replay_agent_tool_mutation(
            &self,
            _command: &AgentToolMutationCommand,
        ) -> Result<Option<AgentToolMutationAdmission>, RuntimeError> {
            unsupported_store("durable model-tool mutation replay unavailable")
        }
        /// Commit admission before any native effect, and fail closed on ambiguity.
        fn begin_agent_tool_mutation(
            &self,
            _command: &AgentToolMutationCommand,
        ) -> Result<AgentToolMutationAdmission, RuntimeError> {
            unsupported_store("durable model-tool mutation admission unavailable")
        }
        fn finish_agent_tool_mutation(
            &self,
            _operation_id: &str,
            _exact_result: &Value,
            _finished_at: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable model-tool mutation receipt unavailable")
        }
    };
}

pub(super) use agent_tool_store_methods;
