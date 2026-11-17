macro_rules! agent_tool_store_methods {
    () => {
        fn plan_member_agent_rebind(
            &self,
            _parent_operation_id: &str,
            _caller_agent_id: &str,
            _policy: crate::WorkspaceMembershipPolicy,
            _destination_id: &str,
            _repository: &crate::RepositoryIdentity,
            _planned_at: i64,
        ) -> Result<crate::MemberAgentRebindCommand, RuntimeError> {
            unsupported_store("durable member rebind planning unavailable")
        }
        fn replay_member_agent_rebind(
            &self,
            _command: &crate::MemberAgentRebindCommand,
        ) -> Result<Option<crate::MemberAgentRebindAdmission>, RuntimeError> {
            unsupported_store("durable member rebind replay unavailable")
        }
        fn admit_member_agent_rebind(
            &self,
            _command: &crate::MemberAgentRebindCommand,
        ) -> Result<crate::MemberAgentRebindAdmission, RuntimeError> {
            unsupported_store("durable member rebind admission unavailable")
        }
        fn reject_member_agent_rebind(
            &self,
            _command: &crate::MemberAgentRebindCommand,
            _reason: &str,
            _at_ms: i64,
        ) -> Result<(), RuntimeError> {
            unsupported_store("durable member rebind rejection unavailable")
        }
        fn quarantine_started_workspace_agent_rebinds(
            &self,
            _at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            Ok(Vec::new())
        }
        fn unresolved_workspace_agent_rebind_operation(
            &self,
            _agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            Ok(None)
        }
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
