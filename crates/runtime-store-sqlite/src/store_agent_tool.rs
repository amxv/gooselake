macro_rules! delegate_agent_tool_methods {
    () => {
        fn plan_member_agent_rebind(
            &self,
            parent: &str,
            caller: &str,
            policy: runtime_core::WorkspaceMembershipPolicy,
            destination: &str,
            repository: &runtime_core::RepositoryIdentity,
            at_ms: i64,
        ) -> Result<runtime_core::MemberAgentRebindCommand, RuntimeError> {
            self.repository.plan_member_agent_rebind(
                parent,
                caller,
                policy,
                destination,
                repository,
                at_ms,
            )
        }
        fn replay_member_agent_rebind(
            &self,
            command: &runtime_core::MemberAgentRebindCommand,
        ) -> Result<Option<runtime_core::MemberAgentRebindAdmission>, RuntimeError> {
            self.repository.replay_member_agent_rebind(command)
        }
        fn admit_member_agent_rebind(
            &self,
            command: &runtime_core::MemberAgentRebindCommand,
        ) -> Result<runtime_core::MemberAgentRebindAdmission, RuntimeError> {
            self.repository.admit_member_agent_rebind(command)
        }
        fn reject_member_agent_rebind(
            &self,
            command: &runtime_core::MemberAgentRebindCommand,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository
                .reject_member_agent_rebind(command, reason, at_ms)
        }
        fn quarantine_started_workspace_agent_rebinds(
            &self,
            at_ms: i64,
        ) -> Result<Vec<String>, RuntimeError> {
            self.repository
                .quarantine_started_workspace_agent_rebinds(at_ms)
        }
        fn unresolved_workspace_agent_rebind_operation(
            &self,
            agent_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            self.repository
                .unresolved_workspace_agent_rebind_operation(agent_id)
        }
        fn plan_member_agent_archives(
            &self,
            parent_operation_id: &str,
            caller_agent_id: &str,
            policy: runtime_core::WorkspaceMembershipPolicy,
            planned_at: i64,
        ) -> Result<Vec<runtime_core::OperatorAgentArchiveCommand>, RuntimeError> {
            self.repository.plan_member_agent_archives(
                parent_operation_id,
                caller_agent_id,
                policy,
                planned_at,
            )
        }
        fn reject_member_agent_archive(
            &self,
            command: &runtime_core::OperatorAgentArchiveCommand,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), RuntimeError> {
            self.repository
                .reject_member_agent_archive(command, reason, at_ms)
        }
        fn replay_agent_tool_mutation(
            &self,
            command: &AgentToolMutationCommand,
        ) -> Result<Option<AgentToolMutationAdmission>, RuntimeError> {
            self.repository.replay_agent_tool_mutation(command)
        }
        fn begin_agent_tool_mutation(
            &self,
            command: &AgentToolMutationCommand,
        ) -> Result<AgentToolMutationAdmission, RuntimeError> {
            self.repository.begin_agent_tool_mutation(command)
        }
        fn finish_agent_tool_mutation(
            &self,
            operation_id: &str,
            exact_result: &Value,
            finished_at: i64,
        ) -> Result<(), RuntimeError> {
            self.repository
                .finish_agent_tool_mutation(operation_id, exact_result, finished_at)
        }
    };
}

pub(crate) use delegate_agent_tool_methods;
