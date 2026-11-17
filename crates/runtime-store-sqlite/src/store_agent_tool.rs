macro_rules! delegate_agent_tool_methods {
    () => {
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
