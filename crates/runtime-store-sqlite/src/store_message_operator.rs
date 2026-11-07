macro_rules! delegate_message_operator_methods {
    () => {
        fn cancel_agent_message_operator(
            &self,
            command: &runtime_core::AgentCancelMessageMutationCommand,
        ) -> Result<runtime_core::AgentCancelMessageMutationResponse, runtime_core::RuntimeError> {
            self.repository.cancel_agent_message_operator(command)
        }

        fn retry_agent_delivery_operator(
            &self,
            command: &runtime_core::AgentRetryDeliveryMutationCommand,
        ) -> Result<runtime_core::AgentRetryDeliveryMutationResponse, runtime_core::RuntimeError> {
            self.repository.retry_agent_delivery_operator(command)
        }
    };
}

pub(crate) use delegate_message_operator_methods;
