// Re-usable durable operator mutations for message/delivery authority.
// Keep the RuntimeStore trait's large source file under its line ceiling.
macro_rules! message_operator_methods {
    () => {
        /// Atomically settle all recipients and persist the exact cancellation
        /// receipt before returning from the operator API.
        fn cancel_agent_message_operator(
            &self,
            _command: &crate::AgentCancelMessageMutationCommand,
        ) -> Result<crate::AgentCancelMessageMutationResponse, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable operator message cancellation unavailable".into(),
            ))
        }

        /// Retry admission is committed before any provider-native work.
        /// The exact receipt describes the queue state at admission.
        fn retry_agent_delivery_operator(
            &self,
            _command: &crate::AgentRetryDeliveryMutationCommand,
        ) -> Result<crate::AgentRetryDeliveryMutationResponse, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable operator delivery retry admission unavailable".into(),
            ))
        }

        /// First native dispatch claims must be a durable compare-and-swap,
        /// including their legacy mirror. `false` means another worker won
        /// the claim. Only unsupported in-memory stores may use the fallback.
        fn compare_and_swap_agent_delivery_status(
            &self,
            _expected: &crate::AgentDeliveryRecord,
            _updated: &crate::AgentDeliveryRecord,
        ) -> Result<bool, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable agent delivery transition CAS unavailable".into(),
            ))
        }
    };
}

pub(super) use message_operator_methods;
