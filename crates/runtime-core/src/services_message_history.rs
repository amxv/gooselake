// Bounded durable message pages are a store capability, not an in-memory
// snapshot concern. Keep this extension outside the large RuntimeStore trait.
macro_rules! message_history_methods {
    () => {
        fn list_agent_messages_page(
            &self,
            _request: &crate::AgentMessageListRequest,
        ) -> Result<crate::AgentMessageListResponse, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "indexed canonical message history unavailable".into(),
            ))
        }
    };
}

pub(super) use message_history_methods;
