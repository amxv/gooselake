use crate::{RuntimeEventRecord, SessionRecord, TurnRecord};

/// Durable projection of one provider result, returned even if an identical
/// receipt was committed before a lost response or a process restart.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderTerminalCommit {
    pub turn: TurnRecord,
    pub session: Option<SessionRecord>,
    pub event: RuntimeEventRecord,
}

macro_rules! provider_terminal_methods {
    () => {
        /// Atomically commit an already-observed provider terminal turn,
        /// attached session update and critical journal receipt. Never
        /// interpret a lost response as permission to resend native work.
        fn commit_provider_turn_result(
            &self,
            _previous_turn: &crate::TurnRecord,
            _updated_turn: &crate::TurnRecord,
            _previous_session: Option<&crate::SessionRecord>,
            _updated_session: Option<&crate::SessionRecord>,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderTerminalCommit, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider terminal result and event journal unavailable".into(),
            ))
        }
    };
}
pub(super) use provider_terminal_methods;
