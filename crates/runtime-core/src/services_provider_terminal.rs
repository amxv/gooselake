use crate::{ApprovalRecord, RuntimeEventRecord, SessionRecord, TurnRecord};

/// Durable projection of one provider result, returned even if an identical
/// receipt was committed before a lost response or a process restart.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderTerminalCommit {
    pub turn: TurnRecord,
    pub session: Option<SessionRecord>,
    pub event: RuntimeEventRecord,
}

/// Already-committed provider-native approval request and its projections.
/// A duplicate native callback returns the prior identity without fabricating
/// an event for legacy approvals that predate the atomic request journal.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderApprovalAdmission {
    pub approval: ApprovalRecord,
    pub turn: TurnRecord,
    pub session: SessionRecord,
    pub event: Option<RuntimeEventRecord>,
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

macro_rules! provider_approval_methods {
    () => {
        /// Atomically journal a provider-native approval callback with its
        /// approval/turn/session state before projecting it into live memory.
        fn admit_provider_approval(
            &self,
            _previous_turn: &crate::TurnRecord,
            _updated_turn: &crate::TurnRecord,
            _previous_session: &crate::SessionRecord,
            _updated_session: &crate::SessionRecord,
            _approval: &crate::ApprovalRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderApprovalAdmission, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider-native approval admission unavailable".into(),
            ))
        }
    };
}
pub(super) use provider_approval_methods;
