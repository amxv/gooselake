use crate::{ApprovalRecord, RuntimeEventRecord, SessionRecord, TurnAdmissionRecord, TurnRecord};

/// One positively acknowledged native turn, its three durable projections and
/// the same Critical start receipt. This is a post-native observation, not an
/// authorization to dispatch again after an uncertain return.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderDispatchStartedCommit {
    pub admission: TurnAdmissionRecord,
    pub turn: TurnRecord,
    pub session: SessionRecord,
    pub event: RuntimeEventRecord,
}

/// A proven no-native-send or an uncertain native-send outcome. Its durable
/// dispatch admission, turn, session, affected approvals and Critical event
/// must have one commit boundary; UNKNOWN is never authorization to resend.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderDispatchOutcomeCommit {
    pub admission: TurnAdmissionRecord,
    pub turn: TurnRecord,
    pub session: SessionRecord,
    pub approvals: Vec<ApprovalRecord>,
    pub event: RuntimeEventRecord,
}

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

/// Critical pre-native decision reservation or a proven no-effect release.
/// A committed reservation is intentionally NOT a native dispatch receipt;
/// its exclusive durable status prevents another process from sending again.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderApprovalDecisionReceipt {
    pub approval: ApprovalRecord,
    pub event: Option<RuntimeEventRecord>,
}

/// Committed provider-native decision result (success or unknown). Returned
/// projections are canonical, including a concurrently completed provider turn.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderApprovalFinalization {
    pub approval: ApprovalRecord,
    pub turn: TurnRecord,
    pub session: SessionRecord,
    pub event: Option<RuntimeEventRecord>,
}

/// A local pre-dispatch denial has no native provider effect. The approval,
/// still-pending admission, interrupted turn, released session and BOTH
/// Critical events must become visible together, including across restart.
#[derive(Debug, Clone, PartialEq)]
pub struct PreDispatchApprovalDeclineCommit {
    pub admission: TurnAdmissionRecord,
    pub approval: ApprovalRecord,
    pub turn: TurnRecord,
    pub session: SessionRecord,
    pub events: Vec<RuntimeEventRecord>,
}

macro_rules! provider_terminal_methods {
    () => {
        /// Commit a positive native send acknowledgement, canonical dispatch
        /// mapping, running turn/session and Critical turn.started together.
        /// A failed receipt leaves `dispatching` fenced for restart review.
        fn commit_provider_turn_started(
            &self,
            _before_admission: &crate::TurnAdmissionRecord,
            _after_admission: &crate::TurnAdmissionRecord,
            _before_turn: &crate::TurnRecord,
            _after_turn: &crate::TurnRecord,
            _before_session: &crate::SessionRecord,
            _after_session: &crate::SessionRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderDispatchStartedCommit, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider dispatch start and Critical journal unavailable".into(),
            ))
        }

        /// Persist a pre-terminal provider send outcome and its Critical
        /// receipt atomically. Pending approvals cancelled for a *proven*
        /// no-send outcome are part of the same transaction.
        fn commit_provider_dispatch_outcome(
            &self,
            _before_admission: &crate::TurnAdmissionRecord,
            _after_admission: &crate::TurnAdmissionRecord,
            _before_turn: &crate::TurnRecord,
            _after_turn: &crate::TurnRecord,
            _before_session: &crate::SessionRecord,
            _after_session: &crate::SessionRecord,
            _approvals: &[(crate::ApprovalRecord, crate::ApprovalRecord)],
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderDispatchOutcomeCommit, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider dispatch outcome and Critical journal unavailable".into(),
            ))
        }

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

        /// An already-terminal turn's contradictory late native observation
        /// may fail its *still-unowned* session, but only with a durable,
        /// turn-bound Critical receipt in the same compare-and-swap commit.
        /// Never let a stale manager overwrite a newer session owner.
        fn commit_provider_terminal_conflict(
            &self,
            _previous_turn: &crate::TurnRecord,
            _previous_session: &crate::SessionRecord,
            _failed_session: &crate::SessionRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<(crate::SessionRecord, Option<crate::RuntimeEventRecord>), crate::RuntimeError>
        {
            Err(crate::RuntimeError::Unsupported(
                "atomic terminal conflict and Critical receipt unavailable".into(),
            ))
        }

        /// An unsuccessful provider wait or lost native event stream is
        /// observed *after* a potentially dispatched turn. Its guarded turn
        /// and session projections must commit together with the critical
        /// recovery/error receipt. No journal fault can reopen native send.
        fn commit_provider_failure_result(
            &self,
            _previous_turn: &crate::TurnRecord,
            _updated_turn: &crate::TurnRecord,
            _previous_session: &crate::SessionRecord,
            _updated_session: &crate::SessionRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderTerminalCommit, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider failure and critical event journal unavailable".into(),
            ))
        }

        /// Commit legacy provider session admission/resume/close projections
        /// with their one critical event. On a post-native journal fault the
        /// caller MUST NOT assume it can replay a native create/close/resume.
        fn commit_session_lifecycle(
            &self,
            _before: Option<&crate::SessionRecord>,
            _after: &crate::SessionRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<(crate::SessionRecord, Option<crate::RuntimeEventRecord>), crate::RuntimeError>
        {
            Err(crate::RuntimeError::Unsupported(
                "atomic legacy provider session lifecycle journal unavailable".into(),
            ))
        }
    };
}
pub(super) use provider_terminal_methods;

macro_rules! provider_approval_methods {
    () => {
        /// Deny a pending local policy gate without touching the provider.
        /// A failed event journal rolls back the entire local authority.
        fn decline_pre_dispatch_approval(
            &self,
            _before_admission: &crate::TurnAdmissionRecord,
            _after_admission: &crate::TurnAdmissionRecord,
            _before_approval: &crate::ApprovalRecord,
            _after_approval: &crate::ApprovalRecord,
            _before_turn: &crate::TurnRecord,
            _after_turn: &crate::TurnRecord,
            _before_session: &crate::SessionRecord,
            _after_session: &crate::SessionRecord,
            _events: &[crate::NewRuntimeEvent],
        ) -> Result<crate::PreDispatchApprovalDeclineCommit, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic local pre-dispatch denial and Critical journal unavailable".into(),
            ))
        }

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

        /// Reserve one pending provider-native approval decision before any
        /// native call. An ambiguous commit must fail closed, not return an
        /// existing reservation as permission to call the provider again.
        fn claim_provider_approval_decision(
            &self,
            _pending: &crate::ApprovalRecord,
            _reserved: &crate::ApprovalRecord,
            _turn: &crate::TurnRecord,
            _session: &crate::SessionRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderApprovalDecisionReceipt, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider-native approval decision claim unavailable".into(),
            ))
        }

        /// Release a claimed decision only after the provider has proved it
        /// could not have executed. Atomic state and event are required.
        fn release_unexecuted_provider_approval_decision(
            &self,
            _reserved: &crate::ApprovalRecord,
            _pending: &crate::ApprovalRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderApprovalDecisionReceipt, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider-native approval no-effect release unavailable".into(),
            ))
        }

        /// A known successful provider acknowledgement is already an effect.
        /// Commit its resolved approval, guarded turn/session projections,
        /// and critical resolved event as one durable, restart-replayable
        /// transaction, never as separate best-effort writes.
        fn finalize_provider_approval_decision(
            &self,
            _reserved: &crate::ApprovalRecord,
            _resolved: &crate::ApprovalRecord,
            _before_turn: &crate::TurnRecord,
            _after_turn: &crate::TurnRecord,
            _before_session: &crate::SessionRecord,
            _after_session: &crate::SessionRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderApprovalFinalization, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider-native approval post-effect result unavailable".into(),
            ))
        }

        /// After an ambiguous native result, keep the reservation fenced and
        /// atomically quarantine the turn/session with its critical receipt.
        /// Do not turn journal failure into permission to resend a decision.
        fn quarantine_provider_approval_decision(
            &self,
            _reserved: &crate::ApprovalRecord,
            _before_turn: &crate::TurnRecord,
            _after_turn: &crate::TurnRecord,
            _before_session: &crate::SessionRecord,
            _after_session: &crate::SessionRecord,
            _event: &crate::NewRuntimeEvent,
        ) -> Result<crate::ProviderApprovalFinalization, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic provider-native approval ambiguous result unavailable".into(),
            ))
        }
    };
}
pub(super) use provider_approval_methods;
