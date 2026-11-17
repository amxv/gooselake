use crate::{SessionRecord, TurnAdmissionRecord, TurnDispatchState, TurnRecord};
pub fn startup_projection_refusal(
    session: &SessionRecord,
    turn: Option<&TurnRecord>,
    admission: Option<&TurnAdmissionRecord>,
    approval_decision_pending: bool,
) -> Option<&'static str> {
    if crate::startup_review_status_blocked(&session.status) {
        return Some("historical reviewed status lacks approved structured native authority");
    }
    if matches!(
        session.status.as_str(),
        "closed" | "failed" | "turn_recovery_required"
    ) || session.closed_at.is_some()
        || matches!(
            session.failure_code.as_deref(),
            Some(
                "startup_provider_resume_failed"
                    | "startup_policy_reattachment_failed"
                    | "startup_restore_reattachment_failed"
                    | "startup_archive_reattachment_failed"
                    | "startup_create_reattachment_failed"
                    | "startup_policy_identity_changed"
                    | "startup_restore_identity_changed"
                    | "startup_archive_identity_changed"
                    | "startup_create_identity_changed"
                    | "startup_turn_identity_restore_failed"
            )
        )
    {
        return Some("stored session is closed, failed, or has an unresolved native attachment");
    }
    if approval_decision_pending {
        return Some("provider approval decision is unresolved");
    }
    let Some(turn_id) = session.active_turn_id.as_deref() else {
        return None;
    };
    let Some(turn) = turn else {
        return Some("active logical turn is missing");
    };
    if turn.id != turn_id || turn.session_id != session.id {
        return Some("active logical turn ownership differs");
    }
    if matches!(
        turn.status.as_str(),
        "dispatch_unknown"
            | "approval_decision_unknown"
            | "provider_event_recovery_required"
            | "provider_session_recovery_required"
    ) {
        return Some("active logical turn is quarantined");
    }
    match admission {
        Some(admission) => {
            if admission.turn_id != turn_id
                || admission.session_id != session.id
                || admission.provider != session.provider
                || matches!(
                    admission.dispatch_state,
                    TurnDispatchState::Dispatching | TurnDispatchState::Unknown
                )
            {
                return Some("original turn dispatch is unresolved or differently owned");
            }
        }
        None if !matches!(turn.status.as_str(), "in_progress" | "waiting_for_approval")
            && turn.completed_at.is_none() =>
        {
            return Some("legacy active turn lacks a supported accepted-turn disposition");
        }
        None => {}
    }
    None
}
