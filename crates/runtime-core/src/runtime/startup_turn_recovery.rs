use super::{helpers::now_ms, RuntimeSessionManager, StartupRecoverySummary};
use crate::{
    ProviderDispatchOutcome, ProviderKind, ProviderSendTurnRequest,
    RetirementReviewSessionProjection, RuntimeError, TurnDispatchState,
};
use std::sync::Arc;

fn active<'a>(
    snapshot: &'a RetirementReviewSessionProjection,
) -> (
    Option<&'a crate::TurnRecord>,
    Option<&'a crate::TurnAdmissionRecord>,
) {
    let id = snapshot.session.active_turn_id.as_deref();
    (
        snapshot
            .turns
            .iter()
            .find(|turn| Some(turn.id.as_str()) == id),
        snapshot
            .admissions
            .iter()
            .find(|admission| Some(admission.turn_id.as_str()) == id),
    )
}

fn projection_refusal(snapshot: &RetirementReviewSessionProjection) -> Option<&'static str> {
    let (turn, admission) = active(snapshot);
    let pending = snapshot
        .approvals
        .iter()
        .any(|approval| approval.status == "decision_pending");
    crate::startup_projection_refusal(&snapshot.session, turn, admission, pending)
}

fn policy_approval_status<'a>(
    snapshot: &'a RetirementReviewSessionProjection,
    turn_id: &str,
) -> Result<Option<&'a str>, RuntimeError> {
    let mut approvals = snapshot.approvals.iter().filter(|approval| {
        approval.turn_id == turn_id && approval.origin == "runtime_pre_dispatch_policy"
    });
    let first = approvals.next();
    if approvals.next().is_some() {
        return Err(RuntimeError::Conflict(
            "multiple original policy approvals for one turn".into(),
        ));
    }
    Ok(first.map(|approval| approval.status.as_str()))
}

impl RuntimeSessionManager {
    pub(super) async fn recover_startup_session(
        self: &Arc<Self>,
        session_id: &str,
        summary: &mut StartupRecoverySummary,
    ) -> Result<(), RuntimeError> {
        let mut snapshot = self.refresh_startup_session(session_id).await?;
        summary.turns_scanned += snapshot.turns.len();
        summary.approvals_scanned += snapshot.approvals.len();
        let parent = self
            .store
            .settle_startup_operator_turn(session_id, now_ms());
        let (turn, admission) = active(&snapshot);
        if let (Some(turn), Some(admission)) = (turn, admission) {
            if admission.dispatch_state == TurnDispatchState::Dispatching
                && admission.provider_native_turn_id.is_none()
                && admission.dispatch_error.is_none()
                && turn.provider_turn_ref.is_none()
                && turn.error.is_none()
                && turn.completed_at.is_none()
                && matches!(turn.status.as_str(), "admitted" | "waiting_for_approval")
                && matches!(
                    snapshot.session.status.as_str(),
                    "turn_admitted" | "waiting_for_approval"
                )
                && snapshot.session.failure_code.is_none()
                && snapshot.session.failure_message.is_none()
            {
                self.commit_provider_dispatch_outcome_from_snapshot(
                    session_id,
                    &turn.id,
                    &RuntimeError::provider_dispatch_unknown(
                        "startup_dispatch_inflight",
                        "startup recovery found the original native dispatch in-flight",
                    ),
                    false,
                    Some(&snapshot),
                )
                .await?;
                summary.turns_reconciled += 1;
                summary.sessions_reconciled += 1;
                return Err(RuntimeError::Conflict(
                    "original dispatch remains quarantined without native replay".into(),
                ));
            }
        }
        if let Some(reason) = projection_refusal(&snapshot) {
            if let Err(error) = parent {
                summary.notes.push(error.to_string());
            }
            return Err(RuntimeError::Conflict(reason.into()));
        }
        if let (Some(turn), Some(admission)) = active(&snapshot) {
            let no_dispatch = match admission.dispatch_state {
                TurnDispatchState::NotDispatched => {
                    let error = admission.dispatch_error.as_ref().ok_or_else(|| {
                        RuntimeError::Conflict(
                            "known non-dispatch lacks its original error receipt".into(),
                        )
                    })?;
                    let code = error["code"].as_str().ok_or_else(|| {
                        RuntimeError::Conflict("known non-dispatch lacks its original code".into())
                    })?;
                    let message = error["message"].as_str().ok_or_else(|| {
                        RuntimeError::Conflict(
                            "known non-dispatch lacks its original message".into(),
                        )
                    })?;
                    Some(RuntimeError::provider_not_dispatched(
                        code,
                        message
                            .strip_prefix("provider dispatch not_dispatched: ")
                            .unwrap_or(message),
                    ))
                }
                TurnDispatchState::Pending => {
                    let approval = policy_approval_status(&snapshot, &turn.id)?;
                    if admission.dispatch_policy.pre_dispatch_approval_required
                        && approval == Some("decline")
                    {
                        Some(RuntimeError::provider_not_dispatched(
                            "pre_dispatch_approval_declined",
                            "runtime pre-dispatch approval was declined before provider dispatch",
                        ))
                    } else if let Some(path) =
                        admission.user_input_snapshot.first_missing_image_path()
                    {
                        Some(RuntimeError::provider_not_dispatched("missing_turn_input_attachment",
                            format!("durable turn input attachment is unavailable during startup recovery: {path}")))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if let Some(error) = no_dispatch {
                self.commit_provider_dispatch_outcome_from_snapshot(
                    session_id,
                    &turn.id,
                    &error,
                    true,
                    Some(&snapshot),
                )
                .await?;
                summary.turns_reconciled += 1;
                summary.sessions_reconciled += 1;
                snapshot = self.refresh_startup_session(session_id).await?;
            }
        }
        self.store
            .settle_startup_operator_turn(session_id, now_ms())?;
        if let Some(reason) = projection_refusal(&snapshot) {
            return Err(RuntimeError::Conflict(reason.into()));
        }
        let Some(provider) = self.attach_startup_session(&snapshot).await? else {
            summary.notes.push(format!("session {session_id} retained its prior startup receipt without repeating native attachment"));
            return Ok(());
        };
        summary.resumed_sessions += 1;
        let attached = self.refresh_startup_session(session_id).await?;
        if attached.session != snapshot.session {
            summary.sessions_reconciled += 1;
        }
        if let Some(reason) = projection_refusal(&attached) {
            return Err(RuntimeError::Conflict(reason.into()));
        }
        self.recover_attached_startup_turn(provider, &attached, summary)
            .await
    }

    async fn recover_attached_startup_turn(
        self: &Arc<Self>,
        provider_kind: ProviderKind,
        snapshot: &RetirementReviewSessionProjection,
        summary: &mut StartupRecoverySummary,
    ) -> Result<(), RuntimeError> {
        let session = &snapshot.session;
        let (turn, admission) = active(snapshot);
        let Some(turn) = turn else {
            return Ok(());
        };
        if turn.completed_at.is_some() {
            return Ok(());
        }
        match admission.map(|row| row.dispatch_state) {
            Some(TurnDispatchState::Pending) => {
                let admission = admission.expect("matched Pending admission");
                if admission.dispatch_policy.pre_dispatch_approval_required {
                    match policy_approval_status(snapshot, &turn.id)? {
                        Some("pending") => return Ok(()),
                        Some("accept") => {}
                        _ => {
                            return Err(RuntimeError::Conflict(
                                "original Pending turn lacks an accepted policy decision".into(),
                            ))
                        }
                    }
                }
                if let Some(path) = admission.user_input_snapshot.first_missing_image_path() {
                    self.commit_provider_dispatch_outcome_from_snapshot(&session.id, &turn.id,
                        &RuntimeError::provider_not_dispatched("missing_turn_input_attachment",
                            format!("durable turn input attachment is unavailable during startup recovery: {path}")),
                        true, Some(snapshot)).await?;
                    self.store
                        .settle_startup_operator_turn(&session.id, now_ms())?;
                    return Ok(());
                }
                let input = turn.input.as_array().cloned().ok_or_else(|| {
                    RuntimeError::Conflict("original Pending turn input is not an array".into())
                })?;
                let provider = self.providers.get(provider_kind).ok_or_else(|| {
                    RuntimeError::ProviderNotRegistered(provider_kind.as_str().into())
                })?;
                let events = provider.subscribe_events();
                let claimed = match self
                    .update_turn_dispatch_authority(
                        &turn.id,
                        TurnDispatchState::Dispatching,
                        None,
                        None,
                    )
                    .await
                {
                    Ok(claimed) => claimed,
                    Err(error) => {
                        if matches!(error, RuntimeError::TurnDispatchNotClaimed { .. }) {
                            self.refresh_startup_session(&session.id).await?;
                        }
                        return Err(error);
                    }
                };
                // The consumed Pending claim is never reset on a changed
                // projection. Compare the exact original authority again
                // before the single native call and retain this frozen bundle.
                let dispatch_snapshot = self.refresh_startup_session(&session.id).await?;
                if dispatch_snapshot.session != *session
                    || dispatch_snapshot.turns.iter().find(|row| row.id == turn.id) != Some(turn)
                    || dispatch_snapshot
                        .admissions
                        .iter()
                        .find(|row| row.turn_id == turn.id)
                        != Some(&claimed)
                    || approvals_for_turn(&dispatch_snapshot, &turn.id)
                        != approvals_for_turn(snapshot, &turn.id)
                {
                    return Err(RuntimeError::Conflict(
                        "startup first dispatch authority changed after its claim; native send withheld and claim retained".into()));
                }
                let request = ProviderSendTurnRequest {
                    runtime_session_id: session.id.clone(),
                    turn_id: turn.id.clone(),
                    input,
                    expected_turn_id: admission.correlation.expected_turn_id.clone(),
                    permission_mode: admission.dispatch_policy.permission_mode.clone(),
                    approval_id: None,
                    turn_settings: admission.dispatch_policy.turn_settings.clone(),
                };
                match provider.send_turn(request).await {
                    Ok(ack) if ack.runtime_session_id == session.id && ack.turn_id == turn.id => {
                        self.commit_provider_dispatch_started_from_snapshot(
                            &session.id,
                            &turn.id,
                            ack.provider_native_turn_id,
                            None,
                            Some(&dispatch_snapshot),
                        )
                        .await?;
                        if let Err(error) = self
                            .store
                            .settle_startup_operator_turn(&session.id, now_ms())
                        {
                            summary.notes.push(error.to_string());
                        }
                        self.spawn_wait_for_turn(
                            provider_kind,
                            session.id.clone(),
                            turn.id.clone(),
                            events,
                        );
                        summary.resumed_waits += 1;
                        summary.turns_reconciled += 1;
                        summary.sessions_reconciled += 1;
                    }
                    Ok(ack) => {
                        let error = RuntimeError::provider_dispatch_unknown(
                            "ack_identity_mismatch",
                            format!(
                                "startup first-send ACK changed session/turn: {}/{}",
                                ack.runtime_session_id, ack.turn_id
                            ),
                        );
                        self.commit_provider_dispatch_outcome_from_snapshot(
                            &session.id,
                            &turn.id,
                            &error,
                            false,
                            Some(&dispatch_snapshot),
                        )
                        .await?;
                        return Err(error);
                    }
                    Err(error)
                        if error.provider_dispatch_outcome()
                            == ProviderDispatchOutcome::NotDispatched =>
                    {
                        self.commit_provider_dispatch_outcome_from_snapshot(
                            &session.id,
                            &turn.id,
                            &error,
                            true,
                            Some(&dispatch_snapshot),
                        )
                        .await?;
                        self.store
                            .settle_startup_operator_turn(&session.id, now_ms())?;
                        return Err(error);
                    }
                    Err(error) => {
                        let unknown = RuntimeError::provider_dispatch_unknown(
                            error
                                .provider_dispatch_code()
                                .unwrap_or("startup_provider_error"),
                            error.to_string(),
                        );
                        self.commit_provider_dispatch_outcome_from_snapshot(
                            &session.id,
                            &turn.id,
                            &unknown,
                            false,
                            Some(&dispatch_snapshot),
                        )
                        .await?;
                        return Err(unknown);
                    }
                }
            }
            Some(TurnDispatchState::Dispatched) => {
                self.spawn_wait_for_turn(provider_kind, session.id.clone(), turn.id.clone(), None);
                summary.resumed_waits += 1;
            }
            None if turn.status == "in_progress" => {
                self.spawn_wait_for_turn(provider_kind, session.id.clone(), turn.id.clone(), None);
                summary.resumed_waits += 1;
            }
            None if turn.status == "waiting_for_approval"
                && snapshot.approvals.iter().any(|approval| {
                    approval.turn_id == turn.id && approval.status == "pending"
                }) => {}
            _ => {
                return Err(RuntimeError::Conflict(
                    "attached turn has no supported startup continuation".into(),
                ))
            }
        }
        Ok(())
    }
}

fn approvals_for_turn(
    snapshot: &RetirementReviewSessionProjection,
    turn_id: &str,
) -> Vec<crate::ApprovalRecord> {
    let mut approvals = snapshot
        .approvals
        .iter()
        .filter(|row| row.turn_id == turn_id)
        .cloned()
        .collect::<Vec<_>>();
    approvals.sort_by(|left, right| left.id.cmp(&right.id));
    approvals
}
