use super::{helpers::now_ms, RuntimeSessionManager};
use crate::{
    ProviderKind, ProviderSession, RetirementReviewSessionProjection, RuntimeError,
    StartupReattachAdmission, StartupReattachCommand, StartupReattachGrant, StartupReattachHold,
    TurnDispatchState,
};
impl RuntimeSessionManager {
    pub(super) async fn refresh_startup_session(
        &self,
        session_id: &str,
    ) -> Result<RetirementReviewSessionProjection, RuntimeError> {
        let mut turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        let mut approvals = self.approvals.write().await;
        let mut admissions = self.turn_admissions.write().await;
        let snapshot = self
            .store
            .get_persisted_retirement_review_session(session_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
        turns.retain(|_, t| t.session_id != session_id);
        approvals.retain(|_, a| a.session_id != session_id);
        admissions.retain(|_, a| a.session_id != session_id);
        sessions.insert(session_id.into(), snapshot.session.clone());
        for turn in &snapshot.turns {
            turns.insert(turn.id.clone(), turn.clone());
        }
        for approval in &snapshot.approvals {
            approvals.insert(approval.id.clone(), approval.clone());
        }
        for admission in &snapshot.admissions {
            admissions.insert(admission.turn_id.clone(), admission.clone());
        }
        Ok(snapshot)
    }
    async fn startup_command(
        &self,
        snapshot: &RetirementReviewSessionProjection,
    ) -> Result<StartupReattachCommand, RuntimeError> {
        let mut commands = self.startup_commands.lock().await;
        if let Some(command) = commands.get(&snapshot.session.id) {
            return Ok(command.clone());
        }
        let agent = self.store.get_workspace_agent_by_id(&snapshot.session.id)?;
        let (provider, request) =
            crate::startup_resume_request_from_authority(&snapshot.session, agent.as_ref())?;
        let key = crate::normalized_json_hash(
            &serde_json::json!({"startup_instance_id":self.startup_instance_id,"session_id":snapshot.session.id}),
        )?;
        let command = StartupReattachCommand {
            operation_id: format!("op_startup_{key}"),
            startup_instance_id: self.startup_instance_id.clone(),
            session_id: snapshot.session.id.clone(),
            provider,
            request,
        };
        commands.insert(snapshot.session.id.clone(), command.clone());
        Ok(command)
    }
    fn hold_startup_attempt(
        &self,
        grant: &StartupReattachGrant,
        acknowledgement: Option<&ProviderSession>,
        diagnostic: &str,
    ) -> RuntimeError {
        let detail=match self.store.hold_startup_reattach(grant,acknowledgement,diagnostic,now_ms()){
   Ok(StartupReattachHold::Held{..})=>diagnostic.to_owned(),
   Ok(StartupReattachHold::Completed(_))=>format!("{diagnostic}; the original receipt is already complete; no native call was repeated"),
   Err(error)=>format!("{diagnostic}; hold persistence also failed: {error}; the original started claim remains authoritative"),
  };
        RuntimeError::operation_unresolved(grant.intent.command.operation_id.clone(), detail)
    }
    pub(super) async fn attach_startup_session(
        &self,
        snapshot: &RetirementReviewSessionProjection,
    ) -> Result<Option<ProviderKind>, RuntimeError> {
        let command = self.startup_command(snapshot).await?;
        let provider = self
            .providers
            .get(command.provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(command.provider.as_str().into()))?;
        let grant = match self.store.admit_startup_reattach(&command, now_ms())? {
            StartupReattachAdmission::Execute(grant) => grant,
            StartupReattachAdmission::Replay(_) => return Ok(None),
            StartupReattachAdmission::Unresolved { operation_id } => {
                return Err(RuntimeError::operation_unresolved(
                    operation_id,
                    "the original startup attachment attempt is unresolved",
                ))
            }
        };
        let ack = match provider
            .resume_session_with_policy(grant.intent.authority.request.clone())
            .await
        {
            Ok(ack) => ack,
            Err(error) => return Err(self.hold_startup_attempt(&grant, None, &error.to_string())),
        };
        if !crate::startup_ack_identity_matches(&grant.intent, &ack) {
            return Err(self.hold_startup_attempt(
                &grant,
                Some(&ack),
                "native startup acknowledgement changed the frozen attachment identity",
            ));
        }
        // Mapping restoration is local adapter state and runs while claim is held.
        if let Some(witness) = grant.intent.terminal_metadata_witness.as_ref() {
            if let Some(admission) = witness.admission.as_ref() {
                if admission.dispatch_state == TurnDispatchState::Dispatched {
                    if let Some(native_id) = admission.provider_native_turn_id.as_deref() {
                        let latest = self
                            .store
                            .get_persisted_retirement_settled_turn(&witness.turn.id);
                        let active = match latest {
                            Ok(Some(current)) => {
                                current.turn.completed_at.is_none()
                                    && current.session.as_ref().is_some_and(|s| {
                                        s.active_turn_id.as_deref()
                                            == Some(witness.turn.id.as_str())
                                    })
                            }
                            Ok(None) => return Err(self.hold_startup_attempt(
                                &grant,
                                Some(&ack),
                                "original dispatched turn disappeared during startup attachment",
                            )),
                            Err(error) => {
                                return Err(self.hold_startup_attempt(
                                    &grant,
                                    Some(&ack),
                                    &error.to_string(),
                                ))
                            }
                        };
                        if active {
                            if let Err(error) = provider
                                .restore_turn_identity_mapping(
                                    &command.session_id,
                                    &witness.turn.id,
                                    native_id,
                                )
                                .await
                            {
                                return Err(self.hold_startup_attempt(
                                    &grant,
                                    Some(&ack),
                                    &format!(
                                        "startup native turn identity restoration failed: {error}"
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
        // Serialize the committed canonical fill with local terminal callbacks.
        // The provider and mapping awaits are already complete; no native await
        // occurs while these cache guards are held.
        let _turns = self.turns.write().await;
        let mut sessions = self.sessions.write().await;
        match self.store.complete_startup_reattach(&grant,&ack,now_ms()){
   Ok(completed)=>{
    sessions.insert(completed.session.id.clone(), completed.session);
    Ok(Some(command.provider))
   },
   Err(error)=>match self.store.hold_startup_reattach(&grant,Some(&ack),&error.to_string(),now_ms()){
    Ok(StartupReattachHold::Completed(completed)) if crate::startup_ack_is_identical(&completed.receipt.acknowledgement,&ack)=>{
     sessions.insert(completed.session.id.clone(), completed.session);
     Ok(Some(command.provider))
    },
    Ok(_)=>Err(RuntimeError::operation_unresolved(command.operation_id,format!("startup ACK observed but completion is unresolved: {error}"))),
    Err(hold)=>Err(RuntimeError::operation_unresolved(command.operation_id,format!("startup ACK observed; completion failed ({error}); hold failed ({hold}); original claim remains"))),
   },
  }
    }
}
