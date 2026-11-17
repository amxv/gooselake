use std::collections::HashMap;

use super::MockStore;
use crate::*;

const CLAIM_KIND: &str = "operator_agent_restore_native";
const OPERATION_KIND: &str = "startup_session_reattach";
const EFFECT_KIND: &str = "provider_session_reattach";

#[derive(Default)]
pub(in crate::runtime) struct MockStartupLedger {
    pub(in crate::runtime) operations: HashMap<String, MockStartupOperation>,
    pub(in crate::runtime) keys: HashMap<(String, String), String>,
    pub(in crate::runtime) claims: HashMap<String, OperationResourceClaimRecord>,
    pub(in crate::runtime) generations: HashMap<String, u64>,
    pub(in crate::runtime) fail_next_receipt: bool,
}

#[derive(Clone)]
pub(in crate::runtime) struct MockStartupOperation {
    pub(in crate::runtime) grant: StartupReattachGrant,
    pub(in crate::runtime) actor: OperationActor,
    pub(in crate::runtime) kind: String,
    pub(in crate::runtime) effect_kind: String,
    pub(in crate::runtime) effect_id: String,
    pub(in crate::runtime) phase: OperationPhase,
    pub(in crate::runtime) effect_phase: String,
    pub(in crate::runtime) attempt_count: u64,
    pub(in crate::runtime) receipt: Option<StartupReattachReceipt>,
    pub(in crate::runtime) held_ack: Option<ProviderSession>,
    pub(in crate::runtime) diagnostic: Option<String>,
    pub(in crate::runtime) updated_at: i64,
}

fn conflict(message: &str) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}

fn hash_intent(intent: &StartupReattachIntent) -> Result<String, RuntimeError> {
    normalized_json_hash(
        &serde_json::to_value(intent)
            .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?,
    )
}

fn bound(entry: &MockStartupOperation, grant: &StartupReattachGrant) -> Result<(), RuntimeError> {
    if entry.grant != *grant
        || entry.kind != OPERATION_KIND
        || entry.effect_kind != EFFECT_KIND
        || entry.effect_id != format!("{}:resume", grant.intent.command.operation_id)
        || entry.attempt_count != 1
        || entry.actor.kind != OperationActorKind::System
        || entry.actor.identifier != "runtime-startup"
        || grant.intent.version != 1
        || grant.intent.command.provider != grant.intent.authority.provider
        || grant.intent.command.request != grant.intent.authority.request
        || grant.intent.command.session_id != grant.intent.authority.request.runtime_session_id
        || grant.normalized_request_hash != hash_intent(&grant.intent)?
        || grant.claim.resource_kind != CLAIM_KIND
        || grant.claim.claim_mode != "exclusive"
        || grant.claim.resource_id != grant.intent.command.session_id
        || grant.claim.owner_operation_id != grant.intent.command.operation_id
        || grant.claim.fence_generation == 0
    {
        return Err(conflict("mock startup original authority does not match"));
    }
    Ok(())
}

fn receipt(entry: &MockStartupOperation) -> Result<StartupReattachReceipt, RuntimeError> {
    bound(entry, &entry.grant)?;
    let receipt = entry
        .receipt
        .clone()
        .ok_or_else(|| conflict("mock completed startup lacks its receipt"))?;
    if entry.phase != OperationPhase::Completed
        || entry.effect_phase != "finalized"
        || receipt.operation_id != entry.grant.intent.command.operation_id
        || receipt.normalized_request_hash != entry.grant.normalized_request_hash
        || receipt.session_id != entry.grant.intent.command.session_id
        || receipt.provider != entry.grant.intent.authority.provider
        || !startup_ack_identity_matches(&entry.grant.intent, &receipt.acknowledgement)
        || receipt
            .metadata_terminal_event_id
            .as_deref()
            .is_some_and(|id| {
                entry
                    .grant
                    .intent
                    .terminal_metadata_witness
                    .as_ref()
                    .is_none_or(|witness| id != format!("evt_turn_{}_terminal", witness.turn.id))
            })
    {
        return Err(conflict(
            "mock startup terminal receipt contradicts its grant",
        ));
    }
    Ok(receipt)
}

fn require_claim(
    ledger: &MockStartupLedger,
    grant: &StartupReattachGrant,
) -> Result<(), RuntimeError> {
    if ledger.claims.get(&grant.intent.command.session_id) != Some(&grant.claim) {
        return Err(conflict("mock startup exclusive claim changed"));
    }
    Ok(())
}

fn guard_projection(snapshot: &RetirementReviewSessionProjection) -> Result<(), RuntimeError> {
    let turn_id = snapshot.session.active_turn_id.as_deref();
    let turn = snapshot
        .turns
        .iter()
        .find(|row| Some(row.id.as_str()) == turn_id);
    let admission = snapshot
        .admissions
        .iter()
        .find(|row| Some(row.turn_id.as_str()) == turn_id);
    let pending = snapshot
        .approvals
        .iter()
        .any(|row| row.status == "decision_pending");
    if let Some(reason) = startup_projection_refusal(&snapshot.session, turn, admission, pending) {
        return Err(conflict(reason));
    }
    Ok(())
}

fn authority(
    snapshot: &RetirementReviewSessionProjection,
) -> Result<StartupAttachmentAuthority, RuntimeError> {
    let (provider, request) = startup_resume_request_from_authority(&snapshot.session, None)?;
    Ok(StartupAttachmentAuthority {
        scope: StartupAttachmentScope::StoredSession { workspace_id: None },
        provider,
        request,
        worktree_id: snapshot.session.worktree_id.clone(),
    })
}

impl MockStore {
    // Caller holds authority_transaction; do not call trait hydration/getters here.
    pub(super) fn startup_snapshot_locked(
        &self,
        session_id: &str,
    ) -> Result<RetirementReviewSessionProjection, RuntimeError> {
        let session = self
            .sessions
            .lock()
            .unwrap()
            .get(session_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
        let turns = self
            .turns
            .lock()
            .unwrap()
            .values()
            .filter(|row| row.session_id == session_id)
            .cloned()
            .collect();
        let approvals = self
            .approvals
            .lock()
            .unwrap()
            .values()
            .filter(|row| row.session_id == session_id)
            .cloned()
            .collect();
        let admissions = self
            .turn_admissions
            .lock()
            .unwrap()
            .values()
            .filter(|row| row.session_id == session_id)
            .cloned()
            .collect();
        Ok(RetirementReviewSessionProjection {
            session,
            turns,
            approvals,
            admissions,
        })
    }

    pub(super) fn startup_resume_owner_locked(&self, session_id: &str) -> Option<String> {
        self.startup
            .lock()
            .unwrap()
            .claims
            .get(session_id)
            .map(|claim| claim.owner_operation_id.clone())
    }

    fn startup_witness_locked(
        &self,
        snapshot: &RetirementReviewSessionProjection,
    ) -> Result<Option<StartupTerminalMetadataWitness>, RuntimeError> {
        let Some(turn_id) = snapshot.session.active_turn_id.as_deref() else {
            return Ok(None);
        };
        let turn = snapshot
            .turns
            .iter()
            .find(|row| row.id == turn_id)
            .ok_or_else(|| conflict("mock startup active turn missing"))?;
        if turn.completed_at.is_some() {
            return Ok(None);
        }
        let admission = snapshot
            .admissions
            .iter()
            .find(|row| row.turn_id == turn_id)
            .cloned();
        match admission.as_ref() {
            Some(row) if row.dispatch_state != TurnDispatchState::Dispatched => return Ok(None),
            None if !matches!(turn.status.as_str(), "in_progress" | "waiting_for_approval") => {
                return Ok(None);
            }
            _ => {}
        }
        if self
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.event_id == format!("evt_turn_{turn_id}_terminal"))
        {
            return Err(conflict(
                "mock nonterminal witness already has a terminal receipt",
            ));
        }
        Ok(Some(StartupTerminalMetadataWitness {
            turn: turn.clone(),
            admission,
        }))
    }

    fn startup_metadata_event_locked(
        &self,
        grant: &StartupReattachGrant,
        current: &StartupAttachmentAuthority,
        snapshot: &RetirementReviewSessionProjection,
    ) -> Result<Option<String>, RuntimeError> {
        if current.request.metadata == grant.intent.authority.request.metadata {
            return startup_completion_metadata_event(grant, current, &snapshot.session, None);
        }
        let witness = grant
            .intent
            .terminal_metadata_witness
            .as_ref()
            .ok_or_else(|| conflict("mock metadata changed without a terminal witness"))?;
        let turn = snapshot
            .turns
            .iter()
            .find(|row| row.id == witness.turn.id)
            .ok_or_else(|| conflict("mock terminal witness turn missing"))?;
        let admission = snapshot
            .admissions
            .iter()
            .find(|row| row.turn_id == witness.turn.id);
        let events = self.events.lock().unwrap();
        let event = events
            .iter()
            .find(|row| row.event_id == format!("evt_turn_{}_terminal", witness.turn.id))
            .ok_or_else(|| conflict("mock terminal metadata has no exact event"))?;
        startup_completion_metadata_event(
            grant,
            current,
            &snapshot.session,
            Some(StartupTerminalMetadataEvidence {
                turn,
                admission,
                event,
            }),
        )
    }

    pub(super) fn startup_settle_parent_for_test(
        &self,
        session_id: &str,
        _at_ms: i64,
    ) -> Result<Option<StartupPendingOperatorTurn>, RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        self.startup_snapshot_locked(session_id)?;
        // OperatorUserTurnStore remains Unsupported on this ordinary-session
        // mock: no operator parent can be admitted here. Real SQLite tests own
        // parent settlement/owner exception coverage.
        Ok(None)
    }

    pub(super) fn startup_admit_for_test(
        &self,
        command: &StartupReattachCommand,
        at_ms: i64,
    ) -> Result<StartupReattachAdmission, RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        for value in [
            &command.operation_id,
            &command.startup_instance_id,
            &command.session_id,
        ] {
            if value.trim().is_empty() || value.trim() != value {
                return Err(conflict("mock startup command has invalid identity"));
            }
        }
        if command.request.runtime_session_id != command.session_id {
            return Err(conflict("mock startup request targets another session"));
        }
        let mut ledger = self.startup.lock().unwrap();
        let key = (
            command.startup_instance_id.clone(),
            command.session_id.clone(),
        );
        if let Some(operation_id) = ledger.keys.get(&key) {
            let entry = ledger
                .operations
                .get(operation_id)
                .ok_or_else(|| conflict("mock startup key lacks its operation"))?;
            bound(entry, &entry.grant)?;
            if entry.grant.intent.command != *command {
                return Err(conflict("mock startup retry changed the frozen command"));
            }
            return if entry.phase == OperationPhase::Completed {
                Ok(StartupReattachAdmission::Replay(receipt(entry)?))
            } else {
                Ok(StartupReattachAdmission::Unresolved {
                    operation_id: operation_id.clone(),
                })
            };
        }
        if ledger.operations.contains_key(&command.operation_id) {
            return Err(conflict("mock startup operation identity already exists"));
        }
        if let Some(entry) = ledger.operations.values().find(|entry| {
            entry.grant.intent.command.session_id == command.session_id
                && matches!(
                    entry.phase,
                    OperationPhase::Requested | OperationPhase::ManualReview
                )
        }) {
            return Ok(StartupReattachAdmission::Unresolved {
                operation_id: entry.grant.intent.command.operation_id.clone(),
            });
        }
        if let Some(claim) = ledger.claims.get(&command.session_id) {
            return Ok(StartupReattachAdmission::Unresolved {
                operation_id: claim.owner_operation_id.clone(),
            });
        }
        let snapshot = self.startup_snapshot_locked(&command.session_id)?;
        guard_projection(&snapshot)?;
        let authority = authority(&snapshot)?;
        if authority.provider != command.provider || authority.request != command.request {
            return Err(conflict("mock startup request changed before grant"));
        }
        let witness = self.startup_witness_locked(&snapshot)?;
        let generation = ledger
            .generations
            .get(&command.session_id)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .filter(|generation| *generation <= i64::MAX as u64)
            .ok_or_else(|| conflict("mock startup claim generation exhausted"))?;
        let intent = StartupReattachIntent {
            version: 1,
            command: command.clone(),
            authority,
            pending_operator_turn: None,
            terminal_metadata_witness: witness,
        };
        let claim = OperationResourceClaimRecord {
            resource_kind: CLAIM_KIND.into(),
            resource_id: command.session_id.clone(),
            claim_mode: "exclusive".into(),
            owner_operation_id: command.operation_id.clone(),
            fence_generation: generation,
            acquired_at: at_ms,
        };
        let grant = StartupReattachGrant {
            normalized_request_hash: hash_intent(&intent)?,
            intent,
            claim: claim.clone(),
        };
        let entry = MockStartupOperation {
            grant: grant.clone(),
            actor: OperationActor {
                kind: OperationActorKind::System,
                identifier: "runtime-startup".into(),
            },
            kind: OPERATION_KIND.into(),
            effect_kind: EFFECT_KIND.into(),
            effect_id: format!("{}:resume", command.operation_id),
            phase: OperationPhase::Requested,
            effect_phase: "started".into(),
            attempt_count: 1,
            receipt: None,
            held_ack: None,
            diagnostic: None,
            updated_at: at_ms,
        };
        ledger
            .generations
            .insert(command.session_id.clone(), generation);
        ledger.claims.insert(command.session_id.clone(), claim);
        ledger.keys.insert(key, command.operation_id.clone());
        ledger
            .operations
            .insert(command.operation_id.clone(), entry);
        Ok(StartupReattachAdmission::Execute(grant))
    }

    pub(super) fn startup_complete_for_test(
        &self,
        grant: &StartupReattachGrant,
        ack: &ProviderSession,
        at_ms: i64,
    ) -> Result<StartupReattachCompletion, RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        let mut ledger = self.startup.lock().unwrap();
        let operation_id = &grant.intent.command.operation_id;
        let mut entry = ledger
            .operations
            .get(operation_id)
            .cloned()
            .ok_or_else(|| conflict("mock startup operation missing"))?;
        bound(&entry, grant)?;
        if entry.phase == OperationPhase::Completed {
            let receipt = receipt(&entry)?;
            if !startup_ack_is_identical(&receipt.acknowledgement, ack) {
                return Err(conflict("mock repeated completion changed the ACK"));
            }
            let session = self.startup_snapshot_locked(&receipt.session_id)?.session;
            return Ok(StartupReattachCompletion { receipt, session });
        }
        if entry.phase != OperationPhase::Requested || entry.effect_phase != "started" {
            return Err(conflict("mock startup attempt is not completable"));
        }
        require_claim(&ledger, grant)?;
        if !startup_ack_identity_matches(&grant.intent, ack) {
            return Err(conflict("mock startup native ACK identity mismatch"));
        }
        let snapshot = self.startup_snapshot_locked(&grant.intent.command.session_id)?;
        guard_projection(&snapshot)?;
        let current = authority(&snapshot)?;
        let metadata_terminal_event_id =
            self.startup_metadata_event_locked(grant, &current, &snapshot)?;
        if ledger.fail_next_receipt {
            ledger.fail_next_receipt = false;
            return Err(RuntimeError::Io("injected startup receipt failure".into()));
        }
        let at_ms = at_ms.max(entry.updated_at);
        let receipt = StartupReattachReceipt {
            operation_id: operation_id.clone(),
            normalized_request_hash: grant.normalized_request_hash.clone(),
            session_id: snapshot.session.id.clone(),
            provider: grant.intent.authority.provider,
            acknowledgement: ack.clone(),
            acknowledged_at: at_ms,
            metadata_terminal_event_id,
        };
        let mut session = snapshot.session;
        let canonical_fill = session.canonical_provider_session_ref.is_none()
            && ack.canonical_provider_session_ref.is_some();
        if canonical_fill && *self.fail_session_write.lock().unwrap() {
            return Err(RuntimeError::Io(
                "forced startup canonical write rejection".into(),
            ));
        }
        if canonical_fill {
            session.canonical_provider_session_ref = ack.canonical_provider_session_ref.clone();
            session.updated_at = session.updated_at.max(at_ms);
        }
        entry.phase = OperationPhase::Completed;
        entry.effect_phase = "finalized".into();
        entry.receipt = Some(receipt.clone());
        entry.updated_at = at_ms;
        if canonical_fill {
            self.sessions
                .lock()
                .unwrap()
                .insert(session.id.clone(), session.clone());
        }
        ledger.operations.insert(operation_id.clone(), entry);
        ledger.claims.remove(&grant.intent.command.session_id);
        let pause = self.startup_after_complete.lock().unwrap().clone();
        if let Some(pause) = pause {
            pause.pause();
        }
        if std::mem::take(&mut *self.fail_next_startup_return_after_commit.lock().unwrap()) {
            return Err(RuntimeError::Io(
                "injected lost startup completion return after durable commit".into(),
            ));
        }
        Ok(StartupReattachCompletion { receipt, session })
    }

    pub(super) fn startup_hold_for_test(
        &self,
        grant: &StartupReattachGrant,
        ack: Option<&ProviderSession>,
        diagnostic: &str,
        at_ms: i64,
    ) -> Result<StartupReattachHold, RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        let mut ledger = self.startup.lock().unwrap();
        let operation_id = &grant.intent.command.operation_id;
        let mut entry = ledger
            .operations
            .get(operation_id)
            .cloned()
            .ok_or_else(|| conflict("mock startup operation missing"))?;
        bound(&entry, grant)?;
        if entry.phase == OperationPhase::Completed {
            let receipt = receipt(&entry)?;
            let session = self.startup_snapshot_locked(&receipt.session_id)?.session;
            return Ok(StartupReattachHold::Completed(StartupReattachCompletion {
                receipt,
                session,
            }));
        }
        require_claim(&ledger, grant)?;
        if entry.phase == OperationPhase::ManualReview {
            if !matches!(entry.effect_phase.as_str(), "observed" | "uncertain") {
                return Err(conflict("mock held startup effect has invalid phase"));
            }
            return Ok(StartupReattachHold::Held {
                operation_id: operation_id.clone(),
            });
        }
        if entry.phase != OperationPhase::Requested || entry.effect_phase != "started" {
            return Err(conflict("mock startup attempt cannot enter hold"));
        }
        entry.phase = OperationPhase::ManualReview;
        entry.effect_phase =
            if ack.is_some_and(|ack| startup_ack_identity_matches(&grant.intent, ack)) {
                "observed"
            } else {
                "uncertain"
            }
            .into();
        entry.held_ack = ack.cloned();
        entry.diagnostic = Some(diagnostic.chars().take(400).collect());
        entry.updated_at = at_ms.max(entry.updated_at);
        ledger.operations.insert(operation_id.clone(), entry);
        Ok(StartupReattachHold::Held {
            operation_id: operation_id.clone(),
        })
    }

    pub(super) fn write_admission_for_test(
        &self,
        requested: &TurnAdmissionRecord,
    ) -> Result<(), RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        let mut admissions = self.turn_admissions.lock().unwrap();
        if requested.dispatch_state == TurnDispatchState::Dispatching {
            let current = admissions
                .get(&requested.turn_id)
                .ok_or_else(|| conflict("mock first dispatch lacks its original admission"))?;
            if current.dispatch_state != TurnDispatchState::Pending
                || current.provider_native_turn_id.is_some()
            {
                return Err(RuntimeError::TurnDispatchNotClaimed {
                    turn_id: requested.turn_id.clone(),
                    blocking_operation_id: None,
                });
            }
            let mut expected = current.clone();
            expected.dispatch_state = TurnDispatchState::Dispatching;
            expected.dispatch_error = None;
            expected.updated_at = requested.updated_at;
            if expected != *requested {
                return Err(conflict("mock first dispatch changed immutable admission"));
            }
            if let Some(operation_id) = self.startup_resume_owner_locked(&requested.session_id) {
                return Err(RuntimeError::TurnDispatchNotClaimed {
                    turn_id: requested.turn_id.clone(),
                    blocking_operation_id: Some(operation_id),
                });
            }
        }
        admissions.insert(requested.turn_id.clone(), requested.clone());
        Ok(())
    }
}

macro_rules! mock_startup_methods {
    () => {
        fn settle_startup_operator_turn(
            &self,
            session_id: &str,
            at_ms: i64,
        ) -> Result<Option<crate::StartupPendingOperatorTurn>, crate::RuntimeError> {
            self.startup_settle_parent_for_test(session_id, at_ms)
        }

        fn admit_startup_reattach(
            &self,
            command: &crate::StartupReattachCommand,
            at_ms: i64,
        ) -> Result<crate::StartupReattachAdmission, crate::RuntimeError> {
            self.startup_admit_for_test(command, at_ms)
        }

        fn complete_startup_reattach(
            &self,
            grant: &crate::StartupReattachGrant,
            ack: &crate::ProviderSession,
            at_ms: i64,
        ) -> Result<crate::StartupReattachCompletion, crate::RuntimeError> {
            self.startup_complete_for_test(grant, ack, at_ms)
        }

        fn hold_startup_reattach(
            &self,
            grant: &crate::StartupReattachGrant,
            ack: Option<&crate::ProviderSession>,
            reason: &str,
            at_ms: i64,
        ) -> Result<crate::StartupReattachHold, crate::RuntimeError> {
            self.startup_hold_for_test(grant, ack, reason, at_ms)
        }

        fn unresolved_operator_agent_restore_for_agent(
            &self,
            session_id: &str,
        ) -> Result<Option<String>, crate::RuntimeError> {
            let _authority = self.authority_transaction.lock().unwrap();
            Ok(self.startup_resume_owner_locked(session_id))
        }

        fn get_persisted_retirement_review_session(
            &self,
            session_id: &str,
        ) -> Result<Option<crate::RetirementReviewSessionProjection>, crate::RuntimeError> {
            let _authority = self.authority_transaction.lock().unwrap();
            match self.startup_snapshot_locked(session_id) {
                Ok(snapshot) => Ok(Some(snapshot)),
                Err(crate::RuntimeError::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }

        fn get_persisted_retirement_settled_turn(
            &self,
            turn_id: &str,
        ) -> Result<Option<crate::RetirementSettledTurnProjection>, crate::RuntimeError> {
            let _authority = self.authority_transaction.lock().unwrap();
            let Some(turn) = self.turns.lock().unwrap().get(turn_id).cloned() else {
                return Ok(None);
            };
            let session = self.sessions.lock().unwrap().get(&turn.session_id).cloned();
            let approvals = self
                .approvals
                .lock()
                .unwrap()
                .values()
                .filter(|row| row.turn_id == turn_id)
                .cloned()
                .collect();
            let admission = self.turn_admissions.lock().unwrap().get(turn_id).cloned();
            Ok(Some(crate::RetirementSettledTurnProjection {
                turn,
                session,
                approvals,
                admission,
            }))
        }
    };
}
pub(super) use mock_startup_methods;
