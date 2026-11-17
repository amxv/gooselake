//! Test-only edit authority. Real SQLite rollback and witness tests live in
//! runtime-store-sqlite; this ledger controls runtime grant/cache interleavings.
use super::MockStore;
use crate::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum EditReceiptRace {
    None,
    AdmissionReplay,
    NativeStartReplay,
}

pub(in crate::runtime) struct MockEditFixture {
    pub agent: WorkspaceAgentRecord,
    pub operation: Option<SessionEditRerunOperation>,
    pub race: EditReceiptRace,
    pub peer_committed: bool,
}

fn unchanged(operation: SessionEditRerunOperation) -> EditRerunTransitionReceipt {
    EditRerunTransitionReceipt {
        operation,
        changed: false,
        journal_event: None,
        committed_session: None,
    }
}

impl MockStore {
    fn edit_commit_locked(
        &self,
        fixture: &mut MockEditFixture,
        operation: SessionEditRerunOperation,
        before: Option<&str>,
        session: Option<SessionRecord>,
    ) -> Result<EditRerunTransitionReceipt, RuntimeError> {
        let kind = match operation.phase.as_str() {
            "intended" => "turn.edit_admitted",
            "native_started" => "turn.edit_native_started",
            "fork_observed" => "turn.edit_fork_observed",
            "completed" => "turn.edit_completed",
            "manual_review" => "turn.edit_manual_review",
            _ => {
                return Err(RuntimeError::Conflict(
                    "unsupported mock edit transition".into(),
                ))
            }
        };
        let owner = SessionControlCoordinate::EditRerun {
            agent_id: operation.agent_id.clone(),
            operation_id: operation.operation_id.clone(),
        };
        let event_id = format!(
            "evt_edit_state_{}",
            normalized_json_hash(&serde_json::json!({"owner":owner,"phase":operation.phase}),)?
        );
        if self
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.event_id == event_id)
        {
            return Err(RuntimeError::Conflict("mock edit event collision".into()));
        }
        let event = self.append_runtime_event_locked(&NewRuntimeEvent {
            event_id, scope: RuntimeEventScope::Session,
            scope_id: operation.agent_id.clone(), session_id: Some(operation.agent_id.clone()),
            team_id: None, turn_id: Some(operation.target_turn_id.clone()),
            kind: kind.into(), criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({
                "owner":owner,"operation_id":operation.operation_id,
                "target_turn_id":operation.target_turn_id,"phase":operation.phase,
                "previous_phase":before,"origin":"request",
                "child_provider_session_ref":operation.child_provider_session_ref,
                "child_canonical_provider_session_ref":operation.child_canonical_provider_session_ref,
                "replacement_turn_id":operation.replacement_turn_id,
                "error_code":operation.error_code,"review_evidence":operation.review_evidence,
            }),
            provider: None, provider_seq: None, created_at: operation.updated_at_ms,
        })?;
        if let Some(session) = session.as_ref() {
            fixture.agent.provider_session_ref = session.provider_session_ref.clone();
            fixture.agent.canonical_provider_session_ref =
                session.canonical_provider_session_ref.clone();
            fixture.agent.metadata = session.metadata.clone();
            fixture.agent.updated_at = session.updated_at;
            fixture.agent.revision += 1;
            self.sessions
                .lock()
                .unwrap()
                .insert(session.id.clone(), session.clone());
        }
        fixture.operation = Some(operation.clone());
        Ok(EditRerunTransitionReceipt {
            operation,
            changed: true,
            journal_event: Some(event),
            committed_session: session,
        })
    }

    pub(super) fn edit_begin_for_test(
        &self,
        proposed: &SessionEditRerunOperation,
    ) -> Result<EditRerunTransitionReceipt, RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        let mut slot = self.edit_fixture.lock().unwrap();
        let fixture = slot
            .as_mut()
            .ok_or_else(|| RuntimeError::Unsupported("edit fixture absent".into()))?;
        proposed.validate()?;
        if let Some(current) = fixture.operation.as_ref() {
            if current.agent_id == proposed.agent_id
                && current.idempotency_key == proposed.idempotency_key
                && current.request_hash == proposed.request_hash
            {
                return Ok(unchanged(current.clone()));
            }
            return Err(RuntimeError::Conflict(
                "another mock edit owns this session".into(),
            ));
        }
        let session = self
            .sessions
            .lock()
            .unwrap()
            .get(&proposed.agent_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound("mock edit session".into()))?;
        if proposed.phase != "intended"
            || proposed.child_provider_session_ref.is_some()
            || proposed.replacement_turn_id.is_some()
            || session.status != "ready"
            || session.active_turn_id.is_some()
            || session.provider_session_ref.as_deref()
                != Some(proposed.source_provider_session_ref.as_str())
            || session.canonical_provider_session_ref
                != proposed.source_canonical_provider_session_ref
            || session.updated_at != proposed.expected_session_updated_at
            || fixture.agent.revision != proposed.agent_revision
        {
            return Err(RuntimeError::Conflict(
                "mock original edit authority changed".into(),
            ));
        }
        let committed = self.edit_commit_locked(fixture, proposed.clone(), None, None)?;
        if fixture.race == EditReceiptRace::AdmissionReplay && !fixture.peer_committed {
            // Model a peer's one durable winner, then the caller's exact lookup.
            // The saved authority/event remain real; this caller gets no grant.
            fixture.peer_committed = true;
            return Ok(unchanged(committed.operation));
        }
        Ok(committed)
    }

    pub(super) fn edit_advance_for_test(
        &self,
        id: &str,
        phase: &str,
        child: Option<&ProviderSession>,
        replacement: Option<&str>,
        code: Option<&str>,
        now: i64,
    ) -> Result<EditRerunTransitionReceipt, RuntimeError> {
        let _authority = self.authority_transaction.lock().unwrap();
        let mut slot = self.edit_fixture.lock().unwrap();
        let fixture = slot
            .as_mut()
            .ok_or_else(|| RuntimeError::Unsupported("edit fixture absent".into()))?;
        let mut current = fixture
            .operation
            .clone()
            .filter(|op| op.operation_id == id)
            .ok_or_else(|| RuntimeError::NotFound(format!("mock edit {id}")))?;
        if phase == "native_started"
            && current.phase == "intended"
            && fixture.race == EditReceiptRace::NativeStartReplay
            && !fixture.peer_committed
        {
            let mut peer = current.clone();
            peer.phase = "native_started".into();
            peer.updated_at_ms = now.max(peer.updated_at_ms);
            current = self
                .edit_commit_locked(fixture, peer, Some("intended"), None)?
                .operation;
            fixture.peer_committed = true;
        }
        if current.phase == phase {
            if current.error_code.as_deref() != code
                || current.replacement_turn_id.as_deref() != replacement
                || (phase == "fork_observed"
                    && !child.is_some_and(|child| {
                        child.runtime_session_id == current.agent_id
                            && current.child_provider_session_ref.as_deref()
                                == Some(child.provider_session_ref.as_str())
                            && current.child_canonical_provider_session_ref
                                == child.canonical_provider_session_ref
                    }))
            {
                return Err(RuntimeError::Conflict(
                    "conflicting mock edit replay".into(),
                ));
            }
            return Ok(unchanged(current));
        }
        let before = current.phase.clone();
        if !matches!(
            (before.as_str(), phase),
            ("intended", "native_started" | "manual_review")
                | ("native_started", "fork_observed" | "manual_review")
                | ("fork_observed", "completed" | "manual_review")
        ) {
            return Err(RuntimeError::Conflict(
                "invalid mock edit transition".into(),
            ));
        }
        let mut committed_session = None;
        if phase == "fork_observed" {
            let child = child.ok_or_else(|| RuntimeError::Conflict("mock child missing".into()))?;
            let mut session = self
                .sessions
                .lock()
                .unwrap()
                .get(&current.agent_id)
                .cloned()
                .unwrap();
            if child.runtime_session_id != current.agent_id
                || child.provider_session_ref.is_empty()
                || child.provider_session_ref == current.source_provider_session_ref
                || session.provider_session_ref.as_deref()
                    != Some(current.source_provider_session_ref.as_str())
                || session.canonical_provider_session_ref
                    != current.source_canonical_provider_session_ref
                || session.updated_at != current.expected_session_updated_at
                || session.status != "ready"
                || session.active_turn_id.is_some()
            {
                return Err(RuntimeError::Conflict(
                    "mock child receipt lost original authority".into(),
                ));
            }
            current.child_provider_session_ref = Some(child.provider_session_ref.clone());
            current.child_canonical_provider_session_ref =
                child.canonical_provider_session_ref.clone();
            session.provider_session_ref = current.child_provider_session_ref.clone();
            session.canonical_provider_session_ref =
                current.child_canonical_provider_session_ref.clone();
            session.updated_at = now.max(session.updated_at + 1);
            session.metadata["session_transcript"] = serde_json::json!([]);
            committed_session = Some(session);
        }
        if phase == "completed" {
            let admissions = self.turn_admissions.lock().unwrap();
            let candidates: Vec<_> = admissions
                .values()
                .filter(|a| {
                    a.session_id == current.agent_id
                        && a.correlation.correlation_id.as_deref() == Some(id)
                })
                .collect();
            if candidates.len() != 1
                || Some(candidates[0].turn_id.as_str()) != replacement
                || candidates[0].user_input_snapshot != current.edited_snapshot
                || !self.events.lock().unwrap().iter().any(|event| {
                    event.event_id == format!("evt_turn_{}_admitted", candidates[0].turn_id)
                        && event.kind == "turn.admitted"
                        && event.criticality == RuntimeEventCriticality::Critical
                })
            {
                return Err(RuntimeError::Conflict(
                    "mock exact replacement not proved".into(),
                ));
            }
            current.replacement_turn_id = replacement.map(str::to_owned);
        }
        current.phase = phase.into();
        current.error_code = code.map(str::to_owned);
        current.updated_at_ms = now.max(current.updated_at_ms);
        let receipt =
            self.edit_commit_locked(fixture, current, Some(&before), committed_session)?;
        drop(slot);
        if phase == "fork_observed" {
            if let Some(pause) = self.edit_after_fork.lock().unwrap().clone() {
                // Preserve the synchronous store-return interval deterministically.
                pause.pause();
            }
        }
        Ok(receipt)
    }
}

macro_rules! mock_edit_receipt_methods {
    () => {
        fn get_workspace_agent_by_id(
            &self,
            id: &str,
        ) -> Result<Option<crate::WorkspaceAgentRecord>, crate::RuntimeError> {
            let _authority = self.authority_transaction.lock().unwrap();
            Ok(self
                .edit_fixture
                .lock()
                .unwrap()
                .as_ref()
                .filter(|f| f.agent.agent_id == id)
                .map(|f| f.agent.clone()))
        }
        fn get_session_edit_rerun(
            &self,
            id: &str,
        ) -> Result<Option<crate::SessionEditRerunOperation>, crate::RuntimeError> {
            let _authority = self.authority_transaction.lock().unwrap();
            Ok(self
                .edit_fixture
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|f| f.operation.clone())
                .filter(|op| op.operation_id == id))
        }
        fn get_session_edit_rerun_by_key(
            &self,
            agent: &str,
            key: &str,
        ) -> Result<Option<crate::SessionEditRerunOperation>, crate::RuntimeError> {
            let _authority = self.authority_transaction.lock().unwrap();
            Ok(self
                .edit_fixture
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|f| f.operation.clone())
                .filter(|op| op.agent_id == agent && op.idempotency_key.as_deref() == Some(key)))
        }
        fn unresolved_session_edit_rerun(&self, agent: &str) -> Result<bool, crate::RuntimeError> {
            let _authority = self.authority_transaction.lock().unwrap();
            Ok(self
                .edit_fixture
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|f| f.operation.as_ref())
                .is_some_and(|op| op.agent_id == agent && op.unresolved()))
        }
        fn begin_session_edit_rerun(
            &self,
            operation: &crate::SessionEditRerunOperation,
        ) -> Result<crate::EditRerunTransitionReceipt, crate::RuntimeError> {
            self.edit_begin_for_test(operation)
        }
        fn advance_session_edit_rerun(
            &self,
            id: &str,
            phase: &str,
            child: Option<&crate::ProviderSession>,
            replacement: Option<&str>,
            code: Option<&str>,
            now: i64,
        ) -> Result<crate::EditRerunTransitionReceipt, crate::RuntimeError> {
            self.edit_advance_for_test(id, phase, child, replacement, code, now)
        }
    };
}
pub(super) use mock_edit_receipt_methods;
