use super::support::*;
use crate::db::open_connection;
use crate::tests::legacy_team_delivery_claim::claim_plan;
use crate::tests::legacy_team_delivery_result::plan as legacy_result;
use crate::tests::legacy_team_messages::{commit as legacy_admit, proposed as legacy_proposed};
use crate::SqliteRuntimeRepository;
use runtime_core::{
    AgentDeliveryRecord, AgentMessageContextKind, AgentMessageRecord, ApprovalRecord, RuntimeError,
    StartupReattachAdmission, TeamDeliveryRecord, TeamMemberRecord, TeamRecord, TurnDispatchState,
    TurnInputProjectionSource,
};
use serde_json::json;
use std::sync::{Arc, Barrier};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Agent,
    Legacy,
}

#[derive(Clone)]
enum Claim {
    Agent {
        previous: AgentDeliveryRecord,
        injecting: AgentDeliveryRecord,
    },
    Legacy {
        previous: TeamDeliveryRecord,
        injecting: TeamDeliveryRecord,
    },
}

impl Claim {
    fn id(&self) -> &str {
        match self {
            Self::Agent { injecting, .. } => &injecting.id,
            Self::Legacy { injecting, .. } => &injecting.id,
        }
    }
    fn message(&self) -> &str {
        match self {
            Self::Agent { injecting, .. } => &injecting.message_id,
            Self::Legacy { injecting, .. } => &injecting.message_id,
        }
    }
    fn claim(&self, repository: &SqliteRuntimeRepository) -> Result<bool, RuntimeError> {
        match self {
            Self::Agent {
                previous,
                injecting,
            } => repository.compare_and_swap_agent_delivery_status(previous, injecting),
            Self::Legacy {
                previous,
                injecting,
            } => {
                let (_, event) = claim_plan(previous, injecting.updated_at, "test");
                repository.claim_legacy_team_delivery_injection(previous, injecting, &event)
            }
        }
    }
    fn verify(
        &self,
        repository: &SqliteRuntimeRepository,
        running: &TurnFixture,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Agent { injecting, .. } => repository.verify_agent_delivery_interrupt_owner(
                injecting,
                &running.session,
                &running.turn,
            ),
            Self::Legacy { injecting, .. } => repository.verify_legacy_delivery_interrupt_owner(
                injecting,
                &running.session,
                &running.turn,
            ),
        }
    }
    fn commit_result(
        &self,
        repository: &SqliteRuntimeRepository,
        turn_id: &str,
    ) -> serde_json::Value {
        match self {
            Self::Agent { injecting, .. } => serde_json::to_value(
                repository
                    .commit_agent_delivery_injected(injecting, turn_id, 2400)
                    .unwrap(),
            )
            .unwrap(),
            Self::Legacy { injecting, .. } => {
                let (after, event) = legacy_result(injecting, turn_id, 2400);
                serde_json::to_value(
                    repository
                        .commit_legacy_team_delivery_injected(injecting, &after, &event)
                        .unwrap(),
                )
                .unwrap()
            }
        }
    }
}

fn seed(fixture: &Fixture, mode: Mode, policy: &str) -> Claim {
    let repository = &fixture.repository;
    let mut sender = fixture.session();
    sender.id = "session_lead".into();
    sender.provider_session_ref = Some("native-delivery-sender".into());
    sender.active_turn_id = None;
    sender.status = "ready".into();
    repository.upsert_session(&sender).unwrap();
    match mode {
        Mode::Agent => {
            let message = AgentMessageRecord {
                id: "startup_delivery_message".into(),
                scope: "direct".into(),
                context_kind: AgentMessageContextKind::GlobalDirect,
                workspace_id: None,
                legacy_team_id: None,
                sender_agent_id: sender.id,
                recipient_agent_ids: vec![SESSION.into()],
                input: json!([{"type":"text","text":"durable delivery"}]),
                image_paths: vec![],
                priority: "normal".into(),
                policy: policy.into(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("original-delivery-key".into()),
                created_at: 1100,
            };
            let previous = AgentDeliveryRecord {
                id: "startup_delivery".into(),
                message_id: message.id.clone(),
                recipient_agent_id: SESSION.into(),
                provider: "codex".into(),
                status: "pending".into(),
                effective_policy: Some(policy.into()),
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: None,
                last_error_message: None,
                created_at: 1100,
                updated_at: 1100,
            };
            repository
                .insert_agent_message_with_deliveries(&message, &[previous.clone()])
                .unwrap();
            let mut injecting = previous.clone();
            injecting.status = "injecting".into();
            injecting.updated_at = 1300;
            Claim::Agent {
                previous,
                injecting,
            }
        }
        Mode::Legacy => {
            let team = TeamRecord {
                id: "startup_delivery_team".into(),
                name: "Delivery startup".into(),
                lead_agent_id: sender.id.clone(),
                created_by: "operator".into(),
                created_at: 1000,
                updated_at: 1000,
                deleted_at: None,
            };
            repository.upsert_team(&team).unwrap();
            for id in [sender.id.as_str(), SESSION] {
                repository
                    .upsert_team_member(&TeamMemberRecord {
                        team_id: team.id.clone(),
                        agent_id: id.to_owned(),
                        title: None,
                        joined_at: 1000,
                        added_by: "operator".into(),
                        creator_agent_id: None,
                        creator_compaction_subscription: "auto".into(),
                        worktree_id: None,
                    })
                    .unwrap();
            }
            let mut admission = legacy_proposed(
                &team,
                "startup_delivery_message",
                &[SESSION],
                Some("original-delivery-key"),
            );
            admission.0.policy = policy.into();
            admission.1[0].effective_policy = Some(policy.into());
            admission.2[0].payload = json!({"message":admission.0,"deliveries":admission.1});
            admission.2[1].payload = json!({"delivery":admission.1[0]});
            legacy_admit(repository, &admission).unwrap();
            let previous = admission.1[0].clone();
            let (injecting, _) = claim_plan(&previous, 1300, "test");
            Claim::Legacy {
                previous,
                injecting,
            }
        }
    }
}

fn known_child(fixture: &Fixture, claim: &Claim, approval: bool) -> TurnFixture {
    let repository = &fixture.repository;
    let mut pending = pending_turn(&fixture.session());
    pending.turn.id = "delivery_accepted_child".into();
    pending.turn.source = Some("agent_message_delivery_transport".into());
    pending.turn.started_at = Some(1400);
    pending.admission.turn_id = pending.turn.id.clone();
    pending.admission.projection_source = TurnInputProjectionSource::AgentMessageDeliveryTransport;
    pending.admission.correlation.correlation_id = Some(claim.message().into());
    pending.admission.admitted_at = 1400;
    pending.admission.updated_at = 1400;
    pending.session.active_turn_id = Some(pending.turn.id.clone());
    pending.session.updated_at = 1400;
    if approval {
        pending.turn.status = "waiting_for_approval".into();
        pending.session.status = "waiting_for_approval".into();
        pending.admission.dispatch_policy.permission_mode = Some("require_approval".into());
        pending
            .admission
            .dispatch_policy
            .pre_dispatch_approval_required = true;
    }
    let approval = approval.then(|| ApprovalRecord {
        id: "delivery_child_approval".into(),
        session_id: SESSION.into(),
        turn_id: pending.turn.id.clone(),
        origin: "runtime_pre_dispatch_policy".into(),
        tool_call_id: None,
        provider_approval_ref: None,
        status: "pending".into(),
        request: json!({"reason":"manual approval required before provider execution"}),
        response: None,
        created_at: 1400,
        resolved_at: None,
    });
    repository
        .admit_turn(
            &pending.admission,
            &pending.turn,
            &pending.session,
            approval.as_ref(),
        )
        .unwrap();
    if approval.is_some() {
        return pending;
    }
    let mut dispatching = pending.admission.clone();
    dispatching.dispatch_state = TurnDispatchState::Dispatching;
    dispatching.updated_at = 1500;
    repository
        .claim_pending_turn_dispatch(&dispatching)
        .unwrap();
    let mut dispatched = dispatching.clone();
    dispatched.dispatch_state = TurnDispatchState::Dispatched;
    dispatched.provider_native_turn_id = Some("delivery-native-child".into());
    dispatched.updated_at = 1510;
    let mut turn = pending.turn.clone();
    turn.status = "in_progress".into();
    let mut session = pending.session.clone();
    session.status = "turn_running".into();
    session.updated_at = 1510;
    let receipt = repository
        .commit_provider_turn_started(
            &dispatching,
            &dispatched,
            &pending.turn,
            &turn,
            &pending.session,
            &session,
            &event(&turn, "started", "turn.started", json!({}), 1510),
        )
        .unwrap();
    TurnFixture {
        session: receipt.session,
        turn: receipt.turn,
        admission: receipt.admission,
    }
}

fn refused(fixture: &Fixture, claim: &Claim, key: &str) {
    let command = fixture.command(key, key);
    let before = snapshot(&fixture.repository);
    let error = fixture
        .repository
        .admit_startup_reattach(&command, 2000)
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Conflict(_)), "{error:?}");
    assert!(error.to_string().contains(claim.id()), "{error}");
    assert!(error.to_string().contains(claim.message()), "{error}");
    assert!(
        !matches!(error, RuntimeError::OperationUnresolved { .. }),
        "delivery ID is not an operation ID"
    );
    assert_eq!(snapshot(&fixture.repository), before);
    assert!(fixture
        .repository
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
}

#[test]
fn startup_winner_preserves_pending_delivery_and_release_allows_only_first_claim() {
    for mode in [Mode::Agent, Mode::Legacy] {
        let fixture = Fixture::new(false);
        let claim = seed(&fixture, mode, "immediate_interrupt");
        let grant = execute(
            &fixture.repository,
            &fixture.command("resume_first", "resume_first"),
            1200,
        );
        let before = snapshot(&fixture.repository);
        assert_unresolved(claim.claim(&fixture.repository), "resume_first");
        assert_eq!(snapshot(&fixture.repository), before, "{mode:?}");
        fixture
            .repository
            .complete_startup_reattach(&grant, &ack(&grant, false), 1250)
            .unwrap();
        assert!(claim.claim(&fixture.repository).unwrap());
        refused(&fixture, &claim, "delivery_now_owns");
    }
}

#[test]
fn unresolved_or_historical_injecting_blocks_only_its_recipient_without_an_operation() {
    for mode in [Mode::Agent, Mode::Legacy] {
        for historical in [false, true] {
            let fixture = Fixture::new(false);
            let claim = seed(&fixture, mode, "immediate_interrupt");
            assert!(claim.claim(&fixture.repository).unwrap());
            if historical {
                open_connection(&fixture.repository.database_path).unwrap().execute(
                    "DELETE FROM runtime_events WHERE kind IN ('agent_delivery.injecting','team_delivery.injecting')",
                    [],
                ).unwrap();
            }
            refused(&fixture, &claim, "delivery_unresolved");
            let mut other = fixture.session();
            other.id = "unrelated_delivery_recipient".into();
            other.provider_session_ref = Some("unrelated-native".into());
            fixture.repository.upsert_session(&other).unwrap();
            execute(
                &fixture.repository,
                &command(
                    &fixture.repository,
                    "other_can_resume",
                    "other_can_resume",
                    &other.id,
                ),
                2100,
            );
            refused(&fixture, &claim, "original_still_unresolved");
        }
    }
}

#[test]
fn concurrent_independent_delivery_and_startup_admissions_have_one_winner() {
    for mode in [Mode::Agent, Mode::Legacy] {
        let fixture = Fixture::new(false);
        let claim = seed(&fixture, mode, "immediate_interrupt");
        let command = fixture.command("delivery_resume_race", "delivery_resume_race");
        let gate = Arc::new(Barrier::new(2));
        let startup_repository = fixture.repository.clone();
        let startup_gate = gate.clone();
        let startup_command = command.clone();
        let startup = std::thread::spawn(move || {
            startup_gate.wait();
            startup_repository.admit_startup_reattach(&startup_command, 1200)
        });
        let delivery_repository = fixture.repository.clone();
        let delivery_claim = claim.clone();
        let delivery = std::thread::spawn(move || {
            gate.wait();
            delivery_claim.claim(&delivery_repository)
        });
        let startup = startup.join().unwrap();
        let delivery = delivery.join().unwrap();
        match startup {
            Ok(StartupReattachAdmission::Execute(grant)) => {
                assert_unresolved(delivery, &command.operation_id);
                assert_attempt(&fixture.repository, &grant, "requested", "started", true);
            }
            Err(RuntimeError::Conflict(_)) => {
                assert!(delivery.unwrap());
                assert!(fixture
                    .repository
                    .get_operation(&command.operation_id)
                    .unwrap()
                    .is_none());
            }
            other => panic!("unexpected race: {mode:?}, startup={other:?}, delivery={delivery:?}"),
        }
    }
}

#[test]
fn proved_child_allows_startup_and_parent_result_release_and_exact_replay() {
    for mode in [Mode::Agent, Mode::Legacy] {
        for approval in [false, true] {
            let fixture = Fixture::new(false);
            let claim = seed(&fixture, mode, "immediate_interrupt");
            assert!(claim.claim(&fixture.repository).unwrap());
            let child = known_child(&fixture, &claim, approval);
            let command = fixture.command("proved_delivery_resume", "proved_delivery_resume");
            let grant = execute(&fixture.repository, &command, 2000);
            let result = claim.commit_result(&fixture.repository, &child.turn.id);
            assert_eq!(result["status"], "injected");
            let before = snapshot(&fixture.repository);
            assert_eq!(
                claim.commit_result(&fixture.repository, &child.turn.id),
                result
            );
            assert_eq!(
                snapshot(&fixture.repository),
                before,
                "known replay under resume claim must be DB-only"
            );
            fixture
                .repository
                .complete_startup_reattach(&grant, &ack(&grant, false), 2500)
                .unwrap();
            assert_attempt(&fixture.repository, &grant, "completed", "finalized", false);
            let before = snapshot(&fixture.repository);
            assert!(matches!(
                fixture
                    .repository
                    .admit_startup_reattach(&command, 2600)
                    .unwrap(),
                StartupReattachAdmission::Replay(_)
            ));
            assert_eq!(snapshot(&fixture.repository), before);
        }
    }
}

#[test]
fn duplicate_correlated_child_or_corrupt_original_claim_never_clears_startup_fence() {
    for mode in [Mode::Agent, Mode::Legacy] {
        for fault in [
            "duplicate",
            "wrong_provider",
            "wrong_source",
            "orphan",
            "claim",
        ] {
            let fixture = Fixture::new(false);
            let claim = seed(&fixture, mode, "immediate_interrupt");
            assert!(claim.claim(&fixture.repository).unwrap());
            let child = known_child(&fixture, &claim, false);
            if fault == "claim" {
                open_connection(&fixture.repository.database_path).unwrap().execute(
                    "UPDATE runtime_events SET payload_json='{}' WHERE kind IN ('agent_delivery.injecting','team_delivery.injecting')", [],
                ).unwrap();
            } else if fault == "orphan" {
                // Deliberate historical corruption: retain an admission whose
                // turn is absent. A JOIN must not hide it beside the good child.
                let db = open_connection(&fixture.repository.database_path).unwrap();
                db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
                db.execute(
                    "INSERT INTO turn_admissions(
                       turn_id,session_id,provider,projection_source,user_input_snapshot_json,
                       dispatch_policy_json,correlation_json,dispatch_state,
                       provider_native_turn_id,dispatch_error_json,admitted_at,updated_at)
                     SELECT 'orphan_correlated_child',session_id,provider,projection_source,
                       user_input_snapshot_json,dispatch_policy_json,correlation_json,dispatch_state,
                       NULL,dispatch_error_json,admitted_at,updated_at
                     FROM turn_admissions WHERE turn_id=?1",
                    [&child.turn.id],
                ).unwrap();
            } else {
                // Deliberate duplicate fixture, not another native attempt.
                let mut duplicate = child.turn.clone();
                duplicate.id = "second_correlated_child".into();
                if fault == "wrong_source" {
                    duplicate.source = Some("user".into());
                }
                fixture.repository.upsert_turn(&duplicate).unwrap();
                let mut admission = child.admission.clone();
                admission.turn_id = duplicate.id;
                admission.provider_native_turn_id = None;
                if fault == "wrong_provider" {
                    admission.provider = "claude".into();
                }
                if fault == "wrong_source" {
                    admission.projection_source = TurnInputProjectionSource::UserVisible;
                }
                fixture
                    .repository
                    .upsert_turn_admission(&admission)
                    .unwrap();
            }
            refused(&fixture, &claim, "ambiguous_delivery_resume");
        }
    }
}

#[test]
fn internal_interrupt_owner_requires_exact_generation_target_policy_and_no_child() {
    for mode in [Mode::Agent, Mode::Legacy] {
        for invalid in [
            "none",
            "policy",
            "provider",
            "turn",
            "generation",
            "child",
            "no_admission",
            "pending",
            "dispatching",
            "unknown",
            "not_dispatched",
            "quarantine",
            "provider_approval",
            "decision_pending",
        ] {
            let fixture = Fixture::new(false);
            let running = running(&fixture);
            let claim = seed(
                &fixture,
                mode,
                if invalid == "policy" {
                    "non_interrupting"
                } else {
                    "immediate_interrupt"
                },
            );
            assert!(claim.claim(&fixture.repository).unwrap());
            let mut requested = TurnFixture {
                session: running.session.clone(),
                turn: running.turn.clone(),
                admission: running.admission.clone(),
            };
            match invalid {
                "provider" => requested.session.provider = "claude".into(),
                "turn" => requested.turn.provider_turn_ref = Some("different-native-turn".into()),
                "generation" => {
                    let table = match mode {
                        Mode::Agent => "agent_deliveries",
                        Mode::Legacy => "team_deliveries",
                    };
                    open_connection(&fixture.repository.database_path)
                        .unwrap()
                        .execute(
                            &format!("UPDATE {table} SET updated_at=updated_at+1 WHERE id=?1"),
                            [claim.id()],
                        )
                        .unwrap();
                }
                "child" => {
                    let mut child = running.turn.clone();
                    child.id = "already_accepted_delivery_child".into();
                    child.source = Some("agent_message_delivery_transport".into());
                    child.provider_turn_ref = Some("already-accepted-delivery-child-native".into());
                    fixture.repository.upsert_turn(&child).unwrap();
                    let mut admission = running.admission.clone();
                    admission.provider_native_turn_id = child.provider_turn_ref.clone();
                    admission.turn_id = child.id;
                    admission.projection_source =
                        TurnInputProjectionSource::AgentMessageDeliveryTransport;
                    admission.correlation.correlation_id = Some(claim.message().into());
                    fixture
                        .repository
                        .upsert_turn_admission(&admission)
                        .unwrap();
                }
                "no_admission" => {
                    open_connection(&fixture.repository.database_path)
                        .unwrap()
                        .execute(
                            "DELETE FROM turn_admissions WHERE turn_id=?1",
                            [&running.turn.id],
                        )
                        .unwrap();
                }
                "pending" | "dispatching" | "unknown" | "not_dispatched" => {
                    open_connection(&fixture.repository.database_path)
                        .unwrap()
                        .execute(
                            "UPDATE turn_admissions SET dispatch_state=?1 WHERE turn_id=?2",
                            rusqlite::params![invalid, running.turn.id],
                        )
                        .unwrap();
                }
                "quarantine" => {
                    let quarantined = quarantine(&fixture.repository, &running);
                    requested = quarantined;
                }
                "provider_approval" | "decision_pending" => {
                    let (mut session, turn, mut approval, mut event) =
                        crate::tests::provider_native_approval_events::native_request(
                            &running.session,
                            &running.turn,
                            "native_interrupt_approval",
                        );
                    session.updated_at = 1500;
                    approval.created_at = 1500;
                    event.created_at = 1500;
                    let receipt = fixture
                        .repository
                        .admit_provider_approval(
                            &running.turn,
                            &turn,
                            &running.session,
                            &session,
                            &approval,
                            &event,
                        )
                        .unwrap();
                    requested.session = receipt.session;
                    requested.turn = receipt.turn;
                    if invalid == "decision_pending" {
                        let mut reserved = receipt.approval;
                        reserved.status = "decision_pending".into();
                        // Existing unresolved reservation projection fixture.
                        fixture.repository.upsert_approval(&reserved).unwrap();
                    }
                }
                _ => {}
            }
            let before = snapshot(&fixture.repository);
            let result = claim.verify(&fixture.repository, &requested);
            if matches!(invalid, "none" | "provider_approval") {
                result.unwrap();
            } else {
                assert!(result.is_err(), "{mode:?}/{invalid}");
            }
            assert_eq!(snapshot(&fixture.repository), before);
        }
    }
}

#[test]
fn legacy_native_blocker_and_incomplete_scan_are_typed_before_claim_and_at_interrupt() {
    use crate::tests::legacy_native_turn_guards::{legacy, LEGACY_ID};
    use runtime_core::legacy_spawn_obligations::{LegacyScanLimit, LegacyTargetBlockReason};
    for mode in [Mode::Agent, Mode::Legacy] {
        for before_claim in [true, false] {
            for history in ["blocked", "malformed", "incomplete"] {
                let fixture = Fixture::new(false);
                let running = running(&fixture);
                let claim = seed(&fixture, mode, "immediate_interrupt");
                if !before_claim {
                    assert!(claim.claim(&fixture.repository).unwrap());
                }
                let db = open_connection(&fixture.repository.database_path).unwrap();
                match history {
                    "blocked" => legacy(
                        &db,
                        "rolled_back",
                        &json!({"spawned_session_id":SESSION}).to_string(),
                        false,
                    ),
                    "malformed" => legacy(&db, "completed", "{broken", false),
                    "incomplete" => {
                        db.execute(
                            "INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
                             VALUES(?1,'history','spawn_member_with_worktree','planned','{}',1,1)",
                            ["x".repeat(4097)],
                        ).unwrap();
                    }
                    _ => unreachable!(),
                }
                let before = snapshot(&fixture.repository);
                let error = if before_claim {
                    claim.claim(&fixture.repository).unwrap_err()
                } else {
                    claim.verify(&fixture.repository, &running).unwrap_err()
                };
                match (&error, history) {
                    (RuntimeError::LegacyNativeObligationBlocked { coordinate, reason }, "blocked") => {
                        assert_eq!(coordinate.operation_id, LEGACY_ID);
                        assert_eq!(*reason, LegacyTargetBlockReason::ExactSession);
                    }
                    (RuntimeError::LegacyNativeObligationBlocked { coordinate, reason }, "malformed") => {
                        assert_eq!(coordinate.operation_id, LEGACY_ID);
                        assert_eq!(*reason, LegacyTargetBlockReason::UnknownSessionScope);
                        assert_eq!(coordinate.evidence_problem.as_deref(), Some("payload_malformed_or_not_object"));
                    }
                    (RuntimeError::LegacyNativeScanIncomplete { incomplete }, "incomplete") => {
                        assert_eq!(incomplete.reason, LegacyScanLimit::MetadataCellLimit);
                        assert!(incomplete.after_operation_id.is_none(), "no original operation coordinate was certified");
                    }
                    _ => panic!("wrong original native cause for {mode:?}/{before_claim}/{history}: {error:?}"),
                }
                assert!(error.legacy_native_recovery().is_some());
                assert_eq!(snapshot(&fixture.repository), before);
            }
        }
    }
}

#[test]
fn late_legacy_blocker_cannot_prevent_known_delivery_result_or_receipt_replay() {
    use crate::tests::legacy_native_turn_guards::legacy;
    for mode in [Mode::Agent, Mode::Legacy] {
        let fixture = Fixture::new(false);
        let claim = seed(&fixture, mode, "immediate_interrupt");
        assert!(claim.claim(&fixture.repository).unwrap());
        let child = known_child(&fixture, &claim, false);
        legacy(
            &open_connection(&fixture.repository.database_path).unwrap(),
            "rolled_back",
            &json!({"spawned_session_id":SESSION}).to_string(),
            false,
        );
        let before = snapshot(&fixture.repository);
        let result = claim.commit_result(&fixture.repository, &child.turn.id);
        assert_eq!(result["status"], "injected");
        let settled = snapshot(&fixture.repository);
        assert_eq!(
            settled.get("team_operation_journal"),
            before.get("team_operation_journal")
        );
        assert_eq!(
            claim.commit_result(&fixture.repository, &child.turn.id),
            result
        );
        assert_eq!(snapshot(&fixture.repository), settled);
    }
}

#[test]
fn exact_canonical_mirror_reuses_one_claim_but_mismatched_generation_refuses() {
    for mismatch in [false, true] {
        let fixture = Fixture::new(false);
        let claim = seed(&fixture, Mode::Agent, "immediate_interrupt");
        assert!(claim.claim(&fixture.repository).unwrap());
        let child = known_child(&fixture, &claim, false);
        let db = open_connection(&fixture.repository.database_path).unwrap();
        // Install the persisted legacy projection of the canonical claim.
        db.execute_batch(
            "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
               VALUES('delivery_mirror_team','Mirror','session_lead','test',1000,1000);
             INSERT INTO team_messages(id,team_id,scope,sender_agent_id,
               recipient_agent_ids_json,input_json,priority,policy,created_at)
               SELECT id,'delivery_mirror_team',scope,sender_agent_id,
                 recipient_agent_ids_json,input_json,priority,policy,created_at
               FROM agent_messages WHERE id='startup_delivery_message';
             INSERT INTO team_deliveries(id,message_id,team_id,recipient_agent_id,provider,status,
               effective_policy,injection_strategy,injected_turn_id,last_error_code,last_error_message,
               created_at,updated_at)
               SELECT id,message_id,'delivery_mirror_team',recipient_agent_id,provider,status,
                 effective_policy,injection_strategy,injected_turn_id,last_error_code,last_error_message,
                 created_at,updated_at FROM agent_deliveries WHERE id='startup_delivery';"
        ).unwrap();
        if mismatch {
            db.execute(
                "UPDATE team_deliveries SET updated_at=updated_at+1 WHERE id=?1",
                [claim.id()],
            )
            .unwrap();
            refused(&fixture, &claim, "mismatched_mirror_resume");
        } else {
            let grant = execute(
                &fixture.repository,
                &fixture.command("mirror_resume", "mirror_resume"),
                2000,
            );
            let result = claim.commit_result(&fixture.repository, &child.turn.id);
            let mirror: (String, Option<String>) = db
                .query_row(
                    "SELECT status,injected_turn_id FROM team_deliveries WHERE id=?1",
                    [claim.id()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(mirror, ("injected".into(), Some(child.turn.id)));
            assert_eq!(result["status"], "injected");
            fixture
                .repository
                .complete_startup_reattach(&grant, &ack(&grant, false), 2500)
                .unwrap();
        }
    }
}

#[test]
fn canonical_same_id_pending_row_cannot_hide_original_legacy_injection() {
    for other_recipient in [false, true] {
        let fixture = Fixture::new(false);
        let claim = seed(&fixture, Mode::Legacy, "immediate_interrupt");
        assert!(claim.claim(&fixture.repository).unwrap());
        let Claim::Legacy { injecting, .. } = &claim else {
            unreachable!()
        };
        let recipient = if other_recipient {
            "session_lead"
        } else {
            SESSION
        };
        let message = AgentMessageRecord {
            id: injecting.message_id.clone(),
            scope: "direct".into(),
            context_kind: AgentMessageContextKind::GlobalDirect,
            workspace_id: None,
            legacy_team_id: None,
            sender_agent_id: "session_lead".into(),
            recipient_agent_ids: vec![recipient.into()],
            input: json!([{"type":"text","text":"historical same-ID projection"}]),
            image_paths: vec![],
            priority: "normal".into(),
            policy: "immediate_interrupt".into(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
            created_at: 1100,
        };
        let canonical = AgentDeliveryRecord {
            id: injecting.id.clone(),
            message_id: message.id.clone(),
            recipient_agent_id: recipient.into(),
            provider: injecting.provider.clone(),
            status: "pending".into(),
            effective_policy: injecting.effective_policy.clone(),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: None,
            last_error_message: None,
            created_at: 1100,
            updated_at: 1100,
        };
        fixture
            .repository
            .insert_agent_message_with_deliveries(&message, &[canonical])
            .unwrap();
        refused(&fixture, &claim, "same_id_is_not_native_clearance");
    }
}
