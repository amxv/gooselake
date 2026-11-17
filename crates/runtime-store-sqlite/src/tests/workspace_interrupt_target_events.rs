use super::*;
use runtime_core::{OperationDetails, OperationPhase, RuntimeError, WorkspaceInterruptCommand};

struct Fixture {
    _temp: tempfile::TempDir,
    repository: SqliteRuntimeRepository,
    workspace_id: String,
    command: WorkspaceInterruptCommand,
    targets: Vec<(String, String)>,
}

impl Fixture {
    fn new(count: usize) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let root = temp.path().join("interrupt-events");
        std::fs::create_dir_all(&root).unwrap();
        let workspace = registered_workspace(&repository, &root, "Interrupt target events");
        let mut targets = Vec::new();
        for index in 0..count {
            let agent = format!("sess_target_{index}");
            let turn = format!("turn_target_{index}");
            create_agent(
                &repository,
                &workspace,
                &agent,
                &format!("target-{index}"),
                "Interrupt target",
                Some(&turn),
            );
            targets.push((agent, turn));
        }
        let command = prepare_workspace_interrupt(
            &workspace.workspace_id,
            OperationActor::operator("workspace-control-test"),
            Some("interrupt-target-event-once".into()),
        )
        .unwrap();
        let WorkspaceInterruptAdmission::Execute(plan) =
            repository.begin_workspace_interrupt(&command).unwrap()
        else {
            panic!("first interrupt must execute")
        };
        assert_eq!(plan.targets.len(), count);
        Self {
            _temp: temp,
            repository,
            workspace_id: workspace.workspace_id,
            command,
            targets,
        }
    }

    fn db(&self) -> Connection {
        Connection::open(&self.repository.database_path).unwrap()
    }

    fn details(&self) -> OperationDetails {
        self.repository
            .get_operation(&self.command.operation_id)
            .unwrap()
            .unwrap()
    }

    fn events(&self) -> Vec<runtime_core::RuntimeEventRecord> {
        self.repository
            .list_runtime_events(
                Some((RuntimeEventScope::Workspace, &self.workspace_id)),
                None,
                100,
            )
            .unwrap()
    }

    fn target_events(&self) -> Vec<runtime_core::RuntimeEventRecord> {
        self.events()
            .into_iter()
            .filter(|event| event.kind.starts_with("workspace.interrupt_target_"))
            .collect()
    }

    fn start(&self, index: usize) {
        let (agent, turn) = &self.targets[index];
        self.repository
            .mark_workspace_interrupt_started(&self.command.operation_id, agent, turn, 200)
            .unwrap();
    }

    fn acknowledgement(&self, index: usize) {
        let (agent, turn) = &self.targets[index];
        // Direct store fixture for an already persisted real-ACK receipt.
        // The authenticated HTTP tests independently exercise the actual fake provider.
        self.repository
            .append_runtime_event(&NewRuntimeEvent {
                event_id: format!("fixture_ack_{agent}"),
                scope: RuntimeEventScope::Session,
                scope_id: agent.clone(),
                session_id: Some(agent.clone()),
                team_id: None,
                turn_id: Some(turn.clone()),
                kind: "turn.interrupt_requested".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: serde_json::json!({"operation_id":self.command.operation_id}),
                provider: None,
                provider_seq: None,
                created_at: 201,
            })
            .unwrap();
    }

    fn fail_event(&self, phase: &str, only_agent: Option<&str>) {
        assert!(matches!(phase, "started" | "finalized" | "uncertain"));
        let target = only_agent
            .map(|agent| {
                assert!(self.targets.iter().any(|(id, _)| id == agent));
                format!(" AND json_extract(NEW.payload_json,'$.agent_id')='{agent}'")
            })
            .unwrap_or_default();
        self.db()
            .execute_batch(&format!(
                "CREATE TRIGGER reject_interrupt_target BEFORE INSERT ON runtime_events
             WHEN NEW.kind='workspace.interrupt_target_{phase}'{target}
             BEGIN SELECT RAISE(ABORT,'fixture interrupt target event failure'); END;"
            ))
            .unwrap();
    }

    fn allow_event(&self) {
        self.db()
            .execute_batch("DROP TRIGGER reject_interrupt_target;")
            .unwrap();
    }
}

#[test]
fn interrupt_started_event_failure_preserves_original_intent_claim_and_attempt() {
    let fixture = Fixture::new(1);
    let before = fixture.details();
    let events = fixture.events();
    fixture.fail_event("started", None);
    let (agent, turn) = &fixture.targets[0];
    let error = fixture
        .repository
        .mark_workspace_interrupt_started(&fixture.command.operation_id, agent, turn, 200)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("fixture interrupt target event failure"),
        "{error}"
    );
    assert_eq!(fixture.details(), before);
    assert_eq!(fixture.events(), events);
    assert_eq!(before.effects[0].phase, "intended");
    assert_eq!(before.effects[0].attempt_count, 0);
    assert_eq!(before.claims.len(), 1);

    fixture.allow_event();
    fixture.start(0);
    let saved = fixture.details();
    assert_eq!(saved.effects[0].phase, "started");
    assert_eq!(saved.effects[0].attempt_count, 1);
    assert_eq!(saved.claims, before.claims);
    let events = fixture.target_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "workspace.interrupt_target_started");
    assert_eq!(events[0].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(
        events[0].payload["operation_id"],
        fixture.command.operation_id
    );
    assert_eq!(events[0].payload["effect_id"], saved.effects[0].effect_id);
    assert_eq!(events[0].payload["agent_id"], *agent);
    assert_eq!(events[0].payload["turn_id"], *turn);
    assert_eq!(events[0].payload["attempt_count"], 1);
    assert!(
        fixture
            .repository
            .mark_workspace_interrupt_started(&fixture.command.operation_id, agent, turn, 999,)
            .is_err(),
        "Started is never a fresh native dispatch token"
    );
    assert_eq!(fixture.details(), saved);
    assert_eq!(fixture.target_events(), events);
}

#[test]
fn interrupt_known_receipt_event_failure_rolls_back_then_exact_outcome_replays() {
    for interrupted in [false, true] {
        let fixture = Fixture::new(1);
        fixture.start(0);
        let before = fixture.details();
        let events = fixture.events();
        let (agent, turn) = &fixture.targets[0];
        fixture.fail_event("finalized", None);
        let error = fixture
            .repository
            .finalize_workspace_interrupt_effect(
                &fixture.command.operation_id,
                agent,
                turn,
                interrupted,
                "original-known-outcome",
                201,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("fixture interrupt target event failure"),
            "{error}"
        );
        assert_eq!(fixture.details(), before);
        assert_eq!(fixture.events(), events);
        fixture.allow_event();
        fixture
            .repository
            .finalize_workspace_interrupt_effect(
                &fixture.command.operation_id,
                agent,
                turn,
                interrupted,
                "original-known-outcome",
                201,
            )
            .unwrap();
        let saved = fixture.details();
        let events = fixture.target_events();
        assert_eq!(events.len(), 2);
        assert_eq!(saved.effects[0].phase, "finalized");
        assert_eq!(saved.effects[0].attempt_count, 1);
        assert_eq!(saved.claims, before.claims);
        assert_eq!(events[1].criticality, RuntimeEventCriticality::Critical);
        assert_eq!(events[1].payload["phase"], "finalized");
        assert_eq!(
            events[1].payload["evidence"]["outcome"],
            if interrupted {
                "interrupted"
            } else {
                "skipped"
            }
        );
        fixture
            .repository
            .finalize_workspace_interrupt_effect(
                &fixture.command.operation_id,
                agent,
                turn,
                interrupted,
                "original-known-outcome",
                999,
            )
            .unwrap();
        assert!(matches!(
            fixture.repository.finalize_workspace_interrupt_effect(
                &fixture.command.operation_id,
                agent,
                turn,
                interrupted,
                "changed-receipt",
                999
            ),
            Err(RuntimeError::Conflict(_))
        ));
        assert_eq!(fixture.details(), saved);
        assert_eq!(fixture.target_events(), events);
    }
}

#[test]
fn interrupt_uncertain_receipt_is_atomic_and_exact_replay_cannot_authorize_dispatch() {
    let fixture = Fixture::new(1);
    fixture.start(0);
    let (agent, turn) = &fixture.targets[0];
    let before = fixture.details();
    let events = fixture.events();
    let error_receipt = serde_json::json!({"message":"original native ACK unavailable"});
    fixture.fail_event("uncertain", None);
    assert!(fixture
        .repository
        .mark_workspace_interrupt_uncertain(
            &fixture.command.operation_id,
            agent,
            turn,
            &error_receipt,
            201,
        )
        .is_err());
    assert_eq!(fixture.details(), before);
    assert_eq!(fixture.events(), events);
    fixture.allow_event();
    fixture
        .repository
        .mark_workspace_interrupt_uncertain(
            &fixture.command.operation_id,
            agent,
            turn,
            &error_receipt,
            201,
        )
        .unwrap();
    let saved = fixture.details();
    let events = fixture.events();
    assert_eq!(saved.effects[0].phase, "uncertain");
    fixture
        .repository
        .mark_workspace_interrupt_uncertain(
            &fixture.command.operation_id,
            agent,
            turn,
            &error_receipt,
            999,
        )
        .unwrap();
    assert!(matches!(
        fixture.repository.mark_workspace_interrupt_uncertain(
            &fixture.command.operation_id,
            agent,
            turn,
            &serde_json::json!({"message":"changed"}),
            999
        ),
        Err(RuntimeError::Conflict(_))
    ));
    let error = fixture
        .repository
        .begin_workspace_interrupt(&fixture.command)
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved { operation_id, .. }
        if operation_id == fixture.command.operation_id)
    );
    assert_eq!(fixture.details(), saved);
    assert_eq!(fixture.events(), events);
}

#[test]
fn interrupt_recovery_late_event_failure_rolls_back_every_known_target_then_replays_once() {
    let fixture = Fixture::new(2);
    fixture.start(0);
    fixture.start(1);
    let (second_agent, second_turn) = &fixture.targets[1];
    fixture
        .repository
        .mark_workspace_interrupt_uncertain(
            &fixture.command.operation_id,
            second_agent,
            second_turn,
            &serde_json::json!({"message":"caller lost native ACK return"}),
            201,
        )
        .unwrap();
    fixture.acknowledgement(0);
    fixture.acknowledgement(1);
    let before = fixture.details();
    let events = fixture.events();
    fixture.fail_event("finalized", Some(second_agent));
    let error = fixture
        .repository
        .begin_workspace_interrupt(&fixture.command)
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved { ref operation_id, ref message, .. }
        if operation_id == &fixture.command.operation_id
            && message.contains("fixture interrupt target event failure"))
    );
    assert_eq!(fixture.details(), before);
    assert_eq!(fixture.events(), events);

    fixture.allow_event();
    let reopened = SqliteRuntimeRepository::new(fixture.repository.database_path.clone());
    let WorkspaceInterruptAdmission::Execute(plan) = reopened
        .begin_workspace_interrupt(&fixture.command)
        .unwrap()
    else {
        panic!("recovery precedes terminal parent receipt")
    };
    assert!(
        plan.targets.is_empty(),
        "known ACK recovery is database-only"
    );
    let settled = fixture.details();
    assert!(settled
        .effects
        .iter()
        .all(|effect| effect.phase == "finalized"));
    assert!(settled
        .effects
        .iter()
        .all(|effect| effect.attempt_count == 1));
    assert_eq!(settled.claims, before.claims);
    let finalized = fixture
        .target_events()
        .into_iter()
        .filter(|event| event.kind == "workspace.interrupt_target_finalized")
        .collect::<Vec<_>>();
    assert_eq!(finalized.len(), 2);
    assert_eq!(finalized[0].payload["from_phase"], "started");
    assert_eq!(finalized[1].payload["from_phase"], "uncertain");
    assert!(finalized
        .iter()
        .all(|event| event.criticality == RuntimeEventCriticality::Critical));
    let completed = reopened
        .complete_workspace_interrupt(&fixture.command.operation_id, 300)
        .unwrap();
    let terminal = fixture.details();
    let events = fixture.events();
    assert_eq!(terminal.operation.phase, OperationPhase::Completed);
    assert!(terminal.claims.is_empty());
    assert_eq!(
        reopened
            .begin_workspace_interrupt(&fixture.command)
            .unwrap(),
        WorkspaceInterruptAdmission::Replay(completed)
    );
    assert_eq!(fixture.details(), terminal);
    assert_eq!(fixture.events(), events);
}

#[test]
fn interrupt_target_event_sequence_exhaustion_rolls_back_started_and_known_receipt() {
    for started in [false, true] {
        let fixture = Fixture::new(1);
        if started {
            fixture.start(0);
        }
        fixture
            .db()
            .execute(
                "INSERT INTO runtime_events
             (event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
             VALUES('fixture_interrupt_max','workspace',?1,?2,
                    'fixture.workspace_history',1,'{}',1)",
                params![fixture.workspace_id, i64::MAX],
            )
            .unwrap();
        let before = fixture.details();
        let events = fixture.events();
        let (agent, turn) = &fixture.targets[0];
        let result = if started {
            fixture.repository.finalize_workspace_interrupt_effect(
                &fixture.command.operation_id,
                agent,
                turn,
                true,
                "known ACK",
                201,
            )
        } else {
            fixture.repository.mark_workspace_interrupt_started(
                &fixture.command.operation_id,
                agent,
                turn,
                200,
            )
        };
        assert!(matches!(result, Err(RuntimeError::Conflict(ref message))
            if message.contains("workspace interrupt event sequence")));
        assert_eq!(fixture.details(), before);
        assert_eq!(fixture.events(), events);
    }
}

#[test]
fn interrupt_ignored_or_damaged_target_receipts_roll_back_claim_settlement_and_recovery() {
    for action in ["started", "finalized", "uncertain", "recovery"] {
        for damage in [
            None,
            Some("delete"),
            Some("critical=0"),
            Some("payload_json='{}'"),
            Some("seq=seq+100"),
            Some("session_id='foreign'"),
            Some("event_id=event_id||':changed'"),
        ] {
            let fixture = Fixture::new(1);
            if action != "started" {
                fixture.start(0);
            }
            if action == "recovery" {
                fixture.acknowledgement(0);
            }
            let before = fixture.details();
            let events = fixture.events();
            let phase = if action == "recovery" {
                "finalized"
            } else {
                action
            };
            let (timing, body) = match damage {
                None => ("BEFORE", "SELECT RAISE(IGNORE);".to_string()),
                Some("delete") => (
                    "AFTER",
                    "DELETE FROM runtime_events WHERE event_id=NEW.event_id;".to_string(),
                ),
                Some(change) => (
                    "AFTER",
                    format!("UPDATE runtime_events SET {change} WHERE event_id=NEW.event_id;"),
                ),
            };
            fixture
                .db()
                .execute_batch(&format!(
                    "CREATE TRIGGER reject_interrupt_target {timing} INSERT ON runtime_events
                 WHEN NEW.kind='workspace.interrupt_target_{phase}'
                 BEGIN {body} END;"
                ))
                .unwrap();
            let (agent, turn) = &fixture.targets[0];
            let mutate = || -> Result<(), RuntimeError> {
                match action {
                    "started" => fixture.repository.mark_workspace_interrupt_started(
                        &fixture.command.operation_id,
                        agent,
                        turn,
                        200,
                    ),
                    "finalized" => fixture.repository.finalize_workspace_interrupt_effect(
                        &fixture.command.operation_id,
                        agent,
                        turn,
                        true,
                        "known ACK",
                        201,
                    ),
                    "uncertain" => fixture.repository.mark_workspace_interrupt_uncertain(
                        &fixture.command.operation_id,
                        agent,
                        turn,
                        &serde_json::json!({"message":"original lost ACK"}),
                        201,
                    ),
                    "recovery" => {
                        let WorkspaceInterruptAdmission::Execute(plan) = fixture
                            .repository
                            .begin_workspace_interrupt(&fixture.command)?
                        else {
                            panic!("known target recovery precedes parent completion")
                        };
                        assert!(
                            plan.targets.is_empty(),
                            "known ACK settlement dispatches nothing"
                        );
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            };
            let error = mutate().unwrap_err();
            // Deleting NEW before the subject projection trigger runs can be
            // rejected by its existing foreign key before receipt verification.
            let message = error.to_string();
            let rejected_deleted_parent = damage == Some("delete")
                && message.contains("failed atomically recording workspace interrupt target")
                && message.contains("FOREIGN KEY constraint failed");
            assert!(
                message.contains("workspace interrupt target event") || rejected_deleted_parent,
                "{action}/{damage:?}: {error}"
            );
            if action == "recovery" {
                assert!(
                    matches!(error, RuntimeError::OperationUnresolved { ref operation_id, .. }
                    if operation_id == &fixture.command.operation_id)
                );
            } else {
                assert!(
                    matches!(error, RuntimeError::Conflict(_))
                        || (rejected_deleted_parent && matches!(error, RuntimeError::Bootstrap(_)))
                );
            }
            assert_eq!(fixture.details(), before, "{action}/{damage:?}");
            assert_eq!(fixture.events(), events);
            assert_eq!(
                before.effects[0].attempt_count,
                if action == "started" { 0 } else { 1 }
            );
            assert_eq!(before.claims.len(), 1);

            fixture.allow_event();
            mutate().unwrap();
            let saved = fixture.details();
            let saved_events = fixture.events();
            assert_eq!(saved.effects[0].phase, phase);
            assert_eq!(saved.effects[0].attempt_count, 1);
            assert_eq!(saved.claims, before.claims);
            assert_eq!(saved_events.len(), events.len() + 1);
            assert_eq!(
                saved_events.last().unwrap().criticality,
                RuntimeEventCriticality::Critical
            );
            if action != "started" {
                mutate().unwrap();
                assert_eq!(fixture.details(), saved);
                assert_eq!(fixture.events(), saved_events);
            }
            assert!(
                fixture
                    .repository
                    .mark_workspace_interrupt_started(
                        &fixture.command.operation_id,
                        agent,
                        turn,
                        999,
                    )
                    .is_err(),
                "a completed first claim never reauthorizes native work"
            );
            assert_eq!(fixture.details(), saved);
            assert_eq!(fixture.events(), saved_events);
        }
    }
}

#[test]
fn interrupt_target_carrier_matches_intended_mutation_before_and_after_its_event() {
    for action in ["started", "finalized", "uncertain", "recovery"] {
        for after_event in [false, true] {
            for damage in [
                "evidence_json=json_set(evidence_json,'$.outcome','tampered','$.reason','tampered')",
                "attempt_count=attempt_count+3",
                "updated_at=updated_at+1",
                "phase='intended'",
            ] {
                let fixture = Fixture::new(1);
                if action != "started" { fixture.start(0); }
                if action == "recovery" { fixture.acknowledgement(0); }
                let before = fixture.details();
                let events = fixture.events();
                let phase = if action == "recovery" { "finalized" } else { action };
                let (on, condition, effect_id) = if after_event {
                    ("INSERT ON runtime_events",
                     format!("NEW.kind='workspace.interrupt_target_{phase}'"),
                     "json_extract(NEW.payload_json,'$.effect_id')")
                } else {
                    ("UPDATE ON runtime_operation_effects",
                     format!("NEW.effect_kind='workspace_turn_interrupt' AND NEW.phase='{phase}'"),
                     "NEW.effect_id")
                };
                fixture.db().execute_batch(&format!(
                    "CREATE TRIGGER reject_interrupt_target AFTER {on}
                     WHEN {condition}
                     BEGIN UPDATE runtime_operation_effects SET {damage}
                       WHERE effect_id={effect_id}; END;"
                )).unwrap();
                let (agent, turn) = &fixture.targets[0];
                let mutate = || -> Result<(), RuntimeError> {
                    match action {
                        "started" => fixture.repository.mark_workspace_interrupt_started(
                            &fixture.command.operation_id, agent, turn, 200,
                        ),
                        "finalized" => fixture.repository.finalize_workspace_interrupt_effect(
                            &fixture.command.operation_id, agent, turn, true, "known ACK", 201,
                        ),
                        "uncertain" => fixture.repository.mark_workspace_interrupt_uncertain(
                            &fixture.command.operation_id, agent, turn,
                            &serde_json::json!({"message":"original lost ACK"}), 201,
                        ),
                        "recovery" => {
                            let WorkspaceInterruptAdmission::Execute(plan) =
                                fixture.repository.begin_workspace_interrupt(&fixture.command)?
                            else { panic!("target settlement precedes parent completion") };
                            assert!(plan.targets.is_empty());
                            Ok(())
                        }
                        _ => unreachable!(),
                    }
                };
                let error = mutate().unwrap_err();
                assert!(error.to_string().contains("carrier"),
                    "{action}/{after_event}/{damage}: {error}");
                if action == "recovery" {
                    assert!(matches!(error, RuntimeError::OperationUnresolved { ref operation_id, .. }
                        if operation_id == &fixture.command.operation_id));
                } else {
                    assert!(matches!(error, RuntimeError::Conflict(_)));
                }
                assert_eq!(fixture.details(), before);
                assert_eq!(fixture.events(), events);
                fixture.allow_event();
                mutate().unwrap();
                let saved = fixture.details();
                let saved_events = fixture.events();
                assert_eq!(saved.effects[0].phase, phase);
                assert_eq!(saved.effects[0].attempt_count, 1);
                assert_eq!(saved.claims, before.claims);
                assert_eq!(saved_events.len(), events.len() + 1);
                if action != "started" {
                    mutate().unwrap();
                    assert_eq!(fixture.details(), saved);
                    assert_eq!(fixture.events(), saved_events);
                }
            }
        }
    }
}

#[test]
fn interrupt_recovery_rechecks_earlier_carriers_and_events_after_the_last_target() {
    for damage_event in [false, true] {
        let fixture = Fixture::new(2);
        fixture.start(0);
        fixture.start(1);
        fixture.acknowledgement(0);
        fixture.acknowledgement(1);
        let before = fixture.details();
        let events = fixture.events();
        let first = &fixture.targets[0].0;
        let second = &fixture.targets[1].0;
        let damage = if damage_event {
            format!(
                "UPDATE runtime_events SET payload_json='{{}}'
                 WHERE kind='workspace.interrupt_target_finalized'
                   AND json_extract(payload_json,'$.operation_id')='{}'
                   AND json_extract(payload_json,'$.agent_id')='{first}';",
                fixture.command.operation_id,
            )
        } else {
            format!(
                "UPDATE runtime_operation_effects SET phase='started'
                 WHERE operation_id='{}' AND target_id='{first}'
                   AND effect_kind='workspace_turn_interrupt';",
                fixture.command.operation_id,
            )
        };
        fixture
            .db()
            .execute_batch(&format!(
                "CREATE TRIGGER reject_interrupt_target AFTER INSERT ON runtime_events
             WHEN NEW.kind='workspace.interrupt_target_finalized'
               AND json_extract(NEW.payload_json,'$.agent_id')='{second}'
             BEGIN {damage} END;"
            ))
            .unwrap();
        let error = fixture
            .repository
            .begin_workspace_interrupt(&fixture.command)
            .unwrap_err();
        assert!(
            matches!(error, RuntimeError::OperationUnresolved {
            ref operation_id, ref message, ..
        } if operation_id == &fixture.command.operation_id
            && message.contains("workspace interrupt target event")),
            "{error}"
        );
        assert_eq!(fixture.details(), before);
        assert_eq!(fixture.events(), events);
        fixture.allow_event();
        let WorkspaceInterruptAdmission::Execute(plan) = fixture
            .repository
            .begin_workspace_interrupt(&fixture.command)
            .unwrap()
        else {
            panic!("known target recovery precedes parent completion")
        };
        assert!(plan.targets.is_empty());
        let saved = fixture.details();
        let saved_events = fixture.events();
        assert!(saved
            .effects
            .iter()
            .all(|effect| effect.phase == "finalized" && effect.attempt_count == 1));
        assert_eq!(saved.claims, before.claims);
        assert_eq!(saved_events.len(), events.len() + 2);
        let WorkspaceInterruptAdmission::Execute(replay) = fixture
            .repository
            .begin_workspace_interrupt(&fixture.command)
            .unwrap()
        else {
            panic!("uncompleted parent remains resumable without native targets")
        };
        assert!(replay.targets.is_empty());
        assert_eq!(fixture.details(), saved);
        assert_eq!(fixture.events(), saved_events);
    }
}
