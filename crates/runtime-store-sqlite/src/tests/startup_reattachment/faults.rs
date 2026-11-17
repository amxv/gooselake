use super::support::*;
use crate::db::open_connection;
use runtime_core::{RuntimeEventCriticality, RuntimeEventScope, StartupReattachAdmission};

#[test]
fn startup_admission_and_first_transition_critical_event_faults_leave_no_native_authority() {
    for kind in ["operation.admitted", "operation.transitioned"] {
        let fixture = Fixture::new(true);
        let command = fixture.command("startup_admission_event_fault", "admission_event_instance");
        let db = open_connection(&fixture.repository.database_path).unwrap();
        db.execute_batch(&format!(
            "CREATE TRIGGER startup_admission_event_fault BEFORE INSERT ON runtime_events
             WHEN NEW.kind='{kind}'
               AND json_extract(NEW.payload_json,'$.operation_id')='startup_admission_event_fault'
             BEGIN SELECT RAISE(ABORT,'startup admission Critical event injection'); END;"
        ))
        .unwrap();
        let before = snapshot(&fixture.repository);
        assert!(
            fixture
                .repository
                .admit_startup_reattach(&command, 300)
                .is_err(),
            "{kind}"
        );
        assert_eq!(snapshot(&fixture.repository), before);
        assert!(fixture
            .repository
            .get_operation(&command.operation_id)
            .unwrap()
            .is_none());
        db.execute_batch("DROP TRIGGER startup_admission_event_fault")
            .unwrap();
        let grant = execute(&fixture.repository, &command, 301);
        assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        let events = fixture
            .repository
            .list_runtime_events(
                Some((RuntimeEventScope::Operation, &command.operation_id)),
                None,
                10,
            )
            .unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["operation.admitted", "operation.transitioned"]
        );
        assert!(events
            .iter()
            .all(|event| event.criticality == RuntimeEventCriticality::Critical));
    }
}

#[test]
fn late_completion_critical_event_failure_rolls_back_receipt_and_never_reauthorizes_native_resume()
{
    for hold_event_also_fails in [false, true] {
        let fixture = Fixture::new(true);
        let grant = fixture.grant("startup_completion_event_fault");
        let acknowledgement = ack(&grant, true);
        let db = open_connection(&fixture.repository.database_path).unwrap();
        let phase_condition = if hold_event_also_fails {
            "1"
        } else {
            "json_extract(NEW.payload_json,'$.to_phase')='completed'"
        };
        db.execute_batch(&format!(
            "CREATE TRIGGER startup_completion_event_fault BEFORE INSERT ON runtime_events
             WHEN NEW.kind='operation.transitioned'
               AND json_extract(NEW.payload_json,'$.operation_id')='startup_completion_event_fault'
               AND json_extract(NEW.payload_json,'$.sequence')=2
               AND ({phase_condition})
             BEGIN SELECT RAISE(ABORT,'startup completion Critical event injection'); END;"
        ))
        .unwrap();
        let before = snapshot(&fixture.repository);
        assert!(fixture
            .repository
            .complete_startup_reattach(&grant, &acknowledgement, 400)
            .is_err());
        assert_eq!(snapshot(&fixture.repository), before);
        assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        assert_eq!(fixture.session().canonical_provider_session_ref, None);
        if hold_event_also_fails {
            assert!(fixture
                .repository
                .hold_startup_reattach(
                    &grant,
                    Some(&acknowledgement),
                    "both receipt journals failed",
                    500,
                )
                .is_err());
            assert_eq!(snapshot(&fixture.repository), before);
            assert_attempt(&fixture.repository, &grant, "requested", "started", true);
            let reopened =
                crate::SqliteRuntimeRepository::new(fixture.repository.database_path.clone());
            assert_unresolved(
                reopened.admit_startup_reattach(
                    &fixture.command("startup_after_double_fault", "double_fault_restart"),
                    501,
                ),
                &grant.intent.command.operation_id,
            );
            assert_eq!(snapshot(&fixture.repository), before);
            db.execute_batch("DROP TRIGGER startup_completion_event_fault")
                .unwrap();
        }
        hold_observed(&fixture.repository, &grant, &acknowledgement);
        if !hold_event_also_fails {
            db.execute_batch("DROP TRIGGER startup_completion_event_fault")
                .unwrap();
        }
        let before = snapshot(&fixture.repository);
        assert!(matches!(
            fixture.repository.admit_startup_reattach(&grant.intent.command, 600).unwrap(),
            StartupReattachAdmission::Unresolved { operation_id } if operation_id == grant.intent.command.operation_id
        ));
        assert_unresolved(
            fixture.repository.admit_startup_reattach(
                &fixture.command("startup_after_event_fault", "event_fault_restart"),
                601,
            ),
            &grant.intent.command.operation_id,
        );
        assert!(fixture
            .repository
            .complete_startup_reattach(&grant, &acknowledgement, 602)
            .is_err());
        assert_eq!(snapshot(&fixture.repository), before);
        let events = fixture
            .repository
            .list_runtime_events(
                Some((
                    RuntimeEventScope::Operation,
                    &grant.intent.command.operation_id,
                )),
                None,
                10,
            )
            .unwrap();
        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .all(|event| event.criticality == RuntimeEventCriticality::Critical));
        assert_eq!(events.last().unwrap().payload["to_phase"], "manual_review");
    }
}
