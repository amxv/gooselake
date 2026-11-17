use super::support::*;
use crate::db::open_connection;
use rusqlite::params;
use serde_json::{json, Value};

fn transcript_metadata() -> Value {
    json!({
        "source":"startup-terminal-witness",
        "untouched":{"nested":[1,2,3]},
        "codex_transcript":(0..79).map(|index| json!({
            "role":"assistant", "text":format!("old-{index}"),
        })).collect::<Vec<_>>(),
    })
}

// Deliberately construct the expected transcript without the production helper.
// Two new entries replace the oldest of 79 legacy entries at the 80-entry limit.
fn completed_metadata() -> Value {
    let mut transcript = (1..79)
        .map(|index| {
            json!({
                "role":"assistant", "text":format!("old-{index}"),
            })
        })
        .collect::<Vec<_>>();
    transcript.push(json!({"role":"user","text":"user prompt"}));
    transcript.push(json!({"role":"assistant","text":"done"}));
    assert_eq!(transcript.len(), 80);
    json!({
        "source":"startup-terminal-witness",
        "untouched":{"nested":[1,2,3]},
        "session_transcript":transcript,
    })
}

#[test]
fn actual_completed_and_interrupted_receipts_supply_exact_terminal_metadata_witness() {
    for status in ["completed", "interrupted"] {
        let fixture = Fixture::with_metadata(true, transcript_metadata());
        let running = running(&fixture);
        let grant = fixture.grant("startup_terminal_witness");
        let witness = grant.intent.terminal_metadata_witness.as_ref().unwrap();
        assert_eq!(witness.turn, running.turn);
        assert_eq!(witness.admission.as_ref(), Some(&running.admission));
        let acknowledgement = ack(&grant, false);
        let (finished_session, finished_turn, terminal_event) =
            terminal(&fixture.repository, &running, status, completed_metadata());
        let result = fixture
            .repository
            .complete_startup_reattach(&grant, &acknowledgement, 500)
            .unwrap();
        assert_eq!(
            result.receipt.metadata_terminal_event_id.as_deref(),
            Some(terminal_event.event_id.as_str())
        );
        assert_eq!(result.session, finished_session);
        assert_eq!(fixture.session(), finished_session);
        assert_attempt(&fixture.repository, &grant, "completed", "finalized", false);
        let durable = fixture
            .repository
            .get_persisted_retirement_review_session(SESSION)
            .unwrap()
            .unwrap();
        assert_eq!(durable.turns, vec![finished_turn]);
        assert_eq!(durable.admissions, vec![running.admission]);
        let before = snapshot(&fixture.repository);
        let replay = fixture
            .repository
            .complete_startup_reattach(&grant, &acknowledgement, 700)
            .unwrap();
        assert_eq!(replay.session, finished_session);
        assert_eq!(
            serde_json::to_value(replay.receipt).unwrap(),
            serde_json::to_value(result.receipt).unwrap()
        );
        assert_eq!(snapshot(&fixture.repository), before);
    }
}

#[test]
fn metadata_exception_rejects_missing_or_changed_canonical_witness_without_overwriting_it() {
    for corruption in 0..8 {
        let fixture = Fixture::with_metadata(true, transcript_metadata());
        let running = running(&fixture);
        let grant = fixture.grant("startup_corrupt_terminal_witness");
        let acknowledgement = ack(&grant, false);
        let (_, _, event) = terminal(
            &fixture.repository,
            &running,
            "completed",
            completed_metadata(),
        );
        let db = open_connection(&fixture.repository.database_path).unwrap();
        match corruption {
            0 => {
                db.execute(
                    "DELETE FROM runtime_events WHERE event_id=?1",
                    [&event.event_id],
                )
                .unwrap();
            }
            1 => {
                db.execute(
                    "UPDATE runtime_events SET scope_id='other-session' WHERE event_id=?1",
                    [&event.event_id],
                )
                .unwrap();
            }
            2 => {
                db.execute("UPDATE runtime_events SET payload_json=json_set(payload_json,'$.extra',true) WHERE event_id=?1", [&event.event_id]).unwrap();
            }
            3 => {
                db.execute(
                    "UPDATE runtime_events SET critical=0 WHERE event_id=?1",
                    [&event.event_id],
                )
                .unwrap();
            }
            4 => {
                db.execute("UPDATE sessions SET metadata_json=json_set(metadata_json,'$.unrelated',true) WHERE id=?1", [SESSION]).unwrap();
            }
            5 => {
                db.execute("UPDATE sessions SET metadata_json=json_set(metadata_json,'$.session_transcript[79].text','other answer') WHERE id=?1", [SESSION]).unwrap();
            }
            6 => {
                db.execute("UPDATE turn_admissions SET provider_native_turn_id='other-native-turn' WHERE turn_id=?1", [&running.turn.id]).unwrap();
            }
            7 => {
                db.execute(
                    "UPDATE turns SET input_json=?2 WHERE id=?1",
                    params![
                        running.turn.id,
                        json!([{"type":"text","text":"other input"}]).to_string()
                    ],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = snapshot(&fixture.repository);
        assert!(
            fixture
                .repository
                .complete_startup_reattach(&grant, &acknowledgement, 500)
                .is_err(),
            "corrupt canonical witness case {corruption} was accepted"
        );
        assert_eq!(snapshot(&fixture.repository), before);
        hold_observed(&fixture.repository, &grant, &acknowledgement);
        let projection = fixture
            .repository
            .get_persisted_retirement_review_session(SESSION)
            .unwrap()
            .unwrap();
        assert_eq!(projection.session.active_turn_id, None);
        assert_eq!(projection.turns[0].status, "completed");
    }
}
