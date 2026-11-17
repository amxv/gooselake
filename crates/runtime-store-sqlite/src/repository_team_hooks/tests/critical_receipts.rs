use super::*;

pub(super) fn raw(repo: &SqliteRuntimeRepository) -> serde_json::Value {
    let db = open_connection(&repo.database_path).unwrap();
    let specs=[
        ("workspace_hook_effects","workspace_id,parent_event_id,event_name,hook_name,hook_json,input_json,phase,attempt_count,receipt_json,created_at_ms,updated_at_ms,application_state,application_error_code,applied_at_ms","workspace_id,parent_event_id,event_name,hook_name"),
        ("runtime_events","event_id,scope,scope_id,session_id,team_id,turn_id,seq,kind,critical,payload_json,provider,provider_seq,created_at","event_id"),
        ("workspace_system_notices","notice_id,workspace_id,recipient_agent_id,kind,correlation_id,input_json,state,created_at_ms,updated_at_ms","notice_id"),
        ("runtime_operations","operation_id,workspace_id,phase,exact_terminal_result_json,error_code,error_message,updated_at","operation_id"),
        ("runtime_operation_effects","effect_id,operation_id,effect_kind,target_id,phase,attempt_count,evidence_json,updated_at","effect_id"),
        ("runtime_operation_resource_claims","resource_kind,resource_id,owner_operation_id,fence_generation","resource_kind,resource_id"),
        ("runtime_operation_transitions","operation_id,sequence,from_phase,to_phase,evidence_json,created_at","operation_id,sequence"),
        ("workspace_retirement_fences","operation_id,workspace_id,phase,snapshot_json,updated_at","operation_id"),
        ("workspace_retirement_reviews","review_id,operation_id,effect_id,actor_id,idempotency_key,normalized_request_hash,normalized_request_json,receipt_json,reviewed_at","review_id"),
    ];
    let mut result = serde_json::Map::new();
    for (table, fields, order) in specs {
        let query = format!("SELECT json_array({fields}) FROM {table} ORDER BY {order}");
        let mut statement = db.prepare(&query).unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        result.insert(table.into(), json!(rows));
    }
    serde_json::Value::Object(result)
}

pub(super) fn fault(repo: &SqliteRuntimeRepository, origin: &str, body: &str) {
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER refuse_hook_journal BEFORE INSERT ON runtime_events
         WHEN NEW.kind='workspace.hook_effect_transitioned'
           AND json_extract(NEW.payload_json,'$.origin')='{origin}'
         BEGIN {body} END;"
        ))
        .unwrap();
}
pub(super) fn clear_fault(repo: &SqliteRuntimeRepository) {
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER refuse_hook_journal")
        .unwrap();
}
pub(super) fn events(repo: &SqliteRuntimeRepository) -> Vec<serde_json::Value> {
    let db = open_connection(&repo.database_path).unwrap();
    let mut statement = db
        .prepare(
            "SELECT payload_json,critical FROM runtime_events
        WHERE kind='workspace.hook_effect_transitioned' ORDER BY seq",
        )
        .unwrap();
    let events = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .map(|row| {
            let (raw, critical) = row.unwrap();
            assert_eq!(critical, 1);
            serde_json::from_str(&raw).unwrap()
        })
        .collect();
    events
}
fn terminal_receipt() -> TeamHookEffectReceipt {
    TeamHookEffectReceipt {
        validated_output: Some(
            json!({"inject":{"caller":{"text":"private_context_token fixture secret"}}}),
        ),
        error_code: None,
    }
}

#[test]
fn hook_intent_receipt_failure_rolls_back_and_exact_replay_is_observation_only() {
    for sql in [
        "SELECT RAISE(ABORT,'receipt fault');",
        "SELECT RAISE(IGNORE);",
    ] {
        let (_temp, repo) = fixture();
        approve(&repo, 0, vec![hook()]);
        let before = raw(&repo);
        fault(&repo, "direct_admission", sql);
        assert!(repo.admit_team_hook_effect(&admission()).is_err());
        assert_eq!(raw(&repo), before);
        clear_fault(&repo);
        let admitted = repo.admit_team_hook_effect(&admission()).unwrap();
        let committed = raw(&repo);
        assert_eq!(repo.admit_team_hook_effect(&admission()).unwrap(), admitted);
        assert_eq!(raw(&repo), committed);
        let journal = events(&repo);
        assert_eq!(journal.len(), 1);
        assert_eq!(journal[0]["after"]["phase"], "intended");
        assert!(!journal[0].to_string().contains("printf ok"));
    }
}

#[test]
fn hook_claim_and_terminal_events_are_atomic_and_known_terminal_replay_survives_revocation() {
    let (_temp, repo) = fixture();
    approve(&repo, 0, vec![hook()]);
    let request = admission();
    let intended = repo.admit_team_hook_effect(&request).unwrap();
    let before = raw(&repo);
    fault(
        &repo,
        "execution_claim",
        "SELECT RAISE(ABORT,'claim fault');",
    );
    assert!(repo
        .claim_team_hook_effect("ws_one", "evt_one", hook().event_name, "bootstrap", 10)
        .is_err());
    assert_eq!(raw(&repo), before);
    clear_fault(&repo);
    let started = repo
        .claim_team_hook_effect("ws_one", "evt_one", hook().event_name, "bootstrap", 10)
        .unwrap();
    assert_eq!(started.phase, TeamHookEffectPhase::Started);
    assert_eq!(started.attempt_count, intended.attempt_count + 1);
    let second = SqliteRuntimeRepository::new(repo.database_path.clone());
    let before = raw(&repo);
    assert!(second
        .claim_team_hook_effect("ws_one", "evt_one", hook().event_name, "bootstrap", 11)
        .is_err());
    assert_eq!(raw(&repo), before);
    fault(
        &repo,
        "execution_receipt",
        "SELECT RAISE(ABORT,'terminal fault');",
    );
    assert!(repo
        .finish_team_hook_effect(
            "ws_one",
            "evt_one",
            hook().event_name,
            "bootstrap",
            TeamHookEffectPhase::Succeeded,
            terminal_receipt(),
            12
        )
        .is_err());
    assert_eq!(raw(&repo), before);
    clear_fault(&repo);
    let terminal = repo
        .finish_team_hook_effect(
            "ws_one",
            "evt_one",
            hook().event_name,
            "bootstrap",
            TeamHookEffectPhase::Succeeded,
            terminal_receipt(),
            12,
        )
        .unwrap();
    approve(&repo, 1, vec![]);
    let before = raw(&repo);
    assert_eq!(
        second
            .finish_team_hook_effect(
                "ws_one",
                "evt_one",
                hook().event_name,
                "bootstrap",
                TeamHookEffectPhase::Succeeded,
                terminal_receipt(),
                99
            )
            .unwrap(),
        terminal
    );
    assert_eq!(raw(&repo), before);
    let journal = events(&repo);
    assert_eq!(journal.len(), 3);
    assert!(!serde_json::to_string(&journal)
        .unwrap()
        .contains("private_context_token"));
    assert!(!serde_json::to_string(&journal)
        .unwrap()
        .contains("fixture secret"));
}

#[test]
fn hook_receipt_requires_critical_exact_event_and_retained_carrier() {
    for body in [
        "UPDATE runtime_events SET critical=0 WHERE event_id=NEW.event_id;",
        "UPDATE runtime_events SET seq=seq+100 WHERE event_id=NEW.event_id;",
        "UPDATE runtime_events SET payload_json='{}' WHERE event_id=NEW.event_id;",
        "DELETE FROM runtime_events WHERE event_id=NEW.event_id;",
        "UPDATE workspace_hook_effects SET application_state='applied';",
    ] {
        let (_temp, repo) = fixture();
        approve(&repo, 0, vec![hook()]);
        let before = raw(&repo);
        open_connection(&repo.database_path)
            .unwrap()
            .execute_batch(&format!(
                "CREATE TRIGGER damage_hook_receipt AFTER INSERT ON runtime_events
             WHEN NEW.kind='workspace.hook_effect_transitioned' BEGIN {body} END;"
            ))
            .unwrap();
        assert!(repo.admit_team_hook_effect(&admission()).is_err(), "{body}");
        assert_eq!(raw(&repo), before, "{body}");
    }
}

#[test]
fn hook_occupied_fresh_event_identity_never_grants_another_intent() {
    let (_temp, repo) = fixture();
    approve(&repo, 0, vec![hook()]);
    repo.admit_team_hook_effect(&admission()).unwrap();
    // An orphan receipt is not a fresh effect grant.
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DELETE FROM workspace_hook_effects")
        .unwrap();
    let before = raw(&repo);
    assert!(repo.admit_team_hook_effect(&admission()).is_err());
    assert_eq!(raw(&repo), before);
}

#[test]
fn queued_pre_add_parent_and_all_effect_intents_share_the_critical_commit() {
    for fault in 0..3 {
        let (_temp, repo) = fixture();
        let mut first = hook();
        first.event_name = TeamHookEventName::PreTeammateAdd;
        let mut second = first.clone();
        second.name = "second".into();
        approve(&repo, 0, vec![first, second]);
        let request = runtime_core::TeamHookPreAddAdmission {
            workspace_id: "ws_one".into(),
            reserved_agent_id: "sess_atomic_parent".into(),
            actor_agent_id: None,
            provider: "codex".into(),
            model: None,
            planned_cwd: "/tmp/ws_one".into(),
            member_title: None,
            onboarding_prompt: None,
            created_at_ms: 20,
        };
        let before = raw(&repo);
        let (when,body)=match fault {
        0=>("BEFORE","SELECT RAISE(ABORT,'second intent fault');"),
        1=>("AFTER","UPDATE workspace_hook_effects SET application_state='applied' WHERE hook_name='bootstrap';"),
        _=>("AFTER","UPDATE runtime_events SET seq=seq+100 WHERE kind='workspace.hook_effect_transitioned' AND json_extract(payload_json,'$.hook_name')='bootstrap';"),
    };
        open_connection(&repo.database_path)
            .unwrap()
            .execute_batch(&format!(
                "CREATE TRIGGER refuse_second_hook {when} INSERT ON runtime_events
         WHEN NEW.kind='workspace.hook_effect_transitioned'
           AND json_extract(NEW.payload_json,'$.hook_name')='second'
         BEGIN {body} END;"
            ))
            .unwrap();
        assert!(repo.admit_pre_add_team_hooks(&request).is_err());
        assert_eq!(raw(&repo), before);
        open_connection(&repo.database_path)
            .unwrap()
            .execute_batch("DROP TRIGGER refuse_second_hook")
            .unwrap();
        let effects = repo.admit_pre_add_team_hooks(&request).unwrap();
        assert_eq!(effects.len(), 2);
        assert_eq!(events(&repo).len(), 2, "one Critical intent per effect");
        let before = raw(&repo);
        assert_eq!(repo.admit_pre_add_team_hooks(&request).unwrap(), effects);
        assert_eq!(raw(&repo), before);
    }
}
