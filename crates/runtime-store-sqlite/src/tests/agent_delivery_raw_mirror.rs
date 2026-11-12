use super::*;
use runtime_core::{
    AgentDeliveryRecord, AgentMessageContextKind, AgentMessageRecord, RuntimeError,
};

fn seed_certified_mirror() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    AgentDeliveryRecord,
    TeamDeliveryRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    db.execute_batch(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
         VALUES('team_exact','Legacy','sender','test',1,1);
         INSERT INTO team_messages(id,team_id,scope,sender_agent_id,
             recipient_agent_ids_json,input_json,priority,policy,created_at)
         VALUES('msg_exact','team_exact','direct','sender','[\"sess_one\"]','[]',
             'normal','non_interrupting',100);",
    )
    .unwrap();
    let message = AgentMessageRecord {
        id: "msg_exact".into(),
        scope: "direct".into(),
        context_kind: AgentMessageContextKind::LegacyTeam,
        workspace_id: None,
        legacy_team_id: Some("team_exact".into()),
        sender_agent_id: "sender".into(),
        recipient_agent_ids: vec!["sess_one".into()],
        input: serde_json::json!([{"type":"text","text":"hello"}]),
        image_paths: vec![],
        priority: "normal".into(),
        policy: "non_interrupting".into(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: None,
        created_at: 100,
    };
    let canonical = AgentDeliveryRecord {
        id: "delivery_exact".into(),
        message_id: message.id.clone(),
        recipient_agent_id: "sess_one".into(),
        provider: "codex".into(),
        status: "pending".into(),
        effective_policy: Some("non_interrupting".into()),
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 100,
        updated_at: 100,
    };
    repository
        .insert_agent_message_with_deliveries(&message, &[canonical.clone()])
        .unwrap();
    let legacy = TeamDeliveryRecord {
        id: canonical.id.clone(),
        message_id: message.id,
        team_id: "team_exact".into(),
        recipient_agent_id: canonical.recipient_agent_id.clone(),
        provider: canonical.provider.clone(),
        status: canonical.status.clone(),
        effective_policy: canonical.effective_policy.clone(),
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: canonical.created_at,
        updated_at: canonical.updated_at,
    };
    repository.upsert_team_delivery(&legacy).unwrap();
    (temp, repository, canonical, legacy)
}

#[test]
fn certified_canonical_raw_mutations_and_recreation_fail_closed() {
    let (temp, repository, original, _) = seed_certified_mirror();
    let attempts = [
        original.clone(),
        AgentDeliveryRecord {
            status: "deferred".into(),
            updated_at: 101,
            ..original.clone()
        },
        AgentDeliveryRecord {
            status: "failed".into(),
            updated_at: 101,
            ..original.clone()
        },
        AgentDeliveryRecord {
            recipient_agent_id: "foreign_recipient".into(),
            updated_at: 101,
            ..original.clone()
        },
        AgentDeliveryRecord {
            provider: "claude".into(),
            updated_at: 101,
            ..original.clone()
        },
        AgentDeliveryRecord {
            message_id: "historical_uncertified".into(),
            updated_at: 101,
            ..original.clone()
        },
    ];
    for forged in attempts {
        assert!(
            matches!(
                repository.upsert_agent_delivery(&forged),
                Err(RuntimeError::Conflict(_))
            ),
            "raw certified upsert must fail: {forged:?}"
        );
        assert_eq!(
            repository.get_agent_delivery(&original.id).unwrap(),
            Some(original.clone())
        );
    }
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute("DELETE FROM team_deliveries WHERE id=?1", [&original.id])
        .unwrap();
    db.execute("DELETE FROM agent_deliveries WHERE id=?1", [&original.id])
        .unwrap();
    assert!(
        repository.upsert_agent_delivery(&original).is_err(),
        "deleted certified row cannot be silently recreated behind message.sent"
    );
}

#[test]
fn mirrored_status_turn_provider_recipient_and_team_must_match_exact_canonical_source() {
    let (temp, repository, canonical, legacy) = seed_certified_mirror();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut variants = Vec::new();
    variants.push(TeamDeliveryRecord {
        status: "injected".into(),
        updated_at: 101,
        injected_turn_id: Some("fake".into()),
        ..legacy.clone()
    });
    variants.push(TeamDeliveryRecord {
        status: "failed".into(),
        updated_at: 101,
        ..legacy.clone()
    });
    variants.push(TeamDeliveryRecord {
        provider: "claude".into(),
        ..legacy.clone()
    });
    variants.push(TeamDeliveryRecord {
        recipient_agent_id: "foreign".into(),
        ..legacy.clone()
    });
    variants.push(TeamDeliveryRecord {
        team_id: "other_team".into(),
        ..legacy.clone()
    });
    variants.push(TeamDeliveryRecord {
        injected_turn_id: Some("fake".into()),
        ..legacy.clone()
    });
    variants.push(TeamDeliveryRecord {
        last_error_code: Some("forged".into()),
        ..legacy.clone()
    });
    for forged in variants {
        assert!(
            matches!(
                repository.upsert_team_delivery(&forged),
                Err(RuntimeError::Conflict(_))
            ),
            "forged canonical-owned legacy mirror must be rejected: {forged:?}"
        );
    }
    assert!(
        repository.upsert_team_delivery(&legacy).is_ok(),
        "matching canonical-owned mirror reapplication remains legal"
    );
    let mut after = canonical.clone();
    after.status = "deferred".into();
    after.last_error_code = Some("busy_before_dispatch".into());
    after.updated_at = 102;
    assert!(repository
        .compare_and_swap_agent_delivery_status(&canonical, &after)
        .unwrap());
    let current_mirror: (String, Option<String>, i64) = db
        .query_row(
            "SELECT status,last_error_code,updated_at FROM team_deliveries WHERE id=?1",
            [&legacy.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        current_mirror,
        ("deferred".into(), Some("busy_before_dispatch".into()), 102)
    );
    assert!(
        repository.upsert_team_delivery(&legacy).is_err(),
        "a stale but formerly matching legacy mirror may not roll back a certified generation"
    );
    let current = TeamDeliveryRecord {
        status: after.status.clone(),
        last_error_code: after.last_error_code.clone(),
        updated_at: after.updated_at,
        ..legacy.clone()
    };
    assert!(repository.upsert_team_delivery(&current).is_ok());
    assert_eq!(
        db.query_row("SELECT count(*) FROM runtime_events WHERE kind='agent_delivery.deferred' AND critical=1",[], |row|row.get::<_,i64>(0)).unwrap(),
        1,
        "legacy projection replay is not another authoritative delivery transition"
    );
}

#[test]
fn historical_uncertified_legacy_only_upserts_remain_compatible() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    db.execute_batch(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
         VALUES('team_old','Old','old_sender','test',1,1);
         INSERT INTO team_messages(id,team_id,scope,sender_agent_id,
             recipient_agent_ids_json,input_json,priority,policy,created_at)
         VALUES('msg_old','team_old','direct','old_sender','[\"old_recipient\"]','[]',
             'normal','non_interrupting',1);",
    )
    .unwrap();
    let mut historical = TeamDeliveryRecord {
        id: "old_delivery".into(),
        message_id: "msg_old".into(),
        team_id: "team_old".into(),
        recipient_agent_id: "old_recipient".into(),
        provider: "codex".into(),
        status: "pending".into(),
        effective_policy: None,
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 1,
        updated_at: 1,
    };
    repository.upsert_team_delivery(&historical).unwrap();
    historical.status = "failed".into();
    historical.updated_at = 2;
    repository.upsert_team_delivery(&historical).unwrap();
    let (status, revision): (String, i64) = db
        .query_row(
            "SELECT status,updated_at FROM team_deliveries WHERE id=?1",
            [&historical.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (status, revision),
        (historical.status, historical.updated_at)
    );
}

#[test]
fn native_acknowledged_mirror_only_replays_exact_correlated_turn_after_restart() {
    let (temp, repository, pending, legacy) = seed_certified_mirror();
    let mut session = sample_session();
    session.id = pending.recipient_agent_id.clone();
    session.status = "ready".into();
    session.active_turn_id = None;
    session.worktree_id = None;
    repository.upsert_session(&session).unwrap();
    let mut turn = sample_turn();
    turn.id = "positive-committed-turn".into();
    turn.session_id = session.id;
    turn.source = Some("agent_message_delivery_transport".into());
    repository.upsert_turn(&turn).unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    db.execute(
        "INSERT INTO turn_admissions(
          turn_id,session_id,provider,projection_source,user_input_snapshot_json,
          dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at)
         VALUES(?1,'sess_one','codex','agent_message_delivery_transport',
          '{}','{}','{\"correlation_id\":\"msg_exact\"}','dispatched',101,101)",
        [turn.id],
    )
    .unwrap();
    let mut injecting = pending.clone();
    injecting.status = "injecting".into();
    injecting.updated_at = 101;
    assert!(repository
        .compare_and_swap_agent_delivery_status(&pending, &injecting)
        .unwrap());
    let accepted = repository
        .commit_agent_delivery_injected(&injecting, "positive-committed-turn", 102)
        .unwrap();
    let exact_mirror = TeamDeliveryRecord {
        status: accepted.status.clone(),
        injection_strategy: accepted.injection_strategy.clone(),
        injected_turn_id: accepted.injected_turn_id.clone(),
        updated_at: accepted.updated_at,
        ..legacy.clone()
    };
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert!(reopened.upsert_team_delivery(&exact_mirror).is_ok());
    let mut forged = exact_mirror.clone();
    forged.injected_turn_id = Some("unrelated-native-turn".into());
    assert!(matches!(
        reopened.upsert_team_delivery(&forged),
        Err(RuntimeError::Conflict(_))
    ));
    forged = exact_mirror.clone();
    forged.updated_at += 1;
    assert!(matches!(
        reopened.upsert_team_delivery(&forged),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(reopened.upsert_agent_delivery(&pending).is_err());
    assert_eq!(
        reopened.get_agent_delivery(&accepted.id).unwrap(),
        Some(accepted)
    );
    assert_eq!(db.query_row(
        "SELECT count(*) FROM runtime_events WHERE kind='agent_delivery.injected' AND critical=1",
        [], |row|row.get::<_,i64>(0)
    ).unwrap(),1);
}
