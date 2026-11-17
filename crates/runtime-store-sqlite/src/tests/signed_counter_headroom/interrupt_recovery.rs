use super::*;

#[test]
fn recovered_interrupt_effect_timestamp_must_not_promote_to_real() {
    let (temp, repository, mut session, agent) = agent_fixture();
    session.active_turn_id = Some("synthetic-active-turn".into());
    session.status = "turn_running".into();
    repository.upsert_session(&session).unwrap();
    let command = prepare_workspace_interrupt(
        &agent.workspace_id,
        OperationActor::operator("signed-timestamp-test"),
        Some("signed-timestamp-recovery".into()),
    )
    .unwrap();
    let plan = match repository.begin_workspace_interrupt(&command).unwrap() {
        WorkspaceInterruptAdmission::Execute(plan) => plan,
        other => panic!("unexpected initial interrupt admission: {other:?}"),
    };
    assert_eq!(plan.targets.len(), 1);
    repository
        .mark_workspace_interrupt_started(
            &plan.operation_id,
            &session.id,
            "synthetic-active-turn",
            200,
        )
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE runtime_operation_effects SET updated_at=?2
         WHERE operation_id=?1 AND effect_kind='workspace_turn_interrupt'",
        params![plan.operation_id, i64::MAX],
    )
    .unwrap();
    // Only the original operation's durable interrupt receipt can certify
    // this effect. A cleared active-turn projection alone is not proof.
    repository
        .append_runtime_event(&runtime_core::NewRuntimeEvent {
            event_id: "evt_signed_interrupt_receipt".into(),
            scope: runtime_core::RuntimeEventScope::Session,
            scope_id: session.id.clone(),
            session_id: Some(session.id.clone()),
            team_id: None,
            turn_id: Some("synthetic-active-turn".into()),
            kind: "turn.interrupt_requested".into(),
            criticality: runtime_core::RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"operation_id":plan.operation_id}),
            provider: None,
            provider_seq: None,
            created_at: 201,
        })
        .unwrap();
    // Recovery must not redispatch or promote its signed timestamp to REAL.
    db.execute(
        "UPDATE sessions SET active_turn_id=NULL WHERE id=?1",
        [&session.id],
    )
    .unwrap();
    let error = repository.begin_workspace_interrupt(&command).unwrap_err();
    assert!(
        matches!(
            &error,
            RuntimeError::OperationUnresolved {operation_id,..} if operation_id==&plan.operation_id
        ),
        "{error}"
    );
    assert!(
        error.to_string().contains("signed timestamp headroom"),
        "{error}"
    );
    let (updated, kind, phase): (i64, String, String) = db
        .query_row(
            "SELECT updated_at,typeof(updated_at),phase FROM runtime_operation_effects
         WHERE operation_id=?1 AND effect_kind='workspace_turn_interrupt'",
            [&plan.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (updated, kind, phase),
        (i64::MAX, "integer".into(), "started".into())
    );

    db.execute(
        "UPDATE runtime_operation_effects SET updated_at=?2
         WHERE operation_id=?1 AND effect_kind='workspace_turn_interrupt'",
        params![plan.operation_id, i64::MAX - 1],
    )
    .unwrap();
    let replay = repository.begin_workspace_interrupt(&command).unwrap();
    assert!(
        matches!(replay, WorkspaceInterruptAdmission::Execute(plan) if plan.targets.is_empty())
    );
    let (updated, kind, phase): (i64, String, String) = db
        .query_row(
            "SELECT updated_at,typeof(updated_at),phase FROM runtime_operation_effects
         WHERE operation_id=?1 AND effect_kind='workspace_turn_interrupt'",
            [&plan.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (updated, kind, phase),
        (i64::MAX, "integer".into(), "finalized".into())
    );
}
