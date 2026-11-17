use super::*;
#[test]
fn acknowledged_close_replays_and_a_later_attachment_requires_a_new_close() {
    let (temp, repository, workspace) = setup();
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "known-close-agent", "known-close-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute("INSERT INTO turns(id,session_id,status,input_json,completed_at) VALUES('historical-terminal-turn',?1,'interrupted','[]',100)",[&session.id]).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "known-close-retirement");
    repository.admit_workspace_retirement(&command).unwrap();
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &session.id, 200)
        .unwrap();
    repository
        .finalize_workspace_retirement_native_close(
            &attempt,
            &runtime_core::ProviderRetirementCloseEvidence {
                runtime_session_id: session.id.clone(),
                provider_session_ref: session.provider_session_ref.clone().unwrap(),
                proof_kind:
                    runtime_core::ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
            },
            201,
        )
        .unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 202)
        .unwrap();
    repository
        .finalize_settled_workspace_retirement(&command.operation_id, 203)
        .unwrap()
        .unwrap();
    let completed = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let before = crate::tests::legacy_native_turn_guards::raw_state(&db);
    assert!(matches!(
        repository.admit_workspace_retirement(&command).unwrap(),
        runtime_core::WorkspaceRetirementAdmission::Replay(_)
    ));
    assert_eq!(
        crate::tests::legacy_native_turn_guards::raw_state(&db),
        before
    );
    let active_workspace = register(
        &repository,
        std::path::Path::new(&workspace.canonical_root),
        "after-known-close",
    );
    // A separate restored generation, without clearing any unknown operation.
    db.execute("UPDATE workspace_agents SET lifecycle_state='active',archived_at=NULL,revision=revision+1 WHERE session_id=?1",[&session.id]).unwrap();
    session.active_turn_id = None;
    session.status = "ready".into();
    session.closed_at = None;
    session.updated_at = 300;
    repository.upsert_session(&session).unwrap();
    let next = retirement_command(&active_workspace, "new-generation-retirement");
    repository.admit_workspace_retirement(&next).unwrap();
    let close = repository
        .get_operation(&next.operation_id)
        .unwrap()
        .unwrap()
        .effects
        .into_iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    assert_eq!(close.phase, "intended");
    assert_eq!(close.attempt_count, 0);
    assert_eq!(
        repository.get_operation(&command.operation_id).unwrap(),
        Some(completed)
    );
    let historical_status: String = db
        .query_row(
            "SELECT status FROM turns WHERE id='historical-terminal-turn'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(historical_status, "interrupted");
}
