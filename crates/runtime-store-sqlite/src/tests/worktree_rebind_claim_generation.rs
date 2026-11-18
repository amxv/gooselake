use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::{
    ManagedWorktreeClaimTransition::Release, ProviderWorkspaceRebindEvidence, RuntimeError,
    WorkspaceAgentRebindOperation, WorkspaceAgentRecord,
};

struct Fixture {
    temp: tempfile::TempDir,
    repository: SqliteRuntimeRepository,
    operation: WorkspaceAgentRebindOperation,
    session: SessionRecord,
    agent: WorkspaceAgentRecord,
    prior: Option<ManagedWorktreeClaimRecord>,
}
fn fixture(same_key: bool, generation: i64, prior_state: &str) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (mut session, mut agent) = workspace_agent_fixture(&workspace, "n06-agent", "n06-agent");
    let source = ManagedWorktreeRecord {
        id: "wt-source".into(),
        repo_root: workspace.canonical_root.clone(),
        worktree_root: temp.path().join("worktrees").display().to_string(),
        worktree_cwd: temp.path().join("worktrees/a").display().to_string(),
        branch_name: "gg/a".into(),
        worktree_name: "a".into(),
        unified_workspace_path: "n06".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 10,
        updated_at: 10,
    };
    repository.upsert_managed_worktree(&source).unwrap();
    session.cwd = Some(source.worktree_cwd.clone());
    session.worktree_id = Some(source.id.clone());
    agent.recreation_policy.authoritative_cwd = source.worktree_cwd.clone();
    repository.create_workspace_agent(&session, &agent).unwrap();
    let agent = repository
        .get_workspace_agent(&workspace.workspace_id, &session.id)
        .unwrap()
        .unwrap();
    let session = repository
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.id == session.id)
        .unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    if prior_state == "absent" {
        db.execute(
            "DELETE FROM managed_worktree_claims WHERE session_id='n06-agent'",
            [],
        )
        .unwrap();
    } else {
        db.execute("UPDATE managed_worktree_claims SET claim_generation=?1,released_at=?2 WHERE session_id='n06-agent'",
            params![generation,if prior_state=="terminal"{Some(120)}else{None}]).unwrap();
    }
    let prior = repository
        .get_managed_worktree_claim(&source.id, &session.id)
        .unwrap();
    let destination = ManagedWorktreeRecord {
        id: if same_key {
            source.id.clone()
        } else {
            "wt-destination".into()
        },
        worktree_cwd: temp.path().join("worktrees/b").display().to_string(),
        branch_name: "gg/b".into(),
        worktree_name: "b".into(),
        updated_at: 20,
        ..source.clone()
    };
    repository.upsert_managed_worktree(&destination).unwrap();
    let operation = WorkspaceAgentRebindOperation {
        operation_id: "n06-rebind".into(),
        workspace_id: workspace.workspace_id,
        agent_id: session.id.clone(),
        idempotency_key: Some("n06-rebind".into()),
        expected_revision: 0,
        previous_worktree_id: Some(source.id),
        destination_worktree_id: Some(destination.id),
        previous_cwd: source.worktree_cwd,
        destination_cwd: destination.worktree_cwd,
        cleanup_previous_worktree: false,
        previous_cleanup_status: None,
        previous_cleanup_diagnostic: None,
        phase: "intended".into(),
        provider_evidence: None,
        error_code: None,
        created_at: 200,
        updated_at: 200,
    };
    Fixture {
        temp,
        repository,
        operation,
        session,
        agent,
        prior,
    }
}
fn state(f: &Fixture) -> serde_json::Value {
    let db = open_connection(&f.repository.database_path).unwrap();
    let mut stmt=db.prepare("SELECT resource_kind,resource_id,last_generation FROM runtime_operation_resource_fences ORDER BY resource_kind,resource_id").unwrap();
    let fences = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    serde_json::json!({"hydrated":f.repository.hydrate_runtime_state().unwrap(),
        "operation":f.repository.get_operation(&f.operation.operation_id).unwrap(),
        "rebind":f.repository.get_workspace_agent_rebind(&f.operation.operation_id).unwrap(),
        "agent":f.repository.get_workspace_agent(&f.operation.workspace_id,&f.session.id).unwrap(),
        "events":f.repository.list_runtime_events(None,None,100).unwrap(),"fences":fences})
}
fn finish(f: &Fixture) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
    finish_at(f, 300)
}
fn finish_at(f: &Fixture, at: i64) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
    let mut session = f.session.clone();
    session.cwd = Some(f.operation.destination_cwd.clone());
    session.worktree_id = f.operation.destination_worktree_id.clone();
    let mut policy = f.agent.recreation_policy.clone();
    policy.authoritative_cwd = f.operation.destination_cwd.clone();
    let evidence = ProviderWorkspaceRebindEvidence {
        binding_generation: None,
        canonical_provider_session_ref: None,
        runtime_session_id: session.id.clone(),
        cwd: f.operation.destination_cwd.clone(),
        provider_session_ref: session.provider_session_ref.clone(),
    };
    f.repository.finalize_workspace_agent_rebind(
        &f.operation.operation_id,
        &session,
        &policy,
        &evidence,
        at,
    )
}
fn destination(f: &Fixture) -> ManagedWorktreeClaimRecord {
    f.repository
        .get_managed_worktree_claim(
            f.operation.destination_worktree_id.as_deref().unwrap(),
            &f.session.id,
        )
        .unwrap()
        .unwrap()
}
fn reject(f: &Fixture) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
    f.repository.classify_workspace_agent_rebind(
        &f.operation.operation_id,
        "rejected",
        "provider_unsupported",
        300,
    )
}

#[test]
fn same_key_rebind_promotes_or_restores_at_max_without_reviving_old_lifetime() {
    for rejected in [false, true] {
        let f = fixture(true, i64::MAX - 1, "active");
        let prior = f.prior.clone().unwrap();
        f.repository
            .begin_workspace_agent_rebind(&f.operation)
            .unwrap();
        let reservation = destination(&f);
        assert_eq!(reservation.claim_generation, Some(i64::MAX as u64));
        if rejected {
            reject(&f).unwrap();
        } else {
            finish(&f).unwrap();
        }
        let final_claim = destination(&f);
        assert_eq!(final_claim.claim_generation, reservation.claim_generation);
        assert_eq!(final_claim.released_at, None);
        assert_eq!(final_claim.claim_role, "owner");
        if rejected {
            assert_eq!(
                final_claim,
                ManagedWorktreeClaimRecord {
                    claim_generation: reservation.claim_generation,
                    ..prior.clone()
                }
            );
            let session = f
                .repository
                .hydrate_runtime_state()
                .unwrap()
                .sessions
                .into_iter()
                .find(|s| s.id == f.session.id)
                .unwrap();
            assert_eq!(session, f.session);
            assert_eq!(
                f.repository
                    .get_workspace_agent(&f.operation.workspace_id, &f.session.id)
                    .unwrap(),
                Some(f.agent.clone())
            );
        } else {
            assert_eq!(final_claim.created_at, 300);
        }
        let before = state(&f);
        assert!(matches!(
            f.repository.transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(500),
                    ..prior
                },
                Release
            ),
            Err(RuntimeError::Conflict(_))
        ));
        assert_eq!(state(&f), before);
    }
}

#[test]
fn exhausted_reservation_generation_rejects_admission_atomically() {
    let f = fixture(true, i64::MAX, "active");
    let before = state(&f);
    assert!(matches!(
        f.repository.begin_workspace_agent_rebind(&f.operation),
        Err(RuntimeError::InvalidState(_))
    ));
    assert_eq!(state(&f), before);
}

#[test]
fn different_keys_keep_each_lifetime_and_same_key_missing_history_is_not_revived() {
    for rejected in [false, true] {
        let f = fixture(false, 9, "active");
        f.repository
            .begin_workspace_agent_rebind(&f.operation)
            .unwrap();
        let reserved = destination(&f);
        assert_eq!(reserved.claim_generation, Some(1));
        if rejected {
            reject(&f).unwrap();
        } else {
            finish(&f).unwrap();
        }
        let old = f
            .repository
            .get_managed_worktree_claim("wt-source", &f.session.id)
            .unwrap()
            .unwrap();
        assert_eq!(old.claim_generation, Some(9));
        assert_eq!(old.released_at, if rejected { None } else { Some(300) });
        let target = destination(&f);
        assert_eq!(target.claim_generation, reserved.claim_generation);
        assert_eq!(target.released_at, if rejected { Some(300) } else { None });
    }
    for prior in ["absent", "terminal"] {
        let f = fixture(true, 7, prior);
        f.repository
            .begin_workspace_agent_rebind(&f.operation)
            .unwrap();
        let reserved = destination(&f);
        assert_eq!(
            reserved.claim_generation,
            Some(if prior == "absent" { 1 } else { 8 })
        );
        reject(&f).unwrap();
        assert_eq!(
            destination(&f),
            ManagedWorktreeClaimRecord {
                released_at: Some(300),
                ..reserved
            }
        );
    }
}

#[test]
fn missing_or_inconsistent_saved_lifetime_evidence_cannot_finalize_or_compensate() {
    let corruptions = [
        "json_remove(evidence_json,'$.claim_fences')",
        "json_remove(evidence_json,'$.claim_fences.previous.claim')",
        "json_remove(evidence_json,'$.claim_fences.previous.claim.claim_generation')",
        "json_set(evidence_json,'$.claim_fences.previous.worktree_id','other')",
        "json_set(evidence_json,'$.claim_fences.previous.claim.claim_generation',2)",
    ];
    for corruption in corruptions {
        for rejected in [false, true] {
            let f = fixture(true, 1, "active");
            f.repository
                .begin_workspace_agent_rebind(&f.operation)
                .unwrap();
            let db = open_connection(&f.repository.database_path).unwrap();
            db.execute(&format!("UPDATE runtime_operation_transitions SET evidence_json={corruption} WHERE operation_id='n06-rebind' AND sequence=1"),[]).unwrap();
            let before = state(&f);
            let result = if rejected { reject(&f) } else { finish(&f) };
            assert!(result.is_err(), "{corruption} {result:?}");
            assert_eq!(state(&f), before);
        }
    }
    let f = fixture(true, 1, "active");
    f.repository
        .begin_workspace_agent_rebind(&f.operation)
        .unwrap();
    let db = open_connection(&f.repository.database_path).unwrap();
    db.execute("UPDATE runtime_operation_transitions SET evidence_json=json_remove(evidence_json,'$.claim_fences') WHERE operation_id='n06-rebind' AND sequence=1",[]).unwrap();
    let claims = f
        .repository
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims;
    let resources = f
        .repository
        .get_operation(&f.operation.operation_id)
        .unwrap()
        .unwrap()
        .claims;
    f.repository
        .classify_workspace_agent_rebind(
            &f.operation.operation_id,
            "manual_review",
            "unknown_native_outcome",
            300,
        )
        .unwrap();
    assert_eq!(
        f.repository
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        claims
    );
    assert_eq!(
        f.repository
            .get_operation(&f.operation.operation_id)
            .unwrap()
            .unwrap()
            .claims,
        resources
    );
}

#[test]
fn admission_saved_prior_trigger_fault_rolls_back_all_authority() {
    let f = fixture(true, 1, "active");
    let db = open_connection(&f.repository.database_path).unwrap();
    db.execute_batch("CREATE TRIGGER n06_saved_prior AFTER INSERT ON runtime_operation_transitions
        WHEN NEW.operation_id='n06-rebind' AND NEW.sequence=1 BEGIN
        UPDATE runtime_operation_transitions SET evidence_json=json_set(evidence_json,
            '$.claim_fences.previous.claim.claim_role','consumer','$.claim_fences.previous.claim.created_at',777)
        WHERE operation_id=NEW.operation_id AND sequence=1;END;").unwrap();
    let before = state(&f);
    assert!(f
        .repository
        .begin_workspace_agent_rebind(&f.operation)
        .is_err());
    assert_eq!(state(&f), before);
}

#[test]
fn late_rebind_event_claim_fault_rolls_back_completion_rejection_and_hold() {
    for action in ["complete", "rejected", "manual_review"] {
        let f = fixture(false, 5, "active");
        f.repository
            .begin_workspace_agent_rebind(&f.operation)
            .unwrap();
        let db = open_connection(&f.repository.database_path).unwrap();
        db.execute_batch("CREATE TRIGGER n06_late_claim AFTER INSERT ON runtime_events
            WHEN NEW.event_id='n06-rebind:event:transition:2' BEGIN
            UPDATE managed_worktree_claims SET claim_generation=claim_generation+1 WHERE worktree_id='wt-destination' AND session_id='n06-agent';END;").unwrap();
        let before = state(&f);
        let result = if action == "complete" {
            finish(&f)
        } else {
            f.repository.classify_workspace_agent_rebind(
                &f.operation.operation_id,
                action,
                "fixture",
                300,
            )
        };
        assert!(result.is_err(), "{action} {result:?}");
        assert_eq!(state(&f), before);
    }
}

#[test]
fn terminal_previous_claim_is_verified_without_rewrite() {
    let f = fixture(false, 7, "terminal");
    f.repository
        .begin_workspace_agent_rebind(&f.operation)
        .unwrap();
    let db = open_connection(&f.repository.database_path).unwrap();
    db.execute_batch("CREATE TRIGGER n06_no_terminal_write BEFORE UPDATE ON managed_worktree_claims WHEN OLD.worktree_id='wt-source'
        BEGIN SELECT RAISE(ABORT,'terminal history must not be rewritten');END;").unwrap();
    finish(&f).unwrap();
    assert_eq!(
        f.repository
            .get_managed_worktree_claim("wt-source", &f.session.id)
            .unwrap(),
        f.prior
    );
    assert_eq!(destination(&f).claim_role, "owner");
    assert!(f.temp.path().exists());
}

#[test]
fn rebind_away_and_back_allocates_a_new_lifetime_and_old_key_replay_cannot_rewind_it() {
    let mut f = fixture(false, 9, "active");
    let first = f.operation.clone();
    let old = f.prior.clone().unwrap();
    f.repository.begin_workspace_agent_rebind(&first).unwrap();
    finish(&f).unwrap();
    f.session = f
        .repository
        .get_persisted_session(&f.session.id)
        .unwrap()
        .unwrap();
    f.agent = f
        .repository
        .get_workspace_agent(&first.workspace_id, &f.session.id)
        .unwrap()
        .unwrap();
    f.operation = WorkspaceAgentRebindOperation {
        operation_id: "n06-rebind-back".into(),
        idempotency_key: Some("n06-rebind-back".into()),
        expected_revision: f.agent.revision,
        previous_worktree_id: first.destination_worktree_id.clone(),
        destination_worktree_id: first.previous_worktree_id.clone(),
        previous_cwd: first.destination_cwd.clone(),
        destination_cwd: first.previous_cwd.clone(),
        created_at: 400,
        updated_at: 400,
        ..first.clone()
    };
    f.repository
        .begin_workspace_agent_rebind(&f.operation)
        .unwrap();
    assert_eq!(destination(&f).claim_generation, Some(10));
    finish_at(&f, 500).unwrap();
    let returned = destination(&f);
    assert_eq!(
        (
            &*returned.claim_role,
            returned.claim_generation,
            returned.released_at
        ),
        ("owner", Some(10), None)
    );
    let departed = f
        .repository
        .get_managed_worktree_claim("wt-destination", &f.session.id)
        .unwrap()
        .unwrap();
    assert_eq!(
        (departed.claim_generation, departed.released_at),
        (Some(1), Some(500))
    );
    let db = open_connection(&f.repository.database_path).unwrap();
    let before = super::legacy_native_turn_guards::raw_state(&db);
    let stale = ManagedWorktreeClaimRecord {
        released_at: Some(600),
        ..old.clone()
    };
    for result in [
        f.repository
            .transition_managed_worktree_claim(&stale, Release),
        f.repository
            .release_managed_worktree_claim_if_unbound(&stale, &old, &first.previous_cwd),
    ] {
        let error = result.unwrap_err();
        assert!(error.to_string().contains("lifetime changed"), "{error}");
    }
    let replay = f.repository.begin_workspace_agent_rebind(&first).unwrap();
    assert_eq!(replay.phase, "completed");
    assert_eq!(super::legacy_native_turn_guards::raw_state(&db), before);
}
