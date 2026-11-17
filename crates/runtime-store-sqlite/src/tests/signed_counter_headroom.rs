use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::workspace_retirement::{retirement_command, setup as retirement_setup};
use super::*;
use runtime_core::{
    prepare_workspace_interrupt, OperationActor, OperatorNativePolicyCommand,
    OperatorNativePolicyKind, OperatorNativePolicyReviewCommand, OperatorNativePolicyReviewInput,
    OperatorNativePolicyReviewResolution, ProviderKind, RuntimeError, WorkspaceAgentLifecycleState,
    WorkspaceAgentRecord, WorkspaceInterruptAdmission,
};
use rusqlite::params;

mod interrupt_recovery;

fn agent_fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    SessionRecord,
    WorkspaceAgentRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("synthetic-workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "signed_policy", "signed-fox");
    repository.create_workspace_agent(&session, &agent).unwrap();
    (temp, repository, session, agent)
}

#[test]
fn signed_policy_cas_max_refuses_without_committing_session_or_real_revision() {
    let (temp, repository, session, agent) = agent_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?2 WHERE session_id=?1",
        params![session.id, i64::MAX],
    )
    .unwrap();
    let mut changed = session.clone();
    changed.updated_at = 250;
    let error = repository
        .compare_and_set_workspace_agent_recreation_policy(
            &changed,
            &session.id,
            i64::MAX as u64,
            &agent.recreation_policy,
            changed.updated_at,
        )
        .unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)), "{error}");
    let (revision, kind, updated_at, policy): (i64, String, i64, String) = db
        .query_row(
            "SELECT a.revision,typeof(a.revision),s.updated_at,a.recreation_policy_json
             FROM workspace_agents a JOIN sessions s ON s.id=a.session_id
             WHERE a.session_id=?1",
            [&session.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        (revision, kind, updated_at),
        (i64::MAX, "integer".into(), session.updated_at)
    );
    assert_eq!(
        serde_json::from_str::<runtime_core::WorkspaceAgentRecreationPolicy>(&policy).unwrap(),
        agent.recreation_policy
    );
}

#[test]
fn signed_policy_cas_last_successor_is_integer_and_cannot_advance_twice() {
    let (temp, repository, session, agent) = agent_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?2 WHERE session_id=?1",
        params![session.id, i64::MAX - 1],
    )
    .unwrap();
    let mut changed = session.clone();
    changed.updated_at = 250;
    let last = repository
        .compare_and_set_workspace_agent_recreation_policy(
            &changed,
            &session.id,
            (i64::MAX - 1) as u64,
            &agent.recreation_policy,
            changed.updated_at,
        )
        .unwrap();
    assert_eq!(last.revision, i64::MAX as u64);
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspace_agents WHERE session_id=?1",
            [&session.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    let error = repository
        .compare_and_set_workspace_agent_recreation_policy(
            &changed,
            &session.id,
            i64::MAX as u64,
            &agent.recreation_policy,
            changed.updated_at + 1,
        )
        .unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)), "{error}");
    assert_eq!(
        repository
            .get_workspace_agent_by_id(&session.id)
            .unwrap()
            .unwrap()
            .revision,
        i64::MAX as u64
    );
}

fn worktree_record() -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: "signed_worktree".into(),
        repo_root: "/tmp/synthetic-counter-repo".into(),
        worktree_root: "/tmp/synthetic-counter-root".into(),
        worktree_cwd: "/tmp/synthetic-counter-root/checkout".into(),
        branch_name: "gg/synthetic".into(),
        worktree_name: "signed".into(),
        unified_workspace_path: "synthetic".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: Some("synthetic_intent".into()),
        created_at: 100,
        updated_at: 100,
    }
}

#[test]
fn signed_worktree_identity_update_refuses_max_without_record_or_event() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let original = worktree_record();
    repository.upsert_managed_worktree(&original).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE managed_worktrees SET revision=?2 WHERE id=?1",
        params![original.id, i64::MAX],
    )
    .unwrap();
    let changed = ManagedWorktreeRecord {
        worktree_name: "altered".into(),
        ..original.clone()
    };
    let error = repository
        .record_managed_worktree_event(&changed, "synthetic", false, None, None)
        .unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)), "{error}");
    assert_eq!(
        repository.get_managed_worktree(&original.id).unwrap(),
        Some(original.clone())
    );
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM managed_worktrees WHERE id=?1",
            [&original.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));

    db.execute(
        "UPDATE managed_worktrees SET revision=?2 WHERE id=?1",
        params![original.id, i64::MAX - 1],
    )
    .unwrap();
    let accepted = repository
        .record_managed_worktree_event(&changed, "synthetic", false, None, None)
        .unwrap();
    assert_eq!(accepted, changed);
    assert_eq!(
        repository.managed_worktree_revision(&original.id).unwrap(),
        i64::MAX as u64
    );
    assert_eq!(
        repository
            .list_runtime_events(None, None, 10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn signed_worktree_id_conflict_refuses_max_without_rewriting_natural_key() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let original = worktree_record();
    repository.upsert_managed_worktree(&original).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE managed_worktrees SET revision=?2 WHERE id=?1",
        params![original.id, i64::MAX],
    )
    .unwrap();
    let changed = ManagedWorktreeRecord {
        branch_name: "gg/another-branch".into(),
        ..original.clone()
    };
    let error = repository.upsert_managed_worktree(&changed).unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)), "{error}");
    assert_eq!(
        repository.get_managed_worktree(&original.id).unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        repository.managed_worktree_revision(&original.id).unwrap(),
        i64::MAX as u64
    );
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());

    db.execute(
        "UPDATE managed_worktrees SET revision=?2 WHERE id=?1",
        params![original.id, i64::MAX - 1],
    )
    .unwrap();
    repository.upsert_managed_worktree(&changed).unwrap();
    assert_eq!(
        repository.get_managed_worktree(&original.id).unwrap(),
        Some(changed)
    );
    assert_eq!(
        repository.managed_worktree_revision(&original.id).unwrap(),
        i64::MAX as u64
    );
}

#[test]
fn retired_model_migration_is_atomic_when_one_agent_revision_is_exhausted() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 8);
    let db = open_connection(&path).unwrap();
    // A disposable version-8 fixture, before migration 9's model normalization.
    db.execute("INSERT INTO workspaces(workspace_id,canonical_root,display_name,lifecycle_state,revision,created_at,updated_at) VALUES('ws_signed','/tmp/synthetic-model','Synthetic','active',0,1,1)", []).unwrap();
    db.execute("INSERT INTO sessions(id,provider,status,cwd,model,created_at,updated_at) VALUES('agent_signed','codex','ready','/tmp/synthetic-model','gpt-5.4',1,1)", []).unwrap();
    db.execute("INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at) VALUES('agent_signed','ws_signed','synthetic',1)", []).unwrap();
    let (_, _, _, mut template) = agent_fixture();
    template.recreation_policy.model = Some("gpt-5.4".into());
    let original_policy = serde_json::to_string(&template.recreation_policy).unwrap();
    db.execute("INSERT INTO workspace_agents(session_id,workspace_id,identity_alias,lifecycle_state,recreation_policy_json,revision,created_at,updated_at) VALUES('agent_signed','ws_signed','synthetic-fox','active',?1,?2,1,1)", params![original_policy, i64::MAX]).unwrap();
    let repository = SqliteRuntimeRepository::new(path.clone());
    let error = repository.initialize_schema().unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::InvalidState(_) | RuntimeError::Bootstrap(_)
        ),
        "{error}"
    );
    let (before_model, before_policy, revision, kind): (String, String, i64, String) = db
        .query_row("SELECT s.model,a.recreation_policy_json,a.revision,typeof(a.revision) FROM sessions s JOIN workspace_agents a ON a.session_id=s.id WHERE s.id='agent_signed'", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))
        .unwrap();
    assert_eq!(before_model, "gpt-5.4");
    assert_eq!(before_policy, original_policy);
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM schema_migrations WHERE version=9",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );

    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id='agent_signed'",
        [i64::MAX - 1],
    )
    .unwrap();
    drop(db);
    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    let (after_model, after_policy, revision, kind): (String, String, i64, String) = db
        .query_row("SELECT s.model,a.recreation_policy_json,a.revision,typeof(a.revision) FROM sessions s JOIN workspace_agents a ON a.session_id=s.id WHERE s.id='agent_signed'", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))
        .unwrap();
    let expected = runtime_core::migrate_retired_model(ProviderKind::Codex, "gpt-5.4").unwrap();
    assert_eq!(after_model, expected);
    assert_eq!(
        serde_json::from_str::<runtime_core::WorkspaceAgentRecreationPolicy>(&after_policy)
            .unwrap()
            .model,
        Some(expected)
    );
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM schema_migrations WHERE version=9",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}

#[test]
fn archiving_a_workspace_lead_must_not_commit_real_workspace_revision() {
    let (temp, repository, session, agent) = agent_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspaces SET lead_agent_id=?2, revision=?3 WHERE workspace_id=?1",
        params![agent.workspace_id, session.id, i64::MAX],
    )
    .unwrap();
    let error = repository
        .set_workspace_agent_lifecycle(
            &session,
            &session.id,
            WorkspaceAgentLifecycleState::Archived,
            Some("synthetic archive"),
            500,
        )
        .unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)), "{error}");
    let (revision, kind, lead): (i64, String, Option<String>) = db
        .query_row(
            "SELECT revision, typeof(revision), lead_agent_id FROM workspaces WHERE workspace_id=?1",
            [&agent.workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    assert_eq!(lead.as_deref(), Some(session.id.as_str()));
    assert_eq!(
        repository
            .get_workspace_agent_by_id(&session.id)
            .unwrap()
            .unwrap()
            .lifecycle_state,
        WorkspaceAgentLifecycleState::Active
    );
}

#[test]
fn interrupt_effect_start_must_not_persist_real_attempt_before_native_work() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO runtime_operations(operation_id,kind,actor_kind,actor_id,
          normalized_request_hash,normalized_request_json,phase,created_at,updated_at)
         VALUES('op_signed_interrupt','workspace_interrupt','operator','synthetic',
          'synthetic_hash','{}','requested',1,1)",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT INTO runtime_operation_effects(effect_id,operation_id,effect_kind,
          target_kind,target_id,phase,idempotency_key,evidence_json,attempt_count,created_at,updated_at)
         VALUES('eff_signed_interrupt','op_signed_interrupt','workspace_turn_interrupt',
          'workspace_agent','agent_signed_interrupt','intended','interrupt:agent_signed_interrupt',
          '{\"turn_id\":\"turn_signed\"}',?1,1,1)",
        [i64::MAX],
    ).unwrap();
    let error = repository
        .mark_workspace_interrupt_started(
            "op_signed_interrupt",
            "agent_signed_interrupt",
            "turn_signed",
            500,
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::Conflict(_) | RuntimeError::InvalidState(_)
        ),
        "{error}"
    );
    let (attempt, kind, phase): (i64, String, String) = db
        .query_row("SELECT attempt_count,typeof(attempt_count),phase FROM runtime_operation_effects WHERE effect_id='eff_signed_interrupt'", [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)))
        .unwrap();
    assert_eq!(
        (attempt, kind, phase),
        (i64::MAX, "integer".into(), "intended".into())
    );

    db.execute("UPDATE runtime_operation_effects SET attempt_count=?1 WHERE effect_id='eff_signed_interrupt'", [i64::MAX - 1]).unwrap();
    repository
        .mark_workspace_interrupt_started(
            "op_signed_interrupt",
            "agent_signed_interrupt",
            "turn_signed",
            501,
        )
        .unwrap();
    let (attempt, kind, phase): (i64, String, String) = db
        .query_row("SELECT attempt_count,typeof(attempt_count),phase FROM runtime_operation_effects WHERE effect_id='eff_signed_interrupt'", [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)))
        .unwrap();
    assert_eq!(
        (attempt, kind, phase),
        (i64::MAX, "integer".into(), "started".into())
    );
}

#[test]
fn passive_retirement_worktree_preservation_requires_signed_revision_headroom() {
    let (temp, repository, workspace) = retirement_setup();
    let original = ManagedWorktreeRecord {
        repo_root: workspace.canonical_root.clone(),
        deletion_policy: "delete_on_last_claim".into(),
        ..worktree_record()
    };
    repository.upsert_managed_worktree(&original).unwrap();
    let command = retirement_command(&workspace, "signed-worktree-preservation");
    let plan = match repository.admit_workspace_retirement(&command).unwrap() {
        runtime_core::WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected retirement admission {other:?}"),
    };
    assert_eq!(plan.preserved_worktree_ids, vec![original.id.clone()]);
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE managed_worktrees SET revision=?2 WHERE id=?1",
        params![original.id, i64::MAX],
    )
    .unwrap();
    let error = repository
        .settle_passive_workspace_retirement(&plan.operation_id, 500)
        .unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::InvalidState(_) | RuntimeError::Conflict(_)
        ),
        "{error}"
    );
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM managed_worktrees WHERE id=?1",
            [&original.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    assert_eq!(
        repository.get_managed_worktree(&original.id).unwrap(),
        Some(original.clone())
    );
    let effect = repository
        .get_operation(&plan.operation_id)
        .unwrap()
        .unwrap()
        .effects
        .into_iter()
        .find(|effect| effect.effect_kind == "workspace_worktree_preserve")
        .unwrap();
    assert_eq!(effect.phase, "intended");

    db.execute(
        "UPDATE managed_worktrees SET revision=?2 WHERE id=?1",
        params![original.id, i64::MAX - 1],
    )
    .unwrap();
    repository
        .settle_passive_workspace_retirement(&plan.operation_id, 501)
        .unwrap();
    assert_eq!(
        repository.managed_worktree_revision(&original.id).unwrap(),
        i64::MAX as u64
    );
    let retained = repository
        .get_managed_worktree(&original.id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.deletion_policy, "retain_on_last_claim");
    let effect = repository
        .get_operation(&plan.operation_id)
        .unwrap()
        .unwrap()
        .effects
        .into_iter()
        .find(|effect| effect.effect_kind == "workspace_worktree_preserve")
        .unwrap();
    assert_eq!(effect.phase, "finalized");
}

fn legacy_native_policy_fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    SessionRecord,
    WorkspaceAgentRecord,
    OperatorNativePolicyCommand,
) {
    let (temp, repository, session, agent) = agent_fixture();
    let normalized_request = serde_json::json!({
        "agent_id":agent.agent_id,
        "kind":"preferences",
        "expected_revision":0,
        "current_preferences":agent.recreation_policy.current_preferences,
    });
    let command = OperatorNativePolicyCommand {
        operation_id: "signed-legacy-native-policy".into(),
        actor: OperationActor::operator("signed-counter-tests"),
        idempotency_key: "signed-legacy-native-policy".into(),
        workspace_id: agent.workspace_id.clone(),
        agent_id: agent.agent_id.clone(),
        kind: OperatorNativePolicyKind::Preferences,
        expected_revision: 0,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&normalized_request).unwrap(),
        normalized_request,
        requested_at: 101,
    };
    // Only durable intent; no provider/native invocation in the fixture.
    repository.admit_operator_native_policy(&command).unwrap();
    (temp, repository, session, agent, command)
}

fn synthesize_legacy_policy_intent_at_revision(
    db: &rusqlite::Connection,
    command: &mut OperatorNativePolicyCommand,
    expected_revision: i64,
) {
    // Simulate an old pre-guard admitted intent at signed MAX. No actual
    // database or provider is accessed; operation/effect proof stays coherent.
    command.expected_revision = expected_revision as u64;
    command.normalized_request["expected_revision"] = serde_json::json!(command.expected_revision);
    command.normalized_request_hash =
        runtime_core::normalized_json_hash(&command.normalized_request).unwrap();
    db.execute(
        "UPDATE runtime_operations SET normalized_request_hash=?2,normalized_request_json=?3
         WHERE operation_id=?1",
        params![
            command.operation_id,
            command.normalized_request_hash,
            serde_json::to_string(&command.normalized_request).unwrap()
        ],
    )
    .unwrap();
    db.execute(
        "UPDATE runtime_operation_effects
         SET evidence_json=json_set(evidence_json,'$.expected_revision',?2)
         WHERE operation_id=?1 AND target_id=?3",
        params![command.operation_id, expected_revision, command.agent_id],
    )
    .unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?2 WHERE session_id=?1",
        params![command.agent_id, expected_revision],
    )
    .unwrap();
}

#[test]
fn legacy_native_policy_acknowledged_receipt_refuses_signed_max_atomically() {
    let (temp, repository, session, agent, mut command) = legacy_native_policy_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    synthesize_legacy_policy_intent_at_revision(&db, &mut command, i64::MAX);
    let exact = serde_json::json!({
        "revision":i64::MAX as u64+1,
        "current_preferences":agent.recreation_policy.current_preferences,
    });
    let error = repository
        .complete_operator_native_policy(&command, &session, &agent.recreation_policy, &exact, 201)
        .unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::InvalidState(_) | RuntimeError::Conflict(_)
        ),
        "{error}"
    );
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Requested
    );
    assert_eq!(operation.effects[0].phase, "intended");
    assert_eq!(operation.claims.len(), 1);

    synthesize_legacy_policy_intent_at_revision(&db, &mut command, i64::MAX - 1);
    let exact = serde_json::json!({
        "revision":i64::MAX as u64,
        "current_preferences":agent.recreation_policy.current_preferences,
    });
    repository
        .complete_operator_native_policy(&command, &session, &agent.recreation_policy, &exact, 202)
        .unwrap();
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(operation.effects[0].phase, "finalized");
    assert!(operation.claims.is_empty());
}

#[test]
fn legacy_native_policy_operator_review_applied_refuses_signed_max_atomically() {
    let (temp, repository, session, agent, mut command) = legacy_native_policy_fixture();
    repository
        .mark_operator_native_policy_uncertain(
            &command.operation_id,
            "synthetic unknown native outcome",
            102,
        )
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    synthesize_legacy_policy_intent_at_revision(&db, &mut command, i64::MAX);
    let mut review = OperatorNativePolicyReviewCommand {
        operation_id: command.operation_id.clone(),
        agent_id: agent.agent_id.clone(),
        actor: command.actor.clone(),
        input: OperatorNativePolicyReviewInput {
            resolution: OperatorNativePolicyReviewResolution::Applied,
            evidence: "Synthetic independent proof of native application".into(),
            provider: session.provider.clone(),
            provider_session_ref: session.provider_session_ref.clone().unwrap(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            expected_revision: i64::MAX as u64,
            session_updated_at: session.updated_at,
        },
        reviewed_at: 203,
    };
    let error = repository
        .resolve_operator_native_policy_review(&review)
        .unwrap_err();
    assert!(
        matches!(
            error,
            RuntimeError::InvalidState(_) | RuntimeError::Conflict(_)
        ),
        "{error}"
    );
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(operation.effects[0].phase, "uncertain");
    assert_eq!(operation.claims.len(), 1);

    synthesize_legacy_policy_intent_at_revision(&db, &mut command, i64::MAX - 1);
    review.input.expected_revision = i64::MAX as u64 - 1;
    review.reviewed_at = 204;
    let resolution = repository
        .resolve_operator_native_policy_review(&review)
        .unwrap();
    assert_eq!(resolution.agent_revision, i64::MAX as u64);
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(operation.effects[0].phase, "finalized");
    assert!(operation.claims.is_empty());
}

#[test]
fn legacy_native_policy_not_applied_review_is_noop_even_at_signed_max() {
    let (temp, repository, session, agent, mut command) = legacy_native_policy_fixture();
    repository
        .mark_operator_native_policy_uncertain(
            &command.operation_id,
            "synthetic unknown native outcome",
            102,
        )
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    synthesize_legacy_policy_intent_at_revision(&db, &mut command, i64::MAX);
    let review = OperatorNativePolicyReviewCommand {
        operation_id: command.operation_id.clone(),
        agent_id: agent.agent_id.clone(),
        actor: command.actor.clone(),
        input: OperatorNativePolicyReviewInput {
            resolution: OperatorNativePolicyReviewResolution::NotApplied,
            evidence: "Synthetic independent proof that provider never applied changes".into(),
            provider: session.provider.clone(),
            provider_session_ref: session.provider_session_ref.clone().unwrap(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            expected_revision: i64::MAX as u64,
            session_updated_at: session.updated_at,
        },
        reviewed_at: 205,
    };
    let settled = repository
        .resolve_operator_native_policy_review(&review)
        .unwrap();
    assert_eq!(settled.agent_revision, i64::MAX as u64);
    assert_eq!(
        repository
            .resolve_operator_native_policy_review(&review)
            .unwrap(),
        settled
    );
    let (revision, kind, status): (i64, String, String) = db
        .query_row(
            "SELECT a.revision,typeof(a.revision),s.status FROM workspace_agents a
         JOIN sessions s ON s.id=a.session_id WHERE a.session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (revision, kind, status),
        (
            i64::MAX,
            "integer".into(),
            "policy_recovery_required".into()
        )
    );
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Failed
    );
    assert_eq!(operation.effects[0].phase, "finalized");
    assert!(operation.claims.is_empty());
}

#[test]
fn malformed_real_revisions_fail_closed_without_coercing_session_or_worktree() {
    let (temp, repository, session, agent) = agent_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=0.5 WHERE session_id=?1",
        [&session.id],
    )
    .unwrap();
    let mut changed_session = session.clone();
    changed_session.updated_at = 345;
    assert!(repository
        .compare_and_set_workspace_agent_recreation_policy(
            &changed_session,
            &session.id,
            0,
            &agent.recreation_policy,
            345,
        )
        .is_err());
    let (kind, updated_at): (String, i64) = db
        .query_row(
            "SELECT typeof(a.revision),s.updated_at FROM workspace_agents a
         JOIN sessions s ON s.id=a.session_id WHERE a.session_id=?1",
            [&session.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((kind, updated_at), ("real".into(), session.updated_at));

    let original = worktree_record();
    repository.upsert_managed_worktree(&original).unwrap();
    db.execute(
        "UPDATE managed_worktrees SET revision=0.5 WHERE id=?1",
        [&original.id],
    )
    .unwrap();
    let changed_worktree = ManagedWorktreeRecord {
        worktree_name: "forged-update".into(),
        ..original.clone()
    };
    let earlier_event_count = repository
        .list_runtime_events(None, None, 100)
        .unwrap()
        .len();
    assert!(repository
        .record_managed_worktree_event(&changed_worktree, "synthetic", false, None, None,)
        .is_err());
    let kind: String = db
        .query_row(
            "SELECT typeof(revision) FROM managed_worktrees WHERE id=?1",
            [&original.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(kind, "real");
    assert_eq!(
        repository.get_managed_worktree(&original.id).unwrap(),
        Some(original)
    );
    assert_eq!(
        repository
            .list_runtime_events(None, None, 100)
            .unwrap()
            .len(),
        earlier_event_count,
    );
}

#[test]
fn signed_scope_event_sequence_max_rejects_before_writing_event() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,
         payload_json,created_at) VALUES('signed-max-parent','process','signed-counter',?1,
         'synthetic.parent',1,'{}',1)",
        [i64::MAX],
    )
    .unwrap();
    let event = runtime_core::NewRuntimeEvent {
        event_id: "signed-counter-next".into(),
        scope: runtime_core::RuntimeEventScope::Process,
        scope_id: "signed-counter".into(),
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: "synthetic.next".into(),
        criticality: runtime_core::RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"source":"fixture"}),
        provider: None,
        provider_seq: None,
        created_at: 2,
    };
    assert!(repository.append_runtime_event(&event).is_err());
    assert_eq!(
        repository
            .list_runtime_events(
                Some((runtime_core::RuntimeEventScope::Process, "signed-counter")),
                None,
                10,
            )
            .unwrap()
            .len(),
        1
    );
    let (seq, kind): (i64, String) = db
        .query_row(
            "SELECT seq,typeof(seq) FROM runtime_events WHERE event_id='signed-max-parent'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((seq, kind), (i64::MAX, "integer".into()));

    db.execute(
        "UPDATE runtime_events SET seq=?1 WHERE event_id='signed-max-parent'",
        [i64::MAX - 1],
    )
    .unwrap();
    let added = repository.append_runtime_event(&event).unwrap();
    assert_eq!(added.seq, i64::MAX);
    assert_eq!(repository.append_runtime_event(&event).unwrap(), added);
    assert_eq!(
        repository
            .list_runtime_events(
                Some((runtime_core::RuntimeEventScope::Process, "signed-counter")),
                None,
                10,
            )
            .unwrap()
            .len(),
        2
    );
}
