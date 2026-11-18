use super::*;
use runtime_core::{
    PersistedUserInputSnapshot, PersistedUserInputSnapshotImageRef,
    PersistedUserInputSnapshotInvocation, RuntimeError, TurnAdmissionRecord, TurnCorrelationState,
    TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
};

#[test]
fn turn_admission_is_atomic_and_retryable_after_storage_failure() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let mut base_session = admission_session();
    repository
        .upsert_session(&base_session)
        .expect("seed session");

    let turn = admission_turn();
    let mut admitted_session = base_session.clone();
    admitted_session.status = "turn_admitted".to_string();
    admitted_session.active_turn_id = Some(turn.id.clone());
    admitted_session.updated_at = 11;
    let approval = admission_approval();
    let admission = admission_record();

    let connection = open_connection(&repository.database_path).expect("connection");
    connection
        .execute_batch(
            "CREATE TRIGGER fail_turn_admission
             BEFORE INSERT ON turn_admissions
             BEGIN
               SELECT RAISE(ABORT, 'forced turn admission failure');
             END;",
        )
        .expect("failure trigger");
    drop(connection);

    assert!(repository
        .admit_turn(&admission, &turn, &admitted_session, Some(&approval))
        .is_err());
    let connection = open_connection(&repository.database_path).expect("after failure");
    for table in ["turns", "approvals", "turn_admissions"] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 0, "{table} must roll back atomically");
    }
    let active_turn_id: Option<String> = connection
        .query_row(
            "SELECT active_turn_id FROM sessions WHERE id = ?1",
            params![base_session.id],
            |row| row.get(0),
        )
        .expect("session active turn");
    assert_eq!(active_turn_id, None);
    connection
        .execute_batch("DROP TRIGGER fail_turn_admission")
        .expect("drop trigger");
    drop(connection);

    repository
        .admit_turn(&admission, &turn, &admitted_session, Some(&approval))
        .expect("retry admission");
    let rows = repository.list_turn_admissions().expect("admissions");
    assert_eq!(rows, vec![admission]);
    base_session = repository
        .hydrate_runtime_state()
        .expect("hydrate")
        .sessions
        .into_iter()
        .find(|session| session.id == admitted_session.id)
        .expect("session");
    assert_eq!(base_session.active_turn_id, Some(turn.id));
}

#[test]
fn turn_admission_round_trips_snapshot_provenance_and_native_mapping() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let base_session = admission_session();
    repository
        .upsert_session(&base_session)
        .expect("seed session");
    let turn = admission_turn();
    let mut admitted_session = base_session;
    admitted_session.status = "turn_admitted".to_string();
    admitted_session.active_turn_id = Some(turn.id.clone());
    let mut admission = admission_record();
    admission.projection_source = TurnInputProjectionSource::AutomationContext;
    admission.dispatch_policy.permission_mode = Some("full_auto".to_string());
    admission.correlation.expected_turn_id = Some("turn_parent".to_string());
    admission.correlation.correlation_id = Some("job_123".to_string());
    repository
        .admit_turn(&admission, &turn, &admitted_session, None)
        .expect("admit");

    admission.dispatch_state = TurnDispatchState::Dispatched;
    admission.provider_native_turn_id = Some("native_turn_7".to_string());
    admission.updated_at = 20;
    repository
        .upsert_turn_admission(&admission)
        .expect("update admission");

    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        reopened
            .list_turn_admissions()
            .expect("reloaded admissions"),
        vec![admission]
    );
}

#[test]
fn retirement_review_snapshot_preserves_all_target_authority_and_ignores_corrupt_foreign_rows() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();

    let base = admission_session();
    repository.upsert_session(&base).unwrap();
    let turn = admission_turn();
    let mut target = base.clone();
    target.status = "turn_admitted".into();
    target.active_turn_id = Some(turn.id.clone());
    target.updated_at = 11;
    let approval = admission_approval();
    let admission = admission_record();
    repository
        .admit_turn(&admission, &turn, &target, Some(&approval))
        .unwrap();

    let mut other = admission_session();
    other.id = "unrelated_snapshot_session".into();
    other.provider_session_ref = Some("unrelated_native_session".into());
    repository.upsert_session(&other).unwrap();
    let mut other_turn = admission_turn();
    other_turn.id = "unrelated_snapshot_turn".into();
    other_turn.session_id = other.id.clone();
    other.active_turn_id = Some(other_turn.id.clone());
    other.status = "turn_admitted".into();
    other.updated_at = 12;
    let mut other_approval = admission_approval();
    other_approval.id = "unrelated_snapshot_approval".into();
    other_approval.session_id = other.id.clone();
    other_approval.turn_id = other_turn.id.clone();
    other_approval.provider_approval_ref = Some("unrelated_native_approval".into());
    let mut other_admission = admission_record();
    other_admission.turn_id = other_turn.id.clone();
    other_admission.session_id = other.id.clone();
    repository
        .admit_turn(&other_admission, &other_turn, &other, Some(&other_approval))
        .unwrap();

    let snapshot = repository
        .get_persisted_retirement_review_session(&target.id)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.session, target);
    assert_eq!(snapshot.turns, vec![turn]);
    assert_eq!(snapshot.approvals, vec![approval]);
    assert_eq!(snapshot.admissions, vec![admission]);
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(
        snapshot.turns,
        hydrated
            .turns
            .into_iter()
            .filter(|row| row.session_id == target.id)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        snapshot.approvals,
        hydrated
            .approvals
            .into_iter()
            .filter(|row| row.session_id == target.id)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        snapshot.admissions,
        repository
            .list_turn_admissions()
            .unwrap()
            .into_iter()
            .filter(|row| row.session_id == target.id)
            .collect::<Vec<_>>()
    );

    let db = open_connection(&repository.database_path).unwrap();
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid' WHERE id=?1",
        [&other.id],
    )
    .unwrap();
    db.execute(
        "UPDATE approvals SET request_json='{invalid' WHERE session_id=?1",
        [&other.id],
    )
    .unwrap();
    // Valid JSON with an invalid shape evades the column's json_valid check;
    // global admission decoding must still fail while the target remains sound.
    db.execute(
        "UPDATE turn_admissions SET user_input_snapshot_json='null' WHERE session_id=?1",
        [&other.id],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    assert!(repository.list_turn_admissions().is_err());
    assert_eq!(
        repository
            .get_persisted_retirement_review_session(&target.id)
            .unwrap(),
        Some(snapshot.clone())
    );
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        reopened
            .get_persisted_retirement_review_session(&target.id)
            .unwrap(),
        Some(snapshot)
    );
}

#[test]
fn retirement_settled_turn_snapshot_matches_authority_and_ignores_corrupt_foreign_rows() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();

    let session = admission_session();
    repository.upsert_session(&session).unwrap();
    let turn = admission_turn();
    let mut admitted_session = session.clone();
    admitted_session.status = "turn_admitted".into();
    admitted_session.active_turn_id = Some(turn.id.clone());
    admitted_session.updated_at = 11;
    let approval = admission_approval();
    let admission = admission_record();
    repository
        .admit_turn(&admission, &turn, &admitted_session, Some(&approval))
        .unwrap();

    let mut foreign = admission_session();
    foreign.id = "settled_foreign_session".into();
    foreign.provider_session_ref = Some("settled_foreign_native".into());
    repository.upsert_session(&foreign).unwrap();
    let mut foreign_turn = admission_turn();
    foreign_turn.id = "settled_foreign_turn".into();
    foreign_turn.session_id = foreign.id.clone();
    foreign.status = "turn_admitted".into();
    foreign.active_turn_id = Some(foreign_turn.id.clone());
    foreign.updated_at = 12;
    let mut foreign_approval = admission_approval();
    foreign_approval.id = "settled_foreign_approval".into();
    foreign_approval.session_id = foreign.id.clone();
    foreign_approval.turn_id = foreign_turn.id.clone();
    foreign_approval.provider_approval_ref = Some("settled_foreign_approval".into());
    let mut foreign_admission = admission_record();
    foreign_admission.turn_id = foreign_turn.id.clone();
    foreign_admission.session_id = foreign.id.clone();
    repository
        .admit_turn(
            &foreign_admission,
            &foreign_turn,
            &foreign,
            Some(&foreign_approval),
        )
        .unwrap();

    let snapshot = repository
        .get_persisted_retirement_settled_turn(&turn.id)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.turn, turn);
    assert_eq!(snapshot.session, Some(admitted_session));
    assert_eq!(snapshot.approvals, vec![approval]);
    assert_eq!(snapshot.admission, Some(admission));
    assert_eq!(
        repository
            .get_persisted_retirement_settled_turn("nonexistent-turn")
            .unwrap(),
        None
    );

    let db = open_connection(&repository.database_path).unwrap();
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid' WHERE id=?1",
        [&foreign.id],
    )
    .unwrap();
    db.execute(
        "UPDATE approvals SET request_json='{invalid' WHERE session_id=?1",
        [&foreign.id],
    )
    .unwrap();
    db.execute(
        "UPDATE turn_admissions SET user_input_snapshot_json='null' WHERE session_id=?1",
        [&foreign.id],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    assert!(repository.list_turn_admissions().is_err());
    assert_eq!(
        repository
            .get_persisted_retirement_settled_turn(&turn.id)
            .unwrap(),
        Some(snapshot.clone())
    );
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        reopened
            .get_persisted_retirement_settled_turn(&turn.id)
            .unwrap(),
        Some(snapshot)
    );
}

#[test]
fn critical_turn_admission_event_rolls_back_authority_on_journal_failure_and_replays_once() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let session = admission_session();
    repository.upsert_session(&session).unwrap();
    let turn = admission_turn();
    let mut admitted = session.clone();
    admitted.status = "turn_admitted".into();
    admitted.active_turn_id = Some(turn.id.clone());
    admitted.updated_at = 11;
    let admission = admission_record();
    let approval = admission_approval();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: repository.database_path.clone(),
    });
    let mut commits = observe_commits(&store);

    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_admission_event BEFORE INSERT ON runtime_events
             WHEN NEW.kind = 'turn.admitted'
             BEGIN SELECT RAISE(ABORT, 'injected critical turn journal failure'); END;",
        )
        .unwrap();
    assert!(repository
        .admit_turn(&admission, &turn, &admitted, Some(&approval))
        .is_err());
    for table in ["turns", "turn_admissions", "approvals"] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} must roll back with failed event");
    }
    let active: Option<String> = connection
        .query_row(
            "SELECT active_turn_id FROM sessions WHERE id=?1",
            [&session.id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(active.is_none());
    assert!(repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 10)
        .unwrap()
        .is_empty());
    assert!(matches!(
        commits.settled_try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    connection
        .execute_batch("DROP TRIGGER fail_admission_event;")
        .unwrap();
    drop(connection);
    repository
        .admit_turn(&admission, &turn, &admitted, Some(&approval))
        .unwrap();
    assert!(commits.wait().is_ok(), "only committed event wakes SSE");
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    let events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 10)
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id, "evt_turn_turn_admission_admitted");
    assert_eq!(events[0].kind, "turn.admitted");
    assert_eq!(events[0].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(events[0].turn_id.as_deref(), Some(turn.id.as_str()));
    assert_eq!(events[0].seq, 1);
    assert_eq!(events[0].payload["projection_source"], "user_visible");
    assert_eq!(events[0].payload["requires_approval"], false);

    // The runtime's post-admission in-memory broadcast uses the identical
    // deterministic ID. Its lookup must be read-only with respect to the
    // canonical event journal and must not wake SSE a second time.
    let same = reopened
        .append_runtime_event(&NewRuntimeEvent {
            event_id: events[0].event_id.clone(),
            scope: RuntimeEventScope::Session,
            scope_id: session.id.clone(),
            session_id: Some(session.id.clone()),
            team_id: None,
            turn_id: Some(turn.id.clone()),
            kind: "turn.admitted".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"noncanonical":"ignored"}),
            provider: None,
            provider_seq: None,
            created_at: 999,
        })
        .unwrap();
    assert_eq!(same, events[0]);
    assert!(matches!(
        commits.settled_try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn forged_existing_turn_event_id_cannot_authorize_new_turn_admission() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    repository.upsert_session(&admission_session()).unwrap();
    let turn = admission_turn();
    let mut admitted = admission_session();
    admitted.status = "turn_admitted".into();
    admitted.active_turn_id = Some(turn.id.clone());
    admitted.updated_at = 11;
    repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_turn_turn_admission_admitted".into(),
            scope: RuntimeEventScope::Session,
            scope_id: "different_session".into(),
            session_id: Some("different_session".into()),
            team_id: None,
            turn_id: Some(turn.id.clone()),
            kind: "forged".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"wrong":"scope"}),
            provider: None,
            provider_seq: None,
            created_at: 1,
        })
        .unwrap();
    assert!(repository
        .admit_turn(&admission_record(), &turn, &admitted, None)
        .is_err());
    assert!(repository.list_turn_admissions().unwrap().is_empty());
    assert!(repository.hydrate_runtime_state().unwrap().turns.is_empty());
    let restored = repository
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .into_iter()
        .find(|row| row.id == admitted.id)
        .unwrap();
    assert!(restored.active_turn_id.is_none());
    let existing = repository
        .list_runtime_events(
            Some((RuntimeEventScope::Session, "different_session")),
            None,
            10,
        )
        .unwrap();
    assert_eq!(
        existing.len(),
        1,
        "forged input must not be deleted or overwritten"
    );
    assert_eq!(existing[0].kind, "forged");
}

#[test]
fn pending_pre_dispatch_approval_journal_failure_rolls_back_waiting_turn_session_and_approval() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let base = admission_session();
    repository.upsert_session(&base).unwrap();
    let mut turn = admission_turn();
    turn.status = "waiting_for_approval".into();
    let mut waiting = base.clone();
    waiting.status = "waiting_for_approval".into();
    waiting.active_turn_id = Some(turn.id.clone());
    waiting.updated_at = 11;
    let mut admission = admission_record();
    admission.dispatch_policy.pre_dispatch_approval_required = true;
    admission.dispatch_policy.permission_mode = Some("require_approval".into());
    let mut approval = admission_approval();
    approval.origin = "runtime_pre_dispatch_policy".into();
    approval.provider_approval_ref = None;

    let db = open_connection(&repository.database_path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_pending_approval_journal BEFORE INSERT ON runtime_events
         WHEN NEW.kind='approval.requested'
         BEGIN SELECT RAISE(ABORT,'forced predispatch approval event failure'); END;",
    )
    .unwrap();
    assert!(repository
        .admit_turn(&admission, &turn, &waiting, Some(&approval))
        .is_err());
    for table in ["turns", "turn_admissions", "approvals"] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            count, 0,
            "{table} must roll back with pending approval event"
        );
    }
    assert!(repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &base.id)), None, 10)
        .unwrap()
        .is_empty());
    assert!(repository
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .iter()
        .find(|session| session.id == base.id)
        .unwrap()
        .active_turn_id
        .is_none());

    db.execute_batch("DROP TRIGGER fail_pending_approval_journal;")
        .unwrap();
    drop(db);
    repository
        .admit_turn(&admission, &turn, &waiting, Some(&approval))
        .unwrap();
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    let hydrated = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(
        hydrated
            .sessions
            .iter()
            .find(|session| session.id == base.id)
            .unwrap()
            .status,
        "waiting_for_approval"
    );
    assert_eq!(
        hydrated
            .turns
            .iter()
            .find(|row| row.id == turn.id)
            .unwrap()
            .status,
        "waiting_for_approval"
    );
    assert!(hydrated
        .approvals
        .iter()
        .any(|row| row.id == approval.id && row.status == "pending"));
    let events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, &base.id)), None, 10)
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, "turn.admitted");
    assert_eq!(events[1].kind, "approval.requested");
    assert_eq!(
        events[1].event_id,
        "evt_approval_approval_admission_requested"
    );
    assert_eq!(events[1].payload["origin"], "runtime_pre_dispatch_policy");
    assert_eq!(events[1].seq, 2);
    assert!(events
        .iter()
        .all(|event| event.criticality == RuntimeEventCriticality::Critical));
}

#[test]
fn explicit_turn_thinking_rechecks_policy_changed_after_runtime_preparation() {
    use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("turn-thinking-policy");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (base_session, agent) =
        workspace_agent_fixture(&workspace, "session_admission", "thinking-authority-target");
    repository
        .create_workspace_agent(&base_session, &agent)
        .unwrap();
    let mut turn = admission_turn();
    turn.started_at = Some(200);
    let mut prepared_session = base_session;
    prepared_session.status = "turn_admitted".into();
    prepared_session.active_turn_id = Some(turn.id.clone());
    prepared_session.updated_at = 200;
    let mut prepared = admission_record();
    prepared.dispatch_policy.turn_settings.thinking_effort =
        Some(runtime_core::ProviderThinkingEffort::High);
    prepared.admitted_at = 200;
    prepared.updated_at = 200;
    // Independent policy commit after the manager prepared an explicit High
    // selection. The IMMEDIATE turn admission must read current authority.
    let independent = open_connection(&repository.database_path).unwrap();
    independent
        .execute(
            "UPDATE workspace_agents SET recreation_policy_json=
        json_set(recreation_policy_json,'$.current_preferences.thinking_effort','low'),
        revision=revision+1,updated_at=150 WHERE session_id=?1",
            [&agent.agent_id],
        )
        .unwrap();
    let counts = || {
        [
            "turns",
            "turn_admissions",
            "runtime_operations",
            "runtime_events",
        ]
        .map(|table| {
            independent
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        })
    };
    let before = counts();
    assert!(
        matches!(
            repository.admit_turn(&prepared, &turn, &prepared_session, None),
            Err(RuntimeError::Conflict(_))
        ),
        "stale thinking selection entered durable dispatch"
    );
    assert_eq!(counts(), before);
    let active: Option<String> = independent
        .query_row(
            "SELECT active_turn_id FROM sessions WHERE id=?1",
            [&agent.agent_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(active.is_none());
    prepared.dispatch_policy.turn_settings.thinking_effort =
        Some(runtime_core::ProviderThinkingEffort::Low);
    repository
        .admit_turn(&prepared, &turn, &prepared_session, None)
        .unwrap();
    assert_eq!(repository.list_turn_admissions().unwrap(), vec![prepared]);
}

#[test]
fn explicit_turn_thinking_requires_workspace_policy_while_omitted_selection_stays_valid() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut session = admission_session();
    repository.upsert_session(&session).unwrap();
    let turn = admission_turn();
    session.status = "turn_admitted".into();
    session.active_turn_id = Some(turn.id.clone());
    session.updated_at = 11;
    let mut admission = admission_record();
    admission.dispatch_policy.turn_settings.thinking_effort =
        Some(runtime_core::ProviderThinkingEffort::High);
    assert!(repository
        .admit_turn(&admission, &turn, &session, None)
        .is_err());
    assert!(repository.list_turn_admissions().unwrap().is_empty());
    admission.dispatch_policy.turn_settings.thinking_effort = None;
    repository
        .admit_turn(&admission, &turn, &session, None)
        .unwrap();
}

fn admission_session() -> SessionRecord {
    SessionRecord {
        id: "session_admission".to_string(),
        provider: "codex".to_string(),
        status: "ready".to_string(),
        cwd: Some("/tmp/repo".to_string()),
        model: Some("test-model".to_string()),
        permission_mode: None,
        system_prompt: None,
        metadata: serde_json::json!({}),
        provider_session_ref: Some("provider_session".to_string()),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 10,
        updated_at: 10,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

fn admission_turn() -> TurnRecord {
    TurnRecord {
        id: "turn_admission".to_string(),
        session_id: "session_admission".to_string(),
        provider_turn_ref: None,
        status: "admitted".to_string(),
        input: serde_json::json!([
            {"type":"text","text":"hello"},
            {"type":"image","path":"/tmp/reference.png","media_type":"image/png"}
        ]),
        source: Some("user".to_string()),
        started_at: Some(11),
        completed_at: None,
        usage: None,
        error: None,
    }
}

fn admission_approval() -> ApprovalRecord {
    ApprovalRecord {
        id: "approval_admission".to_string(),
        session_id: "session_admission".to_string(),
        turn_id: "turn_admission".to_string(),
        origin: "runtime_pre_dispatch_policy".to_string(),
        tool_call_id: None,
        provider_approval_ref: Some("approval_admission".to_string()),
        status: "pending".to_string(),
        request: serde_json::json!({"reason":"test"}),
        response: None,
        created_at: 11,
        resolved_at: None,
    }
}

fn admission_record() -> TurnAdmissionRecord {
    TurnAdmissionRecord {
        turn_id: "turn_admission".to_string(),
        session_id: "session_admission".to_string(),
        provider: "codex".to_string(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot {
            prompt_text: "hello".to_string(),
            image_refs: vec![PersistedUserInputSnapshotImageRef {
                media_type: "image/png".to_string(),
                persisted_absolute_path: Some("/tmp/reference.png".to_string()),
                persisted_display_path: Some("reference.png".to_string()),
            }],
            invocation: Some(PersistedUserInputSnapshotInvocation {
                provider: "runtime".to_string(),
                name: "send".to_string(),
                display_name: Some("Send".to_string()),
                path: Some("session/send".to_string()),
            }),
        },
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: runtime_core::ProviderTurnSettings {
                thinking_effort: None,
                codex_collaboration_mode: Some(runtime_core::CodexCollaborationMode::Plan),
            },
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 11,
        updated_at: 11,
    }
}

#[test]
fn workspace_interrupt_unknown_fences_direct_turn_admission_after_active_projection_clears() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let base = admission_session();
    repository.upsert_session(&base).unwrap();
    let connection = open_connection(&repository.database_path).unwrap();
    // Retained native-effect authority, independent of the new turn's session
    // projection and regardless of whether the old turn still exists locally.
    connection.execute_batch("INSERT INTO runtime_operations
        (operation_id,kind,actor_kind,actor_id,normalized_request_hash,normalized_request_json,phase,created_at,updated_at)
        VALUES ('op_original_interrupt','workspace_interrupt','operator','fixture','hash','{}','requested',1,1);
        INSERT INTO runtime_operation_effects
        (effect_id,operation_id,effect_kind,target_kind,target_id,phase,idempotency_key,evidence_json,attempt_count,created_at,updated_at)
        VALUES ('original_interrupt_effect','op_original_interrupt','workspace_turn_interrupt','agent','session_admission','uncertain','effect-key','{\"turn_id\":\"original_native_turn\"}',1,1,1);").unwrap();
    let mut session = base.clone();
    session.active_turn_id = Some("turn_admission".into());
    session.status = "turn_admitted".into();
    session.updated_at = 11;
    let error = repository
        .admit_turn(
            &admission_record(),
            &admission_turn(),
            &session,
            Some(&admission_approval()),
        )
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved {operation_id,..} if operation_id=="op_original_interrupt")
    );
    for table in ["turns", "turn_admissions", "approvals"] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    assert_eq!(
        repository.hydrate_runtime_state().unwrap().sessions,
        vec![base]
    );
}
