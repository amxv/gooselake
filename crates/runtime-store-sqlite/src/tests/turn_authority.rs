use super::*;
use runtime_core::{
    PersistedUserInputSnapshot, PersistedUserInputSnapshotImageRef,
    PersistedUserInputSnapshotInvocation, TurnAdmissionRecord, TurnCorrelationState,
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
    let mut commits = store.subscribe_event_commits().unwrap();

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
        commits.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    connection
        .execute_batch("DROP TRIGGER fail_admission_event;")
        .unwrap();
    drop(connection);
    repository
        .admit_turn(&admission, &turn, &admitted, Some(&approval))
        .unwrap();
    assert!(commits.try_recv().is_ok(), "only committed event wakes SSE");
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
        commits.try_recv(),
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
                thinking_effort: Some(runtime_core::ProviderThinkingEffort::High),
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
