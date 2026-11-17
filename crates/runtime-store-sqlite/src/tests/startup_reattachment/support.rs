use crate::db::open_connection;
use crate::tests::workspace_agent::{registered_workspace, workspace_agent_fixture};
use crate::SqliteRuntimeRepository;
use runtime_core::{
    NewRuntimeEvent, PersistedUserInputSnapshot, ProviderSession, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionRecord, StartupReattachAdmission,
    StartupReattachCommand, StartupReattachGrant, StartupReattachHold, TurnAdmissionRecord,
    TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
    TurnRecord, WorkspaceRecord,
};
use rusqlite::params;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub(super) const SESSION: &str = "startup_sqlite_session";
pub(super) const RESUME_CLAIM: &str = "operator_agent_restore_native";

pub(super) struct Fixture {
    pub temp: tempfile::TempDir,
    pub repository: SqliteRuntimeRepository,
    pub workspace: WorkspaceRecord,
}

impl Fixture {
    pub fn new(managed: bool) -> Self {
        Self::with_metadata(managed, json!({"source":"startup-sqlite-test"}))
    }

    pub fn with_metadata(managed: bool, metadata: Value) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        std::fs::create_dir_all(&root).unwrap();
        let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
        repository.initialize_schema().unwrap();
        let workspace = registered_workspace(&repository, &root);
        let (mut session, mut agent) =
            workspace_agent_fixture(&workspace, SESSION, "startup-sqlite-heron");
        session.metadata = metadata.clone();
        agent.metadata = metadata;
        if managed {
            repository.create_workspace_agent(&session, &agent).unwrap();
        } else {
            repository.upsert_session(&session).unwrap();
        }
        Self {
            temp,
            repository,
            workspace,
        }
    }

    pub fn session(&self) -> SessionRecord {
        session(&self.repository, SESSION)
    }

    pub fn command(&self, operation: &str, instance: &str) -> StartupReattachCommand {
        command(&self.repository, operation, instance, SESSION)
    }

    pub fn grant(&self, operation: &str) -> StartupReattachGrant {
        execute(&self.repository, &self.command(operation, operation), 300)
    }
}

pub(super) fn session(repository: &SqliteRuntimeRepository, id: &str) -> SessionRecord {
    repository
        .get_persisted_retirement_review_session(id)
        .unwrap()
        .unwrap()
        .session
}

pub(super) fn command(
    repository: &SqliteRuntimeRepository,
    operation: &str,
    instance: &str,
    session_id: &str,
) -> StartupReattachCommand {
    let session = session(repository, session_id);
    let agent = repository.get_workspace_agent_by_id(session_id).unwrap();
    let (provider, request) =
        runtime_core::startup_resume_request_from_authority(&session, agent.as_ref()).unwrap();
    StartupReattachCommand {
        operation_id: operation.into(),
        startup_instance_id: instance.into(),
        session_id: session_id.into(),
        provider,
        request,
    }
}

pub(super) fn execute(
    repository: &SqliteRuntimeRepository,
    command: &StartupReattachCommand,
    at: i64,
) -> StartupReattachGrant {
    match repository.admit_startup_reattach(command, at).unwrap() {
        StartupReattachAdmission::Execute(grant) => grant,
        other => panic!("fresh attempt did not receive authority: {other:?}"),
    }
}

pub(super) fn ack(grant: &StartupReattachGrant, fill_canonical: bool) -> ProviderSession {
    let request = &grant.intent.authority.request;
    ProviderSession {
        runtime_session_id: request.runtime_session_id.clone(),
        provider_session_ref: request.provider_session_ref.clone(),
        canonical_provider_session_ref: if fill_canonical {
            Some(request.provider_session_ref.clone())
        } else {
            request.canonical_provider_session_ref.clone()
        },
    }
}

pub(super) fn assert_unresolved<T: std::fmt::Debug>(result: Result<T, RuntimeError>, owner: &str) {
    assert!(
        matches!(result,
            Err(RuntimeError::OperationUnresolved { ref operation_id, .. }) if operation_id == owner
        ),
        "expected original owner {owner}, got {result:?}"
    );
}

pub(super) fn snapshot(repository: &SqliteRuntimeRepository) -> BTreeMap<String, Vec<Vec<String>>> {
    let db = open_connection(&repository.database_path).unwrap();
    let tables = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).unwrap().query_map([], |row| row.get::<_, String>(0)).unwrap()
        .collect::<Result<Vec<_>, _>>().unwrap();
    tables
        .into_iter()
        .map(|table| {
            let quoted = table.replace('"', "\"\"");
            let mut statement = db.prepare(&format!("SELECT * FROM \"{quoted}\"")).unwrap();
            let columns = statement.column_count();
            let mut rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| Ok(format!("{:?}", row.get_ref(column)?)))
                        .collect::<Result<Vec<_>, rusqlite::Error>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            rows.sort();
            (table, rows)
        })
        .collect()
}

pub(super) fn assert_attempt(
    repository: &SqliteRuntimeRepository,
    grant: &StartupReattachGrant,
    operation_phase: &str,
    effect_phase: &str,
    claim_live: bool,
) {
    let db = open_connection(&repository.database_path).unwrap();
    let operation_id = &grant.intent.command.operation_id;
    let actual: (String, String, i64, String, String, String) = db
        .query_row(
            "SELECT o.phase,e.phase,e.attempt_count,e.target_kind,e.target_id,e.evidence_json
         FROM runtime_operations o JOIN runtime_operation_effects e USING(operation_id)
         WHERE o.operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        (&*actual.0, &*actual.1, actual.2),
        (operation_phase, effect_phase, 1)
    );
    assert_eq!(actual.3, "provider_session");
    assert_eq!(actual.4, grant.intent.command.session_id);
    let evidence: Value = serde_json::from_str(&actual.5).unwrap();
    assert_eq!(evidence["grant"], serde_json::to_value(grant).unwrap());
    let claims: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims
         WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3
           AND claim_mode='exclusive' AND fence_generation=?4 AND acquired_at=?5",
            params![
                RESUME_CLAIM,
                grant.claim.resource_id,
                operation_id,
                grant.claim.fence_generation,
                grant.claim.acquired_at
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claims, i64::from(claim_live));
}

pub(super) fn hold_observed(
    repository: &SqliteRuntimeRepository,
    grant: &StartupReattachGrant,
    acknowledgement: &ProviderSession,
) {
    let projection = repository
        .get_persisted_retirement_review_session(&grant.intent.command.session_id)
        .unwrap()
        .unwrap();
    assert!(matches!(
        repository.hold_startup_reattach(grant, Some(acknowledgement), "receipt withheld", 500).unwrap(),
        StartupReattachHold::Held { operation_id } if operation_id == grant.intent.command.operation_id
    ));
    assert_attempt(repository, grant, "manual_review", "observed", true);
    let saved = repository
        .get_persisted_retirement_review_session(&grant.intent.command.session_id)
        .unwrap()
        .unwrap();
    assert_eq!(saved, projection);
}

pub(super) struct TurnFixture {
    pub session: SessionRecord,
    pub turn: TurnRecord,
    pub admission: TurnAdmissionRecord,
}

pub(super) fn pending_turn(session: &SessionRecord) -> TurnFixture {
    let input = json!([{"type":"text","text":"  user prompt  "}]);
    let turn = TurnRecord {
        id: "startup_sqlite_turn".into(),
        session_id: session.id.clone(),
        provider_turn_ref: None,
        status: "admitted".into(),
        input: input.clone(),
        source: Some("user".into()),
        started_at: Some(200),
        completed_at: None,
        usage: None,
        error: None,
    };
    let admission = TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: session.id.clone(),
        provider: session.provider.clone(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot::from_input(input.as_array().unwrap()),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: session.permission_mode.clone(),
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 200,
        updated_at: 200,
    };
    let mut admitted = session.clone();
    admitted.active_turn_id = Some(turn.id.clone());
    admitted.status = "turn_admitted".into();
    admitted.updated_at = 200;
    TurnFixture {
        session: admitted,
        turn,
        admission,
    }
}

pub(super) fn event(
    turn: &TurnRecord,
    suffix: &str,
    kind: &str,
    payload: Value,
    at: i64,
) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: format!("evt_turn_{}_{suffix}", turn.id),
        scope: RuntimeEventScope::Session,
        scope_id: turn.session_id.clone(),
        session_id: Some(turn.session_id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: at,
    }
}

pub(super) fn start_pending(
    repository: &SqliteRuntimeRepository,
    pending: &TurnFixture,
) -> TurnFixture {
    let mut dispatching = pending.admission.clone();
    dispatching.dispatch_state = TurnDispatchState::Dispatching;
    dispatching.updated_at = 210;
    repository
        .claim_pending_turn_dispatch(&dispatching)
        .unwrap();
    let mut dispatched = dispatching.clone();
    dispatched.dispatch_state = TurnDispatchState::Dispatched;
    dispatched.provider_native_turn_id = Some("startup-native-turn".into());
    dispatched.updated_at = 220;
    let mut running = pending.turn.clone();
    running.status = "in_progress".into();
    let mut session = pending.session.clone();
    session.status = "turn_running".into();
    session.updated_at = 220;
    let receipt = repository
        .commit_provider_turn_started(
            &dispatching,
            &dispatched,
            &pending.turn,
            &running,
            &pending.session,
            &session,
            &event(&running, "started", "turn.started", json!({}), 220),
        )
        .unwrap();
    TurnFixture {
        session: receipt.session,
        turn: receipt.turn,
        admission: receipt.admission,
    }
}

pub(super) fn running(fixture: &Fixture) -> TurnFixture {
    let pending = pending_turn(&fixture.session());
    fixture
        .repository
        .admit_turn(&pending.admission, &pending.turn, &pending.session, None)
        .unwrap();
    start_pending(&fixture.repository, &pending)
}

pub(super) fn quarantine(
    repository: &SqliteRuntimeRepository,
    before: &TurnFixture,
) -> TurnFixture {
    let mut turn = before.turn.clone();
    turn.status = "provider_event_recovery_required".into();
    turn.error = Some(json!({"code":"stream_lost","message":"provider bridge lost"}));
    let mut session = before.session.clone();
    session.status = "turn_recovery_required".into();
    session.failure_code = Some("stream_lost".into());
    session.failure_message = Some("provider bridge lost".into());
    session.updated_at = 400;
    let receipt = repository
        .commit_provider_failure_result(
            &before.turn,
            &turn,
            &before.session,
            &session,
            &event(
                &turn,
                "event_stream_unknown",
                "provider.event_stream_unknown",
                json!({"code":"stream_lost","message":"provider bridge lost"}),
                400,
            ),
        )
        .unwrap();
    TurnFixture {
        session: receipt.session.unwrap(),
        turn: receipt.turn,
        admission: before.admission.clone(),
    }
}

pub(super) fn terminal(
    repository: &SqliteRuntimeRepository,
    before: &TurnFixture,
    status: &str,
    metadata: Value,
) -> (SessionRecord, TurnRecord, NewRuntimeEvent) {
    let mut turn = before.turn.clone();
    turn.status = status.into();
    turn.completed_at = Some(400);
    turn.usage = Some(json!({"assistant_text":"  done  "}));
    let mut session = before.session.clone();
    session.status = "ready".into();
    session.active_turn_id = None;
    session.updated_at = 400;
    session.metadata = metadata;
    let event = event(
        &turn,
        "terminal",
        &format!("turn.{status}"),
        json!({"status":status,"usage":turn.usage,"error":null,"assistant_text":"done"}),
        400,
    );
    let receipt = repository
        .commit_provider_turn_result(
            &before.turn,
            &turn,
            Some(&before.session),
            Some(&session),
            &event,
        )
        .unwrap();
    assert_eq!(receipt.session.as_ref(), Some(&session));
    assert_eq!(receipt.turn, turn);
    (session, turn, event)
}
