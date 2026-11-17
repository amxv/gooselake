//! Real SQLite + deterministic fake-provider boundaries for delivery/startup.
use super::native_review_refusal::database_snapshot;
use super::workspace_retirement_support::{
    retirement_fixture, RetirementFixture, RetirementProvider,
};
use super::*;
use runtime_core::{
    AgentDeliveryRecord, AgentDirectMessageRequest, StartupReattachAdmission,
    StartupReattachCommand, TeamCommsService, TeamDeliveryRecord, TeamInterruptAllRequest,
    TeamMemberRecord, TeamRecord, TeamSendDirectRequest,
};
use std::sync::atomic::Ordering;

const PROBE: &str = "delivery-startup-native-probe";
const TEAM: &str = "delivery-startup-legacy-team";
const LEGACY_OWNER: &str = "delivery-late/original:legacy?%coordinate";

struct Fixture {
    temp: tempfile::TempDir,
    native: RetirementFixture,
    provider: Arc<RetirementProvider>,
    service: Arc<RuntimeTeamCommsService>,
    recipient: String,
    sender: String,
    turn_id: String,
    legacy: bool,
}

enum Delivery {
    Agent(AgentDeliveryRecord),
    Legacy(TeamDeliveryRecord),
}
impl Delivery {
    fn id(&self) -> &str {
        match self {
            Self::Agent(row) => &row.id,
            Self::Legacy(row) => &row.id,
        }
    }
    fn message(&self) -> &str {
        match self {
            Self::Agent(row) => &row.message_id,
            Self::Legacy(row) => &row.message_id,
        }
    }
    fn status(&self) -> &str {
        match self {
            Self::Agent(row) => &row.status,
            Self::Legacy(row) => &row.status,
        }
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

impl Fixture {
    async fn new(legacy: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("delivery-startup-workspace");
        fs::create_dir_all(&root).unwrap();
        let provider = RetirementProvider::new(ProviderKind::Codex);
        let other = RetirementProvider::new(ProviderKind::Claude);
        let native = retirement_fixture(temp.path(), &[provider.clone(), other]).await;
        let workspace = native.register(&root).await;
        let recipient = native
            .agent(&workspace.workspace_id, ProviderKind::Codex)
            .await
            .agent_id;
        let sender = native
            .agent(&workspace.workspace_id, ProviderKind::Claude)
            .await
            .agent_id;
        if legacy {
            native
                .store
                .upsert_team(&TeamRecord {
                    id: TEAM.into(),
                    name: "Delivery startup".into(),
                    lead_agent_id: sender.clone(),
                    created_by: "operator".into(),
                    created_at: now_ms(),
                    updated_at: now_ms(),
                    deleted_at: None,
                })
                .unwrap();
            for id in [&sender, &recipient] {
                native
                    .store
                    .upsert_team_member(&TeamMemberRecord {
                        team_id: TEAM.into(),
                        agent_id: id.clone(),
                        title: None,
                        joined_at: now_ms(),
                        added_by: "operator".into(),
                        creator_agent_id: None,
                        creator_compaction_subscription: "auto".into(),
                        worktree_id: None,
                    })
                    .unwrap();
            }
        }
        let service = RuntimeTeamCommsService::new(
            native.store.clone(),
            native.runtime.clone(),
            RuntimeTeamCommsConfig {
                enabled: true,
                max_pending_deliveries: 100,
            },
        )
        .unwrap();
        let (status, turn) = native
            .call(
                "POST",
                &format!("/v2/agents/{recipient}/turns"),
                Some(json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})),
                Some("original-active-delivery-target"),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{turn}");
        let turn_id = turn["turn_id"].as_str().unwrap().to_owned();
        Self {
            temp,
            native,
            provider,
            service,
            recipient,
            sender,
            turn_id,
            legacy,
        }
    }

    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.temp.path().join("runtime.sqlite3")).unwrap()
    }

    fn delivery(&self) -> Delivery {
        let state = self.native.store.hydrate_runtime_state().unwrap();
        if self.legacy {
            let message = state
                .team_messages
                .iter()
                .find(|row| row.input.to_string().contains(PROBE))
                .unwrap();
            let row = state
                .team_deliveries
                .into_iter()
                .find(|row| row.message_id == message.id)
                .unwrap();
            Delivery::Legacy(row)
        } else {
            let message = state
                .agent_messages
                .iter()
                .find(|row| row.input.to_string().contains(PROBE))
                .unwrap();
            let row = state
                .agent_deliveries
                .into_iter()
                .find(|row| row.message_id == message.id)
                .unwrap();
            Delivery::Agent(row)
        }
    }

    async fn send(&self) -> Result<serde_json::Value, RuntimeError> {
        if self.legacy {
            self.service
                .send_direct(TeamSendDirectRequest {
                    team_id: TEAM.into(),
                    sender_agent_id: self.sender.clone(),
                    recipient_agent_id: self.recipient.clone(),
                    input: json!([{"type":"text","text":PROBE}]),
                    image_paths: vec![],
                    priority: "normal".into(),
                    policy: "immediate_interrupt".into(),
                    correlation_id: None,
                    reply_to_message_id: None,
                    idempotency_key: Some("original-message-key".into()),
                })
                .await
                .map(|ack| serde_json::to_value(ack).unwrap())
        } else {
            self.service
                .send_agent_direct(AgentDirectMessageRequest {
                    sender_agent_id: self.sender.clone(),
                    recipient_agent_id: self.recipient.clone(),
                    input: json!([{"type":"text","text":PROBE}]),
                    image_paths: vec![],
                    priority: "normal".into(),
                    policy: "immediate_interrupt".into(),
                    correlation_id: None,
                    reply_to_message_id: None,
                    idempotency_key: Some("original-message-key".into()),
                })
                .await
                .map(|ack| serde_json::to_value(ack).unwrap())
        }
    }

    async fn command(&self) -> StartupReattachCommand {
        let session = self
            .native
            .runtime
            .get_session(&self.recipient)
            .await
            .unwrap();
        let agent = self
            .native
            .store
            .get_workspace_agent_by_id(&self.recipient)
            .unwrap();
        let (provider, request) =
            runtime_core::startup_resume_request_from_authority(&session, agent.as_ref()).unwrap();
        StartupReattachCommand {
            operation_id: "op_delivery_startup_test".into(),
            startup_instance_id: "delivery_startup_test".into(),
            session_id: self.recipient.clone(),
            provider,
            request,
        }
    }

    fn assert_no_startup_operation_for_recipient(&self) {
        let count: i64 = self
            .db()
            .query_row(
                "SELECT COUNT(*) FROM runtime_operations WHERE kind='startup_session_reattach'
             AND json_extract(normalized_request_json,'$.command.session_id')=?1",
                [&self.recipient],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn held_startup_grant_keeps_both_delivery_variants_pending_with_zero_native_effects() {
    for legacy in [false, true] {
        let fixture = Fixture::new(legacy).await;
        let command = fixture.command().await;
        let grant = match fixture
            .native
            .store
            .admit_startup_reattach(&command, now_ms())
            .unwrap()
        {
            StartupReattachAdmission::Execute(grant) => grant,
            other => panic!("expected first resume authority: {other:?}"),
        };
        let first = fixture.send().await.unwrap();
        let delivery = fixture.delivery();
        assert_eq!(delivery.status(), "pending");
        assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.resumes.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.turns_containing(PROBE).await, 0);
        let before = database_snapshot(&fixture.db());
        let replay = fixture.send().await.unwrap();
        assert_eq!(replay["message"]["id"], first["message"]["id"]);
        assert_eq!(fixture.delivery().id(), delivery.id());
        assert_eq!(database_snapshot(&fixture.db()), before);
        // Release only with this controllable provider's actual acknowledgement.
        let acknowledgement = fixture
            .provider
            .resume_session_with_policy(grant.intent.authority.request.clone())
            .await
            .unwrap();
        fixture
            .native
            .store
            .complete_startup_reattach(&grant, &acknowledgement, now_ms())
            .unwrap();
        assert_eq!(fixture.provider.resumes.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.turns_containing(PROBE).await, 0);
    }
}

#[tokio::test]
async fn injecting_native_approval_interrupt_blocks_startup_and_original_message_never_reinterrupts(
) {
    for legacy in [false, true] {
        let fixture = Arc::new(Fixture::new(legacy).await);
        fixture
            .native
            .runtime
            .record_provider_approval(
                &fixture.recipient,
                &fixture.turn_id,
                "delivery-native-approval",
                None,
                json!({"reason":"provider waiting"}),
            )
            .await
            .unwrap();
        let gate = fixture.provider.pause_next_interrupt().await;
        let sending = fixture.clone();
        let task = tokio::spawn(async move { sending.send().await });
        timeout(Duration::from_secs(5), gate.wait_until_entered())
            .await
            .expect("known Dispatched native approval target must reach its owned interrupt");
        let delivery = fixture.delivery();
        assert_eq!(delivery.status(), "injecting");
        assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 1);
        let summary = fixture.native.runtime.recover_startup().await.unwrap();
        assert_eq!(fixture.provider.resumes.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.turns_containing(PROBE).await, 0);
        assert!(
            summary
                .notes
                .iter()
                .any(|note| note.contains(delivery.id()) && note.contains(delivery.message())),
            "{summary:?}"
        );
        fixture.assert_no_startup_operation_for_recipient();
        gate.release();
        let first = timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let sends = fixture.provider.turns_containing(PROBE).await;
        let replay = fixture.send().await.unwrap();
        assert_eq!(first["message"]["id"], replay["message"]["id"]);
        assert_eq!(fixture.delivery().id(), delivery.id());
        assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.turns_containing(PROBE).await, sends);
    }
}

#[tokio::test]
async fn malformed_or_unknown_native_target_never_reaches_delivery_interrupt() {
    for legacy in [false, true] {
        for fault in [
            "pending",
            "dispatching",
            "unknown",
            "not_dispatched",
            "no_admission",
            "quarantine",
            "wrong_provider",
        ] {
            let fixture = Fixture::new(legacy).await;
            let db = fixture.db();
            match fault {
                "no_admission" => {
                    db.execute(
                        "DELETE FROM turn_admissions WHERE turn_id=?1",
                        [&fixture.turn_id],
                    )
                    .unwrap();
                }
                "quarantine" => {
                    db.execute(
                        "UPDATE sessions SET status='turn_recovery_required' WHERE id=?1",
                        [&fixture.recipient],
                    )
                    .unwrap();
                    db.execute(
                        "UPDATE turns SET status='provider_session_recovery_required' WHERE id=?1",
                        [&fixture.turn_id],
                    )
                    .unwrap();
                }
                "wrong_provider" => {
                    db.execute(
                        "UPDATE turn_admissions SET provider='claude' WHERE turn_id=?1",
                        [&fixture.turn_id],
                    )
                    .unwrap();
                }
                state => {
                    db.execute(
                        "UPDATE turn_admissions SET dispatch_state=?1 WHERE turn_id=?2",
                        rusqlite::params![state, fixture.turn_id],
                    )
                    .unwrap();
                }
            }
            let first = fixture.send().await.unwrap();
            let delivery = fixture.delivery();
            assert_eq!(
                delivery.status(),
                "injecting",
                "legacy={legacy}, fault={fault}"
            );
            assert_eq!(
                fixture.provider.interrupts.load(Ordering::SeqCst),
                0,
                "{fault}"
            );
            assert_eq!(fixture.provider.turns_containing(PROBE).await, 0, "{fault}");
            let before = database_snapshot(&db);
            let replay = fixture.send().await.unwrap();
            assert_eq!(first["message"]["id"], replay["message"]["id"]);
            assert_eq!(fixture.delivery().id(), delivery.id());
            assert_eq!(database_snapshot(&db), before);
            assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn late_legacy_obligation_retains_original_delivery_and_typed_cause_with_zero_interrupt() {
    for legacy in [false, true] {
        let fixture = Fixture::new(legacy).await;
        let event_kind = if legacy {
            "team_delivery.injecting"
        } else {
            "agent_delivery.injecting"
        };
        let payload = json!({"spawned_session_id":fixture.recipient})
            .to_string()
            .replace('\'', "''");
        // The trigger inserts old unresolved history after the first claim
        // guard, before its transaction returns to the native caller.
        fixture.db().execute_batch(&format!(
            "CREATE TRIGGER delivery_late_legacy AFTER INSERT ON runtime_events
             WHEN NEW.kind='{event_kind}'
             BEGIN INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
               VALUES('{LEGACY_OWNER}','original-team','spawn_member_with_worktree','rolled_back','{payload}',1,2);
             END;"
        )).unwrap();
        let first = fixture.send().await.unwrap();
        let delivery = fixture.delivery();
        assert_eq!(delivery.status(), "injecting");
        assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.turns_containing(PROBE).await, 0);
        let projection = fixture
            .native
            .store
            .get_persisted_retirement_review_session(&fixture.recipient)
            .unwrap()
            .unwrap();
        let turn = projection
            .turns
            .iter()
            .find(|row| row.id == fixture.turn_id)
            .unwrap();
        let error = match &delivery {
            Delivery::Agent(row) => fixture.native.store.verify_agent_delivery_interrupt_owner(
                row,
                &projection.session,
                turn,
            ),
            Delivery::Legacy(row) => fixture.native.store.verify_legacy_delivery_interrupt_owner(
                row,
                &projection.session,
                turn,
            ),
        }
        .unwrap_err();
        assert!(
            matches!(&error,RuntimeError::LegacyNativeObligationBlocked{coordinate,..}
            if coordinate.operation_id==LEGACY_OWNER),
            "{error:?}"
        );
        assert!(error.legacy_native_recovery().is_some());
        let before = database_snapshot(&fixture.db());
        let replay = fixture.send().await.unwrap();
        assert_eq!(first["message"]["id"], replay["message"]["id"]);
        assert_eq!(fixture.delivery().id(), delivery.id());
        assert_eq!(database_snapshot(&fixture.db()), before);
        assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn raw_interrupt_and_legacy_interrupt_all_are_repeated_pure_refusals() {
    let fixture = Fixture::new(true).await;
    let before = database_snapshot(&fixture.db());
    let session = fixture
        .native
        .runtime
        .get_session(&fixture.recipient)
        .await
        .unwrap();
    for _ in 0..2 {
        for (session_id, turn_id) in [
            (fixture.recipient.as_str(), fixture.turn_id.as_str()),
            ("missing", "missing"),
            ("", ""),
        ] {
            assert!(matches!(
                fixture
                    .native
                    .runtime
                    .interrupt_turn(session_id, turn_id)
                    .await,
                Err(RuntimeError::Unsupported(_))
            ));
        }
        for team_id in [TEAM, "missing", ""] {
            assert!(matches!(
                fixture
                    .service
                    .interrupt_all_team_turns(TeamInterruptAllRequest {
                        team_id: team_id.into()
                    })
                    .await,
                Err(RuntimeError::Unsupported(_))
            ));
        }
    }
    assert_eq!(database_snapshot(&fixture.db()), before);
    assert_eq!(
        fixture
            .native
            .runtime
            .get_session(&fixture.recipient)
            .await
            .unwrap(),
        session
    );
    assert_eq!(fixture.provider.interrupts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.turns_containing(PROBE).await, 0);
}
