use super::native_review_refusal::{assert_refusal, database_snapshot};
use super::workspace_worktree_routes::call;
use super::*;
use runtime_core::{RuntimeApp, SessionRecord, StartupRecoverySummary};
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, Debug)]
enum StartupFault {
    LostAcknowledgement,
    CompletionReceipt,
    CompletionAndHold,
}

impl StartupFault {
    fn phase(self) -> &'static str {
        match self {
            Self::CompletionAndHold => "requested",
            _ => "manual_review",
        }
    }

    fn effect_phase(self) -> &'static str {
        match self {
            Self::LostAcknowledgement => "uncertain",
            Self::CompletionReceipt => "observed",
            Self::CompletionAndHold => "started",
        }
    }

    fn diagnostic(self) -> &'static str {
        match self {
            Self::LostAcknowledgement => {
                "provider resumed its native session but acknowledgement was lost"
            }
            _ => "startup HTTP receipt fault",
        }
    }
}

struct Fixture {
    token: String,
    temp: tempfile::TempDir,
    provider: Arc<TestProvider>,
    app: Arc<RuntimeApp>,
    session: SessionRecord,
}

impl Fixture {
    async fn new() -> Self {
        let (router, token, temp, provider, runtime, app) =
            build_test_router_with_services(TeamMcpPolicy::default()).await;
        let root = temp.path().join("startup-http-workspace");
        std::fs::create_dir_all(&root).unwrap();
        let (status, workspace) = call(
            &router,
            &token,
            "POST",
            "/v2/workspaces",
            Some(serde_json::json!({"canonical_root": root})),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{workspace}");
        let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
        let (status, created) = call(
            &router,
            &token,
            "POST",
            &format!("/v2/workspaces/{workspace_id}/agents"),
            Some(serde_json::json!({"provider": "codex", "model": "test-model"})),
            Some("startup-http-agent"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let agent_id = created["agent_id"].as_str().unwrap();
        finish_test_onboarding(&runtime, workspace_id, agent_id).await;
        let session = runtime.get_session(agent_id).await.unwrap();
        assert!(session.active_turn_id.is_none());
        assert_eq!(session.status, "ready");
        assert_eq!(provider.create_call_count(), 1);
        assert_eq!(provider.restore_call_count().await, 0);
        Self {
            token,
            temp,
            provider,
            app,
            session,
        }
    }
}

fn startup_ids(db: &rusqlite::Connection, session_id: &str) -> Vec<String> {
    db.prepare(
        "SELECT operation_id FROM runtime_operations
         WHERE kind='startup_session_reattach'
           AND json_extract(normalized_request_json,'$.command.session_id')=?1
         ORDER BY operation_id",
    )
    .unwrap()
    .query_map([session_id], |row| row.get(0))
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

fn install_fault(db: &rusqlite::Connection, fault: StartupFault) {
    if matches!(fault, StartupFault::LostAcknowledgement) {
        return;
    }
    let phase = if matches!(fault, StartupFault::CompletionAndHold) {
        "1"
    } else {
        "json_extract(NEW.payload_json,'$.to_phase')='completed'"
    };
    // Only the second transition of this fixture's startup operation fails.
    // Admission is real and commits before the fake native resume boundary.
    db.execute_batch(&format!(
        "CREATE TRIGGER startup_http_receipt_fault BEFORE INSERT ON runtime_events
         WHEN NEW.kind='operation.transitioned'
           AND json_extract(NEW.payload_json,'$.sequence')=2
           AND ({phase})
           AND EXISTS (
             SELECT 1 FROM runtime_operations AS operation
             WHERE operation.operation_id=json_extract(NEW.payload_json,'$.operation_id')
               AND operation.kind='startup_session_reattach'
           )
         BEGIN SELECT RAISE(ABORT,'startup HTTP receipt fault'); END;"
    ))
    .unwrap();
}

fn assert_withheld(summary: &StartupRecoverySummary, operation_id: &str) {
    assert_eq!(summary.sessions_scanned, 1, "{summary:?}");
    assert_eq!(summary.resumed_sessions, 0, "{summary:?}");
    assert_eq!(summary.resumed_waits, 0, "{summary:?}");
    assert!(
        summary.notes.iter().any(|note| note.contains(operation_id)),
        "startup must preserve the original recovery coordinate: {summary:?}"
    );
}

async fn inspect(router: &Router, token: &str, operation_id: &str) -> serde_json::Value {
    let (status, detail) = call(
        router,
        token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["operation"]["operation_id"], operation_id);
    detail
}

fn assert_original_authority(
    detail: &serde_json::Value,
    fixture: &Fixture,
    operation_id: &str,
    fault: StartupFault,
) {
    let operation = &detail["operation"];
    assert_eq!(operation["kind"], "startup_session_reattach");
    assert_eq!(operation["phase"], fault.phase());
    assert!(operation["exact_terminal_result"].is_null());
    assert_eq!(
        operation["normalized_request"]["command"]["session_id"],
        fixture.session.id
    );
    assert_eq!(detail["effects"].as_array().unwrap().len(), 1);
    assert_eq!(detail["claims"].as_array().unwrap().len(), 1);
    let effect = &detail["effects"][0];
    assert_eq!(effect["effect_id"], format!("{operation_id}:resume"));
    assert_eq!(effect["operation_id"], operation_id);
    assert_eq!(effect["effect_kind"], "provider_session_reattach");
    assert_eq!(effect["target_kind"], "provider_session");
    assert_eq!(effect["target_id"], fixture.session.id);
    assert_eq!(effect["phase"], fault.effect_phase());
    assert_eq!(effect["attempt_count"], 1);
    let claim = &detail["claims"][0];
    assert_eq!(claim["resource_kind"], "operator_agent_restore_native");
    assert_eq!(claim["resource_id"], fixture.session.id);
    assert_eq!(claim["owner_operation_id"], operation_id);
    assert_eq!(claim["claim_mode"], "exclusive");
    assert!(claim["fence_generation"].as_u64().unwrap() > 0);
    let evidence = &effect["evidence"];
    assert_eq!(evidence["grant"]["intent"], operation["normalized_request"]);
    assert_eq!(
        evidence["grant"]["normalized_request_hash"],
        operation["normalized_request_hash"]
    );
    assert_eq!(evidence["grant"]["claim"], *claim);
    let request = &evidence["grant"]["intent"]["authority"]["request"];
    assert_eq!(request["runtime_session_id"], fixture.session.id);
    assert_eq!(
        request["provider_session_ref"],
        fixture.session.provider_session_ref.as_deref().unwrap()
    );
    match fault {
        StartupFault::LostAcknowledgement => {
            assert_eq!(operation["error_code"], "outcome_unresolved");
            assert!(operation["error_message"]
                .as_str()
                .unwrap()
                .contains(fault.diagnostic()));
            assert!(evidence["acknowledgement"].is_null());
            assert_eq!(evidence["identity_matches"], false);
        }
        StartupFault::CompletionReceipt => {
            assert_eq!(operation["error_code"], "outcome_unresolved");
            assert!(operation["error_message"]
                .as_str()
                .unwrap()
                .contains(fault.diagnostic()));
            assert_eq!(evidence["identity_matches"], true);
            assert_eq!(
                evidence["acknowledgement"]["runtime_session_id"],
                fixture.session.id
            );
            assert_eq!(
                evidence["acknowledgement"]["provider_session_ref"],
                request["provider_session_ref"]
            );
            assert_eq!(
                evidence["acknowledgement"]["canonical_provider_session_ref"],
                request["canonical_provider_session_ref"]
            );
        }
        StartupFault::CompletionAndHold => {
            // The runtime saw the ACK, but neither second-transition journal
            // committed. Inspection must not fabricate persisted ACK evidence.
            assert!(operation["error_code"].is_null());
            assert!(operation["error_message"].is_null());
            assert!(evidence.get("acknowledgement").is_none());
            assert!(evidence.get("identity_matches").is_none());
        }
    }
    let transitions = detail["transitions"].as_array().unwrap();
    assert_eq!(
        transitions.len(),
        if matches!(fault, StartupFault::CompletionAndHold) {
            1
        } else {
            2
        }
    );
    assert_eq!(transitions.last().unwrap()["to_phase"], fault.phase());
    assert!(detail["outbox"].as_array().unwrap().is_empty());
    assert!(detail["receipts"].as_array().unwrap().is_empty());
}

async fn exercise_restart_refusal(fault: StartupFault) {
    let fixture = Fixture::new().await;
    let db = rusqlite::Connection::open(fixture.temp.path().join("runtime.sqlite3")).unwrap();
    let sends = fixture.provider.send_invocations.load(Ordering::SeqCst);
    let creates = fixture.provider.create_call_count();
    let resumes = fixture.provider.restore_call_count().await;
    assert!(startup_ids(&db, &fixture.session.id).is_empty());
    if matches!(fault, StartupFault::LostAcknowledgement) {
        fixture.provider.set_restore_failure_mode(2);
    }
    install_fault(&db, fault);
    // This rebuilds real router/runtime caches over SQLite, retaining only the
    // inert provider registry. The helper does not call recovery automatically.
    let (mut router, mut runtime, mut app) = reopen_test_router_with_services(
        &fixture.app,
        fixture.temp.path(),
        &fixture.token,
        TeamMcpPolicy::default(),
    )
    .await;
    assert_eq!(fixture.provider.restore_call_count().await, resumes);
    let summary = runtime.recover_startup().await.unwrap();
    let ids = startup_ids(&db, &fixture.session.id);
    assert_eq!(ids.len(), 1, "{summary:?}");
    let operation_id = ids[0].clone();
    assert_withheld(&summary, &operation_id);
    assert!(
        summary
            .notes
            .iter()
            .any(|note| { note.contains(&operation_id) && note.contains(fault.diagnostic()) }),
        "{summary:?}"
    );
    if matches!(fault, StartupFault::CompletionAndHold) {
        let notes = summary.notes.join("\n");
        assert!(notes.contains("startup ACK observed"), "{notes}");
        assert!(notes.contains("hold failed"), "{notes}");
        assert!(notes.contains("original claim remains"), "{notes}");
    }
    assert_eq!(fixture.provider.restore_call_count().await, resumes + 1);
    let original = inspect(&router, &fixture.token, &operation_id).await;
    assert_original_authority(&original, &fixture, &operation_id, fault);
    assert_eq!(
        app.services
            .store
            .get_persisted_session(&fixture.session.id)
            .unwrap()
            .unwrap(),
        fixture.session
    );
    let before = database_snapshot(&db);

    fixture.provider.set_restore_failure_mode(0);
    if !matches!(fault, StartupFault::LostAcknowledgement) {
        db.execute_batch("DROP TRIGGER startup_http_receipt_fault")
            .unwrap();
    }

    // First retry the same manager, then construct two fresh managers. Clearing
    // the transient fault and changing startup instance IDs grant no new attempt.
    let mut first_error = None;
    for restart in 0..3 {
        if restart > 0 {
            (router, runtime, app) = reopen_test_router_with_services(
                &app,
                fixture.temp.path(),
                &fixture.token,
                TeamMcpPolicy::default(),
            )
            .await;
        }
        let summary = runtime.recover_startup().await.unwrap();
        assert_withheld(&summary, &operation_id);
        let denied_inspection = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v2/operations/{operation_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied_inspection.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            inspect(&router, &fixture.token, &operation_id).await,
            original
        );
        let uri = format!("/v2/agents/{}/turns", fixture.session.id);
        let key = format!("startup-blocked-turn-{restart}");
        for _ in 0..2 {
            let (status, refusal) = call(
                &router,
                &fixture.token,
                "POST",
                &uri,
                Some(serde_json::json!({
                    "input": [{"type": "text", "text": "must await original attachment evidence"}]
                })),
                Some(&key),
            )
            .await;
            assert_refusal(status, &refusal, &operation_id);
            assert!(refusal["error"]["details"]
                .get("dispatch_outcome")
                .is_none());
            // Each envelope has a fresh request ID; the stable error is exact.
            if let Some(first) = &first_error {
                assert_eq!(&refusal["error"], first);
            } else {
                first_error = Some(refusal["error"].clone());
            }
        }
        assert_eq!(startup_ids(&db, &fixture.session.id), ids);
        assert_eq!(
            inspect(&router, &fixture.token, &operation_id).await,
            original
        );
        assert_eq!(
            app.services
                .store
                .get_persisted_session(&fixture.session.id)
                .unwrap()
                .unwrap(),
            fixture.session
        );
        assert_eq!(
            runtime.get_session(&fixture.session.id).await.unwrap(),
            fixture.session
        );
        assert_eq!(fixture.provider.restore_call_count().await, resumes + 1);
        assert_eq!(
            fixture.provider.send_invocations.load(Ordering::SeqCst),
            sends
        );
        assert_eq!(fixture.provider.create_call_count(), creates);
        assert_eq!(database_snapshot(&db), before);
    }
}

#[tokio::test]
async fn startup_lost_ack_keeps_original_http_inspection_across_restarts() {
    exercise_restart_refusal(StartupFault::LostAcknowledgement).await;
}

#[tokio::test]
async fn startup_known_ack_receipt_fault_keeps_observed_http_evidence_across_restarts() {
    exercise_restart_refusal(StartupFault::CompletionReceipt).await;
}

#[tokio::test]
async fn startup_double_journal_fault_keeps_started_http_authority_across_restarts() {
    exercise_restart_refusal(StartupFault::CompletionAndHold).await;
}
