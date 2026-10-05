use super::*;
use runtime_core::ProviderDispatchOutcome;

fn turn_request(session: &str, turn: &str, text: &str) -> ProviderSendTurnRequest {
    ProviderSendTurnRequest {
        runtime_session_id: session.to_string(),
        turn_id: turn.to_string(),
        input: vec![json!({"type":"text","text":text})],
        expected_turn_id: None,
        permission_mode: None,
        approval_id: None,
    }
}

#[tokio::test]
async fn ambiguous_turn_start_stays_locked_against_replay() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider_with_timeout(&temp, false, 2_000);
    provider
        .create_session_with_policy(legacy_create("sess_ambiguous"))
        .await
        .expect("create session");

    let error = provider
        .send_turn(turn_request(
            "sess_ambiguous",
            "logical-ambiguous",
            "ambiguous-start",
        ))
        .await
        .expect_err("turn/start timeout must remain ambiguous");
    assert_eq!(
        error.provider_dispatch_code(),
        Some("codex_request_timeout")
    );
    assert_eq!(
        error.provider_dispatch_outcome(),
        ProviderDispatchOutcome::Unknown
    );

    let replay = provider
        .send_turn(turn_request(
            "sess_ambiguous",
            "logical-replay",
            "must not replay",
        ))
        .await
        .expect_err("ambiguous turn must keep session locked");
    assert_eq!(replay.provider_dispatch_code(), Some("turn_in_progress"));
    assert_eq!(
        replay.provider_dispatch_outcome(),
        ProviderDispatchOutcome::NotDispatched
    );
}

#[tokio::test]
async fn exact_capacity_rejection_is_definitively_not_dispatched() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_capacity_rpc"))
        .await
        .expect("create session");

    let error = provider
        .send_turn(turn_request(
            "sess_capacity_rpc",
            "logical-capacity",
            "model-capacity",
        ))
        .await
        .expect_err("capacity rejection");
    assert_eq!(error.provider_dispatch_code(), Some("codex_model_capacity"));
    assert_eq!(
        error.provider_dispatch_outcome(),
        ProviderDispatchOutcome::NotDispatched
    );

    let ack = provider
        .send_turn(turn_request(
            "sess_capacity_rpc",
            "logical-after-capacity",
            "normal after capacity",
        ))
        .await
        .expect("definitive rejection releases active slot");
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_capacity_rpc".to_string(),
            turn_id: ack.turn_id,
            timeout_ms: Some(2_000),
        })
        .await
        .expect("wait replacement turn");
    assert_eq!(result.status, ProviderTurnStatus::Completed);
}

#[tokio::test]
async fn malformed_notification_surfaces_unknown_turn_outcome() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_malformed"))
        .await
        .expect("create session");
    let mut events = provider.subscribe_events().expect("provider events");

    provider
        .send_turn(turn_request(
            "sess_malformed",
            "logical-malformed",
            "malformed-notification",
        ))
        .await
        .expect("turn/start ack");

    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("unknown event timeout")
        .expect("unknown event channel");
    match event {
        ProviderRuntimeEvent::TurnOutcomeUnknown {
            runtime_session_id,
            turn_id,
            code,
            message,
        } => {
            assert_eq!(runtime_session_id, "sess_malformed");
            assert_eq!(turn_id, "logical-malformed");
            assert_eq!(code, "codex_transport_outcome_unknown");
            assert!(message.contains("failed to parse"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn server_exit_surfaces_unknown_turn_outcome() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_server_close"))
        .await
        .expect("create session");
    let mut events = provider.subscribe_events().expect("provider events");

    provider
        .send_turn(turn_request(
            "sess_server_close",
            "logical-server-close",
            "server-close",
        ))
        .await
        .expect("turn/start ack");

    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("unknown event timeout")
        .expect("unknown event channel");
    match event {
        ProviderRuntimeEvent::TurnOutcomeUnknown {
            runtime_session_id,
            turn_id,
            code,
            ..
        } => {
            assert_eq!(runtime_session_id, "sess_server_close");
            assert_eq!(turn_id, "logical-server-close");
            assert_eq!(code, "codex_transport_outcome_unknown");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn dispatched_turn_wait_timeout_is_recovery_unknown_not_terminal_failure() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_wait_timeout"))
        .await
        .expect("create session");
    provider
        .send_turn(turn_request(
            "sess_wait_timeout",
            "logical-wait-timeout",
            "interrupt this turn",
        ))
        .await
        .expect("start long-running fake turn");

    let error = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_wait_timeout".to_string(),
            turn_id: "logical-wait-timeout".to_string(),
            timeout_ms: Some(25),
        })
        .await
        .expect_err("waiting on an admitted running turn must time out ambiguously");
    assert_eq!(
        error.provider_dispatch_code(),
        Some("codex_turn_wait_timeout")
    );
    assert_eq!(
        error.provider_dispatch_outcome(),
        ProviderDispatchOutcome::Unknown
    );
}

#[tokio::test]
async fn duplicate_terminal_notifications_converge_idempotently() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_duplicate"))
        .await
        .expect("create session");
    let ack = provider
        .send_turn(turn_request(
            "sess_duplicate",
            "logical-duplicate",
            "duplicate-terminal",
        ))
        .await
        .expect("send duplicate turn");

    let first = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_duplicate".to_string(),
            turn_id: ack.turn_id.clone(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("first wait");
    let second = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_duplicate".to_string(),
            turn_id: ack.turn_id,
            timeout_ms: Some(2_000),
        })
        .await
        .expect("second wait");
    assert_eq!(first.status, ProviderTurnStatus::Completed);
    assert_eq!(second.status, ProviderTurnStatus::Completed);
}

#[tokio::test]
async fn restart_resume_restores_mapping_and_reconciles_terminal_turn_from_thread_read() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    let created = provider
        .create_session_with_policy(legacy_create("sess_restart"))
        .await
        .expect("create session");
    let ack = provider
        .send_turn(turn_request(
            "sess_restart",
            "logical-restart",
            "read-reconcile",
        ))
        .await
        .expect("send turn");
    let native_turn_id = ack
        .provider_native_turn_id
        .clone()
        .expect("provider native turn id");
    let persisted: Value = serde_json::from_str(
        &std::fs::read_to_string(temp.path().join("app-server-state.json"))
            .expect("persisted fake app-server state"),
    )
    .expect("parse persisted fake app-server state");
    assert_eq!(
        persisted["threads"]["thread-native-1"]["turns"][0]["id"],
        native_turn_id
    );
    assert_eq!(
        persisted["threads"]["thread-native-1"]["turns"][0]["status"],
        "completed"
    );

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: "sess_restart".to_string(),
            reason: Some("simulate runtime restart".to_string()),
        })
        .await
        .expect("close old provider session");

    let (restarted, _) = fake_provider(&temp, false);
    restarted
        .resume_session_with_policy(
            ProviderResumeSessionPolicyRequest::legacy_compatible(
                "sess_restart".to_string(),
                created.provider_session_ref.clone(),
                created.canonical_provider_session_ref.clone(),
                Some("gpt-6-luna".to_string()),
                Some("/workspace/fake".to_string()),
                Some("workspace_write".to_string()),
                None,
                None,
            )
            .expect("resume policy"),
        )
        .await
        .expect("resume session");
    restarted
        .restore_turn_identity_mapping("sess_restart", "logical-restart", native_turn_id.as_str())
        .await
        .expect("restore logical/native mapping");

    let observation = {
        let sessions = restarted.inner.sessions.read().await;
        let session = &sessions["sess_restart"];
        session
            .transport
            .request(
                "thread/read",
                json!({"threadId": session.provider_session_ref, "includeTurns": true}),
            )
            .await
            .expect("direct restart thread/read")
    };
    assert_eq!(observation["thread"]["turns"][0]["id"], native_turn_id);
    assert_eq!(observation["thread"]["turns"][0]["status"], "completed");

    let result = restarted
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_restart".to_string(),
            turn_id: "logical-restart".to_string(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("thread/read reconciles terminal turn");
    assert_eq!(result.status, ProviderTurnStatus::Completed);
}

#[tokio::test]
async fn workspace_rebind_requires_evidence_and_rolls_back_or_marks_recovery() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, log_path) = fake_provider(&temp, false);
    let mut create = legacy_create("sess_rebind");
    create.launch_policy.system_prompt = Some("preserve this system instruction".to_string());
    provider
        .create_session_with_policy(create)
        .await
        .expect("create session");

    let evidence = provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "sess_rebind".to_string(),
            cwd: "/tmp/rebind-success".to_string(),
        })
        .await
        .expect("verified rebind");
    assert_eq!(evidence.cwd, "/tmp/rebind-success");

    let messages = read_log(&log_path);
    let unsubscribe_index = messages
        .iter()
        .position(|message| message["method"] == "thread/unsubscribe")
        .expect("unsubscribe request");
    let destination_index = messages
        .iter()
        .position(|message| {
            message["method"] == "thread/resume"
                && message["params"]["cwd"] == "/tmp/rebind-success"
        })
        .expect("destination resume");
    assert!(unsubscribe_index < destination_index);
    let instructions = messages[destination_index]["params"]["developerInstructions"]
        .as_str()
        .expect("developer instructions preserved");
    assert!(instructions.contains("preserve this system instruction"));
    assert!(instructions.contains(
        provider_harness_text(ProviderKind::Codex)
            .expect("harness")
            .expect("Codex harness")
            .as_str()
    ));

    let rejected = provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "sess_rebind".to_string(),
            cwd: "/tmp/reject-destination".to_string(),
        })
        .await
        .expect_err("destination rejection should roll back");
    assert!(matches!(rejected, RuntimeError::ProtocolViolation(_)));
    let sessions = provider.inner.sessions.read().await;
    assert_eq!(
        sessions["sess_rebind"].cwd.as_deref(),
        Some("/tmp/rebind-success")
    );
    drop(sessions);

    let unrecoverable = provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "sess_rebind".to_string(),
            cwd: "/tmp/unrecoverable-destination".to_string(),
        })
        .await
        .expect_err("unverifiable rollback must require recovery");
    assert_eq!(
        unrecoverable.provider_dispatch_code(),
        Some("reassignment_recovery_required")
    );
    assert_eq!(
        unrecoverable.provider_dispatch_outcome(),
        ProviderDispatchOutcome::Unknown
    );
}

#[tokio::test]
async fn busy_workspace_rebind_is_rejected_before_provider_mutation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, log_path) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_busy_rebind"))
        .await
        .expect("create session");
    provider
        .send_turn(turn_request(
            "sess_busy_rebind",
            "logical-busy",
            "interrupt this turn",
        ))
        .await
        .expect("start long-running fake turn");

    let error = provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "sess_busy_rebind".to_string(),
            cwd: "/tmp/must-not-bind".to_string(),
        })
        .await
        .expect_err("busy rebind rejected");
    assert!(matches!(error, RuntimeError::Conflict(_)));
    assert!(!read_log(&log_path).iter().any(|message| {
        message["method"] == "thread/unsubscribe"
            || (message["method"] == "thread/resume"
                && message["params"]["cwd"] == "/tmp/must-not-bind")
    }));
}

#[tokio::test]
async fn per_session_codex_homes_isolate_temporary_mcp_caller_identity() {
    let temp = tempfile::tempdir().expect("temp dir");
    let base_home = temp.path().join("codex-home");
    std::fs::create_dir_all(&base_home).expect("base codex home");
    std::fs::write(
        base_home.join("config.toml"),
        r#"model = "base-model"

[mcp_servers.other]
command = "/bin/other"

[mcp_servers.gg]
command = "/bin/stale-gg"

[mcp_servers.gg.env]
GG_MCP_CALLER_AGENT_ID = "stale"
"#,
    )
    .expect("base config");
    let (provider, _) = fake_provider(&temp, true);
    provider
        .create_session_with_policy(legacy_create("sess_one"))
        .await
        .expect("first session");
    provider
        .create_session_with_policy(legacy_create("sess_two"))
        .await
        .expect("second session");

    let one = std::fs::read_to_string(
        temp.path()
            .join("codex-home/runtime-sessions/sess_one/config.toml"),
    )
    .expect("session one config");
    let two = std::fs::read_to_string(
        temp.path()
            .join("codex-home/runtime-sessions/sess_two/config.toml"),
    )
    .expect("session two config");
    assert!(one.contains("GG_MCP_CALLER_AGENT_ID = \"sess_one\""));
    assert!(!one.contains("GG_MCP_CALLER_AGENT_ID = \"sess_two\""));
    assert!(one.contains("model = \"base-model\""));
    assert!(one.contains("[mcp_servers.other]"));
    assert!(one.contains("command = \"/bin/other\""));
    assert!(!one.contains("/bin/stale-gg"));
    assert!(!one.contains("GG_MCP_CALLER_AGENT_ID = \"stale\""));
    assert!(two.contains("GG_MCP_CALLER_AGENT_ID = \"sess_two\""));
    assert!(!two.contains("GG_MCP_CALLER_AGENT_ID = \"sess_one\""));
    assert!(two.contains("model = \"base-model\""));
    assert!(two.contains("[mcp_servers.other]"));
}
