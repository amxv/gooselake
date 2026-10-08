use super::*;

fn real_codex_provider(temp: &tempfile::TempDir) -> CodexProvider {
    let home = std::env::var("HOME").expect("HOME must be set for real Codex smoke");
    let source_auth = PathBuf::from(home)
        .join(".gg")
        .join("codex")
        .join("auth.json");
    assert!(
        source_auth.exists(),
        "expected real Codex auth at {}",
        source_auth.display()
    );
    let provider_home = temp.path().join("codex-home");
    copy_codex_auth_file(source_auth.as_path(), provider_home.as_path())
        .expect("stage real Codex auth into disposable home");
    CodexProvider::new(CodexProviderConfig {
        enabled: true,
        home_dir: provider_home,
        command: std::env::var("GG_CODEX_COMMAND").unwrap_or_else(|_| "codex".to_string()),
        app_server_args: vec!["app-server".to_string()],
        request_timeout_ms: 60_000,
        max_transports: 1,
        max_sessions_per_transport: 2,
        gg_mcp: CodexGgMcpConfig {
            enabled: false,
            ..CodexGgMcpConfig::default()
        },
    })
}

fn live_model() -> String {
    std::env::var("GG_CODEX_SMOKE_MODEL").unwrap_or_else(|_| "gpt-6-astra".to_string())
}

fn live_create(
    runtime_session_id: &str,
    cwd: &std::path::Path,
) -> ProviderCreateSessionPolicyRequest {
    let mut request = ProviderCreateSessionPolicyRequest::legacy_compatible(
        runtime_session_id.to_string(),
        Some(live_model()),
        Some(cwd.display().to_string()),
        Some("workspace_write".to_string()),
        None,
    )
    .expect("live create policy");
    request.current_preferences = ProviderSessionPreferences {
        thinking_effort: Some(ProviderThinkingEffort::High),
    };
    request
}

async fn send_and_wait(
    provider: &CodexProvider,
    runtime_session_id: &str,
    turn_id: &str,
    input: Vec<Value>,
    permission_mode: Option<&str>,
) -> runtime_core::ProviderTurnResult {
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: turn_id.to_string(),
            input,
            expected_turn_id: None,
            permission_mode: permission_mode.map(str::to_string),
            approval_id: None,
        })
        .await
        .expect("live Codex turn dispatch");
    provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: turn_id.to_string(),
            timeout_ms: Some(120_000),
        })
        .await
        .expect("live Codex turn completion")
}

#[tokio::test]
#[ignore = "real Codex live acceptance matrix: requires local ~/.gg/codex/auth.json"]
async fn live_native_inputs_context_compact_rebind_and_hard_fork() {
    let temp = tempfile::tempdir().expect("temp dir");
    let provider = real_codex_provider(&temp);
    let source_cwd = std::env::current_dir().expect("current repo cwd");
    let rebound_cwd = temp.path().join("rebound-workspace");
    std::fs::create_dir_all(&rebound_cwd).expect("create rebound cwd");

    let skills = provider
        .list_skills(ProviderSkillDiscoveryRequest {
            cwd: Some(source_cwd.display().to_string()),
            force_refresh: true,
            ..ProviderSkillDiscoveryRequest::default()
        })
        .await
        .expect("real Codex skills/list");
    let skill = skills
        .iter()
        .find(|skill| skill.path.is_some())
        .expect("expected at least one real Codex-discovered skill with a path");

    let image_path = temp.path().join("codex-live.png");
    // Valid 1x1 RGBA PNG. The test asserts native image acceptance/completion,
    // not any vision-quality claim.
    const ONE_PIXEL_PNG: &[u8] = &[
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f,
        0x15, 0xc4, 0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x08, 0xd7, 0x63, 0xf8,
        0xcf, 0xc0, 0xf0, 0x1f, 0x00, 0x05, 0x00, 0x01, 0xff, 0x89, 0x99, 0x3d, 0x1d, 0x00, 0x00,
        0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    ];
    std::fs::write(&image_path, ONE_PIXEL_PNG).expect("write live PNG");

    let runtime_session_id = "codex-live-controls";
    let created = provider
        .create_session_with_policy(live_create(runtime_session_id, source_cwd.as_path()))
        .await
        .expect("real Codex create session");
    let source_thread_id = created.provider_session_ref.clone();

    let first = send_and_wait(
        &provider,
        runtime_session_id,
        "codex-live-turn-1",
        vec![
            json!({
                "type": "text",
                "text": "Acknowledge the supplied image and skill reference, then reply with exactly: codex_native_ok"
            }),
            json!({"type": "local_image", "path": image_path.display().to_string()}),
            json!({
                "type": "skill",
                "name": skill.name,
                "path": skill.path.as_deref().expect("skill path")
            }),
        ],
        None,
    )
    .await;
    assert_eq!(first.status, ProviderTurnStatus::Completed);
    let first_usage = first.usage.expect("live first-turn usage");
    assert!(
        first_usage
            .get("last_message")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "real native-input turn must retain assistant text"
    );

    let context = provider
        .observe_context_limit(runtime_session_id)
        .await
        .expect("real context observation");
    assert!(context.model_context_window > 0);
    assert!(context.last_total_tokens > 0);

    let compacted = provider
        .compact_session(ProviderCompactSessionRequest {
            runtime_session_id: runtime_session_id.to_string(),
        })
        .await
        .expect("real manual compaction");
    assert_eq!(compacted, ProviderCompactSessionOutcome::Accepted);

    let rebound = provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: runtime_session_id.to_string(),
            cwd: rebound_cwd.display().to_string(),
        })
        .await
        .expect("real workspace rebind");
    assert_eq!(
        std::fs::canonicalize(&rebound.cwd).expect("canonical rebound evidence"),
        std::fs::canonicalize(&rebound_cwd).expect("canonical requested rebound cwd")
    );
    assert_eq!(
        rebound.provider_session_ref.as_deref(),
        Some(source_thread_id.as_str())
    );

    let forked = provider
        .hard_fork_edit_rerun(ProviderHardForkEditRerunRequest {
            runtime_session_id: runtime_session_id.to_string(),
            target_turn_id: "codex-live-turn-1".to_string(),
            edited_input: vec![json!({
                "type": "text",
                "text": "Reply with exactly: codex_edited_rerun_ok"
            })],
        })
        .await
        .expect("real hard fork rollback");
    assert_ne!(forked.provider_session_ref, source_thread_id);

    // The provider primitive performs the verified hard-fork/rollback; the
    // edited input is then admitted as a fresh logical turn on that child.
    let rerun = send_and_wait(
        &provider,
        runtime_session_id,
        "codex-live-edited-rerun",
        vec![json!({
            "type": "text",
            "text": "Reply with exactly: codex_edited_rerun_ok"
        })],
        None,
    )
    .await;
    assert_eq!(rerun.status, ProviderTurnStatus::Completed);
    assert!(
        rerun
            .usage
            .as_ref()
            .and_then(|usage| usage.get("last_message"))
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("codex_edited_rerun_ok")),
        "edited rerun must complete on the forked child thread"
    );

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: runtime_session_id.to_string(),
            reason: Some("codex_live_complete".to_string()),
        })
        .await
        .expect("close real Codex live session");
}

#[tokio::test]
#[ignore = "real Codex live approval/interrupt matrix: requires local ~/.gg/codex/auth.json"]
async fn live_provider_approval_and_interrupt() {
    let temp = tempfile::tempdir().expect("temp dir");
    let provider = real_codex_provider(&temp);
    let cwd = std::env::current_dir().expect("current repo cwd");
    let runtime_session_id = "codex-live-approval-interrupt";
    provider
        .create_session_with_policy(live_create(runtime_session_id, cwd.as_path()))
        .await
        .expect("real Codex create approval session");
    let mut events = provider.subscribe_events().expect("provider events");

    let approval_probe = PathBuf::from("/home/zodex-agent/codex-live-approval-probe.txt");
    let _ = std::fs::remove_file(&approval_probe);
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: "codex-live-approval".to_string(),
            input: vec![json!({
                "type": "text",
                "text": format!(
                    "You must use the shell to run exactly: printf codex_approval_ok > {} . Do not skip the command. After the command succeeds, reply exactly: codex_approval_ok",
                    approval_probe.display()
                )
            })],
            expected_turn_id: None,
            permission_mode: Some("require_approval".to_string()),
            approval_id: None,
        })
        .await
        .expect("dispatch live approval turn");

    let approval_event = tokio::time::timeout(Duration::from_secs(60), events.recv())
        .await
        .expect("timed out waiting for real Codex approval")
        .expect("provider event channel");
    let approval_ref = match approval_event {
        ProviderRuntimeEvent::ApprovalRequested {
            runtime_session_id: event_session_id,
            turn_id,
            provider_approval_ref,
            ..
        } => {
            assert_eq!(event_session_id, runtime_session_id);
            assert_eq!(turn_id, "codex-live-approval");
            provider_approval_ref
        }
        ProviderRuntimeEvent::TurnOutcomeUnknown { code, message, .. } => {
            panic!("unexpected unknown outcome before live approval: {code}: {message}")
        }
        ProviderRuntimeEvent::PermissionObserved { .. } => {
            panic!("unexpected permission observation before live approval")
        }
        ProviderRuntimeEvent::ContextCompactionObserved { .. } => {
            panic!("unexpected compaction observation before live approval")
        }
        ProviderRuntimeEvent::SessionIdentityObserved { .. } => {
            panic!("unexpected native session identity observation before live approval")
        }
    };
    provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: "codex-live-approval".to_string(),
            approval_id: approval_ref,
            decision: "accept".to_string(),
            payload: None,
        })
        .await
        .expect("accept real Codex provider approval");
    let approved = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: "codex-live-approval".to_string(),
            timeout_ms: Some(120_000),
        })
        .await
        .expect("wait for approved live turn");
    assert_eq!(approved.status, ProviderTurnStatus::Completed);
    assert_eq!(
        std::fs::read_to_string(&approval_probe).expect("approval probe file"),
        "codex_approval_ok"
    );
    std::fs::remove_file(&approval_probe).expect("remove approval probe file");

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: "codex-live-interrupt".to_string(),
            input: vec![json!({
                "type": "text",
                "text": "You must use the shell to run exactly: sleep 30 . Do not reply until the command finishes."
            })],
            expected_turn_id: None,
            permission_mode: Some("danger_full_access".to_string()),
            approval_id: None,
        })
        .await
        .expect("dispatch live interrupt turn");
    tokio::time::sleep(Duration::from_secs(2)).await;
    provider
        .interrupt_turn(ProviderInterruptTurnRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: "codex-live-interrupt".to_string(),
        })
        .await
        .expect("interrupt real Codex turn");
    let interrupted = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: "codex-live-interrupt".to_string(),
            timeout_ms: Some(60_000),
        })
        .await
        .expect("wait for interrupted live turn");
    assert_eq!(interrupted.status, ProviderTurnStatus::Interrupted);

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: runtime_session_id.to_string(),
            reason: Some("codex_live_approval_interrupt_complete".to_string()),
        })
        .await
        .expect("close real Codex approval/interrupt session");
}
