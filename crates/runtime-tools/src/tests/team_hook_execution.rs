use super::*;
use crate::team_hook_execution::{MAX_DURABLE_RECEIPT_BYTES, MAX_SCRIPT_STDOUT};
use runtime_core::{
    TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase, TeamHookEffectReceipt,
    TeamHookEventName, TeamHookResolvedEntry, TeamHookScope,
};

fn pre_add_request(root: &std::path::Path) -> runtime_core::WorkspaceAgentCreateRequest {
    runtime_core::WorkspaceAgentCreateRequest {
        provider: runtime_core::ProviderKind::Codex,
        model: Some("test-model".into()),
        permission_intent: runtime_core::ProviderPermissionIntent::ProviderDefault,
        setting_sources_intent: runtime_core::ProviderSettingSourcesIntent::Isolated,
        current_preferences: runtime_core::ProviderSessionPreferences::default(),
        system_prompt: None,
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        cwd: Some(root.to_string_lossy().into_owned()),
        worktree: None,
        harness_version_slot: None,
        title: Some("Member".into()),
        metadata: None,
        onboarding: Some(runtime_core::WorkspaceAgentOnboardingRequest {
            prompt: Some("Inspect tests".into()),
            ..Default::default()
        }),
    }
}

fn pre_add_fixture(
    command: String,
) -> (
    tempfile::TempDir,
    Arc<SqliteRuntimeStore>,
    runtime_core::WorkspaceRecord,
    Arc<RuntimeSessionManager>,
    Arc<super::WorktreeTestProvider>,
) {
    use runtime_core::{prepare_workspace_registration, OperationActor, WorkspaceRegisterRequest};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    }));
    store.repository().initialize_schema().unwrap();
    let workspace = store
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.to_string_lossy().into_owned(),
                    display_name: Some("Hook workspace".into()),
                },
                OperationActor::operator("pre-add-test"),
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .workspace;
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: workspace.workspace_id.clone(),
            expected_revision: 0,
            approved_at_ms: 1,
            hooks: vec![TeamHookResolvedEntry {
                event_name: TeamHookEventName::PreTeammateAdd,
                name: "prepare_member".into(),
                command,
                timeout_ms: 5_000,
                scope: TeamHookScope::Project,
            }],
        })
        .unwrap();
    let provider = Arc::new(super::WorktreeTestProvider::default());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let manager =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128).unwrap());
    (temp, store, workspace, manager, provider)
}

#[tokio::test]
async fn approved_pre_add_changes_only_eligible_route_and_commits_hook_onboarding() {
    // The hook's concrete working-directory path is rooted in the registered
    // workspace, so build the actual disposable fixture first and approve an
    // exact changed command under the same test store.
    let (_fixture, store, workspace, manager, provider) = pre_add_fixture("printf '{}'".into());
    let root = std::path::PathBuf::from(&workspace.canonical_root);
    let child = root.join("eligible");
    std::fs::create_dir(&child).unwrap();
    let output = root.join("output.json");
    std::fs::write(
        &output,
        json!({
            "pre_teammate_add": {
                "spawn_template_mutation":{"cwd":child},
                "onboarding_context_append_text":"Use local fixture context"
            },
            "inject":{"targets":[
                {"agent_id":"__new_member__","text":"Scoped new member notice"}
            ]}
        })
        .to_string(),
    )
    .unwrap();
    let marker = root.join("runs");
    let hook = TeamHookResolvedEntry {
        event_name: TeamHookEventName::PreTeammateAdd,
        name: "prepare_member".into(),
        command: format!(
            "printf x >> '{}'; cat '{}'",
            marker.display(),
            output.display()
        ),
        timeout_ms: 5_000,
        scope: TeamHookScope::Project,
    };
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: workspace.workspace_id.clone(),
            expected_revision: 1,
            hooks: vec![hook],
            approved_at_ms: 2,
        })
        .unwrap();
    manager
        .install_pre_add_hook_executor(Arc::new(NativeTeamHookExecutor::new(store.clone())))
        .await;

    let agent = manager
        .create_workspace_agent(
            &workspace.workspace_id,
            pre_add_request(&root),
            "runtime_operator",
        )
        .await
        .unwrap();
    assert_eq!(
        agent.recreation_policy.authoritative_cwd,
        child.to_string_lossy()
    );
    let notice = store
        .get_workspace_system_notice(&format!("workspace-onboarding:{}", agent.agent_id))
        .unwrap()
        .unwrap();
    assert_eq!(notice.state, "pending");
    let intro = notice.input[0]["text"].as_str().unwrap();
    assert!(intro.contains("Inspect tests"));
    assert!(intro.contains("Context from operator-approved workspace setup hooks"));
    assert!(intro.contains("Use local fixture context"));
    assert!(intro.contains("Scoped new member notice"));
    let effects = store
        .list_team_hook_effects(&workspace.workspace_id, 50)
        .unwrap();
    let pre = effects
        .iter()
        .find(|row| row.hook.event_name == TeamHookEventName::PreTeammateAdd)
        .unwrap();
    assert_eq!(pre.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(pre.application_state, "applied");
    assert_eq!(pre.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: store.database_path().to_path_buf(),
    });
    assert!(reopened
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened
            .get_workspace_system_notice(&notice.notice_id)
            .unwrap()
            .unwrap()
            .input,
        notice.input
    );
    assert_eq!(provider.state.lock().await.sessions.len(), 1);
}

#[tokio::test]
async fn pre_add_rejects_outside_root_without_dispatching_provider() {
    let outside = tempfile::tempdir().unwrap();
    let output = outside.path().join("output.json");
    std::fs::write(
        &output,
        json!({
            "pre_teammate_add": {
                "spawn_template_mutation":{"cwd":outside.path()}
            }
        })
        .to_string(),
    )
    .unwrap();
    let (temp, store, workspace, manager, provider) =
        pre_add_fixture(format!("cat '{}'", output.display()));
    manager
        .install_pre_add_hook_executor(Arc::new(NativeTeamHookExecutor::new(store.clone())))
        .await;
    let result = manager
        .create_workspace_agent(
            &workspace.workspace_id,
            pre_add_request(&temp.path().join("workspace")),
            "runtime_operator",
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::InvalidState(_))));
    assert!(provider.state.lock().await.sessions.is_empty());
    assert!(store
        .list_workspace_agents(&workspace.workspace_id, None)
        .unwrap()
        .is_empty());
    let effect = store
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap()
        .remove(0);
    assert_eq!(effect.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(effect.application_state, "pending");
}

#[tokio::test]
async fn pre_add_cannot_inject_into_unrelated_agents_or_impersonate_caller() {
    let outside = tempfile::tempdir().unwrap();
    let output = outside.path().join("output.json");
    std::fs::write(
        &output,
        json!({
            "inject":{"caller":{"text":"not an operator"},
                "targets":[{"agent_id":"sess_unrelated","text":"forged"}]}
        })
        .to_string(),
    )
    .unwrap();
    let (temp, store, workspace, manager, provider) =
        pre_add_fixture(format!("cat '{}'", output.display()));
    manager
        .install_pre_add_hook_executor(Arc::new(NativeTeamHookExecutor::new(store.clone())))
        .await;
    let result = manager
        .create_workspace_agent(
            &workspace.workspace_id,
            pre_add_request(&temp.path().join("workspace")),
            "runtime_operator",
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::Conflict(_))));
    assert!(provider.state.lock().await.sessions.is_empty());
    assert!(store
        .list_workspace_agents(&workspace.workspace_id, None)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn pre_add_cannot_rebind_explicit_root_route_to_another_directory() {
    let outside = tempfile::tempdir().unwrap();
    let output = outside.path().join("output.json");
    let (_fixture, store, workspace, manager, provider) =
        pre_add_fixture(format!("cat '{}'", output.display()));
    let root = std::path::PathBuf::from(&workspace.canonical_root);
    let child = root.join("explicit-route-denied");
    std::fs::create_dir(&child).unwrap();
    std::fs::write(
        &output,
        json!({
            "pre_teammate_add":{"spawn_template_mutation":{"cwd":child}}
        })
        .to_string(),
    )
    .unwrap();
    manager
        .install_pre_add_hook_executor(Arc::new(NativeTeamHookExecutor::new(store.clone())))
        .await;
    let mut request = pre_add_request(&root);
    request.cwd = None;
    request.worktree = Some(runtime_core::WorkspaceAgentInitialRoute::Root);
    let result = manager
        .create_workspace_agent(&workspace.workspace_id, request, "runtime_operator")
        .await;
    assert!(matches!(result, Err(RuntimeError::Conflict(_))));
    assert!(provider.state.lock().await.sessions.is_empty());
}

#[tokio::test]
async fn conflicting_pre_add_scripts_fail_closed_without_creating_a_provider_or_member() {
    let (_fixture, store, workspace, manager, provider) = pre_add_fixture("printf '{}'".into());
    let root = std::path::PathBuf::from(&workspace.canonical_root);
    let first = root.join("first-route");
    let second = root.join("second-route");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    let mut hooks = Vec::new();
    let mut markers = Vec::new();
    for (name, cwd) in [("alpha", &first), ("beta", &second)] {
        let response = root.join(format!("{name}.json"));
        let marker = root.join(format!("{name}.marker"));
        std::fs::write(
            &response,
            json!({"pre_teammate_add":{"spawn_template_mutation":{"cwd":cwd}}}).to_string(),
        )
        .unwrap();
        hooks.push(TeamHookResolvedEntry {
            event_name: TeamHookEventName::PreTeammateAdd,
            name: name.into(),
            command: format!(
                "printf x >> '{}'; cat '{}'",
                marker.display(),
                response.display()
            ),
            timeout_ms: 5_000,
            scope: TeamHookScope::Project,
        });
        markers.push(marker);
    }
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: workspace.workspace_id.clone(),
            expected_revision: 1,
            hooks,
            approved_at_ms: 2,
        })
        .unwrap();
    manager
        .install_pre_add_hook_executor(Arc::new(NativeTeamHookExecutor::new(store.clone())))
        .await;

    let result = manager
        .create_workspace_agent(
            &workspace.workspace_id,
            pre_add_request(&root),
            "runtime_operator",
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::Conflict(_))));
    assert!(provider.state.lock().await.sessions.is_empty());
    assert!(store
        .list_workspace_agents(&workspace.workspace_id, None)
        .unwrap()
        .is_empty());
    let effects = store
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap();
    assert_eq!(effects.len(), 2);
    assert!(effects.iter().all(|effect| {
        effect.phase == TeamHookEffectPhase::Succeeded
            && effect.application_state == "pending"
            && effect.attempt_count == 1
    }));
    for marker in markers {
        assert_eq!(std::fs::read_to_string(marker).unwrap(), "x");
    }
    // The script's external effects happened; restart classifies their
    // unattached outputs as rejected/unknown without creating the member.
    let restarted = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: store.database_path().to_path_buf(),
    });
    assert_eq!(
        restarted.quarantine_started_team_hook_effects(100).unwrap(),
        0
    );
    assert!(restarted
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap()
        .iter()
        .all(|effect| effect.application_state == "rejected"
            && effect.application_error_code.as_deref()
                == Some("pre_add_provider_outcome_unknown")));
}

struct RevokingPreAddExecutor {
    store: Arc<SqliteRuntimeStore>,
}

#[async_trait::async_trait]
impl runtime_core::RuntimeTeamHookExecutor for RevokingPreAddExecutor {
    async fn execute(
        &self,
        admission: &TeamHookEffectAdmission,
    ) -> Result<runtime_core::TeamHookEffectRecord, RuntimeError> {
        let finished = execute_team_hook_effect(self.store.as_ref(), admission).await?;
        // Simulate operator revocation after the native script succeeded but
        // before the runtime can finish creating the provider and agent.
        self.store
            .update_team_hook_approval(&TeamHookApprovalUpdate {
                workspace_id: admission.workspace_id.clone(),
                expected_revision: 1,
                hooks: Vec::new(),
                approved_at_ms: 99,
            })?;
        Ok(finished)
    }
}

#[tokio::test]
async fn pre_add_revocation_during_provider_creation_cannot_commit_agent_or_context() {
    let (_fixture, store, workspace, manager, provider) = pre_add_fixture("printf '{}'".into());
    manager
        .install_pre_add_hook_executor(Arc::new(RevokingPreAddExecutor {
            store: Arc::clone(&store),
        }))
        .await;

    let result = manager
        .create_workspace_agent(
            &workspace.workspace_id,
            pre_add_request(std::path::Path::new(&workspace.canonical_root)),
            "runtime_operator",
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::Conflict(_))));
    assert!(store
        .list_workspace_agents(&workspace.workspace_id, None)
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .get_team_hook_approval(&workspace.workspace_id)
            .unwrap()
            .revision,
        2
    );
    assert!(store
        .get_team_hook_approval(&workspace.workspace_id)
        .unwrap()
        .hooks
        .is_empty());
    assert!(provider.state.lock().await.sessions.is_empty());
    let effects = store
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(effects[0].application_state, "pending");
}

fn fixture(
    command: String,
    timeout_ms: u64,
) -> (
    tempfile::TempDir,
    SqliteRuntimeStore,
    TeamHookEffectAdmission,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    store.repository().initialize_schema().unwrap();
    let conn = rusqlite::Connection::open(store.database_path()).unwrap();
    conn.execute(
        "INSERT INTO workspaces(workspace_id,canonical_root,display_name,lifecycle_state,revision,created_at,updated_at)
         VALUES('ws_hooks',?1,'Hook test','active',0,1,1)",
        [root.to_string_lossy().as_ref()],
    )
    .unwrap();
    conn.execute_batch(
        "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES('evt_hooks','system','system',1,'workspace.member_added',1,'{\"workspace_id\":\"ws_hooks\"}',1)",
    )
    .unwrap();
    let hook = TeamHookResolvedEntry {
        event_name: TeamHookEventName::PostTeammateAdd,
        name: "runtime_check".into(),
        command,
        timeout_ms,
        scope: TeamHookScope::User,
    };
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: "ws_hooks".into(),
            expected_revision: 0,
            hooks: vec![hook.clone()],
            approved_at_ms: 1,
        })
        .unwrap();
    let admission = TeamHookEffectAdmission {
        workspace_id: "ws_hooks".into(),
        parent_event_id: "evt_hooks".into(),
        hook,
        input: json!({"schema_version":1,"event_id":"evt_hooks","hook_event":"post_teammate_add"}),
        created_at_ms: 2,
    };
    (temp, store, admission)
}

#[tokio::test]
async fn persisted_workspace_parent_dispatches_once_and_reconciles_after_restart() {
    let marker_dir = tempfile::tempdir().unwrap();
    let marker = marker_dir.path().join("script-runs");
    let (temp, store, original) = fixture(
        format!("printf x >> '{}'; printf '{{}}'", marker.display()),
        5_000,
    );
    let mut admitted = original;
    admitted.parent_event_id = "evt_workspace_hook_dispatch".into();
    admitted.input = json!({
        "schema_version":1,
        "workspace_id":"ws_hooks",
        "event_id":admitted.parent_event_id,
        "hook_event":"post_teammate_add",
        "actor_agent_id":null,
        "subject":{"kind":"member","member_agent_id":"sess_test"},
        "eligible_target_agent_ids":[],
    });
    let sql = rusqlite::Connection::open(store.database_path()).unwrap();
    sql.execute(
        "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES(?1,'workspace','ws_hooks',1,'workspace.hook_parent',0,?2,1)",
        rusqlite::params![admitted.parent_event_id, admitted.input.to_string()],
    ).unwrap();
    store.admit_team_hook_effect(&admitted).unwrap();

    assert_eq!(dispatch_team_hook_effects(&store).await.unwrap(), 2);
    let complete = store
        .get_team_hook_effect(
            "ws_hooks",
            &admitted.parent_event_id,
            admitted.hook.event_name,
            &admitted.hook.name,
        )
        .unwrap()
        .unwrap();
    assert_eq!(complete.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(complete.application_state, "applied");
    assert_eq!(complete.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");

    let restarted = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    restarted.repository().initialize_schema().unwrap();
    assert_eq!(dispatch_team_hook_effects(&restarted).await.unwrap(), 0);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
}

#[tokio::test]
async fn operator_granted_hook_runs_once_with_durable_terminal_receipt() {
    let marker = tempfile::tempdir().unwrap();
    let marker_path = marker.path().join("executions");
    let (temp, store, admission) = fixture(
        format!("printf x >> '{}'; printf '{{}}'", marker_path.display()),
        5_000,
    );
    let first = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(first.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(first.attempt_count, 1);
    assert_eq!(first.receipt.unwrap().validated_output, Some(json!({})));
    let replay = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(replay.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(replay.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(marker_path).unwrap(), "x");
    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    let durable = execute_team_hook_effect(&reopened, &admission)
        .await
        .unwrap();
    assert_eq!(durable.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(durable.attempt_count, 1);
}

#[tokio::test]
async fn revoked_or_mismatched_hook_cannot_launch_shell() {
    let marker = tempfile::tempdir().unwrap();
    let marker_path = marker.path().join("must_not_exist");
    let (_temp, store, admission) = fixture(
        format!("printf x > '{}'; printf '{{}}'", marker_path.display()),
        5_000,
    );
    let mut spoof = admission.clone();
    spoof.hook.command = "printf '{}'".into();
    assert!(matches!(
        execute_team_hook_effect(&store, &spoof).await,
        Err(RuntimeError::Conflict(_))
    ));
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: "ws_hooks".into(),
            expected_revision: 1,
            hooks: vec![],
            approved_at_ms: 3,
        })
        .unwrap();
    assert!(matches!(
        execute_team_hook_effect(&store, &admission).await,
        Err(RuntimeError::Conflict(_))
    ));
    assert!(!marker_path.exists());
}

#[tokio::test]
async fn timeouts_and_invalid_output_never_leak_script_stdout() {
    let (_temp, store, admission) = fixture("sleep 2; printf '{}'".into(), 100);
    let timed = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(timed.phase, TeamHookEffectPhase::TimedOut);
    assert_eq!(
        timed.receipt.unwrap().error_code.as_deref(),
        Some("timeout")
    );

    // Keep the secret bytes outside the approved command snapshot. A grant is
    // intentionally auditable (including its command), but neither a failed
    // receipt nor the effect history may retain the *script's output*.
    let secret_dir = tempfile::tempdir().unwrap();
    let secret_file = secret_dir.path().join("invalid-output.json");
    std::fs::write(
        &secret_file,
        br#"{"caller_agent_id":"forged","secret":"not_stored"}"#,
    )
    .unwrap();
    let (_temp, store, admission) = fixture(format!("cat '{}'", secret_file.display()), 5_000);
    let invalid = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(invalid.phase, TeamHookEffectPhase::Failed);
    assert_eq!(
        invalid.receipt.unwrap().error_code.as_deref(),
        Some("invalid_output")
    );
    let durable = store.list_team_hook_effects("ws_hooks", 10).unwrap();
    let encoded = serde_json::to_string(&durable).unwrap();
    assert!(!encoded.contains("not_stored"));
    assert!(!encoded.contains("forged"));
}

#[tokio::test]
async fn near_limit_valid_stdout_cannot_leave_an_unpersistable_started_receipt() {
    let secret_dir = tempfile::tempdir().unwrap();
    let output_file = secret_dir.path().join("bounded-output.json");
    let mut near_limit = None;
    for second_len in (7_900..=8_192).rev() {
        let payload = serde_json::to_vec(&json!({
            "inject": {
                "caller": {"text": "x".repeat(8_192)},
                "targets": [{"agent_id": "sess_other", "text": "y".repeat(second_len)}]
            }
        }))
        .unwrap();
        if payload.len() > MAX_SCRIPT_STDOUT {
            continue;
        }
        let validated = runtime_core::team_hooks::validate_team_hook_script_output(
            TeamHookEventName::PostTeammateAdd,
            &payload,
        )
        .unwrap();
        let receipt = TeamHookEffectReceipt {
            validated_output: Some(serde_json::to_value(validated).unwrap()),
            error_code: None,
        };
        if serde_json::to_vec(&receipt).unwrap().len() > MAX_DURABLE_RECEIPT_BYTES {
            near_limit = Some(payload);
            break;
        }
    }
    let near_limit = near_limit.expect("valid stdout whose durable receipt exceeds 16 KiB");
    std::fs::write(&output_file, near_limit).unwrap();
    let (temp, store, admission) = fixture(format!("cat '{}'", output_file.display()), 5_000);
    let effect = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(effect.phase, TeamHookEffectPhase::Failed);
    assert_eq!(
        effect.receipt.unwrap().error_code.as_deref(),
        Some("output_too_large")
    );

    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    let recovered = execute_team_hook_effect(&reopened, &admission)
        .await
        .unwrap();
    assert_eq!(recovered.phase, TeamHookEffectPhase::Failed);
    assert_eq!(recovered.attempt_count, 1);
    assert_eq!(
        reopened
            .repository()
            .quarantine_started_team_hook_effects(9)
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn competing_dispatchers_can_launch_at_most_one_hook() {
    let marker = tempfile::tempdir().unwrap();
    let marker_path = marker.path().join("executions");
    let (_temp, store, admission) = fixture(
        format!("printf x >> '{}'; printf '{{}}'", marker_path.display()),
        5_000,
    );
    let (a, b) = tokio::join!(
        execute_team_hook_effect(&store, &admission),
        execute_team_hook_effect(&store, &admission),
    );
    assert!(a.is_ok(), "first execution returned {a:?}");
    assert!(b.is_ok(), "second execution returned {b:?}");
    let latest = store
        .get_team_hook_effect(
            "ws_hooks",
            "evt_hooks",
            admission.hook.event_name,
            &admission.hook.name,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        latest.phase,
        TeamHookEffectPhase::Succeeded,
        "receipt: {:?}",
        latest.receipt
    );
    assert_eq!(latest.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(marker_path).unwrap(), "x");
}
