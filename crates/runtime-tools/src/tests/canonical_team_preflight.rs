use super::*;
use runtime_core::{
    normalized_json_hash, prepare_workspace_registration, AgentToolMutationAdmission,
    AgentToolMutationCommand, OperationActor, WorkspaceAgentCreateRequest,
    WorkspaceAgentInitialRoute, WorkspaceRegisterRequest,
};

async fn fixture() -> (RuntimeToolGateway, String, tempfile::TempDir) {
    fixture_with_policy(TeamMcpPolicy::default()).await
}

async fn fixture_with_policy(
    policy: TeamMcpPolicy,
) -> (RuntimeToolGateway, String, tempfile::TempDir) {
    let (gateway, runtime, _comms, temp) = build_team_gateway_fixture_with_policy(policy).await;
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let root = temp.path().join("synthetic-model-workspace");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = store
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.display().to_string(),
                    display_name: Some("Pure validation fixture".into()),
                },
                OperationActor::operator("synthetic-test-operator"),
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .workspace;
    let caller = runtime
        .create_workspace_agent(
            &workspace.workspace_id,
            WorkspaceAgentCreateRequest {
                provider: ProviderKind::Codex,
                model: Some("test-model".into()),
                permission_intent: Default::default(),
                setting_sources_intent: Default::default(),
                current_preferences: Default::default(),
                system_prompt: None,
                allowed_tools: vec![],
                disallowed_tools: vec![],
                cwd: None,
                worktree: Some(WorkspaceAgentInitialRoute::Root),
                harness_version_slot: None,
                title: Some("Synthetic model caller".into()),
                metadata: None,
                onboarding: None,
            },
            "runtime_operator",
        )
        .await
        .unwrap();
    (gateway, caller.agent_id, temp)
}

fn model_request(
    caller: &str,
    invocation_id: &str,
    args: Value,
) -> runtime_core::ToolInvokeRequest {
    runtime_core::ToolInvokeRequest {
        namespace: Some("gg_team".into()),
        tool_name: "gg_team".into(),
        caller_session_id: caller.into(),
        invocation_id: Some(invocation_id.into()),
        args,
    }
}

fn model_operation_count(temp: &tempfile::TempDir) -> i64 {
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.query_row(
        "SELECT COUNT(*) FROM runtime_operations WHERE kind='agent_model_tool_mutation'",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

#[tokio::test]
async fn configured_preset_removal_never_hides_existing_host_outcomes() {
    for completed in [false, true] {
        let (gateway, caller, temp) = fixture().await;
        let store = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp.path().join("runtime.sqlite3"),
        });
        let workspace_id = store
            .get_workspace_agent_by_id(&caller)
            .unwrap()
            .unwrap()
            .workspace_id;
        // Historical durable input can name a configured preset that was
        // removed before this gateway started. This synthetic receipt proves
        // replay authority, not successful native creation.
        let args = json!({"mode":"add","model_preset":"removed-configured-preset"});
        let normalized_request = json!({"tool_name":"gg_team","args":args});
        let command = AgentToolMutationCommand {
            operation_id: "synthetic-before-preset-removal".into(),
            workspace_id,
            caller_agent_id: caller.clone(),
            idempotency_key: "historical-preset-host-key".into(),
            tool_name: "gg_team".into(),
            normalized_request_hash: normalized_json_hash(&normalized_request).unwrap(),
            normalized_request,
            admitted_at: 100,
        };
        store.begin_agent_tool_mutation(&command).unwrap();
        let exact = json!({"ok":true,"result":{"operation":"add","agent_id":"historical-child"}});
        if completed {
            assert!(store
                .finish_agent_tool_mutation(&command.operation_id, &exact, 110)
                .is_err());
            // Seed a pre-journal historical receipt; the current writer must
            // reject a fabricated success without native-child evidence.
            rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap().execute(
                "UPDATE runtime_operations SET phase='completed',exact_terminal_result_json=?2 WHERE operation_id=?1",
                rusqlite::params![command.operation_id,exact.to_string()]).unwrap();
        }
        drop(store);
        let result = gateway
            .invoke_tool(model_request(
                &caller,
                &command.idempotency_key,
                args.clone(),
            ))
            .await
            .unwrap();
        if completed {
            assert_eq!(
                result, exact,
                "changed catalog hid the exact terminal receipt"
            );
        } else {
            assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
            assert_eq!(
                result["error"]["details"]["operation_id"],
                command.operation_id
            );
        }
        let changed = gateway
            .invoke_tool(model_request(
                &caller,
                &command.idempotency_key,
                json!({"mode":"add","model_preset":"removed-configured-preset","title":"changed"}),
            ))
            .await
            .unwrap();
        assert_eq!(changed["error"]["code"], "conflict", "{changed}");
        let new_request = gateway
            .invoke_tool(model_request(&caller, "new-host-key", args))
            .await
            .unwrap();
        assert_eq!(new_request["error"]["code"], "not_found", "{new_request}");
        assert_eq!(model_operation_count(&temp), 1);
    }
}

#[tokio::test]
async fn malformed_pure_team_modes_never_admit_an_irreversible_host_intent() {
    let (gateway, caller, temp) = fixture().await;
    let invalid = [
        (json!({"mode":"add","title":"  "}), "bad_request"),
        (json!({"mode":"add","prompt":"\t"}), "bad_request"),
        (json!({"mode":"add","worktree_name":" "}), "bad_request"),
        (json!({"mode":"add","image_paths":[""]}), "bad_request"),
        (
            json!({"mode":"add","image_paths":["a","b","c","d","e","f","g","h","i"]}),
            "bad_request",
        ),
        (
            json!({"mode":"add","creator_compaction_subscription":"always"}),
            "bad_request",
        ),
        (
            json!({"mode":"add","model_preset":"synthetic-unlisted-preset"}),
            "not_found",
        ),
        (
            json!({"mode":"remove","remove_agent_ids":[]}),
            "bad_request",
        ),
        (
            json!({"mode":"remove","remove_agent_ids":["a","a"]}),
            "bad_request",
        ),
        (
            json!({"mode":"remove","remove_agent_ids":["a"," a "]}),
            "bad_request",
        ),
        (
            json!({"mode":"remove","remove_agent_ids":[" "]}),
            "bad_request",
        ),
        (
            json!({"mode":"remove","remove_agent_ids":["a"],"preserve_worktrees":false}),
            "unsupported",
        ),
        (
            json!({"mode":"assign_worktree","assign_worktree":{"agent_id":" ","worktree_name":"my-feature"}}),
            "bad_request",
        ),
        (
            json!({"mode":"assign_worktree","assign_worktree":{"agent_id":"a","worktree_name":" "}}),
            "bad_request",
        ),
        (
            json!({"mode":"assign_worktree","assign_worktree":{"agent_id":"a","worktree_name":"feature","preserve_previous_worktree":false}}),
            "unsupported",
        ),
    ];
    for (idx, (args, expected)) in invalid.into_iter().enumerate() {
        let response = gateway
            .invoke_tool(model_request(&caller, &format!("pure-invalid-{idx}"), args))
            .await
            .unwrap();
        assert_eq!(response["ok"], false, "{response}");
        assert_eq!(response["error"]["code"], expected, "{response}");
        assert_eq!(
            model_operation_count(&temp),
            0,
            "pure invalid request {idx} admitted irreversible intent"
        );
    }
}

#[tokio::test]
async fn disabled_team_feature_does_not_admit_a_mutation_or_poison_host_key() {
    let (gateway, caller, temp) = fixture_with_policy(TeamMcpPolicy {
        enabled: false,
        non_lead_can_add_members: false,
        non_lead_can_remove_members: false,
    })
    .await;
    for _ in 0..2 {
        let response = gateway
            .invoke_tool(model_request(
                &caller,
                "synthetic-disabled-host-key",
                json!({"mode":"remove","remove_agent_ids":["synthetic-target"]}),
            ))
            .await
            .unwrap();
        assert_eq!(response["error"]["code"], "feature_disabled", "{response}");
        assert_eq!(
            model_operation_count(&temp),
            0,
            "feature-disabled gg_team request admitted a non-replayable host mutation"
        );
    }
}

#[tokio::test]
async fn exact_completed_host_receipt_replays_without_rechecking_mutated_roster() {
    let (gateway, caller, temp) = fixture().await;
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let workspace_id = store
        .get_workspace_agent_by_id(&caller)
        .unwrap()
        .unwrap()
        .workspace_id;
    let args = json!({"mode":"remove","remove_agent_ids":["synthetic-already-removed"]});
    let normalized_request = json!({"tool_name":"gg_team","args":args});
    let command = AgentToolMutationCommand {
        operation_id: "synthetic-completed-team-host-receipt".into(),
        workspace_id,
        caller_agent_id: caller.clone(),
        idempotency_key: "synthetic-model-replay-id".into(),
        tool_name: "gg_team".into(),
        normalized_request_hash: normalized_json_hash(&normalized_request).unwrap(),
        normalized_request,
        admitted_at: 100,
    };
    assert!(matches!(
        store.begin_agent_tool_mutation(&command).unwrap(),
        AgentToolMutationAdmission::Execute { .. }
    ));
    // This is a synthetic exact-result fixture, NOT evidence that a provider
    // session was actually closed. The target is intentionally absent from the
    // live roster, as it would be after a successful removal.
    let exact = json!({"ok":true,"result":{
        "operation":"remove","removed":[{"agent_id":"synthetic-already-removed","lifecycle":"archived"}],
        "preserve_worktrees":true,
    }});
    // Seed a historical receipt from before target-specific native journals;
    // the new finalizer intentionally refuses to fabricate such a success.
    rusqlite::Connection::open(temp.path().join("runtime.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE runtime_operations SET phase='completed',exact_terminal_result_json=?2 WHERE operation_id=?1",
            rusqlite::params![command.operation_id, exact.to_string()],
        )
        .unwrap();
    assert_eq!(
        gateway
            .invoke_tool(model_request(
                &caller,
                &command.idempotency_key,
                args.clone()
            ))
            .await
            .unwrap(),
        exact
    );
    assert_eq!(model_operation_count(&temp), 1);
    let changed_args = json!({"mode":"remove","remove_agent_ids":["different-target"]});
    let changed = gateway
        .invoke_tool(model_request(
            &caller,
            &command.idempotency_key,
            changed_args,
        ))
        .await
        .unwrap();
    assert_eq!(changed["error"]["code"], "conflict", "{changed}");
    assert_eq!(
        gateway
            .invoke_tool(model_request(&caller, &command.idempotency_key, args))
            .await
            .unwrap(),
        exact
    );
    assert_eq!(model_operation_count(&temp), 1);
}

#[tokio::test]
async fn an_invalid_pure_request_does_not_poison_the_same_host_invocation_id() {
    let (gateway, caller, temp) = fixture().await;
    let invocation_id = "synthetic-reusable-host-invocation";
    let first = gateway
        .invoke_tool(model_request(
            &caller,
            invocation_id,
            json!({"mode":"remove","remove_agent_ids":[]}),
        ))
        .await
        .unwrap();
    assert_eq!(first["error"]["code"], "bad_request", "{first}");
    assert_eq!(model_operation_count(&temp), 0);

    // Well-formed but absent target intentionally enters a durable mutation
    // intent, demonstrating that the rejected empty list consumed no key.
    // This test never creates a real worktree or native provider effect.
    let valid = json!({"mode":"assign_worktree","assign_worktree":{
        "agent_id":"synthetic-absent-member","worktree_name":"synthetic-absent-worktree"
    }});
    let second = gateway
        .invoke_tool(model_request(&caller, invocation_id, valid.clone()))
        .await
        .unwrap();
    assert_eq!(second["error"]["code"], "not_found", "{second}");
    assert_eq!(model_operation_count(&temp), 1);
    let replay = gateway
        .invoke_tool(model_request(&caller, invocation_id, valid))
        .await
        .unwrap();
    assert_eq!(replay["error"]["code"], "outcome_unresolved", "{replay}");
    assert_eq!(model_operation_count(&temp), 1);
}

#[tokio::test]
async fn historical_whitespace_duplicate_keeps_original_receipt_and_unresolved_coordinate() {
    for completed in [false, true] {
        let (gateway, caller, temp) = fixture().await;
        let store = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp.path().join("runtime.sqlite3"),
        });
        let workspace_id = store
            .get_workspace_agent_by_id(&caller)
            .unwrap()
            .unwrap()
            .workspace_id;
        // Older preflight treated these raw strings as distinct, while the
        // old target lookup trimmed both IDs. Validator evolution must not
        // hide an outcome admitted under that earlier contract.
        let args =
            json!({"mode":"remove","remove_agent_ids":["historical-target"," historical-target "]});
        let normalized_request = json!({"tool_name":"gg_team","args":args});
        let command = AgentToolMutationCommand {
            operation_id: "historical-whitespace-duplicate".into(),
            workspace_id,
            caller_agent_id: caller.clone(),
            idempotency_key: "historical-whitespace-host-key".into(),
            tool_name: "gg_team".into(),
            normalized_request_hash: normalized_json_hash(&normalized_request).unwrap(),
            normalized_request,
            admitted_at: 100,
        };
        store.begin_agent_tool_mutation(&command).unwrap();
        let exact = json!({"ok":true,"result":{"operation":"remove","removed":[
            {"agent_id":"historical-target","lifecycle":"archived"},
            {"agent_id":"historical-target","lifecycle":"archived"}],"preserve_worktrees":true}});
        if completed {
            rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap().execute(
                "UPDATE runtime_operations SET phase='completed',exact_terminal_result_json=?2 WHERE operation_id=?1",
                rusqlite::params![command.operation_id,exact.to_string()]).unwrap();
        }
        let result = gateway
            .invoke_tool(model_request(
                &caller,
                &command.idempotency_key,
                args.clone(),
            ))
            .await
            .unwrap();
        if completed {
            assert_eq!(result, exact);
        } else {
            assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
            assert_eq!(
                result["error"]["details"]["operation_id"],
                command.operation_id
            );
        }
        let changed = gateway
            .invoke_tool(model_request(
                &caller,
                &command.idempotency_key,
                json!({"mode":"remove","remove_agent_ids":["other"," other "]}),
            ))
            .await
            .unwrap();
        assert_eq!(changed["error"]["code"], "conflict", "{changed}");
        let new_request = gateway
            .invoke_tool(model_request(&caller, "new-whitespace-host-key", args))
            .await
            .unwrap();
        assert_eq!(new_request["error"]["code"], "bad_request");
        assert_eq!(model_operation_count(&temp), 1);
    }
}
