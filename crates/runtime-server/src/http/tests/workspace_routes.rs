use super::*;

#[tokio::test]
async fn approved_pre_add_hook_runs_before_http_agent_creation_and_persists_context() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("pre-add-http-workspace");
    std::fs::create_dir(&root).unwrap();
    let registered = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"canonical_root":root,"display_name":"Pre-add hooks"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(registered.status(), StatusCode::OK);
    let registered: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let workspace = registered.workspace;
    let child = root.join("review");
    std::fs::create_dir(&child).unwrap();
    let output = root.join("output.json");
    std::fs::write(
        &output,
        serde_json::json!({
            "pre_teammate_add": {
                "spawn_template_mutation":{"cwd":child},
                "onboarding_context_append_text":"Use the scoped review directory"
            },
            "inject":{"targets":[{"agent_id":"__new_member__","text":"Review source carefully"}]}
        })
        .to_string(),
    )
    .unwrap();
    let marker = root.join("invoked-once");
    let command = format!(
        "printf x >> '{}'; cat '{}'",
        marker.display(),
        output.display()
    );
    let config_path = root.join(runtime_core::TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(
        &config_path,
        serde_json::json!({
            "version":1,"hooks":{"pre_teammate_add":[
                {"name":"review_setup","command":command,"timeout_ms":5000}
            ]}
        })
        .to_string(),
    )
    .unwrap();

    let discovered = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{}/hooks", workspace.workspace_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovered.status(), StatusCode::OK);
    let discovered: serde_json::Value =
        serde_json::from_slice(&to_bytes(discovered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert!(discovered["executable_events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "pre_teammate_add"));
    assert!(!marker.exists(), "discovery cannot execute a hook");
    let fingerprint = discovered["hooks"][0]["fingerprint"].as_str().unwrap();
    let approve = router.clone().oneshot(Request::builder()
        .method("PUT")
        .uri(format!("/v2/workspaces/{}/hooks/approval",workspace.workspace_id))
        .header(header::AUTHORIZATION,format!("Bearer {token}"))
        .header(header::CONTENT_TYPE,"application/json")
        .body(Body::from(serde_json::json!({
            "expected_revision":0,"hooks":[{
                "event_name":"pre_teammate_add","name":"review_setup","fingerprint":fingerprint
            }]
        }).to_string())).unwrap()).await.unwrap();
    assert_eq!(approve.status(), StatusCode::OK);
    assert!(!marker.exists(), "approval cannot execute a hook");

    let create = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{}/agents", workspace.workspace_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "provider":"codex","model":"test-model",
                        "onboarding":{"prompt":"Check test contracts"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let agent: runtime_core::WorkspaceAgentRecord =
        serde_json::from_slice(&to_bytes(create.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        agent.recreation_policy.authoritative_cwd,
        child.to_string_lossy()
    );
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");

    let onboarding = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/workspaces/{}/agents/{}/onboarding",
                    workspace.workspace_id, agent.agent_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(onboarding.status(), StatusCode::OK);
    let notice: serde_json::Value =
        serde_json::from_slice(&to_bytes(onboarding.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let intro = notice["input"][0]["text"].as_str().unwrap();
    assert!(intro.contains("Check test contracts"));
    assert!(intro.contains("Use the scoped review directory"));
    assert!(intro.contains("Review source carefully"));
    let effects = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/workspaces/{}/hooks/effects",
                    workspace.workspace_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let effects: serde_json::Value =
        serde_json::from_slice(&to_bytes(effects.into_body(), usize::MAX).await.unwrap()).unwrap();
    let pre = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["event_name"] == "pre_teammate_add")
        .unwrap();
    assert_eq!(pre["hook_name"], "review_setup");
    assert!(pre.get("hook").is_none(), "list must redact hook commands");
    assert!(pre.get("input").is_none(), "list must redact hook inputs");
    assert!(
        pre.get("receipt").is_none(),
        "list must redact native receipts"
    );
    assert_eq!(pre["phase"], "succeeded");
    assert_eq!(pre["application_state"], "applied");
    assert_eq!(pre["attempt_count"], 1);
    let parent_event_id = pre["parent_event_id"].as_str().unwrap();
    let detail = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/workspaces/{}/hooks/effects/{parent_event_id}/pre_teammate_add/review_setup",
                    workspace.workspace_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail.status(), StatusCode::OK);
    let detail: serde_json::Value =
        serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(detail["hook"]["event_name"], "pre_teammate_add");
    assert_eq!(detail["hook"]["name"], "review_setup");
    assert_eq!(detail["application_state"], "applied");
}

#[tokio::test]
async fn hook_grants_are_operator_only_and_cannot_become_unreviewed_shell_commands() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("hook-grant-workspace");
    std::fs::create_dir_all(&root).unwrap();
    let registered = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({"canonical_root":root,"display_name":"Approval"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(registered.status(), StatusCode::OK);
    let registered: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let workspace_id = registered.workspace.workspace_id;
    let config_path = root.join(runtime_core::TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    let marker = root.join("unapproved-shell-marker");
    let command = format!("touch {}", marker.display());
    std::fs::write(
        &config_path,
        serde_json::json!({"version":1,"hooks":{
            "pre_teammate_add":[{"name":"Project Hook","command":command,"timeout_ms":250}]
        }})
        .to_string(),
    )
    .unwrap();
    let url = format!("/v2/workspaces/{workspace_id}/hooks/approval");
    let put = |body: serde_json::Value, authorized: bool| {
        let mut request = Request::builder()
            .method("PUT")
            .uri(&url)
            .header(header::CONTENT_TYPE, "application/json");
        if authorized {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        request.body(Body::from(body.to_string())).unwrap()
    };
    let discovered = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace_id}/hooks"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(discovered.status(), StatusCode::OK);
    let discovered: serde_json::Value =
        serde_json::from_slice(&to_bytes(discovered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let fingerprint = discovered["hooks"][0]["fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(fingerprint.starts_with("sha256:"));
    let selection = serde_json::json!({
        "expected_revision":0,
        "hooks":[{"event_name":"pre_teammate_add","name":"project_hook","fingerprint":fingerprint}]
    });
    assert_eq!(
        router
            .clone()
            .oneshot(put(selection.clone(), false))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(router.clone().oneshot(put(serde_json::json!({
        "expected_revision":0,"hooks":[{"event_name":"pre_teammate_add","name":"project_hook","fingerprint":fingerprint,"command":"evil"}]
    }),true)).await.unwrap().status(),StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(router.clone().oneshot(put(serde_json::json!({
        "expected_revision":0,"hooks":[{"event_name":"pre_teammate_add","name":"not_existing","fingerprint":fingerprint}]
    }),true)).await.unwrap().status(),StatusCode::BAD_REQUEST);
    // A model may edit the project config after discovery. The stale
    // fingerprint must not approve the swapped command under the same name.
    std::fs::write(&config_path,serde_json::json!({"version":1,"hooks":{
        "pre_teammate_add":[{"name":"Project Hook","command":format!("{command} --swapped"),"timeout_ms":250}]
    }}).to_string()).unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(put(selection.clone(), true))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    std::fs::write(
        &config_path,
        serde_json::json!({"version":1,"hooks":{
            "pre_teammate_add":[{"name":"Project Hook","command":command,"timeout_ms":250}]
        }})
        .to_string(),
    )
    .unwrap();
    let granted = router
        .clone()
        .oneshot(put(selection.clone(), true))
        .await
        .unwrap();
    assert_eq!(granted.status(), StatusCode::OK);
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(granted.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(value["revision"], 1);
    assert_eq!(value["hooks"][0]["command"], command);
    assert!(!marker.exists());
    assert_eq!(
        router
            .clone()
            .oneshot(put(selection, true))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );

    // Project changes cannot silently replace an operator-granted snapshot.
    std::fs::write(&config_path, r#"{"version":1,"hooks":{"unknown":[]}}"#).unwrap();
    let read = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&url)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK);
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(read.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(value["hooks"][0]["command"], command);
    // Revocation is always available, even when the project config is invalid.
    let revoked = router
        .clone()
        .oneshot(put(
            serde_json::json!({"expected_revision":1,"hooks":[]}),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);
    let revoked: serde_json::Value =
        serde_json::from_slice(&to_bytes(revoked.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(revoked["revision"], 2);
    assert_eq!(revoked["hooks"].as_array().unwrap().len(), 0);
    assert!(!marker.exists());
    let effects = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/workspaces/{workspace_id}/hooks/effects?limit=10"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(effects.status(), StatusCode::OK);
    let effects: serde_json::Value =
        serde_json::from_slice(&to_bytes(effects.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(effects.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn effective_workspace_hooks_are_operator_only_filtered_and_never_executed() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("hook-workspace");
    std::fs::create_dir(&root).unwrap();
    let registered = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({"canonical_root":root,"display_name":"Hook workspace"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(registered.status(), StatusCode::OK);
    let workspace: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let config_path = root.join(runtime_core::TEAM_HOOK_CONFIG_RELATIVE_PATH);
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    let marker = root.join("hook-must-not-run");
    let payload = serde_json::json!({
        "version":1,
        "hooks":{
            "pre_teammate_add":[{
                "name":"Project Setup",
                "command":format!("touch {}",marker.display()),
                "timeout_ms":250
            }],
            "post_compaction":[{"name":"summary","command":"echo summary"}]
        }
    });
    std::fs::write(&config_path, payload.to_string()).unwrap();
    let url = format!("/v2/workspaces/{}/hooks", workspace.workspace.workspace_id);
    let get = |path: &str, authorized: bool| {
        let mut req = Request::builder().uri(path);
        if authorized {
            req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        req.body(Body::empty()).unwrap()
    };
    let unauthenticated = router.clone().oneshot(get(&url, false)).await.unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let all = router.clone().oneshot(get(&url, true)).await.unwrap();
    assert_eq!(all.status(), StatusCode::OK);
    let full: serde_json::Value =
        serde_json::from_slice(&to_bytes(all.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(full["execution_enabled"], true);
    assert_eq!(full["executable_events"].as_array().unwrap().len(), 6);
    assert!(full["executable_events"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("post_compaction")));
    assert_eq!(full["config_version"], 1);
    assert_eq!(full["hooks"].as_array().unwrap().len(), 2);
    assert_eq!(full["hooks"][0]["scope"], "project");
    assert_eq!(full["hooks"][0]["name"], "project_setup");
    assert_eq!(full["hooks"][0]["timeout_ms"], 250);
    assert!(
        !marker.exists(),
        "config discovery cannot execute hook commands"
    );

    let one = router
        .clone()
        .oneshot(get(&format!("{url}?event=post_compaction"), true))
        .await
        .unwrap();
    assert_eq!(one.status(), StatusCode::OK);
    let filtered: serde_json::Value =
        serde_json::from_slice(&to_bytes(one.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(filtered["hooks"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["hooks"][0]["name"], "summary");
    assert_eq!(
        router
            .clone()
            .oneshot(get(&format!("{url}?event=not_a_hook"), true))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        router
            .clone()
            .oneshot(get("/v2/workspaces/no_such_workspace/hooks", true))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    std::fs::write(&config_path, r#"{"version":1,"hooks":{"unknown":[]}}"#).unwrap();
    let invalid = router.clone().oneshot(get(&url, true)).await.unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert!(!marker.exists());
}

#[tokio::test]
async fn workspace_routes_are_durable_idempotent_and_leave_v1_available() {
    let (router, token, temp_dir) = build_test_router().await;
    let workspace_root = temp_dir.path().join("registered-workspace");
    std::fs::create_dir(&workspace_root).expect("workspace root");
    let body = serde_json::json!({
        "canonical_root": workspace_root,
        "display_name": "Registered Workspace"
    });

    let register = |body: serde_json::Value, key: &str| {
        Request::builder()
            .method("POST")
            .uri("/v2/workspaces")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("Idempotency-Key", key)
            .body(Body::from(body.to_string()))
            .expect("registration request")
    };

    let first_response = router
        .clone()
        .oneshot(register(body.clone(), "workspace-register-1"))
        .await
        .expect("first registration response");
    assert_eq!(first_response.status(), StatusCode::OK);
    let first: runtime_core::WorkspaceRegisterResponse = serde_json::from_slice(
        &to_bytes(first_response.into_body(), usize::MAX)
            .await
            .expect("first body"),
    )
    .expect("first registration json");

    let replay_response = router
        .clone()
        .oneshot(register(body.clone(), "workspace-register-1"))
        .await
        .expect("replay response");
    assert_eq!(replay_response.status(), StatusCode::OK);
    let replay: runtime_core::WorkspaceRegisterResponse = serde_json::from_slice(
        &to_bytes(replay_response.into_body(), usize::MAX)
            .await
            .expect("replay body"),
    )
    .expect("replay json");
    assert_eq!(
        replay, first,
        "same key must replay the exact terminal result"
    );

    let second_operation_response = router
        .clone()
        .oneshot(register(body.clone(), "workspace-register-2"))
        .await
        .expect("second operation response");
    assert_eq!(second_operation_response.status(), StatusCode::OK);
    let second_operation: runtime_core::WorkspaceRegisterResponse = serde_json::from_slice(
        &to_bytes(second_operation_response.into_body(), usize::MAX)
            .await
            .expect("second operation body"),
    )
    .expect("second operation json");
    assert_eq!(
        second_operation.workspace.workspace_id,
        first.workspace.workspace_id
    );
    assert_ne!(second_operation.operation_id, first.operation_id);

    let conflict_body = serde_json::json!({
        "canonical_root": body["canonical_root"],
        "display_name": "Changed Input"
    });
    let conflict = router
        .clone()
        .oneshot(register(conflict_body, "workspace-register-1"))
        .await
        .expect("conflict response");
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    let list_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("list request"),
        )
        .await
        .expect("list response");
    assert_eq!(list_response.status(), StatusCode::OK);
    let list: Vec<runtime_core::WorkspaceRecord> = serde_json::from_slice(
        &to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("list body"),
    )
    .expect("list json");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0], first.workspace);

    let get_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{}", first.workspace.workspace_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("get request"),
        )
        .await
        .expect("get response");
    assert_eq!(get_response.status(), StatusCode::OK);
    let fetched: runtime_core::WorkspaceRecord = serde_json::from_slice(
        &to_bytes(get_response.into_body(), usize::MAX)
            .await
            .expect("get body"),
    )
    .expect("get json");
    assert_eq!(fetched, first.workspace);

    let operation_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/operations/{}", first.operation_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("operation request"),
        )
        .await
        .expect("operation response");
    assert_eq!(operation_response.status(), StatusCode::OK);
    let operation: runtime_core::OperationDetails = serde_json::from_slice(
        &to_bytes(operation_response.into_body(), usize::MAX)
            .await
            .expect("operation body"),
    )
    .expect("operation json");
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(operation.transitions.len(), 2);
    assert!(operation.claims.is_empty());

    let v1_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/health")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("v1 health request"),
        )
        .await
        .expect("v1 health response");
    assert_eq!(v1_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn workspace_routes_require_operator_bearer_auth() {
    let (router, _token, _temp_dir) = build_test_router().await;
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v2/workspaces")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
