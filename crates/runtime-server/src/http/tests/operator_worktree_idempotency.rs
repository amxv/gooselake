use super::workspace_worktree_routes::{call, fixture, git};
use super::*;

#[tokio::test]
async fn workspace_worktree_create_without_a_caller_key_still_writes_native_intent_first() {
    let (router, token, _temp, _, workspace, _, _) = fixture().await;
    let uri = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"worktree_name":"one-shot-native"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let operation_id = response
        .headers()
        .get("x-operation-id")
        .expect("unkeyed native worktree creation still has a durable operation")
        .to_str()
        .unwrap()
        .to_string();
    assert!(operation_id.starts_with("op_wt_"));
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["created"], true);
    assert_eq!(
        body["init_script_status"], "skipped_disabled",
        "the workspace operator route defaults to not running shell init scripts"
    );
    assert_eq!(
        body["worktree"]["deletion_policy"], "delete_on_last_claim",
        "omitting the policy must retain the configured default"
    );
    assert_eq!(body["worktree"]["created_by_operation_id"], operation_id);
    let (status, receipt) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["operation"]["phase"], "completed");
    assert_eq!(receipt["effects"][0]["phase"], "finalized");
}

#[tokio::test]
async fn workspace_worktree_keyed_create_replays_original_native_receipt_after_lost_reply() {
    let (router, token, _temp, _, workspace, _, _) = fixture().await;
    let uri = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let payload = serde_json::json!({
        "worktree_name":"durable-create", "run_init_script":false,
        "deletion_policy":"retain_on_last_claim"
    });
    let anonymous = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "lost-reply-worktree-create")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let first = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "lost-reply-worktree-create")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let operation_id = first
        .headers()
        .get("x-operation-id")
        .expect("durable operator operation header")
        .to_str()
        .unwrap()
        .to_string();
    let original: runtime_core::WorktreeCreateResponse =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(original.created);
    assert_eq!(original.init_script_status, "skipped_disabled");
    assert_eq!(
        original.worktree.created_by_operation_id.as_deref(),
        Some(operation_id.as_str())
    );

    let (status, repeated) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(payload.clone()),
        Some("lost-reply-worktree-create"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{repeated:?}");
    assert_eq!(
        repeated,
        serde_json::to_value(&original).unwrap(),
        "replay must return created:true and the *exact* original Git/init receipt"
    );
    let (status, semantically_equal) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({
            "worktree_name":" durable-create ",
            "branch_prefix":"gg",
            "base_ref":"HEAD",
            "deletion_policy":"retain_on_last_claim",
            "run_init_script":false,
        })),
        Some("lost-reply-worktree-create"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{semantically_equal:?}");
    assert_eq!(
        semantically_equal,
        serde_json::to_value(&original).unwrap(),
        "equivalent native checkout inputs share one normalized request identity"
    );
    let (status, changed) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({"worktree_name":"different-on-reused-key"})),
        Some("lost-reply-worktree-create"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{changed:?}");

    let (status, inspected) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{inspected:?}");
    assert_eq!(inspected["operation"]["phase"], "completed");
    assert_eq!(inspected["effects"][0]["phase"], "finalized");
    assert!(inspected["claims"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_worktree_exact_replay_never_runs_init_script_twice() {
    let (router, token, temp, _, workspace, _, _) = fixture().await;
    let repo = Path::new(&workspace.canonical_root);
    let script = repo.join(".agents/gg/worktree-init.sh");
    std::fs::create_dir_all(script.parent().unwrap()).unwrap();
    let marker = temp.path().join("init_invocations");
    std::fs::write(
        &script,
        format!("#!/bin/sh\nprintf x >> '{}'\n", marker.display()),
    )
    .unwrap();
    git(repo, &["add", ".agents/gg/worktree-init.sh"]);
    git(
        repo,
        &["update-index", "--chmod=+x", ".agents/gg/worktree-init.sh"],
    );
    git(repo, &["commit", "-m", "add worktree init fixture"]);

    let uri = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let payload = serde_json::json!({"worktree_name":"script-once", "run_init_script":true});
    let (status, initial) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(payload.clone()),
        Some("exact-script-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{initial:?}");
    assert_eq!(initial["created"], true);
    assert_eq!(initial["init_script_status"], "succeeded");
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");

    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(payload),
        Some("exact-script-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay, initial);
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        "x",
        "a lost-response retry must not repeat an already executed init script"
    );
}

#[tokio::test]
async fn workspace_worktree_unknown_git_result_never_reexecutes_after_retry() {
    let (router, token, _temp, _, workspace, _, _) = fixture().await;
    let uri = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let bad_base = serde_json::json!({
        "worktree_name":"unknown-native-commit",
        "base_ref":"refs/heads/nonexistent-branch-for-native-recovery",
        "run_init_script":false
    });
    let (status, failure) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(bad_base.clone()),
        Some("unknown-native-worktree"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{failure:?}");
    let operation_id = failure.to_string().split("op_wt_").nth(1).map(|suffix| {
        format!(
            "op_wt_{}",
            &suffix[..suffix
                .find(|c: char| !c.is_ascii_hexdigit())
                .unwrap_or(suffix.len())]
        )
    });
    let operation_id = operation_id.expect("conflict identifies recoverable operation ID");
    let (status, repeated) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(bad_base),
        Some("unknown-native-worktree"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{repeated:?}");
    assert!(repeated.to_string().contains(&operation_id));
    let (status, operation) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/operations/{operation_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{operation:?}");
    assert_eq!(operation["operation"]["phase"], "manual_review");
    assert_eq!(operation["effects"][0]["phase"], "uncertain");
    assert_eq!(operation["claims"].as_array().unwrap().len(), 2);
    let (status, different_key) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({"worktree_name":"unknown-native-commit", "run_init_script":false})),
        Some("cannot-bypass-unknown-native-effect"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{different_key:?}");
}
