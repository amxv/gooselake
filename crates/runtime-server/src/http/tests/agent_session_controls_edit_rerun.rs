use super::workspace_worktree_routes::call;
use super::*;

mod native_recovery;

async fn ready_edit_agent(
    mode: u8,
) -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    String,
    String,
) {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    provider.set_edit_mode(mode);
    let root = temp.path().join("edit-root");
    std::fs::create_dir_all(&root).expect("workspace root");
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Edit Test"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents"),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let agent_id = agent["agent_id"].as_str().unwrap().to_string();
    finish_test_onboarding(&runtime, workspace_id, &agent_id).await;
    let (status, accepted) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"original request"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let turn_id = accepted["turn_id"].as_str().unwrap().to_string();
    timeout(Duration::from_secs(5), async {
        loop {
            let (status, session) = call(
                &router,
                &token,
                "GET",
                &format!("/v1/sessions/{agent_id}"),
                None,
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            if session["status"] == "ready" && session["active_turn_id"].is_null() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original native turn completed");
    (router, token, temp, provider, agent_id, turn_id)
}

#[tokio::test]
async fn edit_rejects_unsupported_snapshot_options_before_native_fork_or_operation() {
    let (router, token, temp, provider, agent_id, turn_id) = ready_edit_agent(1).await;
    let uri = format!("/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let count_operations = || -> i64 {
        db.query_row("SELECT COUNT(*) FROM runtime_operations", [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    let before = count_operations();
    for invalid in [
        serde_json::json!({"edited_snapshot":{
            "prompt_text":"edited", "image_refs":[], "invocation":null, "force":true
        }}),
        serde_json::json!({"edited_snapshot":{
            "prompt_text":"edited", "image_refs":[{
                "media_type":"image/png", "persisted_absolute_path":null,
                "persisted_display_path":null, "skip_image_validation":true
            }], "invocation":null
        }}),
        serde_json::json!({"edited_snapshot":{
            "prompt_text":"edited", "image_refs":[], "invocation":{
                "provider":"codex", "name":"audit", "display_name":null,
                "path":null, "bypass_skill_validation":true
            }
        }}),
    ] {
        let (status, error) = call(&router, &token, "POST", &uri, Some(invalid), None).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
        assert_eq!(error["error"]["code"], "invalid_request");
    }
    assert_eq!(provider.edit_calls(), 0, "no provider fork on invalid JSON");
    assert_eq!(count_operations(), before, "no native intent was admitted");
}

#[tokio::test]
async fn edit_and_rerun_forks_verified_provider_and_replays_exact_operation_without_reinvoking() {
    let (router, token, temp, provider, agent_id, turn_id) = ready_edit_agent(1).await;
    let base = format!("/v2/agents/{agent_id}");
    let uri = format!("{base}/turns/{turn_id}/edit-rerun");
    let body = serde_json::json!({"edited_snapshot":{
        "prompt_text":"new edited request","image_refs":[],"invocation":null,
    }});
    let (status, response) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("edit-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["phase"], "completed");
    let edit_id = response["operation_id"].as_str().unwrap();
    let replacement_id = response["replacement_turn_id"].as_str().unwrap();
    assert_ne!(replacement_id, turn_id);
    assert_eq!(provider.edit_calls(), 1);
    let inputs = provider.captured_edit_inputs().await;
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0][0]["text"], "new edited request");

    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("edit-once"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["operation_id"], edit_id);
    assert_eq!(replay["replacement_turn_id"], replacement_id);
    assert_eq!(
        provider.edit_calls(),
        1,
        "idempotent request never forks twice"
    );

    let (status, conflict) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({"edited_snapshot":{
            "prompt_text":"different", "image_refs":[],"invocation":null
        }})),
        Some("edit-once"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");

    let (status, details) = call(
        &router,
        &token,
        "GET",
        &format!("{base}/edits/{edit_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(details["phase"], "completed");
    assert!(details["child_provider_session_ref"]
        .as_str()
        .unwrap()
        .contains("child-of-"));

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let admissions = store.list_turn_admissions().expect("historical admissions");
    let original = admissions
        .iter()
        .find(|row| row.turn_id == turn_id)
        .unwrap();
    assert_eq!(original.user_input_snapshot.prompt_text, "original request");
    assert!(original.provider_native_turn_id.is_some());
    let rerun = admissions
        .iter()
        .find(|row| row.turn_id == replacement_id)
        .unwrap();
    assert_eq!(rerun.user_input_snapshot.prompt_text, "new edited request");
    assert_eq!(rerun.correlation.correlation_id.as_deref(), Some(edit_id));
}

#[tokio::test]
async fn unknown_native_edit_is_fenced_across_idempotent_retries_and_cannot_be_guessed_complete() {
    let (router, token, _temp, provider, agent_id, turn_id) = ready_edit_agent(2).await;
    let uri = format!("/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun");
    let body = serde_json::json!({"edited_snapshot":{
        "prompt_text":"never duplicate this","image_refs":[],"invocation":null,
    }});
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("unknown-fork"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(unknown["error"]["details"]["dispatch_outcome"], "unknown");
    assert_eq!(unknown["error"]["details"]["family"], "edit_rerun");
    assert_eq!(
        unknown["error"]["details"]["native_retry_authorized"],
        false
    );
    assert_eq!(provider.edit_calls(), 1);
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body),
        Some("unknown-fork"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["phase"], "manual_review");
    assert_eq!(
        provider.edit_calls(),
        1,
        "ambiguous fork must not be re-dispatched"
    );
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(serde_json::json!({"input":[{"type":"text","text":"must be fenced"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let operation_id = replay["operation_id"].as_str().unwrap();
    let review_uri = format!("/v2/agents/{agent_id}/edits/{operation_id}/resolve");
    let (status, keyed) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(serde_json::json!({"evidence":"Provider outcome independently examined"})),
        Some("misleading-edit-review-replay"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(keyed["error"]["code"], "idempotency_not_supported");
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &review_uri,
        Some(serde_json::json!({"evidence":"Provider outcome independently examined"})),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unknown child identity cannot be guessed safe"
    );
}

#[tokio::test]
async fn compaction_receipt_replays_during_later_unresolved_edit_without_native_dispatch() {
    let (router, token, temp, provider, agent_id, turn_id) = ready_edit_agent(2).await;
    provider.set_compaction_mode(2);
    let compact_uri = format!("/v2/agents/{agent_id}/compact");
    let original_key = "compact-before-unknown-edit";
    let (status, original) = call(
        &router,
        &token,
        "POST",
        &compact_uri,
        None,
        Some(original_key),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{original}");
    assert_eq!(original["phase"], "not_performed");
    assert_eq!(provider.compaction_calls(), 1);

    let edit_uri = format!("/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun");
    let edit_body = serde_json::json!({"edited_snapshot":{
        "prompt_text":"later edit with unknown native outcome",
        "image_refs":[], "invocation":null
    }});
    let (status, unknown) = call(
        &router,
        &token,
        "POST",
        &edit_uri,
        Some(edit_body.clone()),
        Some("later-unknown-edit"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{unknown}");
    let (status, edit) = call(
        &router,
        &token,
        "POST",
        &edit_uri,
        Some(edit_body),
        Some("later-unknown-edit"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edit}");
    assert_eq!(edit["phase"], "manual_review");
    assert_eq!(provider.edit_calls(), 1);

    let database_path = temp.path().join("runtime.sqlite3");
    let db = rusqlite::Connection::open(&database_path).unwrap();
    let counts = || -> (i64, i64, i64, i64) {
        db.query_row(
            "SELECT (SELECT COUNT(*) FROM session_compaction_runs),
                    (SELECT COUNT(*) FROM session_edit_reruns),
                    (SELECT COUNT(*) FROM runtime_operations),
                    (SELECT COUNT(*) FROM runtime_events)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
    };
    let before = counts();
    for key in [Some("new-compact-during-unknown-edit"), None] {
        let (status, blocked) = call(&router, &token, "POST", &compact_uri, None, key).await;
        assert_eq!(status, StatusCode::CONFLICT, "{blocked}");
        assert!(blocked["error"]["message"]
            .as_str()
            .unwrap()
            .contains("edit-and-rerun remains unresolved"));
    }

    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &compact_uri,
        Some(serde_json::json!({})),
        Some(original_key),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "saved receipt must remain readable: {replay}"
    );
    assert_eq!(
        replay, original,
        "exact retry returns the original durable run"
    );

    // Reopening the same fixture without a provider must still permit receipt
    // inspection while retaining the durable fence against new native work.
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig { database_path }));
    assert!(store.unresolved_session_edit_rerun(&agent_id).unwrap());
    let reopened = Arc::new(
        RuntimeSessionManager::new(
            store.clone(),
            Arc::new(runtime_core::ProviderRegistry::new()),
            128,
        )
        .unwrap(),
    );
    let replay = reopened
        .compact_workspace_agent(&agent_id, Some(original_key.into()))
        .await
        .expect("persisted compaction replay does not require a provider");
    assert_eq!(serde_json::to_value(replay).unwrap(), original);
    for key in [Some("new-compact-after-reopen".into()), None] {
        assert!(matches!(
            reopened.compact_workspace_agent(&agent_id, key).await,
            Err(RuntimeError::Conflict(message))
                if message.contains("edit-and-rerun remains unresolved")
        ));
    }
    assert_eq!(
        serde_json::to_value(
            store
                .get_session_edit_rerun(edit["operation_id"].as_str().unwrap())
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        edit,
        "receipt replay must not settle or change the later unknown edit"
    );
    assert_eq!(
        counts(),
        before,
        "replay and rejected new work make no durable writes"
    );
    assert_eq!(provider.compaction_calls(), 1);
    assert_eq!(provider.edit_calls(), 1);
}

#[tokio::test]
async fn edit_rejects_missing_native_image_before_any_provider_fork() {
    let (router, token, temp, provider, agent_id, turn_id) = ready_edit_agent(1).await;
    let uri = format!("/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun");
    let missing = temp.path().join("not-persisted.png");
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({
            "edited_snapshot":{"prompt_text":"please edit","image_refs":[{
                "media_type":"image/png","persisted_absolute_path":missing,
                "persisted_display_path":"not-persisted.png"
            }],"invocation":null}
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        provider.edit_calls(),
        0,
        "missing attachment fails before effect intent"
    );
}

#[tokio::test]
async fn edit_reconstructs_ordered_persisted_images_and_codex_skill_for_native_fork() {
    let (router, token, temp, provider, agent_id, turn_id) = ready_edit_agent(1).await;
    let first = temp.path().join("first.png");
    let second = temp.path().join("second.jpg");
    let skill = temp.path().join("SKILL.md");
    std::fs::write(&first, b"native-image-one").unwrap();
    std::fs::write(&second, b"native-image-two").unwrap();
    std::fs::write(&skill, b"# Skill\nRun the specified audit.\n").unwrap();
    let (status, op) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun"),
        Some(serde_json::json!({"edited_snapshot":{
            "prompt_text":"Analyze the two images with the skill",
            "image_refs":[
                {"media_type":"image/png","persisted_absolute_path":first,"persisted_display_path":"first.png"},
                {"media_type":"image/jpeg","persisted_absolute_path":second,"persisted_display_path":"second.jpg"}
            ],
            "invocation":{"provider":"codex","name":"audit","display_name":"Audit","path":skill}
        }})),
        Some("ordered-native-edit"),
    ).await;
    assert_eq!(status, StatusCode::OK, "{op}");
    assert_eq!(op["phase"], "completed");
    let captured = provider.captured_edit_inputs().await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].len(), 4);
    assert_eq!(
        captured[0][0]["text"],
        "Analyze the two images with the skill"
    );
    assert_eq!(captured[0][1]["type"], "local_image");
    assert_eq!(captured[0][1]["path"], first.to_string_lossy().as_ref());
    assert_eq!(captured[0][2]["path"], second.to_string_lossy().as_ref());
    assert_eq!(captured[0][3]["type"], "skill");
    assert_eq!(captured[0][3]["name"], "audit");
    assert_eq!(captured[0][3]["path"], skill.to_string_lossy().as_ref());
}

#[tokio::test]
async fn signed_agent_revision_ceiling_rejects_native_edit_before_fake_hard_fork() {
    let (router, token, temp, provider, agent_id, turn_id) = ready_edit_agent(1).await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX, agent_id],
    )
    .unwrap();
    let original_attachment: String = db
        .query_row(
            "SELECT provider_session_ref FROM sessions WHERE id=?1",
            [&agent_id],
            |row| row.get(0),
        )
        .unwrap();
    let (status, response) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun"),
        Some(serde_json::json!({"edited_snapshot":{
            "prompt_text":"edit at signed revision ceiling",
            "image_refs":[],"invocation":null
        }})),
        Some("edit-signed-ceiling"),
    )
    .await;
    assert_eq!(
        provider.edit_calls(),
        0,
        "signed ceiling must never invoke native hard fork: {status} {response}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(response["error"]["code"], "invalid_state");
    let (revision, revision_type, attachment, intents): (i64, String, String, i64) = db
        .query_row(
            "SELECT a.revision,typeof(a.revision),s.provider_session_ref,
                    (SELECT COUNT(*) FROM session_edit_reruns)
             FROM workspace_agents a JOIN sessions s ON s.id=a.session_id
             WHERE a.session_id=?1",
            [&agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        (revision, revision_type, attachment, intents),
        (i64::MAX, "integer".into(), original_attachment, 0)
    );
}

#[tokio::test]
async fn edit_last_signed_successor_commits_then_exact_key_replays_without_native_fork() {
    let (router, token, temp, provider, agent_id, turn_id) = ready_edit_agent(1).await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX - 1, agent_id],
    )
    .unwrap();
    let path = format!("/v2/agents/{agent_id}/turns/{turn_id}/edit-rerun");
    let input = serde_json::json!({"edited_snapshot":{
        "prompt_text":"last signed edit",
        "image_refs":[],"invocation":null
    }});
    let (status, committed) = call(
        &router,
        &token,
        "POST",
        &path,
        Some(input.clone()),
        Some("last-signed-edit"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{committed}");
    assert_eq!(committed["phase"], "completed");
    assert_eq!(provider.edit_calls(), 1);
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspace_agents WHERE session_id=?1",
            [&agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &path,
        Some(input.clone()),
        Some("last-signed-edit"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, committed);
    assert_eq!(
        provider.edit_calls(),
        1,
        "same-key replay cannot fork twice"
    );
    let (status, rejected) = call(
        &router,
        &token,
        "POST",
        &path,
        Some(input),
        Some("new-signed-edit"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    assert_eq!(rejected["error"]["code"], "invalid_state");
    assert_eq!(provider.edit_calls(), 1);
}
