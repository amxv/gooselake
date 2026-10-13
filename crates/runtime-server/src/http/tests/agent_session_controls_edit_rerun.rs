use super::workspace_worktree_routes::call;
use super::*;

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
    let (router, token, temp, provider) =
        build_test_router_with_provider(TeamMcpPolicy::default()).await;
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
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(body.clone()),
        Some("unknown-fork"),
    )
    .await;
    assert_ne!(status, StatusCode::OK);
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
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/agents/{agent_id}/edits/{operation_id}/resolve"),
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
