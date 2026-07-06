use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::{
    envelope::{
        annotate_envelope_with_caller_agent_id, build_ping_payload, build_team_manage_description,
        envelope_to_call_tool_result,
    },
    gateway::{GatewayClientConfig, TeamModelPresetCapabilitySnapshot},
    tool_params::GgTeamStatusRequest,
};

use super::GgMcpServer;

#[test]
fn gg_ping_payload_has_expected_envelope_shape() {
    let payload = build_ping_payload();
    assert_eq!(payload["ok"], serde_json::json!(true));
    assert_eq!(
        payload["result"]["name"],
        serde_json::json!(env!("CARGO_PKG_NAME"))
    );
    assert_eq!(
        payload["result"]["version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn tool_result_marks_error_when_envelope_is_not_ok() {
    let result = envelope_to_call_tool_result(serde_json::json!({
        "ok": false,
        "error": {
            "code": "bad_request",
            "message": "oops",
        }
    }));

    assert_eq!(result.is_error, Some(true));
}

#[test]
fn annotate_envelope_with_caller_agent_id_sets_top_level_and_result_fields() {
    let envelope = annotate_envelope_with_caller_agent_id(
        json!({
            "ok": true,
            "result": {
                "value": 1
            }
        }),
        Some("sess_caller"),
    );

    assert_eq!(envelope["caller_agent_id"], json!("sess_caller"));
    assert_eq!(envelope["result"]["caller_agent_id"], json!("sess_caller"));
}

#[test]
fn team_manage_description_includes_model_presets_when_available() {
    let description =
        build_team_manage_description(&["claude-sonnet-5".to_string(), "gpt-5".to_string()]);
    assert!(description.contains("optional `worktree_name`"));
    assert!(description.contains("optional `image_paths`"));
    assert!(description.contains("optional `use_existing_worktree`"));
    assert!(description.contains("optional `creator_compaction_subscription`"));
    assert!(description.contains("optional `prompt`"));
    assert!(description.contains("defaults to `auto`"));
    assert!(description.contains("set `unsubscribed`"));
    assert!(description.contains("canonical direct TeamMessage"));
    assert!(description.contains("runtime-owned"));
    assert!(description.contains("ui_command"));
    assert!(description.contains("[\"agent_1\", \"agent_2\"]"));
    assert!(description.contains("Available model_preset values: claude-sonnet-5, gpt-5."));
}

#[test]
fn hidden_tool_caller_metadata_is_deserialized_but_not_serialized() {
    let parsed: GgTeamStatusRequest = serde_json::from_value(json!({
        "team_id": "team_hidden_caller_metadata",
        "__gg_caller_agent_id": "sess_hidden_caller",
    }))
    .expect("hidden caller metadata should deserialize");

    assert_eq!(
        parsed.tool_call_metadata.caller_agent_id.as_deref(),
        Some("sess_hidden_caller")
    );

    let serialized = serde_json::to_value(&parsed).expect("request should serialize");
    assert!(
        serialized.get("__gg_caller_agent_id").is_none(),
        "hidden caller metadata should never be forwarded to gateway args"
    );
}

#[tokio::test]
async fn team_message_tool_description_mentions_markdown_and_image_paths() {
    let server = GgMcpServer::new();
    let tools = server.tools_with_runtime_metadata().await;

    let tool_name = "gg_team_message";
    let description = tools
        .iter()
        .find(|tool| tool.name.as_ref() == tool_name)
        .and_then(|tool| tool.description.as_ref())
        .map(|description| description.as_ref())
        .expect("team message tool description should be present");
    assert!(
        description.to_ascii_lowercase().contains("markdown"),
        "expected markdown hint in {tool_name} description"
    );
    assert!(
        description.contains("image_paths"),
        "expected image_paths guidance in {tool_name} description"
    );
    assert!(
        description.to_ascii_lowercase().contains("provided order"),
        "expected ordered image-path guidance in {tool_name} description"
    );
}

#[tokio::test]
async fn team_message_tool_schema_exposes_optional_image_paths() {
    let server = GgMcpServer::new();
    let tools = server.tools_with_runtime_metadata().await;

    let tool_name = "gg_team_message";
    let team_message_tool = tools
        .iter()
        .find(|tool| tool.name.as_ref() == tool_name)
        .expect("team message tool should be present");
    let required_fields = team_message_tool
        .input_schema
        .get("required")
        .and_then(Value::as_array)
        .expect("required fields should be present");
    assert!(
        required_fields
            .iter()
            .any(|entry| entry.as_str() == Some("message"))
    );
    assert!(
        !required_fields
            .iter()
            .any(|entry| entry.as_str() == Some("image_paths"))
    );

    let image_paths_schema = team_message_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("image_paths"))
        .and_then(Value::as_object)
        .expect("image_paths schema should be present");
    let items = image_paths_schema
        .get("items")
        .and_then(Value::as_object)
        .expect("image_paths.items schema should be present");
    assert_eq!(items.get("type").and_then(Value::as_str), Some("string"));
    assert_eq!(items.get("minLength").and_then(Value::as_u64), Some(1));
}

#[tokio::test]
async fn team_manage_tool_schema_exposes_remove_agent_ids_as_array() {
    let server = GgMcpServer::new();
    let tools = server.tools_with_runtime_metadata().await;

    let team_manage_tool = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_team_manage")
        .expect("team manage tool should be present");
    let description = team_manage_tool
        .description
        .as_ref()
        .map(|description| description.as_ref())
        .expect("team manage description should be present");
    assert!(description.contains("optional `prompt`"));
    assert!(description.contains("optional `image_paths`"));
    assert!(description.contains("optional `worktree_name`"));
    assert!(description.contains("optional `use_existing_worktree`"));
    assert!(description.contains("optional `creator_compaction_subscription`"));
    assert!(description.contains("defaults to `auto`"));
    assert!(description.contains("set `unsubscribed`"));
    assert!(description.contains("canonical direct TeamMessage"));
    assert!(description.contains("runtime-owned"));
    assert!(description.contains("ui_command"));

    let worktree_name_schema = team_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("worktree_name"))
        .expect("worktree_name schema should be present");
    assert!(
        !worktree_name_schema.is_null(),
        "worktree_name schema should not be null"
    );
    let image_paths_schema = team_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("image_paths"))
        .expect("image_paths schema should be present");
    assert!(
        !image_paths_schema.is_null(),
        "image_paths schema should not be null"
    );
    let use_existing_worktree_schema = team_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("use_existing_worktree"))
        .expect("use_existing_worktree schema should be present");
    assert!(
        !use_existing_worktree_schema.is_null(),
        "use_existing_worktree schema should not be null"
    );
    let creator_compaction_subscription_schema = team_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("creator_compaction_subscription"))
        .expect("creator_compaction_subscription schema should be present");
    assert!(
        !creator_compaction_subscription_schema.is_null(),
        "creator_compaction_subscription schema should not be null"
    );
    let creator_compaction_subscription_schema_json =
        serde_json::to_string(creator_compaction_subscription_schema)
            .expect("creator_compaction_subscription schema should serialize");
    assert!(
        creator_compaction_subscription_schema_json.contains("auto"),
        "creator_compaction_subscription schema should include `auto`"
    );
    assert!(
        creator_compaction_subscription_schema_json.contains("unsubscribed"),
        "creator_compaction_subscription schema should include `unsubscribed`"
    );

    let remove_agent_ids_schema = team_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("remove_agent_ids"))
        .and_then(Value::as_object)
        .expect("remove_agent_ids schema should be present");
    assert_eq!(
        remove_agent_ids_schema.get("type").and_then(Value::as_str),
        Some("array")
    );
    assert_eq!(
        remove_agent_ids_schema
            .get("minItems")
            .and_then(Value::as_u64),
        Some(1)
    );
    let remove_items = remove_agent_ids_schema
        .get("items")
        .and_then(Value::as_object)
        .expect("remove_agent_ids.items should be present");
    assert_eq!(
        remove_items.get("type").and_then(Value::as_str),
        Some("string")
    );
    assert_eq!(
        remove_items.get("minLength").and_then(Value::as_u64),
        Some(1)
    );
    assert!(
        remove_agent_ids_schema
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("must be omitted")
    );
    assert!(
        remove_agent_ids_schema
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("[\"agent_1\", \"agent_2\"]")
    );
}

#[tokio::test]
async fn team_manage_tool_schema_injects_model_preset_enum_from_cached_capabilities() {
    let server = GgMcpServer {
        tool_router: GgMcpServer::tool_router(),
        gateway_client_config: None,
        gateway_client: Arc::new(tokio::sync::RwLock::new(None)),
        process_tools_enabled: true,
        team_call_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        team_model_presets_cache: Arc::new(tokio::sync::RwLock::new(Some(
            TeamModelPresetCapabilitySnapshot {
                revision: 5,
                presets: vec!["planner".to_string(), "designer".to_string()],
                team_tools_enabled: true,
            },
        ))),
    };
    let tools = server.tools_with_runtime_metadata().await;
    let team_manage_tool = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_team_manage")
        .expect("team manage tool should be present");
    let model_preset_schema = team_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("model_preset"))
        .and_then(Value::as_object)
        .expect("model_preset schema should be present");
    let any_of = model_preset_schema
        .get("anyOf")
        .and_then(Value::as_array)
        .expect("model_preset.anyOf should be present");
    let enum_values = any_of
        .iter()
        .find_map(|variant| variant.get("enum"))
        .and_then(Value::as_array)
        .expect("model_preset string variant should include enum values");
    assert!(
        enum_values
            .iter()
            .any(|entry| entry.as_str() == Some("planner"))
    );
    assert!(
        enum_values
            .iter()
            .any(|entry| entry.as_str() == Some("designer"))
    );
}

#[tokio::test]
async fn gg_markdown_open_tool_is_listed_with_navigation_fields() {
    let server = GgMcpServer::new();
    let tools = server.tools_with_runtime_metadata().await;
    let markdown_tool = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_markdown_open")
        .expect("gg_markdown_open tool should be present");
    let properties = markdown_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .expect("markdown tool properties should be present");
    assert!(properties.contains_key("path"));
    assert!(properties.contains_key("branch"));
    assert!(properties.contains_key("target_agent_id"));
    assert!(properties.contains_key("line"));
    assert!(properties.contains_key("anchor"));
}

#[tokio::test]
async fn tools_list_hides_team_tools_when_cached_capabilities_disable_team_namespace() {
    let server = GgMcpServer {
        tool_router: GgMcpServer::tool_router(),
        gateway_client_config: None,
        gateway_client: Arc::new(tokio::sync::RwLock::new(None)),
        process_tools_enabled: true,
        team_call_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        team_model_presets_cache: Arc::new(tokio::sync::RwLock::new(Some(
            TeamModelPresetCapabilitySnapshot {
                revision: 1,
                presets: Vec::new(),
                team_tools_enabled: false,
            },
        ))),
    };

    let tools = server.tools_with_runtime_metadata().await;
    assert!(
        !tools
            .iter()
            .any(|tool| tool.name.as_ref().starts_with("gg_team_")),
        "gg_team tools should be omitted when runtime capabilities omit gg_team"
    );
    assert!(
        tools
            .iter()
            .any(|tool| tool.name.as_ref() == "gg_process_status"),
        "non-team tools should remain listed"
    );
}

#[tokio::test]
async fn gg_team_calls_are_serialized_per_caller() {
    let concurrency_state = Arc::new(InvokeConcurrencyState::default());
    let app = Router::new()
        .route("/invoke", post(serialization_invoke_stub))
        .with_state(Arc::clone(&concurrency_state));
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("stub listener should bind");
    let gateway_addr = listener.local_addr().expect("listener should provide addr");
    let gateway_handle = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            panic!("stub gateway exited unexpectedly: {error}");
        }
    });

    let server = GgMcpServer {
        tool_router: GgMcpServer::tool_router(),
        gateway_client_config: Some(GatewayClientConfig {
            base_url: format!("http://{}:{}", gateway_addr.ip(), gateway_addr.port()),
            auth_token: "integration_token".to_string(),
            default_caller_agent_id: None,
            require_tool_caller_agent_id: false,
        }),
        gateway_client: Arc::new(tokio::sync::RwLock::new(None)),
        process_tools_enabled: true,
        team_call_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        team_model_presets_cache: Arc::new(tokio::sync::RwLock::new(None)),
    };

    let request_args = json!({
        "team_id": "team_serialization",
        "recipient_agent_id": "sess_receiver",
        "message": "hello",
    });
    let (first, second) = tokio::join!(
        server.invoke_backend_tool(
            "gg_team",
            "gg_team_message",
            &request_args,
            "sess_serialized_caller",
            None,
        ),
        server.invoke_backend_tool(
            "gg_team",
            "gg_team_message",
            &request_args,
            "sess_serialized_caller",
            None,
        ),
    );

    assert_eq!(first.is_error, Some(false));
    assert_eq!(second.is_error, Some(false));
    assert_eq!(
        concurrency_state.max_in_flight.load(Ordering::SeqCst),
        1,
        "gg_team calls for the same caller must be serialized",
    );

    gateway_handle.abort();
}

#[tokio::test]
async fn gg_team_calls_are_parallel_for_distinct_callers() {
    let concurrency_state = Arc::new(InvokeConcurrencyState::default());
    let app = Router::new()
        .route("/invoke", post(serialization_invoke_stub))
        .with_state(Arc::clone(&concurrency_state));
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("stub listener should bind");
    let gateway_addr = listener.local_addr().expect("listener should provide addr");
    let gateway_handle = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            panic!("stub gateway exited unexpectedly: {error}");
        }
    });

    let server = GgMcpServer {
        tool_router: GgMcpServer::tool_router(),
        gateway_client_config: Some(GatewayClientConfig {
            base_url: format!("http://{}:{}", gateway_addr.ip(), gateway_addr.port()),
            auth_token: "integration_token".to_string(),
            default_caller_agent_id: None,
            require_tool_caller_agent_id: false,
        }),
        gateway_client: Arc::new(tokio::sync::RwLock::new(None)),
        process_tools_enabled: true,
        team_call_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        team_model_presets_cache: Arc::new(tokio::sync::RwLock::new(None)),
    };

    let request_args = json!({
        "team_id": "team_serialization",
        "recipient_agent_id": "sess_receiver",
        "message": "hello",
    });
    let (first, second) = tokio::join!(
        server.invoke_backend_tool(
            "gg_team",
            "gg_team_message",
            &request_args,
            "sess_serialized_caller_a",
            None,
        ),
        server.invoke_backend_tool(
            "gg_team",
            "gg_team_message",
            &request_args,
            "sess_serialized_caller_b",
            None,
        ),
    );

    assert_eq!(first.is_error, Some(false));
    assert_eq!(second.is_error, Some(false));
    assert_eq!(
        concurrency_state.max_in_flight.load(Ordering::SeqCst),
        2,
        "gg_team calls for different callers should not be globally serialized",
    );

    gateway_handle.abort();
}

#[derive(Default)]
struct InvokeConcurrencyState {
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

async fn serialization_invoke_stub(
    State(state): State<Arc<InvokeConcurrencyState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let provided_auth = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided_auth != "Bearer integration_token" {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "ok": false,
                "error": {
                    "code": "unauthorized",
                    "message": "invalid auth header",
                }
            })),
        );
    }

    let in_flight_now = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    let mut observed_max = state.max_in_flight.load(Ordering::SeqCst);
    while in_flight_now > observed_max {
        match state.max_in_flight.compare_exchange(
            observed_max,
            in_flight_now,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => break,
            Err(previous) => observed_max = previous,
        }
    }

    tokio::time::sleep(Duration::from_millis(75)).await;
    state.in_flight.fetch_sub(1, Ordering::SeqCst);

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "result": {
                "tool_name": body.get("tool_name").and_then(Value::as_str).unwrap_or_default(),
            }
        })),
    )
}
