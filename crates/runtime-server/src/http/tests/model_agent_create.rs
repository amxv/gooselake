use super::operator_agent_create::{create_http, fixture, reopened_runtime};
use super::*;
use serde_json::{json, Value};

mod hooks;
mod routes;

struct Fixture {
    router: Router,
    token: String,
    temp: tempfile::TempDir,
    provider: Arc<TestProvider>,
    runtime: Arc<RuntimeSessionManager>,
    workspace: String,
    caller: String,
}

impl Fixture {
    async fn new() -> Self {
        let (router, token, temp, provider, runtime, workspace) = fixture().await;
        let (status, created, _) = create_http(
            &router,
            &token,
            &workspace,
            json!({"provider":"codex","model":"test-model","title":"Add caller"}),
            Some("model-add-caller"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let caller = created["agent_id"].as_str().unwrap().to_owned();
        finish_test_onboarding(&runtime, &workspace, &caller).await;
        Self {
            router,
            token,
            temp,
            provider,
            runtime,
            workspace,
            caller,
        }
    }

    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.temp.path().join("runtime.sqlite3")).unwrap()
    }

    fn store(&self) -> Arc<SqliteRuntimeStore> {
        Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: self.temp.path().join("runtime.sqlite3"),
        }))
    }

    async fn add(&self, key: &str) -> Value {
        self.invoke(
            key,
            json!({"mode":"add","title":"New colleague","prompt":"Inspect the fixture"}),
        )
        .await
    }

    async fn invoke(&self, key: &str, args: Value) -> Value {
        let scoped = runtime_core::mint_scoped_mcp_token(&self.token, &self.caller);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/mcp/invoke")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {scoped}"))
                    .header("x-gg-tool-invocation-id", key)
                    .body(Body::from(
                        json!({"tool_name":"gg_team","args":args}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    fn parent(&self, key: &str) -> String {
        self.db().query_row(
            "SELECT operation_id FROM runtime_operations WHERE actor_id=?1 AND idempotency_key=?2 AND kind='agent_model_tool_mutation'",
            rusqlite::params![self.caller, key], |row| row.get(0),
        ).unwrap()
    }
}

#[tokio::test]
async fn model_add_root_freezes_real_agent_child_and_exact_compact_receipt() {
    // Root creation remains supported for an ordinary non-Git directory.
    let f = Fixture::new().await;
    let baseline = f.provider.create_call_count();
    let result = f.add("model-add-root-receipt").await;
    assert_eq!(result["ok"], true, "{result}");
    let id = result["result"]["agent_id"].as_str().unwrap();
    let owner: Option<String> = f
        .db()
        .query_row(
            "SELECT source_operation_id FROM workspace_session_ownership WHERE session_id=?1",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        owner.is_some(),
        "model Add must own a durable native create child"
    );
    let child = owner.unwrap();
    let (actor_kind, actor, phase): (String, String, String) = f
        .db()
        .query_row(
            "SELECT actor_kind,actor_id,phase FROM runtime_operations WHERE operation_id=?1",
            [&child],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (actor_kind.as_str(), actor.as_str(), phase.as_str()),
        ("agent", f.caller.as_str(), "completed")
    );
    let parent = f.parent("model-add-root-receipt");
    let evidence: String = f.db().query_row(
        "SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1 AND effect_kind='model_member_create' AND target_id=?2 AND phase='finalized'",
        rusqlite::params![parent, id], |row| row.get(0),
    ).unwrap();
    let evidence: Value = serde_json::from_str(&evidence).unwrap();
    assert_eq!(evidence["outcome"]["status"], "native_create_confirmed");
    assert_eq!(evidence["outcome"]["model_result"], result["result"]);
    finish_test_onboarding(&f.runtime, &f.workspace, id).await;
    assert_eq!(f.add("model-add-root-receipt").await, result);
    assert_eq!(f.provider.create_call_count(), baseline + 1);
}

#[tokio::test]
async fn model_add_lost_native_ack_retains_original_child_and_never_recreates() {
    let f = Fixture::new().await;
    let baseline = f.provider.create_call_count();
    f.provider.set_create_failure_mode(2);
    let result = f.add("model-add-lost-native-ack").await;
    assert_eq!(result["ok"], false, "{result}");
    let parent = f.parent("model-add-lost-native-ack");
    let targets = result["error"]["details"]["targets"].as_array().unwrap();
    assert_eq!(
        targets.len(),
        1,
        "an unknown native create needs its original reserved target: {result}"
    );
    let reserved = targets[0]["agent_id"].as_str().unwrap();
    let child = targets[0]["operation_id"].as_str().unwrap();
    assert_eq!(targets[0]["status"], "outcome_unresolved");
    assert!(f.provider.native_cwd(reserved).await.is_some());
    let (claims, local): (i64, i64) = f.db().query_row(
        "SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1 AND resource_kind='operator_agent_create_native'),(SELECT COUNT(*) FROM sessions WHERE id=?2)",
        rusqlite::params![child,reserved], |row| Ok((row.get(0)?,row.get(1)?)),
    ).unwrap();
    assert_eq!((claims, local), (1, 0));
    assert_eq!(result["error"]["details"]["operation_id"], parent);
    f.provider.set_create_failure_mode(0);
    let resumed = f.provider.resumed_requests().await.len();
    let fresh = reopened_runtime(&f.temp, f.provider.clone()).await;
    fresh.recover_startup().await.unwrap();
    assert_eq!(
        f.add("model-add-lost-native-ack").await["error"]["details"],
        result["error"]["details"]
    );
    assert_eq!(f.provider.create_call_count(), baseline + 1);
    assert!(f.provider.resumed_requests().await[resumed..]
        .iter()
        .all(|r| r.runtime_session_id != reserved));
}
