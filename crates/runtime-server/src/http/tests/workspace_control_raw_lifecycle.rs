use super::super::native_review_refusal::database_snapshot;
use super::super::workspace_worktree_routes::call;
use super::*;
use runtime_core::{RuntimeApp, WorkspaceAgentLifecycleState, WorkspaceAgentRecord};
use std::sync::atomic::Ordering;

struct MembershipFixture {
    router: Router,
    token: String,
    temp: tempfile::TempDir,
    provider: Arc<TestProvider>,
    runtime: Arc<RuntimeSessionManager>,
    app: Arc<RuntimeApp>,
    workspace: String,
    caller: String,
    peer: String,
}

impl MembershipFixture {
    async fn new() -> Self {
        let (router, token, temp, provider, runtime, app) =
            build_test_router_with_services(TeamMcpPolicy::default()).await;
        let root = temp.path().join("membership-workspace");
        std::fs::create_dir_all(&root).unwrap();
        let (status, registered) = call(
            &router,
            &token,
            "POST",
            "/v2/workspaces",
            Some(serde_json::json!({"canonical_root": root})),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{registered}");
        let workspace = registered["workspace"]["workspace_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut members = Vec::new();
        for (title, key) in [("Caller", "membership-caller"), ("Peer", "membership-peer")] {
            let (status, created) = call(
                &router,
                &token,
                "POST",
                &format!("/v2/workspaces/{workspace}/agents"),
                Some(serde_json::json!({
                    "provider": "codex", "model": "test-model", "cwd": root, "title": title
                })),
                Some(key),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{created}");
            let id = created["agent_id"].as_str().unwrap().to_owned();
            finish_test_onboarding(&runtime, &workspace, &id).await;
            members.push(id);
        }
        Self {
            router,
            token,
            temp,
            provider,
            runtime,
            app,
            workspace,
            caller: members.remove(0),
            peer: members.remove(0),
        }
    }

    fn store(&self) -> SqliteRuntimeStore {
        SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: self.temp.path().join("runtime.sqlite3"),
        })
    }

    fn roster(&self) -> Vec<WorkspaceAgentRecord> {
        self.store()
            .list_workspace_agents(&self.workspace, None)
            .unwrap()
    }

    async fn native_counts(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.provider.create_call_count(),
            self.provider.archive_close_count(),
            self.provider.restore_call_count().await,
            self.provider.send_invocations.load(Ordering::SeqCst),
            self.provider.preferences_mutation_calls(),
        )
    }

    async fn set_policy(&mut self, add: bool, remove: bool) {
        let counts = self.native_counts().await;
        (self.router, self.runtime, self.app) = reopen_test_router_with_services(
            &self.app,
            self.temp.path(),
            &self.token,
            TeamMcpPolicy {
                enabled: true,
                non_lead_can_add_members: add,
                non_lead_can_remove_members: remove,
            },
        )
        .await;
        assert_eq!(self.native_counts().await, counts);
    }

    async fn invoke(&self, caller: &str, key: &str, args: serde_json::Value) -> serde_json::Value {
        let scoped = runtime_core::mint_scoped_mcp_token(&self.token, caller);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/mcp/invoke")
                    .header(header::AUTHORIZATION, format!("Bearer {scoped}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-gg-tool-invocation-id", key)
                    .body(Body::from(
                        serde_json::json!({
                            "tool_name": "gg_team", "args": args
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    async fn add(&self, caller: &str, key: &str, title: &str) -> String {
        let before = self.provider.create_call_count();
        let result = self
            .invoke(
                caller,
                key,
                serde_json::json!({
                    "mode": "add", "title": title
                }),
            )
            .await;
        assert_eq!(result["ok"], true, "{result}");
        let id = result["result"]["agent_id"].as_str().unwrap().to_owned();
        let member = self
            .store()
            .get_workspace_agent(&self.workspace, &id)
            .unwrap()
            .unwrap();
        assert_eq!(member.lifecycle_state, WorkspaceAgentLifecycleState::Active);
        assert_eq!(member.profile.creator_session_id.as_deref(), Some(caller));
        assert_eq!(self.provider.create_call_count(), before + 1);
        finish_test_onboarding(&self.runtime, &self.workspace, &id).await;
        id
    }

    async fn remove(&self, caller: &str, key: &str, target: &str) {
        let before = self.provider.archive_close_count();
        let result = self
            .invoke(
                caller,
                key,
                serde_json::json!({
                    "mode": "remove", "remove_agent_ids": [target]
                }),
            )
            .await;
        assert_eq!(result["ok"], true, "{result}");
        let archived = self
            .store()
            .get_workspace_agent(&self.workspace, target)
            .unwrap()
            .unwrap();
        assert_eq!(
            archived.lifecycle_state,
            WorkspaceAgentLifecycleState::Archived
        );
        assert_eq!(self.provider.archive_close_count(), before + 1);
    }

    async fn assert_denied(&self, caller: &str, key: &str, args: serde_json::Value) {
        let roster = self.roster();
        let counts = self.native_counts().await;
        let result = self.invoke(caller, key, args).await;
        assert_eq!(result["ok"], false, "{result}");
        assert_eq!(result["error"]["code"], "bad_request", "{result}");
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .contains("is not allowed to manage workspace"),
            "{result}"
        );
        assert_eq!(self.roster(), roster);
        assert_eq!(self.native_counts().await, counts);
    }
}

#[tokio::test]
async fn workspace_membership_mutations_follow_leadless_and_configured_non_lead_policy() {
    let mut fixture = MembershipFixture::new().await;
    let caller = fixture.caller.clone();
    let peer = fixture.peer.clone();
    let leadless = fixture.add(&caller, "leadless-add", "Leadless child").await;
    fixture.remove(&caller, "leadless-remove", &leadless).await;

    set_lead(
        &fixture.router,
        &fixture.token,
        &fixture.workspace,
        Some(&caller),
        0,
        "membership-lead",
    )
    .await;
    fixture
        .assert_denied(
            &peer,
            "default-denied-add",
            serde_json::json!({
                "mode": "add", "title": "Denied child"
            }),
        )
        .await;
    fixture
        .assert_denied(
            &peer,
            "default-denied-remove",
            serde_json::json!({
                "mode": "remove", "remove_agent_ids": [&caller]
            }),
        )
        .await;

    fixture.set_policy(true, false).await;
    let allowed = fixture.add(&peer, "configured-add", "Allowed child").await;
    fixture
        .assert_denied(
            &peer,
            "configured-denied-remove",
            serde_json::json!({
                "mode": "remove", "remove_agent_ids": [&allowed]
            }),
        )
        .await;
    fixture.set_policy(false, true).await;
    fixture.remove(&peer, "configured-remove", &allowed).await;
    assert_eq!(
        fixture
            .store()
            .get_workspace(&fixture.workspace)
            .unwrap()
            .unwrap()
            .lead_agent_id
            .as_deref(),
        Some(caller.as_str()),
    );
}

fn assert_raw_unsupported(result: Result<(), RuntimeError>) {
    assert!(
        matches!(result, Err(RuntimeError::Unsupported(_))),
        "{result:?}"
    );
}

#[tokio::test]
async fn raw_workspace_lifecycle_methods_refuse_before_lookup_or_native_effect() {
    let fixture = MembershipFixture::new().await;
    // Establish a real known archive receipt first, so refusal covers both a
    // valid active attachment and a valid already-archived attachment.
    let (status, archived) = call(
        &fixture.router,
        &fixture.token,
        "POST",
        &format!(
            "/v2/workspaces/{}/agents/{}/archive",
            fixture.workspace, fixture.peer
        ),
        Some(serde_json::json!({"reason": "raw boundary fixture"})),
        Some("raw-boundary-known-archive"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(archived["lifecycle_state"], "archived");
    let db = rusqlite::Connection::open(fixture.temp.path().join("runtime.sqlite3")).unwrap();
    let before = database_snapshot(&db);
    let counts = fixture.native_counts().await;
    let policy = runtime_core::WorkspaceMembershipPolicy {
        non_lead_can_add_members: true,
        non_lead_can_remove_members: true,
    };
    for (workspace, agent) in [
        (fixture.workspace.as_str(), fixture.caller.as_str()),
        (fixture.workspace.as_str(), fixture.peer.as_str()),
        ("ws_missing", "sess_missing"),
    ] {
        // Missing paths and missing IDs must still yield the immediate raw
        // API refusal; no normalization, ownership lookup or ACL precedes it.
        let request = membership_agent_request(
            &fixture.temp.path().join("does-not-exist"),
            "Raw unsupported",
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .create_workspace_agent(workspace, request.clone(), "raw-caller")
                .await
                .map(|_| ()),
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .mutate_workspace_agent_permission(
                    runtime_core::ProviderPermissionMutationRequest {
                        runtime_session_id: agent.into(),
                        expected_revision: Some(0),
                        permission_intent: runtime_core::ProviderPermissionIntent::Explicit {
                            mode: "acceptEdits".into(),
                        },
                    },
                )
                .await
                .map(|_| ()),
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .mutate_workspace_agent_preferences(
                    runtime_core::ProviderSessionPreferencesMutationRequest {
                        runtime_session_id: agent.into(),
                        expected_revision: Some(0),
                        current_preferences: runtime_core::ProviderSessionPreferences {
                            thinking_effort: Some(runtime_core::ProviderThinkingEffort::High),
                        },
                    },
                )
                .await
                .map(|_| ()),
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .archive_workspace_agent(workspace, agent, Some("raw"))
                .await
                .map(|_| ()),
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .archive_workspace_agent_with_initiator(workspace, agent, Some("raw"), Some(agent))
                .await
                .map(|_| ()),
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .restore_workspace_agent(workspace, agent)
                .await
                .map(|_| ()),
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .create_workspace_agent_as_member(workspace, request, agent, policy)
                .await
                .map(|_| ()),
        );
        assert_raw_unsupported(
            fixture
                .runtime
                .archive_workspace_agent_as_member(workspace, agent, agent, Some("raw"), policy)
                .await
                .map(|_| ()),
        );
        assert_eq!(database_snapshot(&db), before);
        assert_eq!(fixture.native_counts().await, counts);
    }
}
