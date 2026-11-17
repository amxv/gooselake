use super::*;
use runtime_core::{
    TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase, TeamHookEffectReceipt,
    TeamHookEffectRecord, TeamHookEventName, TeamHookResolvedEntry, TeamHookScope,
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub(super) struct InertHooks {
    store: Arc<SqliteRuntimeStore>,
    database: std::path::PathBuf,
    caller: String,
    pub(super) calls: AtomicUsize,
    action: u8,
}

impl InertHooks {
    fn revoke_caller(&self) {
        rusqlite::Connection::open(&self.database).unwrap().execute(
            "UPDATE workspace_agents SET lifecycle_state='archived',archived_at=1 WHERE session_id=?1",[&self.caller]).unwrap();
    }
}

#[async_trait::async_trait]
impl runtime_core::RuntimeTeamHookExecutor for InertHooks {
    async fn execute(
        &self,
        admission: &TeamHookEffectAdmission,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        if self.action == 6 {
            return Err(RuntimeError::Io("inert failure before shell claim".into()));
        }
        if self.action == 1 {
            self.revoke_caller();
        }
        self.store.claim_team_hook_effect(
            &admission.workspace_id,
            &admission.parent_event_id,
            admission.hook.event_name,
            &admission.hook.name,
            now(),
        )?;
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.action == 5 {
            return Err(RuntimeError::Io("inert lost shell acknowledgement".into()));
        }
        // No shell/provider is used: the counter stands for one inert native
        // script, reached only after the production SQLite claim succeeds.
        let receipt = TeamHookEffectReceipt {
            validated_output: Some(json!({
                "pre_teammate_add":{"onboarding_context_append_text":"private onboarding hook text"},
                "inject":{"caller":{"text":"private caller hook text"}}
            })),
            error_code: None,
        };
        let result = self.store.finish_team_hook_effect(
            &admission.workspace_id,
            &admission.parent_event_id,
            admission.hook.event_name,
            &admission.hook.name,
            TeamHookEffectPhase::Succeeded,
            receipt,
            now(),
        )?;
        if self.action == 2 && index == 0 {
            self.revoke_caller();
        }
        if self.action == 3 {
            self.store
                .update_team_hook_approval(&TeamHookApprovalUpdate {
                    workspace_id: admission.workspace_id.clone(),
                    expected_revision: 1,
                    hooks: Vec::new(),
                    approved_at_ms: now(),
                })?;
        }
        if self.action == 4 {
            #[cfg(unix)]
            {
                let root = std::path::PathBuf::from(
                    admission.input["subject"]["planned_cwd"].as_str().unwrap(),
                );
                let moved = root.with_extension("moved-for-fixture");
                std::fs::rename(&root, &moved).unwrap();
                std::os::unix::fs::symlink(&moved, &root).unwrap();
            }
        }
        if self.action == 7 {
            rusqlite::Connection::open(&self.database).unwrap().execute(
                "UPDATE workspace_hook_effects SET input_json=json_set(input_json,'$.subject.member_title','substituted title') WHERE workspace_id=?1 AND parent_event_id=?2",
                rusqlite::params![admission.workspace_id,admission.parent_event_id]).unwrap();
        }
        Ok(result)
    }
}

pub(super) async fn install(f: &Fixture, action: u8, count: usize) -> Arc<InertHooks> {
    let store = f.store();
    let hooks = (0..count)
        .map(|i| TeamHookResolvedEntry {
            event_name: TeamHookEventName::PreTeammateAdd,
            name: format!("fixture_{i}"),
            command: "inert fixture executor".into(),
            timeout_ms: 1000,
            scope: TeamHookScope::Project,
        })
        .collect();
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: f.workspace.clone(),
            expected_revision: 0,
            hooks,
            approved_at_ms: now(),
        })
        .unwrap();
    let executor = Arc::new(InertHooks {
        store,
        database: f.temp.path().join("runtime.sqlite3"),
        caller: f.caller.clone(),
        calls: AtomicUsize::new(0),
        action,
    });
    f.runtime
        .install_pre_add_hook_executor(executor.clone())
        .await;
    executor
}

fn revoke_after_launch(f: &Fixture, caller: bool, grant: bool) {
    if caller {
        f.db().execute("UPDATE workspace_agents SET lifecycle_state='archived',archived_at=1 WHERE session_id=?1",[&f.caller]).unwrap();
    }
    if grant {
        f.store()
            .update_team_hook_approval(&TeamHookApprovalUpdate {
                workspace_id: f.workspace.clone(),
                expected_revision: 1,
                hooks: Vec::new(),
                approved_at_ms: now(),
            })
            .unwrap();
    }
}

pub(super) async fn review(f: &Fixture, child: &str, reserved: &str, created: bool) -> Value {
    let (status,result)=super::super::workspace_worktree_routes::call(&f.router,&f.token,"POST",
        &format!("/v2/workspaces/{}/agents/creations/{child}/resolve",f.workspace),Some(json!({
            "resolution":if created{"created"}else{"not_created"},"reserved_agent_id":reserved,"provider":"codex",
            "evidence":"Independent inert-provider inspection of the exact reserved attachment",
            "observed_provider_session_ref":created.then(||format!("test-thread-{reserved}")),
            "observed_canonical_provider_session_ref":null,
        })),None).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    result
}

#[tokio::test]
async fn member_hook_each_claim_and_provider_launch_recheck_current_authority() {
    for (action, count, expected_calls) in [(1, 2, 0), (2, 2, 1), (3, 1, 1)] {
        let f = Fixture::new().await;
        let hooks = install(&f, action, count).await;
        let baseline = f.provider.create_call_count();
        let result = f.add(&format!("member-hook-stage-{action}")).await;
        assert_eq!(result["ok"], false, "{result}");
        assert_eq!(hooks.calls.load(Ordering::SeqCst), expected_calls);
        assert_eq!(
            f.provider.create_call_count(),
            baseline,
            "revoked stage must not launch a provider"
        );
        assert_eq!(
            result["error"]["details"]["targets"][0]["status"],
            "outcome_unresolved"
        );
        let started: i64 = f
            .db()
            .query_row(
                "SELECT COUNT(*) FROM workspace_hook_effects WHERE attempt_count=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(started, expected_calls as i64);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn member_hook_path_replacement_is_rejected_before_provider_launch() {
    let f = Fixture::new().await;
    let hooks = install(&f, 4, 1).await;
    let baseline = f.provider.create_call_count();
    let result = f.add("member-hook-route-replacement").await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.provider.create_call_count(), baseline);
    assert_eq!(
        result["error"]["details"]["targets"][0]["status"],
        "outcome_unresolved"
    );
}

#[tokio::test]
async fn member_hook_substituted_input_cannot_authorize_provider_launch() {
    let f = Fixture::new().await;
    let hooks = install(&f, 7, 1).await;
    let baseline = f.provider.create_call_count();
    let result = f.add("substituted-hook-input").await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.provider.create_call_count(), baseline);
}

#[tokio::test]
async fn member_hook_substituted_receipt_cannot_settle_admitted_provider_or_review() {
    let f = Arc::new(Fixture::new().await);
    let hooks = install(&f, 0, 1).await;
    let baseline = f.provider.create_call_count();
    f.provider.allow_lifecycle_close_for_test();
    let (entered, release) = f.provider.hold_next_create();
    let worker = f.clone();
    let task = tokio::spawn(async move { worker.add("substituted-frozen-hook-receipt").await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    f.db().execute("UPDATE workspace_hook_effects SET receipt_json=json_set(receipt_json,'$.validated_output.inject.caller.text','substituted output') WHERE event_name='pre_teammate_add'",[]).unwrap();
    release.notify_one();
    let result = task.await.unwrap();
    assert_eq!(result["ok"], false, "{result}");
    let target = &result["error"]["details"]["targets"][0];
    let reserved = target["agent_id"].as_str().unwrap();
    let child = target["operation_id"].as_str().unwrap();
    assert!(f.provider.native_cwd(reserved).await.is_some());
    assert!(f.store().get_persisted_session(reserved).unwrap().is_none());
    let(status,reviewed)=super::super::workspace_worktree_routes::call(&f.router,&f.token,"POST",
        &format!("/v2/workspaces/{}/agents/creations/{child}/resolve",f.workspace),Some(json!({
            "resolution":"created","reserved_agent_id":reserved,"provider":"codex",
            "evidence":"Independent exact fake attachment, but durable hook history was substituted",
            "observed_provider_session_ref":format!("test-thread-{reserved}"),"observed_canonical_provider_session_ref":null})),None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{reviewed}");
    assert!(f.store().get_persisted_session(reserved).unwrap().is_none());
    assert_eq!(f.provider.create_call_count(), baseline + 1);
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn member_native_receipt_and_created_review_settle_after_caller_and_grant_revocation() {
    for lost_ack in [false, true] {
        for (caller, grant) in [(true, false), (false, true)] {
            let f = Arc::new(Fixture::new().await);
            let hooks = install(&f, 0, 1).await;
            let baseline = f.provider.create_call_count();
            f.provider
                .set_create_failure_mode(if lost_ack { 2 } else { 0 });
            let (entered, release) = f.provider.hold_next_create();
            let task_f = f.clone();
            let task =
                tokio::spawn(async move { task_f.add("member-after-provider-revocation").await });
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            revoke_after_launch(&f, caller, grant);
            release.notify_one();
            let result = task.await.unwrap();
            let (child, reserved) = if lost_ack {
                assert_eq!(result["ok"], false, "{result}");
                let target = &result["error"]["details"]["targets"][0];
                let child = target["operation_id"].as_str().unwrap().to_owned();
                let reserved = target["agent_id"].as_str().unwrap().to_owned();
                let reviewed = review(&f, &child, &reserved, true).await;
                assert_eq!(reviewed["restart_required"], true);
                assert_eq!(review(&f, &child, &reserved, true).await, reviewed);
                (child, reserved)
            } else {
                assert_eq!(result["ok"], true, "{result}");
                let reserved = result["result"]["agent_id"].as_str().unwrap().to_owned();
                let child=f.db().query_row("SELECT source_operation_id FROM workspace_session_ownership WHERE session_id=?1",[&reserved],|r|r.get::<_,String>(0)).unwrap();
                (child, reserved)
            };
            let (added_by,creator):(String,String)=f.db().query_row("SELECT added_by,creator_session_id FROM workspace_agent_profiles WHERE session_id=?1",[&reserved],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
            assert_eq!((added_by, creator), (f.caller.clone(), f.caller.clone()));
            let notice: String = f
                .db()
                .query_row(
                    "SELECT input_json FROM workspace_system_notices WHERE notice_id=?1",
                    [format!("workspace-onboarding:{reserved}")],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(!notice.contains("private onboarding hook text"));
            let caller_context:i64=f.db().query_row("SELECT COUNT(*) FROM workspace_system_notices WHERE recipient_agent_id=?1 AND input_json LIKE '%private caller hook text%'",[&f.caller],|r|r.get(0)).unwrap();
            assert_eq!(caller_context, 0);
            let (phase,application,reason):(String,String,String)=f.db().query_row("SELECT phase,application_state,application_error_code FROM workspace_hook_effects WHERE event_name='pre_teammate_add'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
            assert_eq!(
                (phase.as_str(), application.as_str(), reason.as_str()),
                (
                    "succeeded",
                    "rejected",
                    "member_create_context_authority_revoked"
                )
            );
            assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.provider.create_call_count(), baseline + 1);
            assert!(f.provider.native_cwd(&reserved).await.is_some());
            let actor: String = f
                .db()
                .query_row(
                    "SELECT actor_kind FROM runtime_operations WHERE operation_id=?1",
                    [&child],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(actor, "agent");
            if lost_ack {
                f.provider.set_create_failure_mode(0);
                let fresh = reopened_runtime(&f.temp, f.provider.clone()).await;
                fresh.recover_startup().await.unwrap();
                assert_eq!(fresh.get_session(&reserved).await.unwrap().status, "ready");
                assert_eq!(f.provider.create_call_count(), baseline + 1);
            }
        }
    }
}

#[tokio::test]
async fn member_not_created_review_rejects_context_without_reexecuting_hooks() {
    let f = Fixture::new().await;
    let hooks = install(&f, 0, 1).await;
    let baseline = f.provider.create_call_count();
    f.provider.set_create_failure_mode(4);
    let result = f.add("member-review-not-created").await;
    assert_eq!(result["ok"], false, "{result}");
    let target = &result["error"]["details"]["targets"][0];
    let child = target["operation_id"].as_str().unwrap();
    let reserved = target["agent_id"].as_str().unwrap();
    revoke_after_launch(&f, true, true);
    let reviewed = review(&f, child, reserved, false).await;
    assert_eq!(reviewed["restart_required"], false);
    assert_eq!(review(&f, child, reserved, false).await, reviewed);
    let (phase,state,claims,agents):(String,String,i64,i64)=f.db().query_row(
        "SELECT phase,application_state,(SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1),(SELECT COUNT(*) FROM sessions WHERE id=?2) FROM workspace_hook_effects WHERE event_name='pre_teammate_add'",
        rusqlite::params![child,reserved],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(
        (phase.as_str(), state.as_str(), claims, agents),
        ("succeeded", "rejected", 0, 0)
    );
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.provider.create_call_count(), baseline + 1);
    assert!(f.provider.native_cwd(reserved).await.is_none());
}

#[tokio::test]
async fn member_late_critical_receipt_fault_never_compensates_native_create() {
    let f = Fixture::new().await;
    let hooks = install(&f, 0, 1).await;
    let baseline = f.provider.create_call_count();
    f.provider.allow_lifecycle_close_for_test();
    f.db().execute_batch("CREATE TRIGGER fail_member_create_receipt BEFORE INSERT ON runtime_events WHEN NEW.kind='model_tool.create_target_updated' AND json_extract(NEW.payload_json,'$.status')='native_create_confirmed' BEGIN SELECT RAISE(ABORT,'fixture late member create receipt failure'); END;").unwrap();
    let result = f.add("member-late-create-receipt").await;
    assert_eq!(result["ok"], false, "{result}");
    let target = &result["error"]["details"]["targets"][0];
    let child = target["operation_id"].as_str().unwrap();
    let reserved = target["agent_id"].as_str().unwrap();
    assert!(
        f.provider.native_cwd(reserved).await.is_some(),
        "a failed SQLite receipt must not close the created native session"
    );
    let (agents,claims):(i64,i64)=f.db().query_row("SELECT (SELECT COUNT(*) FROM sessions WHERE id=?1),(SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?2)",rusqlite::params![reserved,child],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!((agents, claims), (0, 1));
    let (phase,application,notices):(String,String,i64)=f.db().query_row("SELECT phase,application_state,(SELECT COUNT(*) FROM workspace_system_notices WHERE recipient_agent_id=?1 OR input_json LIKE '%private caller hook text%') FROM workspace_hook_effects WHERE event_name='pre_teammate_add'",[reserved],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(
        (phase.as_str(), application.as_str(), notices),
        ("succeeded", "pending", 0)
    );
    f.db()
        .execute_batch("DROP TRIGGER fail_member_create_receipt")
        .unwrap();
    assert_eq!(
        f.add("member-late-create-receipt").await["error"]["details"],
        result["error"]["details"]
    );
    review(&f, child, reserved, true).await;
    assert_eq!(f.provider.create_call_count(), baseline + 1);
    assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
    let application:String=f.db().query_row("SELECT application_state FROM workspace_hook_effects WHERE event_name='pre_teammate_add'",[],|r|r.get(0)).unwrap();
    assert_eq!(application, "applied");
}
