//! Actual operator HTTP and scoped MCP ingress; native hooks are inert.
use super::*;
use runtime_core::{
    legacy_spawn_obligations::LegacyNativeRecovery, RuntimeStore, RuntimeTeamHookExecutor,
    TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase, TeamHookEffectReceipt,
    TeamHookEffectRecord, TeamHookEventName, TeamHookResolvedEntry, TeamHookScope,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

const ORIGINAL: &str = "historical/create ?#%é";
const ORIGINAL_TEAM: &str = "historical-create-team";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boundary {
    FirstHook,
    ProviderFreeze,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct LateHistoryHooks {
    store: Arc<SqliteRuntimeStore>,
    database: PathBuf,
    boundary: Boundary,
    entries: AtomicUsize,
    executions: AtomicUsize,
}
impl LateHistoryHooks {
    fn inject(&self) {
        rusqlite::Connection::open(&self.database)
            .unwrap()
            .execute(
                "INSERT INTO team_operation_journal
             (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
             VALUES(?1,?2,'spawn_member_with_worktree','session_created',?3,10,20)",
                rusqlite::params![
                    ORIGINAL,
                    ORIGINAL_TEAM,
                    json!({"spawned_session_id":"positively-different-historical-child"})
                        .to_string()
                ],
            )
            .unwrap();
    }
}
#[async_trait::async_trait]
impl RuntimeTeamHookExecutor for LateHistoryHooks {
    async fn execute(
        &self,
        admission: &TeamHookEffectAdmission,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        self.entries.fetch_add(1, Ordering::SeqCst);
        if self.boundary == Boundary::FirstHook {
            self.inject();
        }
        self.store.claim_team_hook_effect(
            &admission.workspace_id,
            &admission.parent_event_id,
            admission.hook.event_name,
            &admission.hook.name,
            now(),
        )?;
        // An inert effect is counted only after the real first-claim authority.
        self.executions.fetch_add(1, Ordering::SeqCst);
        let result = self.store.finish_team_hook_effect(
            &admission.workspace_id,
            &admission.parent_event_id,
            admission.hook.event_name,
            &admission.hook.name,
            TeamHookEffectPhase::Succeeded,
            TeamHookEffectReceipt {
                validated_output: Some(json!({})),
                error_code: None,
            },
            now(),
        )?;
        if self.boundary == Boundary::ProviderFreeze {
            self.inject();
        }
        Ok(result)
    }
}
async fn install(f: &Fixture, boundary: Boundary) -> Arc<LateHistoryHooks> {
    let store = f.store();
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: f.workspace.clone(),
            expected_revision: 0,
            hooks: vec![TeamHookResolvedEntry {
                event_name: TeamHookEventName::PreTeammateAdd,
                name: "legacy_create_boundary".into(),
                command: "inert fixture executor".into(),
                timeout_ms: 1000,
                scope: TeamHookScope::Project,
            }],
            approved_at_ms: now(),
        })
        .unwrap();
    let hooks = Arc::new(LateHistoryHooks {
        store,
        database: f.temp.path().join("runtime.sqlite3"),
        boundary,
        entries: AtomicUsize::new(0),
        executions: AtomicUsize::new(0),
    });
    f.runtime.install_pre_add_hook_executor(hooks.clone()).await;
    hooks
}
async fn reopen(f: &mut Fixture, hooks: Arc<LateHistoryHooks>) -> Arc<runtime_core::RuntimeApp> {
    let store: Arc<dyn RuntimeStore> = f.store();
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(f.provider.clone()).unwrap();
    let (router, runtime, app) = assemble_test_router(
        f.temp.path(),
        store,
        Arc::new(providers),
        &f.token,
        TeamMcpPolicy::default(),
    )
    .await;
    // Reinstall before replay; a regression must never run the default shell.
    runtime.install_pre_add_hook_executor(hooks).await;
    f.router = router;
    f.runtime = runtime;
    app
}
fn remove_history(f: &Fixture) {
    assert_eq!(
        f.db()
            .execute(
                "DELETE FROM team_operation_journal WHERE operation_id=?1",
                [ORIGINAL]
            )
            .unwrap(),
        1
    );
}
fn assert_owner(details: &Value, owner: &str) {
    assert_eq!(details["operation_id"], owner);
    assert_eq!(
        details["inspection_path"],
        runtime_core::error::canonical_operation_inspection_path(owner)
    );
    assert_eq!(details["native_retry_authorized"], false);
}
fn assert_original(details: &Value) {
    assert_owner(details, details["operation_id"].as_str().unwrap());
    let recovery = &details["legacy_recovery"];
    assert_eq!(recovery["kind"], "blocked", "{details}");
    assert_eq!(
        recovery["coordinate"]["operation_id"], ORIGINAL,
        "{details}"
    );
    assert_eq!(
        recovery["coordinate"]["team_id"], ORIGINAL_TEAM,
        "{details}"
    );
    assert_eq!(recovery["coordinate"]["stage"], "session_created");
    assert_eq!(recovery["reason"], "unknown_checkout_scope");
    assert_eq!(
        recovery["inspection_path"],
        runtime_core::error::legacy_team_operation_inspection_path(ORIGINAL)
    );
    assert_eq!(recovery["native_retry_authorized"], false);
    assert_eq!(details["native_retry_authorized"], false);
}
fn verified(f: &Fixture, owner: &str, kind: &str, reserved: &str) -> LegacyNativeRecovery {
    let store = f.store();
    let details = store.get_operation(owner).unwrap().unwrap();
    let effect = details
        .effects
        .iter()
        .find(|e| e.effect_kind == kind && e.target_id == reserved)
        .unwrap();
    store
        .read_operation_effect_legacy_recovery(owner, &effect.effect_id, kind, reserved)
        .unwrap()
        .unwrap()
}
fn observed(f: &Fixture, owner: &str, kind: &str, reserved: &str) -> Option<LegacyNativeRecovery> {
    let store = f.store();
    let details = store.get_operation(owner).unwrap().unwrap();
    let effect = details
        .effects
        .iter()
        .find(|e| e.effect_kind == kind && e.target_id == reserved)
        .unwrap();
    store
        .read_operation_effect_legacy_recovery(owner, &effect.effect_id, kind, reserved)
        .unwrap()
}
fn reserved_for(f: &Fixture, child: &str) -> String {
    f.db().query_row("SELECT target_id FROM runtime_operation_effects WHERE operation_id=?1 AND effect_kind='provider_session_create'",
        [child], |r| r.get(0)).unwrap()
}
fn operation(f: &Fixture, id: &str) -> Value {
    serde_json::to_value(f.store().get_operation(id).unwrap().unwrap()).unwrap()
}
fn hook(f: &Fixture, reserved: &str) -> TeamHookEffectRecord {
    f.store()
        .get_team_hook_effect(
            &f.workspace,
            &format!("workspace-hook:pre-add:{reserved}"),
            TeamHookEventName::PreTeammateAdd,
            "legacy_create_boundary",
        )
        .unwrap()
        .unwrap()
}
fn assert_child_retained(f: &Fixture, child: &str, reserved: &str) {
    let counts: (i64, i64) = f.db().query_row(
        "SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1
         AND resource_kind='operator_agent_create_native' AND resource_id=?2 AND claim_mode='exclusive'),
         (SELECT COUNT(*) FROM sessions WHERE id=?2)",
        rusqlite::params![child, reserved], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(counts, (1, 0));
}
fn assert_counts(f: &Fixture, hooks: &LateHistoryHooks, baseline: usize) {
    assert_eq!(f.provider.create_call_count(), baseline);
    assert_eq!(hooks.entries.load(Ordering::SeqCst), 1);
    assert_eq!(
        hooks.executions.load(Ordering::SeqCst),
        usize::from(hooks.boundary == Boundary::ProviderFreeze)
    );
}
fn observation_row(f: &Fixture, owner: &str) -> Option<(String, i64, String, String, i64)> {
    use rusqlite::OptionalExtension;
    f.db().query_row("SELECT payload_json,created_at,scope,scope_id,critical FROM runtime_events WHERE kind='operation.legacy_native_recovery_observed' AND json_extract(payload_json,'$.operation_id')=?1", [owner], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().unwrap()
}
fn observation_count(f: &Fixture) -> i64 {
    f.db().query_row("SELECT COUNT(*) FROM runtime_events WHERE kind='operation.legacy_native_recovery_observed'", [], |r|r.get(0)).unwrap()
}
fn fault(f: &Fixture, child_only: bool) {
    let predicate = if child_only {
        "AND EXISTS (SELECT 1 FROM runtime_operations owner WHERE owner.operation_id=json_extract(NEW.payload_json,'$.operation_id') AND owner.kind='operator_agent_create')"
    } else {
        ""
    };
    f.db()
        .execute_batch(&format!(
            "CREATE TRIGGER refuse_create_legacy_observation BEFORE INSERT ON runtime_events
        WHEN NEW.kind='operation.legacy_native_recovery_observed' {predicate}
        BEGIN SELECT RAISE(ABORT,'injected create observation fault'); END;"
        ))
        .unwrap();
}
fn input() -> Value {
    json!({"provider":"codex","model":"test-model","title":"Legacy boundary operator"})
}

#[tokio::test]
async fn operator_create_legacy_boundary_replays_verified_original_after_restart() {
    for boundary in [Boundary::FirstHook, Boundary::ProviderFreeze] {
        let mut f = Fixture::new().await;
        let hooks = install(&f, boundary).await;
        let baseline = f.provider.create_call_count();
        let key = format!("operator-legacy-{boundary:?}");
        let (status, first, _) =
            create_http(&f.router, &f.token, &f.workspace, input(), Some(&key)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{first}");
        assert_eq!(first["error"]["code"], "outcome_unresolved");
        assert_original(&first["error"]["details"]);
        let child = first["error"]["details"]["operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let reserved = reserved_for(&f, &child);
        assert_ne!(child, ORIGINAL);
        assert_child_retained(&f, &child, &reserved);
        assert_eq!(
            verified(&f, &child, "provider_session_create", &reserved).error_details(),
            first["error"]["details"]["legacy_recovery"]
        );
        assert_counts(&f, &hooks, baseline);
        assert!(f.provider.native_cwd(&reserved).await.is_none());
        let before = operation(&f, &child);
        let saved_hook = hook(&f, &reserved);
        assert_eq!(
            saved_hook.phase,
            if boundary == Boundary::FirstHook {
                TeamHookEffectPhase::Intended
            } else {
                TeamHookEffectPhase::Succeeded
            }
        );
        assert_eq!(
            saved_hook.attempt_count,
            u32::from(boundary == Boundary::ProviderFreeze)
        );
        remove_history(&f);
        reopen(&mut f, hooks.clone()).await;
        let (status, replay, _) =
            create_http(&f.router, &f.token, &f.workspace, input(), Some(&key)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{replay}");
        assert_original(&replay["error"]["details"]);
        assert_owner(&replay["error"]["details"], &child);
        assert_eq!(
            replay["error"]["details"]["legacy_recovery"],
            first["error"]["details"]["legacy_recovery"]
        );
        assert_eq!(operation(&f, &child), before);
        assert_eq!(hook(&f, &reserved), saved_hook);
        assert_child_retained(&f, &child, &reserved);
        assert_counts(&f, &hooks, baseline);
    }
}

#[tokio::test]
async fn member_add_legacy_boundary_keeps_parent_and_child_causes_after_restart() {
    for boundary in [Boundary::FirstHook, Boundary::ProviderFreeze] {
        let mut f = Fixture::new().await;
        let hooks = install(&f, boundary).await;
        let baseline = f.provider.create_call_count();
        let key = format!("member-legacy-{boundary:?}");
        let first = f.add(&key).await;
        assert_eq!(first["ok"], false, "{first}");
        assert_original(&first["error"]["details"]);
        let parent = f.parent(&key);
        assert_owner(&first["error"]["details"], &parent);
        assert_eq!(
            first["error"]["details"]["targets"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let target = &first["error"]["details"]["targets"][0];
        let child = target["operation_id"].as_str().unwrap().to_owned();
        let reserved = target["agent_id"].as_str().unwrap().to_owned();
        assert_ne!(parent, child);
        assert_ne!(child, ORIGINAL);
        assert_ne!(parent, ORIGINAL);
        assert_child_retained(&f, &child, &reserved);
        for (owner, kind) in [
            (parent.as_str(), "model_member_create"),
            (child.as_str(), "provider_session_create"),
        ] {
            assert_eq!(
                verified(&f, owner, kind, &reserved).error_details(),
                first["error"]["details"]["legacy_recovery"]
            );
            let actor: (String, String) = f
                .db()
                .query_row(
                    "SELECT actor_kind,actor_id FROM runtime_operations WHERE operation_id=?1",
                    [owner],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(actor, ("agent".into(), f.caller.clone()));
        }
        let saved = (
            operation(&f, &parent),
            operation(&f, &child),
            hook(&f, &reserved),
        );
        assert_counts(&f, &hooks, baseline);
        remove_history(&f);
        reopen(&mut f, hooks.clone()).await;
        let replay = f.add(&key).await;
        assert_eq!(replay["ok"], false, "{replay}");
        assert_original(&replay["error"]["details"]);
        assert_owner(&replay["error"]["details"], &parent);
        assert_eq!(
            replay["error"]["details"]["targets"][0]["operation_id"],
            child
        );
        assert_eq!(
            replay["error"]["details"]["targets"][0]["agent_id"],
            reserved
        );
        assert_eq!(
            replay["error"]["details"]["recorded_legacy_recoveries"][0],
            first["error"]["details"]["legacy_recovery"]
        );
        assert_eq!(
            (
                operation(&f, &parent),
                operation(&f, &child),
                hook(&f, &reserved)
            ),
            saved
        );
        assert_child_retained(&f, &child, &reserved);
        assert_counts(&f, &hooks, baseline);
    }
}

#[tokio::test]
async fn operator_create_legacy_observation_failure_rolls_back_pair_and_does_not_retry() {
    let mut f = Fixture::new().await;
    let hooks = install(&f, Boundary::ProviderFreeze).await;
    let baseline = f.provider.create_call_count();
    fault(&f, false);
    let key = "operator-legacy-observation-fault";
    let (status, first, _) =
        create_http(&f.router, &f.token, &f.workspace, input(), Some(key)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{first}");
    assert_original(&first["error"]["details"]);
    assert!(
        first["error"]["message"]
            .as_str()
            .unwrap()
            .contains("could not be persisted"),
        "{first}"
    );
    let child = first["error"]["details"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let reserved = reserved_for(&f, &child);
    assert_child_retained(&f, &child, &reserved);
    assert_eq!(
        observed(&f, &child, "provider_session_create", &reserved),
        None
    );
    assert_eq!(observation_count(&f), 0);
    assert_eq!(
        f.store()
            .get_operation(&child)
            .unwrap()
            .unwrap()
            .operation
            .phase,
        runtime_core::OperationPhase::Requested
    );
    let saved = operation(&f, &child);
    remove_history(&f);
    reopen(&mut f, hooks.clone()).await;
    let (status, replay, _) =
        create_http(&f.router, &f.token, &f.workspace, input(), Some(key)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{replay}");
    assert_owner(&replay["error"]["details"], &child);
    assert!(
        replay["error"]["details"].get("legacy_recovery").is_none(),
        "{replay}"
    );
    assert_eq!(operation(&f, &child), saved);
    assert_eq!(observation_count(&f), 0);
    assert_counts(&f, &hooks, baseline);
}

#[tokio::test]
async fn operator_create_replay_rejects_deleted_or_mismatched_saved_pair() {
    for delete in [false, true] {
        let mut f = Fixture::new().await;
        let hooks = install(&f, Boundary::ProviderFreeze).await;
        let baseline = f.provider.create_call_count();
        let key = "operator-legacy-damaged-pair";
        let (status, first, _) =
            create_http(&f.router, &f.token, &f.workspace, input(), Some(key)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{first}");
        assert_original(&first["error"]["details"]);
        let child = first["error"]["details"]["operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let reserved = reserved_for(&f, &child);
        let original = verified(&f, &child, "provider_session_create", &reserved);
        let sql = if delete {
            "DELETE FROM runtime_events WHERE kind='operation.legacy_native_recovery_observed' AND json_extract(payload_json,'$.operation_id')=?1"
        } else {
            "UPDATE runtime_events SET created_at=created_at+1 WHERE kind='operation.legacy_native_recovery_observed' AND json_extract(payload_json,'$.operation_id')=?1"
        };
        assert_eq!(f.db().execute(sql, [&child]).unwrap(), 1);
        let events = observation_count(&f);
        let saved_event = observation_row(&f, &child);
        let saved = operation(&f, &child);
        remove_history(&f);
        reopen(&mut f, hooks.clone()).await;
        let (status, replay, _) =
            create_http(&f.router, &f.token, &f.workspace, input(), Some(key)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{replay}");
        assert_owner(&replay["error"]["details"], &child);
        assert_eq!(replay["error"]["details"]["native_retry_authorized"], false);
        assert!(
            replay["error"]["details"].get("legacy_recovery").is_none(),
            "{replay}"
        );
        assert!(
            replay["error"]["message"]
                .as_str()
                .unwrap()
                .contains("original recovery observation inspection unavailable"),
            "{replay}"
        );
        let store = f.store();
        let details = store.get_operation(&child).unwrap().unwrap();
        let effect = details
            .effects
            .iter()
            .find(|e| e.effect_kind == "provider_session_create")
            .unwrap();
        assert!(store
            .read_operation_effect_legacy_recovery(
                &child,
                &effect.effect_id,
                &effect.effect_kind,
                &reserved
            )
            .is_err());
        assert_eq!(
            LegacyNativeRecovery::from_effect_evidence(effect.evidence.as_ref().unwrap()).unwrap(),
            Some(original)
        );
        assert_eq!(operation(&f, &child), saved);
        assert_eq!(observation_count(&f), events);
        assert_eq!(observation_row(&f, &child), saved_event);
        assert_counts(&f, &hooks, baseline);
    }
}

#[tokio::test]
async fn member_create_observation_fault_preserves_exact_parent_child_boundary() {
    for child_only in [false, true] {
        let mut f = Fixture::new().await;
        let hooks = install(&f, Boundary::ProviderFreeze).await;
        let baseline = f.provider.create_call_count();
        fault(&f, child_only);
        let key = "member-legacy-pair-fault";
        let first = f.add(key).await;
        assert_eq!(first["ok"], false, "{first}");
        assert_original(&first["error"]["details"]);
        let parent = f.parent(key);
        assert_owner(&first["error"]["details"], &parent);
        assert_eq!(
            first["error"]["details"]["targets"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let target = &first["error"]["details"]["targets"][0];
        let child = target["operation_id"].as_str().unwrap().to_owned();
        let reserved = target["agent_id"].as_str().unwrap().to_owned();
        assert_child_retained(&f, &child, &reserved);
        assert_eq!(
            observed(&f, &child, "provider_session_create", &reserved),
            None
        );
        if child_only {
            assert_eq!(
                verified(&f, &parent, "model_member_create", &reserved).error_details(),
                first["error"]["details"]["legacy_recovery"]
            );
        } else {
            assert_eq!(
                observed(&f, &parent, "model_member_create", &reserved),
                None
            );
        }
        assert_eq!(observation_count(&f), i64::from(child_only));
        let saved = (operation(&f, &parent), operation(&f, &child));
        remove_history(&f);
        reopen(&mut f, hooks.clone()).await;
        let replay = f.add(key).await;
        assert_eq!(replay["ok"], false, "{replay}");
        assert_owner(&replay["error"]["details"], &parent);
        if child_only {
            assert_original(&replay["error"]["details"]);
        } else {
            assert_eq!(
                replay["error"]["details"]["legacy_recovery_inspection"],
                "not_recorded"
            );
            assert!(
                replay["error"]["details"].get("legacy_recovery").is_none(),
                "{replay}"
            );
        }
        assert_eq!((operation(&f, &parent), operation(&f, &child)), saved);
        assert_eq!(observation_count(&f), i64::from(child_only));
        assert_child_retained(&f, &child, &reserved);
        assert_counts(&f, &hooks, baseline);
    }
}

#[tokio::test]
async fn member_admission_refusal_preserves_parent_before_child_exists() {
    use runtime_core::{
        AgentToolMutationAdmission, AgentToolMutationCommand, MemberAgentCreateRoute,
        WorkspaceAgentCreateRequest, WorkspaceAgentOnboardingRequest, WorkspaceMembershipPolicy,
    };
    let mut f = Fixture::new().await;
    let hooks = install(&f, Boundary::FirstHook).await;
    let app = reopen(&mut f, hooks.clone()).await;
    let baseline = f.provider.create_call_count();
    let key = "member-pre-child-legacy";
    let normalized = json!({"tool_name":"gg_team","args":{"mode":"add","title":"New colleague","prompt":"Inspect the fixture"}});
    let parent = AgentToolMutationCommand {
        operation_id: "op_fixture_pre_child_legacy".into(),
        workspace_id: f.workspace.clone(),
        caller_agent_id: f.caller.clone(),
        idempotency_key: key.into(),
        tool_name: "gg_team".into(),
        normalized_request_hash: runtime_core::normalized_json_hash(&normalized).unwrap(),
        normalized_request: normalized,
        admitted_at: now(),
    };
    assert!(matches!(
        f.store().begin_agent_tool_mutation(&parent).unwrap(),
        AgentToolMutationAdmission::Execute { .. }
    ));
    let workspace = f.store().get_workspace(&f.workspace).unwrap().unwrap();
    let route = MemberAgentCreateRoute::Root {
        canonical_root: workspace.canonical_root,
    };
    let policy = f
        .runtime
        .get_workspace_agent(&f.workspace, &f.caller)
        .unwrap()
        .recreation_policy;
    let request = WorkspaceAgentCreateRequest {
        provider: policy.provider,
        model: policy.model,
        permission_intent: policy.permission_intent,
        setting_sources_intent: policy.setting_sources_intent,
        current_preferences: policy.current_preferences,
        system_prompt: policy.system_prompt,
        allowed_tools: policy.allowed_tools,
        disallowed_tools: policy.disallowed_tools,
        cwd: None,
        worktree: Some(route.request_route()),
        harness_version_slot: policy.harness_version_slot,
        title: Some("New colleague".into()),
        metadata: None,
        onboarding: Some(WorkspaceAgentOnboardingRequest {
            prompt: Some("Inspect the fixture".into()),
            image_paths: vec![],
            skill: None,
            creator_compaction_subscription: None,
        }),
    };
    let command = f
        .runtime
        .plan_workspace_agent_create_as_member(
            &f.workspace,
            &f.caller,
            &parent.operation_id,
            request,
            None,
            route,
            WorkspaceMembershipPolicy::default(),
        )
        .unwrap();
    let child = command.operation.operation_id.clone();
    let reserved = command.operation.reserved_agent_id.clone();
    assert!(f.store().get_operation(&child).unwrap().is_none());
    f.db()
        .execute(
            "INSERT INTO team_operation_journal
        (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES(?1,?2,'spawn_member_with_worktree','session_created',?3,10,20)",
            rusqlite::params![
                ORIGINAL,
                ORIGINAL_TEAM,
                json!({"spawned_session_id":reserved}).to_string()
            ],
        )
        .unwrap();
    let error = f
        .runtime
        .execute_workspace_agent_create_as_member(command, app.services.worktrees.as_ref())
        .await
        .unwrap_err();
    let recovery = error.legacy_native_recovery().unwrap();
    assert_eq!(recovery.error_details()["reason"], "exact_session");
    assert_eq!(
        recovery.error_details()["coordinate"]["operation_id"],
        ORIGINAL
    );
    assert_eq!(
        verified(&f, &parent.operation_id, "model_member_create", &reserved),
        recovery
    );
    assert!(f.store().get_operation(&child).unwrap().is_none());
    let absent: (i64, i64) = f
        .db()
        .query_row(
            "SELECT
        (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1),
        (SELECT COUNT(*) FROM sessions WHERE id=?2)",
            rusqlite::params![child, reserved],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(absent, (0, 0));
    let parent_details = f
        .store()
        .get_operation(&parent.operation_id)
        .unwrap()
        .unwrap();
    let effect = parent_details
        .effects
        .iter()
        .find(|e| e.effect_kind == "model_member_create")
        .unwrap();
    assert_eq!(effect.phase, "intended");
    assert_eq!(effect.attempt_count, 0);
    assert_eq!(f.provider.create_call_count(), baseline);
    assert_eq!(hooks.entries.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.executions.load(Ordering::SeqCst), 0);
    assert_eq!(observation_count(&f), 1);
    let saved = operation(&f, &parent.operation_id);
    remove_history(&f);
    reopen(&mut f, hooks.clone()).await;
    let replay = f.add(key).await;
    assert_eq!(replay["ok"], false, "{replay}");
    assert_owner(&replay["error"]["details"], &parent.operation_id);
    assert_eq!(
        replay["error"]["details"]["legacy_recovery"],
        recovery.error_details()
    );
    assert_eq!(
        replay["error"]["details"]["targets"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replay["error"]["details"]["targets"][0]["operation_id"],
        child
    );
    assert_eq!(
        replay["error"]["details"]["targets"][0]["agent_id"],
        reserved
    );
    assert_eq!(
        replay["error"]["details"]["targets"][0]["status"],
        "not_admitted"
    );
    assert!(f.store().get_operation(&child).unwrap().is_none());
    assert_eq!(operation(&f, &parent.operation_id), saved);
    assert_eq!(observation_count(&f), 1);
    assert_eq!(f.provider.create_call_count(), baseline);
    assert_eq!(hooks.entries.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.executions.load(Ordering::SeqCst), 0);
}
