use super::*;
use runtime_core::{
    AgentToolMutationAdmission, AgentToolMutationCommand, MemberAgentCreateCommand,
    OperatorAgentCreateAdmission, WorkspaceAgentCreateRequest, WorkspaceAgentOnboardingRequest,
    WorkspaceMembershipPolicy,
};

mod reservations;

struct Routed {
    base: Fixture,
    worktrees: Arc<dyn runtime_core::WorktreeService>,
}

impl Routed {
    async fn new() -> Self {
        let (state, temp, provider, workspace, caller) =
            super::super::operator_rebind_receipt_replay::fixture().await;
        let worktrees = state.app.services.worktrees.clone();
        let base = Fixture {
            router: build_router(state.clone()),
            token: state.bearer_token.clone(),
            temp,
            provider,
            runtime: state.runtime.clone(),
            workspace: workspace.workspace_id,
            caller: caller.agent_id,
        };
        Self { base, worktrees }
    }

    fn args(name: &str) -> Value {
        json!({"mode":"add","worktree_name":name,"title":"New colleague","prompt":"Inspect the fixture"})
    }

    fn plan(&self, key: &str, name: &str) -> MemberAgentCreateCommand {
        let f = &self.base;
        let request = json!({"tool_name":"gg_team","args":Self::args(name)});
        let parent = AgentToolMutationCommand {
            operation_id: format!("op_fixture_add_{key}"),
            workspace_id: f.workspace.clone(),
            caller_agent_id: f.caller.clone(),
            idempotency_key: key.into(),
            tool_name: "gg_team".into(),
            normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
            normalized_request: request,
            admitted_at: now(),
        };
        assert!(matches!(
            f.store().begin_agent_tool_mutation(&parent).unwrap(),
            AgentToolMutationAdmission::Execute { .. }
        ));
        let route = self
            .worktrees
            .plan_member_agent_create_route(&f.workspace, name)
            .unwrap();
        let caller = f
            .runtime
            .get_workspace_agent(&f.workspace, &f.caller)
            .unwrap();
        let p = caller.recreation_policy;
        let request = WorkspaceAgentCreateRequest {
            provider: p.provider,
            model: p.model,
            permission_intent: p.permission_intent,
            setting_sources_intent: p.setting_sources_intent,
            current_preferences: p.current_preferences,
            system_prompt: p.system_prompt,
            allowed_tools: p.allowed_tools,
            disallowed_tools: p.disallowed_tools,
            cwd: None,
            worktree: Some(route.request_route()),
            harness_version_slot: p.harness_version_slot,
            title: Some("New colleague".into()),
            metadata: None,
            onboarding: Some(WorkspaceAgentOnboardingRequest {
                prompt: Some("Inspect the fixture".into()),
                image_paths: vec![],
                skill: None,
                creator_compaction_subscription: None,
            }),
        };
        f.runtime
            .plan_workspace_agent_create_as_member(
                &f.workspace,
                &f.caller,
                &parent.operation_id,
                request,
                None,
                route,
                WorkspaceMembershipPolicy::default(),
            )
            .unwrap()
    }

    fn admit(&self, command: &MemberAgentCreateCommand) {
        assert!(matches!(
            self.base
                .store()
                .admit_member_agent_create(command)
                .unwrap(),
            OperatorAgentCreateAdmission::Execute { .. }
        ));
    }

    fn cleanup(
        &self,
        worktree: &runtime_core::ManagedWorktreeRecord,
    ) -> Result<runtime_core::ManagedWorktreeCleanupAdmission, RuntimeError> {
        self.base.store().admit_managed_worktree_cleanup(
            &runtime_core::ManagedWorktreeCleanupCommand {
                operation_id: format!("fixture_cleanup_{}", worktree.id),
                worktree: worktree.clone(),
                reason: Some("Independent fixture cleanup admission only".into()),
                requested_at: now(),
            },
        )
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test]
async fn model_add_existing_and_new_routes_keep_exact_native_agent_and_git_receipts() {
    for name in [" linked ", "new-colleague"] {
        let routed = Routed::new().await;
        let f = &routed.base;
        let baseline = f.provider.create_call_count();
        let result = f.invoke("member-managed-success", Routed::args(name)).await;
        assert_eq!(result["ok"], true, "{result}");
        let id = result["result"]["agent_id"].as_str().unwrap();
        let agent = f.runtime.get_workspace_agent(&f.workspace, id).unwrap();
        let child: String = f
            .db()
            .query_row(
                "SELECT source_operation_id FROM workspace_session_ownership WHERE session_id=?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        let record = f.store().get_operation(&child).unwrap().unwrap().operation;
        assert_eq!(record.actor.kind, runtime_core::OperationActorKind::Agent);
        assert_eq!(record.actor.identifier, f.caller);
        assert!(record.idempotency_key.as_ref().unwrap().len() > 128);
        assert_eq!(
            f.provider.native_cwd(id).await.as_deref(),
            Some(agent.recreation_policy.authoritative_cwd.as_str())
        );
        let git_count:i64=f.db().query_row("SELECT COUNT(*) FROM runtime_operations WHERE actor_kind='agent' AND kind='workspace_worktree_create'",[],|r|r.get(0)).unwrap();
        assert_eq!(git_count, if name == "new-colleague" { 1 } else { 0 });
        let pins:i64=f.db().query_row("SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1",[&child],|r|r.get(0)).unwrap();
        assert_eq!(pins, 0);
        assert_eq!(
            f.invoke("member-managed-success", Routed::args(name)).await,
            result
        );
        assert_eq!(f.provider.create_call_count(), baseline + 1);
        if name == "new-colleague" {
            let (kind,actor,key):(String,String,String)=f.db().query_row("SELECT actor_kind,actor_id,idempotency_key FROM runtime_operations WHERE kind='workspace_worktree_create'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
            assert_eq!(
                (kind.as_str(), actor.as_str()),
                ("agent", f.caller.as_str())
            );
            assert!(key.len() > 128 && key.len() <= 512);
            let session = f.runtime.get_session(id).await.unwrap();
            let worktree = f
                .store()
                .get_managed_worktree(session.worktree_id.as_deref().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(worktree.deletion_policy, "retain_on_last_claim");
            assert_eq!(
                worktree.created_by_session_id.as_deref(),
                Some(f.caller.as_str())
            );
        }
    }
}

#[tokio::test]
async fn model_add_two_independent_connections_grant_one_native_execution() {
    let routed = Routed::new().await;
    let f = &routed.base;
    let command = routed.plan("independent-admission", "linked");
    let baseline = f.provider.create_call_count();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let store = f.store();
        let cmd = command.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            store.admit_member_agent_create(&cmd).unwrap()
        }));
    }
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, OperatorAgentCreateAdmission::Execute { .. }))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, OperatorAgentCreateAdmission::Unresolved { .. }))
            .count(),
        1
    );
    assert!(routed
        .cleanup(command.intent.route.worktree().unwrap())
        .is_err());
    assert_eq!(f.provider.create_call_count(), baseline);
    assert!(f
        .runtime
        .execute_workspace_agent_create_as_member(command, routed.worktrees.as_ref())
        .await
        .is_err());
    assert_eq!(f.provider.create_call_count(), baseline);
}

#[tokio::test]
async fn model_add_existing_canonical_repository_alias_keeps_original_claim_identity() {
    let routed = Routed::new().await;
    let f = &routed.base;
    let original = f
        .store()
        .get_managed_worktree("wt_receipt")
        .unwrap()
        .unwrap();
    let alias = format!("{}/.", original.repo_root);
    let checkout_alias = format!("{}/.", original.worktree_cwd);
    f.db()
        .execute(
            "UPDATE managed_worktrees SET repo_root=?1,worktree_cwd=?2,revision=revision+1 WHERE id='wt_receipt'",
            rusqlite::params![alias,checkout_alias],
        )
        .unwrap();
    let result = f
        .invoke("existing-alias-root", Routed::args("linked"))
        .await;
    assert_eq!(result["ok"], true, "{result}");
    let retained = f
        .store()
        .get_managed_worktree("wt_receipt")
        .unwrap()
        .unwrap();
    assert_eq!(retained.repo_root, alias);
    assert_eq!(retained.worktree_cwd, checkout_alias);
    let session = f
        .runtime
        .get_session(result["result"]["agent_id"].as_str().unwrap())
        .await
        .unwrap();
    assert_eq!(session.cwd.as_deref(), Some(checkout_alias.as_str()));
    let branch_id = json!([retained.repo_root, retained.branch_name]).to_string();
    let generation:i64=f.db().query_row("SELECT last_generation FROM runtime_operation_resource_fences WHERE resource_kind='managed_worktree_native_branch' AND resource_id=?1",[branch_id],|r|r.get(0)).unwrap();
    assert_eq!(generation, 1);
}

#[tokio::test]
async fn model_add_rejects_changed_saved_route_and_fabricated_plan_before_native_admission() {
    for mutate_record in [false, true] {
        let routed = Routed::new().await;
        let f = &routed.base;
        let command = routed.plan("changed-route", "linked");
        let baseline = f.provider.create_call_count();
        let mut candidate = command.clone();
        if mutate_record {
            f.db()
                .execute(
                    "UPDATE managed_worktrees SET revision=revision+1 WHERE id='wt_receipt'",
                    [],
                )
                .unwrap();
        } else {
            candidate
                .intent
                .caller
                .recreation_policy
                .allowed_tools
                .push("fabricated".into());
        }
        assert!(f.store().admit_member_agent_create(&candidate).is_err());
        assert!(f
            .store()
            .get_operation(&command.operation.operation_id)
            .unwrap()
            .is_none());
        assert_eq!(f.provider.create_call_count(), baseline);
    }
}

#[tokio::test]
async fn model_add_native_child_keys_cannot_collide_with_valid_host_invocation_keys() {
    let f = Fixture::new().await;
    let baseline = f.provider.create_call_count();
    let first = f.add("key-domain-original").await;
    assert_eq!(first["ok"], true, "{first}");
    let child: String = f
        .db()
        .query_row(
            "SELECT source_operation_id FROM workspace_session_ownership WHERE session_id=?1",
            [first["result"]["agent_id"].as_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    let old_key = format!(
        "member-create:{}",
        child.strip_prefix("op_member_create_").unwrap()
    );
    assert!(old_key.len() <= 128);
    let second = f.add(&old_key).await;
    assert_eq!(second["ok"], true, "{second}");
    assert_ne!(second["result"]["agent_id"], first["result"]["agent_id"]);
    assert_eq!(f.add(&old_key).await, second);
    assert_eq!(f.provider.create_call_count(), baseline + 2);
    let routed = Routed::new().await;
    let mut command = routed.plan("frozen-git-key", "key-history");
    assert!(command.worktree_command().unwrap().idempotency_key.len() > 128);
    command.intent.git_idempotency_key = None;
    let old = serde_json::to_value(&command.intent).unwrap();
    assert!(old.get("git_idempotency_key").is_none());
    let decoded: runtime_core::MemberAgentCreateIntent =
        serde_json::from_value(old.clone()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), old);
    command.intent = decoded;
    assert_eq!(
        command.worktree_command().unwrap().idempotency_key,
        format!("member-create-git:{}", command.operation.operation_id)
    );
}

#[tokio::test]
async fn model_add_new_git_receipt_fault_preserves_native_checkout_and_original_pins() {
    for transfer_fault in [false, true] {
        let routed = Routed::new().await;
        let f = &routed.base;
        let command = routed.plan("git-receipt-fault", "git-fault");
        let baseline = f.provider.create_call_count();
        let worktree = command.intent.route.worktree().unwrap();
        let git_id = command.operation.worktree_operation_id.as_deref().unwrap();
        if transfer_fault {
            f.db().execute_batch(&format!("CREATE TRIGGER fail_member_git_receipt BEFORE INSERT ON runtime_operation_resource_claims WHEN NEW.owner_operation_id='{}' AND NEW.resource_kind='managed_worktree_native_branch' BEGIN SELECT RAISE(ABORT,'fixture second route pin transfer fault'); END;",command.operation.operation_id)).unwrap();
        } else {
            f.db().execute_batch("CREATE TRIGGER fail_member_git_receipt BEFORE INSERT ON runtime_events WHEN NEW.kind='worktree.created' BEGIN SELECT RAISE(ABORT,'fixture Git record receipt fault'); END;").unwrap();
        }
        assert!(f
            .runtime
            .execute_workspace_agent_create_as_member(command.clone(), routed.worktrees.as_ref())
            .await
            .is_err());
        assert!(Path::new(&worktree.worktree_cwd).is_dir());
        assert!(f
            .store()
            .get_managed_worktree(&worktree.id)
            .unwrap()
            .is_none());
        let (pins,phase):(i64,String)=f.db().query_row("SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1),phase FROM runtime_operations WHERE operation_id=?1",[git_id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!((pins, phase.as_str()), (2, "manual_review"));
        let (create_pins,max_generation):(i64,i64)=f.db().query_row("SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1 AND resource_kind LIKE 'managed_worktree_native_%'),(SELECT MAX(fence_generation) FROM runtime_operation_resource_claims WHERE owner_operation_id=?2)",rusqlite::params![command.operation.operation_id,git_id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!((create_pins, max_generation), (0, 1));
        assert_eq!(f.provider.create_call_count(), baseline);
        f.db()
            .execute_batch("DROP TRIGGER fail_member_git_receipt")
            .unwrap();
        let fresh = reopened_runtime(&f.temp, f.provider.clone()).await;
        fresh.recover_startup().await.unwrap();
        assert!(routed
            .worktrees
            .execute_member_agent_create_worktree(&command)
            .await
            .is_err());
        assert!(f
            .runtime
            .execute_workspace_agent_create_as_member(command.clone(), routed.worktrees.as_ref())
            .await
            .is_err());
        assert!(Path::new(&worktree.worktree_cwd).is_dir());
        assert_eq!(f.provider.create_call_count(), baseline);
        super::hooks::review(
            f,
            &command.operation.operation_id,
            &command.operation.reserved_agent_id,
            false,
        )
        .await;
        let pins: i64 = f
        .db()
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1",
            [git_id],
            |r| r.get(0),
        )
        .unwrap();
        assert_eq!(pins, 2);
    }
}
