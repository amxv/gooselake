use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn model_add_git_requires_two_fence_generations_before_native_dispatch() {
    for headroom in [1, 2] {
        let routed = Routed::new().await;
        let f = &routed.base;
        let command = routed.plan("git-headroom", "headroom");
        let baseline = f.provider.create_call_count();
        let worktree = command.intent.route.worktree().unwrap();
        let resources = [
            (
                "managed_worktree_native_checkout",
                worktree.worktree_cwd.clone(),
            ),
            (
                "managed_worktree_native_branch",
                json!([worktree.repo_root, worktree.branch_name]).to_string(),
            ),
        ];
        for (kind, id) in &resources {
            f.db().execute("INSERT INTO runtime_operation_resource_fences(resource_kind,resource_id,last_generation) VALUES(?1,?2,?3)",
                rusqlite::params![kind,id,i64::MAX-headroom]).unwrap();
        }
        routed.admit(&command);
        let result = routed
            .worktrees
            .execute_member_agent_create_worktree(&command)
            .await;
        if headroom == 1 {
            assert!(
                result.is_err(),
                "one remaining generation must not permit Git dispatch"
            );
            assert!(!Path::new(&worktree.worktree_cwd).exists());
            assert!(f
                .store()
                .get_operation(command.operation.worktree_operation_id.as_deref().unwrap())
                .unwrap()
                .is_none());
        } else {
            let result = result.unwrap();
            assert_eq!(result.worktree, *worktree);
            assert!(result.created);
            for (kind, id) in &resources {
                let (owner,generation):(String,i64)=f.db().query_row("SELECT owner_operation_id,fence_generation FROM runtime_operation_resource_claims WHERE resource_kind=?1 AND resource_id=?2",
                    rusqlite::params![kind,id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
                assert_eq!(owner, command.operation.operation_id);
                assert_eq!(generation, i64::MAX);
            }
            assert_eq!(
                routed
                    .worktrees
                    .execute_member_agent_create_worktree(&command)
                    .await
                    .unwrap()
                    .worktree,
                *worktree
            );
            let count:i64=f.db().query_row("SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_worktree_create'",[],|r|r.get(0)).unwrap();
            assert_eq!(count, 1);
        }
        assert_eq!(f.provider.create_call_count(), baseline);
    }
}

#[tokio::test]
async fn model_add_prose_preserves_original_shell_route_reservations() {
    for (action, restart_before_review) in [(5, false), (5, true), (6, true)] {
        let routed = Routed::new().await;
        let f = &routed.base;
        let hooks = super::super::hooks::install(f, action, 1).await;
        let baseline = f.provider.create_call_count();
        let result = f.invoke("hook-unknown-route", Routed::args("linked")).await;
        assert_eq!(result["ok"], false, "{result}");
        let target = &result["error"]["details"]["targets"][0];
        let child = target["operation_id"].as_str().unwrap();
        let reserved = target["agent_id"].as_str().unwrap();
        if restart_before_review {
            let fresh = reopened_runtime(&f.temp, f.provider.clone()).await;
            fresh.recover_startup().await.unwrap();
        }
        let original = f.store().get_operation(child).unwrap().unwrap();
        let original_parent = f
            .store()
            .get_operation(&f.parent("hook-unknown-route"))
            .unwrap()
            .unwrap();
        let refused = super::super::hooks::review(f, child, reserved, false).await;
        assert_eq!(
            super::super::hooks::review(f, child, reserved, false).await,
            refused
        );
        let(native,pins,phase,application,attempts):(i64,i64,String,String,i64)=f.db().query_row("SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1 AND resource_kind='operator_agent_create_native'),(SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1 AND resource_kind LIKE 'managed_worktree_native_%'),phase,application_state,attempt_count FROM workspace_hook_effects WHERE parent_event_id=?2",rusqlite::params![child,format!("workspace-hook:pre-add:{reserved}")],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).unwrap();
        assert_eq!((native, pins), (1, 2));
        assert_eq!(attempts, i64::from(action == 5));
        assert_eq!(
            phase,
            if restart_before_review {
                "manual_review"
            } else {
                "started"
            }
        );
        assert_eq!(
            application,
            if action == 6 { "rejected" } else { "not_ready" }
        );
        let worktree = f
            .store()
            .get_managed_worktree("wt_receipt")
            .unwrap()
            .unwrap();
        assert!(
            routed.cleanup(&worktree).is_err(),
            "prose must not release admitted create route reservations"
        );
        assert!(Path::new(&worktree.worktree_cwd).exists());
        assert_eq!(hooks.calls.load(Ordering::SeqCst), usize::from(action == 5));
        assert_eq!(f.provider.create_call_count(), baseline);
        assert_eq!(f.store().get_operation(child).unwrap().unwrap(), original);
        assert_eq!(
            f.store()
                .get_operation(&f.parent("hook-unknown-route"))
                .unwrap()
                .unwrap(),
            original_parent
        );
        if action == 5 && !restart_before_review {
            let receipt = runtime_core::TeamHookEffectReceipt {
                validated_output: Some(json!({"inject":{"caller":{"text":"late shell output"}}})),
                error_code: None,
            };
            let late = f
                .store()
                .finish_team_hook_effect(
                    &f.workspace,
                    &format!("workspace-hook:pre-add:{reserved}"),
                    runtime_core::TeamHookEventName::PreTeammateAdd,
                    "fixture_0",
                    runtime_core::TeamHookEffectPhase::Succeeded,
                    receipt,
                    now(),
                )
                .unwrap();
            assert_eq!(late.application_state, "pending");
            assert!(late.application_error_code.is_none());
            assert!(routed.cleanup(&worktree).is_err());
            assert_eq!(
                super::super::hooks::review(f, child, reserved, false).await,
                refused
            );
            assert_eq!(f.store().get_operation(child).unwrap().unwrap(), original);
            let notices:i64=f.db().query_row("SELECT COUNT(*) FROM workspace_system_notices WHERE recipient_agent_id=?1 OR input_json LIKE '%late shell output%'",[reserved],|row|row.get(0)).unwrap();
            assert_eq!(notices, 0);
            assert!(f.store().get_persisted_session(reserved).unwrap().is_none());
            assert_eq!(hooks.calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.provider.create_call_count(), baseline);
            assert!(Path::new(&worktree.worktree_cwd).exists());
        }
    }
}
#[tokio::test]
async fn unclaimed_existing_route_allows_cleanup_admission_without_native_cleanup() {
    let routed = Routed::new().await;
    let f = &routed.base;
    let worktree = f
        .store()
        .get_managed_worktree("wt_receipt")
        .unwrap()
        .unwrap();
    let baseline = f.provider.create_call_count();
    assert!(matches!(
        routed.cleanup(&worktree).unwrap(),
        runtime_core::ManagedWorktreeCleanupAdmission::Execute { .. }
    ));
    assert!(Path::new(&worktree.worktree_cwd).is_dir());
    assert_eq!(f.provider.create_call_count(), baseline);
}
#[tokio::test]
async fn model_add_provider_refusal_and_prose_preserve_git_receipt_and_reservations() {
    let routed = Routed::new().await;
    let f = &routed.base;
    let baseline = f.provider.create_call_count();
    f.provider.set_create_failure_mode(4);
    let result = f
        .invoke("new-git-provider-absent", Routed::args("provider-absent"))
        .await;
    assert_eq!(result["ok"], false, "{result}");
    let target = &result["error"]["details"]["targets"][0];
    let child = target["operation_id"].as_str().unwrap();
    let reserved = target["agent_id"].as_str().unwrap();
    let(git_id,git_result):(String,String)=f.db().query_row("SELECT operation_id,exact_terminal_result_json FROM runtime_operations WHERE kind='workspace_worktree_create' AND phase='completed'",[],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
    let saved: runtime_core::WorktreeCreateResponse = serde_json::from_str(&git_result).unwrap();
    let original = f.store().get_operation(child).unwrap().unwrap();
    assert_eq!(original.claims.len(), 3);
    let original_git = f.store().get_operation(&git_id).unwrap().unwrap();
    let resumed = f.provider.resumed_requests().await.len();
    let fresh = reopened_runtime(&f.temp, f.provider.clone()).await;
    fresh.recover_startup().await.unwrap();
    assert_eq!(
        f.invoke("new-git-provider-absent", Routed::args("provider-absent"))
            .await["error"]["details"],
        result["error"]["details"]
    );
    assert!(f.provider.resumed_requests().await[resumed..]
        .iter()
        .all(|request| request.runtime_session_id != reserved));
    super::super::hooks::review(f, child, reserved, false).await;
    let(pins,after):(i64,String)=f.db().query_row("SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1),exact_terminal_result_json FROM runtime_operations WHERE operation_id=?2",rusqlite::params![child,git_id],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!(pins, 3);
    assert_eq!(after, git_result);
    assert_eq!(f.store().get_operation(child).unwrap().unwrap(), original);
    assert_eq!(
        f.store().get_operation(&git_id).unwrap().unwrap(),
        original_git
    );
    assert!(routed.cleanup(&saved.worktree).is_err());
    assert!(Path::new(&saved.worktree.worktree_cwd).is_dir());
    assert_eq!(f.provider.create_call_count(), baseline + 1);
    assert!(f.provider.native_cwd(reserved).await.is_none());
}
