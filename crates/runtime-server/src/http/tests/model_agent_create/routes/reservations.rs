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
async fn model_add_not_created_keeps_only_attempted_unresolved_shell_route_pins() {
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
        let reviewed = super::super::hooks::review(f, child, reserved, false).await;
        assert_eq!(
            super::super::hooks::review(f, child, reserved, false).await,
            reviewed
        );
        let (native,pins,application):(i64,i64,String)=f.db().query_row(
            "SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1 AND resource_kind='operator_agent_create_native'),(SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1 AND resource_kind LIKE 'managed_worktree_native_%'),application_state FROM workspace_hook_effects WHERE parent_event_id=?2",
            rusqlite::params![child,format!("workspace-hook:pre-add:{reserved}")],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(native, 0);
        assert_eq!(application, "rejected");
        assert_eq!(pins, if action == 5 { 2 } else { 0 });
        let worktree = f
            .store()
            .get_managed_worktree("wt_receipt")
            .unwrap()
            .unwrap();
        let cleanup = routed.cleanup(&worktree);
        if action == 5 {
            assert!(
                cleanup.is_err(),
                "provider absence must not authorize cleanup of an unknown shell route"
            );
        } else {
            assert!(matches!(
                cleanup.unwrap(),
                runtime_core::ManagedWorktreeCleanupAdmission::Execute { .. }
            ));
        }
        assert!(
            Path::new(&worktree.worktree_cwd).exists(),
            "the fixture only attempts store admission, never native cleanup"
        );
        assert_eq!(hooks.calls.load(Ordering::SeqCst), usize::from(action == 5));
        assert_eq!(f.provider.create_call_count(), baseline);
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
            assert_eq!(late.application_state, "rejected");
            assert_eq!(
                late.application_error_code.as_deref(),
                Some("operator_verified_agent_not_created")
            );
            assert!(routed.cleanup(&worktree).is_err());
            assert_eq!(
                super::super::hooks::review(f, child, reserved, false).await,
                reviewed
            );
            let detail = f.store().get_operation(child).unwrap().unwrap();
            let saved = detail
                .effects
                .iter()
                .find(|e| e.effect_kind == "provider_session_create")
                .unwrap()
                .evidence
                .as_ref()
                .unwrap();
            assert_eq!(
                saved["retained_route_reservations"]["owner_operation_id"],
                child
            );
            assert_eq!(
                saved["retained_route_reservations"]["hook_parent_event_id"],
                format!("workspace-hook:pre-add:{reserved}")
            );
            assert!(routed.cleanup(&worktree).is_err());
        }
    }
}

#[tokio::test]
async fn model_add_unknown_provider_preserves_new_git_receipt_and_reservations_until_review() {
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
    let (git_id,git_result):(String,String)=f.db().query_row("SELECT operation_id,exact_terminal_result_json FROM runtime_operations WHERE kind='workspace_worktree_create' AND phase='completed'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    let saved: runtime_core::WorktreeCreateResponse = serde_json::from_str(&git_result).unwrap();
    let pins: i64 = f
        .db()
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1",
            [child],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pins, 3);
    let fresh = reopened_runtime(&f.temp, f.provider.clone()).await;
    fresh.recover_startup().await.unwrap();
    assert_eq!(
        f.invoke("new-git-provider-absent", Routed::args("provider-absent"))
            .await["error"]["details"],
        result["error"]["details"]
    );
    super::super::hooks::review(f, child, reserved, false).await;
    let (pins,after):(i64,String)=f.db().query_row("SELECT (SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1),exact_terminal_result_json FROM runtime_operations WHERE operation_id=?2",rusqlite::params![child,git_id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(pins, 0);
    assert_eq!(after, git_result);
    assert!(Path::new(&saved.worktree.worktree_cwd).is_dir());
    assert_eq!(f.provider.create_call_count(), baseline + 1);
    assert!(f.provider.native_cwd(reserved).await.is_none());
}
