//! Private injected runner: each outcome uses disposable SQLite and inert paths.
use super::*;
use runtime_core::{
    ManagedWorktreeCleanupAdmission, ManagedWorktreeCleanupCommand, OperationPhase,
    WorktreeCleanupRequest, WorktreeService,
};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    CheckoutExit,
    CheckoutUnknown,
    CheckoutReceipt,
    BranchExit,
    BranchUnknown,
    TerminalReceipt,
    HoldReceipt,
    LegacyAfterCheckout,
}

#[tokio::test]
async fn original_cleanup_runner_stops_at_each_uncertain_or_uncommitted_action() {
    for fault in [
        Fault::None,
        Fault::CheckoutExit,
        Fault::CheckoutUnknown,
        Fault::CheckoutReceipt,
        Fault::BranchExit,
        Fault::BranchUnknown,
        Fault::TerminalReceipt,
        Fault::HoldReceipt,
        Fault::LegacyAfterCheckout,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("runtime.sqlite3");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: db_path.clone(),
        }));
        store.initialize().await.unwrap();
        let (runtime, comms) = crate::tests::build_runtime_and_team_comms(store.clone()).await;
        let service = RuntimeWorktreeService::new(
            store.clone(),
            runtime,
            comms,
            WorktreeServiceConfig {
                enabled: true,
                root_dir: temp.path().join("managed").display().to_string(),
                init_script_path: ".agents/gg/worktree-init.sh".into(),
                deletion_policy_default: "retain_on_last_claim".into(),
            },
        )
        .unwrap();
        let checkout = temp.path().join("managed/fixture");
        std::fs::create_dir_all(&checkout).unwrap();
        let marker = checkout.join("inert-marker");
        std::fs::write(&marker, b"never invoke Git here").unwrap();
        let worktree = ManagedWorktreeRecord {
            id: "wt_inert_cleanup".into(),
            repo_root: temp.path().join("inert-repository").display().to_string(),
            worktree_root: temp.path().join("managed").display().to_string(),
            worktree_cwd: checkout.display().to_string(),
            branch_name: "gg/inert".into(),
            worktree_name: "inert".into(),
            unified_workspace_path: "inert".into(),
            deletion_policy: "delete_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 10,
            updated_at: 10,
        };
        store.upsert_managed_worktree(&worktree).unwrap();
        let command = ManagedWorktreeCleanupCommand {
            operation_id: "inert-cleanup".into(),
            worktree: worktree.clone(),
            reason: Some("injected-runner".into()),
            requested_at: 100,
        };
        let grant = match store.admit_managed_worktree_cleanup(&command).unwrap() {
            ManagedWorktreeCleanupAdmission::Execute { grant } => grant,
            other => panic!("{other:?}"),
        };
        let original = store.get_operation(&command.operation_id).unwrap().unwrap();
        let db = rusqlite::Connection::open(&db_path).unwrap();
        let rejected_event = match fault {
            Fault::CheckoutReceipt => Some("worktree.checkout_removed"),
            Fault::TerminalReceipt => Some("worktree.cleaned_up"),
            Fault::HoldReceipt => Some("worktree.cleanup_held"),
            _ => None,
        };
        if let Some(kind) = rejected_event {
            db.execute_batch(&format!("CREATE TRIGGER reject_inert_receipt BEFORE INSERT ON runtime_events
                WHEN NEW.kind='{kind}' BEGIN SELECT RAISE(FAIL,'injected original-action receipt failure'); END;")).unwrap();
        }
        let mut calls = Vec::new();
        let result=service.execute_cleanup(grant,|root,args| {
            assert_eq!(root,worktree.repo_root);
            calls.push(args.iter().map(|s|s.to_string()).collect::<Vec<_>>());
            if calls.len()==1 {
                assert_eq!(args,["worktree","remove",worktree.worktree_cwd.as_str()]);
                if fault==Fault::LegacyAfterCheckout {
                    db.execute_batch("INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
                        VALUES('late-inert-obligation','legacy-team','spawn_member_with_worktree','planned','{}',1,2);").unwrap();
                }
                match fault {
                    Fault::CheckoutExit|Fault::HoldReceipt=>Ok(7),
                    Fault::CheckoutUnknown=>Err(RuntimeError::Bootstrap("original checkout reply lost".into())),
                    _=>Ok(0),
                }
            }else{
                assert_eq!(calls.len(),2,"a grant cannot issue another action");
                assert_eq!(args,["branch","-d",worktree.branch_name.as_str()]);
                match fault {
                    Fault::BranchExit=>Ok(9),
                    Fault::BranchUnknown=>Err(RuntimeError::Bootstrap("original branch reply lost".into())),
                    _=>Ok(0),
                }
            }
        });
        let expected_calls = if matches!(
            fault,
            Fault::None | Fault::BranchExit | Fault::BranchUnknown | Fault::TerminalReceipt
        ) {
            2
        } else {
            1
        };
        assert_eq!(calls.len(), expected_calls, "{fault:?}");
        let detail = store.get_operation(&command.operation_id).unwrap().unwrap();
        if fault == Fault::None {
            let response = result.unwrap();
            assert_eq!(response.status, "deleted");
            assert_eq!(detail.operation.phase, OperationPhase::Completed);
            assert!(detail.claims.is_empty());
            assert!(detail
                .effects
                .iter()
                .all(|e| e.phase == "finalized" && e.attempt_count == 1));
            let replay = service
                .cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: worktree.id.clone(),
                    reason: None,
                })
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                serde_json::to_value(replay).unwrap()
            );
        } else {
            assert!(
                matches!(result,Err(RuntimeError::OperationUnresolved{ref operation_id,..}) if operation_id==&command.operation_id),
                "{fault:?}: {result:?}"
            );
            assert_eq!(
                detail.claims, original.claims,
                "{fault:?}: original reservations must remain"
            );
            if fault == Fault::HoldReceipt {
                assert_eq!(
                    detail, original,
                    "failed hold retains the original pending authority"
                );
            } else {
                assert_eq!(detail.operation.phase, OperationPhase::ManualReview);
                let removal = detail
                    .effects
                    .iter()
                    .find(|e| e.effect_kind == "git_worktree_remove")
                    .unwrap();
                let branch = detail
                    .effects
                    .iter()
                    .find(|e| e.effect_kind == "git_branch_delete")
                    .unwrap();
                if fault == Fault::LegacyAfterCheckout {
                    assert_eq!(
                        (&*removal.phase, &*branch.phase, branch.attempt_count),
                        ("finalized", "intended", 0)
                    );
                } else if expected_calls == 1 {
                    assert_eq!(
                        (&*removal.phase, &*branch.phase, branch.attempt_count),
                        ("uncertain", "intended", 0)
                    );
                } else {
                    assert_eq!(
                        (&*removal.phase, &*branch.phase),
                        ("finalized", "uncertain")
                    );
                }
            }
            for _ in 0..2 {
                let retry = service
                    .cleanup_worktree(WorktreeCleanupRequest {
                        worktree_id: worktree.id.clone(),
                        reason: None,
                    })
                    .await;
                assert!(
                    matches!(retry,Err(RuntimeError::OperationUnresolved{ref operation_id,..}) if operation_id==&command.operation_id),
                    "{fault:?}: {retry:?}"
                );
            }
        }
        assert_eq!(
            store.get_operation(&command.operation_id).unwrap().unwrap(),
            detail
        );
        assert_eq!(std::fs::read(marker).unwrap(), b"never invoke Git here");
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM runtime_operations WHERE kind='managed_worktree_cleanup'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "{fault:?}: no new operation on retry");
    }
}
