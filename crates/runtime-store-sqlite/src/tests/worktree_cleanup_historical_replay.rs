//! Explicit archived old-protocol rows, never a version-stripped typed grant.
use super::legacy_native_turn_guards::raw_state;
use super::worktree_cleanup_receipts::fixture;
use super::*;
use crate::operation_tx::OperationAuthorityTransaction;
use runtime_core::{
    normalized_json_hash, ManagedWorktreeCleanupAdmission, ManagedWorktreeCleanupCommand,
    NewRuntimeEvent, OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase,
    OperationRecord, OperationTransitionRecord, RuntimeEventCriticality, WorktreeCleanupResponse,
};

fn archived(
    was_review: bool,
) -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    ManagedWorktreeCleanupCommand,
    serde_json::Value,
) {
    let (temp, repository, command) = fixture();
    let w = &command.worktree;
    let id = &command.operation_id;
    let response = WorktreeCleanupResponse {
        worktree_id: w.id.clone(),
        status: "deleted".into(),
        deletion_policy: w.deletion_policy.clone(),
        active_claim_count: 0,
        worktree_path_deleted: true,
        branch_deleted: true,
        diagnostics: vec![],
    };
    let exact = serde_json::to_value(&response).unwrap();
    let input = serde_json::json!({"worktree_id":w.id,"worktree_created_at":w.created_at,
        "repo_root":w.repo_root,"checkout_path":w.worktree_cwd,"branch_name":w.branch_name,
        "deletion_policy":w.deletion_policy,"reason":command.reason});
    let mut db = open_connection(&repository.database_path).unwrap();
    let tx = db.transaction().unwrap();
    let authority = OperationAuthorityTransaction::new(&tx);
    authority
        .insert_operation(&OperationRecord {
            operation_id: id.clone(),
            workspace_id: None,
            kind: "managed_worktree_cleanup".into(),
            actor: OperationActor {
                kind: OperationActorKind::System,
                identifier: "managed_worktree_service".into(),
            },
            idempotency_key: Some(format!("{}:{}", w.id, w.created_at)),
            normalized_request_hash: normalized_json_hash(&input).unwrap(),
            normalized_request: input,
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: 100,
            updated_at: 100,
        })
        .unwrap();
    authority
        .append_transition(&OperationTransitionRecord {
            operation_id: id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: Some(serde_json::json!({"native_effect":"git_worktree_cleanup"})),
            created_at: 100,
        })
        .unwrap();
    let checkout = authority
        .acquire_claim(id, "managed_worktree_native_checkout", &w.worktree_cwd, 100)
        .unwrap();
    let branch_key = serde_json::to_string(&[&w.repo_root, &w.branch_name]).unwrap();
    let branch = authority
        .acquire_claim(id, "managed_worktree_native_branch", &branch_key, 100)
        .unwrap();
    authority
        .insert_effect(&OperationEffectRecord {
            effect_id: format!("fx_{id}"),
            operation_id: id.clone(),
            effect_kind: "git_worktree_cleanup".into(),
            target_kind: "git_checkout".into(),
            target_id: w.worktree_cwd.clone(),
            phase: "finalized".into(),
            idempotency_key: "native_git_remove_and_branch_delete".into(),
            evidence: Some(serde_json::json!({"receipt":exact,"native_evidence":"observed"})),
            attempt_count: 1,
            created_at: 100,
            updated_at: 200,
        })
        .unwrap();
    let publish = |suffix: &str, kind: &str, payload: serde_json::Value, at: i64| {
        crate::repository::insert_runtime_event_in_transaction(
            &tx,
            &NewRuntimeEvent {
                event_id: format!("{id}:event:worktree:{suffix}"),
                scope: RuntimeEventScope::Worktree,
                scope_id: w.id.clone(),
                session_id: None,
                team_id: None,
                turn_id: None,
                kind: kind.into(),
                criticality: RuntimeEventCriticality::Critical,
                payload,
                provider: None,
                provider_seq: None,
                created_at: at,
            },
        )
        .unwrap();
    };
    publish(
        "admitted",
        "worktree.cleanup_admitted",
        serde_json::json!({"operation_id":id,
        "checkout_path":w.worktree_cwd,"branch_name":w.branch_name}),
        100,
    );
    if was_review {
        tx.execute("UPDATE runtime_operations SET phase='manual_review',error_code='native_cleanup_incomplete',
            error_message='independent native effect review required',updated_at=150 WHERE operation_id=?1",[id]).unwrap();
        authority
            .append_transition(&OperationTransitionRecord {
                operation_id: id.clone(),
                sequence: 2,
                from_phase: Some(OperationPhase::Requested),
                to_phase: OperationPhase::ManualReview,
                evidence: Some(serde_json::json!({"native_cleanup":"manual_review"})),
                created_at: 150,
            })
            .unwrap();
        let partial = WorktreeCleanupResponse {
            status: "partial".into(),
            branch_deleted: false,
            diagnostics: vec!["branch has unmerged history".into()],
            ..response
        };
        publish(
            "settled",
            "worktree.cleanup_failed",
            serde_json::json!({"operation_id":id,
            "response":partial,"after_manual_review":false}),
            150,
        );
    }
    let from = if was_review {
        OperationPhase::ManualReview
    } else {
        OperationPhase::Requested
    };
    authority
        .terminalize(
            id,
            from,
            OperationPhase::Completed,
            None,
            &exact,
            None,
            None,
            200,
        )
        .unwrap();
    authority
        .append_transition(&OperationTransitionRecord {
            operation_id: id.clone(),
            sequence: if was_review { 3 } else { 2 },
            from_phase: Some(from),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({"native_cleanup":"completed"})),
            created_at: 200,
        })
        .unwrap();
    authority.release_claim(&checkout).unwrap();
    authority.release_claim(&branch).unwrap();
    publish(
        if was_review { "reconciled" } else { "settled" },
        "worktree.cleaned_up",
        serde_json::json!({"operation_id":id,"response":exact,"after_manual_review":was_review}),
        200,
    );
    tx.execute("INSERT INTO managed_worktree_cleanup_receipts (operation_id,worktree_id,worktree_created_at,
        repo_root,checkout_path,branch_name,deletion_policy,reason,phase,response_json,requested_at,completed_at)
        VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'completed',?9,100,200)",
        params![id,w.id,w.created_at,w.repo_root,w.worktree_cwd,w.branch_name,w.deletion_policy,command.reason,
            serde_json::to_string(&exact).unwrap()]).unwrap();
    tx.commit().unwrap();
    (temp, repository, command, exact)
}

#[test]
fn historical_completed_direct_and_review_receipts_replay_without_new_authority() {
    for was_review in [false, true] {
        let (_temp, repository, command, exact) = archived(was_review);
        let db = open_connection(&repository.database_path).unwrap();
        let before = raw_state(&db);
        for replay in [
            repository
                .replay_managed_worktree_cleanup(&command.worktree)
                .unwrap()
                .unwrap(),
            repository.admit_managed_worktree_cleanup(&command).unwrap(),
        ] {
            assert!(
                matches!(replay,ManagedWorktreeCleanupAdmission::Replay{operation_id,response}
                if operation_id==command.operation_id && serde_json::to_value(&response).unwrap()==exact)
            );
        }
        assert_eq!(raw_state(&db), before);
    }
}

#[test]
fn historical_completed_replay_requires_consistent_old_carriers() {
    for was_review in [false, true] {
        for fault in [
            "UPDATE runtime_operations SET normalized_request_hash='changed';",
            "UPDATE runtime_operations SET phase='manual_review',exact_terminal_result_json=NULL;",
            "UPDATE runtime_operations SET exact_terminal_result_json='{}';",
            "DELETE FROM runtime_operation_effects;",
            "INSERT INTO runtime_operation_effects SELECT effect_id||':extra',operation_id,'git_branch_delete',target_kind,target_id,phase,idempotency_key||':extra',evidence_json,attempt_count,created_at,updated_at FROM runtime_operation_effects;",
            "UPDATE runtime_operation_transitions SET evidence_json='{}' WHERE sequence=1;",
            "DELETE FROM runtime_events WHERE kind='worktree.cleaned_up';",
        ] {
            let (_temp,repository,command,_)=archived(was_review);
            let db=open_connection(&repository.database_path).unwrap();
            db.execute_batch(fault).unwrap();
            let before=raw_state(&db);
            assert!(repository.replay_managed_worktree_cleanup(&command.worktree).is_err(),"{fault}");
            assert!(repository.admit_managed_worktree_cleanup(&command).is_err(),"{fault}");
            assert_eq!(raw_state(&db),before,"was_review={was_review},fault={fault}");
        }
    }
}
