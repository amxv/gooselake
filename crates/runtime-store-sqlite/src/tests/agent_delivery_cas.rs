use super::*;
use runtime_core::{
    prepare_workspace_registration, AgentDeliveryRecord, OperationActor, RuntimeError,
    WorkspaceRegisterRequest,
};

fn seed() -> (tempfile::TempDir, SqliteRuntimeRepository) {
    super::agent_message_cancellation::seeded_messages()
}

fn next(expected: &AgentDeliveryRecord, status: &str) -> AgentDeliveryRecord {
    let mut updated = expected.clone();
    updated.status = status.into();
    updated.updated_at += 1;
    updated
}

#[test]
fn independent_workers_compete_for_a_single_durable_native_dispatch_claim() {
    let (_temp, repository) = seed();
    let pending = repository
        .get_agent_delivery("msg_cancel_a")
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, "pending");
    let injecting = next(&pending, "injecting");
    std::thread::scope(|scope| {
        let handles = (0..12)
            .map(|_| {
                let repository = &repository;
                let pending = pending.clone();
                let injecting = injecting.clone();
                scope.spawn(move || {
                    repository
                        .compare_and_swap_agent_delivery_status(&pending, &injecting)
                        .expect("serialized delivery claim")
                })
            })
            .collect::<Vec<_>>();
        let winners = handles
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1, "at most one provider call may follow");
    });
    assert_eq!(
        repository.get_agent_delivery(&pending.id).unwrap().unwrap(),
        injecting
    );

    // An outdated status snapshot must not overwrite the durable native
    // claim even through the legacy best-effort upsert adapter.
    assert!(matches!(
        repository.upsert_agent_delivery(&pending),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository
            .get_agent_delivery(&pending.id)
            .unwrap()
            .unwrap()
            .status,
        "injecting"
    );
    let injected = next(&injecting, "injected");
    assert!(repository
        .compare_and_swap_agent_delivery_status(&injecting, &injected)
        .unwrap());
    let mut stale = pending.clone();
    stale.status = "deferred".into();
    stale.updated_at = injected.updated_at + 1;
    assert!(matches!(
        repository.upsert_agent_delivery(&stale),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository.get_agent_delivery(&pending.id).unwrap(),
        Some(injected),
        "an injected native delivery is terminal against stale rewrites"
    );
}

#[test]
fn delivery_status_cas_rejects_aba_even_when_the_status_returns_to_pending() {
    let (_temp, repository) = seed();
    let first = repository
        .get_agent_delivery("msg_cancel_a")
        .unwrap()
        .unwrap();
    let deferred = next(&first, "deferred");
    assert!(repository
        .compare_and_swap_agent_delivery_status(&first, &deferred)
        .unwrap());
    let pending_again = next(&deferred, "pending");
    assert!(repository
        .compare_and_swap_agent_delivery_status(&deferred, &pending_again)
        .unwrap());
    assert!(!repository
        .compare_and_swap_agent_delivery_status(&first, &next(&first, "injecting"))
        .unwrap());
    assert!(repository
        .compare_and_swap_agent_delivery_status(&pending_again, &next(&pending_again, "injecting"))
        .unwrap());
}

#[test]
fn second_recipient_legacy_write_failure_rolls_back_native_claim_atomically() {
    let (temp, repository) = seed();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
         VALUES ('team_cas','Legacy','sess_sender','test',1,1);
         INSERT INTO team_messages(
           id,team_id,scope,sender_agent_id,recipient_agent_ids_json,input_json,
           priority,policy,created_at
         ) VALUES (
           'msg_cancel','team_cas','direct','sess_sender','[\"sess_a\"]','[]',
           'normal','non_interrupting',1
         );
         INSERT INTO team_deliveries(
           id,message_id,team_id,recipient_agent_id,provider,status,created_at,updated_at
         ) VALUES (
           'msg_cancel_a','msg_cancel','team_cas','sess_a','codex','pending',1,1
         );
         CREATE TRIGGER prevent_cas_mirror BEFORE UPDATE ON team_deliveries
           WHEN NEW.status='injecting'
           BEGIN SELECT RAISE(FAIL,'legacy failure between canonical claim and mirror'); END;",
    )
    .unwrap();
    let first = repository
        .get_agent_delivery("msg_cancel_a")
        .unwrap()
        .unwrap();
    let injecting = next(&first, "injecting");
    assert!(repository
        .compare_and_swap_agent_delivery_status(&first, &injecting)
        .is_err());
    assert_eq!(
        repository.get_agent_delivery(&first.id).unwrap(),
        Some(first.clone()),
        "canonical status cannot commit before legacy mirror"
    );
    let mirror: String = db
        .query_row(
            "SELECT status FROM team_deliveries WHERE id=?1",
            [&first.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mirror, "pending");
    db.execute_batch("DROP TRIGGER prevent_cas_mirror").unwrap();
    assert!(repository
        .compare_and_swap_agent_delivery_status(&first, &injecting)
        .unwrap());
    assert_eq!(
        db.query_row(
            "SELECT status FROM team_deliveries WHERE id=?1",
            [&first.id],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "injecting"
    );
    let stale_legacy = runtime_core::TeamDeliveryRecord {
        id: first.id.clone(),
        message_id: first.message_id.clone(),
        team_id: "team_cas".into(),
        recipient_agent_id: first.recipient_agent_id.clone(),
        provider: first.provider.clone(),
        status: "pending".into(),
        effective_policy: None,
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 1,
        updated_at: 1,
    };
    assert!(matches!(
        repository.upsert_team_delivery(&stale_legacy),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn frozen_or_retired_owner_is_checked_inside_the_native_claim_transaction() {
    let (temp, repository) = seed();
    let root = temp.path().join("cas-owner");
    std::fs::create_dir(&root).unwrap();
    let workspace = repository
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.display().to_string(),
                    display_name: None,
                },
                OperationActor::operator("cas-retirement-test"),
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .workspace;
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE agent_messages SET context_kind='workspace_team',workspace_id=?1
         WHERE id='msg_cancel'",
        [&workspace.workspace_id],
    )
    .unwrap();
    let expected = repository
        .get_agent_delivery("msg_cancel_a")
        .unwrap()
        .unwrap();
    let injecting = next(&expected, "injecting");
    db.execute(
        "UPDATE workspaces SET lifecycle_state='retired' WHERE workspace_id=?1",
        [&workspace.workspace_id],
    )
    .unwrap();
    assert!(matches!(
        repository.compare_and_swap_agent_delivery_status(&expected, &injecting),
        Err(RuntimeError::Conflict(_))
    ));
    db.execute(
        "UPDATE workspaces SET lifecycle_state='active' WHERE workspace_id=?1",
        [&workspace.workspace_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO runtime_operations(
           operation_id,workspace_id,kind,actor_kind,actor_id,
           normalized_request_hash,normalized_request_json,phase,created_at,updated_at
         ) VALUES ('op_cas_freeze',?1,'workspace_retire','operator','test','sha','{}',
                   'requested',1,1)",
        [&workspace.workspace_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO workspace_retirement_fences(
           workspace_id,operation_id,phase,snapshot_json,created_at,updated_at
         ) VALUES (?1,'op_cas_freeze','frozen','{}',1,1)",
        [&workspace.workspace_id],
    )
    .unwrap();
    assert!(matches!(
        repository.compare_and_swap_agent_delivery_status(&expected, &injecting),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository.get_agent_delivery(&expected.id).unwrap(),
        Some(expected.clone()),
    );
    db.execute(
        "DELETE FROM workspace_retirement_fences WHERE workspace_id=?1",
        [&workspace.workspace_id],
    )
    .unwrap();
    assert!(repository
        .compare_and_swap_agent_delivery_status(&expected, &injecting)
        .unwrap());
}
