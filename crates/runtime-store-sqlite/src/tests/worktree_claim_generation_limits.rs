use super::*;
use runtime_core::{
    ManagedWorktreeClaimTransition::{Claim, Release},
    RuntimeError,
};

#[test]
fn invalid_or_missing_observed_generation_never_authorizes_active_or_terminal_release() {
    for terminal in [false, true] {
        let (_temp, repository, intent) = super::worktree_claim_events::fixture();
        let mut observed = repository
            .transition_managed_worktree_claim(&intent, Claim)
            .unwrap();
        if terminal {
            observed = repository
                .transition_managed_worktree_claim(
                    &ManagedWorktreeClaimRecord {
                        released_at: Some(150),
                        ..observed
                    },
                    Release,
                )
                .unwrap();
        }
        let events = repository.list_runtime_events(None, None, 20).unwrap();
        for token in [None, Some(0), Some(i64::MAX as u64 + 1)] {
            let stale = ManagedWorktreeClaimRecord {
                claim_generation: token,
                released_at: Some(999),
                ..observed.clone()
            };
            assert!(matches!(
                repository.transition_managed_worktree_claim(&stale, Release),
                Err(RuntimeError::Conflict(_))
            ));
            let bad_observation = ManagedWorktreeClaimRecord {
                claim_generation: token,
                ..observed.clone()
            };
            assert!(matches!(
                repository.release_managed_worktree_claim_if_unbound(
                    &stale,
                    &bad_observation,
                    "/tmp/worktrees/claim-event"
                ),
                Err(RuntimeError::Conflict(_))
            ));
            assert_eq!(
                repository
                    .get_managed_worktree_claim(&observed.worktree_id, &observed.session_id)
                    .unwrap(),
                Some(observed.clone())
            );
            assert_eq!(
                repository.list_runtime_events(None, None, 20).unwrap(),
                events
            );
        }
        let released = repository
            .transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(200),
                    ..observed.clone()
                },
                Release,
            )
            .unwrap();
        let count = repository
            .list_runtime_events(None, None, 20)
            .unwrap()
            .len();
        assert_eq!(
            repository
                .transition_managed_worktree_claim(
                    &ManagedWorktreeClaimRecord {
                        released_at: Some(300),
                        ..released.clone()
                    },
                    Release
                )
                .unwrap(),
            released
        );
        assert_eq!(
            repository
                .list_runtime_events(None, None, 20)
                .unwrap()
                .len(),
            count
        );
        assert_eq!(released.released_at, Some(if terminal { 150 } else { 200 }));
    }
}

#[test]
fn generation_signed_ceiling_allows_existing_lifetime_settlement_but_no_successor() {
    let (temp, repository, intent) = super::worktree_claim_events::fixture();
    repository
        .transition_managed_worktree_claim(&intent, Claim)
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE managed_worktree_claims SET claim_generation=?1",
        [i64::MAX - 1],
    )
    .unwrap();
    let penultimate = repository
        .get_managed_worktree_claim(&intent.worktree_id, &intent.session_id)
        .unwrap()
        .unwrap();
    let released = repository
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: Some(150),
                ..penultimate.clone()
            },
            Release,
        )
        .unwrap();
    assert_eq!(released.claim_generation, Some((i64::MAX - 1) as u64));
    let last = repository
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: None,
                ..released
            },
            Claim,
        )
        .unwrap();
    assert_eq!(last.claim_generation, Some(i64::MAX as u64));
    let before = repository.list_runtime_events(None, None, 20).unwrap();
    assert_eq!(
        repository
            .transition_managed_worktree_claim(&last, Claim)
            .unwrap(),
        last
    );
    assert_eq!(
        repository.list_runtime_events(None, None, 20).unwrap(),
        before
    );
    let terminal = repository
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: Some(200),
                ..last
            },
            Release,
        )
        .unwrap();
    let before = repository.list_runtime_events(None, None, 20).unwrap();
    let result = repository.transition_managed_worktree_claim(
        &ManagedWorktreeClaimRecord {
            released_at: None,
            ..terminal.clone()
        },
        Claim,
    );
    assert!(
        matches!(result, Err(RuntimeError::InvalidState(_))),
        "{result:?}"
    );
    assert_eq!(
        repository
            .get_managed_worktree_claim(&intent.worktree_id, &intent.session_id)
            .unwrap(),
        Some(terminal.clone())
    );
    assert_eq!(
        repository.list_runtime_events(None, None, 20).unwrap(),
        before
    );
    assert_eq!(
        repository
            .transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(900),
                    ..terminal.clone()
                },
                Release
            )
            .unwrap(),
        terminal
    );
}

#[test]
fn migration_52_preserves_v51_claim_bytes_and_historical_json_has_no_authority() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    super::workspace_agent::apply_schema_through(&repository.database_path, 51);
    let db = open_connection(&repository.database_path).unwrap();
    db.execute_batch("INSERT INTO sessions(id,provider,status,metadata_json,created_at,updated_at)
        VALUES ('legacy-session','codex','closed','{}',1,2);
        INSERT INTO managed_worktrees(id,repo_root,worktree_root,worktree_cwd,branch_name,worktree_name,
            unified_workspace_path,deletion_policy,created_at,updated_at)
        VALUES ('legacy-wt','/fixture/repo','/fixture/worktrees','/fixture/worktrees/one','gg/one','one','fixture','retain_on_last_claim',1,1);
        INSERT INTO managed_worktree_claims(worktree_id,session_id,claim_role,created_at,released_at)
        VALUES ('legacy-wt','legacy-session',' OwNeR ',17,29);").unwrap();
    let raw="{\"claim\": {\"worktree_id\":\"legacy-wt\",\"session_id\":\"legacy-session\",\"claim_role\":\" OwNeR \",\"created_at\":17,\"released_at\":29}}";
    db.execute("INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
        VALUES ('legacy-claim-event','worktree','legacy-wt',1,'worktree.released',1,?1,29)",[raw]).unwrap();
    drop(db);
    let historical = repository
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims;
    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].claim_generation, None);
    assert_eq!(historical[0].claim_role, " OwNeR ");
    repository.initialize_schema().unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    let retained: String = db
        .query_row(
            "SELECT payload_json FROM runtime_events WHERE event_id='legacy-claim-event'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        retained, raw,
        "migration must not rewrite historical event JSON"
    );
    let payload: serde_json::Value = serde_json::from_str(&retained).unwrap();
    let old: ManagedWorktreeClaimRecord = serde_json::from_value(payload["claim"].clone()).unwrap();
    assert_eq!(old.claim_generation, None);
    assert_eq!(historical, vec![old.clone()]);
    let persisted = repository
        .get_managed_worktree_claim("legacy-wt", "legacy-session")
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted,
        ManagedWorktreeClaimRecord {
            claim_generation: Some(1),
            ..old.clone()
        }
    );
    assert!(matches!(
        repository.transition_managed_worktree_claim(&old, Release),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository
            .transition_managed_worktree_claim(&persisted, Release)
            .unwrap(),
        persisted
    );
    assert_eq!(
        repository
            .list_runtime_events(None, None, 20)
            .unwrap()
            .len(),
        1
    );
    for bad in ["0", "-1", "'not-an-integer'"] {
        assert!(db
            .execute(
                &format!("UPDATE managed_worktree_claims SET claim_generation={bad}"),
                []
            )
            .is_err());
    }
    assert_eq!(
        repository
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        vec![persisted]
    );
}

#[test]
fn worktree_claim_hydration_never_downgrades_damaged_current_or_unrecorded_schema() {
    for bad in ["0", "-1", "'invalid'"] {
        let (temp, repository, intent) = super::worktree_claim_events::fixture();
        repository
            .transition_managed_worktree_claim(&intent, Claim)
            .unwrap();
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        db.execute_batch(&format!("PRAGMA ignore_check_constraints=ON; UPDATE managed_worktree_claims SET claim_generation={bad}")).unwrap();
        let events = repository.list_runtime_events(None, None, 20).unwrap();
        assert!(repository.hydrate_runtime_state().is_err(), "{bad}");
        assert_eq!(
            repository.list_runtime_events(None, None, 20).unwrap(),
            events
        );
    }
    for with_claim in [false, true] {
        let (temp, repository, intent) = super::worktree_claim_events::fixture();
        if with_claim {
            repository
                .transition_managed_worktree_claim(&intent, Claim)
                .unwrap();
        }
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        db.execute_batch("ALTER TABLE managed_worktree_claims DROP COLUMN claim_generation")
            .unwrap();
        let events = repository.list_runtime_events(None, None, 20).unwrap();
        let error = repository.hydrate_runtime_state().unwrap_err();
        assert!(
            matches!(error, RuntimeError::Bootstrap(ref message) if message.contains("complete recorded pre-52")),
            "{error:?}"
        );
        assert_eq!(
            repository.list_runtime_events(None, None, 20).unwrap(),
            events
        );
        let columns: i64 = db.query_row("SELECT COUNT(*) FROM pragma_table_info('managed_worktree_claims') WHERE name='claim_generation'", [], |r| r.get(0)).unwrap();
        assert_eq!(columns, 0, "inspection must not repair the damaged schema");
    }
    for damage in [
        "DELETE FROM schema_migrations WHERE version=20",
        "DELETE FROM schema_migrations",
        "DROP TABLE schema_migrations",
        "INSERT INTO schema_migrations(version,applied_at) VALUES(52,1)",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let repository = repo(&temp);
        super::workspace_agent::apply_schema_through(&repository.database_path, 51);
        let db = open_connection(&repository.database_path).unwrap();
        db.execute_batch(damage).unwrap();
        assert!(repository.hydrate_runtime_state().is_err(), "{damage}");
    }
}
