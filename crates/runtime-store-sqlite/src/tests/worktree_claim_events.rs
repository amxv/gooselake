use super::*;
use runtime_core::ManagedWorktreeClaimTransition::{Claim, Release};

fn fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    ManagedWorktreeClaimRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    repository.upsert_session(&sample_session()).unwrap();
    repository
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            id: "wt_claim_event".into(),
            repo_root: "/tmp/claim-repo".into(),
            worktree_root: "/tmp/worktrees".into(),
            worktree_cwd: "/tmp/worktrees/claim-event".into(),
            branch_name: "gg/claim-event".into(),
            worktree_name: "claim-event".into(),
            unified_workspace_path: "claim-repo".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 1,
            updated_at: 1,
        })
        .unwrap();
    let claim = ManagedWorktreeClaimRecord {
        worktree_id: "wt_claim_event".into(),
        session_id: "session_1".into(),
        claim_role: "owner".into(),
        created_at: 100,
        released_at: None,
    };
    (temp, repository, claim)
}

#[test]
fn missing_rebind_authority_schema_fails_closed_without_rewriting_claim() {
    let (temp, repository, claim) = fixture();
    repository.upsert_managed_worktree_claim(&claim).unwrap();
    let connection = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    connection
        .execute_batch("DROP TABLE workspace_agent_rebinds;")
        .unwrap();

    let rewritten = ManagedWorktreeClaimRecord {
        claim_role: "consumer".into(),
        released_at: Some(500),
        ..claim.clone()
    };
    assert!(matches!(
        repository.upsert_managed_worktree_claim(&rewritten),
        Err(runtime_core::RuntimeError::Bootstrap(_))
    ));
    assert_eq!(
        repository
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(claim.clone()),
    );
    assert!(matches!(
        repository.transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: Some(500),
                ..claim.clone()
            },
            Release,
        ),
        Err(runtime_core::RuntimeError::Bootstrap(_))
    ));
    assert_eq!(
        repository
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(claim.clone()),
    );

    // A damaged history must not be mistaken for a legitimate pre-v10
    // database merely because its highest remaining version is below 10.
    connection
        .execute_batch(
            "DELETE FROM schema_migrations WHERE version >= 10;
             DELETE FROM schema_migrations WHERE version = 5;",
        )
        .unwrap();
    assert!(matches!(
        repository.upsert_managed_worktree_claim(&rewritten),
        Err(runtime_core::RuntimeError::Bootstrap(_))
    ));
    assert_eq!(
        repository
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(claim),
    );
}

#[test]
fn historical_claim_repair_cannot_rewrite_a_journaled_terminal_release() {
    let (temp, repository, claim) = fixture();
    repository
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    let released = ManagedWorktreeClaimRecord {
        released_at: Some(200),
        ..claim.clone()
    };
    repository
        .transition_managed_worktree_claim(&released, Release)
        .unwrap();
    let before = repository
        .list_runtime_events(
            Some((RuntimeEventScope::Worktree, &claim.worktree_id)),
            None,
            10,
        )
        .unwrap();
    assert_eq!(before.len(), 2);

    for stale in [
        claim.clone(),
        ManagedWorktreeClaimRecord {
            claim_role: "consumer".into(),
            ..released.clone()
        },
        ManagedWorktreeClaimRecord {
            created_at: 199,
            ..released.clone()
        },
        ManagedWorktreeClaimRecord {
            released_at: Some(300),
            ..released.clone()
        },
    ] {
        assert!(matches!(
            repository.upsert_managed_worktree_claim(&stale),
            Err(runtime_core::RuntimeError::Conflict(_))
        ));
        assert_eq!(
            repository
                .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
                .unwrap(),
            Some(released.clone()),
        );
    }

    // An identical repair neither updates the terminal record nor emits an
    // event; the journaled Claim path remains the only way to reactivate it.
    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER forbid_terminal_upsert BEFORE UPDATE ON managed_worktree_claims
         BEGIN SELECT RAISE(FAIL,'terminal claim must not be re-upserted'); END;",
    )
    .unwrap();
    repository.upsert_managed_worktree_claim(&released).unwrap();
    assert_eq!(
        repository
            .list_runtime_events(
                Some((RuntimeEventScope::Worktree, &claim.worktree_id)),
                None,
                10
            )
            .unwrap(),
        before,
    );
}

#[test]
fn historical_claim_repair_preserves_journaled_active_reclaim_and_unrelated_legacy_normalization() {
    let (temp, repository, claim) = fixture();
    let legacy = ManagedWorktreeClaimRecord {
        claim_role: " owner ".into(),
        ..claim.clone()
    };
    repository.upsert_managed_worktree_claim(&legacy).unwrap();
    repository
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: Some(200),
                ..legacy.clone()
            },
            Release,
        )
        .unwrap();
    let reclaimed = ManagedWorktreeClaimRecord {
        claim_role: "consumer".into(),
        created_at: 300,
        released_at: None,
        ..legacy.clone()
    };
    repository
        .transition_managed_worktree_claim(&reclaimed, Claim)
        .unwrap();
    let before = repository
        .list_runtime_events(
            Some((RuntimeEventScope::Worktree, &claim.worktree_id)),
            None,
            10,
        )
        .unwrap();
    assert_eq!(before.len(), 2);

    for stale in [
        ManagedWorktreeClaimRecord {
            claim_role: "owner".into(),
            ..legacy.clone()
        },
        ManagedWorktreeClaimRecord {
            created_at: 301,
            ..reclaimed.clone()
        },
        ManagedWorktreeClaimRecord {
            claim_role: "owner".into(),
            ..reclaimed.clone()
        },
    ] {
        assert!(matches!(
            repository.upsert_managed_worktree_claim(&stale),
            Err(runtime_core::RuntimeError::Conflict(_)),
        ));
        assert_eq!(
            repository
                .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
                .unwrap(),
            Some(reclaimed.clone()),
        );
    }

    let db = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER forbid_journaled_active_update BEFORE UPDATE ON managed_worktree_claims
         WHEN OLD.session_id='session_1'
         BEGIN SELECT RAISE(FAIL,'journaled active claim must not be upserted'); END;",
    )
    .unwrap();
    repository
        .upsert_managed_worktree_claim(&reclaimed)
        .unwrap();
    assert_eq!(
        repository
            .list_runtime_events(
                Some((RuntimeEventScope::Worktree, &claim.worktree_id)),
                None,
                10,
            )
            .unwrap(),
        before,
        "historical retry cannot append or alter the journal",
    );

    // A journaled claim for a different session on the same worktree does
    // not prevent normalization of this session's old unjournaled row.
    let (_temp, repository, claim) = fixture();
    let mut other_session = sample_session();
    other_session.id = "unrelated_claim_session".into();
    repository.upsert_session(&other_session).unwrap();
    let foreign = ManagedWorktreeClaimRecord {
        session_id: other_session.id,
        ..claim.clone()
    };
    repository
        .transition_managed_worktree_claim(&foreign, Claim)
        .unwrap();
    let legacy = ManagedWorktreeClaimRecord {
        claim_role: " owner ".into(),
        ..claim.clone()
    };
    repository.upsert_managed_worktree_claim(&legacy).unwrap();
    repository.upsert_managed_worktree_claim(&claim).unwrap();
    assert_eq!(
        repository
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(claim.clone()),
    );
    let events = repository
        .list_runtime_events(
            Some((RuntimeEventScope::Worktree, &claim.worktree_id)),
            None,
            10,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].session_id.as_deref(),
        Some("unrelated_claim_session")
    );
}

#[test]
fn independent_historical_claim_repair_and_journaled_release_preserve_terminal_authority() {
    use std::sync::{Arc, Barrier};

    // The historical role normalizer is deliberately unjournaled. A second
    // process can commit a real Release while startup repairs that legacy
    // row; both writers must serialize under SQLite's IMMEDIATE lock, with
    // exactly one Critical event and no revived terminal claim.
    for round in 0_i64..12 {
        let (temp, repository, claim) = fixture();
        let legacy = ManagedWorktreeClaimRecord {
            claim_role: " owner ".into(),
            ..claim.clone()
        };
        repository.upsert_managed_worktree_claim(&legacy).unwrap();
        let expected_release_at = 200 + round;
        let released_request = ManagedWorktreeClaimRecord {
            released_at: Some(expected_release_at),
            ..legacy.clone()
        };
        let normalized = ManagedWorktreeClaimRecord {
            claim_role: "owner".into(),
            ..legacy
        };
        let path = temp.path().join("runtime.sqlite3");
        let gate = Arc::new(Barrier::new(3));

        let repair_gate = gate.clone();
        let repair_writer = SqliteRuntimeRepository::new(path.clone());
        let repair = std::thread::spawn(move || {
            repair_gate.wait();
            repair_writer.upsert_managed_worktree_claim(&normalized)
        });
        let release_gate = gate.clone();
        let release_writer = SqliteRuntimeRepository::new(path.clone());
        let release = std::thread::spawn(move || {
            release_gate.wait();
            release_writer.transition_managed_worktree_claim(&released_request, Release)
        });
        gate.wait();

        let repair_result = repair.join().unwrap();
        assert!(
            repair_result.is_ok()
                || matches!(repair_result, Err(runtime_core::RuntimeError::Conflict(_))),
            "round {round}: historical repair had unexpected result: {repair_result:?}"
        );
        let committed_release = release.join().unwrap().unwrap();
        assert_eq!(committed_release.released_at, Some(expected_release_at));
        let restarted = SqliteRuntimeRepository::new(path);
        let recovered = restarted
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap();
        assert_eq!(
            recovered,
            Some(committed_release.clone()),
            "round {round}: a concurrent startup writer cannot revive or rewrite the release"
        );
        let events = restarted
            .list_runtime_events(
                Some((RuntimeEventScope::Worktree, &claim.worktree_id)),
                None,
                10,
            )
            .unwrap();
        assert_eq!(events.len(), 1, "round {round}: exactly one Release event");
        assert_eq!(events[0].kind, "worktree.released");
        assert_eq!(
            events[0].payload["claim"]["released_at"],
            expected_release_at
        );
        assert_eq!(
            events[0].payload["claim"]["claim_role"], committed_release.claim_role,
            "round {round}: the Critical event must match the winning serialized row"
        );
    }
}

#[test]
fn claim_release_and_reclaim_are_atomic_exact_once_and_restart_safe() {
    let (temp, _repository, claim) = fixture();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let mut subscriber = store.subscribe_event_commits().unwrap();
    let first = store
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    assert_eq!(first, claim);
    assert!(subscriber.try_recv().is_ok(), "committed journal wakes SSE");

    let mut duplicate = claim.clone();
    duplicate.created_at = 101;
    assert_eq!(
        store
            .transition_managed_worktree_claim(&duplicate, Claim)
            .unwrap(),
        claim
    );
    assert!(matches!(
        subscriber.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    let mut released = claim.clone();
    released.released_at = Some(200);
    assert_eq!(
        store
            .transition_managed_worktree_claim(&released, Release)
            .unwrap(),
        released
    );
    assert!(subscriber.try_recv().is_ok());
    released.released_at = Some(201);
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let prior = restarted
        .transition_managed_worktree_claim(&released, Release)
        .unwrap();
    assert_eq!(prior.released_at, Some(200));

    let reclaimed = ManagedWorktreeClaimRecord {
        created_at: 300,
        released_at: None,
        ..claim
    };
    assert_eq!(
        restarted
            .transition_managed_worktree_claim(&reclaimed, Claim)
            .unwrap(),
        reclaimed
    );
    let rows = restarted
        .list_runtime_events(
            Some((RuntimeEventScope::Worktree, "wt_claim_event")),
            None,
            10,
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        rows.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>(),
        ["worktree.claimed", "worktree.released", "worktree.claimed"]
    );
    let unique_ids = rows
        .iter()
        .map(|row| &row.event_id)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(unique_ids.len(), 3);
    assert_eq!(rows[1].payload["claim"]["released_at"], 200);
    assert_eq!(rows[2].payload["claim"]["created_at"], 300);
    assert_eq!(
        restarted
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        [reclaimed]
    );
}

#[test]
fn journal_insert_failure_rolls_back_claim_and_release_with_no_notification() {
    let (temp, repository, claim) = fixture();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let mut subscriber = store.subscribe_event_commits().unwrap();
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_claim BEFORE INSERT ON runtime_events
        WHEN NEW.kind='worktree.claimed' BEGIN SELECT RAISE(FAIL, 'injected claim journal failure'); END;").unwrap();
    assert!(repository
        .transition_managed_worktree_claim(&claim, Claim)
        .is_err());
    assert!(repository
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims
        .is_empty());
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
    assert!(matches!(
        subscriber.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    connection
        .execute_batch("DROP TRIGGER fail_claim;")
        .unwrap();
    repository
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    assert!(subscriber.try_recv().is_ok());
    connection.execute_batch("CREATE TRIGGER fail_release BEFORE INSERT ON runtime_events
        WHEN NEW.kind='worktree.released' BEGIN SELECT RAISE(FAIL, 'injected release journal failure'); END;").unwrap();
    let released = ManagedWorktreeClaimRecord {
        released_at: Some(200),
        ..claim.clone()
    };
    assert!(repository
        .transition_managed_worktree_claim(&released, Release)
        .is_err());
    assert_eq!(
        repository
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims,
        [claim.clone()]
    );
    assert_eq!(
        repository
            .list_runtime_events(None, None, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        subscriber.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    connection
        .execute_batch("DROP TRIGGER fail_release;")
        .unwrap();
    assert_eq!(
        repository
            .transition_managed_worktree_claim(&released, Release)
            .unwrap(),
        released
    );
    assert!(subscriber.try_recv().is_ok());
}

#[test]
fn independent_sqlite_writers_cannot_claim_two_worktrees_for_one_session() {
    use std::sync::{Arc, Barrier};
    let (temp, repository, claim) = fixture();
    let another = ManagedWorktreeRecord {
        id: "wt_competing".into(),
        repo_root: "/tmp/claim-repo".into(),
        worktree_root: "/tmp/worktrees".into(),
        worktree_cwd: "/tmp/worktrees/competing".into(),
        branch_name: "gg/competing".into(),
        worktree_name: "competing".into(),
        unified_workspace_path: "claim-repo".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 1,
        updated_at: 1,
    };
    repository.upsert_managed_worktree(&another).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for worktree_id in [claim.worktree_id.clone(), another.id] {
        let writer = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
        let rendezvous = barrier.clone();
        let mut desired = claim.clone();
        desired.worktree_id = worktree_id;
        workers.push(std::thread::spawn(move || {
            rendezvous.wait();
            writer.transition_managed_worktree_claim(&desired, Claim)
        }));
    }
    let outcomes = workers
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|row| row.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|row| matches!(row, Err(runtime_core::RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let recovered = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        recovered
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims
            .iter()
            .filter(|row| row.released_at.is_none())
            .count(),
        1
    );
    assert_eq!(
        recovered.list_runtime_events(None, None, 10).unwrap().len(),
        1
    );
}
