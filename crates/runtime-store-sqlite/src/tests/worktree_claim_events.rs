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
