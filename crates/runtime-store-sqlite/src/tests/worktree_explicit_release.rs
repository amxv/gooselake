use super::*;
use runtime_core::ManagedWorktreeClaimTransition::{Claim, Release};
use runtime_core::RuntimeError;

#[test]
fn explicit_release_rejects_later_live_cwd_only_binding() {
    let (temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    let worktree = reader
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    let writer = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    writer
        .upsert_session(&SessionRecord {
            worktree_id: None,
            cwd: Some(worktree.worktree_cwd.clone()),
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
    let requested = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..claim.clone()
    };
    assert!(matches!(
        reader.release_managed_worktree_claim_if_unbound(&requested, &worktree.worktree_cwd),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        writer
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(claim)
    );
    assert_eq!(writer.list_runtime_events(None, None, 10).unwrap().len(), 1);
}

#[test]
fn explicit_release_checks_current_worktree_route_after_independent_change() {
    let (temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    let original = reader
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    let writer = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let new_cwd = "/tmp/worktrees/new-native-route";
    writer
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            worktree_cwd: new_cwd.into(),
            updated_at: 250,
            ..original.clone()
        })
        .unwrap();
    writer
        .upsert_session(&SessionRecord {
            worktree_id: None,
            cwd: Some(new_cwd.into()),
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
    assert!(matches!(
        reader.release_managed_worktree_claim_if_unbound(
            &ManagedWorktreeClaimRecord {
                released_at: Some(300),
                ..claim.clone()
            },
            &original.worktree_cwd,
        ),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        writer
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(claim)
    );
    assert_eq!(writer.list_runtime_events(None, None, 10).unwrap().len(), 1);
}

#[test]
fn explicit_release_allows_closed_native_binding_and_exact_retry() {
    let (_temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    let worktree = reader
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    reader
        .upsert_session(&SessionRecord {
            worktree_id: Some(worktree.id),
            cwd: Some(worktree.worktree_cwd.clone()),
            status: "closed".into(),
            ..sample_session()
        })
        .unwrap();
    let requested = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..claim
    };
    assert_eq!(
        reader
            .release_managed_worktree_claim_if_unbound(&requested, &worktree.worktree_cwd)
            .unwrap(),
        requested
    );
    assert_eq!(
        reader
            .release_managed_worktree_claim_if_unbound(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(301),
                    ..requested.clone()
                },
                &worktree.worktree_cwd,
            )
            .unwrap(),
        requested
    );
    let events = reader.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].kind, "worktree.released");
    assert_eq!(events[1].payload["claim"]["released_at"], 300);
}

#[test]
fn generic_member_removal_release_keeps_existing_semantics() {
    let (_temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    let worktree = reader
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    reader
        .upsert_session(&SessionRecord {
            worktree_id: Some(worktree.id),
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
    let released = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..claim
    };
    assert_eq!(
        reader
            .transition_managed_worktree_claim(&released, Release)
            .unwrap(),
        released
    );
    assert_eq!(reader.list_runtime_events(None, None, 10).unwrap().len(), 2);
}
