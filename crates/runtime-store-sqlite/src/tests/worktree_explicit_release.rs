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
        reader.release_managed_worktree_claim_if_unbound(
            &requested,
            &claim,
            &worktree.worktree_cwd
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
fn explicit_release_rejects_later_live_nested_cwd_binding() {
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
            cwd: Some(format!("{}/nested/source", worktree.worktree_cwd)),
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
    let requested = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..claim.clone()
    };
    let result = reader.release_managed_worktree_claim_if_unbound(
        &requested,
        &claim,
        &worktree.worktree_cwd,
    );
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "a live nested native cwd must block explicit release: {result:?}"
    );
    assert_eq!(
        writer
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(claim)
    );
    assert_eq!(writer.list_runtime_events(None, None, 10).unwrap().len(), 1);
}

#[test]
fn explicit_release_does_not_confuse_sibling_cwd_prefix_with_native_binding() {
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
            cwd: Some(format!("{}-other/subdir", worktree.worktree_cwd)),
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
    let requested = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..claim.clone()
    };
    assert_eq!(
        reader
            .release_managed_worktree_claim_if_unbound(&requested, &claim, &worktree.worktree_cwd)
            .unwrap(),
        requested
    );
    assert_eq!(writer.list_runtime_events(None, None, 10).unwrap().len(), 2);
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
            &claim,
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
        ..claim.clone()
    };
    assert_eq!(
        reader
            .release_managed_worktree_claim_if_unbound(&requested, &claim, &worktree.worktree_cwd)
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
                &requested,
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

#[test]
fn stale_explicit_release_must_not_release_newer_journaled_reclaim() {
    let (temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    // Explicit release hydrates this older active claim before the service's
    // per-repository lock; it will submit the release later.
    let observed = reader
        .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
        .unwrap()
        .unwrap();
    let worktree = reader
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();

    // Another real SQLite writer journaled a release AND a new generation
    // of the same (worktree_id, session_id) claim. Session is unbound, so the
    // live-session fence alone cannot protect this newer Critical authority.
    let writer = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    writer
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: Some(150),
                ..claim.clone()
            },
            Release,
        )
        .unwrap();
    let newer = ManagedWorktreeClaimRecord {
        created_at: 200,
        ..claim.clone()
    };
    writer
        .transition_managed_worktree_claim(&newer, Claim)
        .unwrap();
    let stale_release = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..observed.clone()
    };
    let result = reader.release_managed_worktree_claim_if_unbound(
        &stale_release,
        &observed,
        &worktree.worktree_cwd,
    );
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "stale explicit release must preserve a later journaled claim generation: {result:?}"
    );
    assert_eq!(
        writer
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(newer),
    );
    let events = writer.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.len(), 3, "no fourth Critical release event");
    assert_eq!(events[2].kind, "worktree.claimed");
}
