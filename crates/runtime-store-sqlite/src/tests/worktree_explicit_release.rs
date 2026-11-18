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
    let newer = writer
        .transition_managed_worktree_claim(&newer, Claim)
        .unwrap();
    assert_eq!(newer.claim_generation, Some(2));
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

const CLAIM_CHILD_DB: &str = "GOOSELAKE_EXPLICIT_CLAIM_OS_CHILD_DB";

#[test]
fn child_os_process_journals_changed_worktree_claim_generation() {
    let Some(path) = std::env::var_os(CLAIM_CHILD_DB) else {
        return;
    };
    // This exact-filter child is inert without the private path to the
    // parent's disposable SQLite fixture.
    let writer = SqliteRuntimeRepository::new(std::path::PathBuf::from(path));
    let old = writer
        .get_managed_worktree_claim("wt_claim_event", "session_1")
        .unwrap()
        .unwrap();
    assert_eq!(old.created_at, 100);
    writer
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: Some(150),
                ..old.clone()
            },
            Release,
        )
        .unwrap();
    writer
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                created_at: 200,
                ..old
            },
            Claim,
        )
        .unwrap();
}

#[test]
fn stale_explicit_release_conflicts_with_independent_os_process_reclaim() {
    let (temp, reader, old) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&old, Claim)
        .unwrap();
    let observed = reader
        .get_managed_worktree_claim(&old.worktree_id, &old.session_id)
        .unwrap()
        .unwrap();
    let cwd = reader
        .get_managed_worktree(&old.worktree_id)
        .unwrap()
        .unwrap()
        .worktree_cwd;

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::worktree_explicit_release::child_os_process_journals_changed_worktree_claim_generation",
            "--nocapture",
        ])
        .env(CLAIM_CHILD_DB, temp.path().join("runtime.sqlite3"))
        .output()
        .expect("spawn independent OS process on the disposable claim database");
    assert!(
        output.status.success(),
        "child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
        "the child must execute its exact test, not an empty filter"
    );
    let newer = ManagedWorktreeClaimRecord {
        created_at: 200,
        claim_generation: Some(observed.claim_generation.unwrap() + 1),
        ..old.clone()
    };
    let release = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..observed.clone()
    };
    let result = reader.release_managed_worktree_claim_if_unbound(&release, &observed, &cwd);
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "stale value-observed explicit release must not revoke another process's claim: {result:?}"
    );
    assert_eq!(
        reader
            .get_managed_worktree_claim(&old.worktree_id, &old.session_id)
            .unwrap(),
        Some(newer)
    );
    let events = reader.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.len(), 3, "no fourth Critical release event");
    assert_eq!(events[2].kind, "worktree.claimed");
}

const BINDING_CHILD_DB: &str = "GOOSELAKE_EXPLICIT_BINDING_OS_CHILD_DB";
const BINDING_CHILD_ROUTE: &str = "GOOSELAKE_EXPLICIT_BINDING_OS_CHILD_ROUTE";

#[test]
fn child_os_process_persists_live_checkout_route() {
    let Some(path) = std::env::var_os(BINDING_CHILD_DB) else {
        return;
    };
    // Only a parent-owned disposable SQLite path makes this child write.
    let writer = SqliteRuntimeRepository::new(std::path::PathBuf::from(path));
    let worktree = writer
        .get_managed_worktree("wt_claim_event")
        .unwrap()
        .unwrap();
    let cwd = match std::env::var(BINDING_CHILD_ROUTE).unwrap().as_str() {
        "nested" => format!("{}/nested/source", worktree.worktree_cwd),
        "sibling" => format!("{}-other/subdir", worktree.worktree_cwd),
        other => panic!("unsupported private child fixture route: {other}"),
    };
    writer
        .upsert_session(&SessionRecord {
            worktree_id: None,
            cwd: Some(cwd),
            status: "ready".into(),
            ..sample_session()
        })
        .unwrap();
}

fn persist_live_checkout_route_from_child(database_path: &std::path::Path, route: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::worktree_explicit_release::child_os_process_persists_live_checkout_route",
            "--nocapture",
        ])
        .env(BINDING_CHILD_DB, database_path)
        .env(BINDING_CHILD_ROUTE, route)
        .output()
        .expect("spawn independent OS process on the disposable binding database");
    assert!(
        output.status.success(),
        "child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
        "child must execute exact binding fixture, not an empty filter"
    );
}

#[test]
fn explicit_release_rejects_live_nested_route_from_independent_os_process() {
    let (temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    let observed = reader
        .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
        .unwrap()
        .unwrap();
    let cwd = reader
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap()
        .worktree_cwd;
    // A different OS process persists its native route after the parent
    // hydrated the claim, outside any in-memory repository lock.
    persist_live_checkout_route_from_child(&temp.path().join("runtime.sqlite3"), "nested");
    let requested = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..observed.clone()
    };
    let result = reader.release_managed_worktree_claim_if_unbound(&requested, &observed, &cwd);
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "live native checkout descendant must fence explicit release: {result:?}"
    );
    assert_eq!(
        reader
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(observed)
    );
    assert_eq!(reader.list_runtime_events(None, None, 10).unwrap().len(), 1);
}

#[test]
fn explicit_release_allows_sibling_route_from_independent_os_process() {
    let (temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    let observed = reader
        .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
        .unwrap()
        .unwrap();
    let cwd = reader
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap()
        .worktree_cwd;
    persist_live_checkout_route_from_child(&temp.path().join("runtime.sqlite3"), "sibling");
    let requested = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..observed.clone()
    };
    assert_eq!(
        reader
            .release_managed_worktree_claim_if_unbound(&requested, &observed, &cwd)
            .unwrap(),
        requested
    );
    assert_eq!(reader.list_runtime_events(None, None, 10).unwrap().len(), 2);
}

#[test]
fn stale_generic_member_removal_release_must_not_revoke_journaled_reclaim() {
    let (temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
    // Member-removal snapshots active claims before independently journaling
    // their Release transitions, leaving a window for another SQLite writer.
    let observed = reader
        .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
        .unwrap()
        .unwrap();
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
    let newer = writer
        .transition_managed_worktree_claim(&newer, Claim)
        .unwrap();
    assert_eq!(newer.claim_generation, Some(2));

    let stale_release = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..observed
    };
    let result = reader.transition_managed_worktree_claim(&stale_release, Release);
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "member-removal stale Release must not revoke a different durable claim: {result:?}"
    );
    assert_eq!(
        reader
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(newer)
    );
    let events = reader.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.len(), 3, "no fourth member-removal release event");
    assert_eq!(events[2].kind, "worktree.claimed");
}

#[test]
fn stale_generic_release_rejects_different_claim_role_after_reclaim() {
    let (temp, reader, claim) = super::worktree_claim_events::fixture();
    reader
        .transition_managed_worktree_claim(&claim, Claim)
        .unwrap();
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
    // Deliberately reuse the same timestamp; a changed role must still be
    // fenced while an equivalent legacy trim/case normalization is allowed.
    let newer = ManagedWorktreeClaimRecord {
        claim_role: "consumer".into(),
        ..claim.clone()
    };
    let newer = writer
        .transition_managed_worktree_claim(&newer, Claim)
        .unwrap();
    assert_eq!(newer.claim_generation, Some(2));
    let stale = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..claim.clone()
    };
    let result = reader.transition_managed_worktree_claim(&stale, Release);
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "a different durable role must not be released by stale member removal: {result:?}"
    );
    assert_eq!(
        reader
            .get_managed_worktree_claim(&claim.worktree_id, &claim.session_id)
            .unwrap(),
        Some(newer)
    );
    let events = reader.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[2].kind, "worktree.claimed");
}
