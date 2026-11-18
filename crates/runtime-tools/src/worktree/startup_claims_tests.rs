use super::*;
use runtime_core::{RuntimeHydratedState, SessionRecord};

fn session(id: &str) -> SessionRecord {
    SessionRecord {
        id: id.into(),
        provider: "codex".into(),
        status: "closed".into(),
        cwd: None,
        model: None,
        permission_mode: None,
        system_prompt: None,
        metadata: serde_json::json!({}),
        provider_session_ref: None,
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 1,
        updated_at: 1,
        closed_at: Some(1),
        failure_code: None,
        failure_message: None,
    }
}
fn winner() -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: "W".into(),
        repo_root: "/fixture/repo".into(),
        worktree_root: "/fixture/worktrees".into(),
        worktree_cwd: "/fixture/worktrees/w".into(),
        branch_name: "gg/w".into(),
        worktree_name: "w".into(),
        unified_workspace_path: "fixture".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 1,
        updated_at: 1,
    }
}
fn claim(
    id: &str,
    session: &str,
    role: &str,
    created: i64,
    generation: u64,
) -> ManagedWorktreeClaimRecord {
    ManagedWorktreeClaimRecord {
        worktree_id: id.into(),
        session_id: session.into(),
        claim_role: role.into(),
        created_at: created,
        released_at: None,
        claim_generation: Some(generation),
    }
}
fn map(
    rows: Vec<ManagedWorktreeClaimRecord>,
) -> BTreeMap<(String, String), ManagedWorktreeClaimRecord> {
    rows.into_iter()
        .map(|r| ((r.worktree_id.clone(), r.session_id.clone()), r))
        .collect()
}

#[test]
fn permutations_use_destination_token_and_preserve_raw_sources_and_terminal_history() {
    let dest = claim("W", "S", "consumer", 70, 4);
    let left = claim(" L1 ", "S", " owner ", 40, 9);
    let right = claim("L2", "S", "consumer", 30, 2);
    let terminal = ManagedWorktreeClaimRecord {
        released_at: Some(80),
        ..claim("L3", "S", "owner", 10, 6)
    };
    let rows = [dest.clone(), left.clone(), right.clone()];
    let winners = BTreeMap::from([("W".into(), winner())]);
    let aliases = BTreeMap::from([
        (" L1 ".into(), "W".into()),
        ("L2".into(), "W".into()),
        ("L3".into(), "W".into()),
    ]);
    let expected = map(vec![
        ManagedWorktreeClaimRecord {
            claim_role: "owner".into(),
            created_at: 30,
            ..dest
        },
        ManagedWorktreeClaimRecord {
            released_at: Some(100),
            ..left
        },
        ManagedWorktreeClaimRecord {
            released_at: Some(100),
            ..right
        },
    ]);
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let mut claims = order
            .into_iter()
            .map(|i| rows[i].clone())
            .collect::<Vec<_>>();
        claims.push(terminal.clone());
        let state = RuntimeHydratedState {
            sessions: vec![session("S")],
            managed_worktree_claims: claims,
            ..Default::default()
        };
        let actual = map(startup_claims::plan(&state, &winners, &aliases, 100).unwrap());
        assert_eq!(actual, expected);
        assert!(
            !actual.contains_key(&("L1".into(), "S".into())),
            "raw ID is not a trimmed alias"
        );
        assert!(
            !actual.contains_key(&("L3".into(), "S".into())),
            "terminal source is immutable"
        );
    }
}

#[test]
fn destination_absence_and_whitespace_session_ids_have_separate_authority() {
    let winners = BTreeMap::from([("W".into(), winner())]);
    let aliases = BTreeMap::from([(" L1 ".into(), "W".into())]);
    let left = claim(" L1 ", "S", "owner", 40, 9);
    let raw_session = claim(" L1 ", " S ", "owner", 50, 2);
    let raw_dest = claim("W", " S ", "consumer", 70, 8);
    let state = RuntimeHydratedState {
        sessions: vec![session("S"), session(" S ")],
        managed_worktree_claims: vec![left.clone(), raw_session.clone(), raw_dest.clone()],
        ..Default::default()
    };
    let actual = map(startup_claims::plan(&state, &winners, &aliases, 100).unwrap());
    assert_eq!(
        actual[&("W".into(), "S".into())],
        ManagedWorktreeClaimRecord {
            worktree_id: "W".into(),
            claim_generation: None,
            ..left.clone()
        }
    );
    assert_eq!(
        actual[&("W".into(), " S ".into())],
        ManagedWorktreeClaimRecord {
            claim_role: "owner".into(),
            created_at: 50,
            ..raw_dest
        }
    );
    assert_eq!(
        actual[&(" L1 ".into(), "S".into())],
        ManagedWorktreeClaimRecord {
            released_at: Some(100),
            ..left
        }
    );
    assert_eq!(
        actual[&(" L1 ".into(), " S ".into())],
        ManagedWorktreeClaimRecord {
            released_at: Some(100),
            ..raw_session
        }
    );
}
