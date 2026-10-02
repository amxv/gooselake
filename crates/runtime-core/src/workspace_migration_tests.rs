use super::*;
use crate::{ManagedWorktreeClaimRecord, ManagedWorktreeRecord};

fn identity(root: &str) -> RepositoryIdentity {
    RepositoryIdentity {
        canonical_root: format!("/{root}"),
        git_common_dir: format!("/{root}/.git"),
        fingerprint: format!("repo_v2_{root}"),
    }
}

fn session(id: &str, cwd: Option<&str>) -> SessionRecord {
    SessionRecord {
        id: id.to_string(),
        provider: "codex".to_string(),
        status: "active".to_string(),
        cwd: cwd.map(str::to_string),
        model: None,
        permission_mode: None,
        system_prompt: None,
        metadata: json!({}),
        provider_session_ref: None,
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 1,
        updated_at: 1,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

fn resolver(path: &Path) -> Result<RepositoryIdentity, RuntimeError> {
    match path.to_string_lossy().as_ref() {
        "/repo" | "/repo/wt-a" | "/repo/wt-b" => Ok(identity("repo")),
        "/other" => Ok(identity("other")),
        value => Err(RuntimeError::InvalidState(format!("missing {value}"))),
    }
}

#[test]
fn worktrees_from_one_repository_converge_to_one_workspace() {
    let mut first = session("a", Some("/repo/wt-a"));
    first.worktree_id = Some("wt_a".to_string());
    let mut second = session("b", Some("/repo/wt-b"));
    second.worktree_id = Some("wt_b".to_string());
    let state = RuntimeHydratedState {
        sessions: vec![first, second],
        teams: vec![TeamRecord {
            id: "team".to_string(),
            name: "ignored display name".to_string(),
            lead_agent_id: "a".to_string(),
            created_by: "user".to_string(),
            created_at: 1,
            updated_at: 1,
            deleted_at: None,
        }],
        team_members: vec![
            member("team", "a", Some("wt_a")),
            member("team", "b", Some("wt_b")),
        ],
        managed_worktrees: vec![
            worktree("wt_a", "/repo/wt-a"),
            worktree("wt_b", "/repo/wt-b"),
        ],
        ..Default::default()
    };
    let status = plan_with_resolver(&state, &[], resolver);
    assert_eq!(status.unresolved_subjects, 0);
    let team = find(&status, LegacyWorkspaceMigrationSubjectKind::Team, "team");
    assert_eq!(team.repository_fingerprint.as_deref(), Some("repo_v2_repo"));
}

#[test]
fn mixed_repository_team_is_unresolved_without_guessing() {
    let state = RuntimeHydratedState {
        sessions: vec![session("a", Some("/repo")), session("b", Some("/other"))],
        teams: vec![TeamRecord {
            id: "team".to_string(),
            name: "same name says nothing".to_string(),
            lead_agent_id: "a".to_string(),
            created_by: "user".to_string(),
            created_at: 1,
            updated_at: 1,
            deleted_at: None,
        }],
        team_members: vec![member("team", "a", None), member("team", "b", None)],
        ..Default::default()
    };
    let status = plan_with_resolver(&state, &[], resolver);
    let team = find(&status, LegacyWorkspaceMigrationSubjectKind::Team, "team");
    assert_eq!(
        team.classification,
        LegacyWorkspaceMigrationClassification::Unresolved
    );
    assert_eq!(team.reason_code, "mixed_repository_roots");
    assert!(status.cutover_blocked);
}

#[test]
fn active_session_without_repository_evidence_stays_unresolved() {
    let state = RuntimeHydratedState {
        sessions: vec![session("orphan", None)],
        ..Default::default()
    };
    let status = plan_with_resolver(&state, &[], resolver);
    let orphan = find(
        &status,
        LegacyWorkspaceMigrationSubjectKind::Session,
        "orphan",
    );
    assert_eq!(
        orphan.classification,
        LegacyWorkspaceMigrationClassification::Unresolved
    );
    assert_eq!(orphan.reason_code, "missing_repository_evidence");
}

#[test]
fn terminal_session_and_deleted_team_are_archived_history() {
    let mut closed = session("closed", None);
    closed.status = "closed".to_string();
    closed.closed_at = Some(3);
    let state = RuntimeHydratedState {
        sessions: vec![closed],
        teams: vec![TeamRecord {
            id: "deleted".to_string(),
            name: "history".to_string(),
            lead_agent_id: "closed".to_string(),
            created_by: "user".to_string(),
            created_at: 1,
            updated_at: 3,
            deleted_at: Some(3),
        }],
        team_members: vec![member("deleted", "closed", None)],
        ..Default::default()
    };
    let status = plan_with_resolver(&state, &[], resolver);
    assert_eq!(status.unresolved_subjects, 0);
    assert_eq!(status.archived_subjects, 3);
}

#[test]
fn closed_member_is_archived_without_blocking_active_team_mapping() {
    let active = session("active", Some("/repo"));
    let mut closed = session("closed", Some("/other"));
    closed.status = "closed".to_string();
    closed.closed_at = Some(3);
    let state = RuntimeHydratedState {
        sessions: vec![active, closed],
        teams: vec![TeamRecord {
            id: "team".to_string(),
            name: "history must not become authority".to_string(),
            lead_agent_id: "active".to_string(),
            created_by: "user".to_string(),
            created_at: 1,
            updated_at: 3,
            deleted_at: None,
        }],
        team_members: vec![
            member("team", "active", None),
            member("team", "closed", None),
        ],
        ..Default::default()
    };

    let status = plan_with_resolver(&state, &[], resolver);
    let team = find(&status, LegacyWorkspaceMigrationSubjectKind::Team, "team");
    assert_eq!(
        team.classification,
        LegacyWorkspaceMigrationClassification::Mapped
    );
    assert_eq!(team.repository_fingerprint.as_deref(), Some("repo_v2_repo"));

    let historical_member = find(
        &status,
        LegacyWorkspaceMigrationSubjectKind::TeamMember,
        "team:closed",
    );
    assert_eq!(
        historical_member.classification,
        LegacyWorkspaceMigrationClassification::ArchivedHistory
    );
    assert_eq!(historical_member.reason_code, "historical_membership");
    assert!(!status.cutover_blocked);
}

#[test]
fn missing_repository_path_stays_unresolved() {
    let state = RuntimeHydratedState {
        sessions: vec![session("missing", Some("/missing"))],
        ..Default::default()
    };

    let status = plan_with_resolver(&state, &[], resolver);
    let missing = find(
        &status,
        LegacyWorkspaceMigrationSubjectKind::Session,
        "missing",
    );
    assert_eq!(
        missing.classification,
        LegacyWorkspaceMigrationClassification::Unresolved
    );
    assert_eq!(missing.reason_code, "repository_unavailable");
    assert!(status.cutover_blocked);
}

#[test]
fn missing_explicit_worktree_blocks_cwd_only_guess() {
    let mut active = session("agent", Some("/repo"));
    active.worktree_id = Some("missing".to_string());
    let state = RuntimeHydratedState {
        sessions: vec![active],
        ..Default::default()
    };
    let status = plan_with_resolver(&state, &[], resolver);
    let agent = find(
        &status,
        LegacyWorkspaceMigrationSubjectKind::Session,
        "agent",
    );
    assert_eq!(
        agent.classification,
        LegacyWorkspaceMigrationClassification::Unresolved
    );
    assert_eq!(agent.reason_code, "incomplete_worktree_evidence");
}

#[test]
fn contradictory_claims_block_session_mapping() {
    let state = RuntimeHydratedState {
        sessions: vec![session("agent", Some("/repo"))],
        managed_worktrees: vec![worktree("other", "/other")],
        managed_worktree_claims: vec![ManagedWorktreeClaimRecord {
            worktree_id: "other".to_string(),
            session_id: "agent".to_string(),
            claim_role: "primary".to_string(),
            created_at: 1,
            released_at: None,
        }],
        ..Default::default()
    };
    let status = plan_with_resolver(&state, &[], resolver);
    let agent = find(
        &status,
        LegacyWorkspaceMigrationSubjectKind::Session,
        "agent",
    );
    assert_eq!(agent.reason_code, "conflicting_repository_evidence");
}

#[test]
fn contradictory_worktree_paths_are_unresolved() {
    let state = RuntimeHydratedState {
        managed_worktrees: vec![ManagedWorktreeRecord {
            id: "contradictory".to_string(),
            repo_root: "/repo".to_string(),
            worktree_root: "/managed".to_string(),
            worktree_cwd: "/other".to_string(),
            branch_name: "branch".to_string(),
            worktree_name: "contradictory".to_string(),
            unified_workspace_path: "contradictory".to_string(),
            deletion_policy: "retain_on_last_claim".to_string(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 1,
            updated_at: 1,
        }],
        ..Default::default()
    };
    let status = plan_with_resolver(&state, &[], resolver);
    let worktree = find(
        &status,
        LegacyWorkspaceMigrationSubjectKind::ManagedWorktree,
        "contradictory",
    );
    assert_eq!(
        worktree.classification,
        LegacyWorkspaceMigrationClassification::Unresolved
    );
    assert_eq!(worktree.reason_code, "repository_unavailable");
}

fn member(team: &str, agent: &str, worktree_id: Option<&str>) -> TeamMemberRecord {
    TeamMemberRecord {
        team_id: team.to_string(),
        agent_id: agent.to_string(),
        title: None,
        joined_at: 1,
        added_by: "user".to_string(),
        creator_agent_id: None,
        creator_compaction_subscription: "auto".to_string(),
        worktree_id: worktree_id.map(str::to_string),
    }
}

fn worktree(id: &str, cwd: &str) -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: id.to_string(),
        repo_root: cwd.to_string(),
        worktree_root: "/managed".to_string(),
        worktree_cwd: cwd.to_string(),
        branch_name: format!("branch-{id}"),
        worktree_name: id.to_string(),
        unified_workspace_path: id.to_string(),
        deletion_policy: "retain_on_last_claim".to_string(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 1,
        updated_at: 1,
    }
}

fn find<'a>(
    status: &'a LegacyWorkspaceMigrationStatus,
    kind: LegacyWorkspaceMigrationSubjectKind,
    id: &str,
) -> &'a LegacyWorkspaceMigrationSubject {
    status
        .subjects
        .iter()
        .find(|subject| subject.subject_kind == kind && subject.subject_id == id)
        .expect("migration subject")
}
