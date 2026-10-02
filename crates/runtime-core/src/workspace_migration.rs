use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::workspace::{normalize_idempotency_key, normalized_json_hash, opaque_id, unix_time_ms};
use crate::{
    resolve_repository_identity, OperationActor, RepositoryIdentity, RuntimeError,
    RuntimeHydratedState, SessionRecord, TeamMemberRecord, TeamRecord, WorkspaceRecord,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyWorkspaceMigrationSubjectKind {
    Session,
    Team,
    TeamMember,
    ManagedWorktree,
    WorktreeClaim,
}

impl LegacyWorkspaceMigrationSubjectKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Team => "team",
            Self::TeamMember => "team_member",
            Self::ManagedWorktree => "managed_worktree",
            Self::WorktreeClaim => "worktree_claim",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "session" => Some(Self::Session),
            "team" => Some(Self::Team),
            "team_member" => Some(Self::TeamMember),
            "managed_worktree" => Some(Self::ManagedWorktree),
            "worktree_claim" => Some(Self::WorktreeClaim),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyWorkspaceMigrationClassification {
    Mapped,
    ArchivedHistory,
    Unresolved,
}

impl LegacyWorkspaceMigrationClassification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mapped => "mapped",
            Self::ArchivedHistory => "archived_history",
            Self::Unresolved => "unresolved",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "mapped" => Some(Self::Mapped),
            "archived_history" => Some(Self::ArchivedHistory),
            "unresolved" => Some(Self::Unresolved),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyWorkspaceMigrationResolutionSource {
    Deterministic,
    OperatorMap,
    OperatorArchive,
}

impl LegacyWorkspaceMigrationResolutionSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::OperatorMap => "operator_map",
            Self::OperatorArchive => "operator_archive",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "deterministic" => Some(Self::Deterministic),
            "operator_map" => Some(Self::OperatorMap),
            "operator_archive" => Some(Self::OperatorArchive),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyWorkspaceMigrationSubject {
    pub subject_kind: LegacyWorkspaceMigrationSubjectKind,
    pub subject_id: String,
    pub classification: LegacyWorkspaceMigrationClassification,
    pub workspace_id: Option<String>,
    pub canonical_root: Option<String>,
    pub git_common_dir: Option<String>,
    pub repository_fingerprint: Option<String>,
    pub reason_code: String,
    pub evidence: Value,
    pub resolution_source: LegacyWorkspaceMigrationResolutionSource,
    pub applied_at: Option<i64>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyWorkspaceMigrationStatus {
    pub subjects: Vec<LegacyWorkspaceMigrationSubject>,
    pub mapped_subjects: usize,
    pub archived_subjects: usize,
    pub unresolved_subjects: usize,
    pub unresolved_active_sessions: usize,
    pub cutover_blocked: bool,
}

impl LegacyWorkspaceMigrationStatus {
    pub fn from_subjects(mut subjects: Vec<LegacyWorkspaceMigrationSubject>) -> Self {
        subjects.sort_by(|left, right| {
            (left.subject_kind, left.subject_id.as_str())
                .cmp(&(right.subject_kind, right.subject_id.as_str()))
        });
        let mapped_subjects = subjects
            .iter()
            .filter(|subject| {
                subject.classification == LegacyWorkspaceMigrationClassification::Mapped
            })
            .count();
        let archived_subjects = subjects
            .iter()
            .filter(|subject| {
                subject.classification == LegacyWorkspaceMigrationClassification::ArchivedHistory
            })
            .count();
        let unresolved_subjects = subjects
            .iter()
            .filter(|subject| {
                subject.classification == LegacyWorkspaceMigrationClassification::Unresolved
            })
            .count();
        let unresolved_active_sessions = subjects
            .iter()
            .filter(|subject| {
                subject.subject_kind == LegacyWorkspaceMigrationSubjectKind::Session
                    && subject.classification == LegacyWorkspaceMigrationClassification::Unresolved
            })
            .count();
        Self {
            subjects,
            mapped_subjects,
            archived_subjects,
            unresolved_subjects,
            unresolved_active_sessions,
            cutover_blocked: unresolved_subjects > 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyWorkspaceMigrationApplyCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request_hash: String,
    pub normalized_request: Value,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyWorkspaceMigrationApplyResponse {
    pub operation_id: String,
    pub created_workspaces: usize,
    pub mapped_subjects_applied: usize,
    pub archived_subjects_applied: usize,
    pub owned_sessions_created: usize,
    pub unresolved_subjects: usize,
    pub cutover_blocked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyWorkspaceMigrationResolutionAction {
    Map,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyWorkspaceMigrationResolutionRequest {
    pub action: LegacyWorkspaceMigrationResolutionAction,
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyWorkspaceMigrationResolutionCommand {
    pub operation_id: String,
    pub actor: OperationActor,
    pub idempotency_key: Option<String>,
    pub normalized_request_hash: String,
    pub normalized_request: Value,
    pub subject_kind: LegacyWorkspaceMigrationSubjectKind,
    pub subject_id: String,
    pub action: LegacyWorkspaceMigrationResolutionAction,
    pub workspace_id: Option<String>,
    pub requested_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyWorkspaceMigrationResolutionResponse {
    pub operation_id: String,
    pub subject: LegacyWorkspaceMigrationSubject,
}

pub fn prepare_legacy_workspace_migration_apply(
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<LegacyWorkspaceMigrationApplyCommand, RuntimeError> {
    validate_actor(&actor)?;
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = json!({
        "migration": "legacy_workspace_authority",
        "action": "apply"
    });
    Ok(LegacyWorkspaceMigrationApplyCommand {
        operation_id: opaque_id("op"),
        actor,
        idempotency_key,
        normalized_request_hash: normalized_json_hash(&normalized_request)?,
        normalized_request,
        requested_at: unix_time_ms()?,
    })
}

pub fn prepare_legacy_workspace_migration_resolution(
    subject_kind: LegacyWorkspaceMigrationSubjectKind,
    subject_id: String,
    request: LegacyWorkspaceMigrationResolutionRequest,
    actor: OperationActor,
    idempotency_key: Option<String>,
) -> Result<LegacyWorkspaceMigrationResolutionCommand, RuntimeError> {
    validate_actor(&actor)?;
    let subject_id = subject_id.trim().to_string();
    if subject_id.is_empty() {
        return Err(RuntimeError::InvalidState(
            "migration subject id cannot be empty".to_string(),
        ));
    }
    let workspace_id = request
        .workspace_id
        .map(|workspace_id| workspace_id.trim().to_string())
        .filter(|workspace_id| !workspace_id.is_empty());
    match request.action {
        LegacyWorkspaceMigrationResolutionAction::Map if workspace_id.is_none() => {
            return Err(RuntimeError::InvalidState(
                "workspace_id is required when mapping a migration subject".to_string(),
            ));
        }
        LegacyWorkspaceMigrationResolutionAction::Archive if workspace_id.is_some() => {
            return Err(RuntimeError::InvalidState(
                "workspace_id must be omitted when archiving a migration subject".to_string(),
            ));
        }
        _ => {}
    }
    let idempotency_key = normalize_idempotency_key(idempotency_key)?;
    let normalized_request = json!({
        "migration": "legacy_workspace_authority",
        "action": request.action,
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "workspace_id": workspace_id,
    });
    Ok(LegacyWorkspaceMigrationResolutionCommand {
        operation_id: opaque_id("op"),
        actor,
        idempotency_key,
        normalized_request_hash: normalized_json_hash(&normalized_request)?,
        normalized_request,
        subject_kind,
        subject_id,
        action: request.action,
        workspace_id,
        requested_at: unix_time_ms()?,
    })
}

pub fn plan_legacy_workspace_migration(
    state: &RuntimeHydratedState,
    workspaces: &[WorkspaceRecord],
) -> LegacyWorkspaceMigrationStatus {
    plan_with_resolver(state, workspaces, |path| resolve_repository_identity(path))
}

fn plan_with_resolver<F>(
    state: &RuntimeHydratedState,
    workspaces: &[WorkspaceRecord],
    mut resolver: F,
) -> LegacyWorkspaceMigrationStatus
where
    F: FnMut(&Path) -> Result<RepositoryIdentity, RuntimeError>,
{
    let workspace_by_root = workspaces
        .iter()
        .map(|workspace| {
            (
                workspace.canonical_root.clone(),
                workspace.workspace_id.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let sessions = state
        .sessions
        .iter()
        .map(|session| (session.id.as_str(), session))
        .collect::<BTreeMap<_, _>>();
    let teams = state
        .teams
        .iter()
        .map(|team| (team.id.as_str(), team))
        .collect::<BTreeMap<_, _>>();
    let worktrees = state
        .managed_worktrees
        .iter()
        .map(|worktree| (worktree.id.as_str(), worktree))
        .collect::<BTreeMap<_, _>>();

    let mut path_cache = BTreeMap::<String, Result<RepositoryIdentity, String>>::new();
    let mut worktree_identity = BTreeMap::<String, Result<RepositoryIdentity, String>>::new();
    let mut subjects = Vec::new();

    for worktree in &state.managed_worktrees {
        if worktree.repo_root.starts_with("__gg_tombstoned__/")
            || worktree.worktree_cwd.starts_with("__gg_tombstoned__/")
        {
            subjects.push(subject(
                LegacyWorkspaceMigrationSubjectKind::ManagedWorktree,
                worktree.id.clone(),
                LegacyWorkspaceMigrationClassification::ArchivedHistory,
                None,
                "worktree_tombstoned",
                json!({"repo_root": worktree.repo_root, "worktree_cwd": worktree.worktree_cwd}),
                &workspace_by_root,
            ));
            continue;
        }
        let resolved = resolve_worktree_identity(worktree, &mut path_cache, &mut resolver);
        match &resolved {
            Ok(identity) => subjects.push(subject(
                LegacyWorkspaceMigrationSubjectKind::ManagedWorktree,
                worktree.id.clone(),
                LegacyWorkspaceMigrationClassification::Mapped,
                Some(identity.clone()),
                "repository_identity_resolved",
                json!({
                    "repo_root": worktree.repo_root,
                    "worktree_cwd": worktree.worktree_cwd,
                    "branch_name": worktree.branch_name,
                }),
                &workspace_by_root,
            )),
            Err(error) => subjects.push(subject(
                LegacyWorkspaceMigrationSubjectKind::ManagedWorktree,
                worktree.id.clone(),
                LegacyWorkspaceMigrationClassification::Unresolved,
                None,
                "repository_unavailable",
                json!({
                    "repo_root": worktree.repo_root,
                    "worktree_cwd": worktree.worktree_cwd,
                    "error": error,
                }),
                &workspace_by_root,
            )),
        }
        worktree_identity.insert(worktree.id.clone(), resolved);
    }

    let mut session_subjects = BTreeMap::<String, LegacyWorkspaceMigrationSubject>::new();
    for session in &state.sessions {
        let planned = plan_session(
            session,
            &state.team_members,
            &state.managed_worktree_claims,
            &worktree_identity,
            &mut path_cache,
            &mut resolver,
            &workspace_by_root,
        );
        session_subjects.insert(session.id.clone(), planned.clone());
        subjects.push(planned);
    }

    let mut team_subjects = BTreeMap::<String, LegacyWorkspaceMigrationSubject>::new();
    for team in &state.teams {
        let planned = plan_team(
            team,
            &state.team_members,
            &sessions,
            &session_subjects,
            &workspace_by_root,
        );
        team_subjects.insert(team.id.clone(), planned.clone());
        subjects.push(planned);
    }

    for member in &state.team_members {
        subjects.push(plan_team_member(
            member,
            &teams,
            &sessions,
            &team_subjects,
            &session_subjects,
            &workspace_by_root,
        ));
    }

    for claim in &state.managed_worktree_claims {
        subjects.push(plan_worktree_claim(
            claim.worktree_id.as_str(),
            claim.session_id.as_str(),
            claim.released_at,
            worktrees.get(claim.worktree_id.as_str()).copied(),
            &worktree_identity,
            &session_subjects,
            &workspace_by_root,
        ));
    }

    LegacyWorkspaceMigrationStatus::from_subjects(subjects)
}

fn plan_session<F>(
    session: &SessionRecord,
    members: &[TeamMemberRecord],
    claims: &[crate::ManagedWorktreeClaimRecord],
    worktree_identity: &BTreeMap<String, Result<RepositoryIdentity, String>>,
    path_cache: &mut BTreeMap<String, Result<RepositoryIdentity, String>>,
    resolver: &mut F,
    workspace_by_root: &BTreeMap<String, String>,
) -> LegacyWorkspaceMigrationSubject
where
    F: FnMut(&Path) -> Result<RepositoryIdentity, RuntimeError>,
{
    if is_terminal_session(session) {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Session,
            session.id.clone(),
            LegacyWorkspaceMigrationClassification::ArchivedHistory,
            None,
            "terminal_session_history",
            json!({"status": session.status, "closed_at": session.closed_at}),
            workspace_by_root,
        );
    }

    let mut candidates = BTreeMap::<String, RepositoryIdentity>::new();
    let mut evidence = Vec::<Value>::new();
    let mut strong_missing = Vec::<String>::new();
    if let Some(cwd) = session
        .cwd
        .as_deref()
        .map(str::trim)
        .filter(|cwd| !cwd.is_empty())
    {
        match resolve_cached(cwd, path_cache, resolver) {
            Ok(identity) => {
                evidence.push(
                    json!({"source":"session_cwd", "path":cwd, "fingerprint":identity.fingerprint}),
                );
                candidates.insert(identity.fingerprint.clone(), identity);
            }
            Err(error) => evidence.push(json!({"source":"session_cwd", "path":cwd, "error":error})),
        }
    }

    let mut referenced_worktrees = BTreeSet::<String>::new();
    if let Some(worktree_id) = session.worktree_id.as_deref() {
        referenced_worktrees.insert(worktree_id.to_string());
    }
    for member in members
        .iter()
        .filter(|member| member.agent_id == session.id)
    {
        if let Some(worktree_id) = member.worktree_id.as_deref() {
            referenced_worktrees.insert(worktree_id.to_string());
        }
    }
    for claim in claims
        .iter()
        .filter(|claim| claim.session_id == session.id && claim.released_at.is_none())
    {
        referenced_worktrees.insert(claim.worktree_id.clone());
    }
    for worktree_id in referenced_worktrees {
        match worktree_identity.get(&worktree_id) {
            Some(Ok(identity)) => {
                evidence.push(json!({"source":"worktree_association", "worktree_id":worktree_id, "fingerprint":identity.fingerprint}));
                candidates.insert(identity.fingerprint.clone(), identity.clone());
            }
            Some(Err(error)) => {
                strong_missing.push(worktree_id.clone());
                evidence.push(json!({"source":"worktree_association", "worktree_id":worktree_id, "error":error}));
            }
            None => {
                strong_missing.push(worktree_id.clone());
                evidence.push(json!({"source":"worktree_association", "worktree_id":worktree_id, "error":"missing managed worktree record"}));
            }
        }
    }

    if !strong_missing.is_empty() {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Session,
            session.id.clone(),
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "incomplete_worktree_evidence",
            json!({"evidence":evidence, "unresolved_worktree_ids":strong_missing}),
            workspace_by_root,
        );
    }
    if candidates.len() > 1 {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Session,
            session.id.clone(),
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "conflicting_repository_evidence",
            json!({
                "evidence": evidence,
                "candidate_fingerprints": candidates.keys().collect::<Vec<_>>()
            }),
            workspace_by_root,
        );
    }
    if let Some(identity) = candidates.into_values().next() {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Session,
            session.id.clone(),
            LegacyWorkspaceMigrationClassification::Mapped,
            Some(identity),
            "single_repository_consensus",
            json!({"evidence":evidence}),
            workspace_by_root,
        );
    }
    subject(
        LegacyWorkspaceMigrationSubjectKind::Session,
        session.id.clone(),
        LegacyWorkspaceMigrationClassification::Unresolved,
        None,
        if session.cwd.is_some() {
            "repository_unavailable"
        } else {
            "missing_repository_evidence"
        },
        json!({"evidence":evidence}),
        workspace_by_root,
    )
}

fn plan_team(
    team: &TeamRecord,
    members: &[TeamMemberRecord],
    sessions: &BTreeMap<&str, &SessionRecord>,
    session_subjects: &BTreeMap<String, LegacyWorkspaceMigrationSubject>,
    workspace_by_root: &BTreeMap<String, String>,
) -> LegacyWorkspaceMigrationSubject {
    if team.deleted_at.is_some() {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Team,
            team.id.clone(),
            LegacyWorkspaceMigrationClassification::ArchivedHistory,
            None,
            "deleted_team_history",
            json!({"deleted_at":team.deleted_at}),
            workspace_by_root,
        );
    }

    let team_members = members
        .iter()
        .filter(|member| member.team_id == team.id)
        .collect::<Vec<_>>();
    let mut roots = BTreeMap::<String, RepositoryIdentity>::new();
    let mut unresolved_members = Vec::<String>::new();
    let mut active_members = Vec::<String>::new();
    for member in team_members {
        let Some(session) = sessions.get(member.agent_id.as_str()).copied() else {
            unresolved_members.push(member.agent_id.clone());
            continue;
        };
        if is_terminal_session(session) {
            continue;
        }
        active_members.push(member.agent_id.clone());
        match session_subjects.get(&member.agent_id) {
            Some(subject)
                if subject.classification == LegacyWorkspaceMigrationClassification::Mapped =>
            {
                if let (Some(fingerprint), Some(canonical_root), Some(git_common_dir)) = (
                    subject.repository_fingerprint.clone(),
                    subject.canonical_root.clone(),
                    subject.git_common_dir.clone(),
                ) {
                    roots.insert(
                        fingerprint.clone(),
                        RepositoryIdentity {
                            canonical_root,
                            git_common_dir,
                            fingerprint,
                        },
                    );
                } else {
                    unresolved_members.push(member.agent_id.clone());
                }
            }
            _ => unresolved_members.push(member.agent_id.clone()),
        }
    }
    if !unresolved_members.is_empty() {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Team,
            team.id.clone(),
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "member_authority_unresolved",
            json!({"active_member_ids":active_members, "unresolved_member_ids":unresolved_members}),
            workspace_by_root,
        );
    }
    if roots.len() > 1 {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Team,
            team.id.clone(),
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "mixed_repository_roots",
            json!({"active_member_ids":active_members, "candidate_fingerprints":roots.keys().collect::<Vec<_>>()}),
            workspace_by_root,
        );
    }
    if let Some(identity) = roots.into_values().next() {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::Team,
            team.id.clone(),
            LegacyWorkspaceMigrationClassification::Mapped,
            Some(identity),
            "member_repository_consensus",
            json!({"active_member_ids":active_members}),
            workspace_by_root,
        );
    }
    subject(
        LegacyWorkspaceMigrationSubjectKind::Team,
        team.id.clone(),
        LegacyWorkspaceMigrationClassification::Unresolved,
        None,
        "no_active_repository_evidence",
        json!({"active_member_ids":active_members}),
        workspace_by_root,
    )
}

fn plan_team_member(
    member: &TeamMemberRecord,
    teams: &BTreeMap<&str, &TeamRecord>,
    sessions: &BTreeMap<&str, &SessionRecord>,
    team_subjects: &BTreeMap<String, LegacyWorkspaceMigrationSubject>,
    session_subjects: &BTreeMap<String, LegacyWorkspaceMigrationSubject>,
    workspace_by_root: &BTreeMap<String, String>,
) -> LegacyWorkspaceMigrationSubject {
    let id = format!("{}:{}", member.team_id, member.agent_id);
    let Some(team) = teams.get(member.team_id.as_str()).copied() else {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::TeamMember,
            id,
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "missing_team_record",
            json!({"team_id":member.team_id,"session_id":member.agent_id}),
            workspace_by_root,
        );
    };
    let Some(session) = sessions.get(member.agent_id.as_str()).copied() else {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::TeamMember,
            id,
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "missing_session_record",
            json!({"team_id":member.team_id,"session_id":member.agent_id}),
            workspace_by_root,
        );
    };
    if team.deleted_at.is_some() || is_terminal_session(session) {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::TeamMember,
            id,
            LegacyWorkspaceMigrationClassification::ArchivedHistory,
            None,
            "historical_membership",
            json!({"team_deleted_at":team.deleted_at,"session_status":session.status}),
            workspace_by_root,
        );
    }
    let team_subject = team_subjects.get(&member.team_id);
    let session_subject = session_subjects.get(&member.agent_id);
    if let (Some(team_subject), Some(session_subject)) = (team_subject, session_subject) {
        if team_subject.classification == LegacyWorkspaceMigrationClassification::Mapped
            && session_subject.classification == LegacyWorkspaceMigrationClassification::Mapped
            && team_subject.repository_fingerprint == session_subject.repository_fingerprint
        {
            return subject_from_other(
                LegacyWorkspaceMigrationSubjectKind::TeamMember,
                id,
                session_subject,
                "membership_repository_consensus",
                json!({"team_id":member.team_id,"session_id":member.agent_id,"worktree_id":member.worktree_id}),
                workspace_by_root,
            );
        }
    }
    subject(
        LegacyWorkspaceMigrationSubjectKind::TeamMember,
        id,
        LegacyWorkspaceMigrationClassification::Unresolved,
        None,
        "membership_authority_unresolved",
        json!({"team_id":member.team_id,"session_id":member.agent_id,"worktree_id":member.worktree_id}),
        workspace_by_root,
    )
}

fn plan_worktree_claim(
    worktree_id: &str,
    session_id: &str,
    released_at: Option<i64>,
    worktree: Option<&crate::ManagedWorktreeRecord>,
    worktree_identity: &BTreeMap<String, Result<RepositoryIdentity, String>>,
    session_subjects: &BTreeMap<String, LegacyWorkspaceMigrationSubject>,
    workspace_by_root: &BTreeMap<String, String>,
) -> LegacyWorkspaceMigrationSubject {
    let id = format!("{worktree_id}:{session_id}");
    if released_at.is_some() {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::WorktreeClaim,
            id,
            LegacyWorkspaceMigrationClassification::ArchivedHistory,
            None,
            "released_worktree_claim",
            json!({"worktree_id":worktree_id,"session_id":session_id,"released_at":released_at}),
            workspace_by_root,
        );
    }
    if worktree.is_none() {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::WorktreeClaim,
            id,
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "missing_worktree_record",
            json!({"worktree_id":worktree_id,"session_id":session_id}),
            workspace_by_root,
        );
    }
    let Some(Ok(worktree_identity)) = worktree_identity.get(worktree_id) else {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::WorktreeClaim,
            id,
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "worktree_authority_unresolved",
            json!({"worktree_id":worktree_id,"session_id":session_id}),
            workspace_by_root,
        );
    };
    let Some(session_subject) = session_subjects.get(session_id) else {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::WorktreeClaim,
            id,
            LegacyWorkspaceMigrationClassification::Unresolved,
            None,
            "session_authority_unresolved",
            json!({"worktree_id":worktree_id,"session_id":session_id}),
            workspace_by_root,
        );
    };
    if session_subject.classification == LegacyWorkspaceMigrationClassification::Mapped
        && session_subject.repository_fingerprint.as_deref()
            == Some(worktree_identity.fingerprint.as_str())
    {
        return subject(
            LegacyWorkspaceMigrationSubjectKind::WorktreeClaim,
            id,
            LegacyWorkspaceMigrationClassification::Mapped,
            Some(worktree_identity.clone()),
            "claim_repository_consensus",
            json!({"worktree_id":worktree_id,"session_id":session_id}),
            workspace_by_root,
        );
    }
    subject(
        LegacyWorkspaceMigrationSubjectKind::WorktreeClaim,
        id,
        LegacyWorkspaceMigrationClassification::Unresolved,
        None,
        "claim_repository_conflict",
        json!({"worktree_id":worktree_id,"session_id":session_id}),
        workspace_by_root,
    )
}

fn resolve_worktree_identity<F>(
    worktree: &crate::ManagedWorktreeRecord,
    cache: &mut BTreeMap<String, Result<RepositoryIdentity, String>>,
    resolver: &mut F,
) -> Result<RepositoryIdentity, String>
where
    F: FnMut(&Path) -> Result<RepositoryIdentity, RuntimeError>,
{
    let cwd_result = resolve_cached(worktree.worktree_cwd.as_str(), cache, resolver);
    let repo_result = resolve_cached(worktree.repo_root.as_str(), cache, resolver);
    match (cwd_result, repo_result) {
        (Ok(cwd_identity), Ok(repo_identity)) => {
            if cwd_identity.fingerprint != repo_identity.fingerprint {
                Err(format!(
                    "worktree cwd resolves to {} but repository root resolves to {}",
                    cwd_identity.fingerprint, repo_identity.fingerprint
                ))
            } else {
                Ok(cwd_identity)
            }
        }
        (Ok(identity), Err(_)) | (Err(_), Ok(identity)) => Ok(identity),
        (Err(cwd_error), Err(repo_error)) => Err(format!(
            "worktree cwd: {cwd_error}; repository root: {repo_error}"
        )),
    }
}

fn resolve_cached<F>(
    path: &str,
    cache: &mut BTreeMap<String, Result<RepositoryIdentity, String>>,
    resolver: &mut F,
) -> Result<RepositoryIdentity, String>
where
    F: FnMut(&Path) -> Result<RepositoryIdentity, RuntimeError>,
{
    let path = path.trim();
    if path.is_empty() {
        return Err("path is empty".to_string());
    }
    if let Some(result) = cache.get(path) {
        return result.clone();
    }
    let result = resolver(Path::new(path)).map_err(|error| error.to_string());
    cache.insert(path.to_string(), result.clone());
    result
}

fn subject(
    subject_kind: LegacyWorkspaceMigrationSubjectKind,
    subject_id: String,
    classification: LegacyWorkspaceMigrationClassification,
    identity: Option<RepositoryIdentity>,
    reason_code: &str,
    evidence: Value,
    workspace_by_root: &BTreeMap<String, String>,
) -> LegacyWorkspaceMigrationSubject {
    let workspace_id = identity
        .as_ref()
        .and_then(|identity| workspace_by_root.get(&identity.canonical_root).cloned());
    LegacyWorkspaceMigrationSubject {
        subject_kind,
        subject_id,
        classification,
        workspace_id,
        canonical_root: identity
            .as_ref()
            .map(|identity| identity.canonical_root.clone()),
        git_common_dir: identity
            .as_ref()
            .map(|identity| identity.git_common_dir.clone()),
        repository_fingerprint: identity.map(|identity| identity.fingerprint),
        reason_code: reason_code.to_string(),
        evidence,
        resolution_source: LegacyWorkspaceMigrationResolutionSource::Deterministic,
        applied_at: None,
        updated_at: None,
    }
}

fn subject_from_other(
    subject_kind: LegacyWorkspaceMigrationSubjectKind,
    subject_id: String,
    other: &LegacyWorkspaceMigrationSubject,
    reason_code: &str,
    evidence: Value,
    workspace_by_root: &BTreeMap<String, String>,
) -> LegacyWorkspaceMigrationSubject {
    let identity = match (
        other.canonical_root.clone(),
        other.git_common_dir.clone(),
        other.repository_fingerprint.clone(),
    ) {
        (Some(canonical_root), Some(git_common_dir), Some(fingerprint)) => {
            Some(RepositoryIdentity {
                canonical_root,
                git_common_dir,
                fingerprint,
            })
        }
        _ => None,
    };
    subject(
        subject_kind,
        subject_id,
        LegacyWorkspaceMigrationClassification::Mapped,
        identity,
        reason_code,
        evidence,
        workspace_by_root,
    )
}

fn is_terminal_session(session: &SessionRecord) -> bool {
    session.closed_at.is_some() || matches!(session.status.as_str(), "closed" | "failed")
}

fn validate_actor(actor: &OperationActor) -> Result<(), RuntimeError> {
    if actor.identifier.trim().is_empty() {
        return Err(RuntimeError::InvalidState(
            "operation actor identifier cannot be empty".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "workspace_migration_tests.rs"]
mod tests;
