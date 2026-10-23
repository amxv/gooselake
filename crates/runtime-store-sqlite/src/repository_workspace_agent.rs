use runtime_core::{
    ProviderKind, RuntimeError, SessionRecord, TeamHookEffectPhase, TeamHookPreAddContext,
    WorkspaceAgentLifecycleState, WorkspaceAgentOnboardingRequest, WorkspaceAgentProfile,
    WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy, WorkspaceSystemNoticeRecord,
};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection, string_to_json};
use crate::repository_team_hooks::effect_grant_is_current;
use crate::repository_team_hooks::{active_workspace_hook_targets_tx, queue_team_hook_trigger_tx};
use crate::repository_workspace_notice::queue_membership_lead_notice_tx;
use crate::SqliteRuntimeRepository;

const AGENT_SELECT: &str = "SELECT
    a.session_id, a.workspace_id, a.identity_alias, a.lifecycle_state,
    p.title, p.title_provenance, p.added_by, p.creator_session_id,
    p.creator_compaction_subscription, p.joined_at, a.recreation_policy_json,
    s.provider_session_ref, s.canonical_provider_session_ref, s.metadata_json,
    a.archived_at, a.archive_reason, a.revision, a.created_at, a.updated_at
 FROM workspace_agents a
 JOIN workspace_agent_profiles p ON p.session_id = a.session_id AND p.workspace_id = a.workspace_id
 JOIN sessions s ON s.id = a.session_id";

impl SqliteRuntimeRepository {
    pub fn create_workspace_agent(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
    ) -> Result<(), RuntimeError> {
        self.create_workspace_agent_with_optional_onboarding(session, agent, None, None)
    }

    pub fn create_workspace_agent_with_onboarding(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
        onboarding: &WorkspaceAgentOnboardingRequest,
    ) -> Result<(), RuntimeError> {
        self.create_workspace_agent_with_optional_onboarding(session, agent, Some(onboarding), None)
    }

    pub fn create_workspace_agent_with_hook_context(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
        onboarding: &WorkspaceAgentOnboardingRequest,
        hooks: Option<&TeamHookPreAddContext>,
    ) -> Result<(), RuntimeError> {
        self.create_workspace_agent_with_optional_onboarding(
            session,
            agent,
            Some(onboarding),
            hooks,
        )
    }

    fn create_workspace_agent_with_optional_onboarding(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
        onboarding: Option<&WorkspaceAgentOnboardingRequest>,
        hooks: Option<&TeamHookPreAddContext>,
    ) -> Result<(), RuntimeError> {
        if session.id != agent.agent_id
            || session.provider != agent.recreation_policy.provider.as_str()
            || session.cwd.as_deref() != Some(agent.recreation_policy.authoritative_cwd.as_str())
        {
            return Err(RuntimeError::ProtocolViolation(
                "workspace agent session does not match recreation policy".to_string(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting workspace agent transaction", error))?;
        let lifecycle = tx
            .query_row(
                "SELECT lifecycle_state FROM workspaces WHERE workspace_id = ?1",
                params![agent.workspace_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace for agent create", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", agent.workspace_id)))?;
        if lifecycle != "active" {
            return Err(RuntimeError::InvalidState(format!(
                "workspace {} is not active",
                agent.workspace_id
            )));
        }

        let verified_pre_add_actor = if let Some(hooks) = hooks {
            if hooks.completed_hook_names.is_empty()
                || hooks.completed_hook_names.len() > 32
                || hooks
                    .onboarding_context_append_text
                    .as_ref()
                    .is_some_and(|v| v.len() > 32 * 1024)
                || hooks
                    .caller_context_text
                    .as_ref()
                    .is_some_and(|v| v.len() > 16 * 1024)
            {
                return Err(RuntimeError::InvalidState(
                    "invalid bounded pre-add context".into(),
                ));
            }
            let mut statement = tx.prepare(
                "SELECT hook_name,phase,application_state,input_json,hook_json FROM workspace_hook_effects
                 WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name='pre_teammate_add'
                 ORDER BY hook_name",
            ).map_err(|error| db_error("failed preparing pre-add effect verification", error))?;
            let rows = statement
                .query_map(params![agent.workspace_id, hooks.parent_event_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })
                .map_err(|error| db_error("failed querying pre-add effect verification", error))?;
            let collected = rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed reading pre-add effects", error))?;
            let mut names = hooks.completed_hook_names.clone();
            names.sort();
            let observed_names = collected
                .iter()
                .map(|row| row.0.clone())
                .collect::<Vec<_>>();
            if observed_names != names
                || collected.iter().any(|row| {
                    row.1 != TeamHookEffectPhase::Succeeded.as_str() || row.2 != "pending"
                })
            {
                return Err(RuntimeError::Conflict(
                    "pre-add effects have not all succeeded".into(),
                ));
            }
            let mut observed_actor: Option<Option<String>> = None;
            for (_, _, _, json, hook_json) in &collected {
                let input: serde_json::Value = serde_json::from_str(json).map_err(|error| {
                    RuntimeError::Bootstrap(format!("corrupt pre-add parent input: {error}"))
                })?;
                let exact_hook: runtime_core::TeamHookResolvedEntry =
                    serde_json::from_str(hook_json).map_err(|error| {
                        RuntimeError::Bootstrap(format!("corrupt approved pre-add hook: {error}"))
                    })?;
                if !effect_grant_is_current(&tx, &agent.workspace_id, &exact_hook)? {
                    return Err(RuntimeError::Conflict(
                        "pre-add hook approval was revoked before member creation".into(),
                    ));
                }
                if input["subject"]["member_agent_id"] != agent.agent_id
                    || input["hook_event"] != "pre_teammate_add"
                    || input["event_id"] != hooks.parent_event_id
                {
                    return Err(RuntimeError::Conflict(
                        "pre-add parent does not match agent".into(),
                    ));
                }
                let candidate = input["actor_agent_id"].as_str().map(str::to_string);
                if observed_actor
                    .as_ref()
                    .is_some_and(|actor| actor != &candidate)
                {
                    return Err(RuntimeError::Conflict("conflicting pre-add actors".into()));
                }
                observed_actor = Some(candidate);
            }
            let actor = observed_actor.flatten();
            if actor
                .as_deref()
                .is_some_and(|id| id != agent.profile.added_by)
            {
                return Err(RuntimeError::Conflict(
                    "pre-add actor no longer matches member creator".into(),
                ));
            }
            if hooks.caller_context_text.is_some() && actor.is_none() {
                return Err(RuntimeError::Conflict(
                    "pre-add caller has no authenticated agent".into(),
                ));
            }
            actor
        } else {
            None
        };
        if let Some(actor) = verified_pre_add_actor.as_deref() {
            let actor_active: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM workspace_agents
                 WHERE workspace_id=?1 AND session_id=?2 AND lifecycle_state='active')",
                    params![agent.workspace_id, actor],
                    |row| row.get(0),
                )
                .map_err(|error| db_error("failed checking pre-add creator authority", error))?;
            if !actor_active {
                return Err(RuntimeError::Conflict(
                    "pre-add creator lost workspace membership before member creation".into(),
                ));
            }
        }

        insert_session(&tx, session)?;
        tx.execute(
            "INSERT INTO workspace_session_ownership (
                session_id, workspace_id, source, source_operation_id, created_at
             ) VALUES (?1, ?2, 'v2_workspace_agent_create', NULL, ?3)",
            params![agent.agent_id, agent.workspace_id, agent.created_at],
        )
        .map_err(|error| db_error("failed inserting workspace session ownership", error))?;
        tx.execute(
            "INSERT INTO workspace_agent_profiles (
                session_id, workspace_id, title, title_provenance, added_by,
                creator_session_id, creator_compaction_subscription, joined_at,
                revision, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10)",
            params![
                agent.agent_id,
                agent.workspace_id,
                agent.profile.title,
                agent.profile.title_provenance,
                agent.profile.added_by,
                agent.profile.creator_session_id,
                agent.profile.creator_compaction_subscription,
                agent.profile.joined_at,
                agent.created_at,
                agent.updated_at,
            ],
        )
        .map_err(|error| db_error("failed inserting workspace agent profile", error))?;
        tx.execute(
            "INSERT INTO workspace_agents (
                session_id, workspace_id, identity_alias, lifecycle_state,
                recreation_policy_json, archived_at, archive_reason, revision,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                agent.agent_id,
                agent.workspace_id,
                agent.alias,
                agent.lifecycle_state.as_str(),
                serde_json::to_string(&agent.recreation_policy).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "failed serializing workspace agent recreation policy: {error}"
                    ))
                })?,
                agent.archived_at,
                agent.archive_reason,
                i64::try_from(agent.revision).map_err(|_| RuntimeError::Bootstrap(
                    "workspace agent revision overflow".to_string()
                ))?,
                agent.created_at,
                agent.updated_at,
            ],
        )
        .map_err(|error| {
            let message = error.to_string();
            if message.contains("workspace_agents.identity_alias") {
                RuntimeError::Conflict(format!(
                    "workspace agent alias {} already exists",
                    agent.alias
                ))
            } else {
                db_error("failed inserting workspace agent authority", error)
            }
        })?;
        if let Some(worktree_id) = session.worktree_id.as_deref() {
            let route_cwd: Option<String> = tx
                .query_row(
                    "SELECT worktree_cwd FROM managed_worktrees WHERE id = ?1",
                    [worktree_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| db_error("failed verifying initial managed worktree", error))?;
            if route_cwd.as_deref() != session.cwd.as_deref() {
                return Err(RuntimeError::Conflict(
                    "managed worktree destination changed before agent creation committed".into(),
                ));
            }
            tx.execute(
                "INSERT INTO managed_worktree_claims
                 (worktree_id,session_id,claim_role,created_at,released_at)
                 VALUES (?1,?2,'owner',?3,NULL)",
                params![worktree_id, session.id, session.created_at],
            )
            .map_err(|error| db_error("failed claiming initial managed worktree", error))?;
        }
        if let Some(onboarding) = onboarding {
            let (display_name, lead_alias): (String, Option<String>) = tx.query_row(
                "SELECT w.display_name, lead.identity_alias
                 FROM workspaces w LEFT JOIN workspace_agents lead ON lead.session_id = w.lead_agent_id
                 WHERE w.workspace_id = ?1",
                params![agent.workspace_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).map_err(|error| db_error("failed snapshotting onboarding workspace authority", error))?;
            let notice = WorkspaceSystemNoticeRecord::onboarding_with_hook_context(
                agent,
                &display_name,
                lead_alias.as_deref(),
                onboarding,
                hooks.and_then(|hooks| hooks.onboarding_context_append_text.as_deref()),
            )?;
            Self::insert_workspace_system_notice_tx(&tx, &notice)?;
            if let (Some(hooks), Some(actor), Some(text)) = (
                hooks,
                verified_pre_add_actor.as_deref(),
                hooks.and_then(|h| h.caller_context_text.as_deref()),
            ) {
                let caller_active: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM workspace_agents WHERE workspace_id=?1
                     AND session_id=?2 AND lifecycle_state='active')",
                        params![agent.workspace_id, actor],
                        |row| row.get(0),
                    )
                    .map_err(|error| db_error("failed verifying pre-add caller", error))?;
                if !caller_active {
                    return Err(RuntimeError::Conflict(
                        "pre-add caller was archived before creation".into(),
                    ));
                }
                let caller_notice = WorkspaceSystemNoticeRecord::lifecycle(
                    &agent.workspace_id,
                    actor,
                    &hooks.parent_event_id,
                    "hook_pre_add",
                    format!("Operator-approved pre-add hook context (not user-authored):\n{text}"),
                    agent.created_at,
                );
                Self::insert_workspace_system_notice_tx(&tx, &caller_notice)?;
            }
            queue_membership_lead_notice_tx(
                &tx,
                &agent.workspace_id,
                &agent.agent_id,
                &agent.alias,
                Some(&agent.profile.added_by),
                &format!("workspace-agent-created:{}", agent.agent_id),
                "joined",
                agent.created_at,
            )?;
        }
        queue_team_hook_trigger_tx(
            &tx,
            &agent.workspace_id,
            runtime_core::TeamHookEventName::PostTeammateAdd,
            &format!("workspace-hook:joined:{}", agent.agent_id),
            Some(&agent.profile.added_by),
            serde_json::json!({
                "kind": "member", "action": "joined", "member_agent_id": agent.agent_id,
                "member_alias": agent.alias,
            }),
            &active_workspace_hook_targets_tx(&tx, &agent.workspace_id)?,
            agent.created_at,
        )?;
        if let Some(hooks) = hooks {
            let changed = tx
                .execute(
                    "UPDATE workspace_hook_effects SET application_state='applied',
                    applied_at_ms=?3
                 WHERE workspace_id=?1 AND parent_event_id=?2
                   AND event_name='pre_teammate_add' AND phase='succeeded'
                   AND application_state='pending'",
                    params![agent.workspace_id, hooks.parent_event_id, agent.created_at],
                )
                .map_err(|error| db_error("failed acknowledging applied pre-add hooks", error))?;
            if changed != hooks.completed_hook_names.len() {
                return Err(RuntimeError::Conflict(
                    "pre-add hook application raced".into(),
                ));
            }
        }
        tx.commit()
            .map_err(|error| db_error("failed committing workspace agent create", error))?;
        Ok(())
    }

    pub fn list_workspace_agents(
        &self,
        workspace_id: &str,
        lifecycle: Option<WorkspaceAgentLifecycleState>,
    ) -> Result<Vec<WorkspaceAgentRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let (query, lifecycle_arg) = match lifecycle {
            Some(lifecycle) => (
                format!(
                    "{AGENT_SELECT} WHERE a.workspace_id = ?1 AND a.lifecycle_state = ?2 ORDER BY a.created_at, a.session_id"
                ),
                Some(lifecycle.as_str()),
            ),
            None => (
                format!(
                    "{AGENT_SELECT} WHERE a.workspace_id = ?1 ORDER BY a.created_at, a.session_id"
                ),
                None,
            ),
        };
        let mut statement = connection
            .prepare(query.as_str())
            .map_err(|error| db_error("failed preparing workspace agent list", error))?;
        let mut records = Vec::new();
        if let Some(lifecycle) = lifecycle_arg {
            let rows = statement
                .query_map(params![workspace_id, lifecycle], workspace_agent_from_row)
                .map_err(|error| db_error("failed listing workspace agents", error))?;
            for row in rows {
                records.push(row.map_err(|error| db_error("invalid workspace agent row", error))?);
            }
        } else {
            let rows = statement
                .query_map(params![workspace_id], workspace_agent_from_row)
                .map_err(|error| db_error("failed listing workspace agents", error))?;
            for row in rows {
                records.push(row.map_err(|error| db_error("invalid workspace agent row", error))?);
            }
        }
        Ok(records)
    }

    pub fn get_workspace_agent(
        &self,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let query = format!("{AGENT_SELECT} WHERE a.workspace_id = ?1 AND a.session_id = ?2");
        connection
            .query_row(
                query.as_str(),
                params![workspace_id, agent_id],
                workspace_agent_from_row,
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace agent", error))
    }

    pub fn get_workspace_agent_by_id(
        &self,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let query = format!("{AGENT_SELECT} WHERE a.session_id = ?1");
        connection
            .query_row(query.as_str(), params![agent_id], workspace_agent_from_row)
            .optional()
            .map_err(|error| db_error("failed reading workspace agent by id", error))
    }

    pub fn get_workspace_agent_by_alias(
        &self,
        alias: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let query = format!("{AGENT_SELECT} WHERE a.identity_alias = ?1");
        connection
            .query_row(query.as_str(), params![alias], workspace_agent_from_row)
            .optional()
            .map_err(|error| db_error("failed reading workspace agent by alias", error))
    }

    pub fn set_workspace_agent_lifecycle(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        lifecycle: WorkspaceAgentLifecycleState,
        archive_reason: Option<&str>,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.set_workspace_agent_lifecycle_with_initiator(
            session,
            agent_id,
            lifecycle,
            archive_reason,
            changed_at,
            None,
        )
    }

    pub fn set_workspace_agent_lifecycle_with_initiator(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        lifecycle: WorkspaceAgentLifecycleState,
        archive_reason: Option<&str>,
        changed_at: i64,
        initiator_agent_id: Option<&str>,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        if session.id != agent_id {
            return Err(RuntimeError::ProtocolViolation(
                "workspace agent lifecycle session mismatch".to_string(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed starting workspace agent lifecycle transaction",
                    error,
                )
            })?;
        reject_unresolved_route(&tx, agent_id)?;
        let (workspace_id, prior_lifecycle, prior_revision): (String, String, i64) = tx
            .query_row(
                "SELECT workspace_id, lifecycle_state, revision FROM workspace_agents WHERE session_id = ?1",
                params![agent_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace agent lifecycle", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        update_session(&tx, session)?;
        let (archived_at, archive_reason) = match lifecycle {
            WorkspaceAgentLifecycleState::Active => (None, None),
            WorkspaceAgentLifecycleState::Archived => {
                tx.execute(
                    "UPDATE workspaces
                     SET lead_agent_id = NULL, revision = revision + 1, updated_at = ?3
                     WHERE workspace_id = ?1 AND lead_agent_id = ?2",
                    params![workspace_id, agent_id, changed_at],
                )
                .map_err(|error| db_error("failed clearing archived workspace lead", error))?;
                (Some(changed_at), archive_reason.map(str::to_string))
            }
        };
        tx.execute(
            "UPDATE workspace_agents
             SET lifecycle_state = ?2, archived_at = ?3, archive_reason = ?4,
                 revision = revision + 1, updated_at = ?5
             WHERE session_id = ?1",
            params![
                agent_id,
                lifecycle.as_str(),
                archived_at,
                archive_reason,
                changed_at
            ],
        )
        .map_err(|error| db_error("failed updating workspace agent lifecycle", error))?;
        if lifecycle == WorkspaceAgentLifecycleState::Active
            && prior_lifecycle == WorkspaceAgentLifecycleState::Archived.as_str()
        {
            // The notice worker cancels pending onboarding when it observes
            // an archived agent. On restore, requeue that *same* never-admitted
            // snapshot. A dispatched/ambiguous/manual-review notice must not
            // be retried or replaced with a new onboarding identity.
            tx.execute(
                "UPDATE workspace_system_notices
                 SET state = 'pending', last_error = NULL, updated_at_ms = ?2
                 WHERE notice_id = ?1 AND state = 'cancelled' AND turn_id IS NULL
                   AND last_error = 'recipient_archived'",
                params![format!("workspace-onboarding:{agent_id}"), changed_at],
            )
            .map_err(|error| db_error("failed requeueing safe onboarding on restore", error))?;
        }
        if prior_lifecycle != lifecycle.as_str() {
            let alias: String = tx
                .query_row(
                    "SELECT identity_alias FROM workspace_agents WHERE session_id = ?1",
                    [agent_id],
                    |row| row.get(0),
                )
                .map_err(|error| db_error("failed reading lifecycle notice alias", error))?;
            let action = match lifecycle {
                WorkspaceAgentLifecycleState::Active => "restored",
                WorkspaceAgentLifecycleState::Archived => "removed",
            };
            queue_membership_lead_notice_tx(
                &tx,
                &workspace_id,
                agent_id,
                &alias,
                initiator_agent_id,
                &format!(
                    "workspace-agent-lifecycle:{agent_id}:revision-{}",
                    prior_revision
                        .checked_add(1)
                        .ok_or_else(|| RuntimeError::InvalidState(
                            "workspace agent lifecycle revision overflow".into()
                        ))?
                ),
                action,
                changed_at,
            )?;
            if lifecycle == WorkspaceAgentLifecycleState::Archived {
                queue_team_hook_trigger_tx(
                    &tx,
                    &workspace_id,
                    runtime_core::TeamHookEventName::PostTeammateRemove,
                    &format!(
                        "workspace-hook:removed:{agent_id}:rev-{}",
                        prior_revision + 1
                    ),
                    initiator_agent_id,
                    serde_json::json!({
                        "kind":"member", "action":"removed", "member_agent_id":agent_id,
                        "member_alias":alias,
                    }),
                    &active_workspace_hook_targets_tx(&tx, &workspace_id)?,
                    changed_at,
                )?;
            }
        }
        tx.commit()
            .map_err(|error| db_error("failed committing workspace agent lifecycle", error))?;
        self.get_workspace_agent(workspace_id.as_str(), agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))
    }

    pub fn compare_and_set_workspace_agent_recreation_policy(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        expected_revision: u64,
        recreation_policy: &WorkspaceAgentRecreationPolicy,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        if session.id != agent_id
            || session.provider != recreation_policy.provider.as_str()
            || session.cwd.as_deref() != Some(recreation_policy.authoritative_cwd.as_str())
            || session.model != recreation_policy.model
        {
            return Err(RuntimeError::ProtocolViolation(
                "workspace agent session does not match updated recreation policy".to_string(),
            ));
        }
        let expected_revision = i64::try_from(expected_revision).map_err(|_| {
            RuntimeError::InvalidState("workspace agent revision overflow".to_string())
        })?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed starting workspace agent recreation-policy transaction",
                    error,
                )
            })?;
        reject_unresolved_route(&tx, agent_id)?;
        let (workspace_id, current_revision) = tx
            .query_row(
                "SELECT workspace_id, revision FROM workspace_agents WHERE session_id = ?1",
                params![agent_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace agent recreation policy", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        if current_revision != expected_revision {
            return Err(RuntimeError::Conflict(format!(
                "workspace agent {agent_id} revision conflict: expected {expected_revision}, current {current_revision}"
            )));
        }
        update_session(&tx, session)?;
        let policy_json = serde_json::to_string(recreation_policy).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed serializing workspace agent recreation policy: {error}"
            ))
        })?;
        let changed = tx
            .execute(
                "UPDATE workspace_agents
                 SET recreation_policy_json = ?3, revision = revision + 1, updated_at = ?4
                 WHERE session_id = ?1 AND revision = ?2",
                params![agent_id, expected_revision, policy_json, changed_at],
            )
            .map_err(|error| {
                db_error("failed updating workspace agent recreation policy", error)
            })?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "workspace agent {agent_id} recreation policy changed concurrently"
            )));
        }
        tx.commit().map_err(|error| {
            db_error(
                "failed committing workspace agent recreation-policy mutation",
                error,
            )
        })?;
        self.get_workspace_agent(workspace_id.as_str(), agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))
    }
}

fn reject_unresolved_route(connection: &Connection, agent_id: &str) -> Result<(), RuntimeError> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM workspace_agent_rebinds
         WHERE agent_id = ?1 AND phase IN ('intended', 'manual_review')",
            [agent_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking workspace route lifecycle fence", error))?;
    if count > 0 {
        return Err(RuntimeError::Conflict(
            "workspace agent provider worktree binding is unresolved".into(),
        ));
    }
    let edited: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM session_edit_reruns WHERE session_id=?1
         AND phase IN ('intended','native_started','fork_observed','manual_review')",
            [agent_id],
            |row| row.get(0),
        )
        .map_err(|e| db_error("failed checking native edit policy fence", e))?;
    if edited > 0 {
        return Err(RuntimeError::Conflict(
            "workspace agent has unresolved native edit-and-rerun".into(),
        ));
    }
    Ok(())
}

fn insert_session(connection: &Connection, record: &SessionRecord) -> Result<(), RuntimeError> {
    connection
        .execute(
            "INSERT INTO sessions (
                id, provider, status, cwd, model, permission_mode, system_prompt, metadata_json,
                provider_session_ref, canonical_provider_session_ref, active_turn_id, worktree_id,
                created_at, updated_at, closed_at, failure_code, failure_message
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            rusqlite::params_from_iter(session_params(record)?),
        )
        .map_err(|error| db_error("failed inserting workspace agent session", error))?;
    Ok(())
}

fn update_session(connection: &Connection, record: &SessionRecord) -> Result<(), RuntimeError> {
    let changed = connection
        .execute(
            "UPDATE sessions SET
                provider = ?2, status = ?3, cwd = ?4, model = ?5, permission_mode = ?6,
                system_prompt = ?7, metadata_json = ?8, provider_session_ref = ?9,
                canonical_provider_session_ref = ?10, active_turn_id = ?11, worktree_id = ?12,
                updated_at = ?14, closed_at = ?15, failure_code = ?16, failure_message = ?17
             WHERE id = ?1",
            rusqlite::params_from_iter(session_params(record)?),
        )
        .map_err(|error| db_error("failed updating workspace agent session", error))?;
    if changed != 1 {
        return Err(RuntimeError::NotFound(format!("session {}", record.id)));
    }
    Ok(())
}

fn session_params(record: &SessionRecord) -> Result<Vec<rusqlite::types::Value>, RuntimeError> {
    use rusqlite::types::Value as SqlValue;
    let optional = |value: &Option<String>| match value {
        Some(value) => SqlValue::Text(value.clone()),
        None => SqlValue::Null,
    };
    Ok(vec![
        SqlValue::Text(record.id.clone()),
        SqlValue::Text(record.provider.clone()),
        SqlValue::Text(record.status.clone()),
        optional(&record.cwd),
        optional(&record.model),
        optional(&record.permission_mode),
        optional(&record.system_prompt),
        SqlValue::Text(json_to_string(&record.metadata)?),
        optional(&record.provider_session_ref),
        optional(&record.canonical_provider_session_ref),
        optional(&record.active_turn_id),
        optional(&record.worktree_id),
        SqlValue::Integer(record.created_at),
        SqlValue::Integer(record.updated_at),
        record
            .closed_at
            .map(SqlValue::Integer)
            .unwrap_or(SqlValue::Null),
        optional(&record.failure_code),
        optional(&record.failure_message),
    ])
}

fn workspace_agent_from_row(row: &Row<'_>) -> rusqlite::Result<WorkspaceAgentRecord> {
    let lifecycle_raw: String = row.get(3)?;
    let lifecycle_state = WorkspaceAgentLifecycleState::from_str(lifecycle_raw.as_str())
        .ok_or_else(|| conversion_error(3, format!("invalid lifecycle state {lifecycle_raw}")))?;
    let policy_json: String = row.get(10)?;
    let recreation_policy = serde_json::from_str::<WorkspaceAgentRecreationPolicy>(&policy_json)
        .map_err(|error| conversion_error(10, error.to_string()))?;
    if ProviderKind::from_str(recreation_policy.provider.as_str()).is_none() {
        return Err(conversion_error(10, "invalid workspace agent provider"));
    }
    let revision: i64 = row.get(16)?;
    Ok(WorkspaceAgentRecord {
        agent_id: row.get(0)?,
        workspace_id: row.get(1)?,
        alias: row.get(2)?,
        lifecycle_state,
        profile: WorkspaceAgentProfile {
            title: row.get(4)?,
            title_provenance: row.get(5)?,
            added_by: row.get(6)?,
            creator_session_id: row.get(7)?,
            creator_compaction_subscription: row.get(8)?,
            joined_at: row.get(9)?,
        },
        recreation_policy,
        provider_session_ref: row.get(11)?,
        canonical_provider_session_ref: row.get(12)?,
        metadata: string_to_json(row.get(13)?)?,
        archived_at: row.get(14)?,
        archive_reason: row.get(15)?,
        revision: u64::try_from(revision).map_err(|_| conversion_error(16, "negative revision"))?,
        created_at: row.get(17)?,
        updated_at: row.get(18)?,
    })
}

fn conversion_error(index: usize, message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message.into(),
        )),
    )
}
