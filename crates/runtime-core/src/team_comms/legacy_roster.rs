//! /v1 team creation and roster changes have no external native effect, but
//! their critical journal events must never silently vanish after a cache or
//! SQLite write. Hold the same cache lock through durable atomic admission,
//! then publish canonical state only after the store confirms the receipt.

use std::collections::HashMap;

use crate::{
    LegacyTeamMutation, RuntimeError, TeamCreateRequest, TeamJoinRequest, TeamMemberRecord,
    TeamRecord, TeamRemoveMemberRequest, TeamSetLeadRequest, TeamWithMembers,
};

use super::{normalize_non_empty, normalized_non_empty, now_ms, RuntimeTeamCommsService};

impl RuntimeTeamCommsService {
    // `updated_at` is also the SQLite compare-and-swap generation for legacy
    // /v1 team rows. Two owners can mutate within one millisecond; using raw
    // wall-clock time would let both updates match the *same* old generation.
    fn next_roster_update_at(before: &TeamRecord) -> Result<i64, RuntimeError> {
        let next = before.updated_at.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidState("legacy team revision timestamp exhausted".into())
        })?;
        Ok(now_ms().max(next))
    }

    pub(super) async fn legacy_roster_create(
        &self,
        request: TeamCreateRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.ensure_enabled()?;
        let name = normalize_non_empty(&request.name, "name")?;
        let lead_agent_id = normalize_non_empty(&request.lead_agent_id, "lead_agent_id")?;
        let created_by = request.created_by.unwrap_or_else(|| "user".into());
        self.require_session_active(&lead_agent_id).await?;
        for agent in &request.member_agent_ids {
            self.require_session_active(agent).await?;
        }
        let now = now_ms();
        let mut state = self.state.write().await;
        let team_id = self.allocate_team_id(&state);
        let mut members = HashMap::new();
        let lead = TeamMemberRecord {
            team_id: team_id.clone(),
            agent_id: lead_agent_id.clone(),
            title: None,
            joined_at: now,
            added_by: created_by.clone(),
            creator_agent_id: None,
            creator_compaction_subscription: "auto".into(),
            worktree_id: None,
        };
        members.insert(lead.agent_id.clone(), lead);
        for agent in &request.member_agent_ids {
            if agent != &lead_agent_id {
                let member = TeamMemberRecord {
                    team_id: team_id.clone(),
                    agent_id: agent.clone(),
                    title: None,
                    joined_at: now,
                    added_by: created_by.clone(),
                    creator_agent_id: None,
                    creator_compaction_subscription: "auto".into(),
                    worktree_id: None,
                };
                members.insert(agent.clone(), member);
            }
        }
        let team = TeamRecord {
            id: team_id.clone(),
            name,
            lead_agent_id: lead_agent_id.clone(),
            created_by,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };
        let mut initial_members = members.values().cloned().collect::<Vec<_>>();
        initial_members.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        let mutation = LegacyTeamMutation {
            before_team: None,
            after_team: team.clone(),
            initial_members: initial_members.clone(),
            before_member: None,
            after_member: None,
            event: crate::NewRuntimeEvent {
                event_id: format!("evt_team_{team_id}_created"),
                ..self.new_team_event(
                    &team_id,
                    "team.created",
                    serde_json::json!({"team":team,"members":initial_members}),
                    Some(lead_agent_id),
                )
            },
        };
        self.store.commit_legacy_team_mutation(&mutation)?;
        state.teams.insert(team_id.clone(), team);
        state.members_by_team.insert(team_id.clone(), members);
        drop(state);
        self.team_with_members(&team_id).await
    }

    pub(super) async fn legacy_roster_join(
        &self,
        request: TeamJoinRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(&request.team_id, "team_id")?;
        let agent_id = normalize_non_empty(&request.agent_id, "agent_id")?;
        self.require_session_active(&agent_id).await?;
        let now = now_ms();
        let mut state = self.state.write().await;
        let before_team = state
            .teams
            .get(&team_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("team {team_id}")))?;
        if state
            .members_by_team
            .get(&team_id)
            .is_some_and(|members| members.contains_key(&agent_id))
        {
            return Err(RuntimeError::InvalidState(format!(
                "agent {agent_id} is already a member of team {team_id}"
            )));
        }
        let member = TeamMemberRecord {
            team_id: team_id.clone(),
            agent_id: agent_id.clone(),
            title: normalized_non_empty(request.title.as_deref()),
            joined_at: now,
            added_by: request
                .added_by
                .unwrap_or_else(|| before_team.lead_agent_id.clone()),
            creator_agent_id: request.creator_agent_id,
            creator_compaction_subscription: request
                .creator_compaction_subscription
                .unwrap_or_else(|| "auto".into()),
            worktree_id: request.worktree_id,
        };
        let mut updated = before_team.clone();
        updated.updated_at = Self::next_roster_update_at(&before_team)?;
        let event = self.new_team_event(
            &team_id,
            "team.member_joined",
            serde_json::json!({"team":updated,"member":member}),
            Some(agent_id.clone()),
        );
        self.store
            .commit_legacy_team_mutation(&LegacyTeamMutation {
                before_team: Some(before_team),
                after_team: updated.clone(),
                initial_members: Vec::new(),
                before_member: None,
                after_member: Some(member.clone()),
                event,
            })?;
        state
            .members_by_team
            .entry(team_id.clone())
            .or_default()
            .insert(agent_id, member);
        state.teams.insert(team_id.clone(), updated);
        drop(state);
        self.team_with_members(&team_id).await
    }

    pub(super) async fn legacy_roster_remove(
        &self,
        request: TeamRemoveMemberRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(&request.team_id, "team_id")?;
        let agent_id = normalize_non_empty(&request.agent_id, "agent_id")?;
        let mut state = self.state.write().await;
        let before_team = state
            .teams
            .get(&team_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("team {team_id}")))?;
        let members = state
            .members_by_team
            .get(&team_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("team {team_id} members")))?;
        let prior = members.get(&agent_id).cloned().ok_or_else(|| {
            RuntimeError::NotFound(format!("agent {agent_id} not in team {team_id}"))
        })?;
        if before_team.lead_agent_id == agent_id {
            return Err(RuntimeError::InvalidState(format!(
                "cannot remove legacy team lead {agent_id} from {team_id}; reassign the lead explicitly first"
            )));
        }
        if members.len() <= 1 {
            return Err(RuntimeError::InvalidState(format!(
                "cannot remove last member from team {team_id}; delete team instead"
            )));
        }
        let mut updated = before_team.clone();
        updated.updated_at = Self::next_roster_update_at(&before_team)?;
        let event = self.new_team_event(
            &team_id,
            "team.member_left",
            serde_json::json!({"team":updated,"agent_id":agent_id}),
            Some(agent_id.clone()),
        );
        self.store
            .commit_legacy_team_mutation(&LegacyTeamMutation {
                before_team: Some(before_team),
                after_team: updated.clone(),
                initial_members: Vec::new(),
                before_member: Some(prior),
                after_member: None,
                event,
            })?;
        state
            .members_by_team
            .get_mut(&team_id)
            .expect("validated team members")
            .remove(&agent_id);
        state.teams.insert(team_id.clone(), updated);
        drop(state);
        self.team_with_members(&team_id).await
    }

    pub(super) async fn legacy_roster_set_lead(
        &self,
        request: TeamSetLeadRequest,
    ) -> Result<TeamWithMembers, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = normalize_non_empty(&request.team_id, "team_id")?;
        let lead_agent_id = normalize_non_empty(&request.lead_agent_id, "lead_agent_id")?;
        let mut state = self.state.write().await;
        let before_team = state
            .teams
            .get(&team_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("team {team_id}")))?;
        if !state
            .members_by_team
            .get(&team_id)
            .is_some_and(|members| members.contains_key(&lead_agent_id))
        {
            return Err(RuntimeError::NotFound(format!(
                "agent {lead_agent_id} not in team {team_id}"
            )));
        }
        if before_team.lead_agent_id == lead_agent_id {
            drop(state);
            return self.team_with_members(&team_id).await;
        }
        let mut updated = before_team.clone();
        updated.lead_agent_id = lead_agent_id.clone();
        updated.updated_at = Self::next_roster_update_at(&before_team)?;
        let event = self.new_team_event(
            &team_id,
            "team.lead_changed",
            serde_json::json!({"team":updated,"lead_agent_id":lead_agent_id}),
            Some(lead_agent_id),
        );
        self.store
            .commit_legacy_team_mutation(&LegacyTeamMutation {
                before_team: Some(before_team),
                after_team: updated.clone(),
                initial_members: Vec::new(),
                before_member: None,
                after_member: None,
                event,
            })?;
        state.teams.insert(team_id.clone(), updated);
        drop(state);
        self.team_with_members(&team_id).await
    }
}
