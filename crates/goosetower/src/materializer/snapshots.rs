use std::collections::BTreeMap;

use runtime_core::{ApprovalRecord, TeamDeliveryRecord, TeamMemberRecord};
use serde::{Deserialize, Serialize};

use super::state::{
    AgentRowView, ApprovalInboxView, ApprovalRowView, FleetBoardView, LedgerView, ProcessTailView,
    ProcessView, SessionDetailView, SourceHealthView, TeamMemberView, TeamWorkspaceView,
    TranscriptEntryView, WorktreeView,
};
use super::MaterializedState;

pub const MAX_TEAM_MESSAGE_LIMIT: usize = 100;
pub const MAX_TEAM_DELIVERY_LIMIT: usize = 1_000;
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SourceReplacementView {
    pub source_id: String,
    pub source_epoch: String,
    pub source_seq: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardSubscription {
    pub offset: usize,
    pub limit: usize,
    pub status_filter: Option<String>,
    pub team_id: Option<String>,
    pub source_id: Option<String>,
    pub query: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalInboxSubscription {
    pub include_resolved: bool,
    pub session_id: Option<String>,
    pub source_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedSessionSubscription {
    pub session_id: String,
    pub include_text: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedTeamSubscription {
    pub team_id: String,
    pub message_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessTailSubscription {
    pub process_id: String,
    pub tail_lines: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerSubscription {
    pub offset: usize,
    pub limit: usize,
    pub scope: Option<String>,
    pub session_id: Option<String>,
    pub team_id: Option<String>,
    pub process_id: Option<String>,
    pub kind: Option<String>,
    pub criticality: Option<String>,
    pub source_id: Option<String>,
}

impl MaterializedState {
    pub fn snapshot_board(&self, subscription: &BoardSubscription) -> FleetBoardView {
        let mut rows = self
            .sessions
            .keys()
            .filter_map(|session_id| self.agent_row(session_id))
            .filter(|row| board_row_matches(row, subscription))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            right
                .latest_activity_unix_ms
                .cmp(&left.latest_activity_unix_ms)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        let total_rows = rows.len();
        let offset = subscription.offset.min(rows.len());
        let limit = subscription.limit.max(1);
        let rows = rows.into_iter().skip(offset).take(limit).collect();
        FleetBoardView {
            rows,
            total_rows,
            cursor: self.cursor(),
            cursors: self.cursor().into_iter().collect(),
        }
    }

    pub fn snapshot_approval_inbox(
        &self,
        subscription: &ApprovalInboxSubscription,
    ) -> ApprovalInboxView {
        let mut approvals = self
            .approvals
            .values()
            .filter(|approval| subscription.include_resolved || approval.status == "pending")
            .filter(|_| {
                subscription
                    .source_id
                    .as_deref()
                    .is_none_or(|source_id| self.source_id == source_id)
            })
            .filter(|approval| {
                subscription
                    .session_id
                    .as_deref()
                    .is_none_or(|session_id| approval.session_id == session_id)
            })
            .map(|approval| approval_row_from_record(self, approval))
            .collect::<Vec<_>>();
        approvals.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.approval_id.cmp(&right.approval_id))
        });
        ApprovalInboxView { approvals }
    }

    pub fn snapshot_session(
        &self,
        subscription: &SelectedSessionSubscription,
    ) -> Option<SessionDetailView> {
        let session = self.sessions.get(&subscription.session_id)?.clone();
        let team_ids = self
            .members_by_team
            .iter()
            .filter(|(_, members)| members.contains_key(&subscription.session_id))
            .map(|(team_id, _)| team_id.clone())
            .collect::<Vec<_>>();
        let pending_approvals = self
            .approvals
            .values()
            .filter(|approval| {
                approval.session_id == subscription.session_id && approval.status == "pending"
            })
            .map(|approval| approval_row_from_record(self, approval))
            .collect::<Vec<_>>();
        let mut process_rows = self
            .processes
            .values()
            .filter(|process| process.session_id.as_deref() == Some(&subscription.session_id))
            .cloned()
            .collect::<Vec<_>>();
        process_rows.sort_by(|left, right| right.started_at.cmp(&left.started_at));
        let active_processes = process_rows
            .iter()
            .filter(|process| matches!(process.status.as_str(), "queued" | "running"))
            .cloned()
            .collect::<Vec<_>>();
        let recent_processes = process_rows.into_iter().take(20).collect::<Vec<_>>();
        let appended_text = if subscription.include_text {
            self.appended_text_by_session
                .get(&subscription.session_id)
                .map(|chunks| chunks.iter().cloned().collect::<Vec<_>>().join(""))
                .unwrap_or_default()
        } else {
            String::new()
        };
        Some(SessionDetailView {
            source_id: self.source_id.clone(),
            session: session.clone(),
            team_ids,
            pending_approvals,
            active_processes,
            recent_processes,
            transcript: transcript_entries(&session.metadata),
            appended_text,
            latest_activity_unix_ms: self
                .agent_row(&subscription.session_id)
                .map(|row| row.latest_activity_unix_ms)
                .unwrap_or(session.updated_at),
            source_health: self.source_health.state,
            discontinuities: self.discontinuities.iter().cloned().collect(),
            version: self.version("session", &subscription.session_id),
        })
    }

    pub fn snapshot_team(
        &self,
        subscription: &SelectedTeamSubscription,
    ) -> Option<TeamWorkspaceView> {
        let team = self.teams.get(&subscription.team_id)?.clone();
        let members = self
            .members_by_team
            .get(&subscription.team_id)
            .map(|members| {
                members
                    .values()
                    .cloned()
                    .map(|member| self.team_member_view(member))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut messages = self
            .messages_by_team
            .get(&subscription.team_id)
            .cloned()
            .unwrap_or_default();
        messages.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        messages.truncate(subscription.message_limit.clamp(1, MAX_TEAM_MESSAGE_LIMIT));
        messages.reverse();
        let all_deliveries = self
            .deliveries_by_team
            .get(&subscription.team_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        while messages
            .iter()
            .map(|message| {
                all_deliveries
                    .iter()
                    .filter(|delivery| delivery.message_id == message.id)
                    .count()
            })
            .sum::<usize>()
            > MAX_TEAM_DELIVERY_LIMIT
        {
            messages.remove(0);
        }
        let message_ids = messages
            .iter()
            .map(|message| message.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let deliveries = all_deliveries
            .iter()
            .filter(|delivery| message_ids.contains(delivery.message_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        Some(TeamWorkspaceView {
            source_id: self.source_id.clone(),
            team: team.clone(),
            members,
            messages,
            delivery_status_counts: delivery_status_counts(deliveries.iter()),
            deliveries,
            latest_activity_unix_ms: team.updated_at,
            source_health: self.source_health.state,
            discontinuities: self.discontinuities.iter().cloned().collect(),
            version: self.version("team", &subscription.team_id),
        })
    }

    pub fn snapshot_process_tail(&self, subscription: &ProcessTailSubscription) -> ProcessTailView {
        let tail_lines = subscription.tail_lines.max(1);
        ProcessTailView {
            source_id: self.source_id.clone(),
            process: self.processes.get(&subscription.process_id).cloned(),
            stdout: self
                .process_stdout
                .get(&subscription.process_id)
                .map(|lines| {
                    lines
                        .iter()
                        .rev()
                        .take(tail_lines)
                        .cloned()
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect()
                })
                .unwrap_or_default(),
            stderr: self
                .process_stderr
                .get(&subscription.process_id)
                .map(|lines| {
                    lines
                        .iter()
                        .rev()
                        .take(tail_lines)
                        .cloned()
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect()
                })
                .unwrap_or_default(),
            samples: self
                .process_samples
                .get(&subscription.process_id)
                .map(|samples| {
                    samples
                        .iter()
                        .rev()
                        .take(tail_lines)
                        .cloned()
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    pub fn snapshot_ledger(&self, subscription: &LedgerSubscription) -> LedgerView {
        let mut events =
            self.ledger
                .iter()
                .filter(|event| {
                    subscription
                        .source_id
                        .as_deref()
                        .is_none_or(|source_id| event.source_id == source_id)
                })
                .filter(|event| {
                    subscription.scope.as_deref().is_none_or(|scope| {
                        format!("{:?}", event.scope).eq_ignore_ascii_case(scope)
                    })
                })
                .filter(|event| {
                    subscription
                        .session_id
                        .as_deref()
                        .is_none_or(|session_id| event.session_id.as_deref() == Some(session_id))
                })
                .filter(|event| {
                    subscription
                        .team_id
                        .as_deref()
                        .is_none_or(|team_id| event.team_id.as_deref() == Some(team_id))
                })
                .filter(|event| {
                    subscription
                        .process_id
                        .as_deref()
                        .is_none_or(|process_id| event.scope_id == process_id)
                })
                .filter(|event| {
                    subscription
                        .kind
                        .as_deref()
                        .is_none_or(|kind| event.kind == kind)
                })
                .filter(|event| {
                    subscription
                        .criticality
                        .as_deref()
                        .is_none_or(|criticality| event.criticality == criticality)
                })
                .cloned()
                .collect::<Vec<_>>();
        events.sort_by(|left, right| right.source_seq.cmp(&left.source_seq));
        let offset = subscription.offset.min(events.len());
        let limit = subscription.limit.max(1);
        LedgerView {
            events: events.into_iter().skip(offset).take(limit).collect(),
            discontinuities: self.discontinuities.iter().cloned().collect(),
        }
    }

    pub fn snapshot_source_health(&self) -> SourceHealthView {
        self.source_health_view()
    }

    pub fn snapshot_source_replacement(
        &self,
        cursor: &super::state::SourceCursorView,
    ) -> SourceReplacementView {
        SourceReplacementView {
            source_id: self.source_id.clone(),
            source_epoch: cursor.source_epoch.clone(),
            source_seq: cursor.source_seq,
        }
    }

    pub fn snapshot_worktrees(&self) -> Vec<WorktreeView> {
        self.worktrees
            .keys()
            .filter_map(|worktree_id| self.worktree_view(worktree_id))
            .collect()
    }

    fn team_member_view(&self, member: TeamMemberRecord) -> TeamMemberView {
        let session = self.sessions.get(&member.agent_id).cloned();
        let worktree = member
            .worktree_id
            .as_deref()
            .and_then(|worktree_id| self.worktree_view(worktree_id));
        TeamMemberView {
            version: self.version(
                "team_member",
                format!("{}:{}", member.team_id, member.agent_id),
            ),
            member,
            session,
            worktree,
        }
    }
}

pub fn snapshot_cross_source_board(
    states: &BTreeMap<String, MaterializedState>,
    subscription: &BoardSubscription,
) -> FleetBoardView {
    let mut rows = states
        .values()
        .flat_map(|state| {
            state
                .snapshot_board(&BoardSubscription {
                    offset: 0,
                    limit: usize::MAX,
                    ..subscription.clone()
                })
                .rows
        })
        .filter(|row| board_row_matches(row, subscription))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .latest_activity_unix_ms
            .cmp(&left.latest_activity_unix_ms)
            .then_with(|| left.row_id.cmp(&right.row_id))
    });
    let total_rows = rows.len();
    let offset = subscription.offset.min(rows.len());
    let limit = subscription.limit.max(1);
    let rows = rows.into_iter().skip(offset).take(limit).collect();
    let cursors = states
        .values()
        .filter_map(MaterializedState::cursor)
        .collect::<Vec<_>>();
    FleetBoardView {
        rows,
        total_rows,
        cursor: cursors.first().cloned(),
        cursors,
    }
}

pub fn snapshot_cross_source_approval_inbox(
    states: &BTreeMap<String, MaterializedState>,
    subscription: &ApprovalInboxSubscription,
) -> ApprovalInboxView {
    let mut approvals = states
        .values()
        .filter(|state| {
            subscription
                .source_id
                .as_deref()
                .is_none_or(|source_id| state.source_id == source_id)
        })
        .flat_map(|state| {
            state
                .snapshot_approval_inbox(subscription)
                .approvals
                .into_iter()
        })
        .collect::<Vec<_>>();
    approvals.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.source_id.cmp(&right.source_id))
            .then_with(|| left.approval_id.cmp(&right.approval_id))
    });
    ApprovalInboxView { approvals }
}

pub fn snapshot_cross_source_ledger(
    states: &BTreeMap<String, MaterializedState>,
    subscription: &LedgerSubscription,
) -> LedgerView {
    let mut events = states
        .values()
        .filter(|state| {
            subscription
                .source_id
                .as_deref()
                .is_none_or(|source_id| state.source_id == source_id)
        })
        .flat_map(|state| state.snapshot_ledger(subscription).events.into_iter())
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.source_seq.cmp(&left.source_seq))
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
    let offset = subscription.offset.min(events.len());
    let limit = subscription.limit.max(1);
    let discontinuities = states
        .values()
        .filter(|state| {
            subscription
                .source_id
                .as_deref()
                .is_none_or(|source_id| state.source_id == source_id)
        })
        .flat_map(|state| state.discontinuities.iter().cloned())
        .collect();
    LedgerView {
        events: events.into_iter().skip(offset).take(limit).collect(),
        discontinuities,
    }
}

pub fn snapshot_cross_source_health(
    states: &BTreeMap<String, MaterializedState>,
    source_id: Option<&str>,
) -> Vec<SourceHealthView> {
    states
        .values()
        .filter(|state| source_id.is_none_or(|source_id| state.source_id == source_id))
        .map(MaterializedState::source_health_view)
        .collect()
}

pub fn snapshot_cross_source_worktrees(
    states: &BTreeMap<String, MaterializedState>,
    source_id: Option<&str>,
) -> Vec<WorktreeView> {
    states
        .values()
        .filter(|state| source_id.is_none_or(|source_id| state.source_id == source_id))
        .flat_map(MaterializedState::snapshot_worktrees)
        .collect()
}

pub fn approval_row_from_record(
    state: &MaterializedState,
    approval: &ApprovalRecord,
) -> ApprovalRowView {
    ApprovalRowView {
        source_id: state.source_id.clone(),
        approval_id: approval.id.clone(),
        session_id: approval.session_id.clone(),
        turn_id: approval.turn_id.clone(),
        tool_call_id: approval.tool_call_id.clone(),
        status: approval.status.clone(),
        risk: approval
            .request
            .get("risk")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string(),
        summary: approval_summary(approval),
        created_at: approval.created_at,
        resolved_at: approval.resolved_at,
        source_health: state.source_health.state,
        version: state.version("approval", &approval.id),
    }
}

fn board_row_matches(row: &AgentRowView, subscription: &BoardSubscription) -> bool {
    if subscription
        .source_id
        .as_deref()
        .is_some_and(|source_id| row.source_id != source_id)
    {
        return false;
    }
    if subscription
        .status_filter
        .as_deref()
        .is_some_and(|status| row.status != status)
    {
        return false;
    }
    if subscription
        .team_id
        .as_deref()
        .is_some_and(|team_id| row.team_id.as_deref() != Some(team_id))
    {
        return false;
    }
    if let Some(query) = subscription.query.as_deref().map(str::to_ascii_lowercase) {
        return row.session_id.to_ascii_lowercase().contains(&query)
            || row
                .title
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains(&query)
            || row.provider.to_ascii_lowercase().contains(&query)
            || row
                .model
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains(&query);
    }
    true
}

fn approval_summary(approval: &ApprovalRecord) -> String {
    approval
        .request
        .get("summary")
        .and_then(|value| value.as_str())
        .or_else(|| {
            approval
                .request
                .get("reason")
                .and_then(|value| value.as_str())
        })
        .or_else(|| {
            approval
                .request
                .get("tool")
                .and_then(|value| value.as_str())
        })
        .unwrap_or("Approval requested")
        .to_string()
}

fn transcript_entries(metadata: &serde_json::Value) -> Vec<TranscriptEntryView> {
    metadata
        .get("transcript")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            Some(TranscriptEntryView {
                role: entry.get("role")?.as_str()?.to_string(),
                text: entry.get("text")?.as_str()?.to_string(),
            })
        })
        .collect()
}

fn delivery_status_counts<'a>(
    deliveries: impl Iterator<Item = &'a TeamDeliveryRecord>,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for delivery in deliveries {
        *counts.entry(delivery.status.clone()).or_insert(0) += 1;
    }
    counts
}

#[allow(dead_code)]
fn _assert_process_view_send_sync(_: &ProcessView) {}
