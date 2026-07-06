use std::collections::BTreeSet;
use std::fmt;

use runtime_core::{ManagedWorktreeClaimRecord, ProviderKind};
use serde_json::{json, Value};

use crate::runtime::client::{GooselakeRuntimeClient, ProcessLogsQuery, RuntimeClientError};

use super::state::MaterializedState;

#[derive(Debug, Clone)]
pub struct BootstrapOptions {
    pub selected_team_ids: BTreeSet<String>,
    pub default_team_limit: usize,
    pub team_message_limit: usize,
    pub process_tail_lines: usize,
    pub process_log_max_bytes: usize,
    pub replay_cursor_limit: usize,
}

impl Default for BootstrapOptions {
    fn default() -> Self {
        Self {
            selected_team_ids: BTreeSet::new(),
            default_team_limit: 3,
            team_message_limit: 100,
            process_tail_lines: 100,
            process_log_max_bytes: 128 * 1024,
            replay_cursor_limit: 1,
        }
    }
}

#[derive(Debug)]
pub enum BootstrapError {
    Runtime(RuntimeClientError),
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<RuntimeClientError> for BootstrapError {
    fn from(value: RuntimeClientError) -> Self {
        Self::Runtime(value)
    }
}

#[derive(Debug, Clone)]
pub struct SourceBootstrap {
    pub state: MaterializedState,
}

impl SourceBootstrap {
    pub async fn from_runtime_client(
        client: &GooselakeRuntimeClient,
        options: BootstrapOptions,
    ) -> Result<Self, BootstrapError> {
        let mut state = MaterializedState::new(
            client.source_id().to_string(),
            client.source_epoch().to_string(),
        );
        state.mark_replaying();

        let sessions = client.list_sessions().await?;
        for session in sessions {
            state.upsert_session(session);
        }

        let teams = client.list_teams().await?;
        for team_with_members in teams {
            let team_id = team_with_members.team.id.clone();
            state.upsert_team(team_with_members.team);
            for member in team_with_members.members {
                state.upsert_team_member(member);
            }
            if state.default_team_ids.len() < options.default_team_limit {
                state.default_team_ids.insert(team_id.clone());
            }
        }
        state.selected_team_ids = options.selected_team_ids.clone();

        let team_snapshot_ids = state
            .default_team_ids
            .union(&state.selected_team_ids)
            .cloned()
            .collect::<Vec<_>>();
        for team_id in team_snapshot_ids {
            let snapshot = client
                .team_view(
                    &team_id,
                    None,
                    Some(options.team_message_limit),
                    Some(true),
                    None,
                )
                .await?;
            state.upsert_team(snapshot.team.team);
            for member in snapshot.team.members {
                state.upsert_team_member(member);
            }
            for message in snapshot.messages {
                state.upsert_message(message);
            }
            for delivery in snapshot.deliveries_by_message_id.into_values().flatten() {
                state.upsert_delivery(delivery);
            }
        }

        let processes = client.list_processes(None, Some(true)).await?;
        for process in processes {
            let process_id = process.process_id.clone();
            state.upsert_process_summary(process);
            let logs = client
                .get_process_logs(
                    &process_id,
                    &ProcessLogsQuery {
                        session_id: None,
                        stream: None,
                        head_lines: None,
                        tail_lines: Some(options.process_tail_lines),
                        max_bytes: Some(options.process_log_max_bytes),
                    },
                )
                .await?;
            state.append_process_logs(&process_id, logs);
        }

        let worktrees = client.list_worktrees().await?;
        for worktree in worktrees {
            state.upsert_worktree(worktree);
        }
        infer_worktree_claims_from_members(&mut state);

        let providers = client.providers().await?;
        let mut auth_status = Vec::new();
        for provider in &providers.providers {
            if let Some(kind) = ProviderKind::from_str(provider.kind.as_str()) {
                let status = client.provider_auth_status(kind).await.ok();
                auth_status.push(json!({
                    "provider": provider.kind,
                    "display_name": provider.display_name,
                    "auth_status": status,
                }));
            }
        }
        state.provider_status = json!({
            "providers": providers.providers,
            "auth": auth_status,
        });

        let diagnostics = client.diagnostics().await?;
        state.diagnostics_summary = json!({
            "providers": diagnostics.providers,
            "comms": diagnostics.comms,
            "processes": diagnostics.processes,
            "worktrees": diagnostics.worktrees,
            "recovery": diagnostics.recovery,
        });

        let cursor_rows = client
            .replay_global_events(None, Some(options.replay_cursor_limit.max(1)))
            .await?;
        if let Some(last) = cursor_rows.last() {
            state.source_health.transition(
                crate::runtime::events::SourceHealthState::Live,
                Some(last.row_id),
                None,
            );
        }

        state.mark_live();
        Ok(Self { state })
    }
}

fn infer_worktree_claims_from_members(state: &mut MaterializedState) {
    let claims = state
        .members_by_team
        .values()
        .flat_map(|members| members.values())
        .filter_map(|member| {
            Some(ManagedWorktreeClaimRecord {
                worktree_id: member.worktree_id.clone()?,
                session_id: member.agent_id.clone(),
                claim_role: "team_member".to_string(),
                created_at: member.joined_at,
                released_at: None,
            })
        })
        .collect::<Vec<_>>();

    for claim in claims {
        if claim.released_at.is_none() {
            state
                .active_worktree_claims
                .entry(claim.worktree_id)
                .or_default()
                .insert(claim.session_id);
        }
    }
}

#[allow(dead_code)]
fn _assert_value(_: &Value) {}
