use std::collections::BTreeSet;
use std::fmt;

use runtime_core::{ManagedWorktreeClaimRecord, ProviderKind};
use serde_json::{json, Value};

use crate::runtime::client::{GooselakeRuntimeClient, ProcessLogsQuery, RuntimeClientError};

use super::state::{MaterializedState, ModelCapabilityView};

#[derive(Debug, Clone)]
pub struct BootstrapOptions {
    pub selected_team_ids: BTreeSet<String>,
    pub default_team_limit: usize,
    pub team_message_limit: usize,
    pub process_tail_lines: usize,
    pub process_log_max_bytes: usize,
}

impl Default for BootstrapOptions {
    fn default() -> Self {
        Self {
            selected_team_ids: BTreeSet::new(),
            default_team_limit: 3,
            team_message_limit: 100,
            process_tail_lines: 100,
            process_log_max_bytes: 128 * 1024,
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
        let bootstrap = client.source_bootstrap().await?;
        let mut state = MaterializedState::new(
            client.source_id().to_string(),
            bootstrap.source_epoch.clone(),
        );
        state.mark_replaying();

        for session in bootstrap.records.sessions {
            state.upsert_session(session);
        }

        for team in bootstrap.records.teams {
            let team_id = team.id.clone();
            state.upsert_team(team);
            if state.default_team_ids.len() < options.default_team_limit {
                state.default_team_ids.insert(team_id.clone());
            }
        }
        for member in bootstrap.records.team_members {
            state.upsert_team_member(member);
        }
        state.selected_team_ids = options.selected_team_ids.clone();

        let team_snapshot_ids = state
            .default_team_ids
            .union(&state.selected_team_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut messages = bootstrap.records.team_messages;
        messages.reverse();
        let mut message_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut included_message_ids = BTreeSet::new();
        for message in messages {
            if !team_snapshot_ids.contains(&message.team_id) {
                continue;
            }
            let count = message_counts.entry(message.team_id.clone()).or_default();
            if *count >= options.team_message_limit {
                continue;
            }
            *count += 1;
            included_message_ids.insert(message.id.clone());
            state.upsert_message(message);
        }
        for delivery in bootstrap.records.team_deliveries {
            if included_message_ids.contains(&delivery.message_id) {
                state.upsert_delivery(delivery);
            }
        }

        for process in bootstrap.records.processes {
            let process_id = process.id.clone();
            state.upsert_process_summary(runtime_core::ProcessSummary {
                process_id: process.id,
                session_id: process.session_id,
                pid: process.pid,
                status: process.status,
                command: process.command,
                cwd: process.cwd,
                started_at: process.started_at,
                ended_at: process.ended_at,
            });
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

        for worktree in bootstrap.records.managed_worktrees {
            state.upsert_worktree(worktree);
        }
        for claim in bootstrap.records.managed_worktree_claims {
            if claim.released_at.is_none() {
                state
                    .active_worktree_claims
                    .entry(claim.worktree_id)
                    .or_default()
                    .insert(claim.session_id);
            }
        }
        infer_worktree_claims_from_members(&mut state);

        for approval in bootstrap.records.approvals {
            state.upsert_approval(approval);
        }

        let providers = client.providers().await?;
        let mut auth_status = Vec::new();
        let mut model_capabilities = Vec::new();
        for provider in &providers.providers {
            if let Some(kind) = ProviderKind::from_str(provider.kind.as_str()) {
                if let Ok(models) = client.provider_models(kind).await {
                    model_capabilities.extend(models.models.iter().map(|model| {
                        ModelCapabilityView::from_provider_model(provider.kind.as_str(), model)
                    }));
                }
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
        state.source_metadata.model_capabilities = model_capabilities;

        let diagnostics = client.diagnostics().await?;
        state.diagnostics_summary = json!({
            "providers": diagnostics.providers,
            "comms": diagnostics.comms,
            "processes": diagnostics.processes,
            "worktrees": diagnostics.worktrees,
            "recovery": diagnostics.recovery,
        });

        state.source_health.transition(
            crate::runtime::events::SourceHealthState::Live,
            Some(bootstrap.high_watermark),
            None,
        );
        state.mark_bootstrap_watermark(bootstrap.high_watermark);

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
