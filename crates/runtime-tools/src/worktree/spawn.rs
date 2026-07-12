use runtime_core::{
    ProviderKind, RuntimeError, TeamJoinRequest, TeamMemberSpawnRequest, TeamMemberSpawnResponse,
    TeamRemoveMemberRequest, TeamSendDirectRequest, WorktreeCleanupRequest,
    WorktreeMemberRemovedRequest, WorktreeMemberRemovedResponse, WorktreeReleaseRequest,
    WorktreeService,
};

use crate::now_ms;

use super::RuntimeWorktreeService;

impl RuntimeWorktreeService {
    async fn rollback_spawn_after_join(
        &self,
        team_id: &str,
        operation_id: &str,
        spawned_session_id: &str,
        assigned_worktree_id: Option<&str>,
        created_worktree_id: Option<&str>,
        reason_code: &str,
        reason_message: &str,
        payload: serde_json::Value,
    ) {
        let mut rollback_diagnostics = Vec::new();

        if let Err(error) = self
            .team_comms
            .remove_team_member(TeamRemoveMemberRequest {
                team_id: team_id.to_string(),
                agent_id: spawned_session_id.to_string(),
            })
            .await
        {
            rollback_diagnostics.push(format!("team_remove_failed:{error}"));
            let _ = self.store.append_team_operation_diagnostic(
                Some(operation_id),
                Some(team_id),
                "spawn_rollback_team_remove_failed",
                error.to_string().as_str(),
                &serde_json::json!({
                    "spawned_session_id": spawned_session_id
                }),
                now_ms(),
            );
        }

        if let Err(error) = self
            .runtime
            .close_session(
                spawned_session_id,
                Some(format!("spawn_rollback_{reason_code}")),
            )
            .await
        {
            rollback_diagnostics.push(format!("session_close_failed:{error}"));
            let _ = self.store.append_team_operation_diagnostic(
                Some(operation_id),
                Some(team_id),
                "spawn_rollback_session_close_failed",
                error.to_string().as_str(),
                &serde_json::json!({
                    "spawned_session_id": spawned_session_id
                }),
                now_ms(),
            );
            if let Err(force_error) = self
                .runtime
                .force_close_session(
                    spawned_session_id,
                    Some(format!("spawn_rollback_{reason_code}")),
                )
                .await
            {
                rollback_diagnostics.push(format!("session_force_close_failed:{force_error}"));
                let _ = self.store.append_team_operation_diagnostic(
                    Some(operation_id),
                    Some(team_id),
                    "spawn_rollback_session_force_close_failed",
                    force_error.to_string().as_str(),
                    &serde_json::json!({
                        "spawned_session_id": spawned_session_id
                    }),
                    now_ms(),
                );
            }
        }

        if let Some(worktree_id) = assigned_worktree_id {
            let _ = self
                .release_worktree(WorktreeReleaseRequest {
                    worktree_id: worktree_id.to_string(),
                    session_id: spawned_session_id.to_string(),
                    cleanup_if_last_claim: Some(false),
                })
                .await;
        }
        if let Some(worktree_id) = created_worktree_id {
            let _ = self
                .cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: worktree_id.to_string(),
                    reason: Some(format!("spawn_rollback_{reason_code}")),
                })
                .await;
        }

        let _ = self.store.append_team_operation_diagnostic(
            Some(operation_id),
            Some(team_id),
            reason_code,
            reason_message,
            &payload,
            now_ms(),
        );
        let _ = self.record_journal(
            operation_id,
            team_id,
            "rolled_back",
            serde_json::json!({
                "reason": reason_code,
                "message": reason_message,
                "payload": payload,
                "rollback_diagnostics": rollback_diagnostics,
            }),
        );
    }

    fn record_journal(
        &self,
        operation_id: &str,
        team_id: &str,
        stage: &str,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let now = now_ms();
        let existing = self
            .store
            .list_team_operation_journal(Some(team_id))?
            .into_iter()
            .find(|row| row.operation_id == operation_id);
        let created_at = existing.map(|row| row.created_at).unwrap_or(now);
        self.store
            .upsert_team_operation_journal(&runtime_core::TeamOperationJournalRecord {
                operation_id: operation_id.to_string(),
                team_id: team_id.to_string(),
                kind: "spawn_member_with_worktree".to_string(),
                stage: stage.to_string(),
                payload,
                created_at,
                updated_at: now,
            })
    }
}

impl RuntimeWorktreeService {
    pub(super) async fn spawn_team_member_impl(
        &self,
        request: TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError> {
        self.ensure_enabled()?;
        let team_id = request.team_id.trim().to_string();
        if team_id.is_empty() {
            return Err(RuntimeError::InvalidState(
                "team_id is required".to_string(),
            ));
        }
        let operation_id = self.allocate_operation_id();
        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "planned",
            serde_json::json!({
                "source_session_id": request.source_session_id,
                "worktree": request.worktree,
            }),
        )?;

        let source_session = self
            .runtime
            .get_session(request.source_session_id.as_str())
            .await?;
        let source_cwd = source_session
            .cwd
            .clone()
            .ok_or_else(|| RuntimeError::InvalidState("source session has no cwd".to_string()))?;

        let mut worktree_assignment_mode = "none".to_string();
        let mut worktree_created_by_operation = false;
        let mut worktree_record: Option<runtime_core::ManagedWorktreeRecord> = None;
        let mut created_worktree_id: Option<String> = None;

        let worktree_input = request.worktree.clone();
        if let Some(worktree_input) = worktree_input {
            let mode = worktree_input
                .mode
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("create")
                .to_ascii_lowercase();
            let worktree_name = worktree_input
                .name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    RuntimeError::InvalidState("worktree.name is required".to_string())
                })?;
            let repo_root = Self::resolve_repo_root_from_source_cwd(source_cwd.as_str())?;
            let planned = self.plan_worktree_paths(
                repo_root.as_str(),
                worktree_name,
                worktree_input.branch_prefix.as_deref(),
            );
            let reuse_requested = matches!(mode.as_str(), "reuse" | "use_existing");
            if reuse_requested {
                worktree_assignment_mode = "reused".to_string();
                let hydrated = self.store.hydrate_runtime_state()?;
                let existing =
                    if let Some(existing) = self.worktree_by_identity(&hydrated, &planned) {
                        let live_artifacts = Self::has_live_artifacts_for_record(&existing);
                        if !live_artifacts {
                            return Err(RuntimeError::NotFound(format!(
                                "reused worktree identity exists but artifacts are missing: {}",
                                planned.worktree_cwd
                            )));
                        }
                        existing
                    } else {
                        if !std::path::Path::new(planned.worktree_cwd.as_str()).exists() {
                            return Err(RuntimeError::NotFound(format!(
                                "reused worktree path not found: {}",
                                planned.worktree_cwd
                            )));
                        }
                        self.upsert_worktree_record(
                            self.allocate_worktree_id(),
                            &planned,
                            "retain_on_last_claim".to_string(),
                            None,
                            None,
                        )?
                    };
                worktree_record = Some(existing);
            } else {
                worktree_assignment_mode = "created".to_string();
                worktree_created_by_operation = true;
                let created = self
                    .create_worktree(runtime_core::WorktreeCreateRequest {
                        team_id: Some(team_id.clone()),
                        source_session_id: source_session.id.clone(),
                        repo_root: Some(repo_root),
                        worktree_name: worktree_name.to_string(),
                        branch_prefix: worktree_input.branch_prefix.clone(),
                        base_ref: worktree_input.base_ref.clone(),
                        deletion_policy: Some("delete_on_last_claim".to_string()),
                        run_init_script: worktree_input.run_init_script,
                        created_by_session_id: Some(source_session.id.clone()),
                        operation_id: Some(operation_id.clone()),
                    })
                    .await?;
                created_worktree_id = Some(created.worktree.id.clone());
                worktree_record = Some(created.worktree.clone());
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "worktree_created",
                    serde_json::json!({ "worktree": created.worktree }),
                )?;
            }
        }
        let assigned_worktree_id = worktree_record.as_ref().map(|row| row.id.clone());

        let provider = match request.provider.as_deref() {
            Some(provider) => ProviderKind::from_str(provider).ok_or_else(|| {
                RuntimeError::InvalidState(format!("unsupported provider {}", provider))
            })?,
            None => ProviderKind::from_str(source_session.provider.as_str()).ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "source session has unsupported provider {}",
                    source_session.provider
                ))
            })?,
        };

        let spawn_cwd = worktree_record
            .as_ref()
            .map(|row| row.worktree_cwd.clone())
            .or_else(|| source_session.cwd.clone());
        let resolved_permission_mode = request
            .permission_mode
            .clone()
            .or(source_session.permission_mode.clone())
            .or_else(|| {
                if provider == ProviderKind::Codex && worktree_record.is_some() {
                    Some("full_auto".to_string())
                } else {
                    None
                }
            });
        let spawned_session = match self
            .runtime
            .create_session(runtime_core::CreateSessionInput {
                provider,
                model: request.model.clone().or(source_session.model.clone()),
                cwd: spawn_cwd,
                permission_mode: resolved_permission_mode,
                metadata: request.metadata.clone(),
            })
            .await
        {
            Ok(session) => session,
            Err(error) => {
                if let Some(worktree_id) = created_worktree_id {
                    let _ = self
                        .cleanup_worktree(WorktreeCleanupRequest {
                            worktree_id,
                            reason: Some("spawn_session_create_failed".to_string()),
                        })
                        .await;
                }
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "rolled_back",
                    serde_json::json!({
                        "reason": "session_create_failed",
                        "error": error.to_string()
                    }),
                )?;
                return Err(error);
            }
        };

        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "session_created",
            serde_json::json!({ "spawned_session_id": spawned_session.id }),
        )?;

        if let Some(worktree) = worktree_record.as_ref() {
            if let Err(error) = self
                .runtime
                .set_session_worktree_id(spawned_session.id.as_str(), Some(worktree.id.clone()))
                .await
            {
                let close_reason = Some("spawn_set_worktree_id_failed".to_string());
                if let Err(close_error) = self
                    .runtime
                    .close_session(spawned_session.id.as_str(), close_reason.clone())
                    .await
                {
                    let _ = self.store.append_team_operation_diagnostic(
                        Some(operation_id.as_str()),
                        Some(team_id.as_str()),
                        "spawn_set_worktree_id_session_close_failed",
                        close_error.to_string().as_str(),
                        &serde_json::json!({
                            "spawned_session_id": spawned_session.id,
                        }),
                        now_ms(),
                    );
                    let _ = self
                        .runtime
                        .force_close_session(spawned_session.id.as_str(), close_reason.clone())
                        .await;
                }
                if let Some(worktree_id) = created_worktree_id.as_deref() {
                    let _ = self
                        .cleanup_worktree(WorktreeCleanupRequest {
                            worktree_id: worktree_id.to_string(),
                            reason: Some("spawn_set_worktree_id_failed".to_string()),
                        })
                        .await;
                }
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "rolled_back",
                    serde_json::json!({
                        "reason": "set_session_worktree_id_failed",
                        "error": error.to_string(),
                    }),
                )?;
                return Err(error);
            }
        }

        let joined_team = match self
            .team_comms
            .join_team(TeamJoinRequest {
                team_id: team_id.clone(),
                agent_id: spawned_session.id.clone(),
                title: request.title.clone(),
                added_by: Some(source_session.id.clone()),
                creator_agent_id: request.creator_agent_id.clone(),
                creator_compaction_subscription: request.creator_compaction_subscription.clone(),
                worktree_id: worktree_record.as_ref().map(|row| row.id.clone()),
            })
            .await
        {
            Ok(team) => team,
            Err(error) => {
                let _ = self
                    .runtime
                    .close_session(
                        spawned_session.id.as_str(),
                        Some("spawn_join_failed".to_string()),
                    )
                    .await;
                if let Some(worktree_id) = created_worktree_id {
                    let _ = self
                        .cleanup_worktree(WorktreeCleanupRequest {
                            worktree_id,
                            reason: Some("spawn_join_failed".to_string()),
                        })
                        .await;
                }
                self.record_journal(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "rolled_back",
                    serde_json::json!({
                        "reason": "team_join_failed",
                        "error": error.to_string()
                    }),
                )?;
                return Err(error);
            }
        };

        #[cfg(test)]
        if Self::spawn_test_flag(&request.metadata, "__test_force_claim_failure_after_join") {
            let forced_error =
                RuntimeError::InvalidState("forced claim failure after join for test".to_string());
            self.rollback_spawn_after_join(
                team_id.as_str(),
                operation_id.as_str(),
                spawned_session.id.as_str(),
                assigned_worktree_id.as_deref(),
                created_worktree_id.as_deref(),
                "spawn_claim_failed_after_join",
                "spawn worktree claim failed after team join",
                serde_json::json!({
                    "spawned_session_id": spawned_session.id,
                    "forced": true,
                }),
            )
            .await;
            return Err(forced_error);
        }

        if let Some(worktree) = worktree_record.as_ref() {
            if let Err(error) = self
                .claim_worktree(runtime_core::WorktreeClaimRequest {
                    worktree_id: worktree.id.clone(),
                    session_id: spawned_session.id.clone(),
                    claim_role: if worktree_created_by_operation {
                        "owner".to_string()
                    } else {
                        "consumer".to_string()
                    },
                })
                .await
            {
                self.rollback_spawn_after_join(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    "spawn_claim_failed_after_join",
                    "spawn worktree claim failed after team join",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "worktree_id": worktree.id,
                        "error": error.to_string(),
                    }),
                )
                .await;
                return Err(error);
            }
        }

        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "team_joined",
            serde_json::json!({ "spawned_session_id": spawned_session.id }),
        )?;

        #[cfg(test)]
        if Self::spawn_test_flag(
            &request.metadata,
            "__test_force_onboarding_failure_after_join",
        ) {
            let forced_error = RuntimeError::InvalidState(
                "forced onboarding failure after join for test".to_string(),
            );
            self.rollback_spawn_after_join(
                team_id.as_str(),
                operation_id.as_str(),
                spawned_session.id.as_str(),
                assigned_worktree_id.as_deref(),
                created_worktree_id.as_deref(),
                "spawn_onboarding_failed_after_join",
                "spawn onboarding delivery failed after team join",
                serde_json::json!({
                    "spawned_session_id": spawned_session.id,
                    "forced": true,
                }),
            )
            .await;
            return Err(forced_error);
        }

        let onboarding_text = {
            let mut text = format!(
                "You were added to team \"{}\" ({}).\nThe team lead is {}.\nYour name is {}.",
                joined_team.team.name,
                joined_team.team.id,
                joined_team.team.lead_agent_id,
                spawned_session.id
            );
            if let Some(title) = request
                .title
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                text.push_str(format!("\nYour title is {}.", title).as_str());
            }
            if let Some(prompt) = request
                .prompt
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                text.push_str("\n\nRole instructions:\n");
                text.push_str(prompt);
            }
            text
        };
        let onboarding_image_paths = request
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("gg_team_manage_add_image_paths"))
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let onboarding_input = vec![serde_json::json!({
            "type": "text",
            "text": onboarding_text,
        })];
        let onboarding_ack = self
            .team_comms
            .send_direct(TeamSendDirectRequest {
                team_id: team_id.clone(),
                sender_agent_id: source_session.id.clone(),
                recipient_agent_id: spawned_session.id.clone(),
                input: serde_json::json!(onboarding_input),
                image_paths: onboarding_image_paths,
                priority: "normal".to_string(),
                policy: "start_new_turn_only".to_string(),
                correlation_id: Some(format!("spawn-onboarding:{operation_id}")),
                reply_to_message_id: None,
                idempotency_key: Some(format!("spawn-onboarding:{operation_id}")),
            })
            .await;
        let onboarding_ack = match onboarding_ack {
            Ok(ack) => ack,
            Err(error) => {
                self.rollback_spawn_after_join(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    "spawn_onboarding_failed_after_join",
                    "spawn onboarding delivery failed after team join",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "error": error.to_string(),
                    }),
                )
                .await;
                return Err(error);
            }
        };

        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "onboarding_sent",
            serde_json::json!({
                "message_id": onboarding_ack.message.id,
                "delivery_ids": onboarding_ack.deliveries.iter().map(|row| row.id.clone()).collect::<Vec<_>>()
            }),
        )?;
        self.record_journal(
            operation_id.as_str(),
            team_id.as_str(),
            "completed",
            serde_json::json!({
                "spawned_session_id": spawned_session.id,
                "worktree_id": worktree_record.as_ref().map(|row| row.id.clone()),
            }),
        )?;

        let spawned_member = joined_team
            .members
            .iter()
            .find(|member| member.agent_id == spawned_session.id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::InvalidState("spawned member missing after join".to_string())
            })?;

        Ok(TeamMemberSpawnResponse {
            operation_id,
            team: joined_team,
            spawned_session,
            spawned_member,
            worktree: worktree_record,
            worktree_assignment_mode,
            worktree_created_by_operation,
            onboarding: serde_json::json!({
                "status": "sent",
                "message_id": onboarding_ack.message.id,
                "delivery_ids": onboarding_ack.deliveries.into_iter().map(|row| row.id).collect::<Vec<_>>()
            }),
            journal_stage: "completed".to_string(),
        })
    }

    pub(super) async fn on_member_removed_impl(
        &self,
        request: WorktreeMemberRemovedRequest,
    ) -> Result<WorktreeMemberRemovedResponse, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let mut released_claims = Vec::new();
        let mut cleanup_results = Vec::new();
        let mut diagnostics = Vec::new();
        let active_claims = hydrated
            .managed_worktree_claims
            .iter()
            .filter(|row| row.session_id == request.agent_id && row.released_at.is_none())
            .cloned()
            .collect::<Vec<_>>();
        for claim in active_claims {
            let released = runtime_core::ManagedWorktreeClaimRecord {
                released_at: Some(now_ms()),
                ..claim.clone()
            };
            if let Err(error) = self
                .append_worktree_event_with_mutations(
                    released.worktree_id.as_str(),
                    "worktree.released",
                    serde_json::json!({ "claim": released }),
                    Some(released.session_id.clone()),
                    Some(request.team_id.clone()),
                    &[runtime_core::RuntimeRecordMutation::ManagedWorktreeClaim(
                        released.clone(),
                    )],
                )
                .await
            {
                let diag = self.store.append_team_operation_diagnostic(
                    None,
                    Some(request.team_id.as_str()),
                    "worktree_claim_release_failed",
                    error.to_string().as_str(),
                    &serde_json::json!({
                        "worktree_id": claim.worktree_id,
                        "session_id": claim.session_id
                    }),
                    now_ms(),
                )?;
                diagnostics.push(diag);
                continue;
            }
            released_claims.push(released.clone());
            match self
                .cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: released.worktree_id.clone(),
                    reason: Some("team_member_removed".to_string()),
                })
                .await
            {
                Ok(result) => cleanup_results.push(result),
                Err(error) => {
                    let diag = self.store.append_team_operation_diagnostic(
                        None,
                        Some(request.team_id.as_str()),
                        "worktree_cleanup_failed_on_member_remove",
                        error.to_string().as_str(),
                        &serde_json::json!({
                            "worktree_id": released.worktree_id,
                            "session_id": released.session_id
                        }),
                        now_ms(),
                    )?;
                    diagnostics.push(diag);
                }
            }
        }
        Ok(WorktreeMemberRemovedResponse {
            released_claims,
            cleanup_results,
            diagnostics,
        })
    }
}
