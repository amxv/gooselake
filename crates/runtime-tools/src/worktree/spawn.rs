use runtime_core::{
    ProviderKind, RuntimeError, TeamJoinRequest, TeamMemberSpawnAdmission, TeamMemberSpawnRequest,
    TeamMemberSpawnResponse, TeamRemoveMemberRequest, TeamSendDirectRequest,
    WorktreeCleanupRequest, WorktreeMemberRemovedRequest, WorktreeMemberRemovedResponse,
    WorktreeReleaseRequest, WorktreeService,
};

use crate::now_ms;

use super::RuntimeWorktreeService;

impl RuntimeWorktreeService {
    async fn rollback_spawn_resources(
        &self,
        team_id: &str,
        operation_id: &str,
        spawned_session_id: &str,
        assigned_worktree_id: Option<&str>,
        created_worktree_id: Option<&str>,
        remove_member: bool,
        release_claim: bool,
        reason_code: &str,
        reason_message: &str,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        // Record the exact compensation targets *before* touching another
        // native resource. A crash halfway through is an unresolved rollback,
        // never evidence that the next caller may spawn or clean up again.
        self.record_journal(
            operation_id,
            team_id,
            "rollback_pending",
            serde_json::json!({
                "reason": reason_code,
                "spawned_session_id": spawned_session_id,
                "assigned_worktree_id": assigned_worktree_id,
                "created_worktree_id": created_worktree_id,
                "remove_member": remove_member,
                "release_claim": release_claim,
                "payload": payload,
            }),
        )
        .map_err(|error| {
            RuntimeError::operation_unresolved(
                operation_id,
                format!(
                    "rollback intent could not be committed before native compensation: {error}"
                ),
            )
        })?;

        let mut outcomes = serde_json::Map::new();
        let mut uncertainties = Vec::new();

        if remove_member {
            match self
                .team_comms
                .remove_team_member(TeamRemoveMemberRequest {
                    team_id: team_id.to_string(),
                    agent_id: spawned_session_id.to_string(),
                })
                .await
            {
                Ok(_) => {
                    outcomes.insert("team_remove".into(), serde_json::json!("confirmed"));
                }
                Err(error) => {
                    uncertainties.push(format!("team_remove:{error}"));
                    outcomes.insert("team_remove".into(), serde_json::json!("unknown"));
                }
            }
        }

        match self
            .runtime
            .close_session(
                spawned_session_id,
                Some(format!("spawn_rollback_{reason_code}")),
            )
            .await
        {
            Ok(session) if session.status == "closed" => {
                outcomes.insert("session_close".into(), serde_json::json!("confirmed"));
            }
            Ok(session) => {
                uncertainties.push(format!("session_close_nonterminal:{}", session.status));
                outcomes.insert("session_close".into(), serde_json::json!("unknown"));
            }
            Err(error) => {
                uncertainties.push(format!("session_close:{error}"));
                // Force-close only changes the local projection. It does not
                // prove that an unsuccessful native provider close occurred.
                let forced = self
                    .runtime
                    .force_close_session(
                        spawned_session_id,
                        Some(format!("spawn_rollback_{reason_code}")),
                    )
                    .await;
                outcomes.insert(
                    "session_close".into(),
                    serde_json::json!({
                        "status": "unknown_native_close",
                        "force_close": match forced {
                            Ok(_) => "local_projection_closed".to_string(),
                            Err(error) => {
                                uncertainties.push(format!("session_force_close:{error}"));
                                "failed".to_string()
                            }
                        },
                    }),
                );
            }
        }

        // A forced local close after failed native provider acknowledgement
        // is not evidence the provider stopped using its checkout. Retain
        // claim/path until both member removal and native close are proven.
        let safe_to_release = outcomes.get("session_close")
            == Some(&serde_json::json!("confirmed"))
            && (!remove_member
                || outcomes.get("team_remove") == Some(&serde_json::json!("confirmed")));
        if let Some(worktree_id) = assigned_worktree_id.filter(|_| release_claim) {
            if !safe_to_release {
                uncertainties.push("worktree_release_deferred_unconfirmed_native".to_string());
                outcomes.insert("worktree_release".into(), serde_json::json!("deferred"));
            } else {
                match self
                    .release_worktree(WorktreeReleaseRequest {
                        worktree_id: worktree_id.to_string(),
                        session_id: spawned_session_id.to_string(),
                        cleanup_if_last_claim: Some(false),
                    })
                    .await
                {
                    Ok(result) if result.released_claim.released_at.is_some() => {
                        outcomes.insert("worktree_release".into(), serde_json::json!("confirmed"));
                    }
                    Ok(_) => {
                        uncertainties.push("worktree_release_not_confirmed".to_string());
                        outcomes.insert("worktree_release".into(), serde_json::json!("unknown"));
                    }
                    Err(error) => {
                        uncertainties.push(format!("worktree_release:{error}"));
                        outcomes.insert("worktree_release".into(), serde_json::json!("unknown"));
                    }
                }
            }
        }
        if let Some(worktree_id) = created_worktree_id {
            let release_confirmed = !release_claim
                || assigned_worktree_id.is_none()
                || outcomes.get("worktree_release") == Some(&serde_json::json!("confirmed"));
            if !safe_to_release || !release_confirmed {
                uncertainties
                    .push("worktree_cleanup_deferred_unconfirmed_native_or_claim".to_string());
                outcomes.insert("worktree_cleanup".into(), serde_json::json!("deferred"));
            } else {
                match self
                    .cleanup_worktree(WorktreeCleanupRequest {
                        worktree_id: worktree_id.to_string(),
                        reason: Some(format!("spawn_rollback_{reason_code}")),
                    })
                    .await
                {
                    Ok(result) if result.status == "deleted" => {
                        outcomes.insert(
                            "worktree_cleanup".into(),
                            serde_json::json!("confirmed_deleted"),
                        );
                    }
                    Ok(result) => {
                        uncertainties
                            .push(format!("worktree_cleanup_not_deleted:{}", result.status));
                        outcomes.insert(
                            "worktree_cleanup".into(),
                            serde_json::json!({
                                "status": result.status,
                                "diagnostics": result.diagnostics,
                            }),
                        );
                    }
                    Err(error) => {
                        uncertainties.push(format!("worktree_cleanup:{error}"));
                        outcomes.insert("worktree_cleanup".into(), serde_json::json!("unknown"));
                    }
                }
            }
        }

        if let Err(error) = self.store.append_team_operation_diagnostic(
            Some(operation_id),
            Some(team_id),
            reason_code,
            reason_message,
            &payload,
            now_ms(),
        ) {
            uncertainties.push(format!("rollback_diagnostic_write:{error}"));
        }
        let stage = if uncertainties.is_empty() {
            "rolled_back"
        } else {
            "rollback_review_required"
        };
        self.record_journal(
            operation_id,
            team_id,
            stage,
            serde_json::json!({
                "reason": reason_code,
                "message": reason_message,
                "payload": payload,
                "spawned_session_id": spawned_session_id,
                "assigned_worktree_id": assigned_worktree_id,
                "created_worktree_id": created_worktree_id,
                "remove_member": remove_member,
                "release_claim": release_claim,
                "compensation_outcomes": outcomes,
                "uncertainties": uncertainties,
            }),
        ).map_err(|error| RuntimeError::operation_unresolved(
            operation_id,
            format!("compensation outcomes could not be durably receipted; inspect the original rollback_pending intent: {error}"),
        ))?;
        if !uncertainties.is_empty() {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                format!(
                    "compensation needs independent review: {}",
                    uncertainties.join("; ")
                ),
            ));
        }
        Ok(())
    }

    fn record_journal(
        &self,
        operation_id: &str,
        team_id: &str,
        stage: &str,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let now = now_ms();
        // The store preserves the first admission's created_at on every
        // stage update. Never load the team's entire journal on this hot path.
        self.store
            .upsert_team_operation_journal(&runtime_core::TeamOperationJournalRecord {
                operation_id: operation_id.to_string(),
                team_id: team_id.to_string(),
                kind: "spawn_member_with_worktree".to_string(),
                stage: stage.to_string(),
                payload,
                created_at: now,
                updated_at: now,
            })
    }

    // A successful native action followed by a failed stage write is an
    // unknown outcome, not permission to repeat that action with a fresh ID.
    fn record_native_stage(
        &self,
        operation_id: &str,
        team_id: &str,
        stage: &str,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        self.record_journal(operation_id, team_id, stage, payload)
            .map_err(|error| RuntimeError::operation_unresolved(
                operation_id,
                format!("native effect may have succeeded but {stage} receipt failed: {error}; inspect original operation before any retry"),
            ))
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

        let request_hash = if let Some(key) = request.idempotency_key.as_deref() {
            if key.is_empty()
                || key.len() > 512
                || key.trim() != key
                || key.chars().any(char::is_control)
            {
                return Err(RuntimeError::InvalidState(
                    "spawn idempotency key must be 1-512 printable characters without surrounding whitespace".into(),
                ));
            }
            let hash =
                runtime_core::normalized_json_hash(&serde_json::to_value(&request).map_err(
                    |error| RuntimeError::InvalidState(format!("invalid spawn request: {error}")),
                )?)?;
            // Original receipt precedes mutable source-session lookup; a
            // retired session must not break the replay of its original key.
            if let Some(admission) = self.store.replay_team_member_spawn(
                &team_id,
                &request.source_session_id,
                key,
                &hash,
            )? {
                return match admission {
                    TeamMemberSpawnAdmission::Replay { response } => Ok(response),
                    TeamMemberSpawnAdmission::Unresolved { operation_id } => Err(
                        RuntimeError::operation_unresolved(
                            operation_id,
                            "original keyed spawn is not certified complete; inspect its operation before any native retry",
                        ),
                    ),
                    TeamMemberSpawnAdmission::Execute { .. } => Err(RuntimeError::Conflict(
                        "read-only team spawn replay cannot grant native execution".into(),
                    )),
                };
            }
            Some(hash)
        } else {
            None
        };

        let source_session = self
            .runtime
            .get_session(request.source_session_id.as_str())
            .await?;
        let source_cwd = source_session
            .cwd
            .clone()
            .ok_or_else(|| RuntimeError::InvalidState("source session has no cwd".to_string()))?;

        // Resolve provider authority before Git checkout creation or its init
        // script. A bad provider request must never leave native artifacts.
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

        // A registered adapter and valid launch permission are also pure
        // invariants. A valid provider name with no configured adapter must
        // not cause an orphaned Git checkout before create_session fails.
        let preflight_permission_mode = request
            .permission_mode
            .as_deref()
            .or(source_session.permission_mode.as_deref())
            .or(
                if provider == ProviderKind::Codex && request.worktree.is_some() {
                    Some("full_auto")
                } else {
                    None
                },
            );
        self.runtime
            .preflight_legacy_session_create(provider, preflight_permission_mode)?;

        if let Some(worktree) = request.worktree.as_ref() {
            let mode = worktree.mode.as_deref().unwrap_or("create").trim();
            if !mode.is_empty()
                && !["create", "reuse", "use_existing"]
                    .iter()
                    .any(|allowed| mode.eq_ignore_ascii_case(allowed))
            {
                return Err(RuntimeError::InvalidState(format!(
                    "unsupported worktree spawn mode {mode}"
                )));
            }
            Self::validate_managed_worktree_name(worktree.name.as_deref().unwrap_or_default())?;
            if let Some(prefix) = worktree.branch_prefix.as_deref() {
                if !prefix.trim().is_empty() {
                    Self::validate_managed_worktree_name(prefix)?;
                }
            }
        }

        let operation_id = self.allocate_operation_id();
        if let Some(hash) = request_hash.as_deref() {
            match self
                .store
                .admit_team_member_spawn(&operation_id, &request, hash, now_ms())?
            {
                TeamMemberSpawnAdmission::Execute {
                    operation_id: admitted,
                } if admitted == operation_id => {}
                TeamMemberSpawnAdmission::Replay { response } => return Ok(response),
                TeamMemberSpawnAdmission::Unresolved { operation_id } => {
                    return Err(RuntimeError::operation_unresolved(
                        operation_id,
                        "another worker owns the original keyed spawn; no repeated native effect is authorized",
                    ));
                }
                TeamMemberSpawnAdmission::Execute { .. } => {
                    return Err(RuntimeError::Conflict(
                        "team spawn admission did not match new operation ID".into(),
                    ));
                }
            }
        } else {
            // Legacy callers still admit an unkeyed operation for inspection,
            // but their lost reply cannot safely be retried automatically.
            self.record_journal(
                &operation_id,
                &team_id,
                "planned",
                serde_json::json!({
                    "source_session_id": request.source_session_id,
                    "worktree": request.worktree,
                }),
            )?;
        }

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
                let existing = if let Some(existing) =
                    self.worktree_by_identity(&hydrated, &planned)
                {
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
                    self.recover_native_checkout(
                            &planned,
                            &runtime_core::WorktreeCreateRequest {
                                team_id: Some(team_id.clone()),
                                source_session_id: source_session.id.clone(),
                                repo_root: Some(repo_root.clone()),
                                worktree_name: worktree_name.to_string(),
                                branch_prefix: worktree_input.branch_prefix.clone(),
                                base_ref: worktree_input.base_ref.clone(),
                                deletion_policy: Some("retain_on_last_claim".to_string()),
                                run_init_script: Some(false),
                                created_by_session_id: None,
                                operation_id: None,
                            },
                            Some(&source_session.id),
                        )?
                        .ok_or_else(|| RuntimeError::Conflict(
                            "existing native checkout cannot be verified as the intended repository and branch"
                                .into(),
                        ))?
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
                self.record_native_stage(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "worktree_created",
                    serde_json::json!({ "worktree": created.worktree }),
                )?;
            }
        }
        let assigned_worktree_id = worktree_record.as_ref().map(|row| row.id.clone());

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
                // A provider may have created a native session before a
                // transport error. Without its confirmed identity, deleting
                // the checkout or claiming complete rollback would be unsafe.
                self.record_native_stage(
                    operation_id.as_str(),
                    team_id.as_str(),
                    "session_create_review_required",
                    serde_json::json!({
                        "reason": "session_create_failed",
                        "error": error.to_string(),
                        "created_worktree_id": created_worktree_id,
                        "assigned_worktree_id": assigned_worktree_id,
                    }),
                )?;
                return Err(RuntimeError::operation_unresolved(
                    operation_id,
                    format!("provider session create result is unknown, inspect native admission and worktree before any retry: {error}"),
                ));
            }
        };

        self.record_native_stage(
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
                self.rollback_spawn_resources(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    false,
                    false,
                    "spawn_set_worktree_id_failed",
                    "spawn failed to bind new session to worktree",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "error": error.to_string(),
                    }),
                )
                .await?;
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
                // The join may have committed despite a lost response. The
                // freshly created session is its only legitimate member ID.
                self.rollback_spawn_resources(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    true,
                    false,
                    "spawn_join_failed",
                    "spawn team join failed or its acknowledgement is unknown",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "error": error.to_string(),
                    }),
                )
                .await?;
                return Err(error);
            }
        };

        #[cfg(test)]
        if Self::spawn_test_flag(&request.metadata, "__test_force_claim_failure_after_join") {
            let forced_error =
                RuntimeError::InvalidState("forced claim failure after join for test".to_string());
            self.rollback_spawn_resources(
                team_id.as_str(),
                operation_id.as_str(),
                spawned_session.id.as_str(),
                assigned_worktree_id.as_deref(),
                created_worktree_id.as_deref(),
                true,
                true,
                "spawn_claim_failed_after_join",
                "spawn worktree claim failed after team join",
                serde_json::json!({
                    "spawned_session_id": spawned_session.id,
                    "forced": true,
                }),
            )
            .await?;
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
                self.rollback_spawn_resources(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    true,
                    true,
                    "spawn_claim_failed_after_join",
                    "spawn worktree claim failed after team join",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "worktree_id": worktree.id,
                        "error": error.to_string(),
                    }),
                )
                .await?;
                return Err(error);
            }
        }

        self.record_native_stage(
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
            // Simulate a user edit in a disposable native checkout. Rollback
            // must retain dirty Git state and mark operator review required.
            if Self::spawn_test_flag(&request.metadata, "__test_dirty_checkout_before_rollback") {
                if let Some(worktree) = worktree_record.as_ref() {
                    std::fs::write(
                        std::path::Path::new(&worktree.worktree_cwd)
                            .join("preserve-operator-edit.txt"),
                        "uncommitted operator content\n",
                    )
                    .map_err(|error| {
                        RuntimeError::Io(format!(
                            "failed injecting disposable dirty checkout: {error}"
                        ))
                    })?;
                }
            }
            let forced_error = RuntimeError::InvalidState(
                "forced onboarding failure after join for test".to_string(),
            );
            self.rollback_spawn_resources(
                team_id.as_str(),
                operation_id.as_str(),
                spawned_session.id.as_str(),
                assigned_worktree_id.as_deref(),
                created_worktree_id.as_deref(),
                true,
                true,
                "spawn_onboarding_failed_after_join",
                "spawn onboarding delivery failed after team join",
                serde_json::json!({
                    "spawned_session_id": spawned_session.id,
                    "forced": true,
                }),
            )
            .await?;
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
                self.rollback_spawn_resources(
                    team_id.as_str(),
                    operation_id.as_str(),
                    spawned_session.id.as_str(),
                    assigned_worktree_id.as_deref(),
                    created_worktree_id.as_deref(),
                    true,
                    true,
                    "spawn_onboarding_failed_after_join",
                    "spawn onboarding delivery failed after team join",
                    serde_json::json!({
                        "spawned_session_id": spawned_session.id,
                        "error": error.to_string(),
                    }),
                )
                .await?;
                return Err(error);
            }
        };

        self.record_native_stage(
            operation_id.as_str(),
            team_id.as_str(),
            "onboarding_sent",
            serde_json::json!({
                "message_id": onboarding_ack.message.id,
                "delivery_ids": onboarding_ack.deliveries.iter().map(|row| row.id.clone()).collect::<Vec<_>>()
            }),
        )?;
        let spawned_member = joined_team
            .members
            .iter()
            .find(|member| member.agent_id == spawned_session.id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::operation_unresolved(
                    &operation_id,
                    "native onboarding was accepted but joined team has no spawned member",
                )
            })?;

        let response = TeamMemberSpawnResponse {
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
        };
        if request_hash.is_some() {
            self.store
                .complete_team_member_spawn(&response.operation_id, &response, now_ms())
                .map_err(|error| RuntimeError::operation_unresolved(
                    &response.operation_id,
                    format!("native onboarding may have succeeded but exact completion receipt failed: {error}"),
                ))?;
        } else {
            self.record_native_stage(
                &response.operation_id,
                &team_id,
                "completed",
                serde_json::json!({
                    "spawned_session_id": response.spawned_session.id,
                    "worktree_id": response.worktree.as_ref().map(|row| &row.id),
                }),
            )?;
        }
        Ok(response)
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
            let released = match self.store.transition_managed_worktree_claim(
                &released,
                runtime_core::ManagedWorktreeClaimTransition::Release,
            ) {
                Ok(released) => released,
                Err(error) => {
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
            };
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
