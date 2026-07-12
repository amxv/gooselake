use super::*;
use send_turn::command_send_turn_input;

mod send_turn;

impl GatewayState {
    pub(super) async fn admit_and_route_command(
        &self,
        conn: &mut ConnectionState,
        command: Command,
    ) -> RealtimeEnvelope {
        let admission_started = Instant::now();
        if command.command_id.trim().is_empty() {
            self.metrics
                .command_rejected_count
                .fetch_add(1, Ordering::Relaxed);
            return command_rejected("", "missing_command_id", "command_id is required", false);
        }
        if command.created_at_client_unix_ms <= 0 {
            self.metrics
                .command_rejected_count
                .fetch_add(1, Ordering::Relaxed);
            return command_rejected(
                &command.command_id,
                REASON_INVALID_TARGET,
                "created_at_client_unix_ms is required",
                false,
            );
        }
        if !conn.auth.has_scope("gateway:command") {
            self.metrics
                .command_rejected_count
                .fetch_add(1, Ordering::Relaxed);
            return command_rejected(
                &command.command_id,
                REASON_UNAUTHORIZED,
                "ticket does not include gateway:command scope",
                false,
            );
        }

        let payload_kind = command_payload_kind(&command);
        let (target_scope, target_scope_id, target_entity_id) = command_target_labels(&command);
        {
            let mut store = self.command_store.lock().await;
            store.prune(now_ms());
            if let Some(existing) = store.get(&command.command_id) {
                match &existing.disposition {
                    CommandDisposition::Pending => {
                        tracing::debug!(command_id = %command.command_id, "duplicate command is still pending");
                    }
                    CommandDisposition::Accepted { gateway_seq } => {
                        tracing::debug!(command_id = %command.command_id, gateway_seq, "duplicate command was accepted");
                    }
                    CommandDisposition::Rejected {
                        code,
                        message,
                        retryable,
                    } => {
                        tracing::debug!(
                            command_id = %command.command_id,
                            code,
                            message,
                            retryable,
                            "duplicate command was rejected"
                        );
                    }
                }
                tracing::info!(
                    command_id = %command.command_id,
                    reason = REASON_DUPLICATE,
                    payload_kind = payload_kind,
                    target_scope = target_scope,
                    target_scope_id = target_scope_id,
                    target_entity_id = target_entity_id,
                    "gateway audit command.duplicate"
                );
                return command_duplicate(&command.command_id, &existing.original_command_id);
            }
            store.insert_pending(&command.command_id);
        }

        let upstream_started = Instant::now();
        let result = self.route_command(conn, &command).await;
        self.metrics.upstream_command_latency_ms.store(
            upstream_started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        let mut store = self.command_store.lock().await;
        match result {
            Ok(()) => {
                let gateway_seq = self.next_gateway_seq.fetch_add(1, Ordering::Relaxed);
                store.complete(
                    &command.command_id,
                    CommandDisposition::Accepted { gateway_seq },
                );
                self.metrics
                    .command_accepted_count
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.command_admission_latency_ms.store(
                    admission_started.elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
                tracing::info!(
                    command_id = %command.command_id,
                    payload_kind = payload_kind,
                    target_scope = target_scope,
                    target_scope_id = target_scope_id,
                    target_entity_id = target_entity_id,
                    gateway_seq,
                    "gateway audit command.accepted"
                );
                envelope_with_payload(
                    MessageKind::CommandAccepted,
                    Lane::Critical,
                    Payload::CommandAccepted(CommandAccepted {
                        command_id: command.command_id,
                        gateway_seq,
                    }),
                )
            }
            Err(error) => {
                store.complete(
                    &command.command_id,
                    CommandDisposition::Rejected {
                        code: error.code.clone(),
                        message: error.message.clone(),
                        retryable: error.retryable,
                    },
                );
                self.metrics
                    .command_rejected_count
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.command_admission_latency_ms.store(
                    admission_started.elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
                tracing::info!(
                    command_id = %command.command_id,
                    reason = %error.code,
                    retryable = error.retryable,
                    payload_kind = payload_kind,
                    target_scope = target_scope,
                    target_scope_id = target_scope_id,
                    target_entity_id = target_entity_id,
                    "gateway audit command.rejected"
                );
                command_rejected(
                    &command.command_id,
                    &error.code,
                    &error.message,
                    error.retryable,
                )
            }
        }
    }

    async fn route_command(
        &self,
        conn: &ConnectionState,
        command: &Command,
    ) -> Result<(), CommandRouteError> {
        let Some(payload) = command.payload.as_ref() else {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_SCOPE,
                "missing command payload",
                false,
            ));
        };
        self.validate_command_target(command, payload)?;
        let client = self
            .runtime_client_for_command(conn, command, payload)
            .await?;
        match payload {
            crate::protocol::generated::goosetower::v1::command::Payload::CreateSession(input) => {
                let provider = ProviderKind::from_str(non_empty(&input.provider, "provider")?)
                    .ok_or_else(|| {
                        CommandRouteError::with_code(
                            REASON_INVALID_TARGET,
                            format!("unknown provider {}", input.provider),
                            false,
                        )
                    })?;
                let mut metadata = serde_json::Map::new();
                for (key, value) in &input.metadata {
                    if !key.trim().is_empty() {
                        metadata.insert(key.clone(), json!(value));
                    }
                }
                if let Some(title) = optional_string(&input.title) {
                    metadata.insert("title".to_string(), json!(title));
                }
                let session = client
                    .create_session(&runtime_core::CreateSessionInput {
                        provider,
                        model: optional_string(&input.model),
                        cwd: optional_string(&input.cwd),
                        permission_mode: optional_string(&input.permission_mode),
                        metadata: (!metadata.is_empty()).then_some(Value::Object(metadata)),
                    })
                    .await?;
                self.merge_authoritative_session(client.source_id(), session)
                    .await;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::CreateTeam(input) => {
                let team = client
                    .create_team(&TeamCreateInput {
                        name: non_empty(&input.name, "name")?.to_string(),
                        lead_agent_id: non_empty(&input.lead_agent_id, "lead_agent_id")?
                            .to_string(),
                        member_agent_ids: Some(
                            input
                                .member_agent_ids
                                .iter()
                                .filter_map(|member_id| optional_string(member_id))
                                .collect(),
                        ),
                        created_by: optional_string(&input.created_by)
                            .or_else(|| Some(conn.auth.subject.clone())),
                    })
                    .await?;
                self.merge_authoritative_team(client.source_id(), team)
                    .await;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::JoinTeamMember(input) => {
                let team_id = non_empty(&input.team_id, "team_id")?;
                let agent_id = non_empty(&input.agent_id, "agent_id")?;
                let team = client
                    .join_team(
                        team_id,
                        &TeamJoinInput {
                            agent_id: agent_id.to_string(),
                            title: optional_string(&input.title),
                            added_by: optional_string(&input.added_by)
                                .or_else(|| Some(conn.auth.subject.clone())),
                            creator_agent_id: Some(conn.auth.subject.clone()),
                            creator_compaction_subscription: None,
                            worktree_id: None,
                        },
                    )
                    .await?;
                self.merge_authoritative_team(client.source_id(), team)
                    .await;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::SendTurn(input) => {
                let session_id = non_empty(&input.session_id, "session_id")?;
                let turn_input = command_send_turn_input(input)?;
                client
                    .send_turn(
                        session_id,
                        &SendTurnInput {
                            input: turn_input,
                            expected_turn_id: None,
                            permission_mode: None,
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::ResolveApproval(
                input,
            ) => {
                let approval_id = non_empty(&input.approval_id, "approval_id")?;
                let session_id = command
                    .target
                    .as_ref()
                    .map(|target| target.scope_id.as_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CommandRouteError::with_code(
                            REASON_INVALID_TARGET,
                            "target.scope_id session_id is required",
                            false,
                        )
                    })?;
                client
                    .respond_approval(
                        session_id,
                        approval_id,
                        &ApprovalResponseInput {
                            decision: if input.approved { "accept" } else { "reject" }.to_string(),
                            payload: Some(json!({ "reason": input.reason })),
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::InterruptTurn(input) => {
                client
                    .interrupt_turn(
                        non_empty(&input.session_id, "session_id")?,
                        non_empty(&input.turn_id, "turn_id")?,
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::SendTeamMessage(
                input,
            ) => {
                let team_id = non_empty(&input.team_id, "team_id")?;
                let sender_agent_id = self.team_sender_agent_id(team_id, &conn.auth.subject).await;
                let ack = client
                    .send_team_direct(
                        team_id,
                        &TeamDirectInput {
                            sender_agent_id,
                            recipient_agent_id: non_empty(
                                &input.recipient_member_id,
                                "recipient_member_id",
                            )?
                            .to_string(),
                            input: json!([{ "type": "text", "text": input.text }]),
                            image_paths: None,
                            priority: Some("normal".to_string()),
                            policy: Some("non_interrupting".to_string()),
                            correlation_id: Some(command.command_id.clone()),
                            reply_to_message_id: None,
                            idempotency_key: Some(command.command_id.clone()),
                        },
                    )
                    .await?;
                self.merge_authoritative_team_message(client.source_id(), ack)
                    .await;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::BroadcastTeamMessage(
                input,
            ) => {
                let team_id = non_empty(&input.team_id, "team_id")?;
                let sender_agent_id = self.team_sender_agent_id(team_id, &conn.auth.subject).await;
                let ack = client
                    .send_team_broadcast(
                        team_id,
                        &TeamBroadcastInput {
                            sender_agent_id,
                            input: json!([{ "type": "text", "text": input.text }]),
                            image_paths: None,
                            priority: Some("normal".to_string()),
                            policy: Some("non_interrupting".to_string()),
                            include_sender: Some(true),
                            correlation_id: Some(command.command_id.clone()),
                            idempotency_key: Some(command.command_id.clone()),
                        },
                    )
                    .await?;
                self.merge_authoritative_team_message(client.source_id(), ack)
                    .await;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::SpawnTeamMember(
                input,
            ) => {
                let source_session_id = command
                    .target
                    .as_ref()
                    .map(|target| target.entity_id.as_str())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(conn.auth.subject.as_str());
                client
                    .spawn_team_member(
                        non_empty(&input.team_id, "team_id")?,
                        &TeamMemberSpawnInput {
                            source_session_id: source_session_id.to_string(),
                            provider: None,
                            model: if input.model_preset.is_empty() {
                                None
                            } else {
                                Some(input.model_preset.clone())
                            },
                            title: optional_string(&input.title),
                            prompt: optional_string(&input.prompt),
                            permission_mode: None,
                            metadata: None,
                            worktree: None,
                            creator_agent_id: Some(conn.auth.subject.clone()),
                            creator_compaction_subscription: None,
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::RetryDelivery(input) => {
                let team_id = command
                    .target
                    .as_ref()
                    .map(|target| target.scope_id.as_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CommandRouteError::with_code(
                            REASON_INVALID_TARGET,
                            "target.scope_id team_id is required",
                            false,
                        )
                    })?;
                client
                    .retry_team_delivery(team_id, non_empty(&input.delivery_id, "delivery_id")?)
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::CancelDelivery(input) => {
                let team_id = command
                    .target
                    .as_ref()
                    .map(|target| target.scope_id.as_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CommandRouteError::with_code(
                            REASON_INVALID_TARGET,
                            "target.scope_id team_id is required",
                            false,
                        )
                    })?;
                client
                    .cancel_team_message(team_id, non_empty(&input.message_id, "message_id")?)
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::KillProcess(input) => {
                client
                    .kill_process(
                        non_empty(&input.process_id, "process_id")?,
                        &ProcessKillInput {
                            session_id: command
                                .target
                                .as_ref()
                                .map(|target| target.scope_id.clone())
                                .filter(|value| !value.is_empty()),
                            reason: Some(format!("goosetower command {}", command.command_id)),
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::StartProcess(input) => {
                client
                    .start_process(&ProcessStartInput {
                        command: non_empty(&input.command, "command")?.to_string(),
                        cwd: optional_string(&input.cwd),
                        timeout_ms: (input.timeout_ms > 0).then_some(input.timeout_ms),
                        session_id: command
                            .target
                            .as_ref()
                            .map(|target| target.scope_id.clone())
                            .filter(|value| !value.is_empty()),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    fn validate_command_target(
        &self,
        command: &Command,
        payload: &crate::protocol::generated::goosetower::v1::command::Payload,
    ) -> Result<(), CommandRouteError> {
        let Some(target) = command.target.as_ref() else {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                "command target is required",
                false,
            ));
        };
        let target_scope = Scope::try_from(target.scope).unwrap_or(Scope::Unspecified);
        let expected_scope = expected_scope_for_payload(payload);
        if target_scope != expected_scope {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_SCOPE,
                format!(
                    "command target scope must be {:?} for {}",
                    expected_scope,
                    command_payload_kind(command)
                ),
                false,
            ));
        }
        self.validate_entity_version(command, target, expected_scope)
    }

    fn validate_entity_version(
        &self,
        command: &Command,
        target: &EntityRef,
        target_scope: Scope,
    ) -> Result<(), CommandRouteError> {
        let expected = command.base_entity_version.max(target.entity_version);
        if expected == 0 {
            return Ok(());
        }
        let Some(entity_kind) = materialized_entity_kind_for_scope(target_scope) else {
            return Ok(());
        };
        let entity_id = if target.entity_id.starts_with("source:") || target.entity_id.is_empty() {
            target.scope_id.as_str()
        } else {
            target.entity_id.as_str()
        };
        if entity_id.is_empty() {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                "target entity id is required",
                false,
            ));
        }
        let source_id = command_explicit_source_id(command);
        let materialized = self.materialized.try_read().map_err(|_| {
            CommandRouteError::with_code(REASON_SOURCE_STALE, "source state is busy", true)
        })?;
        let states = materialized
            .iter()
            .filter(|(candidate_source_id, state)| {
                source_id.is_none_or(|source_id| candidate_source_id.as_str() == source_id)
                    && state.ownership.owns(entity_kind, entity_id)
            })
            .collect::<Vec<_>>();
        let Some((_, state)) = states.first().copied().or_else(|| {
            source_id.and_then(|source_id| {
                materialized
                    .iter()
                    .find(|(candidate_source_id, _)| candidate_source_id.as_str() == source_id)
            })
        }) else {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                "target source is not materialized",
                true,
            ));
        };
        let actual = state.version(entity_kind, entity_id).0;
        if actual > 0 && actual != expected {
            return Err(CommandRouteError::with_code(
                REASON_STALE_ENTITY_VERSION,
                format!(
                    "stale {entity_kind} version for {entity_id}: expected {expected}, current {actual}"
                ),
                true,
            ));
        }
        Ok(())
    }

    async fn runtime_client_for_command(
        &self,
        conn: &ConnectionState,
        command: &Command,
        payload: &crate::protocol::generated::goosetower::v1::command::Payload,
    ) -> Result<GooselakeRuntimeClient, CommandRouteError> {
        let explicit_source_id = command_explicit_source_id(command);
        let owner = self
            .resolve_command_owner_source(command, payload, explicit_source_id)
            .await?;
        let source = self
            .config
            .runtimes
            .sources
            .iter()
            .find(|candidate| {
                candidate.enabled
                    && candidate.workspace_id == conn.auth.workspace_id
                    && candidate.source_id == owner
            })
            .ok_or_else(|| {
                CommandRouteError::with_code(
                    REASON_SOURCE_UNAVAILABLE,
                    format!("runtime source {owner} is unavailable"),
                    true,
                )
            })?;
        let materialized = self.materialized.read().await;
        let state = materialized.get(source.source_id.as_str()).ok_or_else(|| {
            CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                format!("runtime source {} is not materialized", source.source_id),
                true,
            )
        })?;
        let stale_age = now_ms().saturating_sub(state.source_health.updated_at) as u64;
        self.metrics
            .source_stale_age_ms
            .store(stale_age, Ordering::Relaxed);
        let stale_after = self.config.replay.source_stale_after_ms;
        match state.source_health.state {
            SourceHealthState::Live if stale_age <= stale_after => {}
            SourceHealthState::Draining => {
                return Err(CommandRouteError::with_code(
                    REASON_SOURCE_UNAVAILABLE,
                    format!("runtime source {} is draining", source.source_id),
                    true,
                ));
            }
            SourceHealthState::GapDetected => {
                return Err(CommandRouteError::with_code(
                    REASON_SOURCE_GAP,
                    format!("runtime source {} has a replay gap", source.source_id),
                    true,
                ));
            }
            SourceHealthState::Configured
            | SourceHealthState::Provisioning
            | SourceHealthState::Booting
            | SourceHealthState::Offline
            | SourceHealthState::Failed
            | SourceHealthState::Terminated => {
                return Err(CommandRouteError::with_code(
                    REASON_SOURCE_UNAVAILABLE,
                    format!(
                        "runtime source {} is {}",
                        source.source_id,
                        state.source_health.state.command_admission_label()
                    ),
                    true,
                ));
            }
            _ => {
                return Err(CommandRouteError::with_code(
                    REASON_SOURCE_STALE,
                    format!("runtime source {} is stale", source.source_id),
                    true,
                ));
            }
        }
        drop(materialized);
        runtime_client_from_source(&self.config, source).map_err(|error| {
            CommandRouteError::with_code(REASON_SOURCE_UNAVAILABLE, error.to_string(), true)
        })
    }

    async fn resolve_command_owner_source(
        &self,
        command: &Command,
        payload: &crate::protocol::generated::goosetower::v1::command::Payload,
        explicit_source_id: Option<&str>,
    ) -> Result<String, CommandRouteError> {
        let owner_entity = command_owner_entity(command, payload);
        let materialized = self.materialized.read().await;
        let candidates = match owner_entity {
            Some((entity_kind, entity_id)) => materialized
                .iter()
                .filter(|(_, state)| state.ownership.owns(entity_kind, entity_id))
                .map(|(source_id, _)| source_id.clone())
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };

        if let Some(explicit_source_id) = explicit_source_id {
            let source_exists = materialized.contains_key(explicit_source_id);
            if !source_exists {
                return Err(CommandRouteError::with_code(
                    REASON_SOURCE_UNAVAILABLE,
                    format!("runtime source {explicit_source_id} is unavailable"),
                    true,
                ));
            }
            if !candidates.is_empty()
                && !candidates
                    .iter()
                    .any(|candidate| candidate == explicit_source_id)
            {
                return Err(CommandRouteError::with_code(
                    REASON_CROSS_SOURCE_UNSUPPORTED,
                    "command target is owned by a different runtime source",
                    false,
                ));
            }
            return Ok(explicit_source_id.to_string());
        }

        match candidates.as_slice() {
            [source_id] => Ok(source_id.clone()),
            [] => Err(CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                "target source ownership is unknown",
                true,
            )),
            _ => Err(CommandRouteError::with_code(
                REASON_CROSS_SOURCE_UNSUPPORTED,
                "command target is ambiguous across runtime sources",
                false,
            )),
        }
    }

    pub(crate) async fn merge_authoritative_session(
        &self,
        source_id: &str,
        session: runtime_core::SessionRecord,
    ) {
        if self.source_repair_active(source_id).await {
            return;
        }
        let patches = {
            let mut materialized = self.materialized.write().await;
            let Some(state) = materialized.get_mut(source_id) else {
                return;
            };
            if state.source_health.state == SourceHealthState::GapDetected {
                return;
            }
            if state
                .sessions
                .get(&session.id)
                .is_some_and(|current| current.updated_at >= session.updated_at)
            {
                return;
            }
            let session_id = session.id.clone();
            state.upsert_session(session);
            state.session_patches(&session_id, state.cursor())
        };
        for patch in patches {
            self.publish_materialized_patch(patch).await;
        }
    }

    pub(crate) async fn merge_authoritative_team(
        &self,
        source_id: &str,
        snapshot: runtime_core::TeamWithMembers,
    ) {
        if self.source_repair_active(source_id).await {
            return;
        }
        let patches = {
            let mut materialized = self.materialized.write().await;
            let Some(state) = materialized.get_mut(source_id) else {
                return;
            };
            if state.source_health.state == SourceHealthState::GapDetected {
                return;
            }
            let team_id = snapshot.team.id.clone();
            let current_revision = state.teams.get(&team_id).map(|current| current.updated_at);
            if current_revision.is_some_and(|revision| revision > snapshot.team.updated_at) {
                return;
            }
            let equal_revision = current_revision == Some(snapshot.team.updated_at);
            if equal_revision {
                return;
            }
            let previous_members = state
                .members_by_team
                .get(&team_id)
                .map(|rows| {
                    rows.keys()
                        .cloned()
                        .collect::<std::collections::BTreeSet<String>>()
                })
                .unwrap_or_default();
            state.upsert_team(snapshot.team);
            state
                .members_by_team
                .insert(team_id.clone(), BTreeMap::new());
            for member in snapshot.members {
                state.upsert_team_member(member);
            }
            let mut patches = state.team_patch(&team_id, state.cursor());
            let current_members = state
                .members_by_team
                .get(&team_id)
                .map(|members| {
                    members
                        .keys()
                        .cloned()
                        .collect::<std::collections::BTreeSet<_>>()
                })
                .unwrap_or_default();
            for agent_id in &current_members {
                patches.extend(state.session_patches(agent_id, state.cursor()));
            }
            let removed_members = previous_members
                .difference(&current_members)
                .cloned()
                .collect::<Vec<_>>();
            for agent_id in &removed_members {
                patches.extend(state.session_patches(agent_id, state.cursor()));
            }
            patches
        };
        for patch in patches {
            self.publish_materialized_patch(patch).await;
        }
    }

    pub(crate) async fn merge_authoritative_team_message(
        &self,
        source_id: &str,
        ack: runtime_core::TeamMessageAck,
    ) {
        if self.source_repair_active(source_id).await {
            return;
        }
        let patches = {
            let mut materialized = self.materialized.write().await;
            let Some(state) = materialized.get_mut(source_id) else {
                return;
            };
            if state.source_health.state == SourceHealthState::GapDetected {
                return;
            }
            let team_id = ack.message.team_id.clone();
            let message_id = ack.message.id.clone();
            let stale = state
                .messages_by_team
                .get(&team_id)
                .and_then(|rows| rows.iter().find(|row| row.id == message_id))
                .is_some_and(|current| current.created_at >= ack.message.created_at);
            if stale {
                return;
            }
            state.upsert_message(ack.message);
            for delivery in ack.deliveries {
                let stale = state
                    .deliveries_by_team
                    .get(&team_id)
                    .and_then(|rows| rows.iter().find(|row| row.id == delivery.id))
                    .is_some_and(|current| current.updated_at >= delivery.updated_at);
                if !stale {
                    state.upsert_delivery(delivery);
                }
            }
            state.team_patch(&team_id, state.cursor())
        };
        for patch in patches {
            self.publish_materialized_patch(patch).await;
        }
    }

    async fn team_sender_agent_id(&self, team_id: &str, fallback: &str) -> String {
        let materialized = self.materialized.read().await;
        materialized
            .values()
            .find_map(|state| state.teams.get(team_id))
            .map(|team| team.lead_agent_id.clone())
            .filter(|lead_agent_id| !lead_agent_id.trim().is_empty())
            .unwrap_or_else(|| fallback.to_string())
    }
}

fn command_payload_kind(command: &Command) -> &'static str {
    match command.payload.as_ref() {
        Some(crate::protocol::generated::goosetower::v1::command::Payload::SendTurn(_)) => {
            "send_turn"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::CreateSession(_)) => {
            "create_session"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::CreateTeam(_)) => {
            "create_team"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::JoinTeamMember(_)) => {
            "join_team_member"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::ResolveApproval(_)) => {
            "resolve_approval"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::InterruptTurn(_)) => {
            "interrupt_turn"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::SendTeamMessage(_)) => {
            "send_team_message"
        }
        Some(
            crate::protocol::generated::goosetower::v1::command::Payload::BroadcastTeamMessage(_),
        ) => "broadcast_team_message",
        Some(crate::protocol::generated::goosetower::v1::command::Payload::SpawnTeamMember(_)) => {
            "spawn_team_member"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::RetryDelivery(_)) => {
            "retry_delivery"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::CancelDelivery(_)) => {
            "cancel_delivery"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::KillProcess(_)) => {
            "kill_process"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::StartProcess(_)) => {
            "start_process"
        }
        None => "missing",
    }
}

fn command_target_labels(command: &Command) -> (String, String, String) {
    command
        .target
        .as_ref()
        .map(|target| {
            let scope = Scope::try_from(target.scope).unwrap_or(Scope::Unspecified);
            (
                format!("{scope:?}"),
                target.scope_id.clone(),
                target.entity_id.clone(),
            )
        })
        .unwrap_or_else(|| ("missing".to_string(), String::new(), String::new()))
}

fn command_explicit_source_id(command: &Command) -> Option<&str> {
    command
        .target
        .as_ref()
        .and_then(|target| target.entity_id.strip_prefix("source:"))
        .filter(|source_id| !source_id.is_empty())
}

fn command_owner_entity<'a>(
    command: &'a Command,
    payload: &'a crate::protocol::generated::goosetower::v1::command::Payload,
) -> Option<(&'static str, &'a str)> {
    match payload {
        crate::protocol::generated::goosetower::v1::command::Payload::CreateSession(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::CreateTeam(_) => None,
        crate::protocol::generated::goosetower::v1::command::Payload::SendTurn(input) => {
            (!input.session_id.is_empty()).then_some(("session", input.session_id.as_str()))
        }
        crate::protocol::generated::goosetower::v1::command::Payload::ResolveApproval(_) => command
            .target
            .as_ref()
            .map(|target| target.scope_id.as_str())
            .filter(|session_id| !session_id.is_empty())
            .map(|session_id| ("session", session_id)),
        crate::protocol::generated::goosetower::v1::command::Payload::InterruptTurn(input) => {
            (!input.session_id.is_empty()).then_some(("session", input.session_id.as_str()))
        }
        crate::protocol::generated::goosetower::v1::command::Payload::SendTeamMessage(input) => {
            (!input.team_id.is_empty()).then_some(("team", input.team_id.as_str()))
        }
        crate::protocol::generated::goosetower::v1::command::Payload::BroadcastTeamMessage(
            input,
        ) => (!input.team_id.is_empty()).then_some(("team", input.team_id.as_str())),
        crate::protocol::generated::goosetower::v1::command::Payload::JoinTeamMember(input) => {
            (!input.team_id.is_empty()).then_some(("team", input.team_id.as_str()))
        }
        crate::protocol::generated::goosetower::v1::command::Payload::SpawnTeamMember(input) => {
            (!input.team_id.is_empty()).then_some(("team", input.team_id.as_str()))
        }
        crate::protocol::generated::goosetower::v1::command::Payload::RetryDelivery(input) => {
            (!input.delivery_id.is_empty()).then_some(("team_delivery", input.delivery_id.as_str()))
        }
        crate::protocol::generated::goosetower::v1::command::Payload::CancelDelivery(_) => command
            .target
            .as_ref()
            .map(|target| target.scope_id.as_str())
            .filter(|team_id| !team_id.is_empty())
            .map(|team_id| ("team", team_id)),
        crate::protocol::generated::goosetower::v1::command::Payload::KillProcess(input) => {
            (!input.process_id.is_empty()).then_some(("process", input.process_id.as_str()))
        }
        crate::protocol::generated::goosetower::v1::command::Payload::StartProcess(_) => command
            .target
            .as_ref()
            .map(|target| target.scope_id.as_str())
            .filter(|session_id| !session_id.is_empty())
            .map(|session_id| ("session", session_id)),
    }
}

fn expected_scope_for_payload(
    payload: &crate::protocol::generated::goosetower::v1::command::Payload,
) -> Scope {
    match payload {
        crate::protocol::generated::goosetower::v1::command::Payload::CreateSession(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::CreateTeam(_) => {
            Scope::Source
        }
        crate::protocol::generated::goosetower::v1::command::Payload::SendTurn(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::ResolveApproval(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::InterruptTurn(_) => {
            Scope::Session
        }
        crate::protocol::generated::goosetower::v1::command::Payload::SendTeamMessage(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::BroadcastTeamMessage(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::JoinTeamMember(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::SpawnTeamMember(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::RetryDelivery(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::CancelDelivery(_) => {
            Scope::Team
        }
        crate::protocol::generated::goosetower::v1::command::Payload::KillProcess(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::StartProcess(_) => {
            Scope::Process
        }
    }
}

fn materialized_entity_kind_for_scope(scope: Scope) -> Option<&'static str> {
    match scope {
        Scope::Session => Some("session"),
        Scope::Team => Some("team"),
        Scope::Process => Some("process"),
        Scope::Worktree => Some("worktree"),
        Scope::Source => Some("source"),
        _ => None,
    }
}
