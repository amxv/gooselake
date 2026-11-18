//! Disposable database history for legacy inspection/settlement tests only.
//! No provider, Git, hook, team-service or runtime mutation is dispatched here.
use runtime_core::{
    normalized_json_hash, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, RuntimeError,
    RuntimeStore, SessionRecord, TeamDeliveryRecord, TeamMemberRecord, TeamMemberSpawnAdmission,
    TeamMemberSpawnRequest, TeamMemberSpawnResponse, TeamMessageRecord, TeamOperationJournalRecord,
    TeamWithMembers,
};
use serde_json::json;

#[derive(Clone)]
pub(crate) enum HistoricalWorktreeAssignment {
    Created(ManagedWorktreeRecord),
    Reused(ManagedWorktreeRecord),
}

pub(crate) struct HistoricalSpawnSeed {
    pub request: TeamMemberSpawnRequest,
    pub source: SessionRecord,
    pub response: TeamMemberSpawnResponse,
    pub message: TeamMessageRecord,
    pub delivery: TeamDeliveryRecord,
    pub claim: Option<ManagedWorktreeClaimRecord>,
    construction_fingerprint: String,
}

impl HistoricalSpawnSeed {
    /// Callers prepare any real disposable checkout separately and pass its
    /// original record. Construction does not persist or certify anything.
    pub(crate) fn for_request(
        request: TeamMemberSpawnRequest,
        source: SessionRecord,
        mut team: TeamWithMembers,
        operation_id: &str,
        child_id: &str,
        assignment: Option<HistoricalWorktreeAssignment>,
        historical_at: i64,
    ) -> Result<Self, RuntimeError> {
        let valid_id = |value: &str| {
            !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control)
        };
        let mut original_members = std::collections::BTreeSet::new();
        if ![
            operation_id,
            child_id,
            request.source_session_id.as_str(),
            source.id.as_str(),
            request.team_id.as_str(),
            team.team.id.as_str(),
        ]
        .into_iter()
        .all(valid_id)
            || team.members.iter().any(|member| {
                !valid_id(&member.team_id)
                    || !valid_id(&member.agent_id)
                    || member.team_id != request.team_id
                    || !original_members.insert(member.agent_id.as_str())
            })
        {
            return Err(RuntimeError::InvalidState(
                "invalid original historical coordinates or member snapshot".into(),
            ));
        }
        let key = request.idempotency_key.as_deref().ok_or_else(|| {
            RuntimeError::InvalidState(
                "historical certified fixture requires an explicit original key".into(),
            )
        })?;
        if key.is_empty()
            || key.len() > 512
            || key.trim() != key
            || key.chars().any(char::is_control)
            || request.source_session_id != source.id
            || request.team_id != team.team.id
            || source.id == child_id
            || team.members.iter().any(|m| m.agent_id == child_id)
        {
            return Err(RuntimeError::InvalidState(
                "historical spawn fixture coordinates disagree".into(),
            ));
        }
        let requested_mode = request.worktree.as_ref().map(|w| {
            w.mode
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .unwrap_or("create")
                .to_ascii_lowercase()
        });
        let assignment_matches = match (&assignment, requested_mode.as_deref()) {
            (None, None) => true,
            (Some(HistoricalWorktreeAssignment::Created(_)), Some("create")) => true,
            (Some(HistoricalWorktreeAssignment::Reused(_)), Some("reuse" | "use_existing")) => true,
            _ => false,
        };
        if !assignment_matches {
            return Err(RuntimeError::InvalidState(
                "historical assignment contradicts original request mode".into(),
            ));
        }
        let (worktree, mode, created) = match assignment {
            None => (None, "none", false),
            Some(HistoricalWorktreeAssignment::Created(record)) => {
                if record.created_by_operation_id.as_deref() != Some(operation_id) {
                    return Err(RuntimeError::InvalidState(
                        "created fixture must retain original spawn provenance".into(),
                    ));
                }
                (Some(record), "created", true)
            }
            Some(HistoricalWorktreeAssignment::Reused(record)) => (Some(record), "reused", false),
        };
        if let Some(worktree) = &worktree {
            let name = request
                .worktree
                .as_ref()
                .and_then(|w| w.name.as_deref())
                .map(str::trim);
            if name != Some(worktree.worktree_name.as_str()) || name == Some("") {
                return Err(RuntimeError::InvalidState(
                    "historical checkout name contradicts original request".into(),
                ));
            }
        }
        let worktree_id = worktree.as_ref().map(|w| w.id.clone());
        let mut child = source.clone();
        child.id = child_id.into();
        child.provider = request
            .provider
            .clone()
            .unwrap_or_else(|| source.provider.clone());
        child.status = "ready".into();
        child.cwd = worktree
            .as_ref()
            .map(|w| w.worktree_cwd.clone())
            .or_else(|| source.cwd.clone());
        child.model = request.model.clone().or_else(|| source.model.clone());
        child.permission_mode = request
            .permission_mode
            .clone()
            .or_else(|| source.permission_mode.clone())
            .or_else(|| {
                (child.provider == "codex" && worktree.is_some()).then(|| "full_auto".into())
            });
        child.metadata = request.metadata.clone().unwrap_or_else(|| json!({}));
        child.provider_session_ref = Some(format!("historical-native:{child_id}"));
        child.canonical_provider_session_ref = None;
        child.active_turn_id = None;
        child.worktree_id = worktree_id.clone();
        child.created_at = historical_at;
        child.updated_at = historical_at;
        child.closed_at = None;
        child.failure_code = None;
        child.failure_message = None;
        let member = TeamMemberRecord {
            team_id: request.team_id.clone(),
            agent_id: child_id.into(),
            title: request.title.clone(),
            joined_at: historical_at,
            added_by: source.id.clone(),
            creator_agent_id: request.creator_agent_id.clone(),
            creator_compaction_subscription: request
                .creator_compaction_subscription
                .clone()
                .unwrap_or_else(|| "auto".into()),
            worktree_id: worktree_id.clone(),
        };
        team.members.push(member.clone());
        let message_id = format!("historical-message:{operation_id}");
        let delivery_id = format!("historical-delivery:{operation_id}");
        let correlation = format!("spawn-onboarding:{operation_id}");
        let message = TeamMessageRecord {
            id: message_id.clone(),
            team_id: request.team_id.clone(),
            scope: "direct".into(),
            sender_agent_id: source.id.clone(),
            recipient_agent_ids: json!([child_id]),
            input: json!({"text":request.prompt.clone().unwrap_or_else(||"historical onboarding".into())}),
            image_paths: json!([]),
            priority: "normal".into(),
            policy: "start_new_turn_only".into(),
            correlation_id: Some(correlation.clone()),
            reply_to_message_id: None,
            idempotency_key: Some(correlation),
            created_at: historical_at,
        };
        let delivery = TeamDeliveryRecord {
            id: delivery_id.clone(),
            message_id: message_id.clone(),
            team_id: request.team_id.clone(),
            recipient_agent_id: child_id.into(),
            provider: child.provider.clone(),
            status: "injected".into(),
            effective_policy: Some("start_new_turn_only".into()),
            injection_strategy: Some("historical-import".into()),
            injected_turn_id: None,
            last_error_code: None,
            last_error_message: None,
            created_at: historical_at,
            updated_at: historical_at,
        };
        let claim = worktree_id.map(|worktree_id| ManagedWorktreeClaimRecord {
            claim_generation: Some(1),
            worktree_id,
            session_id: child_id.into(),
            claim_role: if created { "owner" } else { "consumer" }.into(),
            created_at: historical_at,
            released_at: None,
        });
        let response = TeamMemberSpawnResponse {
            operation_id: operation_id.into(),
            team,
            spawned_session: child,
            spawned_member: member,
            worktree,
            worktree_assignment_mode: mode.into(),
            worktree_created_by_operation: created,
            onboarding: json!({"status":"sent","message_id":message_id,"delivery_ids":[delivery_id]}),
            journal_stage: "completed".into(),
        };
        let mut seed = Self {
            request,
            source,
            response,
            message,
            delivery,
            claim,
            construction_fingerprint: String::new(),
        };
        seed.construction_fingerprint = seed.graph_fingerprint()?;
        Ok(seed)
    }

    fn graph_fingerprint(&self) -> Result<String, RuntimeError> {
        normalized_json_hash(&json!({
            "request": self.request, "source": self.source, "response": self.response,
            "message": self.message, "delivery": self.delivery, "claim": self.claim,
        }))
    }

    /// Seed only through the historical onboarding stage. Tests can inject
    /// faults or corrupt one lineage row before the FIRST real completion.
    pub(crate) fn seed_to_onboarding_sent(
        &self,
        store: &dyn RuntimeStore,
        admitted_at: i64,
        onboarding_at: i64,
    ) -> Result<(), RuntimeError> {
        // Public fields permit explicit negative completion candidates, but
        // setup must never import a graph changed after validated construction.
        if self.graph_fingerprint()? != self.construction_fingerprint {
            return Err(RuntimeError::Conflict(
                "historical seed graph changed before persistence".into(),
            ));
        }
        // Only sequential disposable fixture setup calls this helper. Preserve
        // preexisting certified rows exactly; raw import APIs cannot rewrite them.
        let state = store.hydrate_runtime_state()?;
        let conflict = || {
            RuntimeError::Conflict(
                "historical seed collides with existing fixture authority".into(),
            )
        };
        // The exact inspection API also sees admissions whose journal is lost.
        // A key-only lookup cannot prove this proposed operation ID is unused.
        if store
            .inspect_legacy_team_operation(&self.response.operation_id, None, 1)?
            .is_some()
        {
            return Err(conflict());
        }
        let source = state.sessions.iter().find(|s| s.id == self.source.id);
        let team = state
            .teams
            .iter()
            .find(|t| t.id == self.response.team.team.id);
        if source.is_some_and(|s| s != &self.source)
            || team.is_some_and(|t| t != &self.response.team.team)
            || state
                .sessions
                .iter()
                .any(|s| s.id == self.response.spawned_session.id)
            || state
                .team_members
                .iter()
                .any(|m| m.agent_id == self.response.spawned_session.id)
            || state
                .managed_worktree_claims
                .iter()
                .any(|c| c.session_id == self.response.spawned_session.id)
            || state
                .team_operation_journal
                .iter()
                .any(|o| o.operation_id == self.response.operation_id)
            || admitted_at > onboarding_at
        {
            return Err(conflict());
        }
        for old_member in state
            .team_members
            .iter()
            .filter(|m| m.team_id == self.request.team_id)
        {
            if old_member.agent_id == self.response.spawned_session.id
                || !self.response.team.members.contains(old_member)
            {
                return Err(conflict());
            }
        }
        for member in &self.response.team.members {
            if state.team_members.iter().any(|old| {
                old.team_id == member.team_id && old.agent_id == member.agent_id && old != member
            }) {
                return Err(conflict());
            }
        }
        let existing_worktree = if let Some(worktree) = &self.response.worktree {
            if state.managed_worktrees.iter().any(|old| {
                (old.id == worktree.id && old != worktree)
                    || (old.repo_root == worktree.repo_root
                        && old.worktree_cwd == worktree.worktree_cwd
                        && old.branch_name == worktree.branch_name
                        && old.id != worktree.id)
            }) {
                return Err(conflict());
            }
            state.managed_worktrees.iter().any(|old| old == worktree)
        } else {
            false
        };
        if self.claim.as_ref().is_some_and(|claim| {
            state.managed_worktree_claims.iter().any(|old| {
                old.worktree_id == claim.worktree_id && old.session_id == claim.session_id
            })
        }) || state.team_messages.iter().any(|old| {
            old.id == self.message.id
                || (old.team_id == self.message.team_id
                    && old.sender_agent_id == self.message.sender_agent_id
                    && old.scope == self.message.scope
                    && old.idempotency_key == self.message.idempotency_key)
        }) || state.agent_messages.iter().any(|old| {
            old.id == self.message.id || old.idempotency_key == self.message.idempotency_key
        }) || state
            .team_deliveries
            .iter()
            .any(|old| old.id == self.delivery.id)
            || state
                .agent_deliveries
                .iter()
                .any(|old| old.id == self.delivery.id)
        {
            return Err(conflict());
        }
        let hash = normalized_json_hash(&serde_json::to_value(&self.request).map_err(|e| {
            RuntimeError::InvalidState(format!("historical request encoding: {e}"))
        })?)?;
        let key = self
            .request
            .idempotency_key
            .as_deref()
            .ok_or_else(conflict)?;
        if store
            .replay_team_member_spawn(
                &self.request.team_id,
                &self.request.source_session_id,
                key,
                &hash,
            )?
            .is_some()
        {
            return Err(conflict());
        }
        if source.is_none() {
            store.upsert_session(&self.source)?;
        }
        store.upsert_session(&self.response.spawned_session)?;
        if team.is_none() {
            store.upsert_team(&self.response.team.team)?;
        }
        for member in &self.response.team.members {
            if !state.team_members.contains(member) {
                store.upsert_team_member(member)?;
            }
        }
        // Prepare claims before admitting the legacy obligation. There is no
        // exemption from the forthcoming unresolved-history claim fence.
        if !existing_worktree {
            if let Some(worktree) = &self.response.worktree {
                store.upsert_managed_worktree(worktree)?;
            }
        }
        if let Some(claim) = &self.claim {
            store.upsert_managed_worktree_claim(claim)?;
        }
        store.upsert_team_message(&self.message)?;
        store.upsert_team_delivery(&self.delivery)?;
        match store.admit_team_member_spawn(
            &self.response.operation_id,
            &self.request,
            &hash,
            admitted_at,
        )? {
            TeamMemberSpawnAdmission::Execute { operation_id }
                if operation_id == self.response.operation_id => {}
            _ => {
                return Err(RuntimeError::Conflict(
                    "historical seed must own a fresh disposable admission".into(),
                ))
            }
        }
        store.upsert_team_operation_journal(&TeamOperationJournalRecord {
            operation_id:self.response.operation_id.clone(),team_id:self.request.team_id.clone(),
            kind:"spawn_member_with_worktree".into(),stage:"onboarding_sent".into(),
            payload:json!({
                "source_session_id":self.request.source_session_id,
                "spawned_session_id":self.response.spawned_session.id,
                "provider":self.response.spawned_session.provider,
                "provider_session_ref":self.response.spawned_session.provider_session_ref,
                "canonical_provider_session_ref":self.response.spawned_session.canonical_provider_session_ref,
                "worktree_id":self.response.worktree.as_ref().map(|w|&w.id),
                "message_id":self.message.id,
                "delivery_ids":[self.delivery.id],
            }),created_at:admitted_at,updated_at:onboarding_at,
        })
    }

    /// Settlement uses the production lineage validator and atomic response
    /// transaction. Never write completed_response_json directly in setup.
    pub(crate) fn complete_seed(
        &self,
        store: &dyn RuntimeStore,
        completed_at: i64,
    ) -> Result<(), RuntimeError> {
        store.complete_team_member_spawn(&self.response.operation_id, &self.response, completed_at)
    }
}
