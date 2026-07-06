use std::collections::BTreeMap;

use runtime_core::{
    ApprovalRecord, ManagedWorktreeRecord, ProcessDetails, ProcessSummary, SessionRecord,
    TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord, TeamRecord,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::runtime::events::{SourceEvent, SourceEventLane, SourceHealthState};

use super::state::{
    EntityKey, EntityVersion, LedgerEventView, MaterializedPatch, MaterializedPatchKind,
    MaterializedState, ProcessOutputSampleView, SourceCursorView,
};

#[derive(Debug, Clone, PartialEq)]
pub struct PatchEffect {
    pub patches: Vec<MaterializedPatch>,
    pub duplicate: bool,
}

impl MaterializedState {
    pub fn reduce_source_event(&mut self, event: SourceEvent) -> PatchEffect {
        if !self.remember_source_event(&event) {
            return PatchEffect {
                patches: Vec::new(),
                duplicate: true,
            };
        }

        self.next_gateway_seq();
        let cursor = Some(SourceCursorView {
            source_id: event.source_id.clone(),
            source_epoch: event.source_epoch.clone(),
            source_seq: event.source_seq,
        });
        let ledger_event = ledger_event_from_source_event(&event);
        self.append_ledger_event(ledger_event.clone());

        let mut patches = vec![MaterializedPatch {
            kind: MaterializedPatchKind::ListInsert,
            view_kind: "ledger".to_string(),
            entity: Some(EntityKey::new(
                &self.source_id,
                "ledger_event",
                event.source_seq.to_string(),
            )),
            version: None,
            source_cursor: cursor.clone(),
            body: json!(ledger_event),
        }];

        match event.kind.as_str() {
            "session.created" | "session.resumed" | "session.closed" => {
                patches.extend(self.reduce_session_hint_or_record(&event, cursor.clone()));
            }
            "turn.started"
            | "turn.in_progress"
            | "turn.completed"
            | "turn.interrupted"
            | "turn.failed"
            | "provider.error"
            | "turn.interrupt_requested" => {
                patches.extend(self.reduce_turn_event(&event, cursor.clone()));
            }
            "approval.requested" | "approval.resolved" => {
                patches.extend(self.reduce_approval_event(&event, cursor.clone()));
            }
            "team.created" | "team.lead_changed" | "team.deleted" => {
                patches.extend(self.reduce_team_event(&event, cursor.clone()));
            }
            "team.member_joined" | "team.member_left" => {
                patches.extend(self.reduce_team_member_event(&event, cursor.clone()));
            }
            "team_message.created" | "team_message.completed" => {
                patches.extend(self.reduce_team_message_event(&event, cursor.clone()));
            }
            "team_delivery.pending"
            | "team_delivery.deferred"
            | "team_delivery.injecting"
            | "team_delivery.injected"
            | "team_delivery.failed"
            | "team_delivery.cancelled"
            | "team_delivery.updated" => {
                patches.extend(self.reduce_team_delivery_event(&event, cursor.clone()));
            }
            "process.started"
            | "process.completed"
            | "process.timed_out"
            | "process.killed"
            | "process.failed"
            | "process.kill_requested" => {
                patches.extend(self.reduce_process_event(&event, cursor.clone()));
            }
            "process.output" => {
                patches.extend(self.reduce_process_output_event(&event, cursor.clone()));
            }
            "worktree.created" | "worktree.claimed" | "worktree.released" | "worktree.cleaned" => {
                patches.extend(self.reduce_worktree_event(&event, cursor.clone()));
            }
            _ => {}
        }

        if self.source_health.state != SourceHealthState::Live {
            patches.push(self.transition_source_health(SourceHealthState::Live, None));
        }

        PatchEffect {
            patches,
            duplicate: false,
        }
    }

    fn reduce_session_hint_or_record(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let Some(session_id) = event
            .session_id
            .as_deref()
            .or(Some(event.scope_id.as_str()))
        else {
            return Vec::new();
        };
        let mut patches = Vec::new();
        if let Some(session) = event_payload_record::<SessionRecord>(event, "session") {
            self.upsert_session(session);
        } else if let Some(session) = self.sessions.get(session_id).cloned() {
            let mut updated = session;
            match event.kind.as_str() {
                "session.closed" => {
                    updated.status = "closed".to_string();
                    updated.active_turn_id = None;
                    updated.closed_at = Some(event.created_at);
                }
                "session.resumed" => {
                    if updated.status != "closed" {
                        updated.status = "ready".to_string();
                    }
                }
                _ => {}
            }
            updated.updated_at = event.created_at.max(updated.updated_at);
            self.upsert_session(updated);
        } else if event.kind == "session.created" {
            self.upsert_session(session_record_from_created_event(event, session_id));
        }
        patches.extend(self.session_patches(session_id, cursor));
        patches
    }

    fn reduce_turn_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let Some(session_id) = event.session_id.as_deref() else {
            return Vec::new();
        };
        let Some(mut session) = self.sessions.get(session_id).cloned() else {
            return Vec::new();
        };
        match event.kind.as_str() {
            "turn.started" | "turn.in_progress" => {
                session.status = "turn_running".to_string();
                session.active_turn_id = event.turn_id.clone();
            }
            "turn.completed" | "turn.interrupted" => {
                if session.active_turn_id == event.turn_id {
                    session.active_turn_id = None;
                }
                if !matches!(session.status.as_str(), "closed" | "failed") {
                    session.status = "ready".to_string();
                }
            }
            "turn.failed" | "provider.error" => {
                if session.active_turn_id == event.turn_id {
                    session.active_turn_id = None;
                }
                session.status = "failed".to_string();
                session.failure_message = event
                    .payload
                    .pointer("/runtime_event/error")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        event
                            .payload
                            .pointer("/runtime_event/error/message")
                            .and_then(Value::as_str)
                    })
                    .map(str::to_string);
            }
            "turn.interrupt_requested" => {
                session.status = "interrupt_requested".to_string();
            }
            _ => {}
        }
        session.updated_at = event.created_at.max(session.updated_at);
        self.upsert_session(session);

        let mut patches = self.session_patches(session_id, cursor.clone());
        if let Some(text) = assistant_text(event) {
            self.append_text(session_id, text.clone());
            patches.push(MaterializedPatch {
                kind: MaterializedPatchKind::TextAppend,
                view_kind: "session_detail".to_string(),
                entity: Some(EntityKey::new(&self.source_id, "session", session_id)),
                version: Some(self.version("session", session_id)),
                source_cursor: cursor,
                body: json!({
                    "session_id": session_id,
                    "turn_id": event.turn_id,
                    "text": text,
                }),
            });
        }
        patches
    }

    fn reduce_approval_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let approval = event_payload_record::<ApprovalRecord>(event, "approval");
        let approval_id = approval
            .as_ref()
            .map(|approval| approval.id.clone())
            .or_else(|| {
                event
                    .payload
                    .pointer("/runtime_event/approval_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });

        let Some(approval_id) = approval_id else {
            return Vec::new();
        };

        if let Some(approval) = approval {
            self.upsert_approval(approval);
        } else if let Some(existing) = self.approvals.get(&approval_id).cloned() {
            let mut updated = existing;
            if event.kind == "approval.resolved" {
                updated.status = event
                    .payload
                    .pointer("/runtime_event/status")
                    .and_then(Value::as_str)
                    .unwrap_or("resolved")
                    .to_string();
                updated.resolved_at = Some(event.created_at);
            }
            self.upsert_approval(updated);
        } else if event.kind == "approval.requested" {
            let Some(session_id) = event.session_id.clone() else {
                return Vec::new();
            };
            let Some(turn_id) = event.turn_id.clone() else {
                return Vec::new();
            };
            self.upsert_approval(ApprovalRecord {
                id: approval_id.clone(),
                session_id,
                turn_id,
                tool_call_id: None,
                provider_approval_ref: Some(approval_id.clone()),
                status: "pending".to_string(),
                request: json!({ "source": "runtime_event_hint" }),
                response: None,
                created_at: event.created_at,
                resolved_at: None,
            });
        }

        let mut patches = vec![self.entity_upsert_patch(
            "approval_inbox",
            "approval",
            approval_id.clone(),
            self.version("approval", &approval_id),
            cursor.clone(),
            json!(self.approval_row(&approval_id)),
        )];
        if let Some(session_id) = event.session_id.as_deref() {
            patches.extend(self.session_patches(session_id, cursor));
        }
        patches
    }

    fn reduce_team_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let team = event_payload_record::<TeamRecord>(event, "team");
        let team_id = team
            .as_ref()
            .map(|team| team.id.clone())
            .or_else(|| event.team_id.clone())
            .unwrap_or_else(|| event.scope_id.clone());
        if event.kind == "team.deleted" {
            self.teams.remove(&team_id);
            self.members_by_team.remove(&team_id);
            self.messages_by_team.remove(&team_id);
            self.deliveries_by_team.remove(&team_id);
            self.remove_version("team", &team_id);
            return vec![self.entity_remove_patch("team_workspace", "team", team_id, cursor)];
        }
        if let Some(team) = team {
            self.upsert_team(team);
        }
        self.team_patch(&team_id, cursor)
    }

    fn reduce_team_member_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let team_id = event.team_id.as_deref().unwrap_or(event.scope_id.as_str());
        let mut patches = Vec::new();
        if event.kind == "team.member_left" {
            if let Some(agent_id) = event
                .payload
                .pointer("/runtime_event/agent_id")
                .and_then(Value::as_str)
            {
                self.remove_team_member(team_id, agent_id);
                patches.push(self.list_remove_patch(
                    "team_workspace",
                    "team_member",
                    format!("{team_id}:{agent_id}"),
                    cursor.clone(),
                ));
                patches.extend(self.session_patches(agent_id, cursor.clone()));
            }
        } else if let Some(member) = event_payload_record::<TeamMemberRecord>(event, "member") {
            let session_id = member.agent_id.clone();
            self.upsert_team_member(member.clone());
            patches.push(self.list_insert_patch(
                "team_workspace",
                "team_member",
                format!("{}:{}", member.team_id, member.agent_id),
                cursor.clone(),
                json!(member),
            ));
            patches.extend(self.session_patches(&session_id, cursor.clone()));
        }
        patches.extend(self.team_patch(team_id, cursor));
        patches
    }

    fn reduce_team_message_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let message = event_payload_record::<TeamMessageRecord>(event, "message");
        let Some(message) = message else {
            return Vec::new();
        };
        let team_id = message.team_id.clone();
        let message_id = message.id.clone();
        self.upsert_message(message.clone());
        let mut patches = vec![self.list_insert_patch(
            "team_workspace",
            "team_message",
            message_id,
            cursor.clone(),
            json!(message),
        )];
        patches.extend(self.team_patch(&team_id, cursor));
        patches
    }

    fn reduce_team_delivery_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let delivery = event_payload_record::<TeamDeliveryRecord>(event, "delivery");
        let Some(delivery) = delivery else {
            return Vec::new();
        };
        let team_id = delivery.team_id.clone();
        let recipient = delivery.recipient_agent_id.clone();
        let delivery_id = delivery.id.clone();
        self.upsert_delivery(delivery.clone());
        let mut patches = vec![self.entity_upsert_patch(
            "team_workspace",
            "team_delivery",
            delivery_id,
            self.version("team_delivery", &delivery.id),
            cursor.clone(),
            json!(delivery),
        )];
        patches.extend(self.team_patch(&team_id, cursor.clone()));
        patches.extend(self.session_patches(&recipient, cursor));
        patches
    }

    fn reduce_process_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let process_id = event
            .payload
            .pointer("/runtime_event/process_id")
            .and_then(Value::as_str)
            .unwrap_or(event.scope_id.as_str())
            .to_string();

        if let Some(details) = event_payload_record::<ProcessDetails>(event, "process_details") {
            self.upsert_process_details(details);
        } else if let Some(summary) = event_payload_record::<ProcessSummary>(event, "process") {
            self.upsert_process_summary(summary);
        } else if let Some(mut process) = self.processes.get(&process_id).cloned() {
            process.status = match event.kind.as_str() {
                "process.started" => "running".to_string(),
                "process.completed" => "completed".to_string(),
                "process.timed_out" => "timed_out".to_string(),
                "process.killed" => "killed".to_string(),
                "process.kill_requested" => "kill_requested".to_string(),
                "process.failed" => "failed".to_string(),
                _ => process.status,
            };
            process.ended_at = if matches!(
                process.status.as_str(),
                "completed" | "timed_out" | "killed" | "failed"
            ) {
                Some(event.created_at)
            } else {
                process.ended_at
            };
            process.exit_code = event
                .payload
                .pointer("/runtime_event/exit_code")
                .and_then(Value::as_i64)
                .or(process.exit_code);
            process.signal = event
                .payload
                .pointer("/runtime_event/signal")
                .and_then(Value::as_i64)
                .or(process.signal);
            self.processes.insert(process_id.clone(), process);
            let version = self.bump_version("process", &process_id);
            if let Some(process) = self.processes.get_mut(&process_id) {
                process.version = version;
            }
        } else if event.kind == "process.started" {
            let summary = ProcessSummary {
                process_id: process_id.clone(),
                session_id: event.session_id.clone(),
                pid: event
                    .payload
                    .pointer("/runtime_event/pid")
                    .and_then(Value::as_i64),
                status: "running".to_string(),
                command: event
                    .payload
                    .pointer("/runtime_event/command")
                    .cloned()
                    .unwrap_or(Value::Null),
                cwd: event
                    .payload
                    .pointer("/runtime_event/cwd")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                started_at: event.created_at,
                ended_at: None,
            };
            self.upsert_process_summary(summary);
        }

        let mut patches = vec![self.entity_upsert_patch(
            "process_tail",
            "process",
            process_id.clone(),
            self.version("process", &process_id),
            cursor.clone(),
            json!(self.processes.get(&process_id)),
        )];
        if let Some(session_id) = event.session_id.as_deref() {
            patches.extend(self.session_patches(session_id, cursor));
        }
        patches
    }

    fn reduce_process_output_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let process_id = event
            .payload
            .pointer("/runtime_event/process_id")
            .and_then(Value::as_str)
            .unwrap_or(event.scope_id.as_str());
        let stream = event
            .payload
            .pointer("/runtime_event/stream")
            .and_then(Value::as_str)
            .unwrap_or("stdout")
            .to_string();
        let sample = ProcessOutputSampleView {
            source_seq: event.source_seq,
            stream,
            bytes_seen: event
                .payload
                .pointer("/runtime_event/bytes_seen")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize,
            bytes_written: event
                .payload
                .pointer("/runtime_event/bytes_written")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize,
            truncated: event
                .payload
                .pointer("/runtime_event/truncated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            created_at: event.created_at,
        };
        self.append_log_sample(process_id, sample.clone());
        vec![MaterializedPatch {
            kind: MaterializedPatchKind::LogSample,
            view_kind: "process_tail".to_string(),
            entity: Some(EntityKey::new(&self.source_id, "process", process_id)),
            version: Some(self.version("process", process_id)),
            source_cursor: cursor,
            body: json!(sample),
        }]
    }

    fn reduce_worktree_event(
        &mut self,
        event: &SourceEvent,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let worktree = event_payload_record::<ManagedWorktreeRecord>(event, "worktree");
        let worktree_id = worktree
            .as_ref()
            .map(|worktree| worktree.id.clone())
            .unwrap_or_else(|| event.scope_id.clone());
        if let Some(worktree) = worktree {
            self.upsert_worktree(worktree);
        }
        vec![self.entity_upsert_patch(
            "worktree",
            "worktree",
            worktree_id.clone(),
            self.version("worktree", &worktree_id),
            cursor,
            json!(self.worktree_view(&worktree_id)),
        )]
    }

    fn session_patches(
        &mut self,
        session_id: &str,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let mut patches = Vec::new();
        if let Some(row) = self.agent_row(session_id) {
            patches.push(self.entity_upsert_patch(
                "fleet_board",
                "session",
                session_id.to_string(),
                row.version,
                cursor.clone(),
                json!(row),
            ));
        }
        if let Some(session) = self.sessions.get(session_id) {
            patches.push(self.entity_upsert_patch(
                "session_detail",
                "session",
                session_id.to_string(),
                self.version("session", session_id),
                cursor,
                json!({
                    "session": session,
                    "row": self.agent_row(session_id),
                }),
            ));
        }
        patches
    }

    fn team_patch(
        &mut self,
        team_id: &str,
        cursor: Option<SourceCursorView>,
    ) -> Vec<MaterializedPatch> {
        let Some(team) = self.teams.get(team_id) else {
            return Vec::new();
        };
        vec![self.entity_upsert_patch(
            "team_workspace",
            "team",
            team_id.to_string(),
            self.version("team", team_id),
            cursor,
            json!({
                "team": team,
                "members": self.members_by_team.get(team_id),
                "messages": self.messages_by_team.get(team_id),
                "deliveries": self.deliveries_by_team.get(team_id),
            }),
        )]
    }

    fn approval_row(&self, approval_id: &str) -> Option<super::state::ApprovalRowView> {
        let approval = self.approvals.get(approval_id)?;
        Some(super::snapshots::approval_row_from_record(self, approval))
    }

    fn entity_upsert_patch(
        &self,
        view_kind: &str,
        entity_kind: &str,
        entity_id: String,
        version: EntityVersion,
        cursor: Option<SourceCursorView>,
        body: Value,
    ) -> MaterializedPatch {
        MaterializedPatch {
            kind: MaterializedPatchKind::EntityUpsert,
            view_kind: view_kind.to_string(),
            entity: Some(EntityKey::new(&self.source_id, entity_kind, entity_id)),
            version: Some(version),
            source_cursor: cursor,
            body,
        }
    }

    fn entity_remove_patch(
        &self,
        view_kind: &str,
        entity_kind: &str,
        entity_id: String,
        cursor: Option<SourceCursorView>,
    ) -> MaterializedPatch {
        MaterializedPatch {
            kind: MaterializedPatchKind::EntityRemove,
            view_kind: view_kind.to_string(),
            entity: Some(EntityKey::new(&self.source_id, entity_kind, entity_id)),
            version: None,
            source_cursor: cursor,
            body: Value::Null,
        }
    }

    fn list_insert_patch(
        &self,
        view_kind: &str,
        entity_kind: &str,
        entity_id: String,
        cursor: Option<SourceCursorView>,
        body: Value,
    ) -> MaterializedPatch {
        MaterializedPatch {
            kind: MaterializedPatchKind::ListInsert,
            view_kind: view_kind.to_string(),
            entity: Some(EntityKey::new(&self.source_id, entity_kind, entity_id)),
            version: None,
            source_cursor: cursor,
            body,
        }
    }

    fn list_remove_patch(
        &self,
        view_kind: &str,
        entity_kind: &str,
        entity_id: String,
        cursor: Option<SourceCursorView>,
    ) -> MaterializedPatch {
        MaterializedPatch {
            kind: MaterializedPatchKind::ListRemove,
            view_kind: view_kind.to_string(),
            entity: Some(EntityKey::new(&self.source_id, entity_kind, entity_id)),
            version: None,
            source_cursor: cursor,
            body: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CoalescingPatchBuffer {
    state_patches: BTreeMap<(String, String, String), MaterializedPatch>,
    ordered: Vec<MaterializedPatch>,
}

impl CoalescingPatchBuffer {
    pub fn push(&mut self, patch: MaterializedPatch) {
        let coalescable = matches!(
            patch.kind,
            MaterializedPatchKind::EntityUpsert | MaterializedPatchKind::SourceHealthTransition
        );
        if coalescable {
            if let Some(entity) = patch.entity.as_ref() {
                self.state_patches.insert(
                    (
                        patch.view_kind.clone(),
                        entity.entity_kind.clone(),
                        entity.entity_id.clone(),
                    ),
                    patch,
                );
                return;
            }
        }
        self.ordered.push(patch);
    }

    pub fn extend(&mut self, patches: impl IntoIterator<Item = MaterializedPatch>) {
        for patch in patches {
            self.push(patch);
        }
    }

    pub fn drain(mut self) -> Vec<MaterializedPatch> {
        self.ordered.extend(self.state_patches.into_values());
        self.ordered
    }
}

pub fn ledger_event_from_source_event(event: &SourceEvent) -> LedgerEventView {
    LedgerEventView {
        source_id: event.source_id.clone(),
        source_epoch: event.source_epoch.clone(),
        source_seq: event.source_seq,
        upstream_row_id: event.upstream_row_id,
        upstream_scoped_seq: event.upstream_scoped_seq,
        scope: event.scope,
        scope_id: event.scope_id.clone(),
        session_id: event.session_id.clone(),
        team_id: event.team_id.clone(),
        turn_id: event.turn_id.clone(),
        kind: event.kind.clone(),
        criticality: format!("{:?}", event.criticality).to_ascii_lowercase(),
        lane: match event.lane {
            SourceEventLane::Critical => "critical",
            SourceEventLane::State => "state",
            SourceEventLane::Tokens => "tokens",
            SourceEventLane::Bulk => "bulk",
        }
        .to_string(),
        payload: event.payload.clone(),
        created_at: event.created_at,
    }
}

fn event_payload_record<T: DeserializeOwned>(event: &SourceEvent, field: &str) -> Option<T> {
    event
        .payload
        .pointer(&format!("/runtime_event/{field}"))
        .cloned()
        .or_else(|| event.payload.get(field).cloned())
        .and_then(|value| serde_json::from_value(value).ok())
}

fn session_record_from_created_event(event: &SourceEvent, session_id: &str) -> SessionRecord {
    let provider = event
        .payload
        .pointer("/runtime_event/provider")
        .and_then(Value::as_str)
        .or_else(|| event.payload.pointer("/provider").and_then(Value::as_str))
        .unwrap_or("unknown")
        .to_string();
    let model = event
        .payload
        .pointer("/runtime_event/model")
        .and_then(Value::as_str)
        .or_else(|| event.payload.pointer("/model").and_then(Value::as_str))
        .map(str::to_string);

    SessionRecord {
        id: session_id.to_string(),
        provider,
        status: "ready".to_string(),
        cwd: None,
        model,
        permission_mode: None,
        system_prompt: None,
        metadata: Value::Object(Default::default()),
        provider_session_ref: None,
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: event.created_at,
        updated_at: event.created_at,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

fn assistant_text(event: &SourceEvent) -> Option<String> {
    event
        .payload
        .pointer("/runtime_event/assistant_text")
        .and_then(Value::as_str)
        .or_else(|| {
            event
                .payload
                .pointer("/runtime_event/text")
                .and_then(Value::as_str)
        })
        .map(str::to_string)
}
