use std::collections::BTreeMap;

use runtime_core::{
    ApprovalRecord, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, SessionRecord,
    TeamMemberRecord, TeamRecord,
};
use serde_json::json;

use crate::config::RuntimeSourceConfig;
use crate::materializer::state::ModelCapabilityView;
use crate::materializer::{
    snapshot_cross_source_board, ApprovalInboxSubscription, BoardSubscription,
    CoalescingPatchBuffer, LedgerSubscription, MaterializedPatchKind, MaterializedState,
    SelectedSessionSubscription, SelectedTeamSubscription, SessionDetailView, TeamWorkspaceView,
};
use crate::runtime::{events::SourceEvent, SourceHealthState};

#[test]
fn materializer_reduces_session_approval_team_and_process_events() {
    let mut state = seeded_state();

    let approval_event = source_event(
        10,
        "approval.requested",
        RuntimeEventScope::Session,
        "sess_1",
        json!({
            "approval": approval_record("apr_1", "pending"),
        }),
    );
    let effect = state.reduce_source_event(approval_event);

    assert!(!effect.duplicate);
    assert_eq!(state.approvals["apr_1"].status, "pending");
    assert!(effect
        .patches
        .iter()
        .any(|patch| patch.kind == MaterializedPatchKind::EntityUpsert
            && patch.view_kind == "approval_inbox"));

    let process_event = source_event(
        11,
        "process.started",
        RuntimeEventScope::Process,
        "proc_1",
        json!({
            "process_id": "proc_1",
            "pid": 123,
            "command": { "command": "echo hi" },
            "cwd": "/repo",
        }),
    );
    let effect = state.reduce_source_event(process_event);
    assert!(!effect.duplicate);
    assert_eq!(state.processes["proc_1"].status, "running");
    assert!(state.agent_row("sess_1").is_some());

    let completed_event = source_event(
        12,
        "turn.completed",
        RuntimeEventScope::Session,
        "sess_1",
        json!({
            "status": "completed",
            "assistant_text": "done",
        }),
    );
    let effect = state.reduce_source_event(completed_event);
    assert!(!effect.duplicate);
    assert_eq!(state.sessions["sess_1"].status, "ready");
    assert!(effect
        .patches
        .iter()
        .any(|patch| patch.kind == MaterializedPatchKind::TextAppend));
}

#[test]
fn materializer_creates_session_from_live_session_created_hint() {
    let mut state = MaterializedState::new("local", "epoch");
    state.mark_live();

    let effect = state.reduce_source_event(source_event(
        13,
        "session.created",
        RuntimeEventScope::Session,
        "sess_live",
        json!({ "provider": "codex" }),
    ));

    assert!(!effect.duplicate);
    assert_eq!(state.sessions["sess_live"].provider, "codex");
    assert_eq!(state.sessions["sess_live"].status, "ready");
    assert!(effect
        .patches
        .iter()
        .any(|patch| patch.kind == MaterializedPatchKind::EntityUpsert
            && patch.view_kind == "fleet_board"));
}

#[test]
fn apply_source_config_preserves_runtime_model_capabilities() {
    let mut state = MaterializedState::new("local", "epoch");
    state.source_metadata.model_capabilities = vec![ModelCapabilityView {
        provider: "codex".to_string(),
        model: "gpt-5.5".to_string(),
        display_name: "GPT 5.5".to_string(),
        reasoning_levels: vec![
            "medium".to_string(),
            "high".to_string(),
            "extra-high".to_string(),
        ],
    }];

    state.apply_source_config(&RuntimeSourceConfig {
        display_name: "Local Gooselake Runtime".to_string(),
        lifecycle: SourceHealthState::Live,
        ..RuntimeSourceConfig::default()
    });

    assert_eq!(state.source_metadata.model_capabilities.len(), 1);
    assert_eq!(
        state.source_metadata.model_capabilities[0].reasoning_levels,
        ["medium", "high", "extra-high"]
    );

    let source_health = state.source_health_view();
    assert_eq!(source_health.model_capabilities.len(), 1);
    assert_eq!(source_health.model_capabilities[0].model, "gpt-5.5");

    let patch = state.transition_source_health(SourceHealthState::Live, None);
    assert_eq!(patch.body["model_capabilities"][0]["model"], "gpt-5.5");
    assert_eq!(
        patch.body["model_capabilities"][0]["reasoning_levels"],
        json!(["medium", "high", "extra-high"])
    );
}

#[test]
fn terminal_turn_usage_updates_session_context_window() {
    let mut state = seeded_state();

    let effect = state.reduce_source_event(source_event(
        14,
        "turn.completed",
        RuntimeEventScope::Session,
        "sess_1",
        json!({
            "status": "completed",
            "assistant_text": "done",
            "usage": {
                "context_window_size": 1000,
                "last_total_tokens": 730
            }
        }),
    ));

    assert!(!effect.duplicate);
    let snapshot = state
        .snapshot_session(&SelectedSessionSubscription {
            session_id: "sess_1".to_string(),
            include_text: true,
        })
        .expect("session snapshot");
    assert_eq!(
        snapshot.session.metadata["context_window"]["remaining_percent"],
        27
    );
    assert_eq!(
        snapshot.session.metadata["context_window"]["window_tokens"],
        1000
    );
    assert_eq!(
        snapshot.session.metadata["context_window"]["used_tokens"],
        730
    );
    assert!(effect
        .patches
        .iter()
        .any(|patch| patch.kind == MaterializedPatchKind::EntityUpsert
            && patch.view_kind == "fleet_board"));
}

#[test]
fn materializer_snapshots_board_inbox_session_team_and_ledger() {
    let mut state = seeded_state();
    state.reduce_source_event(source_event(
        20,
        "approval.requested",
        RuntimeEventScope::Session,
        "sess_1",
        json!({ "approval": approval_record("apr_1", "pending") }),
    ));
    state.reduce_source_event(source_event(
        21,
        "team.member_joined",
        RuntimeEventScope::Team,
        "team_1",
        json!({
            "member": team_member("team_1", "sess_1", Some("Lead")),
        }),
    ));

    let board = state.snapshot_board(&BoardSubscription {
        limit: 10,
        ..BoardSubscription::default()
    });
    assert_eq!(board.total_rows, 1);
    assert_eq!(board.rows[0].pending_approval_count, 1);

    let inbox = state.snapshot_approval_inbox(&ApprovalInboxSubscription::default());
    assert_eq!(inbox.approvals.len(), 1);
    assert_eq!(inbox.approvals[0].approval_id, "apr_1");

    let session = state
        .snapshot_session(&SelectedSessionSubscription {
            session_id: "sess_1".to_string(),
            include_text: true,
        })
        .expect("session snapshot");
    assert_eq!(session.pending_approvals.len(), 1);
    assert_eq!(session.team_ids, vec!["team_1".to_string()]);

    let team = state
        .snapshot_team(&SelectedTeamSubscription {
            team_id: "team_1".to_string(),
            message_limit: 10,
        })
        .expect("team snapshot");
    assert_eq!(team.members.len(), 1);
    assert_eq!(team.members[0].member.agent_id, "sess_1");

    let ledger = state.snapshot_ledger(&LedgerSubscription {
        limit: 10,
        ..LedgerSubscription::default()
    });
    assert_eq!(ledger.events.len(), 2);
    assert_eq!(ledger.events[0].source_seq, 21);
}

#[test]
fn materializer_dedupes_by_source_cursor() {
    let mut state = seeded_state();
    let event = source_event(
        30,
        "approval.requested",
        RuntimeEventScope::Session,
        "sess_1",
        json!({ "approval": approval_record("apr_1", "pending") }),
    );

    let first = state.reduce_source_event(event.clone());
    let second = state.reduce_source_event(event);

    assert!(!first.duplicate);
    assert!(second.duplicate);
    assert!(second.patches.is_empty());
    assert_eq!(state.ledger.len(), 1);
}

#[test]
fn authoritative_session_revision_rejects_stale_overwrite_and_survives_rebuild() {
    let mut state = MaterializedState::new("local", "epoch");
    let mut current = session_record("sess_revision");
    current.status = "ready".into();
    current.updated_at = 20;
    let version = state.upsert_session(current.clone());

    let mut stale = current.clone();
    stale.status = "turn_running".into();
    stale.updated_at = 19;
    assert_eq!(state.upsert_session(stale), version);
    assert_eq!(state.sessions["sess_revision"].status, "ready");

    let mut rebuilt = MaterializedState::new("local", "epoch");
    assert_eq!(rebuilt.upsert_session(current), version);
}

#[test]
fn source_cursor_dedupe_and_ledger_storage_remain_bounded() {
    let mut state = MaterializedState::new("local", "epoch").with_limits(32, 8, 8);
    for seq in 1..=2_500 {
        let effect = state.reduce_source_event(source_event(
            seq,
            "runtime.progress",
            RuntimeEventScope::System,
            "runtime",
            json!({}),
        ));
        assert!(!effect.duplicate);
    }
    assert_eq!(state.ledger.len(), 32);
    let old = state.reduce_source_event(source_event(
        1,
        "runtime.progress",
        RuntimeEventScope::System,
        "runtime",
        json!({}),
    ));
    assert!(old.duplicate);
    assert_eq!(state.ledger.len(), 32);
}

#[test]
fn materializer_coalesces_repeated_state_patches_without_dropping_terminal_events() {
    let mut state = seeded_state();
    let mut buffer = CoalescingPatchBuffer::default();

    let running = state.reduce_source_event(source_event(
        40,
        "turn.started",
        RuntimeEventScope::Session,
        "sess_1",
        json!({}),
    ));
    buffer.extend(running.patches);

    let completed = state.reduce_source_event(source_event(
        41,
        "turn.completed",
        RuntimeEventScope::Session,
        "sess_1",
        json!({ "assistant_text": "terminal" }),
    ));
    buffer.extend(completed.patches);

    let drained = buffer.drain();
    let board_upserts = drained
        .iter()
        .filter(|patch| {
            patch.kind == MaterializedPatchKind::EntityUpsert && patch.view_kind == "fleet_board"
        })
        .count();
    assert_eq!(board_upserts, 1);
    assert!(drained
        .iter()
        .any(|patch| patch.kind == MaterializedPatchKind::TextAppend));
}

#[test]
fn cross_source_board_aggregates_filters_and_preserves_per_source_order() {
    let mut west = seeded_state_for_source("west", "epoch-west", "west_sess");
    let mut east = seeded_state_for_source("east", "epoch-east", "east_sess");

    west.reduce_source_event(source_event_for_source(
        "west",
        "epoch-west",
        "west_sess",
        2,
        "turn.completed",
        RuntimeEventScope::Session,
        "west_sess",
        json!({ "assistant_text": "west done" }),
    ));
    east.reduce_source_event(source_event_for_source(
        "east",
        "epoch-east",
        "east_sess",
        2,
        "turn.completed",
        RuntimeEventScope::Session,
        "east_sess",
        json!({ "assistant_text": "east done" }),
    ));
    west.reduce_source_event(source_event_for_source(
        "west",
        "epoch-west",
        "west_sess",
        3,
        "approval.requested",
        RuntimeEventScope::Session,
        "west_sess",
        json!({ "approval": approval_record_for_session("west_apr", "west_sess", "pending") }),
    ));

    let states = BTreeMap::from([("east".to_string(), east), ("west".to_string(), west)]);
    let board = snapshot_cross_source_board(
        &states,
        &BoardSubscription {
            limit: 10,
            ..BoardSubscription::default()
        },
    );

    assert_eq!(board.total_rows, 2);
    assert_eq!(board.cursors.len(), 2);
    assert_eq!(
        states["west"]
            .ledger
            .iter()
            .map(|event| event.source_seq)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        states["east"]
            .ledger
            .iter()
            .map(|event| event.source_seq)
            .collect::<Vec<_>>(),
        vec![2]
    );

    let west_only = snapshot_cross_source_board(
        &states,
        &BoardSubscription {
            source_id: Some("west".to_string()),
            limit: 10,
            ..BoardSubscription::default()
        },
    );
    assert_eq!(west_only.total_rows, 1);
    assert_eq!(west_only.rows[0].source_id, "west");
    assert_eq!(west_only.rows[0].pending_approval_count, 1);
}

fn seeded_state() -> MaterializedState {
    let mut state = MaterializedState::new("local", "epoch");
    state.mark_live();
    state.upsert_session(session_record("sess_1"));
    state.upsert_team(team_record("team_1"));
    state
}

fn seeded_state_for_source(
    source_id: &str,
    source_epoch: &str,
    session_id: &str,
) -> MaterializedState {
    let mut state = MaterializedState::new(source_id, source_epoch);
    state.mark_live();
    let mut session = session_record(session_id);
    session.id = session_id.to_string();
    state.upsert_session(session);
    state
}

fn source_event(
    row_id: i64,
    kind: &str,
    scope: RuntimeEventScope,
    scope_id: &str,
    runtime_payload: serde_json::Value,
) -> SourceEvent {
    SourceEvent::from_runtime_event(
        "local",
        "epoch",
        RuntimeEventRecord {
            row_id,
            event_id: format!("evt_{row_id}"),
            scope,
            scope_id: scope_id.to_string(),
            session_id: if matches!(scope, RuntimeEventScope::Session) {
                Some(scope_id.to_string())
            } else {
                Some("sess_1".to_string())
            },
            team_id: if matches!(scope, RuntimeEventScope::Team) {
                Some(scope_id.to_string())
            } else {
                None
            },
            turn_id: Some("turn_1".to_string()),
            seq: row_id,
            kind: kind.to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: runtime_payload,
            provider: Some("codex".to_string()),
            provider_seq: Some(row_id),
            created_at: row_id,
        },
    )
}

fn source_event_for_source(
    source_id: &str,
    source_epoch: &str,
    session_id: &str,
    row_id: i64,
    kind: &str,
    scope: RuntimeEventScope,
    scope_id: &str,
    runtime_payload: serde_json::Value,
) -> SourceEvent {
    SourceEvent::from_runtime_event(
        source_id,
        source_epoch,
        RuntimeEventRecord {
            row_id,
            event_id: format!("{source_id}_evt_{row_id}"),
            scope,
            scope_id: scope_id.to_string(),
            session_id: Some(session_id.to_string()),
            team_id: if matches!(scope, RuntimeEventScope::Team) {
                Some(scope_id.to_string())
            } else {
                None
            },
            turn_id: Some("turn_1".to_string()),
            seq: row_id,
            kind: kind.to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: runtime_payload,
            provider: Some("codex".to_string()),
            provider_seq: Some(row_id),
            created_at: row_id,
        },
    )
}

#[test]
fn snapshot_and_patch_detail_bodies_have_identical_typed_shapes() {
    let mut state = MaterializedState::new("local", "epoch-1");
    state.upsert_session(session_record("sess_1"));
    state.upsert_team(team_record("team_1"));

    let session_snapshot = state
        .snapshot_session(&SelectedSessionSubscription {
            session_id: "sess_1".to_string(),
            include_text: true,
        })
        .expect("session snapshot");
    let session_patch = state
        .session_patches("sess_1", None)
        .into_iter()
        .find(|patch| patch.view_kind == "session_detail")
        .expect("session detail patch");
    let decoded_session_patch: SessionDetailView =
        serde_json::from_value(session_patch.body).expect("typed session patch");
    assert_eq!(decoded_session_patch, session_snapshot);

    let team_snapshot = state
        .snapshot_team(&SelectedTeamSubscription {
            team_id: "team_1".to_string(),
            message_limit: 100,
        })
        .expect("team snapshot");
    let team_patch = state
        .team_patch("team_1", None)
        .into_iter()
        .find(|patch| patch.view_kind == "team_workspace")
        .expect("team workspace patch");
    let decoded_team_patch: TeamWorkspaceView =
        serde_json::from_value(team_patch.body).expect("typed team patch");
    assert_eq!(decoded_team_patch, team_snapshot);
}

fn session_record(id: &str) -> SessionRecord {
    SessionRecord {
        id: id.to_string(),
        provider: "codex".to_string(),
        status: "turn_running".to_string(),
        cwd: Some("/repo".to_string()),
        model: Some("gpt-5".to_string()),
        permission_mode: None,
        system_prompt: None,
        metadata: json!({
            "transcript": [
                { "role": "user", "text": "hello" }
            ]
        }),
        provider_session_ref: None,
        canonical_provider_session_ref: None,
        active_turn_id: Some("turn_1".to_string()),
        worktree_id: None,
        created_at: 1,
        updated_at: 1,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

fn approval_record(id: &str, status: &str) -> ApprovalRecord {
    approval_record_for_session(id, "sess_1", status)
}

fn approval_record_for_session(id: &str, session_id: &str, status: &str) -> ApprovalRecord {
    ApprovalRecord {
        id: id.to_string(),
        session_id: session_id.to_string(),
        turn_id: "turn_1".to_string(),
        tool_call_id: Some("tool_1".to_string()),
        provider_approval_ref: Some(id.to_string()),
        status: status.to_string(),
        request: json!({
            "risk": "medium",
            "summary": "Run command",
        }),
        response: None,
        created_at: 2,
        resolved_at: None,
    }
}

fn team_record(id: &str) -> TeamRecord {
    TeamRecord {
        id: id.to_string(),
        name: "Team".to_string(),
        lead_agent_id: "sess_1".to_string(),
        created_by: "user".to_string(),
        created_at: 1,
        updated_at: 1,
        deleted_at: None,
    }
}

fn team_member(team_id: &str, agent_id: &str, title: Option<&str>) -> TeamMemberRecord {
    TeamMemberRecord {
        team_id: team_id.to_string(),
        agent_id: agent_id.to_string(),
        title: title.map(str::to_string),
        joined_at: 1,
        added_by: "user".to_string(),
        creator_agent_id: None,
        creator_compaction_subscription: "auto".to_string(),
        worktree_id: None,
    }
}
