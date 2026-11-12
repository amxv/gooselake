use super::*;
use runtime_core::{LegacyTeamMutation, RuntimeError};
use std::sync::{Arc, Barrier};

fn fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    LegacyTeamMutation,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    for agent_id in [
        "session_lead",
        "session_member",
        "session_new",
        "session_other",
        "session_a",
        "session_b",
        "session_c",
    ] {
        let mut session = sample_session();
        session.id = agent_id.into();
        session.active_turn_id = None;
        session.provider_session_ref = None;
        repository.upsert_session(&session).unwrap();
    }
    let team = TeamRecord {
        id: "team_42".into(),
        name: "Legacy team".into(),
        lead_agent_id: "session_lead".into(),
        created_by: "operator".into(),
        created_at: 1000,
        updated_at: 1000,
        deleted_at: None,
    };
    let members = ["session_lead", "session_member"]
        .iter()
        .map(|agent_id| member(&team, agent_id))
        .collect::<Vec<_>>();
    let event = event(
        &team,
        "team.created",
        "evt_team_team_42_created",
        &team.lead_agent_id,
        serde_json::json!({"team":team,"members":members}),
    );
    let mutation = LegacyTeamMutation {
        before_team: None,
        after_team: team,
        initial_members: members,
        before_member: None,
        after_member: None,
        event,
    };
    (temp, repository, mutation)
}

fn member(team: &TeamRecord, agent_id: &str) -> TeamMemberRecord {
    TeamMemberRecord {
        team_id: team.id.clone(),
        agent_id: agent_id.into(),
        title: None,
        joined_at: team.created_at,
        added_by: "operator".into(),
        creator_agent_id: None,
        creator_compaction_subscription: "auto".into(),
        worktree_id: None,
    }
}

fn event(
    team: &TeamRecord,
    kind: &str,
    event_id: &str,
    actor: &str,
    payload: serde_json::Value,
) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: event_id.into(),
        scope: RuntimeEventScope::Team,
        scope_id: team.id.clone(),
        session_id: Some(actor.into()),
        team_id: Some(team.id.clone()),
        turn_id: None,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: 1001,
    }
}

fn joined(before: &TeamRecord, agent_id: &str) -> LegacyTeamMutation {
    let mut after = before.clone();
    after.updated_at += 1;
    let mut added = member(&after, agent_id);
    added.joined_at = after.updated_at;
    let id = format!("evt_team_{}_join_{agent_id}", after.id);
    let event = event(
        &after,
        "team.member_joined",
        &id,
        agent_id,
        serde_json::json!({"team":after,"member":added}),
    );
    LegacyTeamMutation {
        before_team: Some(before.clone()),
        after_team: after,
        initial_members: Vec::new(),
        before_member: None,
        after_member: Some(added),
        event,
    }
}

fn lead_changed(before: &TeamRecord, agent_id: &str) -> LegacyTeamMutation {
    let mut after = before.clone();
    after.updated_at += 1;
    after.lead_agent_id = agent_id.into();
    let event = event(
        &after,
        "team.lead_changed",
        &format!("evt_team_{}_lead_change", after.id),
        agent_id,
        serde_json::json!({"team":after,"lead_agent_id":agent_id}),
    );
    LegacyTeamMutation {
        before_team: Some(before.clone()),
        after_team: after,
        initial_members: Vec::new(),
        before_member: None,
        after_member: None,
        event,
    }
}

fn removed(before: &TeamRecord, agent_id: &str) -> LegacyTeamMutation {
    let mut after = before.clone();
    after.updated_at += 1;
    let event = event(
        &after,
        "team.member_left",
        &format!("evt_team_{}_leave_{agent_id}", after.id),
        agent_id,
        serde_json::json!({"team":after,"agent_id":agent_id}),
    );
    LegacyTeamMutation {
        before_team: Some(before.clone()),
        after_team: after,
        initial_members: Vec::new(),
        before_member: Some(member(before, agent_id)),
        after_member: None,
        event,
    }
}

#[test]
fn creation_fault_rolls_back_every_member_and_replay_is_exact_after_restart() {
    let (_temp, repo, create) = fixture();
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER reject_team_creation BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team.created'
         BEGIN SELECT RAISE(ABORT, 'forced team journal failure'); END;",
    )
    .unwrap();
    drop(conn);
    assert!(repo.commit_legacy_team_mutation(&create).is_err());
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert!(hydrated.teams.is_empty());
    assert!(hydrated.team_members.is_empty());
    assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_team_creation")
        .unwrap();

    let first = repo.commit_legacy_team_mutation(&create).unwrap();
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let replay = reopened.commit_legacy_team_mutation(&create).unwrap();
    assert_eq!(first, replay);
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().teams,
        vec![create.after_team.clone()]
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_members.len(),
        2
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        1
    );

    let mut forged = create.clone();
    forged.after_team.name = "forged team".into();
    forged.event.payload = serde_json::json!({
        "team":forged.after_team,"members":forged.initial_members
    });
    assert!(matches!(
        reopened.commit_legacy_team_mutation(&forged),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        1
    );
}

#[test]
fn join_event_fault_rolls_back_member_and_generation_then_recovers() {
    let (_temp, repo, create) = fixture();
    repo.commit_legacy_team_mutation(&create).unwrap();
    let join = joined(&create.after_team, "session_new");
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER reject_team_join BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team.member_joined'
         BEGIN SELECT RAISE(ABORT, 'forced team join journal fault'); END;",
    )
    .unwrap();
    drop(conn);
    assert!(repo.commit_legacy_team_mutation(&join).is_err());
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.teams, vec![create.after_team.clone()]);
    assert_eq!(hydrated.team_members.len(), 2);
    assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 1);
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_team_join")
        .unwrap();
    let committed = repo.commit_legacy_team_mutation(&join).unwrap();
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened.commit_legacy_team_mutation(&join).unwrap(),
        committed
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().team_members.len(),
        3
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        2
    );
    assert!(matches!(
        reopened.commit_legacy_team_mutation(&joined(&create.after_team, "session_other")),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn lead_and_leave_mutations_have_atomic_receipts_and_block_stale_replay() {
    let (_temp, repo, create) = fixture();
    repo.commit_legacy_team_mutation(&create).unwrap();
    let changed = lead_changed(&create.after_team, "session_member");
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER reject_lead BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team.lead_changed'
         BEGIN SELECT RAISE(ABORT, 'leader journal fault'); END;",
    )
    .unwrap();
    drop(conn);
    assert!(repo.commit_legacy_team_mutation(&changed).is_err());
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().teams,
        vec![create.after_team.clone()]
    );
    open_connection(&repo.database_path)
        .unwrap()
        .execute_batch("DROP TRIGGER reject_lead")
        .unwrap();
    repo.commit_legacy_team_mutation(&changed).unwrap();

    let leave = removed(&changed.after_team, "session_lead");
    repo.commit_legacy_team_mutation(&leave).unwrap();
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened.commit_legacy_team_mutation(&leave).unwrap().kind,
        "team.member_left"
    );
    let hydrated = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.teams, vec![leave.after_team.clone()]);
    assert_eq!(hydrated.team_members.len(), 1);
    assert!(matches!(
        reopened.commit_legacy_team_mutation(&changed),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        3
    );
}

#[test]
fn independent_writers_with_same_old_generation_allow_one_winner_only() {
    let (_temp, repo, create) = fixture();
    repo.commit_legacy_team_mutation(&create).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for agent in ["session_a", "session_b"] {
        let repo = repo.clone();
        let barrier = Arc::clone(&barrier);
        let before = create.after_team.clone();
        handles.push(std::thread::spawn(move || {
            let mutation = joined(&before, agent);
            barrier.wait();
            repo.commit_legacy_team_mutation(&mutation)
        }));
    }
    let results = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(RuntimeError::Conflict(_))))
            .count(),
        1
    );
    assert_eq!(repo.hydrate_runtime_state().unwrap().team_members.len(), 3);
    assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 2);

    let mut bad_generation = joined(&create.after_team, "session_c");
    bad_generation.after_team.updated_at = create.after_team.updated_at;
    bad_generation.event.payload = serde_json::json!({
        "team":bad_generation.after_team,"member":bad_generation.after_member
    });
    assert!(matches!(
        repo.commit_legacy_team_mutation(&bad_generation),
        Err(RuntimeError::ProtocolViolation(_))
    ));
}

#[test]
fn malformed_or_noncritical_event_cannot_mutate_roster() {
    let (_temp, repo, create) = fixture();
    let mut malformed = create.clone();
    malformed.event.criticality = RuntimeEventCriticality::Droppable;
    assert!(matches!(
        repo.commit_legacy_team_mutation(&malformed),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    let mut malformed = create.clone();
    malformed.event.payload = serde_json::json!({"team":"forged"});
    assert!(matches!(
        repo.commit_legacy_team_mutation(&malformed),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert!(repo.hydrate_runtime_state().unwrap().teams.is_empty());
    assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
}
