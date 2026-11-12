use super::*;
use crate::TeamJoinRequest;

fn reject_next(store: &TestStore, kind: &str) {
    *store.fail_next_team_event_kind.lock().unwrap() = Some(kind.to_string());
}

fn team_events(store: &TestStore) -> Vec<RuntimeEventRecord> {
    store
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.scope == RuntimeEventScope::Team)
        .cloned()
        .collect()
}

#[tokio::test]
async fn critical_create_event_failure_keeps_cache_and_store_without_ghost_team() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let lead = create_test_session(&runtime).await;
    let teammate = create_test_session(&runtime).await;
    reject_next(&store, "team.created");
    assert!(matches!(
        service
            .create_team(TeamCreateRequest {
                name: "failure first".into(),
                lead_agent_id: lead.clone(),
                member_agent_ids: vec![teammate.clone()],
                created_by: None,
            })
            .await,
        Err(RuntimeError::Io(_))
    ));
    assert!(service.list_teams().await.unwrap().is_empty());
    let snapshot = store.hydrate_runtime_state().unwrap();
    assert!(snapshot.teams.is_empty());
    assert!(snapshot.team_members.is_empty());
    assert!(team_events(&store).is_empty());

    let team = service
        .create_team(TeamCreateRequest {
            name: "successful next".into(),
            lead_agent_id: lead,
            member_agent_ids: vec![teammate],
            created_by: None,
        })
        .await
        .unwrap();
    assert_eq!(team.members.len(), 2);
    assert_eq!(store.hydrate_runtime_state().unwrap().teams.len(), 1);
    let events = team_events(&store);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "team.created");
    assert_eq!(
        events[0].criticality,
        crate::RuntimeEventCriticality::Critical
    );
}

#[tokio::test]
async fn critical_member_and_lead_event_failures_roll_back_cache_and_durable_roster() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let lead = create_test_session(&runtime).await;
    let teammate = create_test_session(&runtime).await;
    let team = service
        .create_team(TeamCreateRequest {
            name: "failure isolation".into(),
            lead_agent_id: lead.clone(),
            member_agent_ids: Vec::new(),
            created_by: Some("operator".into()),
        })
        .await
        .unwrap();
    let original = team.team.clone();

    reject_next(&store, "team.member_joined");
    assert!(matches!(
        service
            .join_team(TeamJoinRequest {
                team_id: original.id.clone(),
                agent_id: teammate.clone(),
                title: None,
                added_by: None,
                creator_agent_id: None,
                creator_compaction_subscription: None,
                worktree_id: None,
            })
            .await,
        Err(RuntimeError::Io(_))
    ));
    assert_eq!(service.get_team(&original.id).await.unwrap().team, original);
    assert_eq!(store.hydrate_runtime_state().unwrap().team_members.len(), 1);

    let joined = service
        .join_team(TeamJoinRequest {
            team_id: original.id.clone(),
            agent_id: teammate.clone(),
            title: None,
            added_by: None,
            creator_agent_id: None,
            creator_compaction_subscription: None,
            worktree_id: None,
        })
        .await
        .unwrap();
    assert_eq!(joined.members.len(), 2);
    assert!(joined.team.updated_at > original.updated_at);

    reject_next(&store, "team.lead_changed");
    assert!(matches!(
        service
            .set_team_lead(TeamSetLeadRequest {
                team_id: original.id.clone(),
                lead_agent_id: teammate.clone(),
            })
            .await,
        Err(RuntimeError::Io(_))
    ));
    assert_eq!(
        service.get_team(&original.id).await.unwrap().team,
        joined.team
    );
    assert_eq!(store.hydrate_runtime_state().unwrap().teams[0], joined.team);

    let changed = service
        .set_team_lead(TeamSetLeadRequest {
            team_id: original.id.clone(),
            lead_agent_id: teammate,
        })
        .await
        .unwrap();
    assert!(changed.team.updated_at > joined.team.updated_at);
    assert_eq!(changed.team.lead_agent_id, changed.members[1].agent_id);
    assert_eq!(team_events(&store).len(), 3);
}

#[tokio::test]
async fn critical_member_removal_failure_does_not_remove_member_from_cache_or_store() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let lead = create_test_session(&runtime).await;
    let teammate = create_test_session(&runtime).await;
    let created = service
        .create_team(TeamCreateRequest {
            name: "removal isolation".into(),
            lead_agent_id: lead,
            member_agent_ids: vec![teammate.clone()],
            created_by: None,
        })
        .await
        .unwrap();
    reject_next(&store, "team.member_left");
    assert!(matches!(
        service
            .remove_team_member(TeamRemoveMemberRequest {
                team_id: created.team.id.clone(),
                agent_id: teammate.clone(),
            })
            .await,
        Err(RuntimeError::Io(_))
    ));
    assert_eq!(
        service
            .get_team(&created.team.id)
            .await
            .unwrap()
            .members
            .len(),
        2
    );
    assert_eq!(store.hydrate_runtime_state().unwrap().team_members.len(), 2);
    assert_eq!(team_events(&store).len(), 1);

    let removed = service
        .remove_team_member(TeamRemoveMemberRequest {
            team_id: created.team.id.clone(),
            agent_id: teammate,
        })
        .await
        .unwrap();
    assert_eq!(removed.members.len(), 1);
    assert_eq!(store.hydrate_runtime_state().unwrap().team_members.len(), 1);
    assert_eq!(team_events(&store).len(), 2);
}
