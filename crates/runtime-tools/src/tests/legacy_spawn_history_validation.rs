use super::legacy_spawn_history::HistoricalSpawnSeed;
use super::worktree_spawn_recovery::fixture;
use super::*;

#[tokio::test]
async fn malformed_or_changed_history_seed_never_partially_writes_fixture_authority() {
    let fixture = fixture().await;
    let request = fixture.keyed_request("history-construction-validation");
    let before = fixture.store.hydrate_runtime_state().unwrap();
    let source = before
        .sessions
        .iter()
        .find(|s| s.id == request.source_session_id)
        .unwrap()
        .clone();
    let team = runtime_core::TeamWithMembers {
        team: before
            .teams
            .iter()
            .find(|t| t.id == request.team_id)
            .unwrap()
            .clone(),
        members: before.team_members.clone(),
    };
    for case in [
        "empty-operation",
        "empty-child",
        "blank-source",
        "blank-team",
        "duplicate-member",
        "foreign-member",
    ] {
        let mut request = request.clone();
        let mut source = source.clone();
        let mut team = team.clone();
        let mut operation = "op_fixture_validation";
        let mut child = "historical_child_validation";
        match case {
            "empty-operation" => operation = "",
            "empty-child" => child = "",
            "blank-source" => {
                request.source_session_id = " ".into();
                source.id = " ".into();
            }
            "blank-team" => {
                request.team_id = " ".into();
                team.team.id = " ".into();
            }
            "duplicate-member" => team.members.push(team.members[0].clone()),
            "foreign-member" => team.members[0].team_id = "foreign-team".into(),
            _ => unreachable!(),
        }
        let result =
            HistoricalSpawnSeed::for_request(request, source, team, operation, child, None, 1000)
                .and_then(|seed| seed.seed_to_onboarding_sent(fixture.store.as_ref(), 1000, 1001));
        assert!(result.is_err(), "{case}");
        assert_eq!(
            fixture.store.hydrate_runtime_state().unwrap(),
            before,
            "{case}"
        );
    }
    for case in ["request", "response", "message", "delivery"] {
        let mut seed = fixture.history(&request, None);
        match case {
            "request" => seed.request.source_session_id = "different-source".into(),
            "response" => seed.response.spawned_member.agent_id = "different-child".into(),
            "message" => seed.message.sender_agent_id = "different-source".into(),
            "delivery" => seed.delivery.recipient_agent_id = "different-child".into(),
            _ => unreachable!(),
        }
        assert!(
            seed.seed_to_onboarding_sent(fixture.store.as_ref(), 1000, 1001)
                .is_err(),
            "{case}"
        );
        assert_eq!(
            fixture.store.hydrate_runtime_state().unwrap(),
            before,
            "{case}"
        );
    }
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}
