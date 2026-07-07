use std::sync::{Arc, Mutex as StdMutex};

use super::*;

#[tokio::test]
async fn team_scoped_join_member_routes_to_team_source() {
    let hits = Arc::new(StdMutex::new(Vec::new()));
    let runtime_addr = spawn_accepting_create_runtime("local", hits.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    {
        let mut materialized = gateway.materialized.write().await;
        materialized
            .get_mut("local")
            .expect("local source")
            .upsert_team(team_record("team_1"));
    }
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(&mut conn, join_team_member_command("cmd_join", "team_1"))
        .await;

    assert!(
        matches!(response.payload, Some(Payload::CommandAccepted(_))),
        "expected join-team-member command to be accepted, got {:?}",
        response.payload
    );
    assert_eq!(
        hits.lock().unwrap().as_slice(),
        ["local:join_team_member:team_1:session_2"]
    );
}
