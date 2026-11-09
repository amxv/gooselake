use super::*;

/// Test fixtures do not start a background worker, by design: some tests need
/// to inspect pending notice authority. Tests exercising ordinary user turns
/// must explicitly establish the required first provider turn via this helper.
pub(super) async fn finish_test_onboarding(
    runtime: &Arc<RuntimeSessionManager>,
    workspace_id: &str,
    agent_id: &str,
) {
    runtime
        .dispatch_workspace_system_notices()
        .await
        .expect("deliver pending system onboarding");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let notice = runtime
                .get_workspace_onboarding_notice(workspace_id, agent_id)
                .expect("read onboarding notice")
                .expect("onboarding intent exists");
            let session = runtime.get_session(agent_id).await.expect("test session");
            if notice.state == "delivered" && session.active_turn_id.is_none() {
                break;
            }
            assert!(
                matches!(notice.state.as_str(), "pending" | "injecting" | "delivered"),
                "onboarding cannot safely finish: {notice:?}"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mandatory onboarding must finish before user turns");
}
