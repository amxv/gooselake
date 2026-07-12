use super::tests::{apply_control, gateway, spawn};
use super::*;
use crate::runtime::SourceHealthState;

#[tokio::test]
async fn live_tower_rebases_to_runtime_epoch_after_reconnect_without_old_epoch_labels() {
    let (_source, base) = spawn().await;
    let gateway = Arc::new(gateway(base.clone()));
    gateway.bootstrap_enabled_sources().await;
    apply_control(&base, FaultControl::DisconnectNext).await;
    let handles = gateway.spawn_runtime_source_tasks().await;
    wait_for(&gateway, |observer| {
        observer.source_health.state == SourceHealthState::Stale
    })
    .await;
    apply_control(&base, FaultControl::ChangeEpoch).await;
    wait_for(&gateway, |observer| {
        observer.source_epoch == "p02-epoch-002"
    })
    .await;
    apply_control(&base, FaultControl::EmitNext).await;
    wait_for(&gateway, |observer| {
        observer.source_health.last_source_seq == Some(1)
    })
    .await;
    let observer = gateway.verification_materializer_observer().await;
    assert_eq!(observer[0].source_epoch, "p02-epoch-002");
    assert_eq!(observer[0].source_health.source_epoch, "p02-epoch-002");
    assert!(observer[0]
        .recent_ledger
        .iter()
        .all(|event| event["source_epoch"] != INITIAL_EPOCH));
    for handle in handles {
        handle.abort();
    }
}

async fn wait_for(
    gateway: &crate::gateway::GatewayState,
    predicate: impl Fn(&crate::gateway::MaterializerObserverSnapshot) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let observer = gateway.verification_materializer_observer().await;
            if predicate(&observer[0]) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observer condition");
}
