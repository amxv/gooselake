use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use goosetower::config::GoosetowerConfig;
use goosetower::gateway::GatewayState;
use goosetower::http::{build_router, AppState, RuntimeHealthClient};
use goosetower::verification::tower_observer::build_tower_observer_router;

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = parse_config_path()?;
    let config = GoosetowerConfig::load(config_path.as_deref())?;
    config.validate()?;
    let api_auth = config.bootstrap_api_auth()?;
    let product_listener = tokio::net::TcpListener::bind(&config.server.bind_address)
        .await
        .with_context(|| format!("failed to bind {}", config.server.bind_address))?;
    let observer_address = std::env::var("P02_TOWER_OBSERVER_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:18113".to_string());
    let observer_secret = std::env::var("P02_TOWER_OBSERVER_SECRET")
        .map_err(|_| anyhow!("P02_TOWER_OBSERVER_SECRET is required"))?;
    let observer_listener = tokio::net::TcpListener::bind(&observer_address)
        .await
        .with_context(|| format!("failed to bind {observer_address}"))?;

    let config = Arc::new(config);
    let gateway = Arc::new(GatewayState::new(config.clone())?);
    gateway.bootstrap_enabled_sources().await;
    let _source_tasks = gateway.spawn_runtime_source_tasks().await;
    let product = build_router(AppState {
        gateway: gateway.clone(),
        config,
        api_bearer_token: Arc::from(api_auth.bearer_token),
        runtime_client: RuntimeHealthClient::new(),
    });
    let observer = build_tower_observer_router(gateway, Arc::<str>::from(observer_secret));

    eprintln!("P02 observed Goosetower product listener ready");
    eprintln!("P02 verification observer listening on http://{observer_address}");
    tokio::select! {
        result = axum::serve(product_listener, product) => result.context("product server failed")?,
        result = axum::serve(observer_listener, observer) => result.context("observer server failed")?,
        _ = tokio::signal::ctrl_c() => {},
    }
    Ok(())
}

fn parse_config_path() -> Result<Option<PathBuf>> {
    let mut args = std::env::args().skip(1);
    let mut config_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        anyhow!("--config requires a path argument")
                    })?));
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    Ok(config_path)
}
