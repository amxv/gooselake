use anyhow::Result;
use goosetower::config::GoosetowerConfig;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = GoosetowerConfig::load(None)?;
    tracing::info!(
        bind = %config.server.bind_address,
        protocol_version = goosetower::protocol::PROTOCOL_VERSION,
        "gg-goosetower skeleton initialized"
    );

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();
}
