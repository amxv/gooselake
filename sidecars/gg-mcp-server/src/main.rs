mod constants;
mod envelope;
mod gateway;
mod schema;
mod server;
mod tool_params;

use rmcp::{ServiceExt, transport::stdio};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::server::GgMcpServer;

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(false),
        )
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let service = GgMcpServer::new().serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
