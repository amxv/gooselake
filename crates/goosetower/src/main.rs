use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use goosetower::config::{AuthTokenSource, GoosetowerConfig};
use goosetower::gateway::GatewayState;
use goosetower::http::{build_router, AppState, RuntimeHealthClient};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = parse_cli()?;
    let config_path = match cli {
        CliCommand::CheckConfig { config_path } => {
            let config = GoosetowerConfig::load(config_path.as_deref())?;
            config.validate()?;
            let api_auth = config.bootstrap_api_auth()?;
            tracing::info!(
                bind = %config.server.bind_address,
                public_base_url = %config.server.public_base_url,
                source_count = config.runtimes.sources.len(),
                api_token_source = %describe_auth_source(&api_auth.source),
                "goosetower config check passed"
            );
            return Ok(());
        }
        CliCommand::Serve { config_path } => config_path,
    };

    let config = GoosetowerConfig::load(config_path.as_deref())?;
    config.validate()?;
    let api_auth = config.bootstrap_api_auth()?;
    tracing::info!(
        bind = %config.server.bind_address,
        public_base_url = %config.server.public_base_url,
        protocol_version = goosetower::protocol::PROTOCOL_VERSION,
        source_count = config.runtimes.sources.len(),
        "goosetower bootstrapped"
    );

    let listener = tokio::net::TcpListener::bind(&config.server.bind_address)
        .await
        .with_context(|| format!("failed to bind {}", config.server.bind_address))?;
    let config = Arc::new(config);
    let gateway = Arc::new(GatewayState::new(config.clone())?);
    gateway.bootstrap_enabled_sources().await;
    let state = AppState {
        gateway,
        config,
        api_bearer_token: Arc::from(api_auth.bearer_token),
        runtime_client: RuntimeHealthClient::new(),
    };
    let router = build_router(state);

    tracing::info!("gg-goosetower listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server failed")?;

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

enum CliCommand {
    Serve { config_path: Option<PathBuf> },
    CheckConfig { config_path: Option<PathBuf> },
}

fn parse_cli() -> Result<CliCommand> {
    let mut args = std::env::args().skip(1);
    let mut config_path = None;
    let mut check_config = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--config requires a path argument"))?;
                config_path = Some(PathBuf::from(value));
            }
            "--check-config" => {
                check_config = true;
            }
            other => {
                return Err(anyhow!("unknown argument: {other}"));
            }
        }
    }
    if check_config {
        return Ok(CliCommand::CheckConfig { config_path });
    }
    Ok(CliCommand::Serve { config_path })
}

fn describe_auth_source(source: &AuthTokenSource) -> String {
    match source {
        AuthTokenSource::InlineConfig => "inline-config".to_string(),
        AuthTokenSource::TokenFile { path } => format!("token-file:{}", path.display()),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut stream) = signal(SignalKind::terminate()) {
            let _ = stream.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
