use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use runtime_server::{
    bootstrap_runtime, build_router, verify_contract_artifact, write_contract_artifact,
    write_openapi_artifact, AppState, AuthBootstrapSource, RuntimeServerConfig,
};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = parse_cli()?;
    let config_path = match cli {
        CliCommand::WriteOpenApi { path } => {
            write_openapi_artifact(path.as_path())
                .with_context(|| format!("failed to write {}", path.display()))?;
            tracing::info!(path = %path.display(), "wrote generated OpenAPI artifact");
            return Ok(());
        }
        CliCommand::CheckConfig { config_path } => {
            let config = RuntimeServerConfig::load(config_path.as_deref())?;
            config.ensure_data_dirs()?;
            let auth = config.bootstrap_auth()?;
            tracing::info!(
                root_dir = %config.resolve_root_dir().display(),
                sqlite_path = %config.resolve_sqlite_path().display(),
                providers_dir = %config.resolve_data_path(&config.data.providers_dir).display(),
                logs_dir = %config.resolve_data_path(&config.data.logs_dir).display(),
                worktrees_dir = %config.resolve_worktree_root().display(),
                token_source = %describe_auth_source(&auth.source),
                "config check passed"
            );
            return Ok(());
        }
        CliCommand::WriteContracts { path } => {
            write_contract_artifact(path.as_path())?;
            tracing::info!(path = %path.display(), "wrote runtime contract artifact");
            return Ok(());
        }
        CliCommand::VerifyContracts { path } => {
            verify_contract_artifact(path.as_path())?;
            tracing::info!(path = %path.display(), "runtime contract artifact matches binary");
            return Ok(());
        }
        CliCommand::Serve { config_path } => config_path,
    };
    let config = RuntimeServerConfig::load(config_path.as_deref())?;
    let bootstrapped = bootstrap_runtime(config).await?;

    let token_source = describe_auth_source(&bootstrapped.auth.source);
    tracing::info!(
        bind = %bootstrapped.bind_address,
        public_base_url = %bootstrapped.public_base_url,
        provider_count = bootstrapped.app.provider_registry.len(),
        token_source = %token_source,
        "runtime bootstrapped"
    );

    let state = AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: bootstrapped.auth.bearer_token,
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(bootstrapped.startup_recovery),
    };

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bootstrapped.bind_address)
        .await
        .with_context(|| format!("failed to bind {}", bootstrapped.bind_address))?;

    tracing::info!("gg-runtime-server listening");

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
    WriteOpenApi { path: PathBuf },
    WriteContracts { path: PathBuf },
    VerifyContracts { path: PathBuf },
    CheckConfig { config_path: Option<PathBuf> },
}

fn parse_cli() -> Result<CliCommand> {
    let mut args = std::env::args().skip(1);
    let mut config_path = None;
    let mut write_openapi_path = None;
    let mut write_contracts_path = None;
    let mut verify_contracts_path = None;
    let mut check_config = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--config requires a path argument"))?;
                config_path = Some(PathBuf::from(value));
            }
            "--write-openapi" => {
                let path = args.next().map(PathBuf::from).unwrap_or_else(|| {
                    PathBuf::from("openapi").join("runtime-server-openapi.yaml")
                });
                write_openapi_path = Some(path);
            }
            "--write-contracts" => {
                let path = args
                    .next()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("contracts").join("runtime-contracts.json"));
                write_contracts_path = Some(path);
            }
            "--verify-contracts" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--verify-contracts requires an artifact path"))?;
                verify_contracts_path = Some(PathBuf::from(path));
            }
            "--check-config" => {
                check_config = true;
            }
            other => {
                return Err(anyhow!("unknown argument: {other}"));
            }
        }
    }
    let mode_count = usize::from(check_config)
        + usize::from(write_openapi_path.is_some())
        + usize::from(write_contracts_path.is_some())
        + usize::from(verify_contracts_path.is_some());
    if mode_count > 1 {
        return Err(anyhow!(
            "check-config and artifact modes cannot be combined"
        ));
    }
    if let Some(path) = write_openapi_path {
        return Ok(CliCommand::WriteOpenApi { path });
    }
    if let Some(path) = write_contracts_path {
        return Ok(CliCommand::WriteContracts { path });
    }
    if let Some(path) = verify_contracts_path {
        return Ok(CliCommand::VerifyContracts { path });
    }
    if check_config {
        return Ok(CliCommand::CheckConfig { config_path });
    }
    Ok(CliCommand::Serve { config_path })
}

fn describe_auth_source(source: &AuthBootstrapSource) -> String {
    match source {
        AuthBootstrapSource::InlineConfig => "inline-config".to_string(),
        AuthBootstrapSource::TokenFileExisting { path } => {
            format!("token-file-existing:{}", path.display())
        }
        AuthBootstrapSource::TokenFileCreated { path } => {
            format!("token-file-created:{}", path.display())
        }
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
