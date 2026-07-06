use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use runtime_core::{ProviderAuthStatus, ProviderTurnStatus, RuntimeError};
use serde_json::Value;

use crate::bridge::fail_bridge;
use crate::config::GG_MCP_MISSING_BAD_REQUEST;
use crate::provider::ClaudeProvider;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaudeAuthMode {
    HostMachine,
    RuntimeManaged,
}

impl ClaudeAuthMode {
    fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("runtime_managed") | Some("runtime-managed") | Some("runtime") => {
                Self::RuntimeManaged
            }
            _ => Self::HostMachine,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::HostMachine => "host_machine",
            Self::RuntimeManaged => "runtime_managed",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaudeConfigResolutionSource {
    EnvOverride,
    GgFallback,
    UpstreamDefault,
}

impl ClaudeConfigResolutionSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::EnvOverride => "env_override",
            Self::GgFallback => "gg_fallback",
            Self::UpstreamDefault => "upstream_default",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClaudeAuthPathsResolution {
    pub(crate) credentials_path: PathBuf,
    pub(crate) config_path: PathBuf,
    pub(crate) config_dir: Option<PathBuf>,
    pub(crate) config_source: ClaudeConfigResolutionSource,
}

#[derive(Debug)]
pub(crate) struct ClaudeAuthImportPayload {
    pub(crate) credentials_json: Option<Value>,
    pub(crate) config_json: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaudeBridgeAuthEnvironment {
    pub(crate) home_dir: PathBuf,
    pub(crate) claude_config_dir: Option<PathBuf>,
    pub(crate) auth_paths: ClaudeAuthPathsResolution,
}

impl ClaudeProvider {
    pub(crate) fn runtime_home_dir(&self) -> PathBuf {
        self.inner
            .config
            .config_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.inner.config.config_dir.clone())
            .join("home")
    }

    pub(crate) fn process_home_dir() -> Option<PathBuf> {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
    }

    pub(crate) fn auth_mode(&self) -> ClaudeAuthMode {
        let env_mode = std::env::var("GG_CLAUDE_AUTH_MODE").ok();
        let configured = self
            .inner
            .config
            .bridge_env
            .get("GG_CLAUDE_AUTH_MODE")
            .map(String::as_str)
            .or(env_mode.as_deref());
        ClaudeAuthMode::parse(configured)
    }

    pub(crate) fn bridge_home_dir(&self) -> PathBuf {
        if let Some(home_override) = self
            .inner
            .config
            .bridge_env
            .get("HOME")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
        {
            return home_override;
        }

        match self.auth_mode() {
            ClaudeAuthMode::HostMachine => {
                Self::process_home_dir().unwrap_or_else(|| self.runtime_home_dir())
            }
            ClaudeAuthMode::RuntimeManaged => self.runtime_home_dir(),
        }
    }

    pub(crate) fn bridge_claude_config_dir_override(&self) -> Option<PathBuf> {
        if let Some(config_override) = self
            .inner
            .config
            .bridge_env
            .get("CLAUDE_CONFIG_DIR")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
        {
            return Some(config_override);
        }

        match self.auth_mode() {
            ClaudeAuthMode::HostMachine => std::env::var_os("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .filter(|path| !path.as_os_str().is_empty()),
            ClaudeAuthMode::RuntimeManaged => Some(self.inner.config.config_dir.clone()),
        }
    }

    pub(crate) fn bridge_auth_overrides_active(&self) -> bool {
        self.inner
            .config
            .bridge_env
            .get("HOME")
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
            || self
                .inner
                .config
                .bridge_env
                .get("CLAUDE_CONFIG_DIR")
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
    }

    pub(crate) fn resolve_bridge_auth_environment(
        &self,
    ) -> Result<ClaudeBridgeAuthEnvironment, RuntimeError> {
        let home_dir = self.bridge_home_dir();
        let claude_config_dir = self.bridge_claude_config_dir_override();
        let auth_paths =
            resolve_claude_auth_paths(claude_config_dir.clone(), Some(home_dir.clone()))
                .ok_or_else(|| {
                    RuntimeError::InvalidState(
                        "unable to resolve Claude auth/config paths".to_string(),
                    )
                })?;
        Ok(ClaudeBridgeAuthEnvironment {
            home_dir,
            claude_config_dir: auth_paths.config_dir.clone(),
            auth_paths,
        })
    }

    pub(crate) fn validate_bridge_auth_environment(
        &self,
        env: &ClaudeBridgeAuthEnvironment,
    ) -> Result<(), RuntimeError> {
        let runtime_api_key_present = std::fs::read_to_string(self.api_key_path())
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        let bridge_api_key_present = self
            .inner
            .config
            .bridge_env
            .get("ANTHROPIC_API_KEY")
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        let process_api_key_present = std::env::var("ANTHROPIC_API_KEY")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        if runtime_api_key_present || bridge_api_key_present || process_api_key_present {
            return Ok(());
        }

        let credentials_path = env.auth_paths.credentials_path.as_path();
        if !credentials_path.exists() {
            return Err(RuntimeError::NotFound(format!(
                "Claude OAuth credentials file missing at {}. Run `claude login`, or import credentials via /v1/providers/claude/auth/import-json or /v1/providers/claude/auth/import-file.",
                credentials_path.display()
            )));
        }

        if read_claude_oauth_access_token(credentials_path).is_none() {
            return Err(RuntimeError::InvalidState(format!(
            "Claude OAuth credentials file at {} is missing a non-empty claudeAiOauth.accessToken.",
            credentials_path.display()
        )));
        }

        let config_path = env.auth_paths.config_path.as_path();
        if !config_path.exists() {
            return Err(RuntimeError::NotFound(format!(
                "Claude config file missing at {}. Ensure canonical Claude config is present (for example ~/.gg/claude/.claude.json or ~/.claude.json).",
                config_path.display()
            )));
        }

        let config_content = std::fs::read_to_string(config_path).map_err(|error| {
            RuntimeError::Io(format!(
                "failed reading Claude config file {}: {error}",
                config_path.display()
            ))
        })?;
        let parsed = serde_json::from_str::<Value>(config_content.as_str()).map_err(|error| {
            RuntimeError::InvalidState(format!(
                "Claude config file {} is not valid JSON: {error}",
                config_path.display()
            ))
        })?;
        if !parsed.is_object() {
            return Err(RuntimeError::InvalidState(format!(
                "Claude config file {} must contain a JSON object",
                config_path.display()
            )));
        }

        Ok(())
    }

    pub(crate) fn runtime_claude_auth_paths(&self) -> ClaudeAuthPathsResolution {
        resolve_claude_auth_paths(
            Some(self.inner.config.config_dir.clone()),
            Some(self.runtime_home_dir()),
        )
        .expect("runtime Claude auth paths should always resolve with runtime home")
    }

    pub(crate) fn claude_credentials_path(&self) -> PathBuf {
        self.runtime_claude_auth_paths().credentials_path
    }

    pub(crate) fn claude_config_path(&self) -> PathBuf {
        self.runtime_claude_auth_paths().config_path
    }

    pub(crate) fn api_key_path(&self) -> PathBuf {
        self.inner.config.config_dir.join("api_key")
    }

    pub(crate) async fn ensure_provider_enabled(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap(
                "claude provider disabled".to_string(),
            ));
        }
        let auth_paths = self.runtime_claude_auth_paths();
        if let Some(config_dir) = auth_paths.config_dir.as_ref() {
            tokio::fs::create_dir_all(config_dir)
                .await
                .map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to create Claude config dir {}: {error}",
                        config_dir.display()
                    ))
                })?;
        }
        if let Some(credentials_dir) = auth_paths.credentials_path.parent() {
            tokio::fs::create_dir_all(credentials_dir)
                .await
                .map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to create Claude credentials dir {}: {error}",
                        credentials_dir.display()
                    ))
                })?;
        }
        tokio::fs::create_dir_all(&self.inner.config.config_dir)
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create Claude config dir {}: {error}",
                    self.inner.config.config_dir.display()
                ))
            })?;
        Ok(())
    }

    pub(crate) async fn read_api_key(&self) -> Result<Option<String>, RuntimeError> {
        let path = self.api_key_path();
        match tokio::fs::read_to_string(path.as_path()).await {
            Ok(content) => {
                let trimmed = content.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed.to_string()))
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RuntimeError::Io(format!(
                "failed reading Claude API key file {}: {error}",
                path.display()
            ))),
        }
    }

    pub(crate) async fn write_api_key(&self, api_key: &str) -> Result<(), RuntimeError> {
        tokio::fs::create_dir_all(&self.inner.config.config_dir)
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create Claude config dir {}: {error}",
                    self.inner.config.config_dir.display()
                ))
            })?;
        let path = self.api_key_path();
        tokio::fs::write(path.as_path(), api_key.as_bytes())
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed writing Claude API key file {}: {error}",
                    path.display()
                ))
            })?;
        set_permissions_if_unix(path.as_path(), 0o600)?;
        Ok(())
    }

    pub(crate) async fn write_claude_json_file(
        &self,
        path: &Path,
        payload: &Value,
    ) -> Result<(), RuntimeError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                RuntimeError::Io(format!(
                    "failed creating Claude auth parent dir {}: {error}",
                    parent.display()
                ))
            })?;
            set_permissions_if_unix(parent, 0o700)?;
        }
        let encoded = serde_json::to_vec_pretty(payload).map_err(|error| {
            RuntimeError::InvalidState(format!(
                "invalid Claude auth JSON payload for {}: {error}",
                path.display()
            ))
        })?;
        tokio::fs::write(path, encoded).await.map_err(|error| {
            RuntimeError::Io(format!(
                "failed writing Claude auth file {}: {error}",
                path.display()
            ))
        })?;
        set_permissions_if_unix(path, 0o600)?;
        Ok(())
    }

    pub(crate) async fn write_oauth_credentials_json(
        &self,
        credentials_json: &Value,
    ) -> Result<(), RuntimeError> {
        let credentials_path = self.claude_credentials_path();
        self.write_claude_json_file(credentials_path.as_path(), credentials_json)
            .await
    }

    pub(crate) async fn write_claude_config_json(
        &self,
        config_json: &Value,
    ) -> Result<(), RuntimeError> {
        let config_path = self.claude_config_path();
        self.write_claude_json_file(config_path.as_path(), config_json)
            .await
    }

    pub(crate) async fn recycle_after_live_auth_change(&self) {
        let sessions = {
            let mut sessions = self.inner.sessions.write().await;
            let mut by_bridge = self.inner.sessions_by_bridge_key.write().await;
            by_bridge.clear();
            std::mem::take(&mut *sessions)
        };
        drop(sessions);

        let bridges = {
            let mut bridges = self.inner.bridges.write().await;
            std::mem::take(&mut *bridges)
                .into_values()
                .collect::<Vec<_>>()
        };
        for bridge in bridges {
            fail_bridge(
                &self.inner,
                &bridge,
                "Claude provider auth-change recycle".to_string(),
            )
            .await;
        }
    }

    pub(crate) async fn provider_auth_status_internal(
        &self,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let auth_mode = self.auth_mode();
        let runtime_auth_paths = self.runtime_claude_auth_paths();
        let runtime_credentials_present =
            has_claude_oauth_access_token(runtime_auth_paths.credentials_path.as_path());
        let runtime_config_json_present = runtime_auth_paths.config_path.exists();
        let runtime_oauth_ready = runtime_credentials_present && runtime_config_json_present;
        let bridge_auth_env = self.resolve_bridge_auth_environment().ok();
        let bridge_credentials_present = bridge_auth_env
            .as_ref()
            .map(|env| has_claude_oauth_access_token(env.auth_paths.credentials_path.as_path()))
            .unwrap_or(false);
        let bridge_config_json_present = bridge_auth_env
            .as_ref()
            .map(|env| env.auth_paths.config_path.exists())
            .unwrap_or(false);
        let bridge_oauth_ready = bridge_credentials_present && bridge_config_json_present;
        let api_key_present = self.read_api_key().await?.is_some();
        let authenticated = match auth_mode {
            ClaudeAuthMode::HostMachine => {
                runtime_oauth_ready || bridge_oauth_ready || api_key_present
            }
            ClaudeAuthMode::RuntimeManaged => {
                runtime_oauth_ready
                    || (self.bridge_auth_overrides_active() && bridge_oauth_ready)
                    || api_key_present
            }
        };
        let oauth_mode_ready = match auth_mode {
            ClaudeAuthMode::HostMachine => runtime_oauth_ready || bridge_oauth_ready,
            ClaudeAuthMode::RuntimeManaged => {
                runtime_oauth_ready || (self.bridge_auth_overrides_active() && bridge_oauth_ready)
            }
        };
        Ok(ProviderAuthStatus {
            authenticated,
            mode: if oauth_mode_ready {
                Some("claude_code_oauth".to_string())
            } else if api_key_present {
                Some("api_key".to_string())
            } else {
                None
            },
            detail: Some(format!(
                "auth_mode={} runtime_credentials_path={} runtime_config_path={} runtime_config_source={} runtime_credentials_present={} runtime_config_present={} runtime_oauth_ready={} bridge_credentials_path={} bridge_config_path={} bridge_config_source={} bridge_credentials_present={} bridge_config_present={} bridge_oauth_ready={} bridge_override_active={} api_key_present={}",
                auth_mode.as_str(),
                runtime_auth_paths.credentials_path.display(),
                runtime_auth_paths.config_path.display(),
                runtime_auth_paths.config_source.as_str(),
                runtime_credentials_present,
                runtime_config_json_present,
                runtime_oauth_ready,
                bridge_auth_env
                    .as_ref()
                    .map(|env| env.auth_paths.credentials_path.display().to_string())
                    .unwrap_or_else(|| "<unresolved>".to_string()),
                bridge_auth_env
                    .as_ref()
                    .map(|env| env.auth_paths.config_path.display().to_string())
                    .unwrap_or_else(|| "<unresolved>".to_string()),
                bridge_auth_env
                    .as_ref()
                    .map(|env| env.auth_paths.config_source.as_str())
                    .unwrap_or("unknown"),
                bridge_credentials_present,
                bridge_config_json_present,
                bridge_oauth_ready,
                self.bridge_auth_overrides_active(),
                api_key_present,
            )),
        })
    }
}

pub(crate) fn resolve_claude_auth_paths(
    env_claude_config_dir: Option<PathBuf>,
    home_dir: Option<PathBuf>,
) -> Option<ClaudeAuthPathsResolution> {
    let home_dir = home_dir?;
    let credentials_path = home_dir.join(".claude").join(".credentials.json");

    if let Some(config_dir) = env_claude_config_dir {
        return Some(ClaudeAuthPathsResolution {
            credentials_path,
            config_path: config_dir.join(".claude.json"),
            config_dir: Some(config_dir),
            config_source: ClaudeConfigResolutionSource::EnvOverride,
        });
    }

    let gg_claude_dir = home_dir.join(".gg").join("claude");
    if gg_claude_dir.is_dir() {
        return Some(ClaudeAuthPathsResolution {
            credentials_path,
            config_path: gg_claude_dir.join(".claude.json"),
            config_dir: Some(gg_claude_dir),
            config_source: ClaudeConfigResolutionSource::GgFallback,
        });
    }

    Some(ClaudeAuthPathsResolution {
        credentials_path,
        config_path: home_dir.join(".claude.json"),
        config_dir: None,
        config_source: ClaudeConfigResolutionSource::UpstreamDefault,
    })
}

pub(crate) fn has_claude_oauth_access_token(credentials_path: &Path) -> bool {
    read_claude_oauth_access_token(credentials_path).is_some()
}

pub(crate) fn read_claude_oauth_access_token(credentials_path: &Path) -> Option<String> {
    let Ok(contents) = std::fs::read_to_string(credentials_path) else {
        return None;
    };
    let Ok(parsed) = serde_json::from_str::<Value>(&contents) else {
        return None;
    };

    let token = parsed
        .get("claudeAiOauth")
        .and_then(Value::as_object)
        .and_then(|oauth| oauth.get("accessToken"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(token.to_string())
}

fn as_json_object(value: &Value, label: &str) -> Result<Value, RuntimeError> {
    if value.is_object() {
        return Ok(value.clone());
    }
    Err(RuntimeError::InvalidState(format!(
        "Claude auth import field '{label}' must be a JSON object"
    )))
}

pub(crate) fn parse_claude_auth_import_payload(
    value: Value,
) -> Result<ClaudeAuthImportPayload, RuntimeError> {
    let object = value.as_object().ok_or_else(|| {
        RuntimeError::InvalidState("Claude auth import payload must be a JSON object".to_string())
    })?;

    let mut credentials_json = if let Some(raw) = object
        .get("credentials_json")
        .or_else(|| object.get("credentials"))
    {
        Some(as_json_object(raw, "credentials_json")?)
    } else {
        None
    };

    let mut config_json =
        if let Some(raw) = object.get("config_json").or_else(|| object.get("config")) {
            Some(as_json_object(raw, "config_json")?)
        } else {
            None
        };

    if credentials_json.is_none() {
        if let Some(token) = object.get("token").and_then(Value::as_str) {
            let token = token.trim();
            if !token.is_empty() {
                credentials_json = Some(serde_json::json!({
                    "claudeAiOauth": {
                        "accessToken": token,
                        "refreshToken": token,
                    }
                }));
            }
        }
    }

    if credentials_json.is_none() && config_json.is_none() {
        if object.get("claudeAiOauth").is_some() {
            credentials_json = Some(Value::Object(object.clone()));
        } else {
            config_json = Some(Value::Object(object.clone()));
        }
    }

    if credentials_json.is_none() && config_json.is_none() {
        return Err(RuntimeError::InvalidState(
            "Claude auth import payload must include credentials/config data".to_string(),
        ));
    }

    Ok(ClaudeAuthImportPayload {
        credentials_json,
        config_json,
    })
}
pub(crate) fn claude_smoke_debug_enabled() -> bool {
    std::env::var("GG_CLAUDE_SMOKE_DEBUG")
        .ok()
        .map(|value| value.trim() == "1")
        .unwrap_or(false)
}

pub(crate) fn extract_turn_status(value: Option<&Value>) -> ProviderTurnStatus {
    match value
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("completed") => ProviderTurnStatus::Completed,
        Some("interrupted") => ProviderTurnStatus::Interrupted,
        Some("failed") => ProviderTurnStatus::Failed,
        _ => ProviderTurnStatus::InProgress,
    }
}

pub(crate) fn extract_assistant_text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn merge_assistant_text_into_usage(
    usage: Option<Value>,
    assistant_text: Option<String>,
) -> Option<Value> {
    let Some(assistant_text) = assistant_text else {
        return usage;
    };
    if assistant_text.trim().is_empty() {
        return usage;
    }

    match usage {
        Some(Value::Object(mut usage_object)) => {
            usage_object.insert(
                "last_message".to_string(),
                Value::String(assistant_text.clone()),
            );
            usage_object.insert("assistant_text".to_string(), Value::String(assistant_text));
            Some(Value::Object(usage_object))
        }
        Some(existing) => Some(serde_json::json!({
            "last_message": assistant_text.clone(),
            "assistant_text": assistant_text,
            "raw_usage": existing,
        })),
        None => Some(serde_json::json!({
            "last_message": assistant_text.clone(),
            "assistant_text": assistant_text,
        })),
    }
}

pub(crate) fn map_bridge_error(error: &Value) -> RuntimeError {
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("INTERNAL_ERROR");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Claude bridge request failed");
    let message = format!("claude bridge {code}: {message}");
    match code {
        "SESSION_NOT_FOUND" | "TURN_NOT_FOUND" | "APPROVAL_NOT_FOUND" => {
            RuntimeError::NotFound(message)
        }
        "BAD_REQUEST" | "TURN_IN_PROGRESS" | "TURN_NOT_IN_PROGRESS" => {
            RuntimeError::InvalidState(message)
        }
        "PROTOCOL_VIOLATION" => RuntimeError::ProtocolViolation(message),
        "UNAUTHORIZED" => RuntimeError::Configuration(message),
        "TIMEOUT" => RuntimeError::InvalidState(message),
        "PROVIDER_PROCESS_EXITED" | "INTERNAL_ERROR" => RuntimeError::Io(message),
        _ => RuntimeError::InvalidState(message),
    }
}

pub(crate) fn is_missing_gg_mcp_server_bad_request(error: &RuntimeError) -> bool {
    match error {
        RuntimeError::InvalidState(message)
        | RuntimeError::ProtocolViolation(message)
        | RuntimeError::NotFound(message)
        | RuntimeError::Io(message)
        | RuntimeError::Configuration(message)
        | RuntimeError::Bootstrap(message)
        | RuntimeError::Unsupported(message)
        | RuntimeError::ProviderAlreadyRegistered(message)
        | RuntimeError::ProviderNotRegistered(message) => {
            message.contains(GG_MCP_MISSING_BAD_REQUEST)
        }
    }
}

pub(crate) fn bridge_session_key(bridge_instance_id: u64, bridge_session_id: &str) -> String {
    format!("{bridge_instance_id}:{bridge_session_id}")
}
pub(crate) fn set_permissions_if_unix(path: &Path, mode: u32) -> Result<(), RuntimeError> {
    #[cfg(unix)]
    {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|error| {
            RuntimeError::Io(format!(
                "failed setting permissions on {}: {error}",
                path.display()
            ))
        })?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
    Ok(())
}
