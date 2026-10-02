use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderDispatchOutcome {
    NotDispatched,
    Unknown,
}

impl ProviderDispatchOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotDispatched => "not_dispatched",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("provider '{0}' is already registered")]
    ProviderAlreadyRegistered(String),

    #[error("provider '{0}' is not registered")]
    ProviderNotRegistered(String),

    #[error("resource not found: {0}")]
    NotFound(String),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("bootstrap error: {0}")]
    Bootstrap(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("provider dispatch {outcome}: {message}")]
    ProviderDispatch {
        code: String,
        outcome: &'static str,
        message: String,
    },
}

impl RuntimeError {
    pub fn provider_not_dispatched(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ProviderDispatch {
            code: code.into(),
            outcome: ProviderDispatchOutcome::NotDispatched.as_str(),
            message: message.into(),
        }
    }

    pub fn provider_dispatch_unknown(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ProviderDispatch {
            code: code.into(),
            outcome: ProviderDispatchOutcome::Unknown.as_str(),
            message: message.into(),
        }
    }

    pub fn provider_dispatch_outcome(&self) -> ProviderDispatchOutcome {
        match self {
            Self::ProviderDispatch { outcome, .. }
                if *outcome == ProviderDispatchOutcome::NotDispatched.as_str() =>
            {
                ProviderDispatchOutcome::NotDispatched
            }
            _ => ProviderDispatchOutcome::Unknown,
        }
    }

    pub fn provider_dispatch_code(&self) -> Option<&str> {
        match self {
            Self::ProviderDispatch { code, .. } => Some(code.as_str()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}
