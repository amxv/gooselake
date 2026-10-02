use thiserror::Error;

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
}

impl From<std::io::Error> for RuntimeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}
