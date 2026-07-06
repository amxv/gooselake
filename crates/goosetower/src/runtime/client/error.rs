use std::fmt;

use reqwest::StatusCode;

#[derive(Debug)]
pub enum RuntimeClientError {
    Transport(reqwest::Error),
    Decode(reqwest::Error),
    Json(serde_json::Error),
    Http { status: StatusCode, body: String },
}

impl fmt::Display for RuntimeClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "runtime transport error: {error}"),
            Self::Decode(error) => write!(f, "runtime decode error: {error}"),
            Self::Json(error) => write!(f, "runtime JSON decode error: {error}"),
            Self::Http { status, body } => {
                write!(f, "runtime HTTP error {status}")?;
                if !body.trim().is_empty() {
                    write!(f, ": {body}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for RuntimeClientError {}
