use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::Mutex;

use crate::config::GoosetowerConfig;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub subject: String,
    pub workspace_id: String,
    pub scopes: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub expires_at_unix_ms: i64,
    pub jti: String,
}

impl AuthContext {
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes
            .iter()
            .any(|value| value == scope || value == "*")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketClaims {
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub workspace_id: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    pub exp: i64,
    pub iat: i64,
    pub jti: String,
}

#[derive(Debug, Clone)]
pub struct TicketIssuer {
    issuer: String,
    audience: String,
    signing_key: Arc<str>,
    ttl: Duration,
}

impl TicketIssuer {
    pub fn from_config(config: &GoosetowerConfig) -> Result<Self> {
        Ok(Self {
            issuer: config.tickets.issuer.clone(),
            audience: config.tickets.audience.clone(),
            signing_key: Arc::from(resolve_ticket_key(config)?.into_boxed_str()),
            ttl: Duration::from_secs(config.tickets.ttl_secs),
        })
    }

    pub fn mint_dev_ticket(
        &self,
        subject: impl Into<String>,
        workspace_id: impl Into<String>,
        scopes: Vec<String>,
        allowed_origins: Vec<String>,
    ) -> Result<String> {
        let now = now_ms();
        let claims = TicketClaims {
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            sub: subject.into(),
            workspace_id: workspace_id.into(),
            scopes,
            allowed_origins,
            exp: now + self.ttl.as_millis() as i64,
            iat: now,
            jti: format!("jti_{now}_{}", std::process::id()),
        };
        sign_claims(&claims, self.signing_key.as_ref())
    }
}

#[derive(Debug)]
pub struct TicketValidator {
    issuer: String,
    audience: String,
    verification_key: Arc<str>,
    consumed_jtis: Mutex<BTreeMap<String, i64>>,
}

impl TicketValidator {
    pub fn from_config(config: &GoosetowerConfig) -> Result<Self> {
        Ok(Self {
            issuer: config.tickets.issuer.clone(),
            audience: config.tickets.audience.clone(),
            verification_key: Arc::from(resolve_ticket_key(config)?.into_boxed_str()),
            consumed_jtis: Mutex::new(BTreeMap::new()),
        })
    }

    pub async fn validate_and_consume(
        &self,
        ticket: &str,
        origin: &str,
    ) -> Result<AuthContext, TicketValidationError> {
        let claims = verify_claims(ticket, self.verification_key.as_ref())?;
        if claims.iss != self.issuer {
            return Err(TicketValidationError::InvalidIssuer);
        }
        if claims.aud != self.audience {
            return Err(TicketValidationError::InvalidAudience);
        }
        if claims.sub.trim().is_empty() {
            return Err(TicketValidationError::MissingSubject);
        }
        if claims.workspace_id.trim().is_empty() {
            return Err(TicketValidationError::MissingWorkspace);
        }
        if claims.scopes.is_empty() {
            return Err(TicketValidationError::MissingScopes);
        }
        if !claims
            .allowed_origins
            .iter()
            .any(|allowed| allowed.as_str() == origin)
        {
            return Err(TicketValidationError::OriginNotAllowed);
        }
        let now = now_ms();
        if claims.exp <= now {
            return Err(TicketValidationError::Expired);
        }
        if claims.exp - now > 5 * 60 * 1000 {
            return Err(TicketValidationError::ExpiryTooLong);
        }
        if claims.jti.trim().is_empty() {
            return Err(TicketValidationError::MissingJti);
        }

        let mut consumed = self.consumed_jtis.lock().await;
        consumed.retain(|_, expires_at| *expires_at > now);
        if consumed.contains_key(&claims.jti) {
            return Err(TicketValidationError::Replay);
        }
        consumed.insert(claims.jti.clone(), claims.exp);
        drop(consumed);

        Ok(AuthContext {
            subject: claims.sub,
            workspace_id: claims.workspace_id,
            scopes: claims.scopes,
            allowed_origins: claims.allowed_origins,
            expires_at_unix_ms: claims.exp,
            jti: claims.jti,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TicketValidationError {
    Malformed,
    BadSignature,
    InvalidIssuer,
    InvalidAudience,
    MissingSubject,
    MissingWorkspace,
    MissingScopes,
    OriginNotAllowed,
    Expired,
    ExpiryTooLong,
    MissingJti,
    Replay,
}

impl std::fmt::Display for TicketValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match self {
            Self::Malformed => "malformed_ticket",
            Self::BadSignature => "bad_signature",
            Self::InvalidIssuer => "invalid_issuer",
            Self::InvalidAudience => "invalid_audience",
            Self::MissingSubject => "missing_subject",
            Self::MissingWorkspace => "missing_workspace",
            Self::MissingScopes => "missing_scopes",
            Self::OriginNotAllowed => "origin_not_allowed",
            Self::Expired => "expired_ticket",
            Self::ExpiryTooLong => "ticket_expiry_too_long",
            Self::MissingJti => "missing_jti",
            Self::Replay => "replayed_ticket",
        };
        f.write_str(code)
    }
}

impl std::error::Error for TicketValidationError {}

pub fn origin_is_allowed(origin: &str, allowed_origins: &[String]) -> bool {
    allowed_origins
        .iter()
        .any(|allowed| allowed.as_str() == origin)
}

pub fn sign_claims(claims: &TicketClaims, key: &str) -> Result<String> {
    let payload = serde_json::to_vec(claims).context("serialize ticket claims")?;
    let payload = URL_SAFE_NO_PAD.encode(payload);
    let signature = sign_payload(payload.as_bytes(), key)?;
    Ok(format!("{payload}.{signature}"))
}

fn verify_claims(ticket: &str, key: &str) -> Result<TicketClaims, TicketValidationError> {
    let (payload, signature) = ticket
        .split_once('.')
        .ok_or(TicketValidationError::Malformed)?;
    let expected =
        sign_payload(payload.as_bytes(), key).map_err(|_| TicketValidationError::Malformed)?;
    if signature != expected {
        return Err(TicketValidationError::BadSignature);
    }
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| TicketValidationError::Malformed)?;
    serde_json::from_slice::<TicketClaims>(&payload).map_err(|_| TicketValidationError::Malformed)
}

fn sign_payload(payload: &[u8], key: &str) -> Result<String> {
    let mut mac =
        HmacSha256::new_from_slice(key.as_bytes()).map_err(|_| anyhow!("invalid ticket key"))?;
    mac.update(payload);
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn resolve_ticket_key(config: &GoosetowerConfig) -> Result<String> {
    if let Some(key) = config
        .tickets
        .verification_key
        .as_ref()
        .or(config.tickets.signing_key.as_ref())
    {
        let trimmed = key.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Some(path) = config
        .tickets
        .verification_key_file
        .as_ref()
        .or(config.tickets.signing_key_file.as_ref())
    {
        let key = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read ticket key file {}", path.display()))?;
        let trimmed = key.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    Err(anyhow!("ticket verification key is required"))
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GoosetowerConfig;

    #[tokio::test]
    async fn validates_signed_ticket_once_for_exact_origin() {
        let config = GoosetowerConfig::default();
        let issuer = TicketIssuer::from_config(&config).expect("issuer");
        let validator = TicketValidator::from_config(&config).expect("validator");
        let ticket = issuer
            .mint_dev_ticket(
                "user_1",
                "default",
                vec!["gateway:connect".to_string()],
                vec!["http://localhost:3000".to_string()],
            )
            .expect("ticket");

        let auth = validator
            .validate_and_consume(&ticket, "http://localhost:3000")
            .await
            .expect("valid ticket");
        assert_eq!(auth.subject, "user_1");
        assert_eq!(
            validator
                .validate_and_consume(&ticket, "http://localhost:3000")
                .await
                .expect_err("replay rejected"),
            TicketValidationError::Replay
        );
    }

    #[test]
    fn origin_matching_is_exact() {
        assert!(origin_is_allowed(
            "https://gooseweb.example.com",
            &["https://gooseweb.example.com".to_string()]
        ));
        assert!(!origin_is_allowed(
            "https://evil.gooseweb.example.com",
            &["https://gooseweb.example.com".to_string()]
        ));
    }
}
