//! Scoped agent credentials for the model-facing MCP gateway.
//!
//! The long-lived operator token remains inside the runtime/provider adapter;
//! only this per-agent HMAC credential is passed to an MCP subprocess.

use ring::hmac;

const TOKEN_VERSION: &str = "gg_agent_v1";
const DOMAIN: &[u8] = b"gooselake:mcp:agent:v1\0";

pub fn mint_scoped_mcp_token(operator_secret: &str, agent_id: &str) -> String {
    assert!(
        valid_agent_id(agent_id),
        "invalid scoped MCP agent identity"
    );
    let signature = signature(operator_secret, agent_id);
    format!(
        "{TOKEN_VERSION}.{}.{}",
        hex(agent_id.as_bytes()),
        hex(signature.as_ref())
    )
}

pub fn agent_from_scoped_mcp_token(operator_secret: &str, token: &str) -> Option<String> {
    if token.len() > 400 {
        return None;
    }
    let mut parts = token.split('.');
    if parts.next()? != TOKEN_VERSION {
        return None;
    }
    let identity = decode_hex(parts.next()?)?;
    let signed = decode_hex(parts.next()?)?;
    if parts.next().is_some() || signed.len() != 32 {
        return None;
    }
    let agent_id = String::from_utf8(identity).ok()?;
    if !valid_agent_id(&agent_id) {
        return None;
    }
    let key = hmac::Key::new(hmac::HMAC_SHA256, operator_secret.as_bytes());
    let mut input = DOMAIN.to_vec();
    input.extend_from_slice(agent_id.as_bytes());
    hmac::verify(&key, &input, &signed).ok()?;
    Some(agent_id)
}

fn signature(secret: &str, identity: &str) -> hmac::Tag {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let mut input = DOMAIN.to_vec();
    input.extend_from_slice(identity.as_bytes());
    hmac::sign(&key, &input)
}

fn valid_agent_id(identity: &str) -> bool {
    !identity.is_empty()
        && identity.len() <= 128
        && identity
            .bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'-')
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    encoded
}

fn decode_hex(encoded: &str) -> Option<Vec<u8>> {
    if encoded.len() % 2 != 0 {
        return None;
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_tokens_are_agent_bound_and_secret_bound() {
        let one = mint_scoped_mcp_token("operator-secret", "sess_one");
        let two = mint_scoped_mcp_token("operator-secret", "sess_two");
        assert_ne!(one, two);
        assert_eq!(
            agent_from_scoped_mcp_token("operator-secret", &one),
            Some("sess_one".into())
        );
        assert_eq!(agent_from_scoped_mcp_token("rotated-secret", &one), None);
        assert_eq!(
            agent_from_scoped_mcp_token("operator-secret", "operator-secret"),
            None
        );
        assert_eq!(
            agent_from_scoped_mcp_token("operator-secret", "Bearer operator-secret"),
            None
        );
        assert_eq!(
            agent_from_scoped_mcp_token("operator-secret", &(one + "ff")),
            None
        );
        // The identity is hex-encoded, so modifying plaintext `sess` would
        // leave the credential untouched. Tamper with the *encoded identity*
        // while retaining the original signature instead.
        let tampered = two.replace(&hex(b"sess_two"), &hex(b"sess_one"));
        assert_ne!(tampered, two);
        assert_eq!(
            agent_from_scoped_mcp_token("operator-secret", &tampered),
            None
        );
        let forged_with_other_secret = mint_scoped_mcp_token("rotated-secret", "sess_two");
        assert_eq!(
            agent_from_scoped_mcp_token("operator-secret", &forged_with_other_secret),
            None
        );
    }

    #[test]
    fn scoped_tokens_fail_closed_on_bad_format() {
        for token in [
            "",
            "gg_agent_v1.foo",
            "gg_agent_v1...",
            "gg_agent_v1.00.00",
            "gg_agent_v2.00.00",
        ] {
            assert!(agent_from_scoped_mcp_token("secret", token).is_none());
        }
        assert_eq!(
            agent_from_scoped_mcp_token("secret", &"x".repeat(401)),
            None
        );
    }
}
