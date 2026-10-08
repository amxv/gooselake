use runtime_core::RuntimeError;
use serde_json::{json, Value};

use crate::state::AcpAgentCapabilities;

/// Preserve protocol-native content. Unsupported native/local image, file and
/// skill inputs must never be reinterpreted as text instructions to the agent.
pub(super) fn build_prompt_blocks(
    input: &[Value],
    capabilities: &AcpAgentCapabilities,
) -> Result<Vec<Value>, RuntimeError> {
    let mut blocks = Vec::with_capacity(input.len());
    for (index, item) in input.iter().enumerate() {
        if let Some(text) = item.as_str() {
            if text.trim().is_empty() {
                return Err(invalid(index, "text must not be blank"));
            }
            blocks.push(json!({ "type": "text", "text": text }));
            continue;
        }
        let type_name = item
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| unsupported(index, "input is not a typed ACP content block"))?;
        match type_name {
            "text" => {
                if item
                    .get("text")
                    .and_then(Value::as_str)
                    .is_none_or(|s| s.trim().is_empty())
                {
                    return Err(invalid(index, "text content requires nonblank text"));
                }
            }
            "resource_link" => {
                if item
                    .get("uri")
                    .and_then(Value::as_str)
                    .is_none_or(|s| s.trim().is_empty())
                    || item
                        .get("name")
                        .and_then(Value::as_str)
                        .is_none_or(|s| s.trim().is_empty())
                {
                    return Err(invalid(index, "resource_link requires uri and name"));
                }
            }
            "image" => {
                if !capabilities.prompt_image {
                    return Err(unsupported(
                        index,
                        "ACP agent did not advertise image prompt support",
                    ));
                }
                require_binary_block(item, index, "image/")?;
            }
            "audio" => {
                if !capabilities.prompt_audio {
                    return Err(unsupported(
                        index,
                        "ACP agent did not advertise audio prompt support",
                    ));
                }
                require_binary_block(item, index, "audio/")?;
            }
            "resource" => {
                if !capabilities.prompt_embedded_context {
                    return Err(unsupported(
                        index,
                        "ACP agent did not advertise embedded context",
                    ));
                }
                if item.get("resource").and_then(Value::as_object).is_none() {
                    return Err(invalid(
                        index,
                        "embedded resource requires a resource object",
                    ));
                }
            }
            _ => {
                return Err(unsupported(
                    index,
                    &format!("ACP content type {type_name:?} is unsupported"),
                ))
            }
        }
        blocks.push(item.clone());
    }
    if blocks.is_empty() {
        return Err(RuntimeError::InvalidState(
            "ACP prompt requires at least one content block".into(),
        ));
    }
    Ok(blocks)
}

fn require_binary_block(item: &Value, index: usize, mime_prefix: &str) -> Result<(), RuntimeError> {
    let mime = item.get("mimeType").and_then(Value::as_str).unwrap_or("");
    let data = item.get("data").and_then(Value::as_str).unwrap_or("");
    if !mime.starts_with(mime_prefix) || data.is_empty() || data.len() > 32 * 1024 * 1024 {
        return Err(invalid(
            index,
            "binary ACP content requires mimeType and bounded base64 data",
        ));
    }
    if data.len() % 4 != 0
        || !data
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        return Err(invalid(index, "binary ACP content data must be base64"));
    }
    Ok(())
}

fn unsupported(index: usize, reason: &str) -> RuntimeError {
    RuntimeError::Unsupported(format!("ACP prompt block {index}: {reason}"))
}

fn invalid(index: usize, reason: &str) -> RuntimeError {
    RuntimeError::InvalidState(format!("ACP prompt block {index}: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_and_links_are_native_and_unknown_inputs_never_flatten() {
        let caps = AcpAgentCapabilities::default();
        let input = vec![
            json!({"type":"text","text":" line one\n"}),
            json!({"type":"resource_link","uri":"file:///tmp/a","name":"a"}),
        ];
        assert_eq!(build_prompt_blocks(&input, &caps).unwrap(), input);
        assert!(matches!(
            build_prompt_blocks(&[json!({"type":"skill","name":"foo"})], &caps),
            Err(RuntimeError::Unsupported(_))
        ));
        assert!(matches!(
            build_prompt_blocks(&[json!({"type":"local_image","path":"/tmp/a.png"})], &caps),
            Err(RuntimeError::Unsupported(_))
        ));
        assert!(matches!(
            build_prompt_blocks(&[], &caps),
            Err(RuntimeError::InvalidState(_))
        ));
    }

    #[test]
    fn images_require_negotiated_capability_and_preserve_order() {
        let image = json!({"type":"image","mimeType":"image/png","data":"YWJj"});
        assert!(matches!(
            build_prompt_blocks(&[image.clone()], &AcpAgentCapabilities::default()),
            Err(RuntimeError::Unsupported(_))
        ));
        let caps = AcpAgentCapabilities {
            prompt_image: true,
            ..Default::default()
        };
        let input = vec![json!({"type":"text","text":"first"}), image.clone()];
        assert_eq!(build_prompt_blocks(&input, &caps).unwrap(), input);
        assert!(build_prompt_blocks(
            &[json!({"type":"image","mimeType":"image/png","data":"not-base64"})],
            &caps
        )
        .is_err());
    }
}
