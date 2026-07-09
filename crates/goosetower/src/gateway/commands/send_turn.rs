use serde_json::{json, Value};

use super::{non_empty, CommandRouteError, REASON_INVALID_TARGET};

pub(super) fn command_send_turn_input(
    input: &crate::protocol::generated::goosetower::v1::CommandSendTurn,
) -> Result<Vec<Value>, CommandRouteError> {
    if input.input.is_empty() {
        let text = non_empty(&input.text, "text")?;
        return Ok(vec![json!({ "type": "text", "text": text })]);
    }

    let mut items = Vec::with_capacity(input.input.len());
    for item in &input.input {
        let item_type = non_empty(&item.r#type, "input.type")?;
        match item_type {
            "text" => {
                let text = non_empty(&item.text, "input.text")?;
                items.push(json!({ "type": "text", "text": text }));
            }
            "image" => {
                let media_type = non_empty(&item.media_type, "input.media_type")?;
                if !media_type.starts_with("image/") {
                    return Err(CommandRouteError::with_code(
                        REASON_INVALID_TARGET,
                        "input.media_type must be an image MIME type",
                        false,
                    ));
                }
                let data = non_empty(&item.data, "input.data")?;
                items.push(json!({
                    "type": "image",
                    "media_type": media_type,
                    "data": data
                }));
            }
            _ => {
                return Err(CommandRouteError::with_code(
                    REASON_INVALID_TARGET,
                    format!("unsupported input.type {item_type}"),
                    false,
                ));
            }
        }
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::generated::goosetower::v1::{CommandInputItem, CommandSendTurn};

    #[test]
    fn send_turn_input_preserves_structured_image_parts() {
        let payload = command_send_turn_input(&CommandSendTurn {
            session_id: "session_1".to_string(),
            text: "fallback".to_string(),
            input: vec![
                CommandInputItem {
                    r#type: "text".to_string(),
                    text: "Inspect this image".to_string(),
                    ..CommandInputItem::default()
                },
                CommandInputItem {
                    r#type: "image".to_string(),
                    media_type: "image/png".to_string(),
                    data: "iVBORw0KGgo=".to_string(),
                    ..CommandInputItem::default()
                },
            ],
        })
        .expect("structured payload");

        assert_eq!(payload.len(), 2);
        assert_eq!(payload[0]["type"], "text");
        assert_eq!(payload[0]["text"], "Inspect this image");
        assert_eq!(payload[1]["type"], "image");
        assert_eq!(payload[1]["media_type"], "image/png");
        assert_eq!(payload[1]["data"], "iVBORw0KGgo=");
    }

    #[test]
    fn send_turn_input_rejects_non_image_media_type() {
        let error = command_send_turn_input(&CommandSendTurn {
            session_id: "session_1".to_string(),
            input: vec![CommandInputItem {
                r#type: "image".to_string(),
                media_type: "application/pdf".to_string(),
                data: "JVBERi0=".to_string(),
                ..CommandInputItem::default()
            }],
            ..CommandSendTurn::default()
        })
        .expect_err("invalid media type");

        assert_eq!(error.code, REASON_INVALID_TARGET);
    }
}
