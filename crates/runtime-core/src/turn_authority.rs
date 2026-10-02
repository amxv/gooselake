use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TurnInputProjectionSource {
    #[default]
    UserVisible,
    AgentMessageDeliveryTransport,
    AutomationContext,
}

impl TurnInputProjectionSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserVisible => "user_visible",
            Self::AgentMessageDeliveryTransport => "agent_message_delivery_transport",
            Self::AutomationContext => "automation_context",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "user_visible" => Some(Self::UserVisible),
            "agent_message_delivery_transport" => Some(Self::AgentMessageDeliveryTransport),
            "automation_context" => Some(Self::AutomationContext),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedUserInputSnapshotImageRef {
    pub media_type: String,
    pub persisted_absolute_path: Option<String>,
    pub persisted_display_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedUserInputSnapshotInvocation {
    pub provider: String,
    pub name: String,
    pub display_name: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedUserInputSnapshot {
    pub prompt_text: String,
    #[serde(default)]
    pub image_refs: Vec<PersistedUserInputSnapshotImageRef>,
    pub invocation: Option<PersistedUserInputSnapshotInvocation>,
}

impl PersistedUserInputSnapshot {
    pub fn from_input(input: &[Value]) -> Self {
        let mut prompt_parts = Vec::new();
        let mut image_refs = Vec::new();
        for item in input {
            if let Some(text) = item
                .get("text")
                .and_then(Value::as_str)
                .or_else(|| item.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                prompt_parts.push(text.to_string());
            }
            let is_image = item.get("type").and_then(Value::as_str) == Some("image");
            if is_image {
                let path = item
                    .get("path")
                    .or_else(|| item.get("file_path"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);
                if let Some(path) = path {
                    let media_type = item
                        .get("media_type")
                        .or_else(|| item.get("mediaType"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("image/*")
                        .to_string();
                    image_refs.push(PersistedUserInputSnapshotImageRef {
                        media_type,
                        persisted_absolute_path: Some(path.clone()),
                        persisted_display_path: Some(path),
                    });
                }
            }
        }
        Self {
            prompt_text: prompt_parts.join("\n\n"),
            image_refs,
            invocation: None,
        }
    }

    pub fn first_missing_image_path(&self) -> Option<String> {
        self.image_refs.iter().find_map(|image| {
            let Some(path) = image.persisted_absolute_path.as_deref() else {
                return Some(
                    image
                        .persisted_display_path
                        .clone()
                        .unwrap_or_else(|| "<missing persisted image path>".to_string()),
                );
            };
            if std::path::Path::new(path).is_file() {
                None
            } else {
                Some(path.to_string())
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnDispatchPolicySnapshot {
    pub permission_mode: Option<String>,
    pub pre_dispatch_approval_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TurnCorrelationState {
    pub expected_turn_id: Option<String>,
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnDispatchState {
    Pending,
    Dispatching,
    Dispatched,
    NotDispatched,
    Unknown,
}

impl TurnDispatchState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Dispatching => "dispatching",
            Self::Dispatched => "dispatched",
            Self::NotDispatched => "not_dispatched",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "pending" => Some(Self::Pending),
            "dispatching" => Some(Self::Dispatching),
            "dispatched" => Some(Self::Dispatched),
            "not_dispatched" => Some(Self::NotDispatched),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnAdmissionRecord {
    pub turn_id: String,
    pub session_id: String,
    pub provider: String,
    pub projection_source: TurnInputProjectionSource,
    pub user_input_snapshot: PersistedUserInputSnapshot,
    pub dispatch_policy: TurnDispatchPolicySnapshot,
    pub correlation: TurnCorrelationState,
    pub dispatch_state: TurnDispatchState,
    pub provider_native_turn_id: Option<String>,
    pub dispatch_error: Option<Value>,
    pub admitted_at: i64,
    pub updated_at: i64,
}
