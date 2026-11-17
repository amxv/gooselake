//! Real session-control owners; neither family is a runtime_operations row.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum SessionControlCoordinate {
    EditRerun {
        agent_id: String,
        operation_id: String,
    },
    Compaction {
        agent_id: String,
        run_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn family_paths_encode_each_original_component_without_inventing_owners() {
        let edit = SessionControlCoordinate::EditRerun {
            agent_id: "agent/ ?#%é".into(),
            operation_id: "..".into(),
        };
        assert_eq!(
            edit.inspection_path(),
            "/v2/agents/agent%2F%20%3F%23%25%C3%A9/edits/%2E%2E"
        );
        let details = edit.error_details(Some("actual-turn"));
        assert_eq!(details["agent_id"], "agent/ ?#%é");
        assert_eq!(details["operation_id"], "..");
        assert_eq!(details["replacement_turn_id"], "actual-turn");
        assert_eq!(details["native_retry_authorized"], false);
        let compaction = SessionControlCoordinate::Compaction {
            agent_id: ".".into(),
            run_id: "run/?%é".into(),
        };
        assert_eq!(
            compaction.inspection_path(),
            "/v2/agents/%2E/compactions/run%2F%3F%25%C3%A9"
        );
        let details = compaction.error_details(Some("not-a-compaction-owner"));
        assert!(details.get("operation_id").is_none());
        assert!(details.get("replacement_turn_id").is_none());
        assert_eq!(details["run_id"], "run/?%é");
    }
}

impl SessionControlCoordinate {
    pub fn agent_id(&self) -> &str {
        match self {
            Self::EditRerun { agent_id, .. } | Self::Compaction { agent_id, .. } => agent_id,
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::EditRerun { operation_id, .. } => operation_id,
            Self::Compaction { run_id, .. } => run_id,
        }
    }

    pub fn inspection_path(&self) -> String {
        let family = match self {
            Self::EditRerun { .. } => "edits",
            Self::Compaction { .. } => "compactions",
        };
        format!(
            "/v2/agents/{}/{}/{}",
            crate::error::encode_inspection_path_segment(self.agent_id()),
            family,
            crate::error::encode_inspection_path_segment(self.id())
        )
    }

    pub fn error_details(&self, replacement_turn_id: Option<&str>) -> Value {
        let mut details = json!(self);
        details["inspection_path"] = json!(self.inspection_path());
        details["native_retry_authorized"] = json!(false);
        if matches!(self, Self::EditRerun { .. }) {
            if let Some(turn_id) = replacement_turn_id {
                details["replacement_turn_id"] = json!(turn_id);
            }
        }
        details
    }
}
