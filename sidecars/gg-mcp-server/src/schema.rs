pub(crate) fn non_empty_string_schema(
    _generator: &mut schemars::SchemaGenerator,
) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "minLength": 1
    })
}

pub(crate) fn nullable_non_empty_string_schema(
    _generator: &mut schemars::SchemaGenerator,
) -> schemars::Schema {
    schemars::json_schema!({
        "anyOf": [
            {
                "type": "string",
                "minLength": 1
            },
            {
                "type": "null"
            }
        ]
    })
}

pub(crate) fn non_empty_string_array_min1_schema(
    _generator: &mut schemars::SchemaGenerator,
) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "items": {
            "type": "string",
            "minLength": 1
        },
        "minItems": 1,
        "description": "Array of agent IDs to remove. When this field is set, remove mode runs and add-member fields (`title`, `prompt`, `model_preset`, `worktree_name`) must be omitted. Example: [\"agent_1\", \"agent_2\"]",
        "examples": [["agent_1", "agent_2"]]
    })
}

pub(crate) fn non_empty_string_array_schema(
    _generator: &mut schemars::SchemaGenerator,
) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "items": {
            "type": "string",
            "minLength": 1
        }
    })
}
