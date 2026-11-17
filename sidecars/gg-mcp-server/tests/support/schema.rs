/// Inspect the contract schema in tests, following local refs rather than
/// accidentally treating description prose as a declared enum.
pub(crate) fn string_enum_values(
    root: &serde_json::Value,
    field: &serde_json::Value,
) -> Vec<String> {
    fn resolve(
        root: &serde_json::Value,
        node: &serde_json::Value,
        depth: usize,
    ) -> Option<Vec<String>> {
        if depth == 0 {
            return None;
        }
        if let Some(values) = node.get("enum").and_then(serde_json::Value::as_array) {
            return values
                .iter()
                .map(|value| value.as_str().map(str::to_string))
                .collect();
        }
        if let Some(path) = node
            .get("$ref")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| s.strip_prefix('#'))
        {
            return root
                .pointer(path)
                .and_then(|target| resolve(root, target, depth - 1));
        }
        for key in ["anyOf", "oneOf"] {
            if let Some(variants) = node.get(key).and_then(serde_json::Value::as_array) {
                if let Some(found) = variants
                    .iter()
                    .find_map(|value| resolve(root, value, depth - 1))
                {
                    return Some(found);
                }
            }
        }
        None
    }
    let mut values = resolve(root, field, 16).unwrap_or_default();
    values.sort();
    values
}
