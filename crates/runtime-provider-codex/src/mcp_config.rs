use crate::CodexGgMcpConfig;

pub(crate) fn format_codex_gg_mcp_config(
    config: &CodexGgMcpConfig,
    runtime_session_id: &str,
) -> String {
    let server_name = config.server_name.trim();
    let server_name = if server_name.is_empty() {
        "gg"
    } else {
        server_name
    };
    let mut output = String::new();
    output.push_str(format!("[mcp_servers.{}]\n", toml_key_segment(server_name)).as_str());
    output.push_str(format!("command = {}\n", toml_string(config.command.as_str())).as_str());
    output.push_str("args = [");
    for (index, arg) in config.args.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(toml_string(arg).as_str());
    }
    output.push_str("]\n\n");
    output.push_str(format!("[mcp_servers.{}.env]\n", toml_key_segment(server_name)).as_str());
    output.push_str(
        format!(
            "GG_MCP_ENABLE_PROCESS_TOOLS = {}\n",
            toml_string(if config.enable_process_tools {
                "1"
            } else {
                "0"
            })
        )
        .as_str(),
    );
    output.push_str("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID = \"1\"\n");
    output.push_str(
        format!(
            "GG_MCP_CALLER_AGENT_ID = {}\n",
            toml_string(runtime_session_id)
        )
        .as_str(),
    );
    if let Some(gateway_url) = config
        .gateway_url
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        output.push_str(format!("GG_MCP_GATEWAY_URL = {}\n", toml_string(gateway_url)).as_str());
    }
    if let Some(gateway_token) = config
        .gateway_token
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        output
            .push_str(format!("GG_MCP_GATEWAY_TOKEN = {}\n", toml_string(gateway_token)).as_str());
    }
    output
}

fn toml_key_segment(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return value.to_string();
    }
    toml_string(value)
}

fn toml_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => escaped.push_str(format!("\\u{:04x}", ch as u32).as_str()),
            ch => escaped.push(ch),
        }
    }
    escaped.push('"');
    escaped
}
