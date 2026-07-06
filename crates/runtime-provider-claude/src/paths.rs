use std::path::{Path, PathBuf};

pub(crate) fn default_bridge_command() -> String {
    std::env::var("GG_CLAUDE_BRIDGE_COMMAND").unwrap_or_else(|_| {
        standalone_claude_bridge_command_path()
            .display()
            .to_string()
    })
}

pub(crate) fn default_bridge_args() -> Vec<String> {
    if let Ok(raw) = std::env::var("GG_CLAUDE_BRIDGE_ARGS_JSON") {
        if let Ok(parsed) = serde_json::from_str::<Vec<String>>(raw.as_str()) {
            if !parsed.is_empty() {
                return parsed;
            }
        }
    }
    Vec::new()
}

fn runtime_install_root_from_executable(executable_path: &Path) -> PathBuf {
    let executable_dir = executable_path.parent().unwrap_or_else(|| Path::new("."));
    if executable_dir.ends_with("bin") {
        executable_dir
            .parent()
            .unwrap_or(executable_dir)
            .to_path_buf()
    } else {
        executable_dir.to_path_buf()
    }
}

pub(crate) fn sidecar_command_path_from_executable(
    executable_path: &Path,
    sidecar: &str,
    binary: &str,
) -> PathBuf {
    runtime_install_root_from_executable(executable_path)
        .join("sidecars")
        .join(sidecar)
        .join(binary)
}

fn workspace_sidecar_command_path_if_present(
    workspace_root: &Path,
    sidecar: &str,
    binary: &str,
    workspace_launcher: &str,
) -> Option<PathBuf> {
    let workspace_sidecar_binary = workspace_root.join("sidecars").join(sidecar).join(binary);
    if workspace_sidecar_binary.exists() {
        return Some(workspace_sidecar_binary);
    }

    let workspace_sidecar_launcher = workspace_root
        .join("sidecars")
        .join(sidecar)
        .join("bin")
        .join(workspace_launcher);
    if workspace_sidecar_launcher.exists() {
        return Some(workspace_sidecar_launcher);
    }

    None
}

pub(crate) fn workspace_root_from_target_binary_path(executable_path: &Path) -> Option<PathBuf> {
    for ancestor in executable_path.ancestors() {
        if ancestor.file_name().is_some_and(|name| name == "target") {
            return ancestor.parent().map(Path::to_path_buf);
        }
    }
    None
}

pub(crate) fn sidecar_command_path_for_executable(
    executable_path: &Path,
    sidecar: &str,
    binary: &str,
    workspace_launcher: &str,
) -> PathBuf {
    let workspace_roots = std::env::current_dir()
        .map(|cwd| vec![cwd])
        .unwrap_or_default();
    sidecar_command_path_for_executable_with_workspace_roots(
        executable_path,
        sidecar,
        binary,
        workspace_launcher,
        workspace_roots.as_slice(),
    )
}

pub(crate) fn sidecar_command_path_for_executable_with_workspace_roots(
    executable_path: &Path,
    sidecar: &str,
    binary: &str,
    workspace_launcher: &str,
    workspace_roots: &[PathBuf],
) -> PathBuf {
    let install_path = sidecar_command_path_from_executable(executable_path, sidecar, binary);
    if install_path.exists() {
        return install_path;
    }

    if let Some(workspace_root) = workspace_root_from_target_binary_path(executable_path) {
        if let Some(workspace_sidecar_path) = workspace_sidecar_command_path_if_present(
            workspace_root.as_path(),
            sidecar,
            binary,
            workspace_launcher,
        ) {
            return workspace_sidecar_path;
        }
    }

    for workspace_root in workspace_roots {
        for ancestor in workspace_root.ancestors() {
            if let Some(workspace_sidecar_path) = workspace_sidecar_command_path_if_present(
                ancestor,
                sidecar,
                binary,
                workspace_launcher,
            ) {
                return workspace_sidecar_path;
            }
        }
    }

    install_path
}

fn sidecar_command_path_for_current_executable(sidecar: &str, binary: &str) -> PathBuf {
    match std::env::current_exe() {
        Ok(executable_path) => sidecar_command_path_for_executable(
            executable_path.as_path(),
            sidecar,
            binary,
            &format!("{binary}-dev"),
        ),
        Err(_) => PathBuf::from("sidecars").join(sidecar).join(binary),
    }
}

pub fn standalone_claude_bridge_command_path() -> PathBuf {
    sidecar_command_path_for_current_executable("claude-bridge", "claude-bridge")
}

pub fn standalone_gg_mcp_server_command_path() -> PathBuf {
    sidecar_command_path_for_current_executable("gg-mcp-server", "gg-mcp-server")
}

pub(crate) fn default_gg_mcp_server_command() -> String {
    std::env::var("GG_MCP_SERVER_PATH").unwrap_or_else(|_| {
        standalone_gg_mcp_server_command_path()
            .display()
            .to_string()
    })
}
