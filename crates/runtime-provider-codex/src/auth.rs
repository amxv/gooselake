use std::path::{Path, PathBuf};

use runtime_core::RuntimeError;

pub fn copy_codex_auth_file(
    source: &Path,
    destination_home: &Path,
) -> Result<PathBuf, RuntimeError> {
    std::fs::create_dir_all(destination_home)?;
    let target = destination_home.join("auth.json");
    std::fs::copy(source, &target).map_err(|error| RuntimeError::Io(error.to_string()))?;
    Ok(target)
}
