//! Low-level file I/O helpers used to persist and load JSON state files
//! (`state.json`, `tokens.json`) and plain-text files (`config.toml`).
//!
//! All write helpers create missing parent directories automatically so callers
//! do not need to prepare the filesystem before writing.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};

/// Deserialize a JSON file into `T`.
///
/// Returns `Ok(None)` when the file does not exist, which lets callers
/// distinguish "not yet created" from a parse error.
pub fn read_json<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(path)?;
    let value = serde_json::from_str(&content)?;
    Ok(Some(value))
}

/// Serialize `value` as pretty-printed JSON and write it to `path`.
///
/// Creates all missing parent directories before writing.
pub fn write_json<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    ensure_parent_dir(path)?;
    let content = serde_json::to_string_pretty(value)?;
    fs::write(path, content)?;
    Ok(())
}

/// Write a plain string to `path`, creating parent directories as needed.
pub fn write_string(path: &Path, value: &str) -> Result<()> {
    ensure_parent_dir(path)?;
    fs::write(path, value)?;
    Ok(())
}

/// Create all directories in `path`'s parent chain if they don't exist yet.
fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }

    Ok(())
}

/// Resolve `path` to its canonical form; falls back to the original path on
/// error (e.g. when the path does not yet exist on disk).
#[allow(dead_code)]
pub fn normalize_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
