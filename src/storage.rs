use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};

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

pub fn write_json<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    ensure_parent_dir(path)?;
    let content = serde_json::to_string_pretty(value)?;
    fs::write(path, content)?;
    Ok(())
}

pub fn write_string(path: &Path, value: &str) -> Result<()> {
    ensure_parent_dir(path)?;
    fs::write(path, value)?;
    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }

    Ok(())
}

#[allow(dead_code)]
pub fn normalize_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
