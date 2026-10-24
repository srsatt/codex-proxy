//! Low-level file I/O helpers used to persist and load JSON state files
//! (`state.json`, `tokens.json`) and plain-text files (`config.toml`).
//!
//! All write helpers create missing parent directories automatically so callers
//! do not need to prepare the filesystem before writing.

use std::{
    fs,
    io::Write,
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
    write_string(path, &serde_json::to_string_pretty(value)?)
}

/// Replace a state file atomically with an owner-only, durable temporary file.
pub fn write_string(path: &Path, value: &str) -> Result<()> {
    ensure_parent_dir(path)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    set_owner_only_permissions(temporary.path())?;
    temporary.write_all(value.as_bytes())?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
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

#[cfg(unix)]
fn set_owner_only_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = fs::Permissions::from_mode(0o600);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_contents_and_keeps_owner_only_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write_json(&path, &vec!["old"]).unwrap();
        write_json(&path, &vec!["new"]).unwrap();
        assert_eq!(
            read_json::<Vec<String>>(&path).unwrap().unwrap(),
            vec!["new"]
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_does_not_follow_destination_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("other");
        fs::write(&target, "untouched").unwrap();
        let path = dir.path().join("tokens.json");
        std::os::unix::fs::symlink(&target, &path).unwrap();
        write_string(&path, "replacement").unwrap();
        assert_eq!(fs::read_to_string(target).unwrap(), "untouched");
        assert!(
            !fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}
