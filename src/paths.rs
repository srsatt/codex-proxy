//! Centralised runtime path management.
//!
//! [`AppPaths`] is constructed once from CLI arguments and then threaded
//! through the entire application so every component reads and writes the same
//! set of files without hard-coding paths in multiple places.
//!
//! Default layout (relative to the working directory):
//! ```text
//! ./config.toml          — runtime configuration (TOML)
//! ./data/state.json      — fake API key and pending OAuth flow state
//! ./data/tokens.json     — persisted OAuth access and refresh tokens
//! ```
//! Inside Docker the working directory is `/app`, so the same relative paths
//! resolve to `/app/config.toml`, `/app/data/state.json`, etc.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;

/// All file paths used by the codex-proxy runtime, derived from the two root
/// locations supplied on the command line.
#[derive(Debug, Clone)]
pub struct AppPaths {
    /// Path to `config.toml`.
    pub config_path: PathBuf,
    /// Directory that contains `state.json` and `tokens.json`.
    pub data_dir: PathBuf,
    /// `<data_dir>/tokens.json` — persisted OAuth tokens.
    pub tokens_path: PathBuf,
    /// `<data_dir>/state.json` — fake API key and pending auth state.
    pub state_path: PathBuf,
}

impl AppPaths {
    /// Build an `AppPaths` from the two root locations.
    ///
    /// `tokens_path` and `state_path` are derived automatically so all
    /// callers use consistent paths.
    pub fn new(config_path: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            config_path,
            tokens_path: data_dir.join("tokens.json"),
            state_path: data_dir.join("state.json"),
            data_dir,
        }
    }

    /// Create the directories required by the runtime layout.
    ///
    /// - Parent directory of `config_path` (e.g. `./` or a custom prefix).
    /// - `data_dir` itself (e.g. `./data/`).
    ///
    /// This is idempotent and safe to call on every startup.
    pub fn ensure_runtime_layout(&self) -> Result<()> {
        ensure_parent_dir(&self.config_path)?;
        fs::create_dir_all(&self.data_dir)?;
        Ok(())
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }

    Ok(())
}
